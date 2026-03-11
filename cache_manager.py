"""
求人データベース管理（SQLite + FTS5）
- 求人データの永続保存・全文検索
- URL重複排除
- フィルタ・統計
"""

import sqlite3
import os
import re
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
CACHE_DB = os.path.join(CACHE_DIR, "jobs_cache.db")

_local = threading.local()


def _ensure_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _get_conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        _ensure_dir()
        _local.conn = sqlite3.connect(CACHE_DB)
        _local.conn.row_factory = sqlite3.Row
        _init_db(_local.conn)
    return _local.conn


def _init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            url TEXT PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            company TEXT DEFAULT '',
            location TEXT DEFAULT '',
            salary TEXT DEFAULT '',
            description TEXT DEFAULT '',
            source TEXT DEFAULT '',
            pub_date TEXT DEFAULT '',
            created_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS jobs_fts USING fts5(
            title, company, location, salary, description,
            content='jobs', content_rowid='rowid'
        );

        CREATE TRIGGER IF NOT EXISTS jobs_ai AFTER INSERT ON jobs BEGIN
            INSERT INTO jobs_fts(rowid, title, company, location, salary, description)
            VALUES (new.rowid, new.title, new.company, new.location, new.salary, new.description);
        END;

        CREATE TRIGGER IF NOT EXISTS jobs_ad AFTER DELETE ON jobs BEGIN
            INSERT INTO jobs_fts(jobs_fts, rowid, title, company, location, salary, description)
            VALUES ('delete', old.rowid, old.title, old.company, old.location, old.salary, old.description);
        END;

        CREATE TRIGGER IF NOT EXISTS jobs_au AFTER UPDATE ON jobs BEGIN
            INSERT INTO jobs_fts(jobs_fts, rowid, title, company, location, salary, description)
            VALUES ('delete', old.rowid, old.title, old.company, old.location, old.salary, old.description);
            INSERT INTO jobs_fts(rowid, title, company, location, salary, description)
            VALUES (new.rowid, new.title, new.company, new.location, new.salary, new.description);
        END;

        -- 自動取得用キーワード管理
        CREATE TABLE IF NOT EXISTS collection_keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            location TEXT DEFAULT '',
            enabled INTEGER DEFAULT 1,
            created_at TEXT DEFAULT ''
        );

        -- 保存済み候補者
        CREATE TABLE IF NOT EXISTS saved_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            info_json TEXT DEFAULT '{}',
            strengths_json TEXT DEFAULT '[]',
            conditions_json TEXT DEFAULT '{}',
            created_at TEXT DEFAULT ''
        );

        -- 提案（候補者×求人の進捗管理）
        CREATE TABLE IF NOT EXISTS proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER NOT NULL,
            job_url TEXT NOT NULL,
            status TEXT DEFAULT '提案済み',
            memo TEXT DEFAULT '',
            next_action TEXT DEFAULT '',
            created_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT '',
            UNIQUE(candidate_id, job_url)
        );

        -- 面談シート
        CREATE TABLE IF NOT EXISTS interview_sheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER NOT NULL,
            raw_input TEXT DEFAULT '',
            sheet_content TEXT DEFAULT '',
            tags_json TEXT DEFAULT '[]',
            created_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

        -- 設定
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT DEFAULT '{}'
        );

        -- 取得ログ
        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at TEXT,
            keywords_used INTEGER,
            jobs_found INTEGER,
            jobs_saved INTEGER,
            sources TEXT,
            duration_sec REAL
        );

        -- チャット履歴
        CREATE TABLE IF NOT EXISTS chat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tab TEXT NOT NULL DEFAULT 'global',
            role TEXT NOT NULL DEFAULT 'user',
            content TEXT NOT NULL DEFAULT '',
            context_json TEXT DEFAULT '{}',
            created_at TEXT DEFAULT ''
        );

        -- アクセスログ（セキュリティ監査用）
        CREATE TABLE IF NOT EXISTS access_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL DEFAULT '',
            detail TEXT DEFAULT '',
            created_at TEXT DEFAULT ''
        );

        -- 候補者アップロードファイル管理
        CREATE TABLE IF NOT EXISTS candidate_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER NOT NULL,
            filename TEXT NOT NULL DEFAULT '',
            file_type TEXT DEFAULT '',
            doc_type TEXT DEFAULT '',
            file_size INTEGER DEFAULT 0,
            extracted_text_length INTEGER DEFAULT 0,
            created_at TEXT DEFAULT '',
            FOREIGN KEY (candidate_id) REFERENCES saved_candidates(id) ON DELETE CASCADE
        );
    """)

    # --- マイグレーション: 既存テーブルへのカラム追加 ---
    # jobs に job_type カラム追加（contracted=契約中 / web=Web掲載中）
    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN job_type TEXT DEFAULT 'web'")
    except sqlite3.OperationalError:
        pass  # 既に存在

    # saved_candidates に tags_json カラム追加
    try:
        conn.execute("ALTER TABLE saved_candidates ADD COLUMN tags_json TEXT DEFAULT '{}'")
    except sqlite3.OperationalError:
        pass  # 既に存在

    # saved_candidates に source_files_json カラム追加
    try:
        conn.execute("ALTER TABLE saved_candidates ADD COLUMN source_files_json TEXT DEFAULT '[]'")
    except sqlite3.OperationalError:
        pass  # 既に存在

    conn.commit()


# ============================================================
# 保存
# ============================================================

def save_jobs(jobs: List[Dict], job_type: str = "web") -> int:
    """求人データを保存（URL重複は更新）。保存件数を返す。"""
    conn = _get_conn()
    now = datetime.now().isoformat()
    saved = 0

    for job in jobs:
        url = job.get("url", "").strip()
        if not url:
            continue

        jt = job.get("job_type", job_type)
        conn.execute("""
            INSERT INTO jobs (url, title, company, location, salary, description, source, pub_date, job_type, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                company=excluded.company,
                location=excluded.location,
                salary=excluded.salary,
                description=excluded.description,
                source=excluded.source,
                job_type=excluded.job_type,
                updated_at=excluded.updated_at
        """, (
            url,
            job.get("title", ""),
            job.get("company", ""),
            job.get("location", ""),
            job.get("salary", ""),
            job.get("description", ""),
            job.get("source", ""),
            job.get("pub_date", ""),
            jt,
            now, now,
        ))
        saved += 1

    conn.commit()
    return saved


# ============================================================
# 検索
# ============================================================

def search_jobs(query: str, filters: Optional[Dict] = None) -> List[Dict]:
    """
    LIKE検索（日本語対応）+ フィルタ

    FTS5は日本語トークン化に問題があるため、LIKE検索をメインで使用。
    SQLiteのLIKEは小規模データ（数千件）では十分高速。

    filters: {
        "location": str,
        "sources": List[str],
    }
    """
    conn = _get_conn()
    terms = [t.strip() for t in query.split() if t.strip()]

    if not terms:
        return get_all_jobs()

    # LIKE検索: 各ターム OR で検索（日本語の部分一致に対応）
    like_parts = []
    params = []
    for term in terms:
        like_parts.append(
            "(title LIKE ? OR company LIKE ? OR location LIKE ? OR salary LIKE ? OR description LIKE ?)"
        )
        p = f"%{term}%"
        params.extend([p, p, p, p, p])

    sql = f"SELECT * FROM jobs WHERE {' OR '.join(like_parts)} ORDER BY updated_at DESC LIMIT 500"
    rows = conn.execute(sql, params).fetchall()
    results = [dict(r) for r in rows]

    # フィルタ適用
    if filters:
        if filters.get("location"):
            loc = filters["location"]
            results = [r for r in results if loc in r.get("location", "")]
        if filters.get("sources"):
            results = [r for r in results if r.get("source", "") in filters["sources"]]

    return results


def get_all_jobs(limit: int = 1000, job_type: str = None) -> List[Dict]:
    """全件取得（新しい順）。job_typeでフィルタ可能。"""
    conn = _get_conn()
    if job_type:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE job_type = ? ORDER BY updated_at DESC LIMIT ?",
            (job_type, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY updated_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


# ============================================================
# 統計・管理
# ============================================================

def get_stats() -> Dict:
    conn = _get_conn()
    total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    sources = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM jobs GROUP BY source ORDER BY cnt DESC"
    ).fetchall()
    newest = conn.execute("SELECT MAX(updated_at) FROM jobs").fetchone()[0]
    oldest = conn.execute("SELECT MIN(created_at) FROM jobs").fetchone()[0]

    return {
        "total_jobs": total,
        "sources": {r["source"]: r["cnt"] for r in sources},
        "newest": newest,
        "oldest": oldest,
    }


def delete_job(url: str) -> bool:
    conn = _get_conn()
    conn.execute("DELETE FROM jobs WHERE url = ?", (url,))
    conn.commit()
    return True


def delete_old_jobs(days: int = 60) -> int:
    conn = _get_conn()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    result = conn.execute("DELETE FROM jobs WHERE updated_at < ?", (cutoff,))
    conn.commit()
    return result.rowcount


def clear_all() -> int:
    conn = _get_conn()
    count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    conn.execute("DELETE FROM jobs")
    conn.execute("INSERT INTO jobs_fts(jobs_fts) VALUES('rebuild')")
    conn.commit()
    return count


# ============================================================
# キーワード管理
# ============================================================

def get_keywords() -> List[Dict]:
    """登録済みキーワード一覧"""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM collection_keywords ORDER BY created_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def add_keyword(keyword: str, location: str = "") -> bool:
    """取得キーワードを追加"""
    conn = _get_conn()
    # 重複チェック
    existing = conn.execute(
        "SELECT id FROM collection_keywords WHERE keyword = ? AND location = ?",
        (keyword, location)
    ).fetchone()
    if existing:
        return False
    conn.execute(
        "INSERT INTO collection_keywords (keyword, location, enabled, created_at) VALUES (?, ?, 1, ?)",
        (keyword, location, datetime.now().isoformat())
    )
    conn.commit()
    return True


def remove_keyword(keyword_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM collection_keywords WHERE id = ?", (keyword_id,))
    conn.commit()


def toggle_keyword(keyword_id: int, enabled: bool):
    conn = _get_conn()
    conn.execute(
        "UPDATE collection_keywords SET enabled = ? WHERE id = ?",
        (1 if enabled else 0, keyword_id)
    )
    conn.commit()


def get_enabled_keywords() -> List[Dict]:
    """有効なキーワードのみ取得"""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM collection_keywords WHERE enabled = 1"
    ).fetchall()
    return [dict(r) for r in rows]


def add_collection_log(keywords_used: int, jobs_found: int, jobs_saved: int,
                       sources: str, duration_sec: float):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO collection_log (ran_at, keywords_used, jobs_found, jobs_saved, sources, duration_sec) VALUES (?, ?, ?, ?, ?, ?)",
        (datetime.now().isoformat(), keywords_used, jobs_found, jobs_saved, sources, duration_sec)
    )
    conn.commit()


# ============================================================
# 候補者管理
# ============================================================

def save_candidate(name: str, info: Dict, strengths: list, conditions: Dict,
                   tags: Dict = None, source_files: list = None) -> int:
    """候補者を保存し、IDを返す"""
    import json
    conn = _get_conn()
    cur = conn.execute(
        "INSERT INTO saved_candidates (name, info_json, strengths_json, conditions_json, tags_json, source_files_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, json.dumps(info, ensure_ascii=False),
         json.dumps(strengths, ensure_ascii=False),
         json.dumps(conditions, ensure_ascii=False),
         json.dumps(tags or {}, ensure_ascii=False),
         json.dumps(source_files or [], ensure_ascii=False),
         datetime.now().isoformat())
    )
    conn.commit()
    return cur.lastrowid


def get_saved_candidates() -> List[Dict]:
    """保存済み候補者一覧を取得"""
    import json
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM saved_candidates ORDER BY created_at DESC"
    ).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        d["info"] = json.loads(d.pop("info_json", "{}"))
        d["strengths"] = json.loads(d.pop("strengths_json", "[]"))
        d["conditions"] = json.loads(d.pop("conditions_json", "{}"))
        d["tags"] = json.loads(d.pop("tags_json", "{}")) if "tags_json" in d else {}
        d["source_files"] = json.loads(d.pop("source_files_json", "[]")) if "source_files_json" in d else []
        results.append(d)
    return results


def update_candidate(candidate_id: int, name: str = None, info: Dict = None,
                     strengths: list = None, conditions: Dict = None,
                     tags: Dict = None, source_files: list = None):
    """候補者情報を部分的に更新"""
    import json
    conn = _get_conn()
    updates = []
    params = []
    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if info is not None:
        updates.append("info_json = ?")
        params.append(json.dumps(info, ensure_ascii=False))
    if strengths is not None:
        updates.append("strengths_json = ?")
        params.append(json.dumps(strengths, ensure_ascii=False))
    if conditions is not None:
        updates.append("conditions_json = ?")
        params.append(json.dumps(conditions, ensure_ascii=False))
    if tags is not None:
        updates.append("tags_json = ?")
        params.append(json.dumps(tags, ensure_ascii=False))
    if source_files is not None:
        updates.append("source_files_json = ?")
        params.append(json.dumps(source_files, ensure_ascii=False))
    if not updates:
        return
    params.append(candidate_id)
    conn.execute(f"UPDATE saved_candidates SET {', '.join(updates)} WHERE id = ?", params)
    conn.commit()


def get_candidate_by_id(candidate_id: int) -> Optional[Dict]:
    """IDで候補者を取得"""
    import json
    conn = _get_conn()
    row = conn.execute("SELECT * FROM saved_candidates WHERE id = ?", (candidate_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    d["info"] = json.loads(d.pop("info_json", "{}"))
    d["strengths"] = json.loads(d.pop("strengths_json", "[]"))
    d["conditions"] = json.loads(d.pop("conditions_json", "{}"))
    d["tags"] = json.loads(d.pop("tags_json", "{}")) if "tags_json" in d else {}
    d["source_files"] = json.loads(d.pop("source_files_json", "[]")) if "source_files_json" in d else []
    return d


def delete_candidate(candidate_id: int):
    """候補者を削除"""
    conn = _get_conn()
    conn.execute("DELETE FROM candidate_files WHERE candidate_id = ?", (candidate_id,))
    conn.execute("DELETE FROM saved_candidates WHERE id = ?", (candidate_id,))
    conn.commit()


def save_candidate_file(candidate_id: int, filename: str, file_type: str = "",
                        doc_type: str = "", file_size: int = 0,
                        extracted_text_length: int = 0) -> int:
    """候補者のアップロードファイル情報を記録"""
    conn = _get_conn()
    cur = conn.execute(
        "INSERT INTO candidate_files (candidate_id, filename, file_type, doc_type, file_size, extracted_text_length, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (candidate_id, filename, file_type, doc_type, file_size, extracted_text_length,
         datetime.now().isoformat())
    )
    conn.commit()
    return cur.lastrowid


def get_candidate_files(candidate_id: int) -> List[Dict]:
    """候補者のアップロードファイル一覧を取得"""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM candidate_files WHERE candidate_id = ? ORDER BY created_at ASC",
        (candidate_id,)
    ).fetchall()
    return [dict(r) for r in rows]


# ============================================================
# 提案（進捗管理）
# ============================================================

PROPOSAL_STATUSES = ["提案済み", "カジュアル面談", "一次面接", "二次面接", "三次面接", "内定", "内定承諾", "決定"]


def save_proposal(candidate_id: int, job_url: str, status: str = "提案済み", memo: str = "") -> int:
    conn = _get_conn()
    now = datetime.now().isoformat()
    cur = conn.execute(
        """INSERT INTO proposals (candidate_id, job_url, status, memo, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(candidate_id, job_url) DO UPDATE SET status=excluded.status, memo=excluded.memo, updated_at=excluded.updated_at""",
        (candidate_id, job_url, status, memo, now, now)
    )
    conn.commit()
    return cur.lastrowid


def update_proposal_status(proposal_id: int, status: str, memo: str = None, next_action: str = None):
    conn = _get_conn()
    now = datetime.now().isoformat()
    if memo is not None and next_action is not None:
        conn.execute("UPDATE proposals SET status=?, memo=?, next_action=?, updated_at=? WHERE id=?",
                     (status, memo, next_action, now, proposal_id))
    elif memo is not None:
        conn.execute("UPDATE proposals SET status=?, memo=?, updated_at=? WHERE id=?",
                     (status, memo, now, proposal_id))
    else:
        conn.execute("UPDATE proposals SET status=?, updated_at=? WHERE id=?",
                     (status, now, proposal_id))
    conn.commit()


def get_proposals() -> List[Dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM proposals ORDER BY updated_at DESC").fetchall()
    return [dict(r) for r in rows]


def delete_proposal(proposal_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM proposals WHERE id=?", (proposal_id,))
    conn.commit()


# ============================================================
# 面談シート
# ============================================================

def save_interview_sheet(candidate_id: int, raw_input: str, sheet_content: str, tags: list) -> int:
    import json
    conn = _get_conn()
    now = datetime.now().isoformat()
    cur = conn.execute(
        "INSERT INTO interview_sheets (candidate_id, raw_input, sheet_content, tags_json, created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (candidate_id, raw_input, sheet_content, json.dumps(tags, ensure_ascii=False), now, now)
    )
    conn.commit()
    return cur.lastrowid


def update_interview_sheet(sheet_id: int, sheet_content: str, tags: list):
    import json
    conn = _get_conn()
    now = datetime.now().isoformat()
    conn.execute("UPDATE interview_sheets SET sheet_content=?, tags_json=?, updated_at=? WHERE id=?",
                 (sheet_content, json.dumps(tags, ensure_ascii=False), now, sheet_id))
    conn.commit()


def get_interview_sheets(candidate_id: int = None) -> List[Dict]:
    import json
    conn = _get_conn()
    if candidate_id:
        rows = conn.execute("SELECT * FROM interview_sheets WHERE candidate_id=? ORDER BY updated_at DESC",
                            (candidate_id,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM interview_sheets ORDER BY updated_at DESC").fetchall()
    results = []
    for r in rows:
        d = dict(r)
        d["tags"] = json.loads(d.pop("tags_json", "[]"))
        results.append(d)
    return results


def delete_interview_sheet(sheet_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM interview_sheets WHERE id=?", (sheet_id,))
    conn.commit()


# ============================================================
# アプリ設定
# ============================================================

def get_app_setting(key: str, default=None):
    import json
    conn = _get_conn()
    row = conn.execute("SELECT value_json FROM app_settings WHERE key=?", (key,)).fetchone()
    if row:
        return json.loads(row[0])
    return default


def set_app_setting(key: str, value):
    import json
    conn = _get_conn()
    conn.execute(
        "INSERT INTO app_settings (key, value_json) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
        (key, json.dumps(value, ensure_ascii=False))
    )
    conn.commit()


def get_collection_logs(limit: int = 10) -> List[Dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM collection_log ORDER BY ran_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


# ============================================================
# チャット履歴
# ============================================================

def add_chat_message(tab: str, role: str, content: str, context: Dict = None) -> int:
    import json
    conn = _get_conn()
    now = datetime.now().isoformat()
    cur = conn.execute(
        "INSERT INTO chat_history (tab, role, content, context_json, created_at) VALUES (?, ?, ?, ?, ?)",
        (tab, role, content, json.dumps(context or {}, ensure_ascii=False), now)
    )
    conn.commit()
    return cur.lastrowid


def get_chat_history(tab: str, limit: int = 50) -> List[Dict]:
    import json
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM chat_history WHERE tab = ? ORDER BY created_at ASC LIMIT ?",
        (tab, limit)
    ).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        d["context"] = json.loads(d.pop("context_json", "{}"))
        results.append(d)
    return results


def clear_chat_history(tab: str = None):
    conn = _get_conn()
    if tab:
        conn.execute("DELETE FROM chat_history WHERE tab = ?", (tab,))
    else:
        conn.execute("DELETE FROM chat_history")
    conn.commit()


def update_job_type(url: str, job_type: str):
    """求人の種別（contracted/web）を更新"""
    conn = _get_conn()
    conn.execute("UPDATE jobs SET job_type = ?, updated_at = ? WHERE url = ?",
                 (job_type, datetime.now().isoformat(), url))
    conn.commit()


def get_job_type_stats() -> Dict:
    """求人種別ごとの件数を取得"""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT COALESCE(job_type, 'web') as jt, COUNT(*) as cnt FROM jobs GROUP BY jt"
    ).fetchall()
    return {r["jt"]: r["cnt"] for r in rows}


# ============================================================
# アクセスログ（セキュリティ監査）
# ============================================================

def add_access_log(event_type: str, detail: str = ""):
    """アクセスログを記録"""
    conn = _get_conn()
    now = datetime.now().isoformat()
    conn.execute(
        "INSERT INTO access_log (event_type, detail, created_at) VALUES (?, ?, ?)",
        (event_type, detail[:500], now)
    )
    conn.commit()


def get_access_logs(limit: int = 50, event_type: str = None) -> List[Dict]:
    """アクセスログを取得"""
    conn = _get_conn()
    if event_type:
        rows = conn.execute(
            "SELECT * FROM access_log WHERE event_type = ? ORDER BY created_at DESC LIMIT ?",
            (event_type, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM access_log ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def clear_old_access_logs(days: int = 90):
    """古いアクセスログを削除"""
    conn = _get_conn()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    conn.execute("DELETE FROM access_log WHERE created_at < ?", (cutoff,))
    conn.commit()


# ============================================================
# DBファイルのパーミッション設定
# ============================================================

def _secure_db_permissions():
    """DBファイルとディレクトリのパーミッションを制限（所有者のみ読み書き）"""
    try:
        if os.path.exists(CACHE_DIR):
            os.chmod(CACHE_DIR, 0o700)
        if os.path.exists(CACHE_DB):
            os.chmod(CACHE_DB, 0o600)
    except OSError:
        pass


# 起動時にパーミッション設定
_secure_db_permissions()
