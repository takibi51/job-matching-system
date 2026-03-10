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
    """)
    conn.commit()


# ============================================================
# 保存
# ============================================================

def save_jobs(jobs: List[Dict]) -> int:
    """求人データを保存（URL重複は更新）。保存件数を返す。"""
    conn = _get_conn()
    now = datetime.now().isoformat()
    saved = 0

    for job in jobs:
        url = job.get("url", "").strip()
        if not url:
            continue

        conn.execute("""
            INSERT INTO jobs (url, title, company, location, salary, description, source, pub_date, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                company=excluded.company,
                location=excluded.location,
                salary=excluded.salary,
                description=excluded.description,
                source=excluded.source,
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


def get_all_jobs(limit: int = 1000) -> List[Dict]:
    """全件取得（新しい順）"""
    conn = _get_conn()
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

def save_candidate(name: str, info: Dict, strengths: list, conditions: Dict) -> int:
    """候補者を保存し、IDを返す"""
    import json
    conn = _get_conn()
    cur = conn.execute(
        "INSERT INTO saved_candidates (name, info_json, strengths_json, conditions_json, created_at) VALUES (?, ?, ?, ?, ?)",
        (name, json.dumps(info, ensure_ascii=False),
         json.dumps(strengths, ensure_ascii=False),
         json.dumps(conditions, ensure_ascii=False),
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
        results.append(d)
    return results


def delete_candidate(candidate_id: int):
    """候補者を削除"""
    conn = _get_conn()
    conn.execute("DELETE FROM saved_candidates WHERE id = ?", (candidate_id,))
    conn.commit()


def get_collection_logs(limit: int = 10) -> List[Dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM collection_log ORDER BY ran_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]
