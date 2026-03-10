"""
求人マッチングシステム v4 — Match
プロトタイプ(match-prototype)のUI/UXと
Web求人自動取得バックエンドを統合
"""

import streamlit as st
import pandas as pd
import io
import sys
import os
import html
import re
import threading
import time as _time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_collector import (
    fetch_from_all_sources, parse_csv_upload, parse_text_input,
    generate_search_urls, SOURCE_NAMES,
)
from scorer import rank_jobs, generate_search_queries, score_job
from candidate_loader import (
    load_all_candidates, load_candidate_upload,
    SUPPORTED_EXTENSIONS,
)
from cache_manager import (
    save_jobs, search_jobs, get_all_jobs, get_stats, delete_old_jobs, clear_all,
    get_keywords, add_keyword, remove_keyword, get_enabled_keywords,
    add_collection_log, get_collection_logs,
    save_candidate, get_saved_candidates, delete_candidate,
    save_proposal, update_proposal_status, get_proposals, delete_proposal, PROPOSAL_STATUSES,
    save_interview_sheet, update_interview_sheet, get_interview_sheets, delete_interview_sheet,
    get_app_setting, set_app_setting,
)
from ai_generator import (
    generate_scout_message, generate_concerns, generate_hireability,
    generate_proposal_resume, generate_interview_analysis,
    generate_progress_analysis, generate_job_improvements,
    evaluate_market_fit, MARKET_FIT_AXES,
)

# ============================================================
# ページ設定
# ============================================================
st.set_page_config(
    page_title="Match - 人材マッチングシステム",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# バックグラウンド取得（スレッドセーフ）
# ============================================================
_BG_LOCK = threading.Lock()
_bg_state = {"status": "idle", "result": "", "started": ""}


def _bg_fetch_worker(kw_list, location, sources):
    """別スレッドで求人取得を実行"""
    try:
        with _BG_LOCK:
            _bg_state["status"] = "running"
            _bg_state["started"] = datetime.now().isoformat()
        start = _time.time()
        jobs = fetch_from_all_sources(kw_list, location, enabled_sources=sources)
        elapsed = _time.time() - start
        with _BG_LOCK:
            if jobs:
                saved = save_jobs(jobs)
                add_collection_log(len(kw_list), len(jobs), saved, ",".join(sources), elapsed)
                _bg_state["result"] = f"✅ {len(jobs)}件取得 → {saved}件保存（{elapsed:.1f}秒）"
            else:
                add_collection_log(len(kw_list), 0, 0, ",".join(sources), elapsed)
                _bg_state["result"] = "⚠️ 取得0件でした"
            _bg_state["status"] = "done"
    except Exception as e:
        with _BG_LOCK:
            _bg_state["status"] = "error"
            _bg_state["result"] = f"❌ エラー: {e}"


def _get_bg_status():
    """スレッドセーフにバックグラウンド状態を取得"""
    with _BG_LOCK:
        return dict(_bg_state)


def start_bg_fetch(kw_list, location, sources):
    """バックグラウンド取得を開始"""
    with _BG_LOCK:
        if _bg_state["status"] == "running":
            return False
        _bg_state["status"] = "running"
        _bg_state["started"] = datetime.now().isoformat()
    t = threading.Thread(target=_bg_fetch_worker, args=(kw_list, location, sources), daemon=True)
    t.start()
    return True


# ============================================================
# ポップアップダイアログ
# ============================================================

@st.dialog("👤 候補者詳細", width="large")
def show_candidate_popup(cand):
    """候補者詳細のモーダルポップアップ"""
    info = cand.get("info", {})
    strengths = cand.get("strengths", [])
    conditions = cand.get("conditions", {})
    mf = evaluate_market_fit(cand)

    # ヘッダー
    h1, h2 = st.columns([3, 1])
    with h1:
        star = "⭐ " if mf["has_star"] else ""
        st.markdown(f"### {star}{cand.get('name', '候補者')}")
        if mf["reason"]:
            st.caption(mf["reason"])
    with h2:
        st.caption(f"登録: {cand.get('created_at', '')[:10]}")

    # 基本情報 & 強み
    p1, p2 = st.columns(2)
    with p1:
        st.markdown("**基本情報**")
        for k, v in list(info.items())[:8]:
            st.markdown(f"- **{k}**: {v}")
    with p2:
        st.markdown("**強み・スキル**")
        for s in strengths[:6]:
            if isinstance(s, (list, tuple)) and len(s) >= 2:
                st.markdown(f"- **{s[0]}**: {s[1][:60]}")
            elif isinstance(s, str):
                st.markdown(f"- {s}")

    # 希望条件
    st.markdown("**希望条件**")
    kw_tags = " ".join(f'`{k}`' for k in conditions.get("keywords", []))
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"🔑 {kw_tags or 'なし'}")
    c2.markdown(f"💰 {conditions.get('salary_min', 0)}〜{conditions.get('salary_max', 0)}万円")
    c3.markdown(f"📍 {conditions.get('location', '未指定')}")

    # Market Fit 5軸
    st.markdown("**Market Fit 5軸**")
    ax_cols = st.columns(5)
    for i, axis in enumerate(MARKET_FIT_AXES):
        val = mf["axes"].get(axis["id"], "neutral")
        icon = "🟢" if val == "positive" else ("🟡" if val == "neutral" else "🔴")
        ax_cols[i].markdown(f"{icon} {axis['label'].split('（')[0]}")

    # 面談シート
    sheets = get_interview_sheets(cand.get("id")) if cand.get("id") else []
    if sheets:
        st.markdown(f"**📝 面談シート: {len(sheets)}件**")

    st.markdown("---")

    # AI アクションボタン
    st.markdown("**🤖 AIアシスタント**")
    a1, a2, a3, a4 = st.columns(4)
    cid = cand.get("id", 0)
    if a1.button("📝 スカウト文", key=f"pop_scout_{cid}"):
        st.session_state[f"pop_ai_{cid}"] = generate_scout_message(cand)
    if a2.button("⚠️ 懸念点", key=f"pop_conc_{cid}"):
        st.session_state[f"pop_ai_{cid}"] = generate_concerns(cand)
    if a3.button("📈 決まりやすさ", key=f"pop_hire_{cid}"):
        st.session_state[f"pop_ai_{cid}"] = generate_hireability(cand)
    if a4.button("📋 推薦文", key=f"pop_resume_{cid}"):
        st.session_state[f"pop_ai_{cid}"] = generate_proposal_resume(cand)

    if st.session_state.get(f"pop_ai_{cid}"):
        st.markdown(st.session_state[f"pop_ai_{cid}"])


@st.dialog("📋 求人詳細", width="large")
def show_job_popup(job, candidates=None):
    """求人詳細のモーダルポップアップ"""
    st.markdown(f"### {job.get('title', '不明')}")

    j1, j2 = st.columns(2)
    with j1:
        st.markdown(f"🏢 **{job.get('company', '不明')}**")
        st.markdown(f"📍 {job.get('location', '不明')}")
        st.markdown(f"💰 {job.get('salary', '情報なし')}")
        st.markdown(f"🔗 ソース: {job.get('source', '')}")
    with j2:
        url = job.get("url", "")
        if url and url.startswith("http"):
            st.link_button("求人ページを開く", url)
        st.caption(f"更新: {job.get('updated_at', '')[:16]}")

    desc = job.get("description", "")
    if desc:
        with st.expander("説明文", expanded=True):
            st.markdown(desc[:500] + ("..." if len(desc) > 500 else ""))

    # マッチする候補者（渡された場合）
    if candidates:
        st.markdown("---")
        st.markdown("**マッチする候補者:**")
        cand_scores = []
        for c in candidates:
            cond = _cand_to_conditions(c)
            sc, reasons = score_job(job, cond)
            if sc >= 10:
                cand_scores.append((c, sc, reasons))
        cand_scores.sort(key=lambda x: x[1], reverse=True)

        for c, sc, reasons in cand_scores[:5]:
            reason_str = " / ".join(reasons[:3])
            mf = evaluate_market_fit(c)
            star = "⭐ " if mf["has_star"] else ""
            st.markdown(f"{_score_badge(sc)} &nbsp; **{star}{esc(c.get('name', ''))}** — {_fit_tags(reason_str)}",
                        unsafe_allow_html=True)

    st.markdown("---")

    # AI アクションボタン
    st.markdown("**🤖 AIアシスタント**")
    jb1, jb2 = st.columns(2)
    jurl = job.get("url", "none")
    if jb1.button("💡 求人改善提案", key=f"pop_jimp_{jurl[:30]}"):
        st.session_state["pop_job_ai"] = generate_job_improvements(job)
    if st.session_state.get("pop_job_ai"):
        st.markdown(st.session_state["pop_job_ai"])


# ============================================================
# CSS
# ============================================================
st.markdown("""
<style>
    /* ナビゲーション */
    .nav-item {
        padding: 0.6rem 1rem; margin: 0.15rem 0; border-radius: 8px;
        cursor: pointer; font-weight: 500; transition: background 0.15s;
    }
    .nav-item:hover { background: #e8ecf1; }
    .nav-active { background: #dce8f5 !important; color: #1a4d7c; font-weight: 700; }

    /* 統計カード */
    .stat-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 12px; padding: 1.2rem; color: white; text-align: center;
    }
    .stat-card-blue {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        border-radius: 12px; padding: 1.2rem; color: white; text-align: center;
    }
    .stat-card-green {
        background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
        border-radius: 12px; padding: 1.2rem; color: #1a3a2a; text-align: center;
    }
    .stat-num { font-size: 2rem; font-weight: 800; }
    .stat-label { font-size: 0.85rem; opacity: 0.9; }

    /* 求人カード */
    .job-card {
        border: 1px solid #e2e8f0; border-radius: 12px; padding: 1.2rem;
        margin-bottom: 0.8rem; background: white;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06); transition: all 0.2s;
    }
    .job-card:hover { border-color: #667eea; box-shadow: 0 4px 12px rgba(102,126,234,0.15); }

    /* 候補者カード */
    .cand-card {
        border: 1px solid #e2e8f0; border-radius: 12px; padding: 1.2rem;
        margin-bottom: 0.8rem; background: white;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06); transition: all 0.2s;
    }
    .cand-card:hover { border-color: #43e97b; box-shadow: 0 4px 12px rgba(67,233,123,0.15); }

    /* スコアバッジ */
    .score-badge {
        display: inline-block; padding: 0.25rem 0.75rem; border-radius: 20px;
        font-weight: 700; font-size: 0.9rem;
    }
    .score-high { background: #d4edda; color: #155724; }
    .score-mid { background: #fff3cd; color: #856404; }
    .score-low { background: #f8d7da; color: #721c24; }

    /* マッチ度プログレス */
    .match-bar { background: #e9ecef; border-radius: 10px; height: 8px; overflow: hidden; margin: 0.3rem 0; }
    .match-fill { height: 100%; border-radius: 10px; transition: width 0.5s; }
    .match-fill-high { background: linear-gradient(90deg, #43e97b, #38f9d7); }
    .match-fill-mid { background: linear-gradient(90deg, #f9d423, #ff4e50); }
    .match-fill-low { background: linear-gradient(90deg, #ff4e50, #c62828); }

    /* フィット理由 */
    .fit-tag {
        display: inline-block; padding: 0.2rem 0.6rem; margin: 0.15rem;
        border-radius: 12px; font-size: 0.78rem; background: #eef2ff; color: #3730a3;
    }

    /* 検索リンク */
    .search-link-btn {
        display: inline-block; padding: 0.5rem 1rem; margin: 0.25rem;
        border-radius: 8px; background: #f0f4ff; border: 1px solid #667eea;
        text-decoration: none; color: #3730a3; font-weight: 600; font-size: 0.85rem;
    }
    .search-link-btn:hover { background: #dce8f5; }

    /* ポップアップ風 */
    .popup-header { font-size: 1.3rem; font-weight: 700; color: #1a202c; margin-bottom: 0.5rem; }
    .popup-section { background: #f7fafc; border-radius: 8px; padding: 0.8rem; margin: 0.5rem 0; }
    .popup-label { font-size: 0.78rem; color: #718096; font-weight: 600; text-transform: uppercase; }

    /* 空状態 */
    .empty-state {
        text-align: center; padding: 3rem; color: #a0aec0;
        border: 2px dashed #e2e8f0; border-radius: 16px; margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def esc(text):
    """XSS対策用HTMLエスケープ"""
    return html.escape(str(text)) if text else ""


def _get_label_thresholds():
    """設定からラベル閾値を取得"""
    defaults = {"recommended": 85, "good": 70}
    saved = get_app_setting("label_thresholds", defaults)
    return saved


def _score_badge(score):
    """スコアに応じたバッジHTML（4段階ラベル）"""
    th = _get_label_thresholds()
    rec = th.get("recommended", 85)
    good = th.get("good", 70)
    if score >= rec:
        cls, label = "score-high", "かなり相性が良い"
    elif score >= good:
        cls, label = "score-high", "相性が良い"
    elif score >= 55:
        cls, label = "score-mid", "可能性あり"
    else:
        cls, label = "score-low", "要検討"
    return f'<span class="score-badge {cls}">{score}点 {label}</span>'


def _match_bar(score):
    """マッチ度バーHTML"""
    cls = "match-fill-high" if score >= 60 else ("match-fill-mid" if score >= 30 else "match-fill-low")
    return f'<div class="match-bar"><div class="match-fill {cls}" style="width:{min(score,100)}%"></div></div>'


def _fit_tags(reasons_str):
    """フィット理由をタグ表示"""
    if not reasons_str:
        return ""
    tags = reasons_str.split(" / ")
    return " ".join(f'<span class="fit-tag">{esc(t)}</span>' for t in tags[:5])


# ============================================================
# サイドバーナビゲーション
# ============================================================
st.sidebar.markdown("## 🎯 Match")
st.sidebar.caption("人材マッチングシステム")
st.sidebar.markdown("---")

# ページ切り替え
pages = {
    "candidate_search": "🔍 候補者検索",
    "job_search": "📋 求人票から検索",
    "compare": "⚖️ 候補者比較",
    "interview": "📝 面談シート作成",
    "progress": "📊 提案済み（進捗）",
    "candidates": "👤 候補者管理",
    "data_mgmt": "📦 データ管理",
    "settings": "⚙️ 設定",
    "search_links": "🌐 検索リンク",
}

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "candidate_search"

for key, label in pages.items():
    if st.sidebar.button(label, key=f"nav_{key}", use_container_width=True):
        st.session_state["current_page"] = key
        st.rerun()

page = st.session_state["current_page"]

# --- 統計 ---
st.sidebar.markdown("---")
stats = get_stats()
saved_cands = get_saved_candidates()
st.sidebar.caption(f"📊 求人: {stats['total_jobs']:,}件 | 候補者: {len(saved_cands)}名")

# --- バックグラウンド取得ステータス ---
_bg_current = _get_bg_status()
if _bg_current["status"] == "running":
    st.sidebar.markdown("---")
    st.sidebar.markdown("🔄 **データ取得中...**")
    st.sidebar.caption("他のタブを自由にお使いください")
    if _bg_current["started"]:
        st.sidebar.caption(f"開始: {_bg_current['started'][:16]}")
elif _bg_current["status"] == "done":
    st.sidebar.markdown("---")
    st.sidebar.success(_bg_current["result"])
    if st.sidebar.button("確認", key="bg_dismiss"):
        with _BG_LOCK:
            _bg_state["status"] = "idle"
            _bg_state["result"] = ""
        st.rerun()
elif _bg_current["status"] == "error":
    st.sidebar.markdown("---")
    st.sidebar.error(_bg_current["result"])
    if st.sidebar.button("確認", key="bg_dismiss_err"):
        with _BG_LOCK:
            _bg_state["status"] = "idle"
        st.rerun()


# ============================================================
# ヘルパー: 候補者条件を整形
# ============================================================
def _cand_to_conditions(cand):
    """保存済み候補者データからconditions dictを返す"""
    c = cand.get("conditions", {})
    return {
        "keywords": c.get("keywords", []),
        "location": c.get("location", "大阪"),
        "salary_min": c.get("salary_min", 0),
        "salary_max": c.get("salary_max", 0),
        "age": c.get("age", 0),
        "prefer_kansai": c.get("prefer_kansai", True),
        "extra_keywords": c.get("extra_keywords", []),
    }


# ============================================================
# ページ1: 候補者検索（候補者 → マッチする求人）
# ============================================================
if page == "candidate_search":
    st.markdown("## 🔍 候補者検索")
    st.caption("候補者の条件にマッチする求人を検索します")

    # --- 候補者選択 ---
    col_sel, col_upload = st.columns([2, 1])

    with col_sel:
        if saved_cands:
            cand_options = ["-- 選択してください --"] + [
                f"{c['name']}（{c['created_at'][:10]}）" for c in saved_cands
            ]
            sel = st.selectbox("保存済み候補者から選択", cand_options, key="cs_select")
            sel_idx = cand_options.index(sel) - 1 if sel != cand_options[0] else -1
        else:
            st.info("候補者が登録されていません。「👤 候補者管理」から追加してください。")
            sel_idx = -1

    with col_upload:
        st.markdown("**クイックアップロード**")
        ext_list = list(SUPPORTED_EXTENSIONS.keys())
        quick_upload = st.file_uploader(
            "候補者ファイル", type=[e.lstrip(".") for e in ext_list],
            key="cs_quick_upload", label_visibility="collapsed",
        )
        if quick_upload:
            file_bytes = quick_upload.read()
            cand_data = load_candidate_upload(file_bytes, quick_upload.name)
            if cand_data:
                st.session_state["quick_cand"] = cand_data
                st.success("読み取り完了")

    # 選択された候補者の条件を取得
    active_cand = None
    conditions = None

    if quick_upload and "quick_cand" in st.session_state:
        active_cand = st.session_state["quick_cand"]
        conditions = active_cand.get("conditions", {})
        # 保存ボタン
        save_name = st.text_input("候補者名", value=quick_upload.name.rsplit(".", 1)[0], key="cs_save_name")
        if st.button("この候補者を保存", key="cs_save"):
            save_candidate(save_name, active_cand.get("info", {}),
                           active_cand.get("strengths", []), conditions)
            st.success(f"「{save_name}」を保存しました")
            st.rerun()
    elif sel_idx >= 0:
        active_cand = saved_cands[sel_idx]
        conditions = _cand_to_conditions(active_cand)

    if active_cand and conditions:
        # --- 候補者情報（ポップアップボタン + サマリー表示） ---
        mf_active = evaluate_market_fit(active_cand)
        star_txt = "⭐ " if mf_active["has_star"] else ""
        kw_tags = " ".join(f'`{k}`' for k in conditions.get("keywords", []))
        st.markdown(f"**{star_txt}{active_cand.get('name', '候補者')}** — {kw_tags}")
        if st.button("👤 候補者詳細を開く", key="cs_cand_popup"):
            show_candidate_popup(active_cand)

        # --- 条件の微調整 ---
        with st.expander("⚙️ 検索条件を調整", expanded=False):
            ac1, ac2, ac3, ac4 = st.columns(4)
            salary_min = ac1.number_input("最低年収(万)", value=conditions.get("salary_min", 300), step=10, key="cs_smin")
            salary_max = ac2.number_input("最高年収(万)", value=conditions.get("salary_max", 600), step=10, key="cs_smax")
            age_val = ac3.number_input("年齢", value=max(conditions.get("age", 30), 18), min_value=18, max_value=70, key="cs_age")
            loc_val = ac4.text_input("勤務地", value=conditions.get("location", "大阪"), key="cs_loc")
            prefer_kansai = st.checkbox("関西優先", value=conditions.get("prefer_kansai", True), key="cs_kansai")

            kw_str = st.text_area("キーワード（改行区切り）",
                                  value="\n".join(conditions.get("keywords", [])), height=60, key="cs_kw")
            kws = [k.strip() for k in kw_str.split("\n") if k.strip()]

            conditions = {
                "keywords": kws,
                "location": loc_val,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "age": age_val,
                "prefer_kansai": prefer_kansai,
                "extra_keywords": conditions.get("extra_keywords", []),
            }

        # --- マッチング実行 ---
        st.markdown("---")

        if stats["total_jobs"] == 0:
            st.markdown('<div class="empty-state"><h3>求人データがまだ登録されていません</h3>'
                        '<p>「📦 データ管理」からデータを追加してください</p></div>',
                        unsafe_allow_html=True)
        else:
            search_query = " ".join(conditions.get("keywords", []) + conditions.get("extra_keywords", [])[:3])
            matched_jobs = search_jobs(search_query) if search_query.strip() else get_all_jobs(limit=300)

            if matched_jobs:
                ranked = rank_jobs(matched_jobs, conditions)

                # フィルタ
                fc1, fc2, fc3, fc4, fc5 = st.columns(5)
                min_score = fc1.slider("最低スコア", 0, 100, 0, key="cs_fscore")
                sources_list = sorted(set(j.get("source", "") for j in ranked if j.get("source")))
                source_filter = fc2.multiselect("ソース", sources_list, default=sources_list, key="cs_fsrc")
                sort_opt = fc3.selectbox("並べ替え", ["マッチ度順", "年収順（高→低）", "新着順"], key="cs_sort")
                require_salary = fc4.checkbox("年収あり", value=False, key="cs_fsal")
                require_company = fc5.checkbox("社名あり", value=False, key="cs_fco")

                filtered = [
                    j for j in ranked
                    if j.get("score", 0) >= min_score
                    and (not source_filter or j.get("source", "") in source_filter)
                    and (not require_salary or j.get("salary", "").strip())
                    and (not require_company or j.get("company", "").strip())
                ]

                if sort_opt == "年収順（高→低）":
                    def _sk(j):
                        nums = re.findall(r'(\d+)', j.get("salary", "").replace(",", ""))
                        return max([int(n) for n in nums]) if nums else 0
                    filtered.sort(key=_sk, reverse=True)
                elif sort_opt == "新着順":
                    filtered.sort(key=lambda x: x.get("updated_at", ""), reverse=True)

                # 件数サマリー
                sal_c = sum(1 for j in filtered if j.get("salary", "").strip())
                co_c = sum(1 for j in filtered if j.get("company", "").strip())
                st.markdown(f"**{len(filtered)}件**マッチ（全{stats['total_jobs']:,}件中）"
                            f" &nbsp;|&nbsp; 💰 年収記載: {sal_c}件 &nbsp;|&nbsp; 🏢 社名記載: {co_c}件")

                # 求人カード表示
                for i, job in enumerate(filtered[:50]):
                    score = job.get("score", 0)
                    st.markdown(f"""
                    <div class="job-card">
                        {_score_badge(score)}
                        &nbsp; <span style="color:#a0aec0; font-size:0.85rem;">#{i+1} / {esc(job.get('source',''))}</span>
                        {_match_bar(score)}
                        <div style="font-size:1.1rem; font-weight:700; color:#1a202c; margin:0.3rem 0;">
                            {esc(job.get('title','不明'))}
                        </div>
                        🏢 <strong>{esc(job.get('company','不明'))}</strong>
                        &nbsp;|&nbsp; 📍 {esc(job.get('location','不明'))}
                        &nbsp;|&nbsp; 💰 {esc(job.get('salary','情報なし'))}
                        <div style="margin-top:0.4rem;">{_fit_tags(job.get('match_reasons',''))}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    # クイックアクション
                    qa1, qa2, qa3 = st.columns([1, 1, 1])
                    if qa1.button("📄 求人詳細", key=f"cs_jpop_{i}"):
                        show_job_popup(job, saved_cands)
                    if active_cand and active_cand.get("id"):
                        if qa2.button("📊 提案登録", key=f"cs_prop_{i}"):
                            save_proposal(active_cand["id"], job.get("url", ""), "提案済み", "")
                            st.success("提案を登録しました")
                            st.rerun()
                    url = job.get("url", "")
                    if url and url.startswith("http"):
                        qa3.link_button("🌐 求人ページ", url, key=f"cs_jext_{i}")

                # テーブル＆ダウンロード
                with st.expander(f"📊 全件テーブル表示（{len(filtered)}件）"):
                    df = pd.DataFrame([{
                        "順位": i, "スコア": j.get("score", 0),
                        "求人タイトル": j.get("title", ""), "企業名": j.get("company", ""),
                        "勤務地": j.get("location", ""), "年収": j.get("salary", ""),
                        "フィット理由": j.get("fit_reason", ""), "ソース": j.get("source", ""),
                        "URL": j.get("url", ""),
                    } for i, j in enumerate(filtered, 1)])
                    st.dataframe(df, column_config={
                        "URL": st.column_config.LinkColumn("URL", display_text="開く"),
                        "スコア": st.column_config.ProgressColumn("スコア", min_value=0, max_value=100),
                    }, hide_index=True, use_container_width=True)

                    c1, c2 = st.columns(2)
                    csv_buf = io.StringIO()
                    df.to_csv(csv_buf, index=False, encoding="utf-8-sig")
                    c1.download_button("CSV DL", csv_buf.getvalue(), "マッチング結果.csv", "text/csv")
                    xls_buf = io.BytesIO()
                    with pd.ExcelWriter(xls_buf, engine="openpyxl") as w:
                        df.to_excel(w, index=False, sheet_name="結果")
                    c2.download_button("Excel DL", xls_buf.getvalue(), "マッチング結果.xlsx",
                                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info("条件に一致する求人がありません。キーワードを調整するか、データを追加してください。")
    else:
        # 候補者未選択時: 手動検索モード
        st.markdown("---")
        st.markdown("#### 手動検索")
        manual_kw = st.text_input("検索キーワード", value="", placeholder="例: Webデザイナー 大阪", key="cs_manual_kw")

        if manual_kw.strip() and stats["total_jobs"] > 0:
            manual_conditions = {
                "keywords": manual_kw.split(), "location": "大阪",
                "salary_min": 300, "salary_max": 800, "age": 30,
                "prefer_kansai": True, "extra_keywords": [],
            }
            results = search_jobs(manual_kw)
            if results:
                ranked = rank_jobs(results, manual_conditions)
                st.markdown(f"**{len(ranked)}件** ヒット")
                for i, job in enumerate(ranked[:20]):
                    score = job.get("score", 0)
                    st.markdown(f"""
                    <div class="job-card">
                        {_score_badge(score)} &nbsp;
                        <strong>{esc(job.get('title',''))}</strong> - {esc(job.get('company',''))}
                        &nbsp;|&nbsp; 📍 {esc(job.get('location',''))} &nbsp;|&nbsp; 💰 {esc(job.get('salary',''))}
                    </div>""", unsafe_allow_html=True)


# ============================================================
# ページ2: 求人票から検索（求人 → マッチする候補者）
# ============================================================
elif page == "job_search":
    st.markdown("## 📋 求人票から検索")
    st.caption("求人にマッチする候補者を探します")

    if stats["total_jobs"] == 0:
        st.markdown('<div class="empty-state"><h3>求人データがまだ登録されていません</h3></div>',
                    unsafe_allow_html=True)
    elif not saved_cands:
        st.markdown('<div class="empty-state"><h3>候補者が登録されていません</h3>'
                    '<p>「👤 候補者管理」から候補者を追加してください</p></div>',
                    unsafe_allow_html=True)
    else:
        # 求人検索
        job_search_kw = st.text_input("求人を検索", placeholder="職種・企業名・キーワードで検索", key="js_kw")

        if job_search_kw.strip():
            job_results = search_jobs(job_search_kw)
        else:
            job_results = get_all_jobs(limit=100)

        # フィルタ
        jf1, jf2, jf3 = st.columns(3)
        js_require_salary = jf1.checkbox("年収あり", key="js_fsal")
        js_require_company = jf2.checkbox("社名あり", key="js_fco")
        js_sort = jf3.selectbox("並び替え", ["新着順", "企業名順", "年収順（高→低）"], key="js_sort")

        if js_require_salary:
            job_results = [j for j in job_results if j.get("salary", "").strip()]
        if js_require_company:
            job_results = [j for j in job_results if j.get("company", "").strip()]

        if js_sort == "企業名順":
            job_results.sort(key=lambda x: x.get("company", ""))
        elif js_sort == "年収順（高→低）":
            def _sk2(j):
                nums = re.findall(r'(\d+)', j.get("salary", "").replace(",", ""))
                return max([int(n) for n in nums]) if nums else 0
            job_results.sort(key=_sk2, reverse=True)
        else:
            job_results.sort(key=lambda x: x.get("updated_at", ""), reverse=True)

        st.markdown(f"**{len(job_results)}件**の求人")

        for i, job in enumerate(job_results[:30]):
            st.markdown(f"""
            <div class="job-card">
                <div style="font-size:1.05rem; font-weight:700; color:#1a202c;">
                    {esc(job.get('title','不明'))}
                </div>
                🏢 {esc(job.get('company','不明'))}
                &nbsp;|&nbsp; 📍 {esc(job.get('location','不明'))}
                &nbsp;|&nbsp; 💰 {esc(job.get('salary','情報なし'))}
                &nbsp;|&nbsp; <span style="color:#a0aec0">{esc(job.get('source',''))}</span>
            </div>
            """, unsafe_allow_html=True)

            # クイックアクション
            jqa1, jqa2 = st.columns(2)
            if jqa1.button("📄 詳細 & マッチ候補者", key=f"js_jpop_{i}"):
                show_job_popup(job, saved_cands)
            url = job.get("url", "")
            if url and url.startswith("http"):
                jqa2.link_button("🌐 求人ページ", url, key=f"js_link_{i}")

            # トップ3候補者をインライン表示
            cand_scores = []
            for cand in saved_cands:
                cond = _cand_to_conditions(cand)
                sc, reasons = score_job(job, cond)
                if sc >= 10:
                    cand_scores.append((cand, sc, reasons))
            cand_scores.sort(key=lambda x: x[1], reverse=True)

            if cand_scores:
                top3 = cand_scores[:3]
                tcols = st.columns(len(top3))
                for ti, (cand, sc, reasons) in enumerate(top3):
                    with tcols[ti]:
                        mf_c = evaluate_market_fit(cand)
                        star = "⭐" if mf_c["has_star"] else ""
                        st.markdown(f"{_score_badge(sc)} {star} **{esc(cand.get('name',''))}**",
                                    unsafe_allow_html=True)
                        if st.button("👤 詳細", key=f"js_cpop_{i}_{cand.get('id',ti)}"):
                            show_candidate_popup(cand)


# ============================================================
# ページ3: 候補者管理
# ============================================================
elif page == "candidates":
    st.markdown("## 👤 候補者管理")
    st.caption("候補者の登録・管理を行います")

    # 統計
    sc1, sc2, sc3 = st.columns(3)
    sc1.markdown(f'<div class="stat-card"><div class="stat-num">{len(saved_cands)}</div>'
                 f'<div class="stat-label">登録候補者数</div></div>', unsafe_allow_html=True)
    csv_cands = load_all_candidates()
    sc2.markdown(f'<div class="stat-card-blue"><div class="stat-num">{len(csv_cands)}</div>'
                 f'<div class="stat-label">CSV候補者数</div></div>', unsafe_allow_html=True)
    sc3.markdown(f'<div class="stat-card-green"><div class="stat-num">{stats["total_jobs"]:,}</div>'
                 f'<div class="stat-label">登録求人数</div></div>', unsafe_allow_html=True)

    st.markdown("")

    # --- アップロード ---
    st.markdown("### 候補者を追加")
    ext_list = list(SUPPORTED_EXTENSIONS.keys())
    st.caption(f"対応形式: {', '.join(ext_list)} | 個人情報は自動的に除外されます")

    up_file = st.file_uploader("ファイルをアップロード", type=[e.lstrip(".") for e in ext_list], key="cm_upload")

    if up_file:
        file_bytes = up_file.read()
        cand_data = load_candidate_upload(file_bytes, up_file.name)
        if cand_data:
            st.success("読み取り完了")

            with st.expander("読み取り内容を確認", expanded=True):
                info = cand_data.get("info", {})
                strengths = cand_data.get("strengths", [])
                conds = cand_data.get("conditions", {})

                ic1, ic2 = st.columns(2)
                with ic1:
                    st.markdown("**基本情報**")
                    for k, v in info.items():
                        st.markdown(f"- {k}: {v}")
                with ic2:
                    st.markdown("**抽出キーワード**")
                    st.markdown(", ".join(f'`{k}`' for k in conds.get("keywords", [])) or "なし")
                    st.markdown("**強み**")
                    for s in strengths[:5]:
                        if isinstance(s, (list, tuple)):
                            st.markdown(f"- {s[0]}")
                        else:
                            st.markdown(f"- {s}")

            save_name = st.text_input("候補者名", value=up_file.name.rsplit(".", 1)[0], key="cm_name")
            if st.button("保存", type="primary", key="cm_save"):
                save_candidate(save_name, info, strengths, conds)
                st.success(f"「{save_name}」を保存しました")
                st.rerun()
        else:
            st.error("ファイルから候補者情報を読み取れませんでした")

    # --- CSVから一括取り込み ---
    if csv_cands:
        st.markdown("---")
        st.markdown("### CSVから一括取り込み")
        if st.button("CSVファイルの候補者をすべて保存", key="cm_bulk"):
            added = 0
            for c in csv_cands:
                save_candidate(
                    c.get("display_name", "候補者"),
                    c.get("info", {}),
                    c.get("strengths", []),
                    c.get("conditions", {}),
                )
                added += 1
            st.success(f"{added}名を保存しました")
            st.rerun()

    # --- 保存済み一覧 ---
    st.markdown("---")
    st.markdown("### 保存済み候補者")

    if saved_cands:
        for cand in saved_cands:
            mf = evaluate_market_fit(cand)
            star_html = ' <span style="color:#f59e0b;">⭐ 決まりやすい</span>' if mf["has_star"] else ""
            st.markdown(f"""
            <div class="cand-card">
                <strong style="font-size:1.05rem;">{esc(cand.get('name','候補者'))}</strong>
                {star_html}
                <span style="color:#a0aec0; margin-left:1rem; font-size:0.8rem;">
                    登録: {esc(cand.get('created_at','')[:10])}
                </span>
            </div>
            """, unsafe_allow_html=True)

            cm_c1, cm_c2 = st.columns([1, 1])
            if cm_c1.button("👤 詳細を開く", key=f"cm_pop_{cand['id']}"):
                show_candidate_popup(cand)
            if cm_c2.button("🗑️ 削除", key=f"cm_del_{cand['id']}"):
                delete_candidate(cand["id"])
                st.success("削除しました")
                st.rerun()
    else:
        st.info("保存済みの候補者がいません。上のフォームからアップロードしてください。")


# ============================================================
# ページ4: データ管理
# ============================================================
elif page == "data_mgmt":
    st.markdown("## 📦 データ管理")

    # 統計
    dc1, dc2, dc3 = st.columns(3)
    dc1.markdown(f'<div class="stat-card"><div class="stat-num">{stats["total_jobs"]:,}</div>'
                 f'<div class="stat-label">登録求人数</div></div>', unsafe_allow_html=True)
    dc2.markdown(f'<div class="stat-card-blue"><div class="stat-num">{len(stats.get("sources",{}))}</div>'
                 f'<div class="stat-label">データソース数</div></div>', unsafe_allow_html=True)
    newest = stats.get("newest")
    date_str = "データなし"
    if newest:
        try:
            date_str = datetime.fromisoformat(newest).strftime("%m/%d %H:%M")
        except (ValueError, TypeError):
            pass
    dc3.markdown(f'<div class="stat-card-green"><div class="stat-num" style="font-size:1.3rem">{date_str}</div>'
                 f'<div class="stat-label">最終更新</div></div>', unsafe_allow_html=True)

    st.markdown("")

    dm_tabs = st.tabs(["🔄 Web自動取得", "📝 キーワード管理", "📤 手動インポート", "🗄️ データ管理"])

    # --- 自動取得 ---
    with dm_tabs[0]:
        st.markdown("### 各サイトから自動取得")
        st.caption("CareerJet・リクルートエージェント・求人ボックスからデータを取得します")

        st.markdown("**取得ソース:**")
        enabled_sources = []
        src_cols = st.columns(len(SOURCE_NAMES))
        for i, name in enumerate(SOURCE_NAMES):
            if src_cols[i].checkbox(name, value=True, key=f"dm_src_{name}"):
                enabled_sources.append(name)

        registered_kws = get_enabled_keywords()
        if registered_kws:
            kw_display = ", ".join([f"「{kw['keyword']}」" for kw in registered_kws[:10]])
            st.markdown(f"**使用キーワード:** {kw_display}")
        else:
            st.info("キーワード未登録。「キーワード管理」タブで追加してください。")

        fetch_loc = st.text_input("取得勤務地", value="大阪", key="dm_fetch_loc")

        bg_running = _get_bg_status()["status"] == "running"
        fetch_label = "🔄 取得中..." if bg_running else "今すぐ自動取得を実行"
        if st.button(fetch_label, type="primary", use_container_width=True,
                     key="dm_fetch", disabled=bg_running):
            kw_list = [kw["keyword"] for kw in registered_kws]
            if not kw_list:
                st.error("キーワードを登録してください")
            elif not enabled_sources:
                st.error("ソースを1つ以上選択してください")
            else:
                if start_bg_fetch(kw_list, fetch_loc, enabled_sources):
                    st.info("🔄 バックグラウンドで取得を開始しました。他のタブを自由にお使いください。")
                    st.rerun()
                else:
                    st.warning("既に取得中です")

        if bg_running:
            st.info("🔄 データ取得中です。サイドバーで進捗を確認できます。")

        logs = get_collection_logs(5)
        if logs:
            with st.expander("取得ログ"):
                for log in logs:
                    ran = log.get("ran_at", "")[:16].replace("T", " ")
                    st.caption(f"{ran} | {log.get('sources','')} | "
                               f"KW:{log.get('keywords_used',0)} → "
                               f"取得:{log.get('jobs_found',0)}/保存:{log.get('jobs_saved',0)} | "
                               f"{log.get('duration_sec',0):.1f}秒")

    # --- キーワード管理 ---
    with dm_tabs[1]:
        st.markdown("### 取得キーワード管理")
        with st.form("dm_add_kw"):
            kc1, kc2 = st.columns([3, 1])
            new_kw = kc1.text_input("キーワード", placeholder="例: Webデザイナー")
            new_kw_loc = kc2.text_input("勤務地", value="大阪", key="dm_kw_loc")
            if st.form_submit_button("追加"):
                if new_kw.strip():
                    if add_keyword(new_kw.strip(), new_kw_loc.strip()):
                        st.success(f"「{new_kw}」を追加")
                        st.rerun()
                    else:
                        st.warning("既に登録済み")

        if st.button("候補者からキーワードを自動追加", key="dm_auto_kw"):
            added = 0
            for c in saved_cands:
                cc = c.get("conditions", {})
                for kw in cc.get("keywords", []):
                    if add_keyword(kw, "大阪"):
                        added += 1
                for kw in cc.get("extra_keywords", [])[:3]:
                    if add_keyword(kw, "大阪"):
                        added += 1
            st.success(f"{added}件追加") if added else st.info("新規なし")
            if added:
                st.rerun()

        all_kws = get_keywords()
        if all_kws:
            st.markdown(f"**登録済み: {len(all_kws)}件**")
            for kw in all_kws:
                kc1, kc2 = st.columns([5, 1])
                icon = "✅" if kw["enabled"] else "⏸️"
                kc1.markdown(f"{icon} **{kw['keyword']}**（{kw.get('location','')}）")
                if kc2.button("削除", key=f"dm_del_kw_{kw['id']}"):
                    remove_keyword(kw["id"])
                    st.rerun()

    # --- 手動インポート ---
    with dm_tabs[2]:
        st.markdown("### 手動インポート")
        imp = st.radio("方法", ["CSV/Excel", "テキスト", "1件登録"], horizontal=True, key="dm_imp")

        if imp == "CSV/Excel":
            st.caption("カラム: 求人タイトル, 企業名, 勤務地, 年収, URL, 説明, ソース")
            uploaded = st.file_uploader("ファイル", type=["csv", "xlsx", "xls"], key="dm_csv")
            if uploaded:
                try:
                    if uploaded.name.endswith(".csv"):
                        content = uploaded.read().decode("utf-8-sig")
                        jobs = parse_csv_upload(content)
                    else:
                        udf = pd.read_excel(uploaded)
                        jobs = parse_csv_upload(udf.to_csv(index=False))
                    if jobs:
                        st.write(f"**{len(jobs)}件**検出")
                        if st.button("インポート", type="primary", key="dm_csv_imp"):
                            saved = save_jobs(jobs)
                            st.success(f"{saved}件保存")
                            st.rerun()
                except Exception as e:
                    st.error(f"エラー: {e}")

        elif imp == "テキスト":
            text = st.text_area("1行1求人（タイトル,企業名,勤務地,年収,URL）", height=100, key="dm_text")
            if st.button("登録", type="primary", key="dm_text_btn") and text.strip():
                jobs = parse_text_input(text)
                if jobs:
                    saved = save_jobs(jobs)
                    st.success(f"{saved}件保存")
                    st.rerun()

        elif imp == "1件登録":
            with st.form("dm_single"):
                j_title = st.text_input("求人タイトル *")
                jc1, jc2 = st.columns(2)
                j_company = jc1.text_input("企業名")
                j_url = jc2.text_input("求人URL")
                jc3, jc4 = st.columns(2)
                j_location = jc3.text_input("勤務地")
                j_salary = jc4.text_input("年収")
                j_desc = st.text_area("説明", height=60)
                if st.form_submit_button("登録", type="primary"):
                    if j_title:
                        save_jobs([{
                            "title": j_title, "company": j_company,
                            "url": j_url or f"manual_{datetime.now().isoformat()}",
                            "location": j_location, "salary": j_salary,
                            "description": j_desc, "source": "手動登録",
                        }])
                        st.success("登録完了")
                        st.rerun()

    # --- データ管理 ---
    with dm_tabs[3]:
        st.markdown("### 登録データ管理")
        if stats["total_jobs"] > 0:
            if stats.get("sources"):
                st.dataframe(
                    pd.DataFrame([{"ソース": s, "件数": c} for s, c in stats["sources"].items()]),
                    hide_index=True, use_container_width=True,
                )
            mc1, mc2 = st.columns(2)
            if mc1.button("60日以上古いデータを削除", key="dm_old"):
                deleted = delete_old_jobs(60)
                st.info(f"{deleted}件削除")
                st.rerun()
            if mc2.button("全データ削除", key="dm_clear"):
                if st.session_state.get("confirm_clear"):
                    clear_all()
                    st.session_state["confirm_clear"] = False
                    st.rerun()
                else:
                    st.session_state["confirm_clear"] = True
                    st.warning("もう一度クリックで実行")
        else:
            st.info("データなし")


# ============================================================
# ページ5: 検索リンク
# ============================================================
elif page == "search_links":
    st.markdown("## 🌐 検索リンク")
    st.caption("各求人サイトで直接検索できます。見つけた求人は「データ管理」からインポートしてください。")

    link_kw = st.text_input("検索キーワード", value="", placeholder="例: Webデザイナー", key="sl_kw")
    link_loc = st.text_input("勤務地", value="大阪", key="sl_loc")

    if link_kw.strip():
        all_links = generate_search_urls(link_kw, link_loc)
        link_html = ""
        for link in all_links:
            link_html += f"""
            <a href="{esc(link['url'])}" target="_blank" class="search-link-btn">
                {link['icon']} {esc(link['site'])}
            </a>"""
        st.markdown(link_html, unsafe_allow_html=True)
    else:
        st.info("キーワードを入力してください")


# ============================================================
# ページ6: 面談シート作成
# ============================================================
elif page == "interview":
    st.markdown("## 📝 面談シート作成")
    st.caption("候補者との面談情報を構造化して記録・管理します")

    iv_tabs = st.tabs(["✏️ 新規作成", "📋 シート一覧"])

    # --- 新規作成 ---
    with iv_tabs[0]:
        if not saved_cands:
            st.markdown('<div class="empty-state"><h3>候補者が登録されていません</h3>'
                        '<p>「👤 候補者管理」から候補者を追加してください</p></div>',
                        unsafe_allow_html=True)
        else:
            # 候補者選択
            iv_cand_options = [f"{c['name']}（ID:{c['id']}）" for c in saved_cands]
            iv_sel = st.selectbox("対象候補者", iv_cand_options, key="iv_cand_sel")
            iv_sel_idx = iv_cand_options.index(iv_sel) if iv_sel else 0
            iv_cand = saved_cands[iv_sel_idx]

            # 候補者情報サマリー
            with st.expander("👤 候補者情報", expanded=False):
                ci = iv_cand.get("info", {})
                cs = iv_cand.get("strengths", [])
                ic1, ic2 = st.columns(2)
                with ic1:
                    st.markdown("**基本情報**")
                    for k, v in ci.items():
                        st.markdown(f"- **{k}**: {v}")
                with ic2:
                    st.markdown("**強み・スキル**")
                    for s in cs[:5]:
                        if isinstance(s, (list, tuple)) and len(s) >= 2:
                            st.markdown(f"- **{s[0]}**: {s[1][:60]}")
                        elif isinstance(s, str):
                            st.markdown(f"- {s}")

            st.markdown("---")

            # 面談メモ入力
            st.markdown("### 面談内容を入力")
            raw_input = st.text_area(
                "面談メモ（自由記述）",
                height=250,
                placeholder="面談で聞き取った内容をそのまま入力してください。\n"
                            "例:\n"
                            "- 現職: ○○クリニックで3年勤務\n"
                            "- 転職理由: キャリアアップしたい\n"
                            "- 希望年収: 400万〜\n"
                            "- 希望勤務地: 大阪市内\n"
                            "- 人柄: 明るく協調性がある",
                key="iv_raw_input",
            )

            # タグ入力
            tag_input = st.text_input(
                "タグ（カンマ区切り）",
                placeholder="例: 医療事務, 即日可, 大阪希望, 経験3年以上, 管理職志向",
                key="iv_tags",
            )

            if st.button("面談シートを生成", type="primary", key="iv_generate"):
                if not raw_input.strip():
                    st.error("面談内容を入力してください")
                else:
                    # 面談シートを構造化して生成
                    tags = [t.strip() for t in tag_input.split(",") if t.strip()] if tag_input else []

                    # 入力テキストから自動タグ抽出
                    auto_tag_patterns = {
                        "即日可": ["即日", "すぐ", "急ぎ"],
                        "大阪希望": ["大阪"],
                        "東京希望": ["東京"],
                        "リモート希望": ["リモート", "在宅"],
                        "管理職志向": ["管理職", "マネジメント", "リーダー"],
                        "未経験": ["未経験"],
                        "医療系": ["医療", "クリニック", "病院", "看護"],
                        "IT系": ["エンジニア", "プログラマ", "SE", "IT"],
                        "営業経験": ["営業"],
                        "年収重視": ["年収", "給与", "高収入"],
                    }
                    for tag_name, keywords in auto_tag_patterns.items():
                        if tag_name not in tags:
                            for kw in keywords:
                                if kw in raw_input:
                                    tags.append(tag_name)
                                    break

                    # 構造化シート生成
                    lines = raw_input.strip().split("\n")
                    sections = {
                        "職歴・経験": [],
                        "転職理由・動機": [],
                        "希望条件": [],
                        "スキル・資格": [],
                        "人物像・印象": [],
                        "その他": [],
                    }

                    current_section = "その他"
                    section_keywords = {
                        "職歴・経験": ["現職", "前職", "経験", "勤務", "年数", "業務", "職歴"],
                        "転職理由・動機": ["転職理由", "動機", "理由", "退職", "辞め"],
                        "希望条件": ["希望", "年収", "勤務地", "時間", "休日", "条件"],
                        "スキル・資格": ["スキル", "資格", "免許", "言語", "ツール"],
                        "人物像・印象": ["人柄", "印象", "性格", "雰囲気", "コミュニケーション"],
                    }

                    for line in lines:
                        line_clean = line.strip()
                        if not line_clean:
                            continue
                        matched_section = None
                        for sec, kws in section_keywords.items():
                            if any(kw in line_clean for kw in kws):
                                matched_section = sec
                                break
                        if matched_section:
                            current_section = matched_section
                        sections[current_section].append(line_clean)

                    # シート本文を組み立て
                    sheet_parts = []
                    sheet_parts.append(f"# 面談シート: {iv_cand.get('name', '候補者')}")
                    sheet_parts.append(f"作成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                    sheet_parts.append("")

                    for sec_name, sec_lines in sections.items():
                        if sec_lines:
                            sheet_parts.append(f"## {sec_name}")
                            for sl in sec_lines:
                                sheet_parts.append(f"- {sl.lstrip('-').lstrip('・').strip()}")
                            sheet_parts.append("")

                    if tags:
                        sheet_parts.append("## タグ")
                        sheet_parts.append(", ".join(f"#{t}" for t in tags))

                    sheet_content = "\n".join(sheet_parts)

                    # AI解析レポートも生成
                    ai_result = generate_interview_analysis(raw_input, iv_cand)
                    ai_tags = ai_result.get("tags", [])
                    # タグをマージ（手動 + AI抽出）
                    for at in ai_tags:
                        tag_clean = at.lstrip("#")
                        if tag_clean not in tags:
                            tags.append(tag_clean)

                    # AI解析レポートをシートに統合
                    sheet_content += "\n\n" + ai_result.get("report", "")

                    # 保存
                    sheet_id = save_interview_sheet(
                        iv_cand["id"], raw_input, sheet_content, tags
                    )
                    st.success(f"面談シートを保存しました（ID: {sheet_id}）")

                    # プレビュー
                    st.markdown("---")
                    st.markdown("### 生成されたシート")
                    st.markdown(sheet_content)

                    if tags:
                        tag_html = " ".join(f'<span class="fit-tag">#{esc(t)}</span>' for t in tags)
                        st.markdown(tag_html, unsafe_allow_html=True)

    # --- シート一覧 ---
    with iv_tabs[1]:
        all_sheets = get_interview_sheets()

        if not all_sheets:
            st.markdown('<div class="empty-state"><h3>面談シートがまだありません</h3>'
                        '<p>「新規作成」タブから面談シートを作成してください</p></div>',
                        unsafe_allow_html=True)
        else:
            # フィルタ
            if saved_cands:
                filter_options = ["全員"] + [c["name"] for c in saved_cands]
                iv_filter = st.selectbox("候補者でフィルタ", filter_options, key="iv_filter")
            else:
                iv_filter = "全員"

            # 候補者名マップ
            cand_map = {c["id"]: c["name"] for c in saved_cands}

            st.markdown(f"**{len(all_sheets)}件**の面談シート")

            for sheet in all_sheets:
                cand_name = cand_map.get(sheet["candidate_id"], f"候補者#{sheet['candidate_id']}")

                if iv_filter != "全員" and cand_name != iv_filter:
                    continue

                tags = sheet.get("tags", [])
                tag_html = " ".join(f'<span class="fit-tag">#{esc(t)}</span>' for t in tags) if tags else ""
                created = sheet.get("created_at", "")[:16].replace("T", " ")

                st.markdown(f"""
                <div class="cand-card">
                    <strong>{esc(cand_name)}</strong>
                    <span style="color:#a0aec0; margin-left:1rem; font-size:0.8rem;">{created}</span>
                    <div style="margin-top:0.3rem;">{tag_html}</div>
                </div>
                """, unsafe_allow_html=True)

                with st.expander(f"📋 シート詳細 - {cand_name}", expanded=False):
                    st.markdown(sheet.get("sheet_content", "内容なし"))

                    st.markdown("---")
                    st.markdown("**元の入力:**")
                    st.text(sheet.get("raw_input", "")[:500])

                    sc1, sc2 = st.columns(2)
                    # 編集
                    edit_key = f"iv_edit_{sheet['id']}"
                    if sc1.button("編集", key=f"iv_edit_btn_{sheet['id']}"):
                        st.session_state[edit_key] = True

                    if st.session_state.get(edit_key):
                        new_content = st.text_area(
                            "シート内容を編集",
                            value=sheet.get("sheet_content", ""),
                            height=200,
                            key=f"iv_edit_ta_{sheet['id']}",
                        )
                        new_tags = st.text_input(
                            "タグ編集（カンマ区切り）",
                            value=", ".join(tags),
                            key=f"iv_edit_tags_{sheet['id']}",
                        )
                        if st.button("更新", type="primary", key=f"iv_update_{sheet['id']}"):
                            parsed_tags = [t.strip() for t in new_tags.split(",") if t.strip()]
                            update_interview_sheet(sheet["id"], new_content, parsed_tags)
                            st.session_state[edit_key] = False
                            st.success("更新しました")
                            st.rerun()

                    # 削除
                    if sc2.button("削除", key=f"iv_del_{sheet['id']}"):
                        delete_interview_sheet(sheet["id"])
                        st.success("削除しました")
                        st.rerun()


# ============================================================
# ページ7: 提案済み（進捗管理）
# ============================================================
elif page == "progress":
    st.markdown("## 📊 提案済み（進捗管理）")
    st.caption("候補者×求人の提案状況を管理します")

    proposals = get_proposals()
    cand_map = {c["id"]: c for c in saved_cands}

    # --- 統計パイプライン ---
    status_counts = {}
    for s in PROPOSAL_STATUSES:
        status_counts[s] = 0
    for p in proposals:
        s = p.get("status", "提案済み")
        if s in status_counts:
            status_counts[s] += 1

    # パイプラインバー
    if proposals:
        pipeline_html = '<div style="display:flex; gap:4px; margin-bottom:1rem;">'
        colors = ["#667eea", "#4facfe", "#43e97b", "#38f9d7", "#f9d423", "#ff6b6b", "#ee5a24", "#6c5ce7"]
        for i, (status, count) in enumerate(status_counts.items()):
            color = colors[i % len(colors)]
            pipeline_html += f"""
            <div style="flex:1; text-align:center; padding:0.5rem; border-radius:8px;
                         background:{color}20; border:2px solid {color};">
                <div style="font-size:1.5rem; font-weight:800; color:{color};">{count}</div>
                <div style="font-size:0.7rem; color:#4a5568;">{esc(status)}</div>
            </div>"""
        pipeline_html += '</div>'
        st.markdown(pipeline_html, unsafe_allow_html=True)

    # --- 新規提案登録 ---
    with st.expander("➕ 新しい提案を登録", expanded=False):
        if not saved_cands:
            st.info("候補者を先に登録してください")
        elif stats["total_jobs"] == 0:
            st.info("求人データを先に登録してください")
        else:
            pr_cand_options = [f"{c['name']}（ID:{c['id']}）" for c in saved_cands]
            pr_cand_sel = st.selectbox("候補者", pr_cand_options, key="pr_cand")
            pr_cand_idx = pr_cand_options.index(pr_cand_sel)
            pr_cand_id = saved_cands[pr_cand_idx]["id"]

            # 求人検索して選択
            pr_job_kw = st.text_input("求人を検索", placeholder="キーワードで検索", key="pr_job_kw")
            if pr_job_kw.strip():
                pr_job_results = search_jobs(pr_job_kw)
            else:
                pr_job_results = get_all_jobs(limit=50)

            if pr_job_results:
                pr_job_options = [
                    f"{j.get('title','不明')} - {j.get('company','')}"
                    for j in pr_job_results[:30]
                ]
                pr_job_sel = st.selectbox("求人を選択", pr_job_options, key="pr_job")
                pr_job_idx = pr_job_options.index(pr_job_sel)
                pr_job = pr_job_results[pr_job_idx]

                pr_status = st.selectbox("ステータス", PROPOSAL_STATUSES, key="pr_status")
                pr_memo = st.text_input("メモ", placeholder="備考など", key="pr_memo")

                if st.button("提案を登録", type="primary", key="pr_save"):
                    save_proposal(pr_cand_id, pr_job.get("url", ""), pr_status, pr_memo)
                    st.success("提案を登録しました")
                    st.rerun()

    # --- 提案一覧 ---
    st.markdown("---")

    if not proposals:
        st.markdown('<div class="empty-state"><h3>提案がまだありません</h3>'
                    '<p>上の「新しい提案を登録」から追加してください</p></div>',
                    unsafe_allow_html=True)
    else:
        # フィルタ
        pf1, pf2, pf3 = st.columns(3)
        pr_filter_status = pf1.multiselect("ステータス", PROPOSAL_STATUSES,
                                            default=PROPOSAL_STATUSES, key="pr_fstatus")
        if saved_cands:
            pr_filter_cand_opts = ["全員"] + [c["name"] for c in saved_cands]
            pr_filter_cand = pf2.selectbox("候補者", pr_filter_cand_opts, key="pr_fcand")
        else:
            pr_filter_cand = "全員"
        pr_sort = pf3.selectbox("並び替え", ["更新日順", "ステータス順"], key="pr_sort")

        # フィルタ適用
        filtered_proposals = []
        for p in proposals:
            if p.get("status", "") not in pr_filter_status:
                continue
            cand_name = cand_map.get(p["candidate_id"], {}).get("name", f"候補者#{p['candidate_id']}")
            if pr_filter_cand != "全員" and cand_name != pr_filter_cand:
                continue
            filtered_proposals.append(p)

        if pr_sort == "ステータス順":
            filtered_proposals.sort(key=lambda x: PROPOSAL_STATUSES.index(x.get("status", "提案済み"))
                                    if x.get("status", "提案済み") in PROPOSAL_STATUSES else 99)

        st.markdown(f"**{len(filtered_proposals)}件**の提案")

        for p in filtered_proposals:
            cand_info = cand_map.get(p["candidate_id"], {})
            cand_name = cand_info.get("name", f"候補者#{p['candidate_id']}")

            # 求人情報取得（URLで検索）
            job_url = p.get("job_url", "")
            job_info = None
            if job_url:
                from cache_manager import _get_conn
                conn = _get_conn()
                row = conn.execute("SELECT * FROM jobs WHERE url = ?", (job_url,)).fetchone()
                if row:
                    job_info = dict(row)

            job_title = job_info.get("title", "不明") if job_info else "不明"
            job_company = job_info.get("company", "") if job_info else ""
            status = p.get("status", "提案済み")
            updated = p.get("updated_at", "")[:16].replace("T", " ")

            # ステータスの色
            status_color_map = {
                "提案済み": "#667eea", "カジュアル面談": "#4facfe",
                "一次面接": "#43e97b", "二次面接": "#38f9d7",
                "三次面接": "#f9d423", "内定": "#ff6b6b",
                "内定承諾": "#ee5a24", "決定": "#6c5ce7",
            }
            s_color = status_color_map.get(status, "#667eea")

            st.markdown(f"""
            <div class="job-card">
                <span style="display:inline-block; padding:0.2rem 0.7rem; border-radius:12px;
                             background:{s_color}20; color:{s_color}; font-weight:700; font-size:0.85rem;">
                    {esc(status)}
                </span>
                <span style="color:#a0aec0; font-size:0.8rem; margin-left:0.5rem;">{updated}</span>
                <div style="margin-top:0.4rem;">
                    <strong>👤 {esc(cand_name)}</strong>
                    &nbsp;→&nbsp;
                    <strong>📋 {esc(job_title)}</strong>
                    {f' ({esc(job_company)})' if job_company else ''}
                </div>
                {f'<div style="color:#718096; font-size:0.85rem; margin-top:0.2rem;">📝 {esc(p.get("memo",""))}</div>' if p.get("memo") else ''}
                {f'<div style="color:#4a5568; font-size:0.85rem;">⏭️ 次: {esc(p.get("next_action",""))}</div>' if p.get("next_action") else ''}
            </div>
            """, unsafe_allow_html=True)

            with st.expander(f"⚙️ 操作 - {cand_name} × {job_title[:20]}", expanded=False):
                # 候補者詳細
                if cand_info and cand_info.get("info"):
                    st.markdown("**👤 候補者情報:**")
                    ci = cand_info.get("info", {})
                    for k, v in list(ci.items())[:5]:
                        st.markdown(f"- **{k}**: {v}")

                # 求人詳細
                if job_info:
                    st.markdown("**📋 求人情報:**")
                    st.markdown(f"- タイトル: {job_info.get('title','')}")
                    st.markdown(f"- 企業: {job_info.get('company','')}")
                    st.markdown(f"- 勤務地: {job_info.get('location','')}")
                    st.markdown(f"- 年収: {job_info.get('salary','')}")
                    url = job_info.get("url", "")
                    if url and url.startswith("http"):
                        st.link_button("求人ページを開く", url, key=f"pr_link_{p['id']}")

                st.markdown("---")

                # ステータス更新
                st.markdown("**ステータス変更:**")
                pu1, pu2 = st.columns(2)
                current_idx = PROPOSAL_STATUSES.index(status) if status in PROPOSAL_STATUSES else 0
                new_status = pu1.selectbox(
                    "新しいステータス", PROPOSAL_STATUSES,
                    index=current_idx, key=f"pr_ns_{p['id']}",
                )
                new_memo = pu2.text_input("メモ更新", value=p.get("memo", ""), key=f"pr_nm_{p['id']}")
                new_next = st.text_input("次のアクション", value=p.get("next_action", ""),
                                          placeholder="例: 一次面接の日程調整", key=f"pr_na_{p['id']}")

                uc1, uc2 = st.columns(2)
                if uc1.button("更新", type="primary", key=f"pr_upd_{p['id']}"):
                    update_proposal_status(p["id"], new_status, new_memo, new_next)
                    st.success("更新しました")
                    st.rerun()

                if uc2.button("この提案を削除", key=f"pr_del_{p['id']}"):
                    delete_proposal(p["id"])
                    st.success("削除しました")
                    st.rerun()

                # AI進捗分析
                st.markdown("---")
                if st.button("🤖 進捗分析レポート", key=f"pr_ai_{p['id']}"):
                    proposal_data = {**p, "job_title": job_title}
                    analysis = generate_progress_analysis(proposal_data, cand_info if cand_info else None)
                    st.session_state[f"pr_analysis_{p['id']}"] = analysis
                if st.session_state.get(f"pr_analysis_{p['id']}"):
                    st.markdown(st.session_state[f"pr_analysis_{p['id']}"])
                    if st.button("閉じる", key=f"close_pr_analysis_{p['id']}"):
                        del st.session_state[f"pr_analysis_{p['id']}"]
                        st.rerun()


# ============================================================
# ページ8: 候補者比較（最大3名）
# ============================================================
elif page == "compare":
    st.markdown("## ⚖️ 候補者比較")
    st.caption("最大3名の候補者を並べて比較できます")

    if len(saved_cands) < 2:
        st.markdown('<div class="empty-state"><h3>候補者が2名以上必要です</h3>'
                    '<p>「👤 候補者管理」から候補者を追加してください</p></div>',
                    unsafe_allow_html=True)
    else:
        cand_names = [c["name"] for c in saved_cands]

        cc1, cc2, cc3 = st.columns(3)
        sel1 = cc1.selectbox("候補者1", ["--"] + cand_names, key="cmp_1")
        sel2 = cc2.selectbox("候補者2", ["--"] + cand_names, key="cmp_2")
        sel3 = cc3.selectbox("候補者3（任意）", ["--"] + cand_names, key="cmp_3")

        selected = []
        for sel_name in [sel1, sel2, sel3]:
            if sel_name != "--":
                c = next((c for c in saved_cands if c["name"] == sel_name), None)
                if c:
                    selected.append(c)

        # 求人を選んでスコア計算
        compare_job = None
        if stats["total_jobs"] > 0:
            with st.expander("📋 特定の求人でスコアを比較", expanded=False):
                cmp_kw = st.text_input("求人を検索", placeholder="キーワード", key="cmp_job_kw")
                if cmp_kw.strip():
                    cmp_jobs = search_jobs(cmp_kw)
                else:
                    cmp_jobs = get_all_jobs(limit=30)
                if cmp_jobs:
                    cmp_job_opts = [f"{j.get('title','不明')} - {j.get('company','')}" for j in cmp_jobs[:20]]
                    cmp_sel = st.selectbox("求人", ["--"] + cmp_job_opts, key="cmp_job_sel")
                    if cmp_sel != "--":
                        cmp_idx = cmp_job_opts.index(cmp_sel)
                        compare_job = cmp_jobs[cmp_idx]

        if len(selected) >= 2:
            st.markdown("---")
            cols = st.columns(len(selected))

            for idx, (col, cand) in enumerate(zip(cols, selected)):
                info = cand.get("info", {})
                strengths = cand.get("strengths", [])
                conditions = cand.get("conditions", {})

                # Market Fit評価
                mf = evaluate_market_fit(cand)

                # 求人選択時はスコア計算
                score_val = None
                if compare_job:
                    cond = _cand_to_conditions(cand)
                    score_val, reasons = score_job(compare_job, cond)

                with col:
                    st.markdown(f"### {esc(cand.get('name', '候補者'))}")
                    if mf["has_star"]:
                        st.markdown("⭐ **Market Fit**")

                    if score_val is not None:
                        st.markdown(_score_badge(score_val), unsafe_allow_html=True)
                        st.markdown(_match_bar(score_val), unsafe_allow_html=True)

                    st.markdown("**基本情報**")
                    for k, v in list(info.items())[:5]:
                        st.caption(f"{k}: {v}")

                    st.markdown("**強み**")
                    for s in strengths[:4]:
                        if isinstance(s, (list, tuple)) and len(s) >= 2:
                            st.caption(f"• {s[0]}")
                        elif isinstance(s, str):
                            st.caption(f"• {s}")

                    st.markdown("**希望条件**")
                    kws = conditions.get("keywords", [])
                    st.caption(f"KW: {', '.join(kws[:3])}")
                    st.caption(f"年収: {conditions.get('salary_min',0)}〜{conditions.get('salary_max',0)}万")
                    st.caption(f"勤務地: {conditions.get('location','')}")

                    # Market Fit 5軸
                    st.markdown("**Market Fit 5軸**")
                    for axis in MARKET_FIT_AXES:
                        ax_val = mf["axes"].get(axis["id"], "neutral")
                        icon = "🟢" if ax_val == "positive" else ("🟡" if ax_val == "neutral" else "🔴")
                        st.caption(f"{icon} {axis['label']}")
        else:
            st.info("2名以上選択してください")


# ============================================================
# ページ9: 設定
# ============================================================
elif page == "settings":
    st.markdown("## ⚙️ 設定")
    st.caption("マッチングスコアの計算方法、ラベル表示、Market Fit評価のルールを設定します")

    set_tabs = st.tabs(["🎯 スコア重み", "🏷️ ラベル閾値", "⭐ Market Fit", "🤖 AIプリセット"])

    # --- スコア重み ---
    with set_tabs[0]:
        st.markdown("### スコア重み設定")
        st.caption("4カテゴリの重みを設定します。合計は100になるよう調整してください。")

        defaults_w = {"job": 35, "skill": 35, "soft": 20, "conditions": 10}
        saved_w = get_app_setting("score_weights", defaults_w)

        sw1, sw2 = st.columns(2)
        w_job = sw1.number_input("職種一致度", min_value=0, max_value=100, value=saved_w.get("job", 35), step=5, key="sw_job")
        w_skill = sw2.number_input("スキル一致度", min_value=0, max_value=100, value=saved_w.get("skill", 35), step=5, key="sw_skill")
        sw3, sw4 = st.columns(2)
        w_soft = sw3.number_input("ソフトスキル", min_value=0, max_value=100, value=saved_w.get("soft", 20), step=5, key="sw_soft")
        w_cond = sw4.number_input("条件一致度", min_value=0, max_value=100, value=saved_w.get("conditions", 10), step=5, key="sw_cond")

        total = w_job + w_skill + w_soft + w_cond
        if total != 100:
            st.warning(f"合計: {total}（100にしてください）")
        else:
            st.success(f"合計: {total} ✓")

        if st.button("重みを保存", type="primary", key="sw_save"):
            set_app_setting("score_weights", {"job": w_job, "skill": w_skill, "soft": w_soft, "conditions": w_cond})
            st.success("保存しました")

        # 現在の重みをビジュアル表示
        st.markdown("**現在の配分:**")
        import json as _json
        bar_data = {"カテゴリ": ["職種一致", "スキル一致", "ソフトスキル", "条件一致"],
                    "重み": [w_job, w_skill, w_soft, w_cond]}
        st.bar_chart(pd.DataFrame(bar_data).set_index("カテゴリ"))

    # --- ラベル閾値 ---
    with set_tabs[1]:
        st.markdown("### ラベル閾値設定")
        st.caption("マッチ度スコアに応じたラベル表示の閾値を設定します")

        defaults_l = {"recommended": 85, "good": 70}
        saved_l = get_app_setting("label_thresholds", defaults_l)

        st.markdown("""
        | スコア範囲 | ラベル |
        |-----------|--------|
        | 閾値1以上 | かなり相性が良い |
        | 閾値2〜閾値1 | 相性が良い |
        | 55〜閾値2 | 可能性あり |
        | 55未満 | 要検討 |
        """)

        lt1, lt2 = st.columns(2)
        th_rec = lt1.number_input("閾値1（かなり相性が良い）", 50, 100, saved_l.get("recommended", 85), step=5, key="lt_rec")
        th_good = lt2.number_input("閾値2（相性が良い）", 30, 100, saved_l.get("good", 70), step=5, key="lt_good")

        # プレビュー
        st.markdown("**プレビュー:**")
        for test_score in [90, 80, 65, 40]:
            if test_score >= th_rec:
                label = "かなり相性が良い"
            elif test_score >= th_good:
                label = "相性が良い"
            elif test_score >= 55:
                label = "可能性あり"
            else:
                label = "要検討"
            st.caption(f"{test_score}点 → {label}")

        if st.button("閾値を保存", type="primary", key="lt_save"):
            set_app_setting("label_thresholds", {"recommended": th_rec, "good": th_good})
            st.success("保存しました")

    # --- Market Fit ---
    with set_tabs[2]:
        st.markdown("### Market Fit ⭐ 評価ルール")
        st.caption("5軸評価で候補者に⭐（決まりやすい）マークを付与するルールです")

        defaults_mf = {"required_positives": 3, "block_on_major_negative": True}
        saved_mf = get_app_setting("market_fit_rules", defaults_mf)

        st.markdown("**5軸評価:**")
        for axis in MARKET_FIT_AXES:
            st.markdown(f"- {axis['label']} — {axis['desc']}")

        mf1, mf2 = st.columns(2)
        mf_req = mf1.number_input("⭐付与に必要なポジティブ軸数", 1, 5,
                                   saved_mf.get("required_positives", 3), key="mf_req")
        mf_block = mf2.checkbox("重大ネガティブ要因がある場合は⭐非付与",
                                 value=saved_mf.get("block_on_major_negative", True), key="mf_block")

        if st.button("Market Fitルールを保存", type="primary", key="mf_save"):
            set_app_setting("market_fit_rules", {
                "required_positives": mf_req,
                "block_on_major_negative": mf_block,
            })
            st.success("保存しました")

        # 候補者のMarket Fit一覧
        if saved_cands:
            st.markdown("---")
            st.markdown("### 候補者のMarket Fit状況")
            for c in saved_cands:
                mf = evaluate_market_fit(c)
                star = "⭐" if mf["has_star"] else "—"
                st.markdown(f"**{star} {c.get('name', '候補者')}** "
                            f"（ポジティブ: {mf['positive_count']}/5, "
                            f"ネガティブ: {'あり' if mf['has_major_negative'] else 'なし'}）")
                if mf["reason"]:
                    st.caption(mf["reason"])

    # --- AIプリセット ---
    with set_tabs[3]:
        st.markdown("### AIプリセット管理")
        st.caption("各画面で表示されるAIサジェストボタンのプリセットを管理します")

        defaults_ai = {
            "candidateSearch": ["💬 コミュニケーション能力が高い人だけ表示", "💪 メンタルが強い人だけ表示", "🚀 ベンチャーマインドが強い人だけ表示"],
            "jobSearch": ["💬 コミュニケーション能力が高い人だけ表示", "💪 メンタルが強い人だけ表示"],
            "interviewSheet": ["🎯 カルチャーフィットしそうな求人順に並べて", "💡 必須が当てはまらなくても可能性が高そうな求人は？"],
            "proposals": ["🚨 急ぎ確認必要な進捗を教えて", "⏸️ 動いていない進捗を教えて"],
        }
        saved_ai = get_app_setting("ai_presets", defaults_ai)

        preset_labels = {
            "candidateSearch": "候補者検索",
            "jobSearch": "求人検索",
            "interviewSheet": "面談シート",
            "proposals": "提案管理",
        }

        for preset_key, preset_label in preset_labels.items():
            st.markdown(f"**{preset_label}プリセット:**")
            current = saved_ai.get(preset_key, defaults_ai.get(preset_key, []))
            new_val = st.text_area(
                f"{preset_label}（1行1プリセット）",
                value="\n".join(current),
                height=80,
                key=f"ai_preset_{preset_key}",
            )
            saved_ai[preset_key] = [l.strip() for l in new_val.split("\n") if l.strip()]

        if st.button("AIプリセットを保存", type="primary", key="ai_save"):
            set_app_setting("ai_presets", saved_ai)
            st.success("保存しました")

        # 理由プール
        st.markdown("---")
        st.markdown("### マッチ理由プール")
        defaults_reasons = [
            "職種経験が豊富", "顧客折衝が強い", "改善推進が得意",
            "リーダーシップあり", "コミュニケーション能力が高い", "論理的思考力がある",
            "チームワークを重視", "自走力がある", "学習意欲が高い",
            "柔軟性がある", "ストレス耐性が高い", "目標達成意識が強い",
        ]
        saved_reasons = get_app_setting("reasons_pool", defaults_reasons)
        reasons_text = st.text_area("理由プール（1行1理由）",
                                     value="\n".join(saved_reasons), height=150, key="reasons_pool")
        if st.button("理由プールを保存", type="primary", key="rp_save"):
            parsed = [l.strip() for l in reasons_text.split("\n") if l.strip()]
            set_app_setting("reasons_pool", parsed)
            st.success("保存しました")
