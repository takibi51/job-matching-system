"""
求人マッチングシステム — Match
プロトタイプ(match-app)のUI/UXに準拠
+ Web求人自動取得機能を追加
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
    add_chat_message, get_chat_history, clear_chat_history,
    update_job_type, get_job_type_stats,
)
from ai_generator import (
    generate_scout_message, generate_concerns, generate_hireability,
    generate_proposal_resume, generate_interview_analysis,
    generate_progress_analysis, generate_job_improvements,
    evaluate_market_fit, MARKET_FIT_AXES,
    generate_chat_response,
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
# バックグラウンド取得（スレッドセーフ・進捗%対応）
# ============================================================
_BG_LOCK = threading.Lock()
_bg_state = {"status": "idle", "result": "", "started": "", "progress": 0, "progress_detail": ""}


def _bg_fetch_worker(kw_list, location, sources):
    """別スレッドで求人取得を実行（進捗%を更新）"""
    try:
        with _BG_LOCK:
            _bg_state["status"] = "running"
            _bg_state["started"] = datetime.now().isoformat()
            _bg_state["progress"] = 0
            _bg_state["progress_detail"] = "準備中..."

        start = _time.time()
        total_steps = len(kw_list) * len(sources)
        completed = 0
        all_jobs = []

        for si, source_name in enumerate(sources):
            for ki, kw in enumerate(kw_list):
                with _BG_LOCK:
                    pct = int((completed / max(total_steps, 1)) * 100)
                    _bg_state["progress"] = pct
                    _bg_state["progress_detail"] = f"{source_name}: 「{kw}」取得中... ({pct}%)"

                try:
                    jobs = fetch_from_all_sources([kw], location, enabled_sources=[source_name])
                    all_jobs.extend(jobs)
                except Exception:
                    pass
                completed += 1

        elapsed = _time.time() - start
        with _BG_LOCK:
            _bg_state["progress"] = 100
            if all_jobs:
                saved = save_jobs(all_jobs)
                add_collection_log(len(kw_list), len(all_jobs), saved, ",".join(sources), elapsed)
                _bg_state["result"] = f"✅ {len(all_jobs)}件取得 → {saved}件保存（{elapsed:.1f}秒）"
            else:
                add_collection_log(len(kw_list), 0, 0, ",".join(sources), elapsed)
                _bg_state["result"] = "⚠️ 取得0件でした"
            _bg_state["progress_detail"] = "完了"
            _bg_state["status"] = "done"
    except Exception as e:
        with _BG_LOCK:
            _bg_state["status"] = "error"
            _bg_state["result"] = f"❌ エラー: {e}"


def _get_bg_status():
    with _BG_LOCK:
        return dict(_bg_state)


def start_bg_fetch(kw_list, location, sources):
    with _BG_LOCK:
        if _bg_state["status"] == "running":
            return False
        _bg_state["status"] = "running"
        _bg_state["progress"] = 0
    t = threading.Thread(target=_bg_fetch_worker, args=(kw_list, location, sources), daemon=True)
    t.start()
    return True


# ============================================================
# ユーティリティ
# ============================================================
def esc(text):
    return html.escape(str(text)) if text else ""


def _get_label_thresholds():
    return get_app_setting("label_thresholds", {"recommended": 85, "good": 70})


def _score_badge(score):
    th = _get_label_thresholds()
    if score >= th.get("recommended", 85):
        cls, label = "score-high", "かなり相性が良い"
    elif score >= th.get("good", 70):
        cls, label = "score-high", "相性が良い"
    elif score >= 55:
        cls, label = "score-mid", "可能性あり"
    else:
        cls, label = "score-low", "要検討"
    return f'<span class="score-badge {cls}">{score}点 {label}</span>'


def _match_bar(score):
    cls = "match-fill-high" if score >= 60 else ("match-fill-mid" if score >= 30 else "match-fill-low")
    return f'<div class="match-bar"><div class="match-fill {cls}" style="width:{min(score,100)}%"></div></div>'


def _fit_tags(reasons_str):
    if not reasons_str:
        return ""
    tags = reasons_str.split(" / ")
    return " ".join(f'<span class="fit-tag">{esc(t)}</span>' for t in tags[:5])


def _cand_to_conditions(cand):
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
# CSS（プロトタイプ準拠）
# ============================================================
st.markdown("""
<style>
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
    .job-card {
        border: 1px solid #e2e8f0; border-radius: 12px; padding: 1.2rem;
        margin-bottom: 0.8rem; background: white;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06); transition: all 0.2s;
    }
    .job-card:hover { border-color: #667eea; box-shadow: 0 4px 12px rgba(102,126,234,0.15); }
    .cand-card {
        border: 1px solid #e2e8f0; border-radius: 12px; padding: 1rem;
        margin-bottom: 0.6rem; background: white;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06); transition: all 0.2s;
    }
    .cand-card:hover { border-color: #43e97b; box-shadow: 0 4px 12px rgba(67,233,123,0.15); }
    .score-badge {
        display: inline-block; padding: 0.2rem 0.6rem; border-radius: 20px;
        font-weight: 700; font-size: 0.85rem;
    }
    .score-high { background: #d4edda; color: #155724; }
    .score-mid { background: #fff3cd; color: #856404; }
    .score-low { background: #f8d7da; color: #721c24; }
    .match-bar { background: #e9ecef; border-radius: 10px; height: 8px; overflow: hidden; margin: 0.3rem 0; }
    .match-fill { height: 100%; border-radius: 10px; transition: width 0.5s; }
    .match-fill-high { background: linear-gradient(90deg, #43e97b, #38f9d7); }
    .match-fill-mid { background: linear-gradient(90deg, #f9d423, #ff4e50); }
    .match-fill-low { background: linear-gradient(90deg, #ff4e50, #c62828); }
    .fit-tag {
        display: inline-block; padding: 0.15rem 0.5rem; margin: 0.1rem;
        border-radius: 12px; font-size: 0.75rem; background: #eef2ff; color: #3730a3;
    }
    .chat-msg {
        padding: 0.7rem 1rem; border-radius: 12px; margin: 0.3rem 0;
        font-size: 0.9rem; line-height: 1.5;
    }
    .chat-user { background: #eef2ff; text-align: right; }
    .chat-ai { background: #f0fdf4; border: 1px solid #d1fae5; }
    .empty-state {
        text-align: center; padding: 3rem; color: #a0aec0;
        border: 2px dashed #e2e8f0; border-radius: 16px; margin: 1rem 0;
    }
    .progress-bar-bg {
        background: #e9ecef; border-radius: 10px; height: 20px; overflow: hidden; margin: 0.5rem 0;
    }
    .progress-bar-fill {
        height: 100%; border-radius: 10px; background: linear-gradient(90deg, #667eea, #764ba2);
        transition: width 0.3s; text-align: center; color: white; font-size: 0.75rem; line-height: 20px;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# サイドバー
# ============================================================
st.sidebar.markdown("## 🎯 Match")
st.sidebar.caption("人材マッチングシステム")
st.sidebar.markdown("---")

tabs_def = {
    "candidate_search": "🔍 候補者検索",
    "job_search": "📋 求人検索",
    "interview": "📝 面談分析",
    "progress": "📊 提案管理",
    "data_import": "📦 データ取込",
}

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "candidate_search"

for key, label in tabs_def.items():
    if st.sidebar.button(label, key=f"nav_{key}", use_container_width=True):
        st.session_state["current_page"] = key
        st.rerun()

page = st.session_state["current_page"]

# 統計
st.sidebar.markdown("---")
stats = get_stats()
saved_cands = get_saved_candidates()
jt_stats = get_job_type_stats()
contracted_count = jt_stats.get("contracted", 0)
web_count = jt_stats.get("web", 0)
st.sidebar.caption(f"👤 候補者: {len(saved_cands)}名")
st.sidebar.caption(f"📌 契約中求人: {contracted_count}件 / 🌐 Web掲載: {web_count}件")

# バックグラウンド取得ステータス
_bg = _get_bg_status()
if _bg["status"] == "running":
    st.sidebar.markdown("---")
    st.sidebar.markdown("🔄 **データ取得中**")
    pct = _bg["progress"]
    st.sidebar.markdown(
        f'<div class="progress-bar-bg"><div class="progress-bar-fill" style="width:{pct}%">{pct}%</div></div>',
        unsafe_allow_html=True
    )
    st.sidebar.caption(_bg["progress_detail"])
    st.sidebar.caption("他のタブを自由にお使いください")
elif _bg["status"] == "done":
    st.sidebar.markdown("---")
    st.sidebar.success(_bg["result"])
    if st.sidebar.button("確認", key="bg_dismiss"):
        with _BG_LOCK:
            _bg_state["status"] = "idle"
        st.rerun()
elif _bg["status"] == "error":
    st.sidebar.markdown("---")
    st.sidebar.error(_bg["result"])
    if st.sidebar.button("確認", key="bg_dismiss_err"):
        with _BG_LOCK:
            _bg_state["status"] = "idle"
        st.rerun()


# ============================================================
# 共通: AIチャットパネル
# ============================================================
def render_chat_panel(tab_key, context=None):
    """各タブの下部にAIチャットパネルを表示"""
    st.markdown("---")
    st.markdown("### 🤖 AIアシスタント")

    # プリセットボタン
    presets = get_app_setting("ai_presets", {})
    tab_presets = presets.get(tab_key, [])
    if tab_presets:
        preset_cols = st.columns(min(len(tab_presets), 3))
        for pi, preset in enumerate(tab_presets[:3]):
            if preset_cols[pi].button(preset, key=f"chat_preset_{tab_key}_{pi}"):
                _handle_chat(tab_key, preset, context)

    # チャット履歴表示
    history = get_chat_history(tab_key, limit=20)
    if history:
        for msg in history:
            role_cls = "chat-user" if msg["role"] == "user" else "chat-ai"
            avatar = "👤" if msg["role"] == "user" else "🤖"
            content_escaped = esc(msg["content"]).replace("\n", "<br>")
            st.markdown(f'<div class="chat-msg {role_cls}">{avatar} {content_escaped}</div>',
                        unsafe_allow_html=True)

    # 入力
    chat_col1, chat_col2 = st.columns([5, 1])
    user_input = chat_col1.text_input(
        "メッセージを入力",
        placeholder="質問やリクエストを自由に入力...",
        key=f"chat_input_{tab_key}",
        label_visibility="collapsed",
    )
    if chat_col2.button("送信", key=f"chat_send_{tab_key}", type="primary"):
        if user_input and user_input.strip():
            _handle_chat(tab_key, user_input.strip(), context)

    # クリアボタン
    if history:
        if st.button("🗑️ チャット履歴をクリア", key=f"chat_clear_{tab_key}"):
            clear_chat_history(tab_key)
            st.rerun()


def _handle_chat(tab_key, message, context):
    """チャットメッセージを処理"""
    add_chat_message(tab_key, "user", message)
    response = generate_chat_response(message, context)
    add_chat_message(tab_key, "ai", response)
    st.rerun()


# ============================================================
# 共通: ポップアップ
# ============================================================
@st.dialog("👤 候補者詳細", width="large")
def show_candidate_popup(cand):
    info = cand.get("info", {})
    strengths = cand.get("strengths", [])
    conditions = cand.get("conditions", {})
    mf = evaluate_market_fit(cand)

    h1, h2 = st.columns([3, 1])
    with h1:
        star = "⭐ " if mf["has_star"] else ""
        st.markdown(f"### {star}{cand.get('name', '候補者')}")
        if mf["reason"]:
            st.caption(mf["reason"])
    with h2:
        st.caption(f"登録: {cand.get('created_at', '')[:10]}")

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

    st.markdown("**希望条件**")
    kw_tags = " ".join(f'`{k}`' for k in conditions.get("keywords", []))
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"🔑 {kw_tags or 'なし'}")
    c2.markdown(f"💰 {conditions.get('salary_min', 0)}〜{conditions.get('salary_max', 0)}万円")
    c3.markdown(f"📍 {conditions.get('location', '未指定')}")

    st.markdown("**Market Fit 5軸**")
    ax_cols = st.columns(5)
    for i, axis in enumerate(MARKET_FIT_AXES):
        val = mf["axes"].get(axis["id"], "neutral")
        icon = "🟢" if val == "positive" else ("🟡" if val == "neutral" else "🔴")
        ax_cols[i].markdown(f"{icon} {axis['label'].split('（')[0]}")

    sheets = get_interview_sheets(cand.get("id")) if cand.get("id") else []
    if sheets:
        st.markdown(f"**📝 面談シート: {len(sheets)}件**")

    st.markdown("---")
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
    st.markdown(f"### {job.get('title', '不明')}")
    jt = job.get("job_type", "web")
    st.caption("📌 契約中" if jt == "contracted" else "🌐 Web掲載")

    j1, j2 = st.columns(2)
    with j1:
        st.markdown(f"🏢 **{job.get('company', '不明')}**")
        st.markdown(f"📍 {job.get('location', '不明')}")
        st.markdown(f"💰 {job.get('salary', '情報なし')}")
    with j2:
        url = job.get("url", "")
        if url and url.startswith("http"):
            st.link_button("求人ページを開く", url)
        st.caption(f"ソース: {job.get('source', '')} / 更新: {job.get('updated_at', '')[:16]}")

    desc = job.get("description", "")
    if desc:
        with st.expander("説明文", expanded=True):
            st.markdown(desc[:600] + ("..." if len(desc) > 600 else ""))

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
            mf = evaluate_market_fit(c)
            star = "⭐ " if mf["has_star"] else ""
            st.markdown(f"{_score_badge(sc)} **{star}{esc(c.get('name', ''))}**", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("**🤖 AIアシスタント**")
    jurl = job.get("url", "none")
    if st.button("💡 求人改善提案", key=f"pop_jimp_{jurl[:30]}"):
        st.session_state["pop_job_ai"] = generate_job_improvements(job)
    if st.session_state.get("pop_job_ai"):
        st.markdown(st.session_state["pop_job_ai"])


# ############################################################
# タブ1: 候補者検索（プロトタイプ準拠）
# ############################################################
if page == "candidate_search":
    st.markdown("## 🔍 候補者検索")
    st.caption("候補者の条件にマッチする求人を検索します")

    col_sel, col_upload = st.columns([2, 1])
    with col_sel:
        if saved_cands:
            cand_options = ["-- 選択してください --"] + [
                f"{c['name']}（{c.get('created_at','')[:10]}）" for c in saved_cands
            ]
            sel = st.selectbox("保存済み候補者から選択", cand_options, key="cs_select")
            sel_idx = cand_options.index(sel) - 1 if sel != cand_options[0] else -1
        else:
            st.info("候補者が登録されていません。「📦 データ取込」から追加してください。")
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

    active_cand = None
    conditions = None

    if quick_upload and "quick_cand" in st.session_state:
        active_cand = st.session_state["quick_cand"]
        conditions = active_cand.get("conditions", {})
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
        mf_active = evaluate_market_fit(active_cand)
        star_txt = "⭐ " if mf_active["has_star"] else ""
        kw_tags = " ".join(f'`{k}`' for k in conditions.get("keywords", []))
        st.markdown(f"**{star_txt}{active_cand.get('name', '候補者')}** — {kw_tags}")
        if st.button("👤 候補者詳細を開く", key="cs_cand_popup"):
            show_candidate_popup(active_cand)

        with st.expander("⚙️ 検索条件を調整", expanded=False):
            ac1, ac2, ac3, ac4 = st.columns(4)
            salary_min = ac1.number_input("最低年収(万)", value=conditions.get("salary_min", 300), step=10, key="cs_smin")
            salary_max = ac2.number_input("最高年収(万)", value=conditions.get("salary_max", 600), step=10, key="cs_smax")
            age_val = ac3.number_input("年齢", value=max(conditions.get("age", 30), 18), min_value=18, max_value=70, key="cs_age")
            loc_val = ac4.text_input("勤務地", value=conditions.get("location", "大阪"), key="cs_loc")
            kw_str = st.text_area("キーワード（改行区切り）",
                                  value="\n".join(conditions.get("keywords", [])), height=60, key="cs_kw")
            kws = [k.strip() for k in kw_str.split("\n") if k.strip()]
            conditions = {
                "keywords": kws, "location": loc_val,
                "salary_min": salary_min, "salary_max": salary_max,
                "age": age_val, "prefer_kansai": True,
                "extra_keywords": conditions.get("extra_keywords", []),
            }

        st.markdown("---")

        # 求人種別フィルタ
        jt_filter = st.radio("求人種別", ["すべて", "📌 契約中", "🌐 Web掲載"], horizontal=True, key="cs_jt")
        jt_val = None
        if "契約中" in jt_filter:
            jt_val = "contracted"
        elif "Web" in jt_filter:
            jt_val = "web"

        if stats["total_jobs"] == 0:
            st.markdown('<div class="empty-state"><h3>求人データがまだありません</h3>'
                        '<p>「📦 データ取込」からデータを追加してください</p></div>',
                        unsafe_allow_html=True)
        else:
            search_query = " ".join(conditions.get("keywords", []) + conditions.get("extra_keywords", [])[:3])
            matched_jobs = search_jobs(search_query) if search_query.strip() else get_all_jobs(limit=300, job_type=jt_val)
            if jt_val and matched_jobs:
                matched_jobs = [j for j in matched_jobs if j.get("job_type", "web") == jt_val]

            if matched_jobs:
                ranked = rank_jobs(matched_jobs, conditions)
                fc1, fc2, fc3 = st.columns(3)
                min_score = fc1.slider("最低スコア", 0, 100, 0, key="cs_fscore")
                sort_opt = fc2.selectbox("並べ替え", ["マッチ度順", "年収順（高→低）", "新着順"], key="cs_sort")
                require_salary = fc3.checkbox("年収あり", value=False, key="cs_fsal")

                filtered = [j for j in ranked if j.get("score", 0) >= min_score
                            and (not require_salary or j.get("salary", "").strip())]
                if sort_opt == "年収順（高→低）":
                    def _sk(j):
                        nums = re.findall(r'(\d+)', j.get("salary", "").replace(",", ""))
                        return max([int(n) for n in nums]) if nums else 0
                    filtered.sort(key=_sk, reverse=True)
                elif sort_opt == "新着順":
                    filtered.sort(key=lambda x: x.get("updated_at", ""), reverse=True)

                st.markdown(f"**{len(filtered)}件**マッチ（全{stats['total_jobs']:,}件中）")

                for i, job in enumerate(filtered[:50]):
                    score = job.get("score", 0)
                    jt_icon = "📌" if job.get("job_type") == "contracted" else "🌐"
                    st.markdown(f"""
                    <div class="job-card">
                        {_score_badge(score)} <span style="color:#a0aec0;font-size:0.8rem;">{jt_icon} #{i+1}</span>
                        {_match_bar(score)}
                        <div style="font-size:1.05rem;font-weight:700;color:#1a202c;margin:0.3rem 0;">
                            {esc(job.get('title','不明'))}
                        </div>
                        🏢 <strong>{esc(job.get('company',''))}</strong>
                        &nbsp;|&nbsp; 📍 {esc(job.get('location',''))}
                        &nbsp;|&nbsp; 💰 {esc(job.get('salary',''))}
                        <div style="margin-top:0.3rem;">{_fit_tags(job.get('match_reasons',''))}</div>
                    </div>""", unsafe_allow_html=True)

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

                with st.expander(f"📊 テーブル表示（{len(filtered)}件）"):
                    df = pd.DataFrame([{
                        "順位": i, "スコア": j.get("score", 0),
                        "種別": "契約中" if j.get("job_type") == "contracted" else "Web",
                        "求人タイトル": j.get("title", ""), "企業名": j.get("company", ""),
                        "勤務地": j.get("location", ""), "年収": j.get("salary", ""),
                        "ソース": j.get("source", ""), "URL": j.get("url", ""),
                    } for i, j in enumerate(filtered, 1)])
                    st.dataframe(df, hide_index=True, use_container_width=True)
                    csv_buf = io.StringIO()
                    df.to_csv(csv_buf, index=False, encoding="utf-8-sig")
                    st.download_button("CSV DL", csv_buf.getvalue(), "マッチング結果.csv", "text/csv")
            else:
                st.info("条件に一致する求人がありません。")
    else:
        st.markdown("---")
        st.markdown("#### 手動検索")
        manual_kw = st.text_input("検索キーワード", placeholder="例: Webデザイナー 大阪", key="cs_manual_kw")
        if manual_kw.strip() and stats["total_jobs"] > 0:
            results = search_jobs(manual_kw)
            if results:
                st.markdown(f"**{len(results)}件** ヒット")
                for i, job in enumerate(results[:20]):
                    jt_icon = "📌" if job.get("job_type") == "contracted" else "🌐"
                    st.markdown(f"""
                    <div class="job-card">
                        {jt_icon} <strong>{esc(job.get('title',''))}</strong> - {esc(job.get('company',''))}
                        &nbsp;|&nbsp; 📍 {esc(job.get('location',''))} &nbsp;|&nbsp; 💰 {esc(job.get('salary',''))}
                    </div>""", unsafe_allow_html=True)

    chat_ctx = {"candidate": active_cand if active_cand else None, "tab": "candidateSearch"}
    render_chat_panel("candidateSearch", chat_ctx)


# ############################################################
# タブ2: 求人検索
# ############################################################
elif page == "job_search":
    st.markdown("## 📋 求人検索")
    st.caption("求人にマッチする候補者を探します")

    js_jt = st.radio("求人種別", ["すべて", "📌 契約中", "🌐 Web掲載"], horizontal=True, key="js_jt_filter")
    js_jt_val = None
    if "契約中" in js_jt:
        js_jt_val = "contracted"
    elif "Web" in js_jt:
        js_jt_val = "web"

    job_search_kw = st.text_input("求人を検索", placeholder="職種・企業名・キーワード", key="js_kw")
    if job_search_kw.strip():
        job_results = search_jobs(job_search_kw)
    else:
        job_results = get_all_jobs(limit=100, job_type=js_jt_val)
    if js_jt_val and job_results:
        job_results = [j for j in job_results if j.get("job_type", "web") == js_jt_val]

    selected_job_for_chat = None

    if not job_results:
        st.markdown('<div class="empty-state"><h3>求人がありません</h3></div>', unsafe_allow_html=True)
    elif not saved_cands:
        st.markdown('<div class="empty-state"><h3>候補者が登録されていません</h3></div>', unsafe_allow_html=True)
    else:
        st.markdown(f"**{len(job_results)}件**の求人")
        for i, job in enumerate(job_results[:30]):
            jt_icon = "📌" if job.get("job_type") == "contracted" else "🌐"
            st.markdown(f"""
            <div class="job-card">
                <div style="font-size:1.05rem;font-weight:700;color:#1a202c;">
                    {jt_icon} {esc(job.get('title',''))}
                </div>
                🏢 {esc(job.get('company',''))} &nbsp;|&nbsp; 📍 {esc(job.get('location',''))}
                &nbsp;|&nbsp; 💰 {esc(job.get('salary',''))}
            </div>""", unsafe_allow_html=True)

            jqa1, jqa2 = st.columns(2)
            if jqa1.button("📄 詳細 & マッチ候補者", key=f"js_jpop_{i}"):
                show_job_popup(job, saved_cands)
            url = job.get("url", "")
            if url and url.startswith("http"):
                jqa2.link_button("🌐 求人ページ", url, key=f"js_link_{i}")

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
            if i == 0:
                selected_job_for_chat = job

    chat_ctx = {"job": selected_job_for_chat if job_results else None, "tab": "jobSearch"}
    render_chat_panel("jobSearch", chat_ctx)


# ############################################################
# タブ3: 面談分析
# ############################################################
elif page == "interview":
    st.markdown("## 📝 面談分析")
    st.caption("面談内容をAIが構造化し、候補者特性を抽出します")

    iv_tabs = st.tabs(["✏️ 新規作成", "📋 シート一覧"])

    with iv_tabs[0]:
        if not saved_cands:
            st.markdown('<div class="empty-state"><h3>候補者を先に登録してください</h3></div>',
                        unsafe_allow_html=True)
        else:
            iv_cand_options = [f"{c['name']}（ID:{c['id']}）" for c in saved_cands]
            iv_sel = st.selectbox("対象候補者", iv_cand_options, key="iv_cand_sel")
            iv_sel_idx = iv_cand_options.index(iv_sel) if iv_sel else 0
            iv_cand = saved_cands[iv_sel_idx]

            if st.button("👤 候補者詳細", key="iv_cand_popup"):
                show_candidate_popup(iv_cand)

            st.markdown("---")
            raw_input = st.text_area(
                "面談内容を入力（自由記述）", height=250,
                placeholder="面談で聞き取った内容をそのまま入力してください。",
                key="iv_raw_input",
            )
            tag_input = st.text_input("タグ（カンマ区切り）", placeholder="例: 医療事務, 即日可", key="iv_tags")

            if st.button("面談シートを生成", type="primary", key="iv_generate"):
                if not raw_input.strip():
                    st.error("面談内容を入力してください")
                else:
                    tags = [t.strip() for t in tag_input.split(",") if t.strip()] if tag_input else []
                    auto_patterns = {
                        "即日可": ["即日", "すぐ"], "大阪希望": ["大阪"], "東京希望": ["東京"],
                        "リモート希望": ["リモート", "在宅"], "管理職志向": ["管理職", "マネジメント"],
                        "医療系": ["医療", "クリニック", "病院"], "IT系": ["エンジニア", "IT"],
                    }
                    for tag_name, keywords in auto_patterns.items():
                        if tag_name not in tags and any(kw in raw_input for kw in keywords):
                            tags.append(tag_name)

                    lines = raw_input.strip().split("\n")
                    sections = {"職歴・経験": [], "転職理由・動機": [], "希望条件": [],
                                "スキル・資格": [], "人物像・印象": [], "その他": []}
                    section_kw = {
                        "職歴・経験": ["現職", "前職", "経験", "勤務", "職歴"],
                        "転職理由・動機": ["転職理由", "動機", "退職"],
                        "希望条件": ["希望", "年収", "勤務地", "条件"],
                        "スキル・資格": ["スキル", "資格", "免許"],
                        "人物像・印象": ["人柄", "印象", "性格"],
                    }
                    cur_sec = "その他"
                    for line in lines:
                        lc = line.strip()
                        if not lc:
                            continue
                        for sec, kws in section_kw.items():
                            if any(kw in lc for kw in kws):
                                cur_sec = sec
                                break
                        sections[cur_sec].append(lc)

                    parts = [f"# 面談シート: {iv_cand.get('name', '候補者')}",
                             f"作成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}", ""]
                    for sec_name, sec_lines in sections.items():
                        if sec_lines:
                            parts.append(f"## {sec_name}")
                            for sl in sec_lines:
                                parts.append(f"- {sl.lstrip('-').lstrip('・').strip()}")
                            parts.append("")
                    if tags:
                        parts.append("## タグ")
                        parts.append(", ".join(f"#{t}" for t in tags))
                    sheet_content = "\n".join(parts)

                    ai_result = generate_interview_analysis(raw_input, iv_cand)
                    for at in ai_result.get("tags", []):
                        tc = at.lstrip("#")
                        if tc not in tags:
                            tags.append(tc)
                    sheet_content += "\n\n" + ai_result.get("report", "")

                    sheet_id = save_interview_sheet(iv_cand["id"], raw_input, sheet_content, tags)
                    st.success(f"面談シートを保存しました（ID: {sheet_id}）")
                    st.markdown("---")
                    st.markdown("### 生成されたシート")
                    st.markdown(sheet_content)
                    if tags:
                        tag_html = " ".join(f'<span class="fit-tag">#{esc(t)}</span>' for t in tags)
                        st.markdown(tag_html, unsafe_allow_html=True)

    with iv_tabs[1]:
        all_sheets = get_interview_sheets()
        if not all_sheets:
            st.markdown('<div class="empty-state"><h3>面談シートがまだありません</h3></div>',
                        unsafe_allow_html=True)
        else:
            cand_map = {c["id"]: c["name"] for c in saved_cands}
            if saved_cands:
                iv_filter = st.selectbox("候補者でフィルタ", ["全員"] + [c["name"] for c in saved_cands], key="iv_filter")
            else:
                iv_filter = "全員"

            for sheet in all_sheets:
                cand_name = cand_map.get(sheet["candidate_id"], f"#{sheet['candidate_id']}")
                if iv_filter != "全員" and cand_name != iv_filter:
                    continue
                tags = sheet.get("tags", [])
                tag_html = " ".join(f'<span class="fit-tag">#{esc(t)}</span>' for t in tags)
                created = sheet.get("created_at", "")[:16].replace("T", " ")
                st.markdown(f"""
                <div class="cand-card">
                    <strong>{esc(cand_name)}</strong>
                    <span style="color:#a0aec0;margin-left:1rem;font-size:0.8rem;">{created}</span>
                    <div style="margin-top:0.3rem;">{tag_html}</div>
                </div>""", unsafe_allow_html=True)
                with st.expander(f"📋 詳細 - {cand_name}"):
                    st.markdown(sheet.get("sheet_content", ""))
                    if st.button("🗑️ 削除", key=f"iv_del_{sheet['id']}"):
                        delete_interview_sheet(sheet["id"])
                        st.rerun()

    render_chat_panel("interviewSheet", {"tab": "interviewSheet"})


# ############################################################
# タブ4: 提案管理
# ############################################################
elif page == "progress":
    st.markdown("## 📊 提案管理")
    st.caption("候補者×求人の提案状況を管理します")

    proposals = get_proposals()
    cand_map = {c["id"]: c for c in saved_cands}

    status_counts = {s: 0 for s in PROPOSAL_STATUSES}
    for p in proposals:
        s = p.get("status", "提案済み")
        if s in status_counts:
            status_counts[s] += 1

    if proposals:
        colors = ["#667eea", "#4facfe", "#43e97b", "#38f9d7", "#f9d423", "#ff6b6b", "#ee5a24", "#6c5ce7"]
        pipeline_html = '<div style="display:flex;gap:4px;margin-bottom:1rem;">'
        for i, (status, count) in enumerate(status_counts.items()):
            color = colors[i % len(colors)]
            pipeline_html += f'<div style="flex:1;text-align:center;padding:0.5rem;border-radius:8px;background:{color}20;border:2px solid {color};"><div style="font-size:1.5rem;font-weight:800;color:{color};">{count}</div><div style="font-size:0.7rem;color:#4a5568;">{esc(status)}</div></div>'
        pipeline_html += '</div>'
        st.markdown(pipeline_html, unsafe_allow_html=True)

    with st.expander("➕ 新しい提案を登録"):
        if saved_cands and stats["total_jobs"] > 0:
            pr_cand_opts = [f"{c['name']}（ID:{c['id']}）" for c in saved_cands]
            pr_cand_sel = st.selectbox("候補者", pr_cand_opts, key="pr_cand")
            pr_cand_idx = pr_cand_opts.index(pr_cand_sel)
            pr_cand_id = saved_cands[pr_cand_idx]["id"]
            pr_job_kw = st.text_input("求人を検索", key="pr_job_kw")
            pr_jobs = search_jobs(pr_job_kw) if pr_job_kw.strip() else get_all_jobs(limit=50)
            if pr_jobs:
                pr_job_opts = [f"{j.get('title','不明')} - {j.get('company','')}" for j in pr_jobs[:30]]
                pr_job_sel = st.selectbox("求人", pr_job_opts, key="pr_job")
                pr_job = pr_jobs[pr_job_opts.index(pr_job_sel)]
                pr_memo = st.text_input("メモ", key="pr_memo")
                if st.button("提案を登録", type="primary", key="pr_save"):
                    save_proposal(pr_cand_id, pr_job.get("url", ""), "提案済み", pr_memo)
                    st.success("登録しました")
                    st.rerun()
        else:
            st.info("候補者と求人の両方が必要です")

    st.markdown("---")

    if not proposals:
        st.markdown('<div class="empty-state"><h3>提案がまだありません</h3></div>', unsafe_allow_html=True)
    else:
        pf1, pf2 = st.columns(2)
        pr_filter_status = pf1.multiselect("ステータス", PROPOSAL_STATUSES, default=PROPOSAL_STATUSES, key="pr_fstatus")
        if saved_cands:
            pr_filter_cand = pf2.selectbox("候補者", ["全員"] + [c["name"] for c in saved_cands], key="pr_fcand")
        else:
            pr_filter_cand = "全員"

        filtered_proposals = []
        for p in proposals:
            if p.get("status", "") not in pr_filter_status:
                continue
            cn = cand_map.get(p["candidate_id"], {}).get("name", f"#{p['candidate_id']}")
            if pr_filter_cand != "全員" and cn != pr_filter_cand:
                continue
            filtered_proposals.append(p)

        st.markdown(f"**{len(filtered_proposals)}件**")

        for p in filtered_proposals:
            cand_info = cand_map.get(p["candidate_id"], {})
            cand_name = cand_info.get("name", f"#{p['candidate_id']}")
            status = p.get("status", "提案済み")
            updated = p.get("updated_at", "")[:16].replace("T", " ")

            from cache_manager import _get_conn
            job_info = None
            job_url = p.get("job_url", "")
            if job_url:
                conn = _get_conn()
                row = conn.execute("SELECT * FROM jobs WHERE url = ?", (job_url,)).fetchone()
                if row:
                    job_info = dict(row)
            job_title = job_info.get("title", "不明") if job_info else "不明"

            s_colors = {"提案済み": "#667eea", "カジュアル面談": "#4facfe", "一次面接": "#43e97b",
                        "二次面接": "#38f9d7", "三次面接": "#f9d423", "内定": "#ff6b6b",
                        "内定承諾": "#ee5a24", "決定": "#6c5ce7"}
            s_color = s_colors.get(status, "#667eea")

            st.markdown(f"""
            <div class="job-card">
                <span style="display:inline-block;padding:0.2rem 0.7rem;border-radius:12px;background:{s_color}20;color:{s_color};font-weight:700;font-size:0.85rem;">{esc(status)}</span>
                <span style="color:#a0aec0;font-size:0.8rem;margin-left:0.5rem;">{updated}</span>
                <div style="margin-top:0.4rem;">
                    <strong>👤 {esc(cand_name)}</strong> → <strong>📋 {esc(job_title)}</strong>
                </div>
                {f'<div style="color:#718096;font-size:0.85rem;">📝 {esc(p.get("memo",""))}</div>' if p.get("memo") else ''}
            </div>""", unsafe_allow_html=True)

            with st.expander(f"⚙️ 操作 - {cand_name}"):
                pu1, pu2 = st.columns(2)
                current_idx = PROPOSAL_STATUSES.index(status) if status in PROPOSAL_STATUSES else 0
                new_status = pu1.selectbox("ステータス", PROPOSAL_STATUSES, index=current_idx, key=f"pr_ns_{p['id']}")
                new_memo = pu2.text_input("メモ", value=p.get("memo", ""), key=f"pr_nm_{p['id']}")
                new_next = st.text_input("次のアクション", value=p.get("next_action", ""), key=f"pr_na_{p['id']}")

                uc1, uc2, uc3 = st.columns(3)
                if uc1.button("更新", type="primary", key=f"pr_upd_{p['id']}"):
                    update_proposal_status(p["id"], new_status, new_memo, new_next)
                    st.success("更新しました")
                    st.rerun()
                if uc2.button("🗑️ 削除", key=f"pr_del_{p['id']}"):
                    delete_proposal(p["id"])
                    st.rerun()
                if uc3.button("🤖 進捗分析", key=f"pr_ai_{p['id']}"):
                    analysis = generate_progress_analysis({**p, "job_title": job_title}, cand_info or None)
                    st.session_state[f"pr_analysis_{p['id']}"] = analysis
                if st.session_state.get(f"pr_analysis_{p['id']}"):
                    st.markdown(st.session_state[f"pr_analysis_{p['id']}"])

    render_chat_panel("proposals", {"tab": "proposals"})


# ############################################################
# タブ5: データ取込（新規追加機能）
# ############################################################
elif page == "data_import":
    st.markdown("## 📦 データ取込")
    st.caption("求人データの取り込み・候補者の登録を行います")

    dc1, dc2, dc3 = st.columns(3)
    dc1.markdown(f'<div class="stat-card"><div class="stat-num">{contracted_count}</div>'
                 f'<div class="stat-label">📌 契約中求人</div></div>', unsafe_allow_html=True)
    dc2.markdown(f'<div class="stat-card-blue"><div class="stat-num">{web_count}</div>'
                 f'<div class="stat-label">🌐 Web掲載求人</div></div>', unsafe_allow_html=True)
    dc3.markdown(f'<div class="stat-card-green"><div class="stat-num">{len(saved_cands)}</div>'
                 f'<div class="stat-label">👤 候補者数</div></div>', unsafe_allow_html=True)

    st.markdown("")
    dm_tabs = st.tabs(["🌐 Web求人取得", "📌 契約中求人登録", "👤 候補者登録", "🗄️ データ管理"])

    # --- Web求人取得 ---
    with dm_tabs[0]:
        st.markdown("### Web求人の自動取得")
        st.caption("求人サイトからキーワード検索で自動取得。バックグラウンドで実行されます。")

        st.markdown("**取得ソース:**")
        enabled_sources = []
        src_cols = st.columns(len(SOURCE_NAMES))
        for i, name in enumerate(SOURCE_NAMES):
            if src_cols[i].checkbox(name, value=True, key=f"dm_src_{name}"):
                enabled_sources.append(name)

        registered_kws = get_enabled_keywords()
        if registered_kws:
            st.markdown("**キーワード:** " + ", ".join([f"「{kw['keyword']}」" for kw in registered_kws[:10]]))

        with st.expander("🔑 キーワード管理"):
            with st.form("dm_add_kw"):
                kc1, kc2 = st.columns([3, 1])
                new_kw = kc1.text_input("キーワード", placeholder="例: Webデザイナー")
                new_kw_loc = kc2.text_input("勤務地", value="大阪", key="dm_kw_loc")
                if st.form_submit_button("追加"):
                    if new_kw.strip():
                        if add_keyword(new_kw.strip(), new_kw_loc.strip()):
                            st.success(f"「{new_kw}」を追加")
                            st.rerun()
            all_kws = get_keywords()
            for kw in all_kws:
                kc1, kc2 = st.columns([5, 1])
                kc1.markdown(f"{'✅' if kw['enabled'] else '⏸️'} **{kw['keyword']}**（{kw.get('location','')}）")
                if kc2.button("削除", key=f"dm_del_kw_{kw['id']}"):
                    remove_keyword(kw["id"])
                    st.rerun()

        fetch_loc = st.text_input("取得勤務地", value="大阪", key="dm_fetch_loc")

        bg = _get_bg_status()
        if bg["status"] == "running":
            pct = bg["progress"]
            st.markdown(
                f'<div class="progress-bar-bg"><div class="progress-bar-fill" style="width:{pct}%">{pct}%</div></div>',
                unsafe_allow_html=True
            )
            st.caption(bg["progress_detail"])
            st.info("🔄 取得中... 他のタブで作業を続けられます。")
        else:
            if st.button("🔄 Web求人を自動取得", type="primary", use_container_width=True, key="dm_fetch"):
                kw_list = [kw["keyword"] for kw in registered_kws]
                if not kw_list:
                    st.error("キーワードを登録してください")
                elif not enabled_sources:
                    st.error("ソースを1つ以上選択してください")
                else:
                    if start_bg_fetch(kw_list, fetch_loc, enabled_sources):
                        st.info("🔄 バックグラウンドで取得を開始しました。")
                        st.rerun()

        logs = get_collection_logs(5)
        if logs:
            with st.expander("取得ログ"):
                for log in logs:
                    ran = log.get("ran_at", "")[:16].replace("T", " ")
                    st.caption(f"{ran} | {log.get('sources','')} | 取得:{log.get('jobs_found',0)}/保存:{log.get('jobs_saved',0)} | {log.get('duration_sec',0):.1f}秒")

    # --- 契約中求人登録 ---
    with dm_tabs[1]:
        st.markdown("### 契約中求人の登録")
        st.caption("現在契約中の求人を登録します。「📌 契約中」として区別して管理されます。")

        imp = st.radio("登録方法", ["CSV/Excel", "1件ずつ登録"], horizontal=True, key="dm_c_imp")
        if imp == "CSV/Excel":
            st.caption("カラム: 求人タイトル, 企業名, 勤務地, 年収, URL, 説明, ソース")
            uploaded = st.file_uploader("ファイル", type=["csv", "xlsx", "xls"], key="dm_c_csv")
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
                        if st.button("契約中求人として登録", type="primary", key="dm_c_imp_btn"):
                            saved = save_jobs(jobs, job_type="contracted")
                            st.success(f"{saved}件を契約中求人として保存")
                            st.rerun()
                except Exception as e:
                    st.error(f"エラー: {e}")
        else:
            with st.form("dm_c_single"):
                j_title = st.text_input("求人タイトル *")
                jc1, jc2 = st.columns(2)
                j_company = jc1.text_input("企業名")
                j_url = jc2.text_input("求人URL")
                jc3, jc4 = st.columns(2)
                j_location = jc3.text_input("勤務地")
                j_salary = jc4.text_input("年収")
                j_desc = st.text_area("説明", height=60)
                if st.form_submit_button("契約中求人として登録", type="primary"):
                    if j_title:
                        save_jobs([{
                            "title": j_title, "company": j_company,
                            "url": j_url or f"contracted_{datetime.now().isoformat()}",
                            "location": j_location, "salary": j_salary,
                            "description": j_desc, "source": "手動登録",
                        }], job_type="contracted")
                        st.success("契約中求人として登録しました")
                        st.rerun()

        st.markdown("---")
        st.markdown("### 登録済み契約中求人")
        contracted_jobs = get_all_jobs(limit=100, job_type="contracted")
        if contracted_jobs:
            for cj in contracted_jobs:
                st.markdown(f"""
                <div class="job-card">
                    📌 <strong>{esc(cj.get('title',''))}</strong>
                    &nbsp;|&nbsp; 🏢 {esc(cj.get('company',''))}
                    &nbsp;|&nbsp; 📍 {esc(cj.get('location',''))}
                    &nbsp;|&nbsp; 💰 {esc(cj.get('salary',''))}
                </div>""", unsafe_allow_html=True)
        else:
            st.info("契約中求人はまだありません")

    # --- 候補者登録 ---
    with dm_tabs[2]:
        st.markdown("### 候補者を登録")
        ext_list = list(SUPPORTED_EXTENSIONS.keys())
        st.caption(f"対応形式: {', '.join(ext_list)}")

        up_file = st.file_uploader("ファイルをアップロード", type=[e.lstrip(".") for e in ext_list], key="cm_upload")
        if up_file:
            file_bytes = up_file.read()
            cand_data = load_candidate_upload(file_bytes, up_file.name)
            if cand_data:
                st.success("読み取り完了")
                info = cand_data.get("info", {})
                conds = cand_data.get("conditions", {})
                ic1, ic2 = st.columns(2)
                with ic1:
                    st.markdown("**基本情報**")
                    for k, v in info.items():
                        st.markdown(f"- {k}: {v}")
                with ic2:
                    st.markdown("**キーワード**")
                    st.markdown(", ".join(f'`{k}`' for k in conds.get("keywords", [])) or "なし")
                save_name = st.text_input("候補者名", value=up_file.name.rsplit(".", 1)[0], key="cm_name")
                if st.button("保存", type="primary", key="cm_save"):
                    save_candidate(save_name, info, cand_data.get("strengths", []), conds)
                    st.success(f"「{save_name}」を保存しました")
                    st.rerun()

        csv_cands = load_all_candidates()
        if csv_cands:
            st.markdown("---")
            st.markdown("### CSVから一括取り込み")
            if st.button("CSVの候補者をすべて保存", key="cm_bulk"):
                added = 0
                for c in csv_cands:
                    save_candidate(c.get("display_name", "候補者"), c.get("info", {}),
                                   c.get("strengths", []), c.get("conditions", {}))
                    added += 1
                st.success(f"{added}名を保存しました")
                st.rerun()

        st.markdown("---")
        st.markdown("### 保存済み候補者")
        if saved_cands:
            for cand in saved_cands:
                mf = evaluate_market_fit(cand)
                star_html = " ⭐" if mf["has_star"] else ""
                st.markdown(f"""
                <div class="cand-card">
                    <strong>{esc(cand.get('name',''))}</strong>{star_html}
                    <span style="color:#a0aec0;margin-left:1rem;font-size:0.8rem;">{esc(cand.get('created_at','')[:10])}</span>
                </div>""", unsafe_allow_html=True)
                cm_c1, cm_c2 = st.columns([1, 1])
                if cm_c1.button("👤 詳細", key=f"cm_pop_{cand['id']}"):
                    show_candidate_popup(cand)
                if cm_c2.button("🗑️ 削除", key=f"cm_del_{cand['id']}"):
                    delete_candidate(cand["id"])
                    st.rerun()
        else:
            st.info("候補者がまだ登録されていません")

    # --- データ管理 ---
    with dm_tabs[3]:
        st.markdown("### データ管理")
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

        with st.expander("⚙️ 設定"):
            st.markdown("**スコア重み**")
            defaults_w = {"job": 35, "skill": 35, "soft": 20, "conditions": 10}
            saved_w = get_app_setting("score_weights", defaults_w)
            sw1, sw2 = st.columns(2)
            w_job = sw1.number_input("職種一致度", 0, 100, saved_w.get("job", 35), step=5, key="sw_job")
            w_skill = sw2.number_input("スキル一致度", 0, 100, saved_w.get("skill", 35), step=5, key="sw_skill")
            sw3, sw4 = st.columns(2)
            w_soft = sw3.number_input("ソフトスキル", 0, 100, saved_w.get("soft", 20), step=5, key="sw_soft")
            w_cond = sw4.number_input("条件一致度", 0, 100, saved_w.get("conditions", 10), step=5, key="sw_cond")
            if st.button("重みを保存", key="sw_save"):
                set_app_setting("score_weights", {"job": w_job, "skill": w_skill, "soft": w_soft, "conditions": w_cond})
                st.success("保存しました")

            st.markdown("**ラベル閾値**")
            defaults_l = {"recommended": 85, "good": 70}
            saved_l = get_app_setting("label_thresholds", defaults_l)
            lt1, lt2 = st.columns(2)
            th_rec = lt1.number_input("かなり相性が良い", 50, 100, saved_l.get("recommended", 85), step=5, key="lt_rec")
            th_good = lt2.number_input("相性が良い", 30, 100, saved_l.get("good", 70), step=5, key="lt_good")
            if st.button("閾値を保存", key="lt_save"):
                set_app_setting("label_thresholds", {"recommended": th_rec, "good": th_good})
                st.success("保存しました")
