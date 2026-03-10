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


def _score_badge(score):
    """スコアに応じたバッジHTML"""
    if score >= 60:
        cls, label = "score-high", "推奨"
    elif score >= 30:
        cls, label = "score-mid", "良好"
    else:
        cls, label = "score-low", "要確認"
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
    "candidates": "👤 候補者管理",
    "data_mgmt": "📦 データ管理",
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
        # --- 候補者情報ポップアップ ---
        with st.expander("📋 候補者詳細", expanded=False):
            info = active_cand.get("info", {})
            strengths = active_cand.get("strengths", [])

            pc1, pc2 = st.columns(2)
            with pc1:
                st.markdown("**基本情報（個人情報除外済み）**")
                for k, v in info.items():
                    st.markdown(f"- **{k}**: {v}")
            with pc2:
                st.markdown("**強み・スキル**")
                for s in strengths[:6]:
                    if isinstance(s, (list, tuple)) and len(s) >= 2:
                        st.markdown(f"- **{s[0]}**: {s[1][:60]}")
                    elif isinstance(s, str):
                        st.markdown(f"- {s}")

            st.markdown("**検索キーワード**")
            kw_tags = " ".join(f'`{k}`' for k in conditions.get("keywords", []))
            st.markdown(kw_tags or "なし")

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

                    # 求人詳細ポップアップ
                    with st.expander(f"📄 詳細を見る - {job.get('title','')[:30]}", expanded=False):
                        dp1, dp2 = st.columns([2, 1])
                        with dp1:
                            st.markdown(f"**求人タイトル**: {job.get('title','')}")
                            st.markdown(f"**企業名**: {job.get('company','不明')}")
                            st.markdown(f"**勤務地**: {job.get('location','不明')}")
                            st.markdown(f"**年収**: {job.get('salary','情報なし')}")
                            st.markdown(f"**ソース**: {job.get('source','')}")
                            desc = job.get('description', '')
                            if desc:
                                st.markdown(f"**説明**: {desc[:300]}{'...' if len(desc)>300 else ''}")
                        with dp2:
                            st.markdown("**マッチ分析**")
                            st.markdown(f"スコア: **{score}点**")
                            st.markdown(f"フィット理由:")
                            for r in job.get("match_reasons", "").split(" / "):
                                if r.strip():
                                    st.markdown(f"- {r}")
                            url = job.get("url", "")
                            if url and url.startswith("http"):
                                st.link_button("求人ページを開く", url)

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

            with st.expander(f"🔍 マッチする候補者を見る - {job.get('title','')[:25]}", expanded=False):
                # 求人詳細
                st.markdown(f"""<div class="popup-section">
                    <div class="popup-label">求人詳細</div>
                    <strong>{esc(job.get('title',''))}</strong><br>
                    🏢 {esc(job.get('company',''))} | 📍 {esc(job.get('location',''))} | 💰 {esc(job.get('salary',''))}<br>
                    {esc(job.get('description','')[:200])}
                </div>""", unsafe_allow_html=True)

                url = job.get("url", "")
                if url and url.startswith("http"):
                    st.link_button("求人ページを開く", url, key=f"js_link_{i}")

                # 各候補者とのマッチスコアを算出
                st.markdown("**マッチする候補者:**")
                cand_scores = []
                for cand in saved_cands:
                    cond = _cand_to_conditions(cand)
                    sc, reasons = score_job(job, cond)
                    cand_scores.append((cand, sc, reasons))

                cand_scores.sort(key=lambda x: x[1], reverse=True)

                for cand, sc, reasons in cand_scores:
                    if sc < 10:
                        continue
                    reason_str = " / ".join(reasons[:3])
                    st.markdown(f"""
                    <div class="cand-card">
                        {_score_badge(sc)}
                        &nbsp; <strong>{esc(cand.get('name','候補者'))}</strong>
                        {_match_bar(sc)}
                        <div style="margin-top:0.3rem;">{_fit_tags(reason_str)}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    # 候補者詳細ポップアップ
                    with st.expander(f"👤 {cand.get('name','')} の詳細", expanded=False):
                        ci = cand.get("info", {})
                        cs = cand.get("strengths", [])
                        cc = cand.get("conditions", {})
                        cp1, cp2 = st.columns(2)
                        with cp1:
                            st.markdown("**基本情報**")
                            for k, v in ci.items():
                                st.markdown(f"- **{k}**: {v}")
                        with cp2:
                            st.markdown("**強み**")
                            for s in cs[:5]:
                                if isinstance(s, (list, tuple)) and len(s) >= 2:
                                    st.markdown(f"- **{s[0]}**: {s[1][:50]}")
                                elif isinstance(s, str):
                                    st.markdown(f"- {s}")
                            st.markdown("**希望条件**")
                            st.markdown(f"- キーワード: {', '.join(cc.get('keywords', []))}")
                            st.markdown(f"- 年収: {cc.get('salary_min',0)}〜{cc.get('salary_max',0)}万")
                            st.markdown(f"- 勤務地: {cc.get('location','')}")

                if not any(sc >= 10 for _, sc, _ in cand_scores):
                    st.caption("マッチする候補者が見つかりませんでした")


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
            st.markdown(f"""
            <div class="cand-card">
                <strong style="font-size:1.05rem;">{esc(cand.get('name','候補者'))}</strong>
                <span style="color:#a0aec0; margin-left:1rem; font-size:0.8rem;">
                    登録: {esc(cand.get('created_at','')[:10])}
                </span>
            </div>
            """, unsafe_allow_html=True)

            with st.expander(f"📋 {cand.get('name','')} の詳細", expanded=False):
                ci = cand.get("info", {})
                cs = cand.get("strengths", [])
                cc = cand.get("conditions", {})

                dp1, dp2 = st.columns(2)
                with dp1:
                    st.markdown("**基本情報（個人情報除外済み）**")
                    for k, v in ci.items():
                        st.markdown(f"- **{k}**: {v}")
                with dp2:
                    st.markdown("**強み**")
                    for s in cs[:6]:
                        if isinstance(s, (list, tuple)) and len(s) >= 2:
                            st.markdown(f"- **{s[0]}**: {s[1][:50]}")
                        elif isinstance(s, str):
                            st.markdown(f"- {s}")
                    st.markdown("**検索条件**")
                    st.markdown(f"- キーワード: {', '.join(cc.get('keywords', []))}")
                    st.markdown(f"- 年収: {cc.get('salary_min',0)}〜{cc.get('salary_max',0)}万")
                    st.markdown(f"- 勤務地: {cc.get('location','')}")

                if st.button("この候補者を削除", key=f"cm_del_{cand['id']}"):
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

        if st.button("今すぐ自動取得を実行", type="primary", use_container_width=True, key="dm_fetch"):
            kw_list = [kw["keyword"] for kw in registered_kws]
            if not kw_list:
                st.error("キーワードを登録してください")
            elif not enabled_sources:
                st.error("ソースを1つ以上選択してください")
            else:
                import time as _time
                start = _time.time()
                with st.spinner("各サイトから求人を取得中..."):
                    jobs = fetch_from_all_sources(kw_list, fetch_loc,
                                                  enabled_sources=enabled_sources)
                elapsed = _time.time() - start
                if jobs:
                    saved = save_jobs(jobs)
                    add_collection_log(len(kw_list), len(jobs), saved,
                                       ",".join(enabled_sources), elapsed)
                    st.success(f"**{len(jobs)}件**取得 → **{saved}件**保存（{elapsed:.1f}秒）")
                    st.rerun()
                else:
                    add_collection_log(len(kw_list), 0, 0, ",".join(enabled_sources), elapsed)
                    st.warning("取得0件でした")

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
