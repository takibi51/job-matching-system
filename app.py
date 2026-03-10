"""
求人検索マッチングシステム v3
- 候補者CSV自動読み取り → 即座にDBマッチング
- 3タブ構成: マッチング結果 / データ管理 / 検索リンク
- SQLite + FTS5 による高速全文検索
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
from scorer import rank_jobs, generate_search_queries
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
    page_title="求人マッチングシステム",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .main-header { font-size: 1.8rem; font-weight: 700; color: #1F4E79; margin-bottom: 0.3rem; }
    .sub-header { color: #666; font-size: 0.9rem; margin-bottom: 1rem; }
    .stat-box {
        background: #f0f4f8; border-radius: 8px; padding: 0.8rem 1rem;
        text-align: center; border: 1px solid #dce3eb;
    }
    .stat-num { font-size: 1.6rem; font-weight: 700; color: #1F4E79; }
    .stat-label { font-size: 0.8rem; color: #666; }
    .score-high { color: #27ae60; font-weight: bold; }
    .score-mid { color: #f39c12; font-weight: bold; }
    .score-low { color: #e74c3c; font-weight: bold; }
    .job-card {
        border: 1px solid #ddd; border-radius: 8px;
        padding: 1rem; margin-bottom: 0.7rem; background: #fafafa;
        transition: border-color 0.2s;
    }
    .job-card:hover { border-color: #4472C4; }
    .fit-reason { color: #2c7a2c; font-size: 0.88rem; margin-top: 0.3rem; }
    .job-summary { color: #555; font-size: 0.85rem; margin-top: 0.2rem; }
    .search-link-btn {
        display: inline-block; padding: 0.6rem 1.2rem; margin: 0.3rem;
        border-radius: 6px; background: #EBF0FA; border: 1px solid #4472C4;
        text-decoration: none; color: #1F4E79; font-weight: 600;
    }
    .search-link-btn:hover { background: #D6E4F0; }
    .empty-state {
        text-align: center; padding: 2rem; color: #888;
        border: 2px dashed #ddd; border-radius: 12px; margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def esc(text):
    """XSS対策用HTMLエスケープ"""
    return html.escape(str(text)) if text else ""


# ============================================================
# サイドバー: 候補者選択 & 条件設定
# ============================================================
st.sidebar.markdown("## 候補者選択")

candidates = load_all_candidates()
saved_cands = get_saved_candidates()
_input_options = ["保存済み候補者", "ファイルアップロード", "候補者CSVから選択", "手動入力"]
# デフォルトは保存済みがあれば保存済み、なければ手動入力
_default_idx = 0 if saved_cands else (2 if candidates else 3)
input_mode = st.sidebar.radio(
    "入力方法", _input_options, index=_default_idx,
    label_visibility="collapsed",
)

# --- 共通: 候補者データから条件を表示するヘルパー ---
def _show_candidate_sidebar(cand_data):
    """候補者データからサイドバーの条件入力欄を表示し、conditionsを返す"""
    cond = cand_data["conditions"]
    info = cand_data.get("info", {})
    strengths = cand_data.get("strengths", [])

    with st.sidebar.expander("候補者情報（個人情報除外済み）", expanded=True):
        if info:
            for key, val in info.items():
                st.caption(f"**{key}**: {val}")
        if strengths:
            st.caption("**主な強み:**")
            for name, detail in strengths[:5]:
                label = f"- {name}"
                if detail:
                    label += f": {detail[:40]}..."
                st.caption(label)

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 検索条件")
    kw_str = st.sidebar.text_area(
        "キーワード（改行区切り）",
        value="\n".join(cond.get("keywords", [])), height=80,
    )
    kws = [k.strip() for k in kw_str.split("\n") if k.strip()]

    ex_str = st.sidebar.text_area(
        "追加キーワード",
        value="\n".join(cond.get("extra_keywords", [])), height=60,
    )
    ex_kws = [k.strip() for k in ex_str.split("\n") if k.strip()]

    c1, c2 = st.sidebar.columns(2)
    s_min = c1.number_input("最低年収(万)", value=cond.get("salary_min", 300), step=10)
    s_max = c2.number_input("最高年収(万)", value=cond.get("salary_max", 600), step=10)
    a = st.sidebar.number_input("年齢", value=max(cond.get("age", 30), 18), min_value=18, max_value=70)
    loc = st.sidebar.text_input("希望勤務地", value=cond.get("location", "大阪"))
    kansai = st.sidebar.checkbox("関西優先", value=cond.get("prefer_kansai", True))

    return kws, ex_kws, s_min, s_max, a, loc, kansai


if input_mode == "保存済み候補者" and saved_cands:
    cand_names = [f"{c['name']}（{c['created_at'][:10]}）" for c in saved_cands]
    sel_idx = st.sidebar.selectbox(
        "候補者を選択", range(len(cand_names)),
        format_func=lambda i: cand_names[i],
    )
    selected_saved = saved_cands[sel_idx]
    # 候補者データを _show_candidate_sidebar 形式に変換
    _saved_data = {
        "conditions": selected_saved["conditions"],
        "info": selected_saved["info"],
        "strengths": [(s[0], s[1]) if isinstance(s, list) else (s, "") for s in selected_saved.get("strengths", [])],
    }
    keywords, extra_keywords, salary_min, salary_max, age, location, prefer_kansai = \
        _show_candidate_sidebar(_saved_data)

    # 削除ボタン
    if st.sidebar.button("この候補者を削除", key="del_saved_cand"):
        delete_candidate(selected_saved["id"])
        st.sidebar.success("削除しました")
        st.rerun()

elif input_mode == "保存済み候補者" and not saved_cands:
    st.sidebar.info("保存済みの候補者がいません。ファイルアップロードまたは手動入力で候補者を追加してください。")
    # 手動入力にフォールバック
    st.sidebar.markdown("### 検索条件")
    keywords_str = st.sidebar.text_area("キーワード", value="Webデザイナー\nLP制作", height=80, key="saved_kw")
    keywords = [k.strip() for k in keywords_str.split("\n") if k.strip()]
    extra_str = st.sidebar.text_area("追加キーワード", value="", height=60, key="saved_ex")
    extra_keywords = [k.strip() for k in extra_str.split("\n") if k.strip()]
    col1, col2 = st.sidebar.columns(2)
    salary_min = col1.number_input("最低年収(万)", value=300, step=10, key="saved_smin")
    salary_max = col2.number_input("最高年収(万)", value=600, step=10, key="saved_smax")
    age = st.sidebar.number_input("年齢", value=30, min_value=18, max_value=70, key="saved_age")
    location = st.sidebar.text_input("希望勤務地", value="大阪", key="saved_loc")
    prefer_kansai = st.sidebar.checkbox("関西優先", value=True, key="saved_kansai")

elif input_mode == "候補者CSVから選択" and candidates:
    candidate_names = [c["display_name"] for c in candidates]
    selected_idx = st.sidebar.selectbox(
        "候補者", range(len(candidate_names)),
        format_func=lambda i: candidate_names[i],
    )
    selected = candidates[selected_idx]
    keywords, extra_keywords, salary_min, salary_max, age, location, prefer_kansai = \
        _show_candidate_sidebar(selected)

elif input_mode == "ファイルアップロード":
    ext_list = list(SUPPORTED_EXTENSIONS.keys())
    st.sidebar.caption(f"対応形式: {', '.join(ext_list)}")
    st.sidebar.caption("個人情報は自動的に除外されます")
    uploaded_candidate = st.sidebar.file_uploader(
        "候補者ファイル",
        type=[e.lstrip(".") for e in ext_list],
        key="candidate_upload",
    )

    if uploaded_candidate is not None:
        # アップロードされたファイルを処理
        if "uploaded_candidate_data" not in st.session_state or \
           st.session_state.get("uploaded_candidate_name") != uploaded_candidate.name:
            file_bytes = uploaded_candidate.read()
            cand_data = load_candidate_upload(file_bytes, uploaded_candidate.name)
            if cand_data:
                st.session_state["uploaded_candidate_data"] = cand_data
                st.session_state["uploaded_candidate_name"] = uploaded_candidate.name
            else:
                st.sidebar.error("ファイルから候補者情報を読み取れませんでした")
                st.session_state.pop("uploaded_candidate_data", None)

        if "uploaded_candidate_data" in st.session_state:
            cand_data = st.session_state["uploaded_candidate_data"]
            st.sidebar.success(f"読み取り完了: {uploaded_candidate.name}")

            # 保存ボタン
            save_name = st.sidebar.text_input(
                "候補者名（保存用）",
                value=uploaded_candidate.name.rsplit(".", 1)[0],
                key="save_cand_name",
            )
            if st.sidebar.button("この候補者を保存", key="save_uploaded_cand"):
                strengths = cand_data.get("strengths", [])
                save_candidate(
                    save_name,
                    cand_data.get("info", {}),
                    strengths,
                    cand_data.get("conditions", {}),
                )
                st.sidebar.success(f"「{save_name}」を保存しました")
                st.rerun()

            keywords, extra_keywords, salary_min, salary_max, age, location, prefer_kansai = \
                _show_candidate_sidebar(cand_data)
        else:
            # 読み取り失敗時は手動入力にフォールバック
            st.sidebar.markdown("### 検索条件")
            keywords_str = st.sidebar.text_area("キーワード", value="", height=80)
            keywords = [k.strip() for k in keywords_str.split("\n") if k.strip()]
            extra_str = st.sidebar.text_area("追加キーワード", value="", height=60)
            extra_keywords = [k.strip() for k in extra_str.split("\n") if k.strip()]
            col1, col2 = st.sidebar.columns(2)
            salary_min = col1.number_input("最低年収(万)", value=300, step=10)
            salary_max = col2.number_input("最高年収(万)", value=600, step=10)
            age = st.sidebar.number_input("年齢", value=30, min_value=18, max_value=70)
            location = st.sidebar.text_input("希望勤務地", value="大阪")
            prefer_kansai = st.sidebar.checkbox("関西優先", value=True)
    else:
        st.sidebar.info("候補者のファイルをアップロードしてください")
        st.sidebar.markdown("### 検索条件")
        keywords_str = st.sidebar.text_area("キーワード", value="", height=80)
        keywords = [k.strip() for k in keywords_str.split("\n") if k.strip()]
        extra_str = st.sidebar.text_area("追加キーワード", value="", height=60)
        extra_keywords = [k.strip() for k in extra_str.split("\n") if k.strip()]
        col1, col2 = st.sidebar.columns(2)
        salary_min = col1.number_input("最低年収(万)", value=300, step=10)
        salary_max = col2.number_input("最高年収(万)", value=600, step=10)
        age = st.sidebar.number_input("年齢", value=30, min_value=18, max_value=70)
        location = st.sidebar.text_input("希望勤務地", value="大阪")
        prefer_kansai = st.sidebar.checkbox("関西優先", value=True)

else:
    # 手動入力モード
    if not candidates:
        st.sidebar.info("候補者CSVが見つかりません。ファイルアップロードまたは手動入力をご利用ください。")

    st.sidebar.markdown("### 検索条件")
    keywords_str = st.sidebar.text_area("キーワード", value="Webデザイナー\nLP制作", height=80)
    keywords = [k.strip() for k in keywords_str.split("\n") if k.strip()]
    extra_str = st.sidebar.text_area("追加キーワード", value="", height=60)
    extra_keywords = [k.strip() for k in extra_str.split("\n") if k.strip()]
    col1, col2 = st.sidebar.columns(2)
    salary_min = col1.number_input("最低年収(万)", value=300, step=10)
    salary_max = col2.number_input("最高年収(万)", value=600, step=10)
    age = st.sidebar.number_input("年齢", value=30, min_value=18, max_value=70)
    location = st.sidebar.text_input("希望勤務地", value="大阪")
    prefer_kansai = st.sidebar.checkbox("関西優先", value=True)

# 検索条件をまとめる
conditions = {
    "keywords": keywords,
    "location": location,
    "salary_min": salary_min,
    "salary_max": salary_max,
    "age": age,
    "prefer_kansai": prefer_kansai,
    "extra_keywords": extra_keywords,
}


# ============================================================
# メインエリア
# ============================================================
st.markdown('<p class="main-header">求人マッチングシステム</p>', unsafe_allow_html=True)

# DB統計サマリー
stats = get_stats()
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f"""<div class="stat-box">
        <div class="stat-num">{stats['total_jobs']:,}</div>
        <div class="stat-label">登録求人数</div>
    </div>""", unsafe_allow_html=True)
with col2:
    source_count = len(stats.get("sources", {}))
    st.markdown(f"""<div class="stat-box">
        <div class="stat-num">{source_count}</div>
        <div class="stat-label">データソース数</div>
    </div>""", unsafe_allow_html=True)
with col3:
    newest = stats.get("newest")
    if newest:
        try:
            dt = datetime.fromisoformat(newest)
            date_str = dt.strftime("%Y/%m/%d %H:%M")
        except (ValueError, TypeError):
            date_str = "不明"
    else:
        date_str = "データなし"
    st.markdown(f"""<div class="stat-box">
        <div class="stat-num" style="font-size:1rem;">{date_str}</div>
        <div class="stat-label">最終更新</div>
    </div>""", unsafe_allow_html=True)

st.markdown("")

# ============================================================
# 3タブ構成
# ============================================================
tab1, tab2, tab3 = st.tabs(["🔍 マッチング結果", "📦 データ管理", "🌐 検索リンク"])


# ============================================================
# タブ1: マッチング結果
# ============================================================
with tab1:
    if stats["total_jobs"] == 0:
        st.markdown("""
        <div class="empty-state">
            <h3>求人データがまだ登録されていません</h3>
            <p>「📦 データ管理」タブから求人データを追加してください</p>
            <p>追加方法: RSS自動取得 / CSVアップロード / 手動入力</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        # キーワードからDB検索 → スコアリング
        search_query = " ".join(keywords + extra_keywords[:3])

        if search_query.strip():
            matched_jobs = search_jobs(search_query)
        else:
            matched_jobs = get_all_jobs(limit=200)

        if matched_jobs:
            # スコアリング
            ranked = rank_jobs(matched_jobs, conditions)

            # --- フィルタ & ソート ---
            st.markdown("#### フィルタ & ソート")
            fr1, fr2 = st.columns(2)

            with fr1:
                fc1, fc2, fc3 = st.columns(3)
                min_score = fc1.slider("最低スコア", 0, 100, 0, key="f_score")
                sources_list = sorted(set(j.get("source", "") for j in ranked if j.get("source")))
                source_filter = fc2.multiselect("ソース", sources_list, default=sources_list, key="f_src")
                location_filter = fc3.text_input("勤務地絞り込み", "", key="f_loc")

            with fr2:
                fc4, fc5, fc6 = st.columns(3)
                sort_option = fc4.selectbox(
                    "並べ替え",
                    ["スコア順（高→低）", "スコア順（低→高）", "企業名順", "年収順（高→低）", "新着順"],
                    key="f_sort",
                )
                require_salary = fc5.checkbox("年収ありのみ", value=False, key="f_salary")
                require_company = fc6.checkbox("社名ありのみ", value=False, key="f_company")

            # フィルタ適用
            filtered = [
                j for j in ranked
                if j.get("score", 0) >= min_score
                and (not source_filter or j.get("source", "") in source_filter)
                and (not location_filter or location_filter in j.get("location", ""))
                and (not require_salary or (j.get("salary", "").strip() != ""))
                and (not require_company or (j.get("company", "").strip() != ""))
            ]

            # ソート適用
            if sort_option == "スコア順（低→高）":
                filtered.sort(key=lambda x: x.get("score", 0))
            elif sort_option == "企業名順":
                filtered.sort(key=lambda x: x.get("company", ""))
            elif sort_option == "年収順（高→低）":
                def _sal_key(j):
                    nums = re.findall(r'(\d+)', j.get("salary", "").replace(",", ""))
                    return max([int(n) for n in nums]) if nums else 0
                filtered.sort(key=_sal_key, reverse=True)
            elif sort_option == "新着順":
                filtered.sort(key=lambda x: x.get("updated_at", ""), reverse=True)

            # 件数サマリー
            salary_count = sum(1 for j in filtered if j.get("salary", "").strip())
            company_count = sum(1 for j in filtered if j.get("company", "").strip())
            st.markdown(
                f"**{len(filtered)}件** マッチ（全{stats['total_jobs']}件中）"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;💰 年収記載: {salary_count}件"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;🏢 社名記載: {company_count}件"
            )

            # --- カード表示 ---
            for i, job in enumerate(filtered[:30]):
                score = job.get("score", 0)
                sc = "score-high" if score >= 60 else ("score-mid" if score >= 30 else "score-low")

                st.markdown(f"""
                <div class="job-card">
                    <span class="{sc}">スコア {score}</span>
                    &nbsp;|&nbsp; <strong>{i+1}位</strong>
                    &nbsp;|&nbsp; <span style="color:#888">{esc(job.get('source', ''))}</span>
                    <br>
                    <a href="{esc(job.get('url', '#'))}" target="_blank"
                       style="font-size:1.05rem; font-weight:600; color:#1a5276;">
                        {esc(job.get('title', '不明'))}
                    </a>
                    <br>
                    🏢 <strong>{esc(job.get('company', '不明'))}</strong>
                    &nbsp;|&nbsp; 📍 {esc(job.get('location', '不明'))}
                    &nbsp;|&nbsp; 💰 {esc(job.get('salary', '情報なし'))}
                    <div class="job-summary">📋 {esc(job.get('job_summary', ''))}</div>
                    <div class="fit-reason">✅ {esc(job.get('fit_reason', ''))}</div>
                </div>
                """, unsafe_allow_html=True)

            # --- テーブル表示 ---
            if filtered:
                with st.expander(f"全件テーブル表示（{len(filtered)}件）"):
                    df = pd.DataFrame([{
                        "順位": i,
                        "スコア": j.get("score", 0),
                        "求人タイトル": j.get("title", ""),
                        "企業名": j.get("company", ""),
                        "勤務地": j.get("location", ""),
                        "年収": j.get("salary", ""),
                        "フィット理由": j.get("fit_reason", ""),
                        "ソース": j.get("source", ""),
                        "URL": j.get("url", ""),
                    } for i, j in enumerate(filtered, 1)])

                    st.dataframe(
                        df,
                        column_config={
                            "URL": st.column_config.LinkColumn("URL", display_text="開く"),
                            "スコア": st.column_config.ProgressColumn("スコア", min_value=0, max_value=100),
                        },
                        hide_index=True, use_container_width=True,
                    )

                    # ダウンロード
                    c1, c2 = st.columns(2)
                    csv_buf = io.StringIO()
                    df.to_csv(csv_buf, index=False, encoding="utf-8-sig")
                    c1.download_button("CSV", csv_buf.getvalue(), "求人マッチング結果.csv", "text/csv")

                    xls_buf = io.BytesIO()
                    with pd.ExcelWriter(xls_buf, engine="openpyxl") as w:
                        df.to_excel(w, index=False, sheet_name="結果")
                    c2.download_button("Excel", xls_buf.getvalue(), "求人マッチング結果.xlsx",
                                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("現在の検索条件に一致する求人がデータベースにありません。キーワードを調整するか、「データ管理」タブでデータを追加してください。")


# ============================================================
# タブ2: データ管理
# ============================================================
with tab2:
    dm_tab1, dm_tab2, dm_tab3, dm_tab4 = st.tabs([
        "🔄 自動取得", "📝 取得キーワード管理", "📤 手動インポート", "🗄️ データ管理",
    ])

    # --- 自動取得 ---
    with dm_tab1:
        st.markdown("### 各サイトから自動取得")
        st.markdown("登録済みキーワードを使って、複数の求人サイトからデータを自動取得します。")

        # 取得対象ソース選択
        st.markdown("**取得ソース:**")
        enabled_sources = []
        src_cols = st.columns(len(SOURCE_NAMES))
        for i, name in enumerate(SOURCE_NAMES):
            if src_cols[i].checkbox(name, value=True, key=f"src_{name}"):
                enabled_sources.append(name)

        # キーワード表示
        registered_kws = get_enabled_keywords()
        if registered_kws:
            kw_display = ", ".join([f"「{kw['keyword']}」" for kw in registered_kws[:10]])
            st.markdown(f"**使用キーワード:** {kw_display}")
            if len(registered_kws) > 10:
                st.caption(f"他{len(registered_kws)-10}件...")
        else:
            st.info("取得キーワードが未登録です。「取得キーワード管理」タブで追加してください。候補者CSVから自動追加もできます。")

        fetch_loc = st.text_input("取得勤務地", value=location or "大阪", key="fetch_loc")

        if st.button("今すぐ自動取得を実行", type="primary", use_container_width=True):
            kw_list = [kw["keyword"] for kw in registered_kws] if registered_kws else keywords
            if not kw_list:
                st.error("キーワードを登録してください")
            elif not enabled_sources:
                st.error("取得ソースを1つ以上選択してください")
            else:
                import time as _time
                start = _time.time()
                progress = st.empty()
                status = st.empty()

                with st.spinner("各サイトから求人を取得中..."):
                    jobs = fetch_from_all_sources(
                        kw_list, fetch_loc,
                        enabled_sources=enabled_sources,
                        progress_callback=lambda m: progress.text(m),
                    )

                elapsed = _time.time() - start
                if jobs:
                    saved = save_jobs(jobs)
                    add_collection_log(len(kw_list), len(jobs), saved,
                                       ",".join(enabled_sources), elapsed)
                    st.success(f"✅ **{len(jobs)}件**取得 → **{saved}件**保存（{elapsed:.1f}秒）")
                    st.rerun()
                else:
                    add_collection_log(len(kw_list), 0, 0,
                                       ",".join(enabled_sources), elapsed)
                    st.warning("取得できた求人が0件でした。")

        # 取得ログ
        logs = get_collection_logs(5)
        if logs:
            with st.expander("直近の取得ログ"):
                for log in logs:
                    ran = log.get("ran_at", "")[:16].replace("T", " ")
                    st.caption(
                        f"{ran} | {log.get('sources','')} | "
                        f"KW:{log.get('keywords_used',0)} → "
                        f"取得:{log.get('jobs_found',0)} / 保存:{log.get('jobs_saved',0)} | "
                        f"{log.get('duration_sec',0):.1f}秒"
                    )

    # --- キーワード管理 ---
    with dm_tab2:
        st.markdown("### 取得キーワード管理")
        st.markdown("自動取得で使用するキーワードを管理します。12時間ごとの自動更新でもこのキーワードが使われます。")

        # キーワード追加
        with st.form("add_kw"):
            kw_col1, kw_col2 = st.columns([3, 1])
            new_kw = kw_col1.text_input("キーワード", placeholder="例: Webデザイナー")
            new_kw_loc = kw_col2.text_input("勤務地", value="大阪", key="new_kw_loc")
            if st.form_submit_button("追加"):
                if new_kw.strip():
                    if add_keyword(new_kw.strip(), new_kw_loc.strip()):
                        st.success(f"「{new_kw}」を追加しました")
                        st.rerun()
                    else:
                        st.warning("同じキーワードが既に登録されています")

        # 候補者CSVから一括追加
        if st.button("候補者CSVからキーワードを自動追加"):
            added = 0
            for cand in (candidates or []):
                cond_c = cand.get("conditions", {})
                for kw in cond_c.get("keywords", []):
                    if add_keyword(kw, location or "大阪"):
                        added += 1
                for kw in cond_c.get("extra_keywords", [])[:3]:
                    if add_keyword(kw, location or "大阪"):
                        added += 1
            if added > 0:
                st.success(f"✅ {added}件のキーワードを追加しました")
                st.rerun()
            else:
                st.info("新しいキーワードはありませんでした")

        # 一覧表示
        all_kws = get_keywords()
        if all_kws:
            st.markdown(f"**登録済み: {len(all_kws)}件**")
            for kw in all_kws:
                kc1, kc2, kc3 = st.columns([4, 1, 1])
                status_icon = "✅" if kw["enabled"] else "⏸️"
                kc1.markdown(f"{status_icon} **{kw['keyword']}**（{kw.get('location', '')}）")
                if kc2.button("削除", key=f"del_kw_{kw['id']}"):
                    remove_keyword(kw["id"])
                    st.rerun()
        else:
            st.info("キーワードが未登録です。上のフォームから追加するか、「候補者CSVから自動追加」ボタンを押してください。")

    # --- 手動インポート ---
    with dm_tab3:
        st.markdown("### 手動インポート")
        import_method = st.radio(
            "方法", ["CSV/Excelアップロード", "テキスト貼り付け", "1件ずつ登録"],
            horizontal=True, key="import_method",
        )

        if import_method == "CSV/Excelアップロード":
            st.markdown("**対応カラム:** `求人タイトル`, `企業名`, `勤務地`, `年収`, `URL`, `説明`, `ソース`")
            uploaded = st.file_uploader("ファイル", type=["csv", "xlsx", "xls"])
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
                        st.dataframe(pd.DataFrame(jobs).head(5))
                        if st.button("インポート", type="primary", key="csv_import"):
                            saved = save_jobs(jobs)
                            st.success(f"✅ {saved}件保存")
                            st.rerun()
                    else:
                        st.warning("カラム名を確認してください")
                except Exception as e:
                    st.error(f"エラー: {e}")

        elif import_method == "テキスト貼り付け":
            st.markdown("1行1求人、カンマまたはタブ区切り: `タイトル, 企業名, 勤務地, 年収, URL`")
            text = st.text_area("求人データ", height=120, key="text_import")
            if st.button("登録", type="primary", key="text_btn") and text.strip():
                jobs = parse_text_input(text)
                if jobs:
                    saved = save_jobs(jobs)
                    st.success(f"✅ {saved}件保存")
                    st.rerun()

        elif import_method == "1件ずつ登録":
            with st.form("single_job"):
                j_title = st.text_input("求人タイトル *")
                j_company = st.text_input("企業名")
                j_url = st.text_input("求人URL")
                jc1, jc2 = st.columns(2)
                j_location = jc1.text_input("勤務地")
                j_salary = jc2.text_input("年収")
                j_desc = st.text_area("説明", height=60)
                j_source = st.text_input("媒体", value="手動登録")
                if st.form_submit_button("登録", type="primary"):
                    if j_title:
                        save_jobs([{
                            "title": j_title, "company": j_company,
                            "url": j_url or f"manual_{datetime.now().isoformat()}",
                            "location": j_location, "salary": j_salary,
                            "description": j_desc, "source": j_source,
                        }])
                        st.success(f"✅ 登録完了")
                        st.rerun()

    # --- データ管理 ---
    with dm_tab4:
        st.markdown("### 登録データ管理")
        if stats["total_jobs"] > 0:
            if stats.get("sources"):
                st.dataframe(
                    pd.DataFrame([{"ソース": s, "件数": c} for s, c in stats["sources"].items()]),
                    hide_index=True, use_container_width=True,
                )
            mc1, mc2 = st.columns(2)
            if mc1.button("60日以上古いデータを削除"):
                deleted = delete_old_jobs(60)
                st.info(f"{deleted}件削除")
                st.rerun()
            if mc2.button("全データ削除"):
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
# タブ3: 検索リンク
# ============================================================
with tab3:
    st.markdown("### 各求人サイトで直接検索")
    st.markdown("以下のリンクをクリックすると、各サイトの検索結果ページが開きます。見つけた求人は「データ管理」タブから登録できます。")

    search_kw = " ".join(keywords[:3]) if keywords else "求人"
    all_links = generate_search_urls(search_kw, location)

    # ボタン風リンク表示
    link_html = ""
    for link in all_links:
        link_html += f"""
        <a href="{esc(link['url'])}" target="_blank" class="search-link-btn">
            {link['icon']} {esc(link['site'])}
        </a>
        """
    st.markdown(link_html, unsafe_allow_html=True)

    # キーワード別リンク
    st.markdown("---")
    st.markdown("#### キーワード別検索")
    for kw in keywords[:5]:
        with st.expander(f"「{kw}」の検索リンク"):
            kw_links = generate_search_urls(kw, location)
            for link in kw_links:
                st.markdown(f"- [{link['icon']} {link['site']}]({link['url']})")
