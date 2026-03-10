"""
求人データ自動収集モジュール
- CareerJet（メインソース: 安定して大量取得可能）
- リクルートエージェント（Next.jsデータから取得）
- 求人ボックス（補助ソース）
- CSV/テキスト手動インポート
- 各サイト検索URL生成
"""

import requests
import urllib.parse
import re
import csv
import io
import time
import random
import json
import html as html_mod
from typing import List, Dict, Optional, Callable
from datetime import datetime

# 共通ヘッダー
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
}


def _safe_get(url: str, params: dict = None, timeout: int = 15, headers: dict = None) -> Optional[str]:
    """安全なHTTP GET。失敗時はNone。"""
    try:
        h = headers or _HEADERS.copy()
        resp = requests.get(url, params=params, headers=h, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except requests.RequestException:
        pass
    return None


# ============================================================
# 1. CareerJet（メインソース）
# ============================================================

def fetch_careerjet(keyword: str, location: str = "") -> List[Dict]:
    """CareerJetから求人取得（ソート別に取得して件数を最大化、Indeed含む多数サイトを集約）"""
    jobs = []
    # ソートを変えると異なる求人が返るため、3種類で取得
    for sort in ["relevance", "date", "salary"]:
        text = _safe_get(
            "https://www.careerjet.jp/search/jobs",
            params={"s": keyword, "l": location, "sort": sort},
        )
        if not text:
            continue

        articles = re.findall(r'<article[^>]*>(.*?)</article>', text, re.DOTALL)
        for art in articles:
            job = _parse_careerjet_article(art, location)
            if job:
                jobs.append(job)

        time.sleep(random.uniform(1.0, 2.0))

    return _deduplicate(jobs)


def _parse_careerjet_article(art_html: str, default_location: str = "") -> Optional[Dict]:
    """CareerJetの<article>HTMLから求人データを抽出"""
    job = {}

    # タイトル & URL
    title_m = re.search(r'<h2[^>]*>.*?<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', art_html, re.DOTALL)
    if not title_m:
        return None
    href = title_m.group(1)
    job["url"] = ("https://www.careerjet.jp" + href) if href.startswith("/") else href
    job["title"] = re.sub(r'<[^>]+>', '', title_m.group(2)).strip()
    if not job["title"]:
        return None

    # 企業名
    comp_m = re.search(r'<p[^>]*class="[^"]*company[^"]*"[^>]*>(.*?)</p>', art_html, re.DOTALL)
    job["company"] = re.sub(r'<[^>]+>', '', comp_m.group(1)).strip() if comp_m else ""

    # 勤務地
    loc_m = re.search(r'<ul[^>]*class="[^"]*location[^"]*"[^>]*>(.*?)</ul>', art_html, re.DOTALL)
    job["location"] = re.sub(r'<[^>]+>', '', loc_m.group(1)).strip() if loc_m else default_location

    # 給与
    sal_m = re.search(r'<li[^>]*class="[^"]*salary[^"]*"[^>]*>(.*?)</li>', art_html, re.DOTALL)
    job["salary"] = re.sub(r'<[^>]+>', '', sal_m.group(1)).strip() if sal_m else ""

    # 概要
    desc_m = re.search(r'<div[^>]*class="[^"]*desc[^"]*"[^>]*>(.*?)</div>', art_html, re.DOTALL)
    job["description"] = re.sub(r'<[^>]+>', '', desc_m.group(1)).strip()[:300] if desc_m else ""

    job["source"] = "CareerJet"
    job["pub_date"] = ""
    return job


# ============================================================
# 2. リクルートエージェント（Next.js __NEXT_DATA__ から取得）
# ============================================================

def fetch_recruit_agent(keyword: str, location: str = "") -> List[Dict]:
    """リクルートエージェントから求人取得（__NEXT_DATA__のrecommendedJobs）"""
    text = _safe_get(
        "https://www.r-agent.com/kensaku/",
        params={"keyword": keyword},
        timeout=20,
    )
    if not text:
        return []

    # __NEXT_DATA__ からJSONを抽出
    nd_m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', text, re.DOTALL)
    if not nd_m:
        return []

    jobs = []
    try:
        nd = json.loads(nd_m.group(1))
        recommended = nd.get("props", {}).get("pageProps", {}).get("data", {}).get("recommendedJobs", [])

        for item in recommended:
            title = item.get("title", "").strip()
            jkey = item.get("indeedJobKey", "")
            if not title or not jkey:
                continue

            company = item.get("companyName", "")
            salary = item.get("salary", "")
            work_location = item.get("workLocation", "") or location
            url = f"https://www.r-agent.com/viewjob/{jkey}/"

            jobs.append({
                "title": title[:100],
                "company": company,
                "location": work_location,
                "salary": salary,
                "url": url,
                "description": "",
                "source": "リクルートエージェント",
                "pub_date": "",
            })
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    return _deduplicate(jobs)


# ============================================================
# 3. 求人ボックス（補助ソース）
# ============================================================

def fetch_kyujinbox(keyword: str, location: str = "") -> List[Dict]:
    """求人ボックスから求人取得"""
    query = f"{keyword} {location}".strip() if location else keyword
    text = _safe_get(
        "https://xn--pckua2a7gp15o89zb.com/",
        params={"q": query},
    )
    if not text:
        return []

    jobs = []
    # /jbi/ パスの求人リンクを抽出
    all_links = re.findall(r'<a[^>]+href="(/jbi/[^"]+)"[^>]*>(.*?)</a>', text, re.DOTALL)
    for href, title_html in all_links:
        title = re.sub(r'<[^>]+>', '', title_html).strip()
        # タイトルが短すぎる or 不要なテキストを除外
        if not title or len(title) < 5:
            continue
        url = "https://xn--pckua2a7gp15o89zb.com" + href
        jobs.append({
            "title": title[:100],
            "company": "",
            "location": location or "",
            "salary": "",
            "url": url,
            "description": "",
            "source": "求人ボックス",
            "pub_date": "",
        })

    return _deduplicate(jobs)


# ============================================================
# 統合: 全ソースから自動取得
# ============================================================

# ソース定義
SOURCES = {
    "CareerJet": {"func": fetch_careerjet, "enabled": True, "type": "scrape"},
    "リクルートエージェント": {"func": fetch_recruit_agent, "enabled": True, "type": "scrape"},
    "求人ボックス": {"func": fetch_kyujinbox, "enabled": True, "type": "scrape"},
}

SOURCE_NAMES = list(SOURCES.keys())


def fetch_from_all_sources(keywords: List[str], location: str = "",
                           enabled_sources: List[str] = None,
                           progress_callback: Callable = None) -> List[Dict]:
    """全ソースから求人を自動取得"""
    if enabled_sources is None:
        enabled_sources = [name for name, info in SOURCES.items() if info["enabled"]]

    all_jobs = []
    total_steps = len(keywords) * len(enabled_sources)
    step = 0

    for kw in keywords[:15]:  # 最大15キーワード
        for source_name in enabled_sources:
            step += 1
            if source_name not in SOURCES:
                continue

            if progress_callback:
                progress_callback(f"[{step}/{total_steps}] {source_name}: 「{kw}」")

            try:
                func = SOURCES[source_name]["func"]
                jobs = func(kw, location)
                if progress_callback:
                    progress_callback(f"  → {len(jobs)}件取得")
                all_jobs.extend(jobs)
            except Exception as e:
                if progress_callback:
                    progress_callback(f"  → エラー: {e}")

            time.sleep(random.uniform(0.5, 1.5))

    return _deduplicate(all_jobs)


# ============================================================
# CSV/テキスト インポート
# ============================================================

def parse_csv_upload(file_content: str) -> List[Dict]:
    """CSVテキストから求人データを解析"""
    jobs = []
    reader = csv.DictReader(io.StringIO(file_content))

    col_map = {
        "求人タイトル": "title", "タイトル": "title", "職種": "title", "title": "title",
        "企業名": "company", "会社名": "company", "company": "company",
        "勤務地": "location", "勤務場所": "location", "location": "location",
        "年収": "salary", "給与": "salary", "salary": "salary",
        "url": "url", "URL": "url", "リンク": "url", "求人URL": "url",
        "説明": "description", "概要": "description", "description": "description",
        "ソース": "source", "媒体": "source", "source": "source",
    }

    for row in reader:
        job = {}
        for csv_col, value in row.items():
            if csv_col and csv_col.strip() in col_map:
                mapped = col_map[csv_col.strip()]
                job[mapped] = value.strip() if value else ""
        if job.get("title") or job.get("url"):
            if not job.get("source"):
                job["source"] = "CSVインポート"
            if not job.get("title"):
                job["title"] = job.get("url", "不明")
            jobs.append(job)

    return jobs


def parse_text_input(text: str) -> List[Dict]:
    """テキスト貼り付けから求人データを解析"""
    jobs = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t") if "\t" in line else line.split(",")
        parts = [p.strip() for p in parts]

        url = ""
        other = []
        for p in parts:
            if re.match(r'https?://', p):
                url = p
            else:
                other.append(p)

        job = {
            "title": other[0] if len(other) > 0 else "",
            "company": other[1] if len(other) > 1 else "",
            "location": other[2] if len(other) > 2 else "",
            "salary": other[3] if len(other) > 3 else "",
            "url": url, "description": "", "source": "手動入力",
        }
        if job["title"] or job["url"]:
            jobs.append(job)

    return jobs


# ============================================================
# 検索URL生成
# ============================================================

def generate_search_urls(keywords: str, location: str = "") -> List[Dict]:
    q = urllib.parse.quote(keywords)
    loc = urllib.parse.quote(location) if location else ""
    kw_loc = urllib.parse.quote(f"{keywords} {location}") if location else q

    return [
        {"site": "Indeed", "url": f"https://jp.indeed.com/jobs?q={q}&l={loc}", "icon": "🔵"},
        {"site": "求人ボックス", "url": f"https://求人ボックス.com/求人検索?q={q}&l={loc}", "icon": "🟢"},
        {"site": "doda", "url": f"https://doda.jp/DodaFront/View/JobSearchList.action?kw={q}&ka={loc}", "icon": "🔴"},
        {"site": "リクナビNEXT", "url": f"https://next.rikunabi.com/rnc/docs/cp_s00890.jsp?keyword={q}", "icon": "🟡"},
        {"site": "ビズリーチ", "url": f"https://www.bizreach.jp/job-feed/public-search/?keyword={q}", "icon": "🟤"},
        {"site": "Green", "url": f"https://www.green-japan.com/search?keyword={q}", "icon": "🟩"},
        {"site": "Wantedly", "url": f"https://www.wantedly.com/search?q={q}", "icon": "🔷"},
        {"site": "Google", "url": f"https://www.google.com/search?q={kw_loc}+求人+募集", "icon": "🔍"},
    ]


# ============================================================
# ユーティリティ
# ============================================================

def _deduplicate(jobs: List[Dict]) -> List[Dict]:
    seen = set()
    result = []
    for job in jobs:
        url = job.get("url", "")
        if url and url not in seen:
            seen.add(url)
            result.append(job)
    return result
