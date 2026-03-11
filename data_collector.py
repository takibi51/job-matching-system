"""
求人データ自動収集モジュール（v3）
- CareerJet（メインソース: スクレイピング + ページネーション）
- Jooble API（公開REST API — 安定・高品質）
- 求人ボックス（スクレイピング + ページネーション）
- リクルートエージェント（Next.jsデータ）
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
import logging
import os
from typing import List, Dict, Optional, Callable
from datetime import datetime

# Streamlit Cloud判定（クラウドではスクレイピングが動かないためAPI優先）
_IS_CLOUD = bool(os.environ.get("STREAMLIT_SERVER_ADDRESS") or os.path.exists("/mount/src"))

# エラーログ（バックグラウンド取得の問題特定用）
_fetch_log: List[str] = []


def get_fetch_log() -> List[str]:
    """直近の取得ログを返す"""
    return list(_fetch_log[-100:])


def _log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    _fetch_log.append(entry)
    if len(_fetch_log) > 200:
        _fetch_log.pop(0)


try:
    from bs4 import BeautifulSoup
    _HAS_BS4 = True
except ImportError:
    _HAS_BS4 = False

# User-Agent ローテーション（ブロック対策）
_HEADERS_LIST = [
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.5",
        "Connection": "keep-alive",
    },
]


def _get_headers():
    return random.choice(_HEADERS_LIST).copy()


# ドメイン別レート制限
_last_request_time = {}


def _rate_limit(domain: str, min_interval: float = 1.5):
    now = time.time()
    last = _last_request_time.get(domain, 0)
    wait = min_interval - (now - last)
    if wait > 0:
        time.sleep(wait + random.uniform(0, 0.5))
    _last_request_time[domain] = time.time()


def _safe_get(url: str, params: dict = None, timeout: int = 15,
              headers: dict = None, max_retries: int = 3,
              return_soup: bool = False):
    """リトライ・バックオフ付きHTTP GET（詳細ログ付き）"""
    for attempt in range(max_retries):
        try:
            h = headers or _get_headers()
            resp = requests.get(url, params=params, headers=h,
                                timeout=timeout, allow_redirects=True)
            if resp.status_code == 200:
                _log(f"GET {url} → 200 OK ({len(resp.text)}文字)")
                if return_soup and _HAS_BS4:
                    return BeautifulSoup(resp.text, "html.parser")
                return resp.text
            elif resp.status_code in (403, 429, 503):
                _log(f"GET {url} → {resp.status_code} (リトライ {attempt+1}/{max_retries})")
                wait = (2 ** attempt) + random.uniform(0.5, 1.5)
                time.sleep(wait)
                continue
            else:
                _log(f"GET {url} → {resp.status_code} (スキップ)")
                return None
        except requests.RequestException as e:
            _log(f"GET {url} → 例外: {e} (リトライ {attempt+1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1.0, 2.0))
    _log(f"GET {url} → 全リトライ失敗")
    return None


# ============================================================
# 1. CareerJet（メインソース — スクレイピング）
# ============================================================

def fetch_careerjet(keyword: str, location: str = "", max_pages: int = 3) -> List[Dict]:
    """CareerJetから求人取得（ソート別×ページネーション）"""
    jobs = []
    _log(f"CareerJet: keyword={keyword}, location={location}, max_pages={max_pages}")

    for sort in ["relevance", "date", "salary"]:
        for page in range(1, max_pages + 1):
            _rate_limit("www.careerjet.jp", 1.5)

            if _HAS_BS4:
                soup = _safe_get(
                    "https://www.careerjet.jp/search/jobs",
                    params={"s": keyword, "l": location, "sort": sort, "p": page},
                    return_soup=True,
                )
                if not soup:
                    break
                articles = soup.select("article")
                if not articles:
                    _log(f"CareerJet: sort={sort}, page={page} → article要素0件")
                    break
                for art in articles:
                    job = _parse_careerjet_article_bs(art, location)
                    if job:
                        jobs.append(job)
            else:
                text = _safe_get(
                    "https://www.careerjet.jp/search/jobs",
                    params={"s": keyword, "l": location, "sort": sort, "p": page},
                )
                if not text:
                    break
                articles = re.findall(r'<article[^>]*>(.*?)</article>', text, re.DOTALL)
                if not articles:
                    break
                for art in articles:
                    job = _parse_careerjet_article_re(art, location)
                    if job:
                        jobs.append(job)

    result = _deduplicate(jobs)
    _log(f"CareerJet: {len(result)}件取得完了")
    return result


def _parse_careerjet_article_bs(article, default_location: str = "") -> Optional[Dict]:
    """BeautifulSoupでCareerJet記事をパース"""
    job = {}
    title_link = article.select_one("h2 a")
    if not title_link:
        return None
    href = title_link.get("href", "")
    job["url"] = ("https://www.careerjet.jp" + href) if href.startswith("/") else href
    job["title"] = title_link.get_text(strip=True)
    if not job["title"]:
        return None

    comp = article.select_one('p[class*="company"]')
    job["company"] = comp.get_text(strip=True) if comp else ""

    loc = article.select_one('ul[class*="location"]')
    job["location"] = loc.get_text(strip=True) if loc else default_location

    sal = article.select_one('li[class*="salary"]')
    job["salary"] = sal.get_text(strip=True) if sal else ""

    desc = article.select_one('div[class*="desc"]')
    job["description"] = desc.get_text(strip=True)[:2000] if desc else ""

    job["source"] = "CareerJet"
    job["pub_date"] = ""
    return job


def _parse_careerjet_article_re(art_html: str, default_location: str = "") -> Optional[Dict]:
    """正規表現でCareerJet記事をパース（BS4未インストール時のフォールバック）"""
    job = {}
    title_m = re.search(r'<h2[^>]*>.*?<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', art_html, re.DOTALL)
    if not title_m:
        return None
    href = title_m.group(1)
    job["url"] = ("https://www.careerjet.jp" + href) if href.startswith("/") else href
    job["title"] = re.sub(r'<[^>]+>', '', title_m.group(2)).strip()
    if not job["title"]:
        return None

    comp_m = re.search(r'<p[^>]*class="[^"]*company[^"]*"[^>]*>(.*?)</p>', art_html, re.DOTALL)
    job["company"] = re.sub(r'<[^>]+>', '', comp_m.group(1)).strip() if comp_m else ""

    loc_m = re.search(r'<ul[^>]*class="[^"]*location[^"]*"[^>]*>(.*?)</ul>', art_html, re.DOTALL)
    job["location"] = re.sub(r'<[^>]+>', '', loc_m.group(1)).strip() if loc_m else default_location

    sal_m = re.search(r'<li[^>]*class="[^"]*salary[^"]*"[^>]*>(.*?)</li>', art_html, re.DOTALL)
    job["salary"] = re.sub(r'<[^>]+>', '', sal_m.group(1)).strip() if sal_m else ""

    desc_m = re.search(r'<div[^>]*class="[^"]*desc[^"]*"[^>]*>(.*?)</div>', art_html, re.DOTALL)
    job["description"] = re.sub(r'<[^>]+>', '', desc_m.group(1)).strip()[:2000] if desc_m else ""

    job["source"] = "CareerJet"
    job["pub_date"] = ""
    return job


# ============================================================
# 2. Jooble API（公開REST API — 認証不要でもテスト可能）
# ============================================================

# Jooble APIキー（設定から取得、未設定でもデフォルトで動作テスト）
_JOOBLE_API_KEY = ""


def set_jooble_api_key(key: str):
    global _JOOBLE_API_KEY
    _JOOBLE_API_KEY = key


def fetch_jooble(keyword: str, location: str = "", max_pages: int = 3) -> List[Dict]:
    """Jooble APIから求人取得（公開REST API）"""
    api_key = _JOOBLE_API_KEY
    if not api_key:
        if _IS_CLOUD:
            _log("⚠️ Jooble APIキーが未設定です。クラウド環境ではAPIキーが必要です。")
            _log("📋 設定方法: データ管理タブ → API設定 でJooble APIキーを入力してください")
            _log("🔗 キー取得: https://jooble.org/api/about （無料・即時発行）")
            return []
        return _fetch_jooble_scrape(keyword, location, max_pages)

    jobs = []
    _log(f"Jooble API: keyword={keyword}, location={location}, key={api_key[:8]}...")

    # 日本版エンドポイントを優先、失敗時に国際版にフォールバック
    _jooble_endpoints = [
        f"https://jp.jooble.org/api/{api_key}",
        f"https://jooble.org/api/{api_key}",
    ]

    for page in range(1, max_pages + 1):
        _rate_limit("jooble.org", 1.0)
        try:
            payload = {
                "keywords": keyword,
                "location": location or "",
                "page": page,
            }

            data = None
            for endpoint in _jooble_endpoints:
                _log(f"Jooble API: POST {endpoint[:45]}... payload={payload}")
                resp = requests.post(
                    endpoint,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "Mozilla/5.0 (compatible; JobSearchBot/1.0)",
                    },
                    timeout=20,
                )
                _log(f"Jooble API: status={resp.status_code}, length={len(resp.text)}")
                if resp.status_code == 200:
                    data = resp.json()
                    total_count = data.get("totalCount", 0)
                    if total_count > 0:
                        _log(f"Jooble API: {endpoint[:30]}... → totalCount={total_count}")
                        # このエンドポイントが有効なので以降はこれだけ使う
                        _jooble_endpoints = [endpoint]
                        break
                    else:
                        _log(f"Jooble API: {endpoint[:30]}... → totalCount=0, 次のエンドポイントを試行")
                else:
                    _log(f"Jooble API: {endpoint[:30]}... → {resp.status_code}: {resp.text[:200]}")

            if not data:
                break

            items = data.get("jobs", [])
            _log(f"Jooble API: totalCount={data.get('totalCount', '?')}, jobs={len(items)}")
            if not items:
                break

            for item in items:
                title = item.get("title", "").strip()
                link = item.get("link", "").strip()
                if not title or not link:
                    continue
                jobs.append({
                    "title": title[:100],
                    "company": item.get("company", ""),
                    "location": item.get("location", ""),
                    "salary": item.get("salary", ""),
                    "url": link,
                    "description": item.get("snippet", "")[:2000],
                    "source": "Jooble",
                    "pub_date": item.get("updated", ""),
                })
            _log(f"Jooble API: page={page} → {len(items)}件")
        except Exception as e:
            _log(f"Jooble API: page={page} → エラー: {e}")
            break

    result = _deduplicate(jobs)
    _log(f"Jooble API: {len(result)}件取得完了")
    return result


def _fetch_jooble_scrape(keyword: str, location: str = "", max_pages: int = 3) -> List[Dict]:
    """Joobleからスクレイピングで求人取得（APIキー未設定時のフォールバック）"""
    if not _HAS_BS4:
        return []
    jobs = []
    _log(f"Jooble(scrape): keyword={keyword}")
    query = f"{keyword} {location}".strip() if location else keyword

    for page in range(1, max_pages + 1):
        _rate_limit("jp.jooble.org", 1.5)
        soup = _safe_get(
            "https://jp.jooble.org/SearchResult",
            params={"ukw": query, "p": page},
            return_soup=True,
            timeout=20,
        )
        if not soup:
            break

        # 求人カードを複数セレクタで検索
        cards = (
            soup.select('article[class*="vacancy"]')
            or soup.select('div[class*="vacancy-card"]')
            or soup.select('div[data-test="serp-item"]')
            or soup.select('a[class*="vacancy"]')
        )
        if not cards:
            _log(f"Jooble(scrape): page={page} → カード0件")
            break

        for card in cards:
            job = {}
            title_el = card.select_one('h2, h3, [class*="header"], [class*="title"]')
            if title_el:
                job["title"] = title_el.get_text(strip=True)[:100]

            link = card if card.name == "a" else card.select_one('a[href]')
            if link and link.get("href"):
                href = link["href"]
                job["url"] = ("https://jp.jooble.org" + href) if href.startswith("/") else href

            comp_el = card.select_one('[class*="company"]')
            job["company"] = comp_el.get_text(strip=True) if comp_el else ""

            loc_el = card.select_one('[class*="location"], [class*="geo"]')
            job["location"] = loc_el.get_text(strip=True) if loc_el else ""

            sal_el = card.select_one('[class*="salary"]')
            job["salary"] = sal_el.get_text(strip=True) if sal_el else ""

            desc_el = card.select_one('[class*="snippet"], [class*="desc"]')
            job["description"] = desc_el.get_text(strip=True)[:500] if desc_el else ""

            job["source"] = "Jooble"
            job["pub_date"] = ""

            if job.get("title") and job.get("url"):
                jobs.append(job)

        _log(f"Jooble(scrape): page={page} → {len(cards)}カード")

    result = _deduplicate(jobs)
    _log(f"Jooble(scrape): {len(result)}件取得完了")
    return result


# ============================================================
# 3. 求人ボックス（スクレイピング + ページネーション）
# ============================================================

def fetch_kyujinbox(keyword: str, location: str = "", max_pages: int = 3) -> List[Dict]:
    """求人ボックスから求人取得"""
    jobs = []
    query = f"{keyword} {location}".strip() if location else keyword
    _log(f"求人ボックス: query={query}")

    for page in range(1, max_pages + 1):
        _rate_limit("xn--pckua2a7gp15o89zb.com", 1.5)

        if _HAS_BS4:
            soup = _safe_get(
                "https://xn--pckua2a7gp15o89zb.com/",
                params={"q": query, "p": page},
                return_soup=True,
            )
            if not soup:
                break

            cards = (
                soup.select('article.p-search-job')
                or soup.select('a.p-search-job__link')
                or soup.select('div.p-search-job')
                or soup.select('[class*="search-job"]')
            )
            # フォールバック: /jbi/ リンクを探す
            if not cards:
                links = soup.select('a[href*="/jbi/"]')
                for link in links:
                    title = link.get_text(strip=True)
                    if title and len(title) >= 5:
                        href = link.get("href", "")
                        url = ("https://xn--pckua2a7gp15o89zb.com" + href) if href.startswith("/") else href
                        jobs.append({
                            "title": title[:100], "company": "", "location": location or "",
                            "salary": "", "url": url, "description": "",
                            "source": "求人ボックス", "pub_date": "",
                        })
                if links:
                    _log(f"求人ボックス: page={page} → {len(links)}リンク(フォールバック)")
                continue

            if not cards:
                _log(f"求人ボックス: page={page} → カード0件")
                break

            for card in cards:
                job = _parse_kyujinbox_card_bs(card, location)
                if job:
                    jobs.append(job)
            _log(f"求人ボックス: page={page} → {len(cards)}カード")
        else:
            text = _safe_get(
                "https://xn--pckua2a7gp15o89zb.com/",
                params={"q": query, "p": page},
            )
            if not text:
                break
            all_links = re.findall(r'<a[^>]+href="(/jbi/[^"]+)"[^>]*>(.*?)</a>', text, re.DOTALL)
            if not all_links:
                break
            for href, title_html in all_links:
                title = re.sub(r'<[^>]+>', '', title_html).strip()
                if not title or len(title) < 5:
                    continue
                url = "https://xn--pckua2a7gp15o89zb.com" + href
                jobs.append({
                    "title": title[:100], "company": "", "location": location or "",
                    "salary": "", "url": url, "description": "",
                    "source": "求人ボックス", "pub_date": "",
                })

    result = _deduplicate(jobs)
    _log(f"求人ボックス: {len(result)}件取得完了")
    return result


def _parse_kyujinbox_card_bs(card, default_location: str = "") -> Optional[Dict]:
    """求人ボックスカードをBS4パース"""
    job = {}

    for sel in ['h3 a', '.p-search-job__title', 'a[class*="title"]', 'h3', 'h2']:
        el = card.select_one(sel)
        if el and el.get_text(strip=True):
            job["title"] = el.get_text(strip=True)[:100]
            break
    if not job.get("title"):
        return None

    link = card.select_one('a[href]') if card.name != 'a' else card
    if link and link.get("href"):
        href = link["href"]
        job["url"] = ("https://xn--pckua2a7gp15o89zb.com" + href) if href.startswith("/") else href
    else:
        return None

    for sel in ['[class*="company"]', '[class*="corp"]', '.p-search-job__company']:
        el = card.select_one(sel)
        if el:
            job["company"] = el.get_text(strip=True)
            break
    job.setdefault("company", "")

    for sel in ['[class*="location"]', '[class*="area"]', '.p-search-job__area']:
        el = card.select_one(sel)
        if el:
            job["location"] = el.get_text(strip=True)
            break
    job.setdefault("location", default_location)

    for sel in ['[class*="salary"]', '[class*="income"]', '.p-search-job__salary']:
        el = card.select_one(sel)
        if el:
            job["salary"] = el.get_text(strip=True)
            break
    job.setdefault("salary", "")

    job["description"] = ""
    job["source"] = "求人ボックス"
    job["pub_date"] = ""
    return job


# ============================================================
# 4. リクルートエージェント（Next.js __NEXT_DATA__）
# ============================================================

def fetch_recruit_agent(keyword: str, location: str = "", max_pages: int = 2) -> List[Dict]:
    """リクルートエージェントから求人取得"""
    jobs = []
    _log(f"リクルートエージェント: keyword={keyword}")

    for page in range(1, max_pages + 1):
        _rate_limit("www.r-agent.com", 2.0)
        text = _safe_get(
            "https://www.r-agent.com/kensaku/",
            params={"keyword": keyword, "page": page},
            timeout=20,
        )
        if not text:
            break

        nd_m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', text, re.DOTALL)
        if not nd_m:
            _log(f"リクルートエージェント: page={page} → __NEXT_DATA__なし")
            break

        try:
            nd = json.loads(nd_m.group(1))
            page_props = nd.get("props", {}).get("pageProps", {}).get("data", {})

            found_any = False
            for key in ["recommendedJobs", "searchResult", "jobs", "jobList"]:
                items = page_props.get(key, [])
                if isinstance(items, dict):
                    items = items.get("items", []) or items.get("jobs", []) or items.get("list", [])
                if not isinstance(items, list):
                    continue
                for item in items:
                    job = _parse_recruit_item(item, location)
                    if job:
                        jobs.append(job)
                        found_any = True
            if not found_any:
                _log(f"リクルートエージェント: page={page} → 求人データキーなし (keys: {list(page_props.keys())[:5]})")
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            _log(f"リクルートエージェント: page={page} → パースエラー: {e}")

    result = _deduplicate(jobs)
    _log(f"リクルートエージェント: {len(result)}件取得完了")
    return result


def _parse_recruit_item(item: dict, default_location: str = "") -> Optional[Dict]:
    """リクルートエージェントの求人アイテムをパース"""
    title = item.get("title", "").strip()
    jkey = item.get("indeedJobKey", "") or item.get("jobKey", "") or item.get("id", "")
    if not title or not jkey:
        return None
    return {
        "title": title[:100],
        "company": item.get("companyName", ""),
        "location": item.get("workLocation", "") or default_location,
        "salary": item.get("salary", ""),
        "url": f"https://www.r-agent.com/viewjob/{jkey}/",
        "description": item.get("description", "") or item.get("catchCopy", ""),
        "source": "リクルートエージェント",
        "pub_date": "",
    }


# ============================================================
# 統合: 全ソースから自動取得
# ============================================================

SOURCES = {
    "Jooble": {"func": fetch_jooble, "enabled": True},
    "CareerJet": {"func": fetch_careerjet, "enabled": not _IS_CLOUD},
    "求人ボックス": {"func": fetch_kyujinbox, "enabled": not _IS_CLOUD},
    "リクルートエージェント": {"func": fetch_recruit_agent, "enabled": not _IS_CLOUD},
}

if _IS_CLOUD:
    _log("☁️ クラウド環境を検出: Jooble APIをメインソースとして使用します")

SOURCE_NAMES = list(SOURCES.keys())


def fetch_from_all_sources(keywords: List[str], location: str = "",
                           enabled_sources: List[str] = None,
                           progress_callback: Callable = None,
                           max_pages: int = 3) -> List[Dict]:
    """全ソースから求人を自動取得"""
    if enabled_sources is None:
        enabled_sources = [name for name, info in SOURCES.items() if info["enabled"]]

    all_jobs = []
    total_steps = len(keywords[:15]) * len(enabled_sources)
    step = 0

    _log(f"=== 全ソース取得開始: keywords={keywords[:5]}, sources={enabled_sources} ===")

    for kw in keywords[:15]:
        for source_name in enabled_sources:
            step += 1
            if source_name not in SOURCES:
                continue

            if progress_callback:
                progress_callback(f"[{step}/{total_steps}] {source_name}: 「{kw}」")

            try:
                func = SOURCES[source_name]["func"]
                jobs = func(kw, location, max_pages=max_pages)
                if progress_callback:
                    progress_callback(f"  → {len(jobs)}件取得")
                all_jobs.extend(jobs)
            except Exception as e:
                _log(f"ソース {source_name} エラー: {e}")
                if progress_callback:
                    progress_callback(f"  → エラー: {e}")

            time.sleep(random.uniform(0.3, 0.8))

    result = _deduplicate(all_jobs)
    _log(f"=== 全ソース取得完了: {len(result)}件（重複除外後） ===")
    return result


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
