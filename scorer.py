"""
求人マッチングスコアリングエンジン
候補者の条件と求人のマッチ度を算出し、理由を生成
"""

import re
import math
from typing import List, Dict, Tuple


# 関西エリアキーワード
KANSAI_KEYWORDS = [
    "大阪", "京都", "神戸", "兵庫", "奈良", "滋賀", "和歌山",
    "梅田", "難波", "心斎橋", "三宮", "堺", "高槻", "豊中",
    "西宮", "尼崎", "枚方", "茨木", "吹田", "関西",
]

# リモートワーク関連キーワード
REMOTE_KEYWORDS = ["リモート", "在宅", "テレワーク", "フルリモート", "remote"]

# 勤務地エリアグループ（都道府県 → 主要都市・地名のマッピング）
_LOCATION_AREA_MAP = {
    "北海道": ["北海道", "札幌"],
    "東京都": ["東京", "渋谷", "新宿", "港区", "千代田", "品川", "目黒", "中央区", "六本木", "丸の内", "大手町", "秋葉原"],
    "神奈川県": ["神奈川", "横浜", "川崎", "相模原", "藤沢"],
    "埼玉県": ["埼玉", "さいたま", "大宮", "川越"],
    "千葉県": ["千葉", "船橋", "柏", "幕張"],
    "愛知県": ["愛知", "名古屋", "豊田"],
    "大阪府": ["大阪", "梅田", "難波", "心斎橋", "堺", "豊中", "吹田", "高槻"],
    "京都府": ["京都"],
    "兵庫県": ["兵庫", "神戸", "三宮", "西宮", "尼崎", "姫路"],
    "福岡県": ["福岡", "博多", "北九州"],
    "広島県": ["広島"],
    "宮城県": ["宮城", "仙台"],
    "リモート": ["リモート", "在宅", "テレワーク", "フルリモート", "remote"],
}


def _count_occurrences(text: str, keyword: str) -> int:
    """テキスト中のキーワード出現回数を数える（大文字小文字無視）"""
    return len(re.findall(re.escape(keyword.lower()), text.lower()))


def _keyword_specificity(keyword: str) -> float:
    """キーワードの特異度（具体性）を計算。長い・複合的なほど高スコア"""
    kw = keyword.strip()
    length = len(kw)
    # 基本スコア: 文字数ベース（2文字=1.0、4文字=1.3、6文字以上=1.5+）
    base = 1.0 + min(0.6, max(0, (length - 2)) * 0.12)
    # 複合語ボーナス: スペースやカタカナ+漢字の組み合わせ
    if " " in kw or "・" in kw:
        base += 0.3
    # 英字+日本語の混在ボーナス（例: "Webマーケ"）
    has_ascii = bool(re.search(r'[a-zA-Z]', kw))
    has_jp = bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', kw))
    if has_ascii and has_jp:
        base += 0.2
    return base


def _location_matches(job_location: str, job_text: str, desired_location: str) -> bool:
    """勤務地が一致するかをエリアグループも考慮して判定"""
    if not desired_location or desired_location == "全国":
        return True
    # 直接一致
    if desired_location in job_location or desired_location in job_text:
        return True
    # エリアグループによる一致
    area_keywords = _LOCATION_AREA_MAP.get(desired_location, [])
    if area_keywords:
        combined = job_location + " " + job_text
        for kw in area_keywords:
            # 「京都」が「東京都」にマッチする誤判定を防止
            if kw in combined and not re.search(r'(?<!東)' + re.escape(kw), combined):
                continue
            if re.search(r'(?<!東)' + re.escape(kw), combined):
                return True
    return False


def score_job(job: Dict, conditions: Dict) -> Tuple[float, List[str]]:
    """
    求人と候補者条件のマッチ度スコアを算出

    Args:
        job: 求人情報 (title, company, location, salary, description, source)
        conditions: 候補者条件 {
            "keywords": List[str],    # 職種・スキルキーワード
            "location": str,          # 希望勤務地
            "salary_min": int,        # 希望最低年収（万円）
            "salary_max": int,        # 希望最高年収（万円）
            "age": int,               # 年齢
            "prefer_kansai": bool,    # 関西優先
            "extra_keywords": List[str],  # 追加キーワード
        }

    Returns:
        (score, reasons): スコア(0-100)とマッチ理由のリスト
    """
    score = 0.0
    reasons = []

    job_text = " ".join([
        job.get("title", ""),
        job.get("company", ""),
        job.get("location", ""),
        job.get("salary", ""),
        job.get("description", ""),
    ]).lower()

    job_title = job.get("title", "").lower()

    # === 1. キーワードマッチ (最大60点) ===
    # 一致数ベース: 多く一致するほど高スコア（職種の適合度を最重視）
    keywords = conditions.get("keywords", [])
    if keywords:
        matched_kw = []
        title_matched_kw = []
        for kw in keywords:
            kw_lower = kw.lower()
            if kw_lower in job_text:
                matched_kw.append(kw)
                if kw_lower in job_title:
                    title_matched_kw.append(kw)

        if matched_kw:
            # 一致数ベーススコア: 1個=8点、上限48点
            count_score = min(48, len(matched_kw) * 8)
            # タイトル一致ボーナス: タイトルに含まれるKWごとに+3点、上限12点
            title_bonus = min(12, len(title_matched_kw) * 3)
            kw_score = min(60, count_score + title_bonus)
            score += kw_score
            reasons.append(f"キーワード一致({len(matched_kw)}個): {', '.join(matched_kw[:5])}")

    # === 2. 勤務地マッチ (最大25点) ===
    # 優先順位: _locations(設定値) > location(希望勤務地) > リモート
    job_location = job.get("location", "")
    desired_location = conditions.get("location", "")
    multi_locations = conditions.get("_locations", [])

    # 設定された勤務地を最優先（複数の場合はOR条件）
    _active_locs = [l for l in multi_locations if l and l != "全国"] if multi_locations else []
    if not _active_locs and desired_location and desired_location != "全国":
        _active_locs = [desired_location]

    if _active_locs:
        loc_matched_names = [
            loc for loc in _active_locs
            if _location_matches(job_location, job_text, loc)
        ]
        if loc_matched_names:
            score += 25
            reasons.append(f"希望勤務地({', '.join(loc_matched_names[:2])})に一致")
        elif any(kw in job_text for kw in REMOTE_KEYWORDS):
            score += 15
            reasons.append("リモートワーク可能")
        else:
            score += 0
    else:
        if any(kw in job_text for kw in REMOTE_KEYWORDS):
            score += 10
            reasons.append("リモートワーク可能")
        else:
            score += 5

    # === 3. 年収マッチ (最大10点) ===
    salary_min = conditions.get("salary_min", 0)
    salary_max = conditions.get("salary_max", 0)
    job_salary = job.get("salary", "")

    if job_salary and salary_min > 0:
        job_sal_range = _parse_salary(job_salary)
        if job_sal_range[1] > 0:
            if job_sal_range[1] >= salary_min:
                if job_sal_range[0] <= salary_max * 1.3 if salary_max > 0 else True:
                    score += 10
                    reasons.append(f"年収条件に合致 ({job_salary})")
                else:
                    score += 5
                    reasons.append(f"年収やや高め ({job_salary})")

    # === 4. 追加キーワードマッチ (最大5点) ===
    extra_kw = conditions.get("extra_keywords", [])
    if extra_kw:
        matched_extra = [kw for kw in extra_kw if kw.lower() in job_text]
        if matched_extra:
            extra_score = min(5, (len(matched_extra) / len(extra_kw)) * 5)
            score += extra_score
            reasons.append(f"追加条件一致: {', '.join(matched_extra[:3])}")

    if not reasons:
        reasons.append("検索キーワードに関連")

    return min(100, score), reasons


def _parse_salary(salary_text: str) -> Tuple[int, int]:
    """給与テキストを年収（万円）のレンジに変換"""
    if not salary_text:
        return (0, 0)

    text = salary_text.replace(",", "").replace("、", "").replace("，", "")

    # 万円単位の数値を抽出
    man_matches = re.findall(r'(\d+)\s*万', text)
    if man_matches:
        nums = [int(n) for n in man_matches]
        # 月給の場合は年収に変換
        if "月" in text:
            nums = [n * 12 for n in nums]
        if len(nums) >= 2:
            return (min(nums), max(nums))
        elif len(nums) == 1:
            return (nums[0], nums[0])

    # 円単位の数値（万なし）を抽出
    plain_matches = re.findall(r'(\d+)', text)
    if plain_matches:
        nums = [int(n) for n in plain_matches]
        # 大きい数字は円単位 → 万円に変換
        nums = [n // 10000 if n > 10000 else n for n in nums]
        # 月給・月収の場合は年収に換算（×12）
        if "月" in text:
            nums = [n * 12 for n in nums]
        if nums:
            return (min(nums), max(nums))

    return (0, 0)


def generate_job_summary(job: Dict) -> str:
    """求人情報から簡潔な要約を生成"""
    parts = []
    title = job.get("title", "")
    company = job.get("company", "")
    location = job.get("location", "")
    salary = job.get("salary", "")
    description = job.get("description", "")

    if company:
        parts.append(f"{company}")
    if title and title != company:
        parts.append(f"の{title}ポジション" if company else title)
    if location:
        parts.append(f"（{location}）")
    if salary:
        parts.append(f"。年収: {salary}")
    if description:
        desc_short = description[:80] + "..." if len(description) > 80 else description
        parts.append(f"。{desc_short}")

    return "".join(parts) if parts else "詳細は求人ページをご確認ください"


def generate_fit_reason(job: Dict, conditions: Dict, reasons: List[str]) -> str:
    """候補者と求人のフィット理由を詳細に生成"""
    fit_parts = []

    keywords = conditions.get("keywords", [])
    job_text = " ".join([
        job.get("title", ""),
        job.get("company", ""),
        job.get("description", ""),
    ]).lower()

    # キーワードマッチの詳細
    matched = [kw for kw in keywords if kw.lower() in job_text]
    if matched:
        fit_parts.append(f"候補者の経験・スキル（{', '.join(matched[:3])}）が求人内容と合致")

    # 勤務地の理由
    location_reasons = [r for r in reasons if "関西" in r or "勤務地" in r or "リモート" in r]
    if location_reasons:
        fit_parts.append(location_reasons[0])

    # 年収の理由
    salary_reasons = [r for r in reasons if "年収" in r]
    if salary_reasons:
        fit_parts.append(salary_reasons[0])

    # 年齢の理由
    age_reasons = [r for r in reasons if "ミドル" in r or "若手" in r or "年齢" in r]
    if age_reasons:
        fit_parts.append(age_reasons[0])

    # 追加条件の理由
    extra_reasons = [r for r in reasons if "追加条件" in r]
    if extra_reasons:
        fit_parts.append(extra_reasons[0])

    if not fit_parts:
        fit_parts.append("検索条件に関連する求人")

    return " / ".join(fit_parts)


def rank_jobs(jobs: List[Dict], conditions: Dict) -> List[Dict]:
    """求人リストをスコア順にランキング（要約・フィット理由付き）"""
    scored_jobs = []
    for job in jobs:
        score, reasons = score_job(job, conditions)
        job_with_score = {**job}
        job_with_score["score"] = round(score, 1)
        job_with_score["match_reasons"] = " / ".join(reasons)
        job_with_score["job_summary"] = generate_job_summary(job)
        job_with_score["fit_reason"] = generate_fit_reason(job, conditions, reasons)
        scored_jobs.append(job_with_score)

    # スコア降順でソート
    scored_jobs.sort(key=lambda x: x["score"], reverse=True)
    return scored_jobs


def generate_search_queries(conditions: Dict) -> List[str]:
    """候補者条件から複数の検索クエリを生成"""
    keywords = conditions.get("keywords", [])
    extra = conditions.get("extra_keywords", [])
    location = conditions.get("location", "")
    age = conditions.get("age", 0)

    queries = []

    # メインキーワードの組み合わせ
    if keywords:
        # 全キーワード結合
        queries.append(" ".join(keywords[:3]))
        # 個別キーワード
        for kw in keywords[:5]:
            queries.append(kw)
        # キーワード + 追加条件
        for kw in keywords[:3]:
            for ex in extra[:2]:
                queries.append(f"{kw} {ex}")

    # 年齢に応じた追加クエリ
    if age >= 40:
        for kw in keywords[:2]:
            queries.append(f"{kw} ミドル 経験者")
    elif age <= 30:
        for kw in keywords[:2]:
            queries.append(f"{kw} 第二新卒")

    # 重複除去
    seen = set()
    unique_queries = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            unique_queries.append(q)

    return unique_queries
