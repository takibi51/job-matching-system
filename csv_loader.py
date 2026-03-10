"""
候補者CSVファイルの読み込みモジュール
既存のCSVデータから候補者条件を自動抽出
個人情報（氏名・連絡先・住所等）は自動除外
"""

import csv
import os
import re
import glob
from typing import List, Dict, Optional


CSV_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 個人情報として除外する項目キーワード
PERSONAL_INFO_KEYS = [
    "氏名", "名前", "フルネーム", "本名",
    "電話", "携帯", "TEL", "tel", "Phone", "phone",
    "メール", "メアド", "Email", "email", "e-mail",
    "住所", "自宅", "現住所", "郵便番号", "〒",
    "生年月日", "誕生日",
    "マイナンバー", "保険証", "免許証",
    "LINE", "line", "SNS",
]


def _is_personal_info(key: str) -> bool:
    """項目名が個人情報に該当するかチェック"""
    key_lower = key.strip().lower()
    for pi_key in PERSONAL_INFO_KEYS:
        if pi_key.lower() in key_lower:
            return True
    return False


def list_candidate_csvs() -> List[str]:
    """候補者CSVファイルの一覧を取得"""
    pattern = os.path.join(CSV_DIR, "[0-9][0-9]_候補者*.csv")
    files = sorted(glob.glob(pattern))
    return files


def load_candidate_csv(filepath: str) -> Optional[Dict]:
    """候補者CSVを読み込み、検索条件に変換"""
    if not os.path.exists(filepath):
        return None

    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)

    candidate = {
        "name": os.path.basename(filepath),
        "info": {},
        "strengths": [],
        "keywords": [],
        "extra_keywords": [],
    }

    section = None  # "info", "strengths", "jobs"

    for row in rows:
        if not row or all(cell.strip() == "" for cell in row):
            continue

        first_cell = row[0].strip()

        # セクション判定
        if first_cell == "候補者情報" or first_cell == "項目":
            section = "info"
            continue
        elif first_cell in ("候補者の強み", "強み"):
            section = "strengths"
            continue
        elif first_cell in ("転職先候補求人リスト", "No."):
            section = "jobs"
            continue

        # データ抽出（個人情報は自動除外）
        if section == "info" and len(row) >= 2:
            key = row[0].strip()
            val = row[1].strip()
            if key and val and not _is_personal_info(key):
                candidate["info"][key] = val
        elif section == "strengths" and len(row) >= 2:
            strength_name = row[0].strip()
            strength_detail = row[1].strip() if len(row) > 1 else ""
            if strength_name:
                candidate["strengths"].append((strength_name, strength_detail))

    # 検索条件を自動生成
    candidate["conditions"] = _extract_conditions(candidate)

    return candidate


def _extract_conditions(candidate: Dict) -> Dict:
    """候補者情報から検索条件を自動抽出"""
    info = candidate.get("info", {})
    strengths = candidate.get("strengths", [])

    conditions = {
        "keywords": [],
        "location": "大阪",
        "salary_min": 0,
        "salary_max": 0,
        "age": 0,
        "prefer_kansai": True,
        "extra_keywords": [],
    }

    # 年齢
    age_str = info.get("年齢", "")
    age_match = re.search(r'(\d+)', age_str)
    if age_match:
        conditions["age"] = int(age_match.group(1))

    # 年収
    salary_str = info.get("現年収", "")
    salary_nums = re.findall(r'[\d,]+', salary_str.replace(",", ""))
    if salary_nums:
        salary_val = int(salary_nums[0])
        conditions["salary_min"] = int(salary_val * 0.9)  # 現年収の90%以上
        conditions["salary_max"] = int(salary_val * 1.5)  # 現年収の150%まで

    # 役割・職種からキーワード生成
    role = info.get("役割", "")
    department = info.get("所属部署", "")
    position = info.get("役職", "")

    if role:
        conditions["keywords"].append(role)

    # 部署名からキーワード抽出
    dept_keyword_map = {
        "クリエイティブ": ["Webデザイナー", "デザイン", "クリエイティブ"],
        "UX推進": ["UXデザイン", "UXディレクター", "UI/UX"],
        "マーケティング": ["Webマーケティング", "デジタルマーケティング", "広告運用"],
        "デジタルマーケティング": ["デジタルマーケティング", "Web広告", "マーケティング"],
        "コンサルティング": ["コンサルタント", "コンサルティング営業", "法人営業"],
    }

    for dept_key, kws in dept_keyword_map.items():
        if dept_key in department:
            conditions["keywords"].extend(kws)
            break

    # 役職からキーワード追加
    if position and position != "一般":
        position_map = {
            "事業部長": ["事業部長", "マネージャー", "管理職", "部長"],
            "チームリーダー": ["チームリーダー", "マネージャー", "リーダー"],
        }
        for pos_key, kws in position_map.items():
            if pos_key in position:
                conditions["keywords"].extend(kws)
                break

    # 強みからキーワード抽出
    strength_keywords = set()
    for name, detail in strengths:
        text = f"{name} {detail}"
        # 重要なキーワードを抽出
        important_terms = [
            "Web広告", "SNS広告", "Google広告", "LP制作", "LPO",
            "サイト制作", "コンバージョン", "SEO", "CRM", "MA",
            "データ分析", "プロジェクト管理", "クライアント",
            "営業", "マネジメント", "コンサルティング",
            "UXリサーチ", "UI設計", "ワイヤーフレーム",
        ]
        for term in important_terms:
            if term in text:
                strength_keywords.add(term)

    conditions["extra_keywords"] = list(strength_keywords)

    # 重複除去
    conditions["keywords"] = list(dict.fromkeys(conditions["keywords"]))

    return conditions


def get_candidate_display_name(filepath: str) -> str:
    """ファイル名から表示用の候補者名を取得"""
    basename = os.path.basename(filepath)
    # 例: "01_候補者1_53歳_デザイナー.csv" -> "候補者1 (53歳・デザイナー)"
    match = re.match(r'\d+_(候補者\d+)_(\d+歳)_(.+)\.csv', basename)
    if match:
        return f"{match.group(1)} ({match.group(2)}・{match.group(3)})"
    return basename


def load_all_candidates() -> List[Dict]:
    """全候補者CSVを読み込み"""
    csvs = list_candidate_csvs()
    candidates = []
    for path in csvs:
        cand = load_candidate_csv(path)
        if cand:
            cand["display_name"] = get_candidate_display_name(path)
            candidates.append(cand)
    return candidates
