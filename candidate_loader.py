"""
候補者情報の読み込みモジュール（マルチフォーマット対応）
- CSV / Excel
- テキストファイル（.txt / .md）
- PDF
- 画像（.png / .jpg / .jpeg）→ OCR
- Streamlitアップロード（バイナリ）

個人情報（氏名・連絡先・住所等）は自動除外し、
マッチングに必要な情報のみ抽出する。
"""

import csv
import os
import re
import glob
import io
from typing import List, Dict, Optional, Tuple

CSV_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================
# 個人情報フィルタ
# ============================================================

# 個人情報として除外する項目キーワード
PERSONAL_INFO_KEYS = [
    "氏名", "名前", "フルネーム", "本名", "候補者名",
    "電話", "携帯", "TEL", "tel", "Phone", "phone",
    "メール", "メアド", "Email", "email", "e-mail",
    "住所", "自宅", "現住所", "郵便番号", "〒",
    "生年月日", "誕生日", "生まれ",
    "マイナンバー", "保険証", "免許証",
    "LINE", "line", "SNS",
    "家族", "配偶者", "扶養",
]

# テキストから除去する個人情報パターン（正規表現）
_PERSONAL_PATTERNS = [
    r'[\w.+-]+@[\w.-]+\.\w+',                    # メールアドレス
    r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}',      # 電話番号
    r'〒?\d{3}[-\s]?\d{4}',                       # 郵便番号
    r'(?:東京都|北海道|(?:京都|大阪)府|.{2,3}県).{1,6}[市区町村郡].{1,20}[\d丁目番号-]+',  # 住所
]


def _is_personal_info(key: str) -> bool:
    """項目名が個人情報に該当するかチェック"""
    key_lower = key.strip().lower()
    for pi_key in PERSONAL_INFO_KEYS:
        if pi_key.lower() in key_lower:
            return True
    return False


def _remove_personal_from_text(text: str) -> str:
    """テキストから個人情報パターンを除去"""
    cleaned = text
    for pattern in _PERSONAL_PATTERNS:
        cleaned = re.sub(pattern, "[個人情報除外]", cleaned)
    return cleaned


# ============================================================
# マッチング用キーワード抽出
# ============================================================

# 職種・スキル関連のキーワード辞書
_SKILL_KEYWORDS = [
    # デザイン
    "Webデザイナー", "UIデザイナー", "UXデザイナー", "UI/UX",
    "グラフィックデザイナー", "デザイン", "クリエイティブ",
    "Figma", "Photoshop", "Illustrator", "XD",
    # マーケティング
    "Webマーケティング", "デジタルマーケティング", "マーケター",
    "Web広告", "SNS広告", "Google広告", "Meta広告", "リスティング広告",
    "SEO", "SEM", "LPO", "CRO", "MA", "CRM",
    "コンテンツマーケティング", "SNS運用", "広告運用",
    # 開発・IT
    "エンジニア", "プログラマー", "SE", "フロントエンド", "バックエンド",
    "HTML", "CSS", "JavaScript", "Python", "React", "Vue",
    "WordPress", "EC", "ECサイト",
    # ビジネス
    "営業", "法人営業", "コンサルタント", "コンサルティング",
    "プロジェクトマネージャー", "PM", "ディレクター", "プロデューサー",
    "企画", "事業企画", "経営企画",
    # 管理
    "マネジメント", "チームリーダー", "管理職", "事業部長", "部長",
    "人事", "採用", "総務", "経理", "労務",
    # 制作
    "LP制作", "サイト制作", "コーディング", "ライティング",
    "動画制作", "映像制作", "写真撮影",
    # 分析
    "データ分析", "アクセス解析", "Google Analytics", "KPI",
    "プロジェクト管理", "業務改善",
    # 業界
    "医療", "ヘルスケア", "不動産", "金融", "教育", "EC",
    "BtoB", "BtoC", "SaaS", "IT", "Web",
]

# 部署名 → キーワード マッピング
_DEPT_KEYWORD_MAP = {
    "クリエイティブ": ["Webデザイナー", "デザイン", "クリエイティブ"],
    "UX推進": ["UXデザイン", "UXディレクター", "UI/UX"],
    "マーケティング": ["Webマーケティング", "デジタルマーケティング", "広告運用"],
    "デジタルマーケティング": ["デジタルマーケティング", "Web広告", "マーケティング"],
    "コンサルティング": ["コンサルタント", "コンサルティング営業", "法人営業"],
    "営業": ["営業", "法人営業", "ソリューション営業"],
    "開発": ["エンジニア", "開発", "プログラマー"],
    "企画": ["企画", "事業企画", "プロデューサー"],
    "人事": ["人事", "採用", "HR"],
    "管理": ["管理職", "マネジメント", "部長"],
}

# 役職 → キーワード マッピング
_POSITION_KEYWORD_MAP = {
    "事業部長": ["事業部長", "マネージャー", "管理職", "部長"],
    "チームリーダー": ["チームリーダー", "マネージャー", "リーダー"],
    "マネージャー": ["マネージャー", "管理職", "リーダー"],
    "ディレクター": ["ディレクター", "マネージャー"],
    "リーダー": ["リーダー", "チームリーダー"],
}


def _extract_keywords_from_text(text: str) -> List[str]:
    """テキストからマッチング用キーワードを抽出"""
    found = []
    for kw in _SKILL_KEYWORDS:
        if kw.lower() in text.lower():
            found.append(kw)
    return list(dict.fromkeys(found))  # 重複除去・順序保持


def _extract_age(text: str) -> int:
    """テキストから年齢を抽出"""
    m = re.search(r'(?:年齢|Age)[：:\s]*(\d{1,2})歳?', text)
    if m:
        return int(m.group(1))
    m = re.search(r'(\d{2})歳', text)
    if m:
        age = int(m.group(1))
        if 18 <= age <= 70:
            return age
    return 0


def _extract_salary(text: str) -> Tuple[int, int]:
    """テキストから年収を抽出して（min, max）を返す"""
    # 「現年収: 380万円」「年収380万」パターン
    m = re.search(r'(?:現年収|年収|想定年収|希望年収)[：:\s]*(\d{2,4})万', text)
    if m:
        val = int(m.group(1))
        return (int(val * 0.9), int(val * 1.5))
    # 「300万〜500万」パターン
    m = re.search(r'(\d{2,4})万[円〜~～-]+(\d{2,4})万', text)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return (0, 0)


def _extract_location(text: str) -> str:
    """テキストから希望勤務地を抽出"""
    m = re.search(r'(?:希望勤務地|勤務地|勤務希望)[：:\s]*(.+?)(?:\n|$|、|。)', text)
    if m:
        return m.group(1).strip()[:10]
    # 関西圏のキーワードがあれば
    kansai = ["大阪", "京都", "神戸", "兵庫", "奈良"]
    for loc in kansai:
        if loc in text:
            return loc
    return "大阪"


def _build_conditions(info: Dict, strengths: List[Tuple[str, str]],
                      full_text: str) -> Dict:
    """候補者データから検索条件を自動構築"""
    conditions = {
        "keywords": [],
        "location": "大阪",
        "salary_min": 0,
        "salary_max": 0,
        "age": 0,
        "prefer_kansai": True,
        "extra_keywords": [],
    }

    # --- info辞書からの抽出 ---
    # 年齢
    age_str = info.get("年齢", "")
    age_m = re.search(r'(\d+)', age_str)
    if age_m:
        conditions["age"] = int(age_m.group(1))

    # 年収
    salary_str = info.get("現年収", "") or info.get("年収", "") or info.get("希望年収", "")
    salary_nums = re.findall(r'[\d,]+', salary_str.replace(",", ""))
    if salary_nums:
        val = int(salary_nums[0])
        conditions["salary_min"] = int(val * 0.9)
        conditions["salary_max"] = int(val * 1.5)

    # 役割・部署・役職
    role = info.get("役割", "") or info.get("職種", "") or info.get("希望職種", "")
    department = info.get("所属部署", "") or info.get("部署", "")
    position = info.get("役職", "") or info.get("ポジション", "")

    if role:
        conditions["keywords"].append(role)

    for dept_key, kws in _DEPT_KEYWORD_MAP.items():
        if dept_key in department:
            conditions["keywords"].extend(kws)
            break

    if position and position != "一般":
        for pos_key, kws in _POSITION_KEYWORD_MAP.items():
            if pos_key in position:
                conditions["keywords"].extend(kws)
                break

    # 勤務地
    loc = info.get("希望勤務地", "") or info.get("勤務地", "")
    if loc:
        conditions["location"] = loc

    # --- 強みからの抽出 ---
    strength_text = " ".join(f"{n} {d}" for n, d in strengths)
    conditions["extra_keywords"] = _extract_keywords_from_text(strength_text)

    # --- full_text からの補完（info/strengthsで不足する場合） ---
    if not conditions["age"]:
        conditions["age"] = _extract_age(full_text)
    if not conditions["salary_min"]:
        conditions["salary_min"], conditions["salary_max"] = _extract_salary(full_text)
    if not conditions["keywords"]:
        conditions["keywords"] = _extract_keywords_from_text(full_text)[:5]
    if not conditions["extra_keywords"]:
        conditions["extra_keywords"] = _extract_keywords_from_text(full_text)[5:10]

    # 勤務地の補完
    if conditions["location"] == "大阪":
        extracted_loc = _extract_location(full_text)
        if extracted_loc != "大阪":
            conditions["location"] = extracted_loc

    # 重複除去
    conditions["keywords"] = list(dict.fromkeys(conditions["keywords"]))
    conditions["extra_keywords"] = [
        kw for kw in dict.fromkeys(conditions["extra_keywords"])
        if kw not in conditions["keywords"]
    ]

    return conditions


# ============================================================
# CSV 読み込み（既存フォーマット互換）
# ============================================================

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

    section = None
    full_lines = []

    for row in rows:
        if not row or all(cell.strip() == "" for cell in row):
            continue

        first_cell = row[0].strip()
        full_lines.append(" ".join(cell.strip() for cell in row if cell.strip()))

        if first_cell == "候補者情報" or first_cell == "項目":
            section = "info"
            continue
        elif first_cell in ("候補者の強み", "強み"):
            section = "strengths"
            continue
        elif first_cell in ("転職先候補求人リスト", "No."):
            section = "jobs"
            continue

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

    full_text = "\n".join(full_lines)
    candidate["conditions"] = _build_conditions(
        candidate["info"], candidate["strengths"], full_text
    )
    return candidate


# ============================================================
# テキストファイル読み込み
# ============================================================

def load_candidate_text(text: str, filename: str = "テキスト入力") -> Optional[Dict]:
    """プレーンテキストから候補者情報を抽出"""
    if not text or not text.strip():
        return None

    cleaned = _remove_personal_from_text(text)

    candidate = {
        "name": filename,
        "info": {},
        "strengths": [],
        "keywords": [],
        "extra_keywords": [],
    }

    # 「項目: 値」パターンで info を抽出
    for line in cleaned.split("\n"):
        line = line.strip()
        if not line:
            continue
        # 「項目：値」「項目: 値」パターン
        m = re.match(r'^([^：:]+)[：:](.+)$', line)
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            if key and val and not _is_personal_info(key):
                candidate["info"][key] = val

    # 「強み」「スキル」「経験」セクションの抽出
    strength_section = re.search(
        r'(?:強み|スキル|得意|経験|実績)[：:\s]*\n?((?:[-・●▪■]\s*.+\n?)+)',
        cleaned, re.MULTILINE
    )
    if strength_section:
        for line in strength_section.group(1).split("\n"):
            line = re.sub(r'^[-・●▪■]\s*', '', line).strip()
            if line:
                candidate["strengths"].append((line, ""))

    candidate["conditions"] = _build_conditions(
        candidate["info"], candidate["strengths"], cleaned
    )
    return candidate


# ============================================================
# PDF 読み込み
# ============================================================

def load_candidate_pdf(filepath_or_bytes, filename: str = "PDF") -> Optional[Dict]:
    """PDFから候補者情報を抽出"""
    try:
        import pdfplumber
    except ImportError:
        return None

    text_parts = []
    try:
        if isinstance(filepath_or_bytes, (str, os.PathLike)):
            pdf = pdfplumber.open(filepath_or_bytes)
        else:
            pdf = pdfplumber.open(io.BytesIO(filepath_or_bytes))

        for page in pdf.pages[:20]:  # 最大20ページ
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)

            # テーブルも抽出
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if row:
                        cells = [str(c).strip() for c in row if c]
                        if cells:
                            text_parts.append(" ".join(cells))
        pdf.close()
    except Exception:
        return None

    full_text = "\n".join(text_parts)
    if not full_text.strip():
        return None

    return load_candidate_text(full_text, filename)


# ============================================================
# 画像 読み込み（OCR）
# ============================================================

def load_candidate_image(filepath_or_bytes, filename: str = "画像") -> Optional[Dict]:
    """画像からOCRで候補者情報を抽出"""
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        return None

    try:
        if isinstance(filepath_or_bytes, (str, os.PathLike)):
            img = Image.open(filepath_or_bytes)
        else:
            img = Image.open(io.BytesIO(filepath_or_bytes))

        # OCR実行（日本語 + 英語）
        text = pytesseract.image_to_string(img, lang="jpn+eng")
    except Exception:
        return None

    if not text or not text.strip():
        return None

    return load_candidate_text(text, filename)


# ============================================================
# Excel 読み込み
# ============================================================

def load_candidate_excel(filepath_or_bytes, filename: str = "Excel") -> Optional[Dict]:
    """Excelから候補者情報を抽出"""
    try:
        import pandas as pd
    except ImportError:
        return None

    try:
        if isinstance(filepath_or_bytes, (str, os.PathLike)):
            df = pd.read_excel(filepath_or_bytes, header=None)
        else:
            df = pd.read_excel(io.BytesIO(filepath_or_bytes), header=None)

        lines = []
        for _, row in df.iterrows():
            cells = [str(c).strip() for c in row if pd.notna(c) and str(c).strip()]
            if cells:
                lines.append(",".join(cells))
        text = "\n".join(lines)
    except Exception:
        return None

    if not text.strip():
        return None

    return load_candidate_text(text, filename)


# ============================================================
# 統合: ファイル形式を自動判定して読み込み
# ============================================================

SUPPORTED_EXTENSIONS = {
    ".csv": "CSV",
    ".txt": "テキスト",
    ".md": "テキスト",
    ".pdf": "PDF",
    ".png": "画像",
    ".jpg": "画像",
    ".jpeg": "画像",
    ".xlsx": "Excel",
    ".xls": "Excel",
}


def load_candidate_file(filepath: str) -> Optional[Dict]:
    """ファイルパスから自動判定して候補者情報を読み込み"""
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)

    if ext == ".csv":
        return load_candidate_csv(filepath)
    elif ext in (".txt", ".md"):
        with open(filepath, "r", encoding="utf-8") as f:
            return load_candidate_text(f.read(), filename)
    elif ext == ".pdf":
        return load_candidate_pdf(filepath, filename)
    elif ext in (".png", ".jpg", ".jpeg"):
        return load_candidate_image(filepath, filename)
    elif ext in (".xlsx", ".xls"):
        return load_candidate_excel(filepath, filename)
    return None


def load_candidate_upload(file_bytes: bytes, filename: str) -> Optional[Dict]:
    """Streamlitアップロードファイルから候補者情報を読み込み"""
    ext = os.path.splitext(filename)[1].lower()

    if ext == ".csv":
        text = file_bytes.decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text)))
        # 既存CSV形式かチェック（候補者情報セクションがあるか）
        has_section = any(
            row and row[0].strip() in ("候補者情報", "項目", "候補者の強み", "強み")
            for row in rows
        )
        if has_section:
            # 一時ファイル不要: テキストとして処理
            candidate = {
                "name": filename,
                "info": {},
                "strengths": [],
                "keywords": [],
                "extra_keywords": [],
            }
            section = None
            full_lines = []
            for row in rows:
                if not row or all(cell.strip() == "" for cell in row):
                    continue
                first_cell = row[0].strip()
                full_lines.append(" ".join(c.strip() for c in row if c.strip()))
                if first_cell in ("候補者情報", "項目"):
                    section = "info"
                    continue
                elif first_cell in ("候補者の強み", "強み"):
                    section = "strengths"
                    continue
                elif first_cell in ("転職先候補求人リスト", "No."):
                    section = "jobs"
                    continue
                if section == "info" and len(row) >= 2:
                    key = row[0].strip()
                    val = row[1].strip()
                    if key and val and not _is_personal_info(key):
                        candidate["info"][key] = val
                elif section == "strengths" and len(row) >= 2:
                    sn = row[0].strip()
                    sd = row[1].strip() if len(row) > 1 else ""
                    if sn:
                        candidate["strengths"].append((sn, sd))
            full_text = "\n".join(full_lines)
            candidate["conditions"] = _build_conditions(
                candidate["info"], candidate["strengths"], full_text
            )
            return candidate
        else:
            # 非定型CSV → テキストとして処理
            return load_candidate_text(text, filename)

    elif ext in (".txt", ".md"):
        text = file_bytes.decode("utf-8-sig")
        return load_candidate_text(text, filename)

    elif ext == ".pdf":
        return load_candidate_pdf(file_bytes, filename)

    elif ext in (".png", ".jpg", ".jpeg"):
        return load_candidate_image(file_bytes, filename)

    elif ext in (".xlsx", ".xls"):
        return load_candidate_excel(file_bytes, filename)

    return None


# ============================================================
# 既存CSVファイルの一括読み込み（後方互換）
# ============================================================

def list_candidate_csvs() -> List[str]:
    """候補者CSVファイルの一覧を取得"""
    pattern = os.path.join(CSV_DIR, "[0-9][0-9]_候補者*.csv")
    return sorted(glob.glob(pattern))


def get_candidate_display_name(filepath: str) -> str:
    """ファイル名から表示用の候補者名を取得"""
    basename = os.path.basename(filepath)
    match = re.match(r'\d+_(候補者\d+)_(\d+歳)_(.+)\.csv', basename)
    if match:
        return f"{match.group(1)} ({match.group(2)}・{match.group(3)})"
    # 拡張子を除いたファイル名
    name = os.path.splitext(basename)[0]
    # 番号プレフィックスを除去
    name = re.sub(r'^\d+[_\-]', '', name)
    return name


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
