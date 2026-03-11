"""
AI生成モジュール — シニア人材コンサルタントペルソナ

ペルソナ: 業界歴10年以上のシニア人材コンサルタント
構造: C.E.R. (Connect→Evidence→Reframe)
出力: 200字以上の構造化テキスト
"""

import random
import re
from datetime import datetime


# ============================================================
# トーン&マナー管理
# ============================================================

_OPENING_PHRASES = [
    "エージェントとして率直に申し上げますと",
    "市場の需給バランスを鑑みると",
    "私の経験則から申し上げると",
]

_BRIDGE_PHRASES = [
    "という経験を、貴社の課題と読み替えると",
    "という実績は、まさに今回の募集背景に直結します",
    "の視点で見ると、相互にメリットが見えてきます",
]

_EVIDENCE_PHRASES = [
    "具体的なエビデンスとして",
    "過去の実績を紐解くと",
    "職務経歴から抽出すると",
]

_REFRAME_PHRASES = [
    "むしろポジティブに捉えると",
    "この点を「成長への意欲」と読み替えれば",
    "逆説的ですが、これは強みに転換できます",
]

# 禁止ワード→推奨表現 変換
_REPLACEMENTS = [
    ("見つかりませんでした", "現時点では条件に完全一致する候補はありませんが、視点を変えると以下の可能性が見えてきます"),
    ("思います", "考えられます"),
    ("多分", "市場動向を踏まえると"),
    ("わかりません", "より詳細な情報があれば精度の高い分析が可能です"),
]


def _phrase(category):
    pool = {
        "opening": _OPENING_PHRASES,
        "bridge": _BRIDGE_PHRASES,
        "evidence": _EVIDENCE_PHRASES,
        "reframe": _REFRAME_PHRASES,
    }
    items = pool.get(category, [])
    return random.choice(items) if items else ""


def _reframe_text(text):
    for old, new in _REPLACEMENTS:
        text = text.replace(old, new)
    return text


def _get_candidate_fields(candidate):
    """候補者データから主要フィールドを安全に取得（タグ情報対応）"""
    info = candidate.get("info", {})
    strengths = candidate.get("strengths", [])
    conditions = candidate.get("conditions", {})
    tags = candidate.get("tags", {})

    name = candidate.get("name", "候補者")

    # hardSkills: strengthsの最初の要素やinfoから推定
    hard_skills = []
    soft_skills = []
    for s in strengths:
        if isinstance(s, (list, tuple)) and len(s) >= 2:
            hard_skills.append(s[0])
        elif isinstance(s, str):
            hard_skills.append(s)

    # タグからスキルを補完
    if tags.get("skills"):
        for skill in tags["skills"]:
            if skill not in hard_skills:
                hard_skills.append(skill)

    # タグから面談タグ→ソフトスキルを補完
    if tags.get("interview_tags"):
        soft_skills.extend(tags["interview_tags"])

    # infoからの情報取得
    job_type = info.get("職種", info.get("専門", info.get("経験領域", "")))
    experience = info.get("経験年数", info.get("経験", ""))
    if not experience and tags.get("experience_years"):
        experience = f"{tags['experience_years']}年"
    salary = conditions.get("salary_text", f"{conditions.get('salary_min', 300)}万〜{conditions.get('salary_max', 600)}万円")
    location = conditions.get("location", "")
    keywords = conditions.get("keywords", [])

    # negativeChecks: infoの注意点や懸念
    negatives = []
    for k, v in info.items():
        if any(w in k for w in ["注意", "懸念", "リスク", "ネガティブ"]):
            negatives.append(str(v))

    # 転職回数によるリスク検出
    job_count = tags.get("job_history_count", 0)
    exp_years = tags.get("experience_years", 0)
    if job_count >= 5 and exp_years > 0 and exp_years / job_count < 2:
        negatives.append(f"転職回数が多い（{job_count}社 / {exp_years}年）")

    # marketScore推定 (タグ情報で精緻化)
    base_score = 50
    base_score += min(20, len(hard_skills) * 4)  # スキル数
    base_score += min(10, len(keywords) * 2)  # キーワード数
    if tags.get("certifications"):
        base_score += min(10, len(tags["certifications"]) * 3)
    if tags.get("management", {}).get("has_experience"):
        base_score += 5
    if tags.get("languages"):
        base_score += min(5, len(tags["languages"]) * 3)
    if tags.get("experience_level") in ("シニア", "リード", "エグゼクティブ"):
        base_score += 5
    if tags.get("achievements"):
        base_score += min(5, len(tags["achievements"]) * 2)
    market_score = min(100, base_score)

    return {
        "name": name,
        "job_type": job_type or (keywords[0] if keywords else "専門領域"),
        "experience": experience or "経験あり",
        "hard_skills": hard_skills[:10],
        "soft_skills": soft_skills[:8],
        "negatives": negatives,
        "market_score": market_score,
        "salary": salary,
        "location": location,
        "keywords": keywords,
        "resume_summary": info.get("職務要約", info.get("概要", "")),
        "certifications": tags.get("certifications", []),
        "industries": tags.get("industries", []),
        "languages": tags.get("languages", []),
        "management": tags.get("management", {}),
        "experience_level": tags.get("experience_level", ""),
        "achievements": tags.get("achievements", []),
        "education": tags.get("education", {}),
        "work_styles": tags.get("work_styles", []),
        "availability": tags.get("availability", ""),
        "career_change_reasons": tags.get("career_change_reasons", []),
    }


# ============================================================
# 1. スカウトメッセージ生成
# ============================================================
def generate_scout_message(candidate):
    f = _get_candidate_fields(candidate)
    skills_str = "・".join(f["hard_skills"][:3]) or "豊富なスキル"
    negatives = f["negatives"]

    reframe_section = ""
    if negatives and any(w in negatives[0] for w in ["離職", "転職", "短期"]):
        reframe_section = f"""
■ 改めてお伝えしたいこと
ご経歴を拝見し、直近のご転職について触れさせてください。{_phrase('reframe')}、**「自らのキャリアに真摯に向き合い、より良い環境を求められた」**という前向きな姿勢の表れと受け止めております。"""

    return f"""【{f['name']}様へのスカウトメッセージ】

{f['name']}様

{_phrase('opening')}、{f['name']}様のご経験は現在の人材市場において非常に高い価値を持っています。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ ご経歴への着目ポイント（Connect）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{f['job_type']}領域での{f['experience']}のご経験、特に**{skills_str}**のスキルセットは、現在私がお手伝いしている複数の企業が「まさにこういう方を探していた」と声を揃える人材像です。

{_phrase('evidence')}、{f['resume_summary'][:100] if f['resume_summary'] else '貴殿の職務経験は、単なる業務遂行に留まらず、組織への具体的な成果貢献が読み取れます'}。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 市場価値の客観評価（Evidence）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

・市場決まりやすさスコア：**{f['market_score']}%**（上位30%圏内）
・想定オファー年収帯：{f['salary']}（現市場相場と整合）
・複数企業からの好条件オファー獲得確度：**高**
{reframe_section}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ ご提案したい求人の特徴
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

現在、年収{f['salary']}という条件で、{f['name']}様の志向性に合致する求人を複数確保しております。いずれも**「経験者を本気で採りたい」**という企業様です。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 次のステップ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

まずは15〜20分程度のオンライン面談で、ご希望やキャリアの方向性をお伺いできればと存じます。

ご都合の良い日時を2〜3候補いただけますと幸いです。
何卒よろしくお願いいたします。"""


# ============================================================
# 2. 懸念点・確認質問生成
# ============================================================
def generate_concerns(candidate):
    f = _get_candidate_fields(candidate)
    negatives = f["negatives"]

    concern1 = {
        "concern": "転職動機の深さ",
        "analysis": '現職に明確な不満がない場合、選考途中での辞退リスクがあります。**「なぜ今なのか」**の納得解を得ることが重要です。',
        "reframe": '逆に言えば、明確な動機があれば決定までスムーズに進みやすい候補者です。',
        "question": '「今回転職活動を始められたきっかけは何ですか？現職では満たせない、具体的な目標や環境の変化があればお聞かせください。」',
    }
    if negatives and any(w in negatives[0] for w in ["短期", "離職", "転職"]):
        concern1 = {
            "concern": "直近の短期離職",
            "analysis": '表面的には「定着性への懸念」と映りますが、**深層にある本当の理由**を確認する必要があります。組織フェーズのミスマッチや、キャリア軸の再定義による戦略的判断の可能性もあります。',
            "reframe": 'むしろ「ミスマッチを避けて次に進む決断力」として評価できるケースも多いです。',
            "question": '「前職での離職理由について、ご自身が最も重視していた軸は何でしたか？」',
        }

    return f"""【{f['name']}様の懸念点分析と確認質問】

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ エージェントとしての所見
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{_phrase('opening')}、{f['name']}様は総合的に見てマッチング確度の高い候補者です。ただし、企業への推薦前に以下の点を確認・整理することで、**選考通過率と決定率を最大化**できると考えます。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 懸念点の深掘り分析（C.E.R.構造）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【懸念1】{concern1['concern']}

📊 **分析（Evidence）**
{concern1['analysis']}

🔄 **再定義（Reframe）**
{concern1['reframe']}

💬 **確認質問**
{concern1['question']}

【懸念2】希望条件の柔軟性

📊 **分析（Evidence）**
現在の希望（年収{f['salary']}、勤務地{f['location'] or '応相談'}）が固定的な場合、マッチする求人が限定される可能性があります。

🔄 **再定義（Reframe）**
条件交渉の余地を確認することで、提案幅が広がります。

💬 **確認質問**
「ご希望条件について、優先順位をつけるとすればどのような順番になりますか？また、どの条件なら相談の余地がありますか？」

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 追加確認質問（深掘り用）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

3. 「5年後のキャリアイメージを教えてください。どのようなスキルや役職を獲得していたいですか？」
   → **キャリアビジョンの一貫性**を確認

4. 「チームで仕事をする際、どのようなポジション（リーダー/サポーター/独立型）を取ることが多いですか？」
   → **組織適合度**の推測材料

5. 「今回ご紹介する求人で、特に確認したい点や気になる点はありますか？」
   → **隠れた懸念や条件**の把握

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 企業への伝え方の準備
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

上記の確認を経て、{f['name']}様の**「転職軸」と「キャリアビジョン」**を明確に言語化できれば、企業への推薦文において「この方でなければならない理由」を説得力をもって伝えられます。"""


# ============================================================
# 3. 決まりやすさ分析
# ============================================================
def generate_hireability(candidate):
    f = _get_candidate_fields(candidate)
    ms = f["market_score"]
    skills_str = "、".join(f["hard_skills"]) or "マルチスキル"

    if ms >= 80:
        market_analysis = "**プレミアム人材**として複数企業から引く手あまたの状態です。早期アプローチと条件提示のスピードが鍵となります。"
    elif ms >= 70:
        market_analysis = "**優良人材**として、複数の選択肢から最適な環境を選べる立場にあります。企業の魅力訴求が重要です。"
    else:
        market_analysis = "適切なマッチングと丁寧なフォローにより、高い決定率が期待できます。"

    neg_section = "✅ 特筆すべきリスク要因は見当たりません。"
    if f["negatives"]:
        neg_section = f"""⚠️ 「{f['negatives'][0]}」について
{_phrase('reframe')}、これは企業への事前説明で十分にカバー可能です。面談時のフォローで解消を図ります。"""

    return f"""【{f['name']}様の決まりやすさ分析】

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 市場価値サマリ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

市場決まりやすさスコア：**{ms}%** {'⭐' if ms >= 70 else ''}
（{('上位20%の希少人材' if ms >= 80 else '上位40%の優良人材') if ms >= 70 else '条件次第で高マッチ可能'}）

{_phrase('opening')}、{f['name']}様は{market_analysis}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 決まりやすい理由（3つの視点）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【1. 市場需給の観点】
・{f['job_type']}人材の需要は現在**高水準**で推移しており、供給が追いついていません
・{skills_str}のスキルセットは、複数業界から求められる汎用性があります
・市場の需給バランスを鑑みると、**複数オファーの獲得確率は高い**と判断できます

【2. 条件マッチングの観点】
・希望年収帯（{f['salary']}）が市場相場と整合しており、条件面でのミスマッチが起きにくい
・勤務地（{f['location'] or '柔軟'}）の選択肢が広い
・入社時期の柔軟性があれば、企業側の採用タイミングに合わせやすい

【3. ソフトスキル・人物像の観点】
・書類・面談での客観的な説明力が高いと推測される経歴構成
・**「一緒に働きたい」**と思わせる人間的魅力がプラスに作用する見込み

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ リスク要因と対策
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{neg_section}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 推奨アクションシナリオ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

**【フェーズ1】初動（〜1週間）**
・厳選した2〜3社への同時応募で市場反応を確認
・書類選考結果を待つ間に、想定質問への回答準備をサポート

**【フェーズ2】選考進行（1〜2週間）**
・1次面接後、企業評価と候補者所感をすり合わせ
・必要に応じて希望条件の微調整を提案

**【フェーズ3】決定（3〜4週間）**
・内定獲得後、条件交渉で上積みを狙う
・複数内定の場合は比較軸を整理し最適解を導く

📅 決定までの予測期間: **2〜4週間**"""


# ============================================================
# 4. 提案用レジュメ/推薦文生成
# ============================================================
def generate_proposal_resume(candidate, job=None):
    f = _get_candidate_fields(candidate)
    job_title = job.get("title", "本ポジション") if job else "本ポジション"
    job_desc = job.get("description", "") if job else ""
    score = candidate.get("match_score", 80)

    bridging = f"""
本候補者の最大の武器は、**単なる業務遂行者ではなく「課題解決型」のスタンス**にあります。{f['resume_summary'][:80] + 'という経験は、' if f['resume_summary'] else ''}{f'「{job_title}」が抱える課題に対し、即座に価値を発揮できると確信しています。' if job_desc else '御社の組織課題に対し、具体的な成果で貢献できる人材です。'}"""

    neg_section = ""
    if f["negatives"] and any(w in f["negatives"][0] for w in ["離職", "短期"]):
        neg_section = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 懸念点の再定義（Reframe）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

直近の退職について、率直にご説明させてください。
これは**本人の志向と組織フェーズのミスマッチ**によるものであり、スキルや人間性の問題ではありませんでした。
{_phrase('reframe')}、今回は**「自ら市場を切り拓きたい」**という強い覚悟を持って選考に臨んでいます。"""

    return f"""【推薦状】{f['name']} → {job_title}

══════════════════════════════════════════════════════
■ 推薦理由サマリ（マッチ度：{score}%）
══════════════════════════════════════════════════════

{_phrase('opening')}、{f['name']}様を**強く推薦**いたします。
{bridging}

══════════════════════════════════════════════════════
■ なぜ今、貴社にこの方なのか（Connect）
══════════════════════════════════════════════════════

【貴社の課題】
{'「' + job_desc[:60] + '...」という背景から、' if job_desc else '即戦力人材の確保と組織力強化が急務と推察します。'}

【候補者の強み】
1. **{f['job_type']}での実践経験**：理論だけでなく現場で成果を出してきた再現性
2. **{f['hard_skills'][0] if f['hard_skills'] else '専門スキル'}の深い知見**：導入から運用まで一気通貫で対応可能
3. **変化への適応力**：新しい環境でも早期にキャッチアップし成果を出す姿勢

【ブリッジング】
{f['name']}様の経験を御社の文脈で読み替えると、**{job_title}において即座に機能する即戦力**として期待できます。
{neg_section}

══════════════════════════════════════════════════════
■ 本ポジションとの適合度
══════════════════════════════════════════════════════

| 評価軸 | 評価 | コメント |
|:--|:--:|:--|
| ハードスキル | ★★★★☆ | {f['hard_skills'][0] if f['hard_skills'] else '専門スキル'}が強み |
| ソフトスキル | ★★★★★ | 高評価ポイント |
| カルチャーフィット | ★★★★☆ | 適応力あり |
| 条件マッチ | ★★★★☆ | 希望年収{f['salary']}、交渉余地あり |

══════════════════════════════════════════════════════
■ 次のアクション推奨
══════════════════════════════════════════════════════

📅 **書類選考通過後、1週間以内に1次面接を設定**することを推奨します。

💡 他社選考も並行して進行中の可能性が高く、**スピード感を持った対応**が内定獲得の鍵となります。"""


# ============================================================
# 5. 面談解析レポート
# ============================================================

_TAG_PATTERNS = [
    (r"リーダー|マネジ|管理|統括", "#マネジメント経験"),
    (r"営業|セールス|商談|受注", "#営業力"),
    (r"企画|プランニング|戦略", "#企画力"),
    (r"開発|エンジニア|プログラ", "#開発経験"),
    (r"コミュニケーション|対話|折衝", "#対人スキル"),
    (r"分析|データ|数値", "#分析力"),
    (r"挑戦|チャレンジ|新規", "#挑戦志向"),
    (r"成長|学習|スキルアップ", "#成長意欲"),
    (r"チーム|協力|連携", "#チームワーク"),
    (r"改善|効率|最適化", "#改善推進力"),
    (r"医療|クリニック|病院|看護", "#医療系"),
    (r"採用|人事|HR", "#人事経験"),
]

_DEFAULT_TAGS = ["#即戦力", "#ポテンシャル", "#安定志向", "#柔軟性", "#論理的思考"]


def generate_interview_analysis(transcript, candidate=None):
    """面談テキストからタグ抽出 + 構造化レポート生成"""
    name = candidate.get("name", "候補者") if candidate else "候補者"

    # タグ抽出
    tags = []
    for pattern, tag in _TAG_PATTERNS:
        if re.search(pattern, transcript):
            tags.append(tag)
    # 最低5つ確保
    for dt in _DEFAULT_TAGS:
        if dt not in tags and len(tags) < 5:
            tags.append(dt)

    f = _get_candidate_fields(candidate) if candidate else {"hard_skills": [], "job_type": "専門領域"}

    report = f"""【面談解析レポート】{name}様

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ エージェント所見
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{_phrase('opening')}、{name}様は**企業が「会ってみたい」と思える要素**を十分に備えた候補者です。面談内容を分析した結果、以下のポイントが浮かび上がりました。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ マッチングタグ（抽出キーワード）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{' '.join(tags)}

これらのタグは、求人とのマッチング精度を高めるキーワードとして活用できます。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 決定シナリオ予測
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【この候補者が響く口説き文句】
・「あなたの{f.get('hard_skills', ['専門性'])[0] if f.get('hard_skills') else '専門性'}を存分に発揮できる環境です」
・「裁量を持って新しいことに挑戦できるポジションです」
・「成長フェーズの企業で、将来の幹部候補として期待しています」

【提示すべきキャリアパス】
・**短期（1年）**: 即戦力として成果を出し、チーム内での信頼を獲得
・**中期（3年）**: マネジメントまたはスペシャリストとしてのキャリア選択
・**長期（5年）**: 組織を牽引するリーダーポジション、または専門領域の第一人者

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 深掘り質問案（次回面談用）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 「これまでのキャリアで最も困難だった局面と、それをどう乗り越えましたか？」
   → **ストレス耐性・問題解決力**の確認

2. 「チームで意見が対立した際、どのように解決に導きましたか？」
   → **コンフリクトマネジメント能力**の確認

3. 「5年後、どのようなスキルや立場を獲得していたいですか？」
   → **キャリアビジョンの一貫性**の確認

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 推奨アクション
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 本レポートを踏まえ、マッチ度の高い求人を選定
2. 選定した求人の推薦文を作成し、1週間以内に書類提出を目指す"""

    return {"report": report, "tags": tags}


# ============================================================
# 6. 進捗停滞分析
# ============================================================
def generate_progress_analysis(proposal, candidate=None):
    status = proposal.get("status", "提案済み")
    name = candidate.get("name", "候補者") if candidate else "候補者"
    job_title = proposal.get("job_title", "求人")

    return f"""【進捗分析レポート】{name}様 → {job_title}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 現在のステータス: 📊 **{status}**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 停滞原因の3軸分析
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【1. 市場環境要因】
・同ポジションの競争率が高く、選考に時間がかかっている可能性
・採用予算の見直しや組織変更が発生している可能性

【2. 候補者の意向要因】
・他社選考との並行により、優先順位が下がっている可能性
・現職からの引き止めや条件改善の提示を受けている可能性

【3. 企業の選考スピード要因】
・面接官のスケジュール調整に時間がかかっている
・社内稟議や意思決定プロセスの複雑さ

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 推進アドバイス（具体的アクション）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📞 **今すぐできること**
1. {name}様への連絡：他社選考状況と現在の優先順位を再確認
2. 企業人事への確認：選考状況と次回ステップの見込み日程
3. 関係構築：業界ニュースや事例記事を送付し、接点を維持

📋 **1週間以内に実施**
1. 双方の温度感を再評価し、プッシュすべきか静観すべきか判断
2. 必要に応じて条件面での譲歩余地を確認
3. 決定を後押しする追加情報の提供

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 代替シナリオ（ピボット戦略）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

もし本件が見送りになった場合、{name}様のスキルセットを別の角度から活かせる求人があります。

💡 **ピボット提案の視点**
・職種を別の角度から読み替えると、意外なマッチが見つかることがあります
・業界を変えてスキルを活かす「越境転職」も選択肢として提示"""


# ============================================================
# 7. 求人改善提案
# ============================================================
def generate_job_improvements(job):
    title = job.get("title", "求人")
    salary = job.get("salary", "")
    has_remote = "リモート" in job.get("description", "").lower() or "リモート" in job.get("location", "")

    return f"""【{title}の改善提案】

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 現状分析
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

現状の内容でも応募は見込めますが、**以下の改善で応募数20〜30%増加**が期待できます。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 改善ポイント（優先度順）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【1. 年収表記の透明化】（効果：応募率+15%）
📝 **Before**: 「{salary or '400万〜600万円'}」
📝 **After**: 「{salary or '400万〜600万円'}（経験・能力により決定。前職年収を考慮します）」

💡 年収レンジが広い場合、候補者は「自分はどこに位置するのか」に不安を感じます。判断基準を明示しましょう。

【2. {'リモートワーク方針の具体化' if has_remote else '勤務形態の柔軟性アピール'}】（効果：応募率+20%）
{'📝 「リモート可」→「週2日リモート可（入社3ヶ月後からフルリモートも相談可）」のように具体化しましょう。' if has_remote else '📝 「出社勤務」→「一部リモート相談可、フレックス制度あり」など柔軟性を示しましょう。'}

【3. 募集背景のストーリー化】（効果：興味喚起+25%）
📝 **Before**: 「事業拡大に伴う増員」
📝 **After**: 具体的な成長数値やフェーズを伝えて、「このフェーズに参加したい」という意欲を引き出しましょう。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 追記推奨項目
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ 選考フロー（面接回数・期間の目安）
✅ チーム構成（何名規模・年齢層・雰囲気）
✅ 入社者の声「なぜうちを選んだか」
✅ 1日のスケジュール例

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 表現の改善例
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

| Before | After |
|:--|:--|
| 「営業経験者歓迎」 | 「法人営業2年以上歓迎。異業種出身者も活躍中！」 |
| 「コミュニケーション力がある方」 | 「顧客の課題を引き出し、解決策を提案できる方」 |
| 「成長意欲のある方」 | 「半年後にはチームリーダーを目指したい方」 |

これらの改善により、**質の高い応募が増加**し、選考効率の向上も期待できます。"""


# ============================================================
# Market Fit ⭐ 5軸評価
# ============================================================

# ============================================================
# 8. 自然文チャット応答（generateChatResponse）
# ============================================================

def generate_chat_response(message, context=None):
    """
    自然言語メッセージに対してコンサルタント風の応答を返す。
    context: { "candidate": dict or None, "job": dict or None, "tab": str }
    """
    context = context or {}
    candidate = context.get("candidate")
    job = context.get("job")
    tab = context.get("tab", "")
    msg_lower = message.lower()

    # --- インテント検出 ---

    # スカウト文
    if any(w in msg_lower for w in ["スカウト", "メッセージ", "声かけ"]):
        if candidate:
            return generate_scout_message(candidate)
        return "💡 候補者を選択した状態で「スカウト文を提案して」とお伝えください。選択された候補者に合わせたスカウトメッセージを作成します。"

    # 懸念点
    if any(w in msg_lower for w in ["懸念", "リスク", "確認質問", "心配", "気になる"]):
        if candidate:
            return generate_concerns(candidate)
        return "💡 候補者を選択すると、その方に関する懸念点の分析と確認質問を生成します。"

    # 決まりやすさ
    if any(w in msg_lower for w in ["決まり", "市場価値", "採用可能性", "内定"]):
        if candidate:
            return generate_hireability(candidate)
        return "💡 候補者を選択すると、市場での決まりやすさを分析します。"

    # 推薦文
    if any(w in msg_lower for w in ["推薦", "レジュメ", "紹介文"]):
        if candidate:
            return generate_proposal_resume(candidate, job)
        return "💡 候補者を選択した状態で「推薦文を作成して」とお伝えください。"

    # 求人改善
    if any(w in msg_lower for w in ["改善", "求人票", "応募", "表現"]):
        if job:
            return generate_job_improvements(job)
        return "💡 求人を選択した状態で「求人票を改善して」とお伝えください。"

    # 面談分析
    if any(w in msg_lower for w in ["面談", "面接", "分析", "解析"]):
        if candidate:
            return f"""面談分析のサポートが可能です。

「📝 面談分析」タブで面談内容をテキスト入力していただくと、以下を自動生成します：

• **マッチングタグ**: 候補者の特性をキーワード化
• **決定シナリオ予測**: 口説き文句・キャリアパス提案
• **深掘り質問案**: 次回面談で確認すべきポイント

候補者「{candidate.get('name', '')}」様の面談メモがあれば、こちらに入力してください。"""
        return "💡 「面談分析」タブで候補者を選択し、面談メモを入力すると、AI分析レポートを生成します。"

    # 進捗・提案
    if any(w in msg_lower for w in ["進捗", "提案", "ステータス", "停滞", "動いてない"]):
        return f"""提案の進捗管理についてサポートします。

📊 **確認可能な情報:**
• 全提案のステータス一覧
• 停滞している案件のアラート
• 次アクションの推奨

「提案管理」タブで個別の提案を選択すると、詳しい進捗分析レポートも生成できます。何か特定の案件についてお聞きになりたいことはありますか？"""

    # 比較
    if any(w in msg_lower for w in ["比較", "どっち", "どちら", "違い"]):
        if candidate:
            f = _get_candidate_fields(candidate)
            return f"""**{f['name']}様の特徴サマリ:**

• 職種: {f['job_type']}（経験{f['experience']}）
• スキル: {', '.join(f['hard_skills'][:4])}
• 希望年収: {f['salary']}
• 市場スコア: {f['market_score']}%

他の候補者と比較するには、「候補者検索」タブで複数選択してください。具体的な比較軸（年収、スキル、カルチャーフィットなど）を指定していただくとより詳しく分析できます。"""
        return "💡 比較したい候補者を選択してください。最大3名までの並列比較が可能です。"

    # 年収・条件
    if any(w in msg_lower for w in ["年収", "給与", "条件", "オファー"]):
        if candidate:
            f = _get_candidate_fields(candidate)
            return f"""{_phrase('opening')}、{f['name']}様への条件提示について分析します。

**現在の市場データ:**
• 候補者の希望年収: {f['salary']}
• 市場スコア: {f['market_score']}%（{'競争力のある候補者' if f['market_score'] >= 70 else '条件次第'}）

**推奨オファー戦略:**
{'• 市場スコアが高いため、希望年収に近い提示が望ましいです' if f['market_score'] >= 70 else '• 希望年収の下限付近からの提示で交渉の余地を確保できます'}
• 年収以外の魅力（成長環境、裁量、リモート等）を積極的にアピール
• 他社選考状況を確認し、スピード感のある対応を推奨"""
        return "💡 候補者を選択すると、適正年収や条件提示の戦略を分析できます。"

    # あいさつ
    if any(w in msg_lower for w in ["こんにちは", "おはよう", "はじめまして", "hello", "hi"]):
        return f"""こんにちは！Matchアシスタントです。

以下のサポートが可能です：

**📋 候補者サポート**
• 「スカウト文を提案して」→ パーソナライズされたスカウトメッセージ
• 「懸念点を分析して」→ リスク分析と確認質問
• 「決まりやすさを教えて」→ 市場価値分析
• 「推薦文を作成して」→ 企業向け推薦状

**📋 求人サポート**
• 「求人票を改善して」→ 応募数増加のための改善提案

**💡 ヒント**
候補者や求人を選択した状態で質問すると、より具体的なアドバイスが可能です。"""

    # ヘルプ・使い方
    if any(w in msg_lower for w in ["ヘルプ", "使い方", "何ができる", "機能", "help"]):
        return """**Matchアシスタントの機能一覧：**

🔍 **候補者関連**
• スカウト文の生成
• 懸念点の分析・確認質問の作成
• 市場価値・決まりやすさの分析
• 推薦文の作成

📋 **求人関連**
• 求人票の改善提案
• 候補者とのマッチング分析

📝 **面談関連**
• 面談メモの構造化
• タグの自動抽出
• 深掘り質問の提案

📊 **提案管理**
• 進捗の停滞分析
• 次アクションの推奨

自由に質問してください。候補者・求人を選択した状態だとより具体的な回答が可能です。"""

    # --- デフォルト応答（汎用） ---
    ctx_info = ""
    if candidate:
        f = _get_candidate_fields(candidate)
        ctx_info = f"\n\n現在選択中: **{f['name']}様**（{f['job_type']}、経験{f['experience']}）"
    if job:
        ctx_info += f"\n選択中の求人: **{job.get('title', '')}**"

    return f"""承知しました。{ctx_info}

以下のサポートが可能です：

• 📝 **候補者**: 「スカウト文を提案して」「懸念点を分析して」「決まりやすさを教えて」
• 📋 **求人**: 「求人票を改善して」「○○の求人を探して」
• 🔍 **検索**: 「営業経験のある候補者を出して」「この候補者に合う求人を探して」
• 📊 **分析**: 「年収いくらで提示すべき？」「比較して」

お気軽にどうぞ！"""


# ============================================================
# 10. スマートチャット: インテント検出 + アクション生成
# ============================================================

def detect_chat_action(message, tab="", context=None):
    """
    ユーザーメッセージからインテント（意図）を検出し、アクション辞書を返す。

    Returns:
        {
            "action": str,  # "search_jobs", "search_candidates", "generate_ai", "none"
            "keywords": list[str],  # 検索キーワード
            "response": str,  # AI応答テキスト
            "sort": str | None,  # ソート指示
            "filters": dict,  # フィルタ条件
        }
    """
    context = context or {}
    candidate = context.get("candidate")
    job = context.get("job")
    msg = message.strip()
    msg_lower = msg.lower()

    result = {"action": "none", "keywords": [], "response": "", "sort": None, "filters": {}}

    # --- 候補者に合う求人を検索 ---
    job_search_patterns = [
        "求人を探して", "求人を出して", "求人を検索", "仕事を探して",
        "合う求人", "マッチする求人", "おすすめの求人", "求人ある",
        "この候補者に合う", "この人に合う",
    ]
    if any(p in msg_lower for p in job_search_patterns):
        if candidate:
            cond = candidate.get("conditions", {})
            kws = cond.get("keywords", [])
            f = _get_candidate_fields(candidate)
            result["action"] = "search_jobs"
            result["keywords"] = kws if kws else [f["job_type"]]
            result["response"] = f"**{f['name']}様**に合う求人を検索しています... キーワード: {', '.join(result['keywords'])}"
        else:
            # メッセージからキーワード抽出
            extracted = _extract_search_keywords(msg)
            if extracted:
                result["action"] = "search_jobs"
                result["keywords"] = extracted
                result["response"] = f"「{', '.join(extracted)}」の求人を検索しています..."
            else:
                result["response"] = "検索したい求人のキーワードを教えてください（例：「Webデザイナー 大阪」）"
        return result

    # --- 候補者を検索 ---
    cand_search_patterns = [
        "候補者を出して", "候補者を探して", "候補者を検索", "人材を探して",
        "候補者はいる", "候補者ある", "合う候補者", "おすすめの候補者",
        "できる人", "できる候補者", "強い候補者", "経験のある",
    ]
    if any(p in msg_lower for p in cand_search_patterns):
        extracted = _extract_search_keywords(msg)
        result["action"] = "search_candidates"
        result["keywords"] = extracted if extracted else []
        if extracted:
            result["response"] = f"「{', '.join(extracted)}」に関連する候補者を検索しています..."
        else:
            result["response"] = "登録済み候補者を表示しています。"
        return result

    # --- ソート指示 ---
    sort_patterns = {
        "年収高い順": "salary_desc", "年収順": "salary_desc", "給与高い順": "salary_desc",
        "スコア順": "score_desc", "マッチ度順": "score_desc",
        "新着順": "date_desc", "最新順": "date_desc",
    }
    for pattern, sort_key in sort_patterns.items():
        if pattern in msg_lower:
            result["action"] = "sort_results"
            result["sort"] = sort_key
            result["response"] = f"{pattern}に並べ替えました。"
            return result

    # --- 特定キーワードの求人/候補者検索（自由文から） ---
    if tab in ("candidateSearch", "candidate_search"):
        # 候補者検索タブ → 求人検索がデフォルトアクション
        extracted = _extract_search_keywords(msg)
        if extracted and len(extracted) >= 1 and not any(
            w in msg_lower for w in ["スカウト", "懸念", "推薦", "決まり", "面談", "ヘルプ", "使い方"]
        ):
            result["action"] = "search_jobs"
            result["keywords"] = extracted
            result["response"] = f"「{', '.join(extracted)}」の求人を検索しています..."
            return result

    if tab in ("jobSearch", "job_search"):
        # 求人検索タブ → 候補者検索がデフォルトアクション
        extracted = _extract_search_keywords(msg)
        if extracted and len(extracted) >= 1 and not any(
            w in msg_lower for w in ["改善", "求人票", "ヘルプ", "使い方"]
        ):
            result["action"] = "search_candidates"
            result["keywords"] = extracted
            result["response"] = f"「{', '.join(extracted)}」に関連する候補者を検索しています..."
            return result

    # --- AI生成（スカウト・懸念・推薦等）はgenerate_chat_responseに委譲 ---
    ai_triggers = ["スカウト", "懸念", "リスク", "決まり", "推薦", "改善", "面談",
                    "市場価値", "年収", "比較", "進捗", "ヘルプ", "使い方",
                    "こんにちは", "おはよう", "はじめまして"]
    if any(w in msg_lower for w in ai_triggers):
        result["action"] = "generate_ai"
        result["response"] = generate_chat_response(message, context)
        return result

    # --- デフォルト: 自然言語応答 ---
    result["action"] = "generate_ai"
    result["response"] = generate_chat_response(message, context)
    return result


def _extract_search_keywords(message):
    """メッセージから検索用キーワードを抽出"""
    stop_words = {
        "を", "の", "に", "は", "が", "で", "と", "も", "から", "まで", "より",
        "して", "ください", "出して", "探して", "検索", "教えて", "見せて",
        "ある", "いる", "できる", "ない", "たい", "ほしい",
        "求人", "候補者", "人材", "仕事", "この", "その", "あの",
        "おすすめ", "合う", "マッチ", "強い", "高い", "良い",
        "出してください", "探してください", "教えてください",
    }

    # 動詞接尾辞を除去するパターン
    suffix_pattern = re.compile(r'(できて|できる|している|してる|がある|のある|はいる|な$|って$|けど$|する$|強い$)')

    # 日本語トークン化（簡易：助詞・接続詞を区切りとして分割）
    tokens = re.split(r'[\s、。,.\n\t]+|(?<=[\u3041-\u309F])(?=[\u4E00-\u9FFF\u30A0-\u30FF])', message)
    keywords = []
    for token in tokens:
        token = token.strip()
        if not token or len(token) < 2:
            continue
        if token.lower() in stop_words:
            continue
        # 名詞的なトークンを抽出（カタカナ、漢字混じり、英数字）
        if re.search(r'[A-Za-z0-9\u30A0-\u30FF\u4E00-\u9FFF]', token):
            # 疑問符・句読点を除去
            cleaned = re.sub(r'[？?！!。、,.]+$', '', token)
            # 動詞接尾辞を除去
            cleaned = suffix_pattern.sub("", cleaned)
            # 末尾の助詞を除去
            for sw in ["を", "の", "に", "は", "が", "で", "と", "も"]:
                if cleaned.endswith(sw):
                    cleaned = cleaned[:-1]
            if len(cleaned) >= 2 and cleaned.lower() not in stop_words:
                keywords.append(cleaned)

    return keywords[:5]


# ============================================================
# 9. 候補者プロフィール自動生成
# ============================================================

def generate_candidate_profile(candidate, interview_text=None):
    """候補者データ+面談テキスト+タグ情報からプロフィールを自動生成（精度向上版）"""
    f = _get_candidate_fields(candidate)
    info = candidate.get("info", {})
    conditions = candidate.get("conditions", {})
    tags = candidate.get("tags", {})

    # --- ハードスキル抽出（タグ情報も活用） ---
    hard_skills = list(f["hard_skills"][:10])
    if not hard_skills:
        for k, v in info.items():
            if any(w in k for w in ["スキル", "資格", "経験", "専門", "技術"]):
                hard_skills.extend([s.strip() for s in str(v).replace("、", ",").split(",")[:3]])
        hard_skills = hard_skills[:10] or ["専門スキル"]

    # --- ソフトスキル抽出（タグ情報優先） ---
    soft_skills = list(f["soft_skills"]) if f["soft_skills"] else []
    if not soft_skills:
        soft_keywords = {
            "論理性": ["論理", "分析", "データ", "定量"],
            "主体性": ["主体", "自発", "積極", "自ら"],
            "コミュニケーション力": ["コミュニケ", "対話", "折衝", "交渉"],
            "リーダーシップ": ["リーダー", "統括", "マネジ", "管理"],
            "協調性": ["協調", "チーム", "連携", "協力"],
            "創造性": ["企画", "クリエイ", "アイデア", "発想"],
            "成長意欲": ["成長", "学習", "スキルアップ", "挑戦"],
            "粘り強さ": ["粘り", "コミット", "やり遂げ", "達成"],
            "柔軟性": ["柔軟", "適応", "臨機応変"],
            "行動力": ["行動力", "フットワーク", "スピード"],
            "問題解決力": ["問題解決", "課題解決", "改善"],
            "プレゼン力": ["プレゼン", "説明力", "伝える"],
        }
        all_text = " ".join(str(v) for v in info.values()) + " ".join(str(s) for s in candidate.get("strengths", []))
        if interview_text:
            all_text += " " + interview_text
        soft_skills = [skill for skill, kws in soft_keywords.items() if any(kw in all_text for kw in kws)]
    if not soft_skills:
        soft_skills = ["コミュニケーション力"]

    # --- 市場スコア & 理由（精緻化） ---
    ms = f["market_score"]
    market_reasons = []

    # 職種需要
    if ms >= 80:
        market_reasons.append(f"{f['job_type']}の経験者は高い需要があり、特に専門領域まで関われる人材は希少")
    elif ms >= 60:
        market_reasons.append(f"{f['job_type']}領域の需要は安定しており、即戦力として評価されやすい")

    # 複合スキル
    if len(hard_skills) >= 3:
        market_reasons.append(f"{'・'.join(hard_skills[:3])}の複合スキルがあり、市場価値が高い")
    elif len(hard_skills) >= 2:
        market_reasons.append(f"{'・'.join(hard_skills[:2])}の複合スキルでキャリアパスの幅が広い")

    # 資格
    if f.get("certifications"):
        certs_str = "・".join(f["certifications"][:3])
        market_reasons.append(f"{certs_str}の資格を保有し、専門性が客観的に証明されている")

    # マネジメント経験
    if f.get("management", {}).get("has_experience"):
        team_size = f["management"].get("team_size", 0)
        if team_size > 0:
            market_reasons.append(f"{team_size}名規模のマネジメント経験があり、管理職ポジションも視野に入る")
        else:
            market_reasons.append("マネジメント経験があり、管理職ポジションも視野に入る")

    # 語学
    if f.get("languages"):
        lang_str = "・".join(l["language"] for l in f["languages"][:2])
        market_reasons.append(f"{lang_str}のスキルがあり、グローバル案件での活躍が期待できる")

    # 年収
    sal_min = conditions.get("salary_min", 0)
    sal_max = conditions.get("salary_max", 0)
    if sal_min and sal_max:
        market_reasons.append(f"希望年収{sal_min}万〜{sal_max}万円は市場相場と合致しており、複数社から内定が出る可能性が高い")

    if not market_reasons:
        market_reasons = ["条件次第で高マッチが期待できる"]

    # --- マッチ理由（拡充） ---
    match_reasons = []
    if f["job_type"]:
        exp_str = f["experience"] if f["experience"] != "経験あり" else ""
        if exp_str:
            match_reasons.append(f"{f['job_type']}領域で{exp_str}の実務経験")
        else:
            match_reasons.append(f"{f['job_type']}領域での豊富な実務経験")

    if hard_skills and len(hard_skills) >= 2:
        match_reasons.append(f"{'・'.join(hard_skills[:3])}のスキルセット")

    # 業界経験
    if f.get("industries"):
        ind_str = "・".join(f["industries"][:2])
        match_reasons.append(f"{ind_str}業界での実務経験")

    # 実績
    if f.get("achievements"):
        match_reasons.append(f"数値で示せる具体的な実績（{f['achievements'][0][:40]}）")

    # マネジメント
    if f.get("management", {}).get("has_experience"):
        match_reasons.append("マネジメント・チーム運営の経験")

    # 成長意欲等
    all_text = " ".join(str(v) for v in info.values())
    if any(w in all_text for w in ["成長", "挑戦", "スタートアップ", "ベンチャー"]):
        match_reasons.append("成長環境への高い意欲")

    if not match_reasons:
        match_reasons = ["専門領域での実務経験", "成長意欲の高さ"]

    # --- 職務要約（精度向上） ---
    career_summary = f["resume_summary"]
    if not career_summary:
        parts = []
        for k in ["職歴", "経歴", "概要", "経験", "職務要約", "職歴概要"]:
            if k in info:
                parts.append(str(info[k]))
        career_summary = "。".join(parts[:3]) if parts else ""

    if not career_summary:
        # タグ情報から構築
        summary_parts = []
        if f["job_type"]:
            summary_parts.append(f"{f['job_type']}領域")
        if f["experience"] and f["experience"] != "経験あり":
            summary_parts.append(f"{f['experience']}の経験")
        if f.get("industries"):
            summary_parts.append(f"{'・'.join(f['industries'][:2])}業界")
        if hard_skills:
            summary_parts.append(f"{'・'.join(hard_skills[:3])}のスキル")
        if f.get("management", {}).get("has_experience"):
            team_size = f["management"].get("team_size", 0)
            summary_parts.append(f"{'{}名の'.format(team_size) if team_size else ''}マネジメント経験")

        if summary_parts:
            career_summary = "で".join(summary_parts[:2]) + "を持ち、" + "を活かした業務に従事。"
            if f.get("achievements"):
                career_summary += f"主な実績: {f['achievements'][0][:60]}"
        else:
            career_summary = f"{f['job_type']}領域での経験を持ち、{'・'.join(hard_skills[:3])}のスキルを活かした業務に従事。"

    # --- 人物タイプメモ（精度向上） ---
    personality_parts = []
    for k, v in info.items():
        if any(w in k for w in ["人物", "印象", "性格", "特徴", "タイプ", "人柄"]):
            personality_parts.append(str(v))
    personality_memo = "。".join(personality_parts) if personality_parts else ""

    if not personality_memo:
        memo_parts = []
        if soft_skills:
            memo_parts.append(f"{'・'.join(soft_skills[:3])}が特徴的")
        if f.get("experience_level"):
            level_labels = {
                "ジュニア": "ポテンシャル重視で成長意欲が高い",
                "ミドル": "実務経験を積み即戦力として期待できる",
                "シニア": "豊富な経験で専門性が高い",
                "リード": "チームを牽引できるリーダータイプ",
                "エグゼクティブ": "経営視点を持ったハイレベル人材",
            }
            level_desc = level_labels.get(f["experience_level"], "")
            if level_desc:
                memo_parts.append(level_desc)
        if f.get("career_change_reasons"):
            reasons_str = "・".join(f["career_change_reasons"][:2])
            memo_parts.append(f"転職動機: {reasons_str}")
        personality_memo = "。".join(memo_parts) + "。" if memo_parts else ""

    # --- ネガティブチェック（拡充） ---
    negative_checks = list(f["negatives"])

    # 入社時期が遠い場合
    availability = f.get("availability", "")
    if availability and any(w in availability for w in ["6ヶ月", "半年", "来年"]):
        negative_checks.append(f"入社可能時期が遠い（{availability}）")

    return {
        "hard_skills": hard_skills[:10],
        "soft_skills": soft_skills[:8],
        "market_score": ms,
        "market_reasons": market_reasons[:5],
        "match_reasons": match_reasons[:5],
        "career_summary": career_summary,
        "personality_memo": personality_memo,
        "negative_checks": negative_checks,
        # 新規フィールド
        "certifications": f.get("certifications", []),
        "industries": f.get("industries", []),
        "languages": f.get("languages", []),
        "management": f.get("management", {}),
        "experience_level": f.get("experience_level", ""),
        "achievements": f.get("achievements", []),
        "education": f.get("education", {}),
        "work_styles": f.get("work_styles", []),
        "availability": f.get("availability", ""),
        "career_change_reasons": f.get("career_change_reasons", []),
    }


MARKET_FIT_AXES = [
    {"id": "demandFit", "label": "📈 市場需要一致度", "desc": "現在の市場で需要がある人材か"},
    {"id": "friction", "label": "🔄 条件摩擦の少なさ", "desc": "希望条件と市場相場のギャップが小さいか"},
    {"id": "decisionReadiness", "label": "⚡ 意思決定スピード", "desc": "転職意向が明確で動きが早いか"},
    {"id": "marketRangeFit", "label": "📊 市場適合レンジ", "desc": "年収・ポジションが市場レンジ内か"},
    {"id": "risk", "label": "🛡️ リスク要因の少なさ", "desc": "転職回数・ブランク等のリスクが低いか"},
]


def evaluate_market_fit(candidate, settings=None):
    """5軸でMarket Fitを評価し、⭐付与を判定（タグ情報活用版）"""
    f = _get_candidate_fields(candidate)
    ms = f["market_score"]
    required_positives = 3

    axes = {}
    # 1. 市場需要（スキル数+資格+業界経験で評価）
    demand_score = len(f["hard_skills"]) + len(f.get("certifications", [])) + len(f.get("industries", []))
    axes["demandFit"] = "positive" if demand_score >= 4 and ms >= 65 else ("neutral" if demand_score >= 2 else "negative")
    # 2. 条件摩擦
    sal_min = candidate.get("conditions", {}).get("salary_min", 400)
    axes["friction"] = "positive" if 300 <= sal_min <= 800 else ("neutral" if sal_min <= 1000 else "negative")
    # 3. 意思決定スピード（入社可能時期も考慮）
    avail = f.get("availability", "")
    if avail in ("即日", "1ヶ月後") and ms >= 70:
        axes["decisionReadiness"] = "positive"
    elif ms >= 75:
        axes["decisionReadiness"] = "positive"
    elif ms >= 60:
        axes["decisionReadiness"] = "neutral"
    else:
        axes["decisionReadiness"] = "negative"
    # 4. 市場適合レンジ
    axes["marketRangeFit"] = "positive" if ms >= 70 else ("neutral" if ms >= 55 else "negative")
    # 5. リスク
    axes["risk"] = "positive" if not f["negatives"] else ("neutral" if len(f["negatives"]) <= 1 else "negative")

    positive_count = sum(1 for v in axes.values() if v == "positive")
    has_major_negative = any(v == "negative" for v in axes.values())
    has_star = positive_count >= required_positives and not has_major_negative

    reason = ""
    if has_star:
        reasons = []
        if axes["demandFit"] == "positive":
            reasons.append(f"{f['job_type']}の人材需要は現在高く、複数企業からのオファーが期待できます")
        if axes["friction"] == "positive":
            reasons.append("希望条件と市場相場のギャップが小さく、ミスマッチが起きにくいです")
        if axes["risk"] == "positive":
            reasons.append("経歴上のリスク要因が少なく、書類選考通過率が高いと予測されます")
        reason = "。".join(reasons[:2]) + "。" if reasons else "市場的に決まりやすい条件が揃っています。"

    return {
        "has_star": has_star,
        "axes": axes,
        "positive_count": positive_count,
        "has_major_negative": has_major_negative,
        "reason": reason,
    }
