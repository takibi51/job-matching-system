#!/usr/bin/env python3
"""
求人データ 自動更新スクリプト
DBに登録されたキーワードで全ソースから取得。
12時間ごとにlaunchdから自動実行。

使い方:
  python3 refresh_cache.py                  # DB登録キーワードで更新
  python3 refresh_cache.py --from-csv       # 候補者CSVからもキーワード追加
  python3 refresh_cache.py --setup          # macOS自動更新セットアップ
"""

import argparse
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_collector import fetch_from_all_sources
from cache_manager import (
    save_jobs, get_stats, delete_old_jobs,
    get_enabled_keywords, add_keyword, add_collection_log,
)
from candidate_loader import load_all_candidates

DEFAULT_KEYWORDS = [
    "Webデザイナー", "Webマーケティング", "デジタルマーケティング",
    "UI/UXデザイナー", "Web広告運用", "LP制作",
    "グラフィックデザイナー", "コンサルタント", "営業", "事務",
]


def ensure_keywords_from_csv():
    """候補者CSVからキーワードをDBに追加"""
    candidates = load_all_candidates()
    added = 0
    for cand in candidates:
        cond = cand.get("conditions", {})
        for kw in cond.get("keywords", []):
            if add_keyword(kw, "大阪"):
                added += 1
        for kw in cond.get("extra_keywords", [])[:3]:
            if add_keyword(kw, "大阪"):
                added += 1
    return added


def refresh():
    """メイン更新処理"""
    # DB登録キーワードを取得
    kw_records = get_enabled_keywords()
    if not kw_records:
        # デフォルトキーワードを登録
        for kw in DEFAULT_KEYWORDS:
            add_keyword(kw, "大阪")
        kw_records = get_enabled_keywords()

    keywords = [r["keyword"] for r in kw_records]
    locations = list(set(r.get("location", "") for r in kw_records if r.get("location")))
    main_location = locations[0] if locations else "大阪"

    print(f"更新開始: {len(keywords)}キーワード, 勤務地: {main_location}")
    start = time.time()

    jobs = fetch_from_all_sources(
        keywords, main_location,
        progress_callback=lambda m: print(f"  {m}"),
    )

    elapsed = time.time() - start
    saved = 0
    if jobs:
        saved = save_jobs(jobs)

    # ログ記録
    add_collection_log(len(keywords), len(jobs), saved, "全ソース", elapsed)

    # 古いデータ削除
    deleted = delete_old_jobs(60)

    stats = get_stats()
    print(f"完了: 取得{len(jobs)}件 → 保存{saved}件 ({elapsed:.1f}秒)")
    if deleted:
        print(f"古いデータ{deleted}件削除")
    print(f"DB合計: {stats['total_jobs']}件")


def setup_launchd():
    """macOS launchd で12時間ごとの自動更新をセットアップ"""
    script = os.path.abspath(__file__)
    python = sys.executable
    plist_name = "com.takibi.job-cache-refresh"
    plist_path = os.path.expanduser(f"~/Library/LaunchAgents/{plist_name}.plist")
    log_dir = os.path.join(os.path.dirname(script), "logs")
    os.makedirs(log_dir, exist_ok=True)

    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{plist_name}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python}</string>
        <string>{script}</string>
        <string>--from-csv</string>
    </array>
    <key>StartInterval</key>
    <integer>43200</integer>
    <key>WorkingDirectory</key>
    <string>{os.path.dirname(script)}</string>
    <key>StandardOutPath</key>
    <string>{log_dir}/refresh_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>{log_dir}/refresh_stderr.log</string>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>"""

    with open(plist_path, "w") as f:
        f.write(plist_content)

    # 登録
    os.system(f"launchctl unload '{plist_path}' 2>/dev/null")
    os.system(f"launchctl load '{plist_path}'")

    print("=" * 50)
    print("  自動更新セットアップ完了")
    print("=" * 50)
    print(f"  更新間隔: 12時間ごと")
    print(f"  plist: {plist_path}")
    print(f"  ログ: {log_dir}/")
    print()
    print("管理コマンド:")
    print(f"  状態確認: launchctl list | grep {plist_name}")
    print(f"  停止: launchctl unload '{plist_path}'")
    print(f"  再開: launchctl load '{plist_path}'")
    print(f"  手動実行: {python} {script}")


def main():
    parser = argparse.ArgumentParser(description="求人データ自動更新")
    parser.add_argument("--from-csv", action="store_true", help="候補者CSVからキーワード追加")
    parser.add_argument("--setup", action="store_true", help="12時間自動更新をセットアップ")
    args = parser.parse_args()

    if args.setup:
        setup_launchd()
        return

    if args.from_csv:
        added = ensure_keywords_from_csv()
        if added:
            print(f"CSVから{added}件のキーワードを追加")

    refresh()


if __name__ == "__main__":
    main()
