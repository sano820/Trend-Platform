#!/usr/bin/env python3
import argparse
from datetime import datetime
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

from airflow.experiments.naver_dup_metrics_store import build_daily_report  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Rebuild daily duplicate-rate report from saved run metrics.")
    ap.add_argument(
        "--base-dir",
        default=str(REPO_ROOT / "logs" / "naver_dup_metrics"),
        help="Metrics root directory (default: logs/naver_dup_metrics)",
    )
    ap.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Target date (YYYY-MM-DD).",
    )
    args = ap.parse_args()

    out_json, out_csv = build_daily_report(base_dir=args.base_dir, date_key=args.date)
    print(f"[ok] daily json: {out_json}")
    print(f"[ok] daily csv : {out_csv}")


if __name__ == "__main__":
    main()
