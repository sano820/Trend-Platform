#!/usr/bin/env python3
import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import sys


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

from airflow.experiments.naver_dup_metrics_store import save_run_metrics  # noqa: E402


PATH_RE = re.compile(
    r"dag_id=(?P<dag_id>[^/]+)/run_id=(?P<run_id>[^/]+)/task_id=(?P<task_id>[^/]+)/attempt=(?P<attempt>\d+)\.log$"
)
TS_RE = re.compile(r"^\[(?P<ts>\d{4}-\d{2}-\d{2}T[^\]]+)\]")
SUMMARY_NEW_RE = re.compile(
    r"\[discover\]\[summary\] api_calls=(\d+) unique_urls=(\d+) total_items=(\d+) avg_items=([0-9.]+) "
    r"total_new=(\d+) total_dup=(\d+) total_dup_rate=([0-9.]+) zero_item_calls=(\d+) zero_rate=([0-9.]+)"
)
SUMMARY_OLD_RE = re.compile(r"\[discover\] API calls=(\d+), unique_urls=(\d+)")
CALL_RE = re.compile(
    r"\[discover\]\[call\] #(\d+) token=(.*?) start=(\d+) items=(\d+) new=(\d+) dup=(\d+) dup_rate=([0-9.]+) unique_total=(\d+)"
)
POSTS_NEW_RE = re.compile(r"\[discover\] 완료: posts=(\d+) failures=(\d+)")
POSTS_OLD_RE = re.compile(r"\[content\] saved=(\d+), failed=(\d+)")
FINAL_STATE_RE = re.compile(r"Marking task as (SUCCESS|FAILED)\.")


def parse_ts_to_iso(ts_text: str) -> Optional[str]:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(ts_text, fmt).isoformat()
        except ValueError:
            continue
    return None


def parse_log_file(path: Path) -> Optional[Dict[str, Any]]:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    mpath = PATH_RE.search(str(path))
    if not mpath:
        return None

    discovered_at: Optional[str] = None
    summary: Optional[Dict[str, Any]] = None
    calls: List[Dict[str, Any]] = []
    posts_count = 0
    failures_count = 0
    final_state = "UNKNOWN"

    for ln in lines:
        if discovered_at is None:
            mts = TS_RE.search(ln)
            if mts:
                discovered_at = parse_ts_to_iso(mts.group("ts"))

        ms = SUMMARY_NEW_RE.search(ln)
        if ms:
            summary = {
                "api_calls": int(ms.group(1)),
                "unique_urls": int(ms.group(2)),
                "total_items": int(ms.group(3)),
                "avg_items": float(ms.group(4)),
                "total_new": int(ms.group(5)),
                "total_dup": int(ms.group(6)),
                "total_dup_rate": float(ms.group(7)),
                "zero_item_calls": int(ms.group(8)),
                "zero_rate": float(ms.group(9)),
            }
            continue

        mo = SUMMARY_OLD_RE.search(ln)
        if mo and summary is None:
            api_calls = int(mo.group(1))
            unique_urls = int(mo.group(2))
            summary = {
                "api_calls": api_calls,
                "unique_urls": unique_urls,
                "total_items": 0,
                "avg_items": 0.0,
                "total_new": unique_urls,
                "total_dup": 0,
                "total_dup_rate": 0.0,
                "zero_item_calls": 0,
                "zero_rate": 0.0,
            }
            continue

        mc = CALL_RE.search(ln)
        if mc:
            calls.append(
                {
                    "call_no": int(mc.group(1)),
                    "token": mc.group(2),
                    "start": int(mc.group(3)),
                    "items": int(mc.group(4)),
                    "new": int(mc.group(5)),
                    "dup": int(mc.group(6)),
                    "dup_rate": float(mc.group(7)),
                    "unique_total": int(mc.group(8)),
                }
            )
            continue

        mpn = POSTS_NEW_RE.search(ln)
        if mpn:
            posts_count = int(mpn.group(1))
            failures_count = int(mpn.group(2))
            continue

        mpo = POSTS_OLD_RE.search(ln)
        if mpo:
            posts_count = int(mpo.group(1))
            failures_count = int(mpo.group(2))
            continue

        mfs = FINAL_STATE_RE.search(ln)
        if mfs:
            final_state = mfs.group(1)

    if summary is None:
        return None

    if discovered_at is None:
        discovered_at = datetime.now().isoformat()

    if calls and summary.get("total_new", 0) == 0 and summary.get("total_dup", 0) == 0:
        total_new = sum(c["new"] for c in calls)
        total_dup = sum(c["dup"] for c in calls)
        denom = total_new + total_dup
        summary["total_new"] = total_new
        summary["total_dup"] = total_dup
        summary["total_dup_rate"] = (total_dup / denom) if denom else 0.0

    ctx = mpath.groupdict()
    metrics = {
        "discovered_at": discovered_at,
        "params": {"source": "log_backfill"},
        "summary": summary,
        "tokens": [],
        "calls": calls,
        "discovered_urls": [],
        "discovered_count": int(summary.get("unique_urls", 0)),
        "log_file": str(path),
        "task_state": final_state,
    }
    run_context = {
        "dag_id": ctx["dag_id"],
        "task_id": ctx["task_id"],
        "run_id": ctx["run_id"],
        "execution_date": "",
        "attempt": ctx["attempt"],
        "source": "log_backfill",
        "task_state": final_state,
    }
    return {
        "metrics": metrics,
        "run_context": run_context,
        "posts_count": posts_count,
        "failures_count": failures_count,
    }


def has_existing_run(base_dir: Path, date_key: str, run_id: str) -> bool:
    runs_dir = base_dir / "runs" / date_key
    if not runs_dir.exists():
        return False
    pattern = f"*__{run_id}.json"
    return any(runs_dir.glob(pattern))


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill duplicate metrics from existing Airflow task logs.")
    ap.add_argument(
        "--logs-root",
        default=str(ROOT / "logs" / "dag_id=naver_blog_latest_dump_10min"),
        help="Path to Airflow DAG log root.",
    )
    ap.add_argument(
        "--base-dir",
        default=str(REPO_ROOT / "logs" / "naver_dup_metrics"),
        help="Metrics output root directory.",
    )
    ap.add_argument("--include-failed", action="store_true", help="Include FAILED runs if summary exists.")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite by allowing same run_id to be saved again.")
    ap.add_argument(
        "--date-mode",
        choices=["log_time", "run_time"],
        default="log_time",
        help="log_time: keep original log timestamp date, run_time: use current execution date/time.",
    )
    ap.add_argument("--limit", type=int, default=0, help="Max number of logs to process (0 = all).")
    ap.add_argument("--dry-run", action="store_true", help="Parse only, do not write files.")
    args = ap.parse_args()

    logs_root = Path(args.logs_root)
    base_dir = Path(args.base_dir)
    files = sorted(logs_root.glob("run_id=*/task_id=*/attempt=*.log"))
    if args.limit > 0:
        files = files[: args.limit]

    processed = 0
    skipped = 0
    saved = 0

    for p in files:
        parsed = parse_log_file(p)
        if parsed is None:
            skipped += 1
            continue

        if args.date_mode == "run_time":
            parsed["metrics"]["discovered_at"] = datetime.now().astimezone().isoformat()

        state = str((parsed["metrics"] or {}).get("task_state", "UNKNOWN"))
        if state == "FAILED" and not args.include_failed:
            skipped += 1
            continue

        date_key = str(parsed["metrics"]["discovered_at"])[:10]
        run_id = str(parsed["run_context"]["run_id"])
        if (not args.overwrite) and has_existing_run(base_dir, date_key, run_id):
            skipped += 1
            continue

        processed += 1
        if args.dry_run:
            print(f"[dry-run] {p}")
            continue

        paths = save_run_metrics(
            base_dir=str(base_dir),
            run_context=parsed["run_context"],
            metrics=parsed["metrics"],
            posts_count=int(parsed["posts_count"]),
            failures_count=int(parsed["failures_count"]),
        )
        saved += 1
        print(f"[saved] {p} -> {paths['run_json']}")

    print(f"[done] scanned={len(files)} processed={processed} saved={saved} skipped={skipped}")


if __name__ == "__main__":
    main()
