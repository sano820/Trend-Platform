import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _to_set(urls: Any) -> Set[str]:
    if not isinstance(urls, list):
        return set()
    return {str(u) for u in urls if u}


def compare_url_sets(prev_urls: Set[str], curr_urls: Set[str]) -> Dict[str, Any]:
    if not prev_urls and not curr_urls:
        return {
            "prev_count": 0,
            "curr_count": 0,
            "overlap_count": 0,
            "overlap_rate_vs_prev": 0.0,
            "overlap_rate_vs_curr": 0.0,
            "jaccard": 0.0,
            "curr_new_vs_prev": 0,
        }

    inter = prev_urls & curr_urls
    union = prev_urls | curr_urls
    return {
        "prev_count": len(prev_urls),
        "curr_count": len(curr_urls),
        "overlap_count": len(inter),
        "overlap_rate_vs_prev": (len(inter) / len(prev_urls)) if prev_urls else 0.0,
        "overlap_rate_vs_curr": (len(inter) / len(curr_urls)) if curr_urls else 0.0,
        "jaccard": (len(inter) / len(union)) if union else 0.0,
        "curr_new_vs_prev": len(curr_urls - prev_urls),
    }


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _ensure_summary_header(csv_path: Path) -> None:
    if csv_path.exists():
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "saved_at",
                "dag_id",
                "task_id",
                "run_id",
                "api_calls",
                "unique_urls",
                "total_new",
                "total_dup",
                "total_dup_rate",
                "posts_count",
                "failures_count",
                "jaccard_vs_prev_run",
                "overlap_vs_prev_run",
            ],
        )
        writer.writeheader()


def save_run_metrics(
    base_dir: str,
    run_context: Dict[str, str],
    metrics: Dict[str, Any],
    posts_count: int,
    failures_count: int,
) -> Dict[str, str]:
    base = Path(base_dir)
    discovered_at = str(metrics.get("discovered_at") or datetime.now().isoformat())
    date_key = discovered_at[:10]
    stamp = discovered_at.replace(":", "").replace("+", "_").replace("-", "")

    runs_dir = base / "runs" / date_key
    daily_dir = base / "daily"
    runs_dir.mkdir(parents=True, exist_ok=True)
    daily_dir.mkdir(parents=True, exist_ok=True)

    prev_run_file: Optional[Path] = None
    run_files = sorted(runs_dir.glob("*.json"))
    if run_files:
        prev_run_file = run_files[-1]

    curr_urls = _to_set(metrics.get("discovered_urls"))
    prev_cmp: Optional[Dict[str, Any]] = None
    if prev_run_file:
        prev_data = _load_json(prev_run_file) or {}
        prev_urls = _to_set((prev_data.get("metrics") or {}).get("discovered_urls"))
        prev_cmp = compare_url_sets(prev_urls, curr_urls)

    payload = {
        "saved_at": datetime.now().isoformat(),
        "context": run_context,
        "metrics": metrics,
        "posts_count": posts_count,
        "failures_count": failures_count,
        "comparison_with_prev_run": prev_cmp,
    }

    out_json = runs_dir / f"{stamp}__{run_context.get('run_id', 'manual')}.json"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_csv = runs_dir / "summary.csv"
    _ensure_summary_header(summary_csv)
    summary = metrics.get("summary") or {}
    with summary_csv.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "saved_at",
                "dag_id",
                "task_id",
                "run_id",
                "api_calls",
                "unique_urls",
                "total_new",
                "total_dup",
                "total_dup_rate",
                "posts_count",
                "failures_count",
                "jaccard_vs_prev_run",
                "overlap_vs_prev_run",
            ],
        )
        writer.writerow(
            {
                "saved_at": payload["saved_at"],
                "dag_id": run_context.get("dag_id"),
                "task_id": run_context.get("task_id"),
                "run_id": run_context.get("run_id"),
                "api_calls": summary.get("api_calls", 0),
                "unique_urls": summary.get("unique_urls", 0),
                "total_new": summary.get("total_new", 0),
                "total_dup": summary.get("total_dup", 0),
                "total_dup_rate": summary.get("total_dup_rate", 0.0),
                "posts_count": posts_count,
                "failures_count": failures_count,
                "jaccard_vs_prev_run": (prev_cmp or {}).get("jaccard", 0.0),
                "overlap_vs_prev_run": (prev_cmp or {}).get("overlap_rate_vs_curr", 0.0),
            }
        )

    daily_json, daily_csv = build_daily_report(base_dir=base_dir, date_key=date_key)
    return {
        "run_json": str(out_json),
        "summary_csv": str(summary_csv),
        "daily_json": daily_json,
        "daily_csv": daily_csv,
    }


def build_daily_report(base_dir: str, date_key: Optional[str] = None) -> Tuple[str, str]:
    base = Path(base_dir)
    if date_key is None:
        date_key = datetime.now().strftime("%Y-%m-%d")

    runs_dir = base / "runs" / date_key
    daily_dir = base / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    out_json = daily_dir / f"{date_key}.json"
    out_csv = daily_dir / f"{date_key}.csv"

    run_files = sorted(runs_dir.glob("*.json")) if runs_dir.exists() else []
    rows: List[Dict[str, Any]] = []
    for p in run_files:
        data = _load_json(p)
        if not data:
            continue
        metrics = data.get("metrics") or {}
        summary = metrics.get("summary") or {}
        comp = data.get("comparison_with_prev_run") or {}
        rows.append(
            {
                "file": str(p),
                "saved_at": data.get("saved_at"),
                "run_id": (data.get("context") or {}).get("run_id"),
                "api_calls": int(summary.get("api_calls", 0) or 0),
                "unique_urls": int(summary.get("unique_urls", 0) or 0),
                "total_new": int(summary.get("total_new", 0) or 0),
                "total_dup": int(summary.get("total_dup", 0) or 0),
                "total_dup_rate": _safe_float(summary.get("total_dup_rate", 0.0)),
                "jaccard_vs_prev_run": _safe_float(comp.get("jaccard", 0.0)),
                "overlap_vs_prev_run": _safe_float(comp.get("overlap_rate_vs_curr", 0.0)),
            }
        )

    run_count = len(rows)
    avg_dup_rate = sum(r["total_dup_rate"] for r in rows) / run_count if run_count else 0.0
    avg_jaccard = sum(r["jaccard_vs_prev_run"] for r in rows) / run_count if run_count else 0.0
    avg_overlap = sum(r["overlap_vs_prev_run"] for r in rows) / run_count if run_count else 0.0

    report = {
        "date": date_key,
        "generated_at": datetime.now().isoformat(),
        "run_count": run_count,
        "avg_dup_rate": avg_dup_rate,
        "min_dup_rate": min((r["total_dup_rate"] for r in rows), default=0.0),
        "max_dup_rate": max((r["total_dup_rate"] for r in rows), default=0.0),
        "avg_jaccard_vs_prev_run": avg_jaccard,
        "avg_overlap_vs_prev_run": avg_overlap,
        "runs": rows,
    }
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "run_count",
                "avg_dup_rate",
                "min_dup_rate",
                "max_dup_rate",
                "avg_jaccard_vs_prev_run",
                "avg_overlap_vs_prev_run",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "date": date_key,
                "run_count": run_count,
                "avg_dup_rate": avg_dup_rate,
                "min_dup_rate": report["min_dup_rate"],
                "max_dup_rate": report["max_dup_rate"],
                "avg_jaccard_vs_prev_run": avg_jaccard,
                "avg_overlap_vs_prev_run": avg_overlap,
            }
        )

    return str(out_json), str(out_csv)
