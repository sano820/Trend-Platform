#!/usr/bin/env python3
import argparse
import csv
import html
import json
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

from naver_blog_latest_dump import (  # noqa: E402
    build_token_pool,
    naver_blog_search,
    to_mobile_naver_blog_url,
)
from airflow.experiments.naver_dup_metrics_store import compare_url_sets, save_run_metrics  # noqa: E402


def _parse_starts(raw: str) -> List[int]:
    out: List[int] = []
    for x in raw.split(","):
        x = x.strip()
        if not x:
            continue
        out.append(int(x))
    return out or [1]


def _to_serializable_cmp(left: int, right: int, comp: Dict[str, Any]) -> Dict[str, Any]:
    row = {"left_call_no": left, "right_call_no": right}
    row.update(comp)
    return row


def _write_calls_csv(path: Path, run_id: str, calls: List[Dict[str, Any]]) -> None:
    if not calls:
        return
    fieldnames = [
        "run_id",
        "call_no",
        "token",
        "start",
        "items",
        "new",
        "dup",
        "dup_rate",
        "unique_total",
        "call_url_count",
        "new_url_count",
        "dup_url_count",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in calls:
            writer.writerow(
                {
                    "run_id": run_id,
                    "call_no": c.get("call_no", 0),
                    "token": c.get("token", ""),
                    "start": c.get("start", 0),
                    "items": c.get("items", 0),
                    "new": c.get("new", 0),
                    "dup": c.get("dup", 0),
                    "dup_rate": c.get("dup_rate", 0.0),
                    "unique_total": c.get("unique_total", 0),
                    "call_url_count": c.get("call_url_count", 0),
                    "new_url_count": c.get("new_url_count", 0),
                    "dup_url_count": c.get("dup_url_count", 0),
                }
            )


def _write_call_cmp_csv(path: Path, run_id: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = [
        "run_id",
        "left_call_no",
        "right_call_no",
        "prev_count",
        "curr_count",
        "overlap_count",
        "overlap_rate_vs_prev",
        "overlap_rate_vs_curr",
        "jaccard",
        "curr_new_vs_prev",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            out = {"run_id": run_id}
            out.update(r)
            writer.writerow(out)


def run_experiment(
    client_id: str,
    client_secret: str,
    target_unique: int,
    display: int,
    tokens_per_run: int,
    starts: List[int],
    seed: int,
    sleep_s: float,
) -> Dict[str, Any]:
    token_pool = build_token_pool(seed=seed)
    random.shuffle(token_pool)
    tokens = token_pool[: min(tokens_per_run, len(token_pool))]

    discovered_at = datetime.now().astimezone().isoformat()
    uniq: Dict[str, Dict[str, Any]] = {}
    call_details: List[Dict[str, Any]] = []
    call_url_sets: Dict[int, Set[str]] = {}

    api_calls = 0
    total_items = 0
    total_new = 0
    total_dup = 0
    zero_item_calls = 0

    for token in tokens:
        for st in starts:
            if len(uniq) >= target_unique:
                break

            api_calls += 1
            data = naver_blog_search(
                token,
                display=display,
                start=st,
                client_id=client_id,
                client_secret=client_secret,
            )

            items = data.get("items", []) or []
            n_items = len(items)
            total_items += n_items
            if n_items == 0:
                zero_item_calls += 1

            call_urls: Set[str] = set()
            call_new_urls: Set[str] = set()
            call_dup_urls: Set[str] = set()
            new_cnt = 0
            dup_cnt = 0

            for it in items:
                link = it.get("link")
                if not link or "blog.naver.com" not in link:
                    continue

                norm = to_mobile_naver_blog_url(link)
                call_urls.add(norm)

                if norm in uniq:
                    dup_cnt += 1
                    call_dup_urls.add(norm)
                    continue

                title = it.get("title") or None
                if title:
                    title = re.sub(r"<[^>]+>", "", title)
                    title = html.unescape(title)

                uniq[norm] = {
                    "url": norm,
                    "postdate": it.get("postdate") or None,
                    "title": title,
                    "bloggername": it.get("bloggername") or None,
                    "bloggerlink": it.get("bloggerlink") or None,
                    "token": token,
                    "discovered_at": discovered_at,
                }
                new_cnt += 1
                call_new_urls.add(norm)

                if len(uniq) >= target_unique:
                    break

            total_new += new_cnt
            total_dup += dup_cnt
            denom = new_cnt + dup_cnt
            dup_rate = (dup_cnt / denom) if denom else 0.0

            call_url_sets[api_calls] = set(call_urls)
            call_details.append(
                {
                    "call_no": api_calls,
                    "token": token,
                    "start": st,
                    "items": n_items,
                    "new": new_cnt,
                    "dup": dup_cnt,
                    "dup_rate": dup_rate,
                    "unique_total": len(uniq),
                    "call_url_count": len(call_urls),
                    "new_url_count": len(call_new_urls),
                    "dup_url_count": len(call_dup_urls),
                    "call_urls": sorted(call_urls),
                    "new_urls": sorted(call_new_urls),
                    "dup_urls": sorted(call_dup_urls),
                }
            )
            time.sleep(sleep_s)

        if len(uniq) >= target_unique:
            break

    denom_all = total_new + total_dup
    total_dup_rate = (total_dup / denom_all) if denom_all else 0.0
    avg_items = (total_items / api_calls) if api_calls else 0.0
    zero_rate = (zero_item_calls / api_calls) if api_calls else 0.0

    call_comparisons: List[Dict[str, Any]] = []
    if 1 in call_url_sets and 2 in call_url_sets:
        comp_1_2 = compare_url_sets(call_url_sets[1], call_url_sets[2])
        call_comparisons.append(_to_serializable_cmp(1, 2, comp_1_2))

    sorted_calls = sorted(call_url_sets.keys())
    for i in range(1, len(sorted_calls)):
        left = sorted_calls[i - 1]
        right = sorted_calls[i]
        comp = compare_url_sets(call_url_sets[left], call_url_sets[right])
        call_comparisons.append(_to_serializable_cmp(left, right, comp))

    metrics: Dict[str, Any] = {
        "discovered_at": discovered_at,
        "params": {
            "target_unique": target_unique,
            "display": display,
            "starts": starts,
            "tokens_per_run": tokens_per_run,
            "seed": seed,
            "sleep_s": sleep_s,
            "experiment_mode": "dup_rate_per_call_and_call_diversity",
        },
        "summary": {
            "api_calls": api_calls,
            "unique_urls": len(uniq),
            "total_items": total_items,
            "avg_items": avg_items,
            "total_new": total_new,
            "total_dup": total_dup,
            "total_dup_rate": total_dup_rate,
            "zero_item_calls": zero_item_calls,
            "zero_rate": zero_rate,
        },
        "tokens": tokens,
        "calls": call_details,
        "call_comparisons": call_comparisons,
        "discovered_urls": sorted(list(uniq.keys())),
        "discovered_count": len(uniq),
    }
    return metrics


def main() -> None:
    ap = argparse.ArgumentParser(
        description="실험용: API 호출별 중복률 + 1번/2번 호출 다양성 비교 + /logs 누적 저장"
    )
    ap.add_argument("--target", type=int, default=400)
    ap.add_argument("--display", type=int, default=100)
    ap.add_argument(
        "--starts",
        type=str,
        default="1",
        help="키워드당 1회 호출은 starts=1. 예: 1 또는 1,101",
    )
    ap.add_argument("--tokens", type=int, default=40, help="키워드(토큰) 개수")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument(
        "--base-dir",
        type=str,
        default=str(REPO_ROOT / "logs" / "naver_dup_metrics_experiments"),
        help="누적 저장 루트 디렉터리",
    )
    args = ap.parse_args()

    client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise SystemExit("환경변수 NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 가 필요합니다.")

    starts = _parse_starts(args.starts)
    metrics = run_experiment(
        client_id=client_id,
        client_secret=client_secret,
        target_unique=args.target,
        display=min(args.display, 100),
        tokens_per_run=args.tokens,
        starts=starts,
        seed=args.seed,
        sleep_s=args.sleep,
    )

    run_id = f"manual__exp__{datetime.now().astimezone().isoformat()}"
    run_context = {
        "dag_id": "exp_naver_dup_metrics",
        "task_id": "exp_collect",
        "run_id": run_id,
        "execution_date": "",
    }
    saved_paths = save_run_metrics(
        base_dir=args.base_dir,
        run_context=run_context,
        metrics=metrics,
        posts_count=0,
        failures_count=0,
    )

    date_key = str(metrics.get("discovered_at", ""))[:10]
    stamp = str(metrics.get("discovered_at", "")).replace(":", "").replace("+", "_").replace("-", "")
    runs_dir = Path(args.base_dir) / "runs" / date_key
    calls_csv = runs_dir / f"{stamp}__{run_id}__calls.csv"
    cmp_csv = runs_dir / f"{stamp}__{run_id}__call_comparisons.csv"
    _write_calls_csv(calls_csv, run_id=run_id, calls=metrics.get("calls", []))
    _write_call_cmp_csv(cmp_csv, run_id=run_id, rows=metrics.get("call_comparisons", []))

    output = {
        "run_json": saved_paths["run_json"],
        "summary_csv": saved_paths["summary_csv"],
        "daily_json": saved_paths["daily_json"],
        "daily_csv": saved_paths["daily_csv"],
        "calls_csv": str(calls_csv),
        "call_comparisons_csv": str(cmp_csv),
        "one_vs_two_comp": next(
            (r for r in metrics.get("call_comparisons", []) if r.get("left_call_no") == 1 and r.get("right_call_no") == 2),
            None,
        ),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
