#!/usr/bin/env python3
import json
import os
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

from airflow.experiments.naver_dup_metrics_store import save_run_metrics  # noqa: E402
from experiments.exp_dup_metrics_collect import run_experiment  # noqa: E402


def main() -> None:
    client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise SystemExit("환경변수 NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 가 필요합니다.")

    metrics = run_experiment(
        client_id=client_id,
        client_secret=client_secret,
        target_unique=1300,
        display=100,
        tokens_per_run=40,
        starts=[1],  # 핵심: 토큰당 1회 호출 제한
        seed=42,
        sleep_s=0.1,
    )

    run_id = f"manual__one_call__{datetime.now().astimezone().isoformat()}"
    run_context = {
        "dag_id": "exp_naver_dup_metrics_one_call",
        "task_id": "exp_collect_one_call",
        "run_id": run_id,
        "execution_date": "",
        "experiment": "starts_1_only",
    }
    out = save_run_metrics(
        base_dir=str(REPO_ROOT / "airflow" / "logs" / "naver_dup_metrics_one_call"),
        run_context=run_context,
        metrics=metrics,
        posts_count=0,
        failures_count=0,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
