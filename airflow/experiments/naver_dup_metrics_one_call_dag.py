import os
import sys
from pathlib import Path

sys.path.insert(0, "/opt/airflow/airflow")

import logging  # noqa: E402

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from naver_blog_latest_dump import discover_latest  # noqa: E402
from airflow.experiments.naver_dup_metrics_store import save_run_metrics  # noqa: E402

logger = logging.getLogger(__name__)


def collect_one_call_metrics() -> None:
    client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("환경변수 NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 를 설정해 주세요.")

    logger.info("[DAG one-call] discover_latest 호출 시작")
    posts, failures, metrics = discover_latest(
        client_id=client_id,
        client_secret=client_secret,
        target_unique=1300,
        display=100,
        tokens_per_run=40,
        starts=[1],  # 토큰당 1회 호출
        sleep_s=0.1,
        max_fetch=1,  # discover_latest 내부 ThreadPool 제약(0 불가) 회피
        fetch_sleep_s=0.2,
        return_metrics=True,
    )
    logger.info("[DAG one-call] discover_latest 완료: posts=%d failures=%d", len(posts), len(failures))

    repo_root = Path(__file__).resolve().parents[2]
    metrics_dir = os.getenv(
        "ONE_CALL_DUP_METRICS_DIR",
        str(repo_root / "airflow" / "logs" / "naver_dup_metrics_one_call"),
    )
    run_context = {
        "dag_id": os.getenv("AIRFLOW_CTX_DAG_ID", "naver_dup_metrics_one_call_10min"),
        "task_id": os.getenv("AIRFLOW_CTX_TASK_ID", "collect_one_call_metrics"),
        "run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual"),
        "execution_date": os.getenv("AIRFLOW_CTX_EXECUTION_DATE", ""),
        "experiment": "starts_1_only",
    }
    saved_paths = save_run_metrics(
        base_dir=metrics_dir,
        run_context=run_context,
        metrics=metrics,
        posts_count=len(posts),
        failures_count=len(failures),
    )
    logger.info(
        "[DAG one-call] 파일 저장 완료 run_json=%s summary_csv=%s daily_json=%s",
        saved_paths["run_json"],
        saved_paths["summary_csv"],
        saved_paths["daily_json"],
    )


with DAG(
    dag_id="naver_dup_metrics_one_call_10min",
    start_date=datetime(2026, 3, 4),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["naver", "dup-metrics", "experiment", "one-call"],
) as dag:
    run_one_call = PythonOperator(
        task_id="collect_one_call_metrics",
        python_callable=collect_one_call_metrics,
    )
