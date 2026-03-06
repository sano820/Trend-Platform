import os
import sys

# Airflow 컨테이너 내부 경로 — 커스텀 모듈 import 전에 반드시 먼저 실행되어야 한다.
sys.path.insert(0, "/opt/airflow/airflow")  # naver_blog_latest_dump
sys.path.insert(0, "/opt/airflow/kafka")    # producer

import logging  # noqa: E402

import producer  # noqa: E402
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from naver_blog_latest_dump import discover_latest  # noqa: E402
from experiments.naver_dup_metrics_store import save_run_metrics  # noqa: E402

logger = logging.getLogger(__name__)


def crawl_and_publish() -> None:
    """
    무상태(stateless) 크롤링 → Kafka publish.
    중복 제거는 Kafka key(URL) 기반으로 다운스트림에서 처리한다.
    로컬 파일·DB 접근 없음.
    """
    client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("환경변수 NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 를 설정해 주세요.")

    logger.info("[DAG 5min] discover_latest 호출 시작")
    posts, failures, metrics = discover_latest(
        client_id=client_id,
        client_secret=client_secret,
        target_unique=1300,
        display=100,
        tokens_per_run=40,
        starts=[1, 101, 201, 301],
        sleep_s=0.1,
        max_fetch=500,
        fetch_sleep_s=0.2,
        return_metrics=True,
    )
    logger.info("[DAG 5min] discover_latest 완료: posts=%d failures=%d", len(posts), len(failures))

    metrics_dir = os.getenv("DUP_METRICS_DIR_5MIN", "/opt/airflow/logs/naver_dup_metrics_5min")
    run_context = {
        "dag_id": os.getenv("AIRFLOW_CTX_DAG_ID", "naver_blog_latest_dump_5min"),
        "task_id": os.getenv("AIRFLOW_CTX_TASK_ID", "run_dump"),
        "run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual"),
        "execution_date": os.getenv("AIRFLOW_CTX_EXECUTION_DATE", ""),
        "experiment": "5min_schedule",
    }
    saved_paths = save_run_metrics(
        base_dir=metrics_dir,
        run_context=run_context,
        metrics=metrics,
        posts_count=len(posts),
        failures_count=len(failures),
    )
    logger.info(
        "[DAG 5min] 중복률 파일 저장 완료 run_json=%s summary_csv=%s daily_json=%s",
        saved_paths["run_json"],
        saved_paths["summary_csv"],
        saved_paths["daily_json"],
    )

    if not posts and not failures:
        logger.warning("[DAG 5min] 수집된 데이터가 없습니다. publish를 생략합니다.")
        return

    logger.info("[DAG 5min] producer.publish 호출")
    producer.publish(posts, failures)
    logger.info("[DAG 5min] producer.publish 완료")


with DAG(
    dag_id="naver_blog_latest_dump_5min",
    start_date=datetime(2026, 3, 5),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["naver", "blog", "crawl", "experiment", "5min"],
) as dag:
    crawl_task = PythonOperator(
        task_id="run_dump",
        python_callable=crawl_and_publish,
    )
