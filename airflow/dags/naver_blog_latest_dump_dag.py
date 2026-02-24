from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="naver_blog_latest_dump_10min",
    start_date=datetime(2026, 2, 24),
    schedule="*/10 * * * *",  # 10분마다
    catchup=False,
    max_active_runs=1,        # 겹침 방지
    tags=["naver", "blog", "crawl"],
) as dag:
    run_dump = BashOperator(
        task_id="run_dump",
        bash_command="""
        set -e
        cd /opt/airflow/airflow
        python naver_blog_latest_dump.py \
          --target 1300 \
          --display 100 \
          --tokens 40 \
          --starts 1,101,201,301 \
          --sleep 0.1 \
          --fetch-content \
          --max-fetch 1000 \
          --out-dir /data \
          --seen-db /data/seen_urls.sqlite \
          --seen-ttl-hours 24
        """,
    )