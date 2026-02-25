FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# producer.py는 라이브러리 모듈 — Airflow 컨테이너에서 import하여 사용
# 직접 실행하지 않음 (standalone 실행 진입점 없음)
COPY producer.py .
