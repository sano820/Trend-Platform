# Trend-Platform

스트리밍과 배치 분석을 결합해 트렌드를 실시간으로 집계하고 다음날 키워드를 예측하는 플랫폼입니다.

**핵심 구성**
- 수집: Airflow + Kafka + MySQL
- 스트리밍 분석: Flink + Redis
- 배치 예측: Spark
- API: FastAPI (backend)
- UI: React (frontend)

**폴더 구조**
- `airflow/`: DAG 및 Airflow 컨테이너
- `kafka/`: Kafka producer/consumer
- `mysql/`: DB 스키마 초기화
- `flink/`: 스트리밍 집계 → Redis
- `spark/`: 일 배치 파이프라인 + 리포트 생성
- `backend/`: 대시보드/리포트 API
- `frontend/`: React UI (Vite)

**환경 변수**
- `.env.example`을 복사해 `.env`를 만든 뒤 실제 값을 입력하세요.
- `.env`는 커밋하면 안 됩니다.

```
cp .env.example .env
```

**중요 시크릿**
- `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
- `MYSQL_PASSWORD`, `CONSUMER_MYSQL_PASSWORD`
- `OPENAI_API_KEY`

**기본 포트**
- Airflow: `8080`
- Kafka UI: `8085`
- Flink UI: `8081`
- Spark UI: `8088`
- Redis: `6379`
- MySQL: `3307` (host 매핑)
- Backend API: `8000` (로컬 실행)
- Frontend: `5173` (Vite)

**실행 순서 (로컬/단일 머신 기준)**
1. `.env` 설정
2. 수집 스택 실행
3. 분석 스택 실행
4. Spark 배치 실행(예측)
5. 리포트 생성(OpenAI)
6. 백엔드 API 실행
7. 프론트 실행

**실행 커맨드**
1. 수집 스택 (Airflow + Kafka + consumer)

```
docker compose -f docker-compose.collect.yml up -d
```

2. 분석 스택 (Flink + Redis)

```
docker compose -f docker-compose.analytics.yml up -d
```

3. Spark 배치 (예측)

```
docker compose up spark-batch
```

4. 리포트 생성 (OpenAI)

```
docker compose run --rm spark-batch \
  python /opt/spark-app/spark/daily_pipeline/report_generator.py \
  --report-date 2026-02-26 \
  --top-n 20
```

5. 백엔드 API 실행

```
cd backend
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

6. 프론트 실행

```
cd frontend
npm install
npm run dev
```

**참고**
- 멀티 머신 운영 시 분리된 compose 파일을 각 노드에서 실행하세요.
- Flink는 Redis에 다음 키로 저장합니다: `trend:traffic:10m`, `trend:top_tokens:10m`, `trend:rising_tokens:10m`.
- 보고서는 `daily_reports` 테이블에 저장됩니다.
