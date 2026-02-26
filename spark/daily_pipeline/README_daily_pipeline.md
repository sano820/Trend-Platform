# Spark Daily Pipeline (Cleaned Docs → Daily Token Stats → D+1 Predictions)

이 폴더는 **Trend-Platform 레포의 `spark/` 폴더에 그대로 복사**해서 쓰는 것을 전제로 합니다.

## 1) 레포에 넣는 위치

- `spark/jobs/daily_pipeline/` : 배치 잡 코드
- `spark/resources/stopwords_ko.txt` : 불용어 목록(드라이버에서만 읽음)
- `spark/requirements.batch.txt` : 파이썬 의존성(기존 Spark 이미지/환경에 추가)

## 2) 형태소 분석 사용

기본값은 형태소 분석(kiwipiepy)입니다.

- `TOKENIZER_MODE=morph` (기본)
- `TOKENIZER_MODE=split` (공백 split 베이스라인)

명사/외래어 POS 태그는 기본값이 아래입니다.

- `MORPH_ALLOWED_TAGS=NNG,NNP,NNB,NR,NP,SL`

## 3) 실행 방법(spark-submit)

아래는 예시입니다. (Spark 실행 방식은 팀 레포의 기존 `spark/docker-compose.yml` 또는 스크립트에 맞춰 적용하세요.)

```bash
spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-app/jobs/daily_pipeline/daily_pipeline_job.py \
  --run-date 2026-02-25
```

## 4) 주요 환경변수

### MySQL
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`

### 테이블명
- `RAW_TABLE=blog_post`
- `CLEANED_TABLE=cleaned_docs`
- `STATS_TABLE=daily_token_stats`
- `PRED_TABLE=predictions`

### 날짜 버킷
- `DISCOVERED_AT_IS_UTC=true|false`

### 불용어 경로
- `STOPWORDS_PATH=/path/to/stopwords_ko.txt`

`STOPWORDS_PATH`가 없으면 아래 순서로 자동 탐색합니다.
1) `<repo>/spark/resources/stopwords_ko.txt`
2) `/opt/spark-app/resources/stopwords_ko.txt`
3) `/opt/bitnami/spark/resources/stopwords_ko.txt`

## 5) 의존성 주의(중요)

형태소 분석을 쓰려면 **Spark 드라이버/워커(파이썬 워커) 모두에 `kiwipiepy`가 설치**되어 있어야 합니다.

- `spark/requirements.batch.txt`를 팀 Spark 이미지 빌드 과정에 포함시키거나
- 워커까지 동일하게 `pip install -r requirements.batch.txt`가 적용되도록 구성하세요.
