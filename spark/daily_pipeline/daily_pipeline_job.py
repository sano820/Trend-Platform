from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# NOTE: spark-submit은 이 파일을 "스크립트"로 실행한다.
# 이때 Spark는 스크립트가 있는 디렉토리를 PYTHONPATH에 자동으로 추가해주기 때문에,
# 같은 폴더(형제 모듈)에 있는 파일들을 import 할 수 있다.
from config import PipelineConfig, load_mysql_config_from_env
from mysql_utils import delete_by_date, run_many
from predictor import PredictionParams, predict_next_day
from text_cleaner import build_cleaned_text
from tokenizer import load_stopwords, make_distinct_udf, make_tokenize_udf


def parse_args() -> argparse.Namespace:
    """
    커맨드라인 인자 파싱.
    - 기본값은 .env(환경변수)로부터 읽고, 인자로 주면 인자가 우선한다.
    - run-date는 필수(오늘 날짜 D). 예측은 D+1로 저장한다.
    """
    p = argparse.ArgumentParser(
        description="Trend-Platform 일 배치: Cleaned Doc -> Daily Token Stats -> D+1 Predictions"
    )

    p.add_argument("--run-date", required=True, help="실행 기준 날짜(오늘, D). YYYY-MM-DD 또는 YYYYMMDD")
    p.add_argument("--window-days", type=int, default=int(os.getenv("WINDOW_DAYS", "7")), help="최근 윈도우 일수(기본 7)")
    p.add_argument("--top-n", type=int, default=int(os.getenv("TOP_N", "50")), help="예측 결과 Top-N(기본 50)")

    p.add_argument("--rebuild-window", default=os.getenv("REBUILD_WINDOW", "true"), help="true/false (윈도우 전체 재계산 여부)")

    # ─────────────────────────────────────────────
    # 후보 토큰 필터 임계치(오늘 기준 최소 활동량)
    # ─────────────────────────────────────────────
    p.add_argument("--min-doc-today", type=int, default=int(os.getenv("MIN_DOC_TODAY", "5")), help="오늘 등장 문서 수 최소값")
    p.add_argument("--min-count-today", type=int, default=int(os.getenv("MIN_COUNT_TODAY", "10")), help="오늘 등장 횟수 최소값")

    # ─────────────────────────────────────────────
    # 급상승(버스트) 분류 임계치
    # ─────────────────────────────────────────────
    p.add_argument("--growth-ratio-min", type=float, default=float(os.getenv("GROWTH_RATIO_MIN", "2.0")), help="성장비율 최소값")
    p.add_argument("--prev-mean-max", type=float, default=float(os.getenv("PREV_MEAN_MAX", "3.0")), help="과거 평균 최대값(오늘 제외)")

    # ─────────────────────────────────────────────
    # 랭킹 점수 가중치(상태별 부스팅)
    # ─────────────────────────────────────────────
    p.add_argument("--w-burst", type=float, default=float(os.getenv("W_BURST", "1.5")), help="급상승형 가중치")
    p.add_argument("--w-stable", type=float, default=float(os.getenv("W_STABLE", "0.7")), help="안정형 가중치")
    p.add_argument("--w-uncertain", type=float, default=float(os.getenv("W_UNCERTAIN", "0.3")), help="불확실 가중치")

    # ─────────────────────────────────────────────
    # 폭주 방지 캡(예측값 상한)
    # ─────────────────────────────────────────────
    p.add_argument("--yhat-cap-multiplier", type=float, default=float(os.getenv("YHAT_CAP_MULTIPLIER", "3.0")), help="예측 상한 배수(기본 3.0)")

    # ─────────────────────────────────────────────
    # 토큰 길이 필터(품질 필터)
    # ─────────────────────────────────────────────
    p.add_argument("--min-token-len", type=int, default=int(os.getenv("MIN_TOKEN_LEN", "2")), help="최소 토큰 길이")
    p.add_argument("--max-token-len", type=int, default=int(os.getenv("MAX_TOKEN_LEN", "20")), help="최대 토큰 길이")

    return p.parse_args()


def _parse_run_date(s: str) -> date:
    """
    run-date 입력 포맷을 date로 변환한다.
    - 'YYYY-MM-DD' 또는 'YYYYMMDD' 둘 다 지원
    """
    s = s.strip()
    if "-" in s:
        return datetime.strptime(s, "%Y-%m-%d").date()
    return datetime.strptime(s, "%Y%m%d").date()


def build_pipeline_config(args: argparse.Namespace) -> PipelineConfig:
    """
    argparse 결과를 PipelineConfig로 변환한다.
    - rebuild-window는 다양한 입력(true/false/1/0/yes/no)을 허용한다.
    """
    run_date = _parse_run_date(args.run_date)
    rebuild_window = str(args.rebuild_window).lower() in {"1", "true", "yes", "y"}

    return PipelineConfig(
        run_date=run_date,
        window_days=args.window_days,
        top_n=args.top_n,
        min_doc_today=args.min_doc_today,
        min_count_today=args.min_count_today,
        growth_ratio_min=args.growth_ratio_min,
        prev_mean_max=args.prev_mean_max,
        w_burst=args.w_burst,
        w_stable=args.w_stable,
        w_uncertain=args.w_uncertain,
        yhat_cap_multiplier=args.yhat_cap_multiplier,
        rebuild_window=rebuild_window,
        min_token_len=args.min_token_len,
        max_token_len=args.max_token_len,
    )


def build_spark(app_name: str) -> SparkSession:
    """
    SparkSession 생성.
    - 세션 타임존을 Asia/Seoul로 고정해서 날짜/시간 컬럼 처리 시 일자 밀림을 방지한다.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )


def main() -> None:
    # 1) 인자/환경변수 기반 설정 로딩
    args = parse_args()
    config = build_pipeline_config(args)
    mysql = load_mysql_config_from_env()

    # 2) Spark 세션 생성 및 로그 레벨 조정
    spark = build_spark("trend_daily_pipeline")
    spark.sparkContext.setLogLevel("WARN")

    # 3) 사용할 테이블명(환경변수로 오버라이드 가능)
    raw_table = os.getenv("RAW_TABLE", "blog_post")
    cleaned_table = os.getenv("CLEANED_TABLE", "cleaned_docs")
    stats_table = os.getenv("STATS_TABLE", "daily_token_stats")
    pred_table = os.getenv("PRED_TABLE", "predictions")

    # 4) discovered_at이 UTC로 저장되어 있다면 KST로 변환 후 날짜를 뽑아야 함
    discovered_at_is_utc = str(os.getenv("DISCOVERED_AT_IS_UTC", "true")).lower() in {"1", "true", "yes", "y"}

    # ---------------------------------------------------------------------
    # Step B) Cleaned Doc 만들기 (유효성 검사 + 날짜 기준 결정 + 텍스트 정리)
    # ---------------------------------------------------------------------
    # 처리 대상 날짜 범위:
    # - rebuild_window=True: 최근 window_days(기본 7일) 전체를 다시 계산(로컬 테스트/재처리에 유용)
    # - rebuild_window=False: run_date(D) 하루만 처리(운영 증분 처리에 유용)
    if config.rebuild_window:
        process_dates = [config.run_date - timedelta(days=i) for i in range(config.window_days - 1, -1, -1)]
    else:
        process_dates = [config.run_date]

    process_dates_str = [d.isoformat() for d in process_dates]

    # 1) MySQL에서 원본(Raw) 데이터 읽기
    raw_df = (
        spark.read.format("jdbc")
        .option("url", mysql.jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", raw_table)
        .option("user", mysql.user)
        .option("password", mysql.password)
        .load()
    )

    # 2) 문서 기준 날짜(doc_date) 결정 규칙
    #    - 우선순위 1: postdate(DATE)가 있으면 그 날짜 사용
    #    - 우선순위 2: postdate가 없으면 discovered_at을 날짜로 변환해서 사용
    #      (단, discovered_at이 UTC로 저장돼 있으면 KST로 변환 후 날짜를 뽑는다)
    postdate_date = F.to_date(F.col("postdate"))

    discovered_ts = F.col("discovered_at").cast("timestamp")
    discovered_local = F.from_utc_timestamp(discovered_ts, "Asia/Seoul") if discovered_at_is_utc else discovered_ts
    discovered_date = F.to_date(discovered_local)

    doc_date = F.when(postdate_date.isNotNull(), postdate_date).otherwise(discovered_date)
    date_source = F.when(postdate_date.isNotNull(), F.lit("postdate")).otherwise(F.lit("discovered_at"))

    # 3) 텍스트 정리: 제목+본문 결합 후 URL/특수문자/연속공백 등 노이즈 제거
    cleaned_text = build_cleaned_text(F.col("title"), F.col("content_text"))

    # 4) Cleaned Doc 데이터프레임 구성
    #    - doc_id는 url 기반 sha1로 만들어서 동일 문서는 항상 같은 id가 되게 함(멱등성에 도움)
    #    - doc_date가 처리 대상 날짜(process_dates)에 포함되는 문서만 남김
    #    - cleaned_text가 빈 문자열이면 버림(유효성 검사)
    cleaned_df = (
        raw_df
        .withColumn("doc_date", doc_date)
        .withColumn("date_source", date_source)
        .withColumn("cleaned_text", cleaned_text)
        .withColumn("doc_id", F.sha1(F.col("url")))
        .filter(F.col("doc_date").isin(process_dates_str))
        .filter(F.length(F.col("cleaned_text")) > 0)
        .select(
            "doc_id",
            "url",
            "doc_date",
            "date_source",
            "postdate",
            "discovered_at",
            "title",
            "content_text",
            "cleaned_text",
        )
    )

    # 5) 멱등성(Idempotent) 보장: 해당 날짜 데이터는 먼저 삭제 후 append
    for d in process_dates:
        delete_by_date(mysql, cleaned_table, "doc_date", d.isoformat())

    (
        cleaned_df.write.format("jdbc")
        .option("url", mysql.jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", cleaned_table)
        .option("user", mysql.user)
        .option("password", mysql.password)
        .mode("append")
        .save()
    )

    # ---------------------------------------------------------------------
    # Step C + D) 토큰화 + 일별 집계(Daily Token Stats)
    #   - mention_count: 문서 내 반복 포함(토큰 전체)
    #   - doc_count: 문서 내 중복 제거 후 문서 수(토큰 셋)
    # ---------------------------------------------------------------------
    # stopwords 파일은 드라이버(driver)에서만 읽는다.
    # STOPWORDS_PATH가 없으면:
    # - 레포 상대 경로(<repo>/spark/resources/stopwords_ko.txt)를 우선 찾고
    # - 흔히 쓰는 컨테이너 경로도 추가로 시도한다.
    def _resolve_stopwords_path() -> str:
        env_path = os.getenv("STOPWORDS_PATH", "").strip()
        candidates: list[Path] = []
        if env_path:
            candidates.append(Path(env_path))

        # 레포 상대 경로: <repo>/spark/resources/stopwords_ko.txt
        candidates.append(Path(__file__).resolve().parents[1] / "resources" / "stopwords_ko.txt")

        # 자주 쓰는 컨테이너 경로 후보
        candidates.append(Path("/opt/spark-app/resources/stopwords_ko.txt"))
        candidates.append(Path("/opt/bitnami/spark/resources/stopwords_ko.txt"))

        for c in candidates:
            if c.exists():
                return str(c)

        # 후보에서 못 찾으면 env_path 또는 첫 후보 경로를 반환(로드 함수에서 비어있는 set로 처리)
        return env_path or str(candidates[0])

    stopwords_path = _resolve_stopwords_path()
    stopwords = load_stopwords(stopwords_path)

    # tokenize_udf:
    # - 형태소 분석 또는 split 방식(토크나이저 구현에 따라)
    # - stopwords/길이/숫자/ㅋㅋㅋ 등 필터를 적용한 최종 토큰 리스트 반환
    # distinct_udf:
    # - 한 문서 내 중복 토큰 제거(set)로 doc_count 계산에 사용
    tokenize_udf = make_tokenize_udf(stopwords, min_len=config.min_token_len, max_len=config.max_token_len)
    distinct_udf = make_distinct_udf()

    token_df = (
        cleaned_df
        .withColumn("tokens_all", tokenize_udf(F.col("cleaned_text")))
        .withColumn("tokens_set", distinct_udf(F.col("tokens_all")))
    )

    # mention_count:
    # - tokens_all을 explode (문서 내 동일 토큰 반복도 그대로 카운트)
    mention_stats = (
        token_df
        .select("doc_date", "doc_id", F.explode(F.col("tokens_all")).alias("token"))
        .groupBy("doc_date", "token")
        .agg(F.count(F.lit(1)).alias("mention_count"))
    )

    # doc_count:
    # - tokens_set을 explode (문서 내 중복 제거된 토큰만)
    # - (doc_date, token, doc_id) 중복 제거 후 문서 수 카운트
    doc_stats = (
        token_df
        .select("doc_date", "doc_id", F.explode(F.col("tokens_set")).alias("token"))
        .dropDuplicates(["doc_date", "token", "doc_id"])
        .groupBy("doc_date", "token")
        .agg(F.count(F.lit(1)).alias("doc_count"))
    )

    # 일별 토큰 통계 최종 스키마로 변환(stat_date, token, mention_count, doc_count)
    daily_stats_df = (
        mention_stats.join(doc_stats, on=["doc_date", "token"], how="inner")
        .select(
            F.col("doc_date").alias("stat_date"),
            "token",
            "mention_count",
            "doc_count",
        )
    )

    # 해당 날짜 범위는 멱등성 보장을 위해 먼저 삭제 후 append
    for d in process_dates:
        delete_by_date(mysql, stats_table, "stat_date", d.isoformat())

    (
        daily_stats_df.write.format("jdbc")
        .option("url", mysql.jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", stats_table)
        .option("user", mysql.user)
        .option("password", mysql.password)
        .mode("append")
        .save()
    )

    # ---------------------------------------------------------------------
    # Step E) 예측 실행 (최근 window_days의 Daily Token Stats로 D+1 예측)
    # ---------------------------------------------------------------------
    # 예측에 사용할 윈도우 범위: [D-window_days+1, D]
    window_start = config.run_date - timedelta(days=config.window_days - 1)
    window_start_str = window_start.isoformat()
    run_date_str = config.run_date.isoformat()

    # MySQL에서 최근 window_days 범위만 읽어온다
    stats_window_df = (
        spark.read.format("jdbc")
        .option("url", mysql.jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", stats_table)
        .option("user", mysql.user)
        .option("password", mysql.password)
        .load()
        .filter((F.col("stat_date") >= F.lit(window_start_str)) & (F.col("stat_date") <= F.lit(run_date_str)))
        .select("stat_date", "token", "mention_count", "doc_count")
    )

    # 드라이버 메모리 절약:
    # - "오늘 후보 기준"을 통과한 토큰만 남기고 Pandas로 내려받는다(toPandas는 driver에 다 올림)
    today_tokens_df = (
        stats_window_df
        .filter(F.col("stat_date") == F.lit(run_date_str))
        .filter(
            (F.col("doc_count") >= F.lit(config.min_doc_today)) &
            (F.col("mention_count") >= F.lit(config.min_count_today))
        )
        .select("token")
        .distinct()
    )

    stats_window_df = stats_window_df.join(today_tokens_df, on="token", how="inner")

    # Spark -> Pandas (예측 로직은 pandas 기반으로 수행)
    pdf = stats_window_df.toPandas()

    # 예측 파라미터 구성(임계치/가중치/캡 등)
    params = PredictionParams(
        window_days=config.window_days,
        top_n=config.top_n,
        min_doc_today=config.min_doc_today,
        min_count_today=config.min_count_today,
        growth_ratio_min=config.growth_ratio_min,
        prev_mean_max=config.prev_mean_max,
        w_burst=config.w_burst,
        w_stable=config.w_stable,
        w_uncertain=config.w_uncertain,
        yhat_cap_multiplier=config.yhat_cap_multiplier,
    )

    # D+1 예측 수행 (상태 분류 + ŷ + burst + score 계산 후 Top-N 반환)
    pred_df = predict_next_day(daily_stats=pdf, run_date=config.run_date, params=params)

    # 저장 대상 날짜는 D+1
    predict_date = (config.run_date + timedelta(days=1)).isoformat()
    delete_by_date(mysql, pred_table, "predict_date", predict_date)

    # 예측 결과를 MySQL에 넣기 위한 rows 생성
    rows = []
    for r in pred_df.itertuples(index=False):
        rows.append(
            (
                r.predict_date.isoformat(),
                r.token,
                float(r.predicted_count),
                float(r.score),
                str(r.status),
                float(r.burst),
                int(r.today_count),
                int(r.yest_count),
                float(r.avg_3d_count),
                float(r.avg_7d_count),
                int(r.doc_today),
                int(r.doc_yest),
            )
        )

    # executemany로 한 번에 INSERT
    insert_sql = (
        f"INSERT INTO {pred_table} "
        "(predict_date, token, predicted_count, score, status, burst, today_count, yest_count, avg_3d_count, avg_7d_count, doc_today, doc_yest) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    run_many(mysql, insert_sql, rows)

    # 실행 결과 요약 출력(디버깅/로그 확인용)
    print("\n=== 파이프라인 완료 ===")
    print(f"cleaned_docs 저장 날짜: {process_dates_str}")
    print(f"daily_token_stats 저장 날짜: {process_dates_str}")
    print(f"predictions 저장 predict_date: {predict_date} (top_n={config.top_n})")

    spark.stop()


if __name__ == "__main__":
    main()