import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional

import redis
from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, ProcessAllWindowFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.table import StreamTableEnvironment


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("top-tokens-10m")


URL_RE = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
NON_WORD_RE = re.compile(r"[^\w\s]", re.UNICODE)
SPACES_RE = re.compile(r"\s+")
ONLY_DIGITS_RE = re.compile(r"^\d+$")
NOISE_RE = re.compile(r"^[ㅋㅎㅠㅜ]+$")


DEFAULT_STOPWORDS = {
    "그리고",
    "그러나",
    "하지만",
    "오늘",
    "진짜",
    "근데",
    "그냥",
    "정말",
    "너무",
    "이번",
    "저는",
    "제가",
    "에서",
    "으로",
    "하다",
    "있는",
    "없는",
}


def format_utc_z(epoch_ms: int) -> str:
    return (
        datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def tokenize_text(
    title: Optional[str],
    content_text: Optional[str],
    stopwords: set[str],
    max_tokens_per_post: int,
) -> list[str]:
    raw_text = f"{title or ''} {content_text or ''}"

    # 1) URL/이메일 제거 (특수문자 제거 전에 먼저)
    text = URL_RE.sub(" ", raw_text)
    text = EMAIL_RE.sub(" ", text)

    # 2) 소문자 + 공백 정규화
    text = text.lower()
    text = SPACES_RE.sub(" ", text).strip()

    # 3) 문자/숫자/공백만 유지 (#은 제거되고 단어는 유지됨)
    text = NON_WORD_RE.sub(" ", text)
    text = SPACES_RE.sub(" ", text).strip()

    if not text:
        return []

    tokens = []
    for token in text.split(" "):
        if not token:
            continue
        if len(token) < 2 or len(token) > 20:
            continue
        if ONLY_DIGITS_RE.match(token):
            continue
        if NOISE_RE.match(token):
            continue
        if token in stopwords:
            continue
        tokens.append(token)
        if len(tokens) >= max_tokens_per_post:
            break

    return tokens


class WindowTopTokensFunction(ProcessAllWindowFunction):
    def __init__(self, top_n: int, stopwords: set[str], max_tokens_per_post: int):
        self.top_n = top_n
        self.stopwords = stopwords
        self.max_tokens_per_post = max_tokens_per_post

    def process(self, context, elements):
        # top_tokens는 DF 집계 유지:
        # - total_posts: 윈도우 내 고유 게시글 수(url dedup)
        # - count[token]: 토큰을 포함한 고유 게시글 수
        total_posts = 0
        token_counts = {}
        seen_post_ids = set()

        for raw in elements:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # 요구사항: post_id는 url 기준
            post_id = data.get("url")
            if not post_id:
                continue
            if post_id in seen_post_ids:
                continue
            seen_post_ids.add(post_id)

            total_posts += 1

            tokens = tokenize_text(
                title=data.get("title"),
                content_text=data.get("content_text"),
                stopwords=self.stopwords,
                max_tokens_per_post=self.max_tokens_per_post,
            )

            # DF 방식: 게시글당 토큰 1회만 카운트
            for token in set(tokens):
                token_counts[token] = token_counts.get(token, 0) + 1

        # 정렬 우선순위: count desc, token asc
        sorted_tokens = sorted(token_counts.items(), key=lambda x: (-x[1], x[0]))
        top_items = [{"token": t, "count": c} for t, c in sorted_tokens[: self.top_n]]

        window_start_ms = context.window().start
        window_end_ms = context.window().end

        payload = {
            "window_start_ms": window_start_ms,
            "window_end_ms": window_end_ms,
            "total_posts": total_posts,
            "top_n": self.top_n,
            "top_items": top_items,
            # prev_count 계산 정확도를 위해 전체 token count를 함께 전달
            "all_counts": token_counts,
        }
        yield json.dumps(payload, ensure_ascii=False)


class TopTokensTrendProcess(KeyedProcessFunction):
    def __init__(self, redis_host: str, redis_port: int, redis_db: int, redis_key: str, ttl_seconds: int):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.ttl_seconds = ttl_seconds
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
        # UTC 일자 내 누적 카운트 상태
        self.cum_counts = runtime_context.get_map_state(
            MapStateDescriptor("cum_counts", Types.STRING(), Types.LONG())
        )
        # 누적 기준 UTC 일자 (YYYYMMDD int)
        self.current_utc_day = runtime_context.get_state(
            ValueStateDescriptor("current_utc_day", Types.INT())
        )
        self.last_window_end = runtime_context.get_state(
            ValueStateDescriptor("last_window_end", Types.LONG())
        )
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            decode_responses=True,
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        _, raw_payload = value
        data = json.loads(raw_payload)

        window_start_ms = data["window_start_ms"]
        window_end_ms = data["window_end_ms"]
        total_posts = data["total_posts"]
        top_n = data["top_n"]
        all_counts = data["all_counts"]

        # 같은 윈도우 중복 처리 방지
        last_end = self.last_window_end.value()
        if last_end is not None and window_end_ms <= last_end:
            return

        # UTC 날짜가 바뀌면 누적 상태 초기화
        utc_day = int(
            datetime.fromtimestamp(window_end_ms / 1000, tz=timezone.utc).strftime("%Y%m%d")
        )
        prev_day = self.current_utc_day.value()
        if prev_day is None:
            self.current_utc_day.update(utc_day)
        elif utc_day != prev_day:
            for token in list(self.cum_counts.keys()):
                self.cum_counts.remove(token)
            self.current_utc_day.update(utc_day)

        # count_cum[token] += window_count[token]
        for token, window_count in all_counts.items():
            previous = self.cum_counts.get(token)
            if previous is None:
                previous = 0
            self.cum_counts.put(token, int(previous) + int(window_count))

        # 누적 기준 Top-N 추출
        cum_items = sorted(
            ((token, int(count)) for token, count in self.cum_counts.items()),
            key=lambda x: (-x[1], x[0]),
        )[:top_n]

        items = []
        for idx, (token, count) in enumerate(cum_items, start=1):
            window_increment = int(all_counts.get(token, 0))
            prev_count = count - window_increment

            # share는 DF 기준 점유율:
            # count(토큰 포함 게시글 수) / total_posts(고유 게시글 수)
            share = (count / total_posts) if total_posts > 0 else 0.0

            if prev_count > 0:
                increase_rate = (count - prev_count) / prev_count
                increase_label = None
            elif count == 0:
                increase_rate = None
                increase_label = "—"
            else:
                increase_rate = None
                increase_label = "NEW"

            items.append(
                {
                    "rank": idx,
                    "token": token,
                    "count": count,
                    "share": round(share, 4),
                    "prev_count": prev_count,
                    "increase_rate": None if increase_rate is None else round(increase_rate, 4),
                    "increase_label": increase_label,
                }
            )

        output = {
            "window_start": format_utc_z(window_start_ms),
            "window_end": format_utc_z(window_end_ms),
            "total_posts": total_posts,
            "top_n": top_n,
            "items": items,
        }
        output_json = json.dumps(output, ensure_ascii=False)

        # 최신 결과 1건 유지 + TTL 24h
        self.redis_client.set(self.redis_key, output_json, ex=self.ttl_seconds)
        yield output_json

        self.last_window_end.update(window_end_ms)

    def close(self):
        if self.redis_client is not None:
            self.redis_client.close()


def load_stopwords() -> set[str]:
    # 1) 파일 로드 우선 (줄 단위, '#' 주석 허용)
    stopwords_file = os.getenv(
        "TOP_TOKENS_STOPWORDS_FILE",
        os.path.join(os.path.dirname(__file__), "stopwords_ko.txt"),
    )
    file_words: set[str] = set()
    if os.path.exists(stopwords_file):
        try:
            with open(stopwords_file, "r", encoding="utf-8") as f:
                for line in f:
                    word = line.strip().lower()
                    if not word or word.startswith("#"):
                        continue
                    file_words.add(word)
        except OSError:
            file_words = set()

    # 2) 환경변수 CSV 추가/덮어쓰기
    csv_value = os.getenv("TOP_TOKENS_STOPWORDS", "").strip()
    if not csv_value:
        return file_words if file_words else set(DEFAULT_STOPWORDS)

    configured = {s.strip().lower() for s in csv_value.split(",") if s.strip()}
    merged = file_words | configured
    if merged:
        return merged
    return set(DEFAULT_STOPWORDS)


def build_job():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "blog.cleaned")
    kafka_group_id = os.getenv("KAFKA_TOP_TOKENS_GROUP_ID", "flink-top-tokens-10m")
    kafka_start = os.getenv("KAFKA_STARTING_OFFSETS", "latest").lower()
    startup_mode = "earliest-offset" if kafka_start == "earliest" else "latest-offset"

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_key = os.getenv("REDIS_TOP_TOKENS_KEY", "trend:top_tokens:10m:latest")
    ttl_seconds = int(os.getenv("REDIS_TOP_TOKENS_TTL_SECONDS", "86400"))

    top_n = int(os.getenv("TOP_TOKENS_N", "20"))
    max_tokens_per_post = int(os.getenv("MAX_TOKENS_PER_POST", "300"))
    stopwords = load_stopwords()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)
    table_env.get_config().set("table.local-time-zone", "UTC")

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE blog_cleaned_source_top_tokens (
          raw_value STRING
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{kafka_topic}',
          'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
          'properties.group.id' = '{kafka_group_id}',
          'scan.startup.mode' = '{startup_mode}',
          'format' = 'raw'
        )
        """
    )

    source_table = table_env.sql_query(
        """
        SELECT raw_value
        FROM blog_cleaned_source_top_tokens
        """
    )

    source_stream = table_env.to_data_stream(source_table)

    events = source_stream.map(lambda row: row[0], output_type=Types.STRING())

    top_tokens_windowed = events.window_all(
        TumblingProcessingTimeWindows.of(Time.minutes(10))
    ).process(
        WindowTopTokensFunction(
            top_n=top_n,
            stopwords=stopwords,
            max_tokens_per_post=max_tokens_per_post,
        ),
        output_type=Types.STRING(),
    )

    results = (
        top_tokens_windowed.map(
            lambda x: ("global", x),
            output_type=Types.TUPLE([Types.STRING(), Types.STRING()]),
        )
        .key_by(lambda x: x[0], key_type=Types.STRING())
        .process(
            TopTokensTrendProcess(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db,
                redis_key=redis_key,
                ttl_seconds=ttl_seconds,
            ),
            output_type=Types.STRING(),
        )
    )

    results.print()

    logger.info(
        "Starting top-tokens job: topic=%s window=10m tumbling top_n=%s redis_key=%s ttl=%ss time_basis=processing_time count_mode=cumulative_daily_reset_utc",
        kafka_topic,
        top_n,
        redis_key,
        ttl_seconds,
    )
    env.execute("blog-cleaned-top-tokens-10m")


if __name__ == "__main__":
    build_job()
