import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import redis
from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, ProcessAllWindowFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.table import StreamTableEnvironment


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rising-tokens-10m")


SCHEMA_VERSION = "v1"
TIME_BASIS = "processing_time"
KST = timezone(timedelta(hours=9))

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


def format_kst_iso(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=KST).isoformat()


def now_kst_iso() -> str:
    return datetime.now(tz=KST).isoformat()


def tokenize_text(
    title: Optional[str],
    content_text: Optional[str],
    stopwords: set[str],
    max_tokens_per_post: int,
) -> list[str]:
    raw_text = f"{title or ''} {content_text or ''}"
    text = URL_RE.sub(" ", raw_text)
    text = EMAIL_RE.sub(" ", text)
    text = text.lower()
    text = SPACES_RE.sub(" ", text).strip()
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


class WindowTokenCounts(ProcessAllWindowFunction):
    def __init__(self, stopwords: set[str], max_tokens_per_post: int):
        self.stopwords = stopwords
        self.max_tokens_per_post = max_tokens_per_post

    def process(self, context, elements):
        # v1 정의:
        # - total_posts: 윈도우 내 이벤트 수
        # - all_counts[token]: 토큰 DF(고유 게시글 기준)
        total_posts = 0
        token_counts: dict[str, int] = {}
        seen_post_ids = set()

        for raw in elements:
            total_posts += 1
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            post_id = data.get("url")
            if not post_id:
                continue
            if post_id in seen_post_ids:
                continue
            seen_post_ids.add(post_id)

            tokens = tokenize_text(
                title=data.get("title"),
                content_text=data.get("content_text"),
                stopwords=self.stopwords,
                max_tokens_per_post=self.max_tokens_per_post,
            )
            for token in set(tokens):
                token_counts[token] = token_counts.get(token, 0) + 1

        payload = {
            "schema_version": SCHEMA_VERSION,
            "window_start": format_kst_iso(context.window().start),
            "window_end": format_kst_iso(context.window().end),
            "total_posts": total_posts,
            "all_counts": token_counts,
            "time_basis": TIME_BASIS,
            "generated_at": now_kst_iso(),
        }
        yield json.dumps(payload, ensure_ascii=False)


class RisingTokensProcess(KeyedProcessFunction):
    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        redis_db: int,
        redis_key: str,
        redis_window_key_prefix: str,
        ttl_seconds: int,
        redis_window_ttl_seconds: int,
        top_n: int,
        min_count: int,
        min_delta: int,
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.redis_window_key_prefix = redis_window_key_prefix
        self.ttl_seconds = ttl_seconds
        self.redis_window_ttl_seconds = redis_window_ttl_seconds
        self.top_n = top_n
        self.min_count = min_count
        self.min_delta = min_delta
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
        self.prev_counts = runtime_context.get_map_state(
            MapStateDescriptor("rising_prev_counts", Types.STRING(), Types.LONG())
        )
        self.last_window_end = runtime_context.get_state(
            ValueStateDescriptor("rising_last_window_end", Types.STRING())
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

        window_end = data["window_end"]
        last_end = self.last_window_end.value()
        if last_end is not None and window_end <= last_end:
            return

        total_posts = int(data.get("total_posts", 0))
        all_counts = {k: int(v) for k, v in data.get("all_counts", {}).items()}

        candidates = []
        tokens = set(self.prev_counts.keys()) | set(all_counts.keys())
        for token in tokens:
            count = int(all_counts.get(token, 0))
            prev_count = self.prev_counts.get(token)
            if prev_count is None:
                prev_count = 0

            delta = count - prev_count
            if count < self.min_count or delta < self.min_delta:
                continue

            if prev_count > 0:
                rise_rate = delta / prev_count
                increase_label = None
            elif count > 0:
                rise_rate = None
                increase_label = "NEW"
            else:
                rise_rate = None
                increase_label = "—"

            share = (count / total_posts) if total_posts > 0 else 0.0
            candidates.append(
                {
                    "token": token,
                    "count": count,
                    "prev_count": int(prev_count),
                    "delta": int(delta),
                    "rise_rate": rise_rate,
                    "increase_label": increase_label,
                    "share": round(share, 4),
                }
            )

        # 정렬 규칙: delta desc, rise_rate desc, count desc, token asc
        candidates.sort(
            key=lambda x: (
                -x["delta"],
                -(x["rise_rate"] if x["rise_rate"] is not None else -1.0),
                -x["count"],
                x["token"],
            )
        )

        items = []
        for idx, item in enumerate(candidates[: self.top_n], start=1):
            items.append(
                {
                    "rank": idx,
                    "token": item["token"],
                    "count": item["count"],
                    "prev_count": item["prev_count"],
                    "delta": item["delta"],
                    "rise_rate": None
                    if item["rise_rate"] is None
                    else round(item["rise_rate"], 4),
                    "increase_label": item["increase_label"],
                    "share": item["share"],
                }
            )

        output = {
            "schema_version": SCHEMA_VERSION,
            "window_start": data["window_start"],
            "window_end": window_end,
            "generated_at": now_kst_iso(),
            "total_posts": total_posts,
            "top_n": self.top_n,
            "time_basis": TIME_BASIS,
            "items": items,
        }
        output_json = json.dumps(output, ensure_ascii=False)

        self.redis_client.set(self.redis_key, output_json, ex=self.ttl_seconds)
        self.redis_client.set(
            f"{self.redis_window_key_prefix}:{window_end}",
            output_json,
            ex=self.redis_window_ttl_seconds,
        )

        yield output_json

        for token in list(self.prev_counts.keys()):
            self.prev_counts.remove(token)
        for token, count in all_counts.items():
            self.prev_counts.put(token, int(count))
        self.last_window_end.update(window_end)

    def close(self):
        if self.redis_client is not None:
            self.redis_client.close()


def load_stopwords() -> set[str]:
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
    kafka_group_id = os.getenv("KAFKA_RISING_TOKENS_GROUP_ID", "flink-rising-tokens-10m")
    kafka_start = os.getenv("KAFKA_STARTING_OFFSETS", "group-offsets").lower()
    kafka_auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

    if kafka_start in ("earliest", "earliest-offset"):
        startup_mode = "earliest-offset"
    elif kafka_start in ("latest", "latest-offset"):
        startup_mode = "latest-offset"
    else:
        startup_mode = "group-offsets"

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_key = os.getenv("REDIS_KEY_RISING", "trend:rising_tokens:10m")
    redis_window_key_prefix = os.getenv(
        "REDIS_KEY_RISING_WINDOW_PREFIX", "trend:rising_tokens:10m"
    )
    ttl_seconds = int(os.getenv("REDIS_TOP_TOKENS_TTL_SECONDS", "86400"))
    redis_window_ttl_seconds = int(os.getenv("REDIS_WINDOW_TTL_SECONDS", "86400"))

    top_n = int(os.getenv("RISING_TOKENS_N", "20"))
    min_count = int(os.getenv("RISING_MIN_COUNT", "5"))
    min_delta = int(os.getenv("RISING_MIN_DELTA", "3"))
    max_tokens_per_post = int(os.getenv("MAX_TOKENS_PER_POST", "300"))
    stopwords = load_stopwords()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE blog_cleaned_source_rising_tokens (
          raw_value STRING
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{kafka_topic}',
          'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
          'properties.group.id' = '{kafka_group_id}',
          'properties.auto.offset.reset' = '{kafka_auto_offset_reset}',
          'scan.startup.mode' = '{startup_mode}',
          'format' = 'raw'
        )
        """
    )

    source_table = table_env.sql_query(
        """
        SELECT raw_value
        FROM blog_cleaned_source_rising_tokens
        """
    )

    events = table_env.to_data_stream(source_table).map(
        lambda row: row[0], output_type=Types.STRING()
    )

    rising_windowed = events.window_all(
        TumblingProcessingTimeWindows.of(Time.minutes(10))
    ).process(
        WindowTokenCounts(
            stopwords=stopwords,
            max_tokens_per_post=max_tokens_per_post,
        ),
        output_type=Types.STRING(),
    )

    results = (
        rising_windowed.map(
            lambda x: ("global", x),
            output_type=Types.TUPLE([Types.STRING(), Types.STRING()]),
        )
        .key_by(lambda x: x[0], key_type=Types.STRING())
        .process(
            RisingTokensProcess(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db,
                redis_key=redis_key,
                redis_window_key_prefix=redis_window_key_prefix,
                ttl_seconds=ttl_seconds,
                redis_window_ttl_seconds=redis_window_ttl_seconds,
                top_n=top_n,
                min_count=min_count,
                min_delta=min_delta,
            ),
            output_type=Types.STRING(),
        )
    )

    results.print()
    logger.info(
        "Starting rising-tokens job(v1): topic=%s window=10m tumbling redis_key=%s time_basis=%s",
        kafka_topic,
        redis_key,
        TIME_BASIS,
    )
    env.execute("blog-cleaned-rising-tokens-10m")


if __name__ == "__main__":
    build_job()
