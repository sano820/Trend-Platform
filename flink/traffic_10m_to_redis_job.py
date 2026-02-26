import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import redis
from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.table import StreamTableEnvironment


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("traffic-10m")


class WindowEventCount(ProcessWindowFunction):
    def process(self, key, context, elements):
        # 요구사항 반영:
        # total_posts는 "고유 게시글 수"가 아니라 "윈도우 내 이벤트(메시지) 수"로 계산한다.
        event_count = 0
        for _ in elements:
            event_count += 1
        yield context.window().end, event_count


class TrafficMetricsProcess(KeyedProcessFunction):
    def __init__(self, redis_host: str, redis_port: int, redis_db: int, redis_key: str):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
        self.window_counts = runtime_context.get_map_state(
            MapStateDescriptor("window_counts", Types.LONG(), Types.LONG())
        )
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            decode_responses=True,
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        _, window_end_ms, total_posts = value
        self.window_counts.put(window_end_ms, total_posts)

        prev_window_end_ms = window_end_ms - (10 * 60 * 1000)
        prev_total_posts = self.window_counts.get(prev_window_end_ms)
        if prev_total_posts is None:
            prev_total_posts = 0

        delta = total_posts - prev_total_posts
        if delta > 0:
            delta_str = f"+{delta}"
        elif delta < 0:
            delta_str = str(delta)
        else:
            delta_str = "±0"

        traffic_increase_rate: Optional[float] = None
        if prev_total_posts > 0:
            traffic_increase_rate = (delta / prev_total_posts) * 100.0
            pct_or_status = f"({traffic_increase_rate:+.1f}%)"
        elif total_posts == 0:
            pct_or_status = "(변동 없음)"
        else:
            pct_or_status = "(신규 유입)"

        summary = (
            f"최근10분 {total_posts}건 | 직전10분 {prev_total_posts}건 | "
            f"{delta_str} {pct_or_status}"
        )
        window_end = (
            datetime.fromtimestamp(window_end_ms / 1000, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )

        payload = {
            "total_posts": total_posts,
            "prev_total_posts": prev_total_posts,
            "traffic_increase_rate": traffic_increase_rate,
            "window_end": window_end,
            "summary": summary,
        }
        payload_json = json.dumps(payload, ensure_ascii=False)
        self.redis_client.set(self.redis_key, payload_json)
        yield payload_json

        expire_before = window_end_ms - (40 * 60 * 1000)
        stale_keys = [k for k in self.window_counts.keys() if k < expire_before]
        for k in stale_keys:
            self.window_counts.remove(k)

    def close(self):
        if self.redis_client is not None:
            self.redis_client.close()


def build_job():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "blog.cleaned")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "flink-traffic-10m")
    kafka_start = os.getenv("KAFKA_STARTING_OFFSETS", "latest").lower()
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_key = os.getenv("REDIS_KEY_TREND", "trend:traffic:10m")

    startup_mode = "earliest-offset" if kafka_start == "earliest" else "latest-offset"

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    table_env = StreamTableEnvironment.create(env)
    table_env.get_config().set("table.local-time-zone", "UTC")

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE blog_cleaned_source (
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
        FROM blog_cleaned_source
        """
    )

    events = table_env.to_data_stream(source_table)

    window_counts = (
        events.map(
            lambda x: ("traffic", x[0]),
            output_type=Types.TUPLE([Types.STRING(), Types.STRING()]),
        )
        .key_by(lambda x: x[0], key_type=Types.STRING())
        .window(TumblingProcessingTimeWindows.of(Time.minutes(10)))
        .process(
            WindowEventCount(),
            output_type=Types.TUPLE([Types.LONG(), Types.LONG()]),
        )
        .map(
            lambda x: ("global", x[0], x[1]),
            output_type=Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG()]),
        )
        .key_by(lambda x: x[0], key_type=Types.STRING())
        .process(
            TrafficMetricsProcess(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db,
                redis_key=redis_key,
            ),
            output_type=Types.STRING(),
        )
    )

    window_counts.print()
    logger.info(
        "Starting job: topic=%s window=10m tumbling redis_key=%s time_basis=processing_time",
        kafka_topic,
        redis_key,
    )
    env.execute("blog-cleaned-traffic-10m")


if __name__ == "__main__":
    build_job()
