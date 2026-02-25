import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from pyflink.common import Duration, Time, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows

import redis


# 기본 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("traffic-10m")


class EventTimestampAssigner(TimestampAssigner):
    # 레코드 튜플의 첫 번째 원소(밀리초 epoch)를 event-time으로 사용한다.
    def extract_timestamp(self, value, record_timestamp) -> int:
        return value[0]


class CountAggregate(AggregateFunction):
    # 윈도우 내 건수 집계를 위한 accumulator 초기값
    def create_accumulator(self):
        return 0

    # 레코드가 들어올 때마다 1씩 증가
    def add(self, value, accumulator):
        return accumulator + 1

    # 최종 집계 결과 반환
    def get_result(self, accumulator):
        return accumulator

    # 세션 윈도우 등 병합 시 사용(현재 시나리오에서는 단순 합)
    def merge(self, a, b):
        return a + b


class WindowToCount(ProcessWindowFunction):
    # aggregate 결과(total_posts)와 window_end를 함께 다음 단계로 전달
    def process(self, key, context, elements):
        # PyFlink 런타임 구현 차이로 list/iterator 둘 다 방어
        total_posts = elements[0] if isinstance(elements, list) else next(elements)
        yield context.window().end, total_posts


class TrafficMetricsProcess(KeyedProcessFunction):
    # Redis 연결 정보를 주입받아 집계 결과를 저장한다.
    def __init__(self, redis_host: str, redis_port: int, redis_db: int, redis_key: str):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
        # window_end -> total_posts 매핑 상태.
        # 직전 10분 구간 값을 조회하기 위해 저장한다.
        self.window_counts = runtime_context.get_map_state(
            MapStateDescriptor(
                "window_counts",
                Types.LONG(),
                Types.LONG(),
            )
        )
        # 작업 시작 시 Redis 커넥션 생성
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            decode_responses=True,
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        # value: ("global", window_end_ms, total_posts)
        _, window_end_ms, total_posts = value
        self.window_counts.put(window_end_ms, total_posts)

        # A안: 현재 윈도우 바로 이전 비중첩 10분(= window_end - 10m) 값을 prev로 사용
        prev_window_end_ms = window_end_ms - (10 * 60 * 1000)
        prev_total_posts = self.window_counts.get(prev_window_end_ms)
        if prev_total_posts is None:
            prev_total_posts = 0

        # 증감 건수 문자열(+N / -N / ±0)
        delta = total_posts - prev_total_posts
        if delta > 0:
            delta_str = f"+{delta}"
        elif delta < 0:
            delta_str = str(delta)
        else:
            delta_str = "±0"

        # 증감률/상태 규칙
        # - prev > 0: 일반 증감률 계산
        # - prev = 0 & total = 0: 변동 없음
        # - prev = 0 & total > 0: 신규 유입
        traffic_increase_rate: Optional[float] = None
        if prev_total_posts > 0:
            traffic_increase_rate = (delta / prev_total_posts) * 100.0
            pct_or_status = f"({traffic_increase_rate:+.1f}%)"
        elif total_posts == 0:
            pct_or_status = "(변동 없음)"
        else:
            pct_or_status = "(신규 유입)"

        # 요구된 출력 문구 포맷
        summary = (
            f"최근10분 {total_posts}건 | 직전10분 {prev_total_posts}건 | "
            f"{delta_str} {pct_or_status}"
        )

        # window_end는 UTC ISO8601 (Z suffix)
        window_end = (
            datetime.fromtimestamp(window_end_ms / 1000, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )

        # Redis 저장 payload
        payload = {
            "total_posts": total_posts,
            "prev_total_posts": prev_total_posts,
            "traffic_increase_rate": traffic_increase_rate,
            "window_end": window_end,
            "summary": summary,
        }
        payload_json = json.dumps(payload, ensure_ascii=False)

        # 최신 집계 결과를 단일 키에 upsert
        self.redis_client.set(self.redis_key, payload_json)
        yield payload_json

        # 상태 메모리 누수 방지를 위해 오래된 window_end는 제거
        expire_before = window_end_ms - (40 * 60 * 1000)
        stale_keys = [k for k in self.window_counts.keys() if k < expire_before]
        for k in stale_keys:
            self.window_counts.remove(k)

    def close(self):
        # 작업 종료 시 Redis 커넥션 정리
        if self.redis_client is not None:
            self.redis_client.close()

# Event-time 파서:
# discovered_at(ISO8601) 우선, 없거나 실패 시 postdate(YYYY-MM-DD) 보조 사용
def parse_event_timestamp_ms(raw: str) -> Optional[int]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # JSON 형식이 아니면 폐기
        return None

    discovered_at = data.get("discovered_at")
    if discovered_at:
        try:
            dt = datetime.fromisoformat(discovered_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass

    postdate = data.get("postdate")
    if postdate:
        try:
            dt = datetime.strptime(postdate, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass

    return None


def to_timestamped_record(raw: str):
    # 파싱 실패 레코드는 -1 timestamp를 부여해 후단 filter에서 제거
    ts = parse_event_timestamp_ms(raw)
    if ts is None:
        return -1, raw
    return ts, raw


def build_job():
    # 실행 환경 변수
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    # blog.cleaned만 Kafka Source로 소비
    kafka_topic = os.getenv("KAFKA_TOPIC", "blog.cleaned")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "flink-traffic-10m")
    kafka_start = os.getenv("KAFKA_STARTING_OFFSETS", "latest").lower()
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_key = os.getenv("REDIS_KEY", "trend:traffic:10m")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka Source 생성
    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap_servers)
        .set_topics(kafka_topic)
        .set_group_id(kafka_group_id)
        .set_value_only_deserializer(SimpleStringSchema())
    )

    # 시작 오프셋 정책
    if kafka_start == "earliest":
        source_builder = source_builder.set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
    else:
        source_builder = source_builder.set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )

    source = source_builder.build()

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "blog-cleaned-source",
    )

    # 1) timestamp 파싱 2) 유효 이벤트만 필터 3) 워터마크 할당
    events = (
        raw_stream.map(
            to_timestamped_record,
            output_type=Types.TUPLE([Types.LONG(), Types.STRING()]),
        )
        .filter(lambda x: x[0] >= 0)
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
            # Kafka 일부 파티션이 잠시 유휴 상태여도 전체 워터마크가 멈추지 않게 처리
            .with_idleness(Duration.of_minutes(2))
            .with_timestamp_assigner(EventTimestampAssigner())
        )
    )

    # 10분 윈도우/10분 슬라이드로 total_posts 계산 후,
    # prev_total_posts/증감률을 계산해 Redis에 저장
    window_counts = (
        events.map(
            lambda _: ("traffic", 1),
            output_type=Types.TUPLE([Types.STRING(), Types.INT()]),
        )
        .key_by(lambda x: x[0], key_type=Types.STRING())  # 10분 Sliding Window + 10분 slide 집계
        .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(10)))
        .aggregate(
            CountAggregate(),
            WindowToCount(),
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

    # 디버깅용 stdout 출력(운영 시 제거 가능)
    window_counts.print()

    logger.info(
        "Starting job: topic=%s window=10m slide=10m redis_key=%s event_time=discovered_at/postdate",
        kafka_topic,
        redis_key,
    )
    env.execute("blog-cleaned-traffic-10m")


if __name__ == "__main__":
    build_job()
