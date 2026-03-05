"""
LLM Preprocessor for blog.cleaned -> blog.entities
=================================================
- blog.cleaned 소비
- URL 기준 dedup (Redis)
- OpenAI로 엔티티 추출/정규화
- blog.entities에 전송 (Flink는 그대로 사용)

설계 원칙:
- Flink 코드를 변경하지 않는다.
- content_text를 "엔티티 토큰" 문자열로 대체한다.
- 멀티워드 엔티티는 '_'로 연결해 단일 토큰으로 보존한다.
"""

import json
import logging
import os
import signal
import sys
import re
from datetime import datetime, timezone
from typing import Iterable, List

import redis
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata
from openai import OpenAI

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("llm_preprocessor")

# ──────────────────────────────────────────────
# Graceful Shutdown 플래그
# ──────────────────────────────────────────────
_shutdown_requested = False


def _handle_signal(sig: int, frame) -> None:
    global _shutdown_requested
    logger.info("종료 신호 수신 (signal=%d). 현재 메시지 완료 후 종료합니다...", sig)
    _shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ──────────────────────────────────────────────
# 환경변수 로딩
# ──────────────────────────────────────────────

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"환경변수 {name}를 설정해 주세요.")
    return value


def _optional_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


KAFKA_BOOTSTRAP_SERVERS = _require_env("KAFKA_BOOTSTRAP_SERVERS").split(",")
KAFKA_TOPIC_INPUT = _require_env("KAFKA_TOPIC_INPUT")
KAFKA_TOPIC_OUTPUT = _require_env("KAFKA_TOPIC_OUTPUT")
KAFKA_GROUP_ID = _require_env("KAFKA_GROUP_ID")
KAFKA_AUTO_OFFSET_RESET = _require_env("KAFKA_AUTO_OFFSET_RESET")
CONSUMER_POLL_TIMEOUT_MS = int(_require_env("CONSUMER_POLL_TIMEOUT_MS"))

REDIS_HOST = _require_env("REDIS_HOST")
REDIS_PORT = int(_require_env("REDIS_PORT"))
REDIS_DB = int(_require_env("REDIS_DB"))

DEDUP_TTL_SECONDS = int(_optional_env("ENTITIES_DEDUP_TTL_SECONDS", "604800"))
DEDUP_KEY_PREFIX = _optional_env("ENTITIES_DEDUP_KEY_PREFIX", "entities:dedup:")

OPENAI_MODEL = _optional_env(
    "OPENAI_ENTITIES_MODEL",
    _optional_env("OPENAI_MODEL", "gpt-4o-mini"),
)
OPENAI_TEMPERATURE = float(_optional_env("OPENAI_ENTITIES_TEMPERATURE", "0"))
OPENAI_MAX_OUTPUT_TOKENS = int(_optional_env("OPENAI_ENTITIES_MAX_OUTPUT_TOKENS", "400"))
OPENAI_TIMEOUT_SEC = float(_optional_env("OPENAI_ENTITIES_TIMEOUT_SEC", "8"))

LLM_MAX_INPUT_CHARS = int(_optional_env("LLM_MAX_INPUT_CHARS", "4000"))
LLM_MAX_ENTITIES = int(_optional_env("LLM_MAX_ENTITIES", "60"))
LLM_FAIL_STRATEGY = _optional_env("LLM_FAIL_STRATEGY", "pass_through").lower()

# Flink tokenize_text와 동일한 길이 제약을 맞춘다.
MAX_TOKEN_LENGTH = int(_optional_env("ENTITIES_MAX_TOKEN_LENGTH", "20"))
MIN_TOKEN_LENGTH = int(_optional_env("ENTITIES_MIN_TOKEN_LENGTH", "2"))
TOKEN_JOINER = _optional_env("ENTITIES_TOKEN_JOINER", "_")

# ──────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────

_WHITESPACE_RE = re.compile(r"\s+")
_TRIM_PUNCT_RE = re.compile(r"^[^\w]+|[^\w]+$")


def _dedup_key(url: str) -> str:
    return f"{DEDUP_KEY_PREFIX}{url}"


def _normalize_entity(text: str) -> str:
    # 주변 특수문자 제거 + 공백 정규화
    cleaned = _TRIM_PUNCT_RE.sub("", text.strip())
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned


def _entity_to_token(entity: str) -> str:
    # 멀티워드는 '_'로 연결해 하나의 토큰으로 유지한다.
    return entity.replace(" ", TOKEN_JOINER)


def _filter_entities(entities: Iterable[str]) -> List[str]:
    seen = set()
    results: List[str] = []
    for raw in entities:
        normalized = _normalize_entity(raw)
        if not normalized:
            continue
        token = _entity_to_token(normalized)
        if len(token) < MIN_TOKEN_LENGTH or len(token) > MAX_TOKEN_LENGTH:
            continue
        if token in seen:
            continue
        seen.add(token)
        results.append(token)
        if len(results) >= LLM_MAX_ENTITIES:
            break
    return results


# ──────────────────────────────────────────────
# Kafka/Redis/OpenAI 생성
# ──────────────────────────────────────────────

def create_consumer() -> KafkaConsumer:
    consumer = KafkaConsumer(
        KAFKA_TOPIC_INPUT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=50,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
        max_poll_interval_ms=300_000,
    )
    logger.info(
        "KafkaConsumer 생성 완료 [input_topic=%s group=%s bootstrap=%s]",
        KAFKA_TOPIC_INPUT, KAFKA_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS,
    )
    return consumer


def create_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        enable_idempotence=True,
        max_in_flight_requests_per_connection=1,
        retries=5,
        retry_backoff_ms=300,
        request_timeout_ms=15000,
        delivery_timeout_ms=30000,
        linger_ms=10,
        compression_type="gzip",
    )
    logger.info("KafkaProducer 생성 완료 [output_topic=%s]", KAFKA_TOPIC_OUTPUT)
    return producer


def create_redis() -> redis.Redis:
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
    )
    logger.info("Redis 연결 완료 [%s:%d db=%d]", REDIS_HOST, REDIS_PORT, REDIS_DB)
    return client


def create_openai_client() -> OpenAI:
    # timeout은 OpenAI SDK 내부 HTTP 클라이언트에 전달된다.
    return OpenAI(timeout=OPENAI_TIMEOUT_SEC)


# ──────────────────────────────────────────────
# OpenAI 호출
# ──────────────────────────────────────────────

def _build_prompt(title: str, content_text: str) -> str:
    # 입력 길이를 제한해서 비용/지연을 통제한다.
    merged = f"{title}\n{content_text}".strip()
    if len(merged) > LLM_MAX_INPUT_CHARS:
        merged = merged[:LLM_MAX_INPUT_CHARS]

    return (
        "너는 한국어 텍스트에서 고유명사(인물/조직/제품/지명/작품명)를 추출하는 모델이다.\n"
        "아래 텍스트에서 고유명사만 뽑고, 조사/어미를 제거해 표준형으로 정규화해라.\n"
        "멀티워드 고유명사는 공백을 유지한 그대로 반환하되, 불필요한 수식어는 제거한다.\n"
        "중복은 제거하고, JSON만 반환하라.\n\n"
        "출력 스키마:\n"
        "{\n"
        "  \"entities\": string[]\n"
        "}\n\n"
        f"TEXT:\n{merged}"
    )


def extract_entities(client: OpenAI, title: str, content_text: str) -> List[str] | None:
    prompt = _build_prompt(title, content_text)

    response = client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {"role": "system", "content": "Return JSON only."},
            {"role": "user", "content": prompt},
        ],
        text={"format": {"type": "json_object"}},
        temperature=OPENAI_TEMPERATURE,
        max_output_tokens=OPENAI_MAX_OUTPUT_TOKENS,
    )

    try:
        payload = json.loads(response.output_text)
    except json.JSONDecodeError:
        logger.warning("OpenAI 응답 JSON 파싱 실패")
        return None

    raw_entities = payload.get("entities")
    if not isinstance(raw_entities, list):
        return None

    # LLM 출력 정규화 및 토큰 변환
    return _filter_entities([str(x) for x in raw_entities if x])


# ──────────────────────────────────────────────
# Offset 정밀 Commit
# ──────────────────────────────────────────────

def _commit_offset(consumer: KafkaConsumer, message) -> None:
    tp = TopicPartition(message.topic, message.partition)
    consumer.commit(offsets={tp: OffsetAndMetadata(message.offset + 1, None, -1)})


# ──────────────────────────────────────────────
# 메인 처리 루프
# ──────────────────────────────────────────────

def process_messages(consumer: KafkaConsumer, producer: KafkaProducer, cache: redis.Redis) -> None:
    client = create_openai_client()
    logger.info("LLM 전처리 루프 시작 [poll_timeout=%dms]", CONSUMER_POLL_TIMEOUT_MS)

    while not _shutdown_requested:
        records = consumer.poll(timeout_ms=CONSUMER_POLL_TIMEOUT_MS)
        if not records:
            continue

        for partition, messages in records.items():
            for message in messages:
                if _shutdown_requested:
                    return

                payload = message.value
                if not isinstance(payload, dict):
                    logger.error("잘못된 메시지 형식 [offset=%d]: %r", message.offset, payload)
                    _commit_offset(consumer, message)
                    continue

                url = payload.get("url")
                if not url:
                    logger.warning("url 누락 메시지 스킵 [offset=%d]", message.offset)
                    _commit_offset(consumer, message)
                    continue

                # URL 기준 dedup (Redis)
                if cache.exists(_dedup_key(url)):
                    logger.info("중복 스킵 url=%s offset=%d", url, message.offset)
                    _commit_offset(consumer, message)
                    continue

                title = payload.get("title", "")
                content_text = payload.get("content_text", "")

                entities = None
                try:
                    entities = extract_entities(client, title, content_text)
                except Exception as e:
                    logger.error("OpenAI 호출 실패 url=%s offset=%d: %s", url, message.offset, e)

                llm_failed = entities is None
                if llm_failed:
                    if LLM_FAIL_STRATEGY == "skip":
                        logger.warning("LLM 실패로 스킵 url=%s offset=%d", url, message.offset)
                        _commit_offset(consumer, message)
                        continue
                    if LLM_FAIL_STRATEGY == "empty":
                        entities = []
                    else:
                        # pass_through: 원문 유지 (품질은 낮지만 데이터 손실 방지)
                        entities = []

                # Flink 호환: content_text를 엔티티 토큰 문자열로 대체
                if entities:
                    entities_text = " ".join(entities)
                elif llm_failed and LLM_FAIL_STRATEGY == "pass_through":
                    entities_text = content_text
                else:
                    entities_text = ""

                output_payload = dict(payload)
                output_payload["schema_version"] = output_payload.get("schema_version", "1.0")
                # Flink tokenize_text는 title+content_text를 함께 사용하므로,
                # 원문 title을 비워 LLM 결과만 집계되도록 만든다.
                output_payload["title"] = ""
                output_payload["content_text"] = entities_text
                output_payload["entities"] = entities
                output_payload["entities_updated_at"] = datetime.now(timezone.utc).isoformat()

                try:
                    future = producer.send(KAFKA_TOPIC_OUTPUT, key=url, value=output_payload)
                    future.get(timeout=10)
                except Exception as e:
                    logger.error("Kafka 전송 실패 url=%s offset=%d: %s", url, message.offset, e)
                    # 전송 실패 시 offset commit 안 함 → 재처리
                    continue

                # 성공 시 dedup 마킹 + offset commit
                cache.set(_dedup_key(url), "1", ex=DEDUP_TTL_SECONDS)
                _commit_offset(consumer, message)

                logger.info("LLM 전처리 완료 url=%s offset=%d entities=%d", url, message.offset, len(entities))


# ──────────────────────────────────────────────
# 엔트리포인트
# ──────────────────────────────────────────────

def main() -> None:
    consumer = create_consumer()
    producer = create_producer()
    cache = create_redis()

    try:
        process_messages(consumer, producer, cache)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            producer.close()
        except Exception:
            pass
        try:
            cache.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
