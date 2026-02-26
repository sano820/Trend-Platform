"""
Naver Blog Kafka Producer  - 재사용 가능한 라이브러리 모듈
==========================================================
토픽 구조:
  blog.raw      - 원본 크롤링 데이터 (수정 금지, event log)
  blog.cleaned  - 전처리 완료 데이터 (필드 정리, 날짜 정규화)
  blog.error    - 파싱 실패 / 크롤링 실패

토픽 설정 (init-topics.sh 기준):
  partitions       = 3
  replication-factor = 1
  retention.ms     = 604800000  (7일)
  cleanup.policy   = delete     (기본값 유지 — 아래 참고)

[cleanup.policy=compact 권고 불채택 이유]
  compact 정책은 동일 key(URL)의 오래된 메시지를 백그라운드에서
  비동기적으로 삭제하여 "최신값만 보존"하는 changelog 패턴에 적합하다.
  그러나 이 파이프라인에서는 적합하지 않다:
    1) 비동기 압축이 실행되기 전까지 컨슈머는 중복을 그대로 수신한다.
    2) blog.raw 는 append-only 이벤트 로그이므로, 압축으로 이전 이벤트가
       삭제되면 감사(audit) 및 재처리 목적으로 사용할 수 없게 된다.
  대신 다음 전략을 권장한다:
    - Producer: enable.idempotence=True  → 재시도에 의한 중복 전송 방지
    - Consumer: URL(key)을 기준으로 처리 여부를 자체 추적
    - 필요시: Kafka Streams / ksqlDB 로 별도 dedup 스트림을 구성

공개 API:
  publish(posts, failures=None) -> None
"""

import json
import logging
import os
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────
def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"환경변수 {name}를 설정해 주세요.")  # 변경: 기본값 제거
    return value


# 변경: 하드코딩 제거, .env에서만 읽음
KAFKA_BOOTSTRAP_SERVERS = _require_env("KAFKA_BOOTSTRAP_SERVERS").split(",")

TOPICS = {
    "raw":     _require_env("KAFKA_TOPIC_RAW"),
    "cleaned": _require_env("KAFKA_TOPIC_CLEANED"),
    "error":   _require_env("KAFKA_TOPIC_ERROR"),
}

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Producer 생성
# ──────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # 직렬화
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # 전송 보장
        acks="all",
        enable_idempotence=True,
        # kafka-python 의 idempotent producer는 max_in_flight=1 만 허용한다.
        # (Java 클라이언트는 5까지 허용하지만 kafka-python 구현 제약)
        max_in_flight_requests_per_connection=1,
        retries=5,
        retry_backoff_ms=300,
        # 타임아웃
        request_timeout_ms=15000,        # 브로커 응답 대기 15s
        delivery_timeout_ms=30000,       # 전체 전송 제한 30s (retries 포함)
        # 성능
        linger_ms=10,                    # 소규모 배치 묶음 (10ms)
        compression_type="gzip",
    )


# ──────────────────────────────────────────────
# 내부 헬퍼
# ──────────────────────────────────────────────
def _validate_post(post: dict) -> tuple[bool, str]:
    for field in ("url", "title", "content_text"):
        if not post.get(field):
            return False, f"필수 필드 누락: '{field}'"
    if not isinstance(post.get("content_text", ""), str):
        return False, "content_text가 문자열이 아님"
    return True, ""


def _clean_post(post: dict) -> dict:
    raw_date = post.get("postdate", "")
    try:
        normalized_date = datetime.strptime(raw_date, "%Y%m%d").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        normalized_date = raw_date

    return {
        "url":           post.get("url"),
        "title":         post.get("title", "").strip(),
        "content_text":  post.get("content_text", "").strip(),
        "postdate":      normalized_date,
        "discovered_at": post.get("discovered_at"),
    }


def _build_error_payload(url: str, error_type: str, message: str, raw_data: dict) -> dict:
    return {
        "url":           url,
        "error_type":    error_type,
        "error_message": message,
        "raw_data":      raw_data,
        "occurred_at":   datetime.now(timezone.utc).isoformat(),
    }


def _send(producer: KafkaProducer, topic: str, key: str, value: dict) -> bool:
    """
    메시지를 Kafka 토픽으로 전송하고 브로커 ack를 동기적으로 확인한다.
    future.get(timeout=10) 으로 블로킹하여 실패 시 반드시 False를 반환한다.
    """
    try:
        future = producer.send(topic, key=key, value=value)
        future.get(timeout=10)  # 브로커 ack 대기 — 실패 시 예외 발생
        return True
    except KafkaError as e:
        logger.error("전송 실패 [%s] key=%s : %s", topic, key, e)
        return False
    except Exception as e:
        logger.error("전송 오류(예외) [%s] key=%s : %s", topic, key, e)
        return False


# ──────────────────────────────────────────────
# 토픽별 publish 함수
# ──────────────────────────────────────────────
def _publish_posts(producer: KafkaProducer, posts: list) -> dict:
    """각 포스트를 raw → cleaned(또는 error) 순으로 publish."""
    counts = {"raw": 0, "cleaned": 0, "error": 0}

    for post in posts:
        url = post.get("url", "unknown")

        # 1. Raw: 원본 보존
        if _send(producer, TOPICS["raw"], url, post):
            counts["raw"] += 1

        # 2. 유효성 검사
        is_valid, err_msg = _validate_post(post)
        if not is_valid:
            payload = _build_error_payload(url, "validation_error", err_msg, post)
            _send(producer, TOPICS["error"], url, payload)
            counts["error"] += 1
            continue

        # 3. Cleaned: 정제 후 전송
        try:
            cleaned = _clean_post(post)
            if _send(producer, TOPICS["cleaned"], url, cleaned):
                counts["cleaned"] += 1
        except Exception as e:
            payload = _build_error_payload(url, "cleaning_error", str(e), post)
            _send(producer, TOPICS["error"], url, payload)
            counts["error"] += 1

    return counts


def _publish_failures(producer: KafkaProducer, failures: list) -> int:
    """크롤링 실패 항목을 blog.error 토픽으로 publish."""
    count = 0
    for failure in failures:
        url = failure.get("url", "unknown")
        payload = _build_error_payload(
            url,
            "crawl_failure",
            failure.get("error", "크롤링 실패 (원인 불명)"),
            failure,
        )
        if _send(producer, TOPICS["error"], url, payload):
            count += 1
    return count


# ──────────────────────────────────────────────
# 공개 API
# ──────────────────────────────────────────────
def publish(posts: list, failures: list | None = None) -> None:
    """
    크롤링 결과를 Kafka 토픽으로 publish.

    Args:
        posts:    크롤링된 포스트 딕셔너리 리스트
        failures: 크롤링 실패 항목 리스트 (없으면 빈 리스트로 처리)
    """
    if failures is None:
        failures = []

    logger.info(
        "=== [publish] 시작: posts=%d failures=%d | bootstrap=%s ===",
        len(posts), len(failures), KAFKA_BOOTSTRAP_SERVERS,
    )

    if not posts and not failures:
        logger.warning("[publish] 전송할 데이터가 없습니다. 종료.")
        return

    try:
        p = create_producer()
        logger.info("[publish] KafkaProducer 생성 완료: %s", KAFKA_BOOTSTRAP_SERVERS)
    except Exception as e:
        logger.error("[publish] KafkaProducer 생성 실패: %s", e, exc_info=True)
        raise

    try:
        counts = _publish_posts(p, posts)
        failure_count = _publish_failures(p, failures)

        # future.get()으로 이미 개별 ack를 확인했으나,
        # 내부 버퍼에 남은 메시지를 보장하기 위해 flush도 유지한다.
        p.flush()

        logger.info("─" * 50)
        logger.info("[publish] 전송 완료 요약")
        logger.info("  [%s] 성공 %d / 요청 %d", TOPICS["raw"],     counts["raw"],     len(posts))
        logger.info("  [%s] 성공 %d / 요청 %d", TOPICS["cleaned"], counts["cleaned"], len(posts))
        logger.info("  [%s] 전송 %d건 (validation + crawl 실패)",
                    TOPICS["error"], counts["error"] + failure_count)
        logger.info("─" * 50)

        # posts가 있는데 raw 전송이 0건이면 Kafka 연결 문제로 간주하여 예외 발생
        if len(posts) > 0 and counts["raw"] == 0:
            raise RuntimeError(
                f"posts={len(posts)}건이 있으나 {TOPICS['raw']} 전송 성공 0건 — "
                f"Kafka 연결({KAFKA_BOOTSTRAP_SERVERS}) 또는 토픽 설정을 확인하세요."
            )

    except Exception as e:
        logger.error("[publish] 처리 중 오류: %s", e, exc_info=True)
        raise
    finally:
        p.close()
        logger.info("[publish] Producer 종료")
