"""
Blog Raw Kafka Consumer — MySQL Sink
=====================================
blog.raw 토픽의 메시지를 소비하여 MySQL의 blog_post 테이블에 저장한다.

동작 보장:
  - at-least-once  : DB 저장 성공 시에만 해당 파티션/오프셋을 정밀하게 commit
  - 중복 방지      : INSERT IGNORE (url PRIMARY KEY 기반)
  - 재처리 안전    : DB 실패 시 rollback + offset commit 안 함 → 재시작 시 재처리
  - graceful shutdown : SIGINT/SIGTERM 수신 시 현재 메시지 완료 후 종료

토픽: KAFKA_TOPIC 환경변수로 지정
  메시지 형식 (JSON):
    schema_version : "1.0"
    url            : string
    title          : string
    content_text   : string
    postdate       : "YYYYMMDD" 또는 "YYYY-MM-DD"
    discovered_at  : ISO8601 문자열
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime

import mysql.connector
from mysql.connector import Error as MySQLError
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

# ──────────────────────────────────────────────
# 설정 (변경: 하드코딩 제거, .env에서만 읽음)
# ──────────────────────────────────────────────

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"환경변수 {name}를 설정해 주세요.")  # 변경: 기본값 제거
    return value

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS  = _require_env("KAFKA_BOOTSTRAP_SERVERS").split(",")
KAFKA_TOPIC              = _require_env("KAFKA_TOPIC")
KAFKA_GROUP_ID           = _require_env("KAFKA_GROUP_ID")
KAFKA_AUTO_OFFSET_RESET  = _require_env("KAFKA_AUTO_OFFSET_RESET")
CONSUMER_POLL_TIMEOUT_MS = int(_require_env("CONSUMER_POLL_TIMEOUT_MS"))

# MySQL 설정
MYSQL_HOST     = _require_env("MYSQL_HOST")
MYSQL_PORT     = int(_require_env("MYSQL_PORT"))
MYSQL_DATABASE = _require_env("MYSQL_DATABASE")
MYSQL_USER     = _require_env("MYSQL_USER")
MYSQL_PASSWORD = _require_env("MYSQL_PASSWORD")

# DB 재연결 설정
DB_CONNECT_RETRIES     = int(_require_env("DB_CONNECT_RETRIES"))
DB_CONNECT_RETRY_DELAY = int(_require_env("DB_CONNECT_RETRY_DELAY"))   # 초

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,  # Docker 로그 수집을 위해 stdout 사용
)
logger = logging.getLogger("blog_consumer")

# ──────────────────────────────────────────────
# Graceful Shutdown 플래그
# ──────────────────────────────────────────────
_shutdown_requested = False


def _handle_signal(sig: int, frame) -> None:
    """
    SIGINT (Ctrl+C) 또는 SIGTERM (docker stop) 수신 시 종료 플래그를 설정한다.
    현재 처리 중인 메시지는 완료 후 종료된다.
    """
    global _shutdown_requested
    logger.info("종료 신호 수신 (signal=%d). 현재 메시지 완료 후 종료합니다...", sig)
    _shutdown_requested = True


# ──────────────────────────────────────────────
# MySQL 연결 관리
# ──────────────────────────────────────────────

def create_db_connection() -> mysql.connector.MySQLConnection:
    """
    MySQL 연결을 생성한다.
    실패 시 DB_CONNECT_RETRIES 횟수만큼 재시도하며,
    모두 실패하면 RuntimeError를 발생시킨다.
    """
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                database=MYSQL_DATABASE,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                charset="utf8mb4",
                autocommit=False,        # 수동 트랜잭션 제어
                connection_timeout=10,
            )
            logger.info(
                "MySQL 연결 성공 [%s:%d/%s]", MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE
            )
            return conn
        except MySQLError as e:
            logger.warning(
                "MySQL 연결 실패 (%d/%d): %s", attempt, DB_CONNECT_RETRIES, e
            )
            if attempt < DB_CONNECT_RETRIES:
                time.sleep(DB_CONNECT_RETRY_DELAY)

    raise RuntimeError(
        f"MySQL 연결 최대 재시도({DB_CONNECT_RETRIES}회) 초과 "
        f"[{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}]"
    )


def ensure_connection(
    conn: mysql.connector.MySQLConnection,
) -> mysql.connector.MySQLConnection:
    """
    연결 상태를 확인하고, 끊어진 경우 재연결한다.
    ping(reconnect=True) 실패 시 새로운 연결을 반환한다.
    """
    try:
        conn.ping(reconnect=True, attempts=3, delay=1)
        return conn
    except MySQLError:
        logger.warning("MySQL 연결 끊김 감지. 재연결 시도...")
        return create_db_connection()


# ──────────────────────────────────────────────
# Kafka Consumer 생성
# ──────────────────────────────────────────────

def create_consumer() -> KafkaConsumer:
    """
    KafkaConsumer를 생성한다.

    핵심 설정:
      enable_auto_commit=False : DB 저장 성공 후 수동으로 offset commit (at-least-once)
      max_poll_records=50      : 1회 poll에서 최대 50개 메시지 처리
      max_poll_interval_ms     : DB 처리 시간을 고려해 충분히 크게 설정
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=False,          # 수동 commit (at-least-once)
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=50,               # 1회 poll 최대 메시지 수
        session_timeout_ms=30_000,         # 브로커가 consumer를 dead로 간주하는 시간
        heartbeat_interval_ms=10_000,      # heartbeat 주기 (session_timeout의 1/3 이하)
        max_poll_interval_ms=300_000,      # poll() 호출 간 최대 허용 간격 (DB 처리 포함)
    )
    logger.info(
        "KafkaConsumer 생성 완료 [topic=%s group=%s bootstrap=%s]",
        KAFKA_TOPIC, KAFKA_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS,
    )
    return consumer


# ──────────────────────────────────────────────
# 날짜 파싱 유틸리티
# ──────────────────────────────────────────────

def parse_postdate(raw: str | None) -> str | None:
    """
    postdate 문자열을 MySQL DATE 형식(YYYY-MM-DD)으로 정규화한다.
    지원 형식: 'YYYYMMDD', 'YYYY-MM-DD'
    파싱 불가 시 None 반환 (DB에 NULL 저장).
    """
    if not raw:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    logger.warning("postdate 파싱 실패 '%s' → NULL 저장", raw)
    return None


def parse_discovered_at(raw: str | None) -> str | None:
    """
    discovered_at ISO8601 문자열을 MySQL DATETIME 형식(YYYY-MM-DD HH:MM:SS)으로 변환한다.
    'Z' suffix와 timezone offset(+00:00) 모두 지원.
    파싱 불가 시 None 반환.
    """
    if not raw:
        return None
    try:
        # 'Z' → '+00:00' 으로 치환하여 fromisoformat 호환
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        logger.warning("discovered_at 파싱 실패 '%s' → NULL 저장", raw)
        return None


# ──────────────────────────────────────────────
# DB INSERT
# ──────────────────────────────────────────────

# INSERT IGNORE: url(PRIMARY KEY) 중복 시 오류 없이 무시
_INSERT_SQL = """
    INSERT IGNORE INTO blog_post
        (url, title, content_text, postdate, discovered_at)
    VALUES
        (%(url)s, %(title)s, %(content_text)s, %(postdate)s, %(discovered_at)s)
"""


def insert_post(cursor, payload: dict) -> bool:
    """
    blog_post 테이블에 레코드를 삽입한다.

    INSERT IGNORE 사용으로 url(PRIMARY KEY) 중복 시 오류 없이 건너뜀.
    반환값:
      True  — 신규 삽입됨 (rowcount == 1)
      False — 중복으로 무시됨 (rowcount == 0)
    """
    params = {
        "url":           payload.get("url"),
        "title":         payload.get("title", ""),
        "content_text":  payload.get("content_text", ""),
        "postdate":      parse_postdate(payload.get("postdate")),
        "discovered_at": parse_discovered_at(payload.get("discovered_at")),
    }
    cursor.execute(_INSERT_SQL, params)
    return cursor.rowcount == 1


# ──────────────────────────────────────────────
# Offset 정밀 Commit 헬퍼
# ──────────────────────────────────────────────

def _commit_offset(consumer: KafkaConsumer, message) -> None:
    """
    특정 메시지의 파티션 오프셋만 정밀하게 commit한다.
    인자 없는 consumer.commit()은 모든 파티션의 마지막 poll offset을 커밋하므로,
    메시지별 처리 시에는 반드시 이 함수를 사용해야 at-least-once가 보장된다.
    """
    tp = TopicPartition(message.topic, message.partition)
    # offset + 1: 다음 처리 시작 위치를 가리킴 (Kafka 관례)
    # kafka-python 2.3.0+: OffsetAndMetadata(offset, metadata, leader_epoch)
    # leader_epoch=-1 은 "알 수 없음"을 의미하는 관례값
    consumer.commit(offsets={tp: OffsetAndMetadata(message.offset + 1, None, -1)})


# ──────────────────────────────────────────────
# 메시지 처리 루프
# ──────────────────────────────────────────────

def process_messages(
    consumer: KafkaConsumer,
    conn: mysql.connector.MySQLConnection,
) -> mysql.connector.MySQLConnection:
    """
    Kafka 메시지를 폴링하고 MySQL에 저장하는 메인 루프.

    at-least-once 보장 전략:
      1. DB INSERT + conn.commit() 성공
      2. _commit_offset()으로 해당 파티션 오프셋만 commit
      → 1과 2 사이에 프로세스가 죽으면 메시지는 재처리됨.
        INSERT IGNORE로 재처리 시 중복 insert가 무해하게 처리됨.

    반환값: 최종 DB 연결 (재연결 후 교체될 수 있으므로 main에서 재할당)
    """
    logger.info("메시지 처리 루프 시작 [poll_timeout=%dms]", CONSUMER_POLL_TIMEOUT_MS)
    total_inserted = 0
    total_skipped  = 0

    while not _shutdown_requested:
        # ── 연결 상태 확인 (끊긴 경우 재연결) ──
        conn = ensure_connection(conn)

        # ── 메시지 폴링 ──
        # poll()은 timeout_ms 동안 대기하다가 메시지가 있으면 즉시 반환한다.
        # 반환 타입: {TopicPartition: [ConsumerRecord, ...]}
        records = consumer.poll(timeout_ms=CONSUMER_POLL_TIMEOUT_MS)

        if not records:
            # 메시지 없음 → 다음 poll 대기
            continue

        for partition, messages in records.items():
            for message in messages:
                # 종료 요청 확인 (메시지 처리 중에도 신속하게 반응)
                if _shutdown_requested:
                    logger.info("종료 요청으로 처리 루프 중단")
                    return conn

                url    = message.key or "(no key)"
                offset = message.offset

                logger.debug(
                    "수신 [topic=%s partition=%d offset=%d] url=%s",
                    message.topic, partition.partition, offset, url,
                )

                try:
                    # ── 1. 메시지 타입 검증 ──
                    payload = message.value
                    if not isinstance(payload, dict):
                        # 역직렬화됐지만 dict가 아닌 경우 (예: 리스트, null)
                        logger.error(
                            "잘못된 메시지 형식 [offset=%d]: %r", offset, payload
                        )
                        # poison pill 방지: 재처리해도 동일 결과이므로 offset commit
                        _commit_offset(consumer, message)
                        continue

                    # ── 2. DB INSERT ──
                    with conn.cursor() as cursor:
                        inserted = insert_post(cursor, payload)
                    conn.commit()   # DB 트랜잭션 커밋

                    # ── 3. Kafka offset commit (DB commit 성공 후에만 실행) ──
                    _commit_offset(consumer, message)

                    if inserted:
                        total_inserted += 1
                        logger.info(
                            "저장 완료 [offset=%d total_inserted=%d] url=%s",
                            offset, total_inserted, url,
                        )
                    else:
                        total_skipped += 1
                        logger.info(
                            "중복 무시 [offset=%d total_skipped=%d] url=%s",
                            offset, total_skipped, url,
                        )

                except MySQLError as e:
                    # DB 오류: rollback 후 offset commit 안 함 → 재시작 시 재처리
                    logger.error(
                        "DB 저장 실패 [offset=%d] url=%s → rollback, 재처리 예정: %s",
                        offset, url, e,
                        exc_info=True,
                    )
                    try:
                        conn.rollback()
                    except MySQLError:
                        pass
                    # 연결 자체가 깨진 경우를 대비해 재연결
                    conn = create_db_connection()

                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                    # 파싱/타입 오류: 재시도해도 동일하므로 commit으로 건너뜀
                    logger.error(
                        "메시지 파싱 오류 [offset=%d] → 건너뜀: %s",
                        offset, e,
                        exc_info=True,
                    )
                    _commit_offset(consumer, message)

                except Exception as e:
                    # 알 수 없는 오류: 보수적으로 offset commit 안 함 (재처리 허용)
                    logger.error(
                        "예기치 않은 오류 [offset=%d] url=%s: %s",
                        offset, url, e,
                        exc_info=True,
                    )

    logger.info(
        "처리 루프 종료 — 총 삽입=%d / 중복무시=%d", total_inserted, total_skipped
    )
    return conn


# ──────────────────────────────────────────────
# main
# ──────────────────────────────────────────────

def main() -> None:
    """
    Consumer 진입점.

    초기화 순서:
      1. 시그널 핸들러 등록 (SIGINT, SIGTERM)
      2. MySQL 연결 (재시도 포함)
      3. KafkaConsumer 생성
      4. 메시지 처리 루프 진입
      5. 종료 시 자원 정리 (finally)
    """
    # ── 시그널 핸들러 등록 ──
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("=" * 60)
    logger.info("Blog Raw Consumer 시작")
    logger.info(
        "  Kafka : %s → topic=%s group=%s",
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID,
    )
    logger.info(
        "  MySQL : %s:%d/%s (user=%s)",
        MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER,
    )
    logger.info("=" * 60)

    consumer: KafkaConsumer | None = None
    conn: mysql.connector.MySQLConnection | None = None

    try:
        # ── DB 및 Kafka 초기화 ──
        conn     = create_db_connection()
        consumer = create_consumer()

        # ── 메시지 처리 루프 ──
        # process_messages는 내부에서 재연결 후 conn을 교체할 수 있으므로 재할당
        conn = process_messages(consumer, conn)

    except KeyboardInterrupt:
        # _handle_signal이 등록돼 있지만 일부 환경에서 직접 도달할 수 있음
        logger.info("KeyboardInterrupt 수신")

    except Exception as e:
        logger.critical("Consumer 초기화/실행 중 치명적 오류: %s", e, exc_info=True)
        sys.exit(1)

    finally:
        # ── 자원 정리 ──
        logger.info("자원 정리 중...")

        if consumer:
            try:
                consumer.close()
                logger.info("KafkaConsumer 종료 완료")
            except Exception as e:
                logger.warning("KafkaConsumer 종료 중 오류: %s", e)

        if conn:
            try:
                conn.close()
                logger.info("MySQL 연결 종료 완료")
            except Exception as e:
                logger.warning("MySQL 연결 종료 중 오류: %s", e)

        logger.info("Blog Raw Consumer 종료 완료")


if __name__ == "__main__":
    main()
