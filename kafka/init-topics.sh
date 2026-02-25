#!/bin/bash
set -e

echo "Kafka 준비 대기중..."
# 변경: 하드코딩 제거, 환경변수로만 설정
: "${KAFKA_BOOTSTRAP_SERVERS:?환경변수 KAFKA_BOOTSTRAP_SERVERS 필요}"
: "${KAFKA_TOPICS:?환경변수 KAFKA_TOPICS 필요}"
: "${KAFKA_TOPIC_PARTITIONS:?환경변수 KAFKA_TOPIC_PARTITIONS 필요}"
: "${KAFKA_TOPIC_REPLICATION_FACTOR:?환경변수 KAFKA_TOPIC_REPLICATION_FACTOR 필요}"
: "${KAFKA_TOPIC_RETENTION_MS:?환경변수 KAFKA_TOPIC_RETENTION_MS 필요}"

until kafka-broker-api-versions --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" 2>/dev/null; do
    echo "Kafka 미준비. 5초 대기..."
    sleep 5
done

echo "=== 토픽 생성 시작 ==="

IFS=',' read -ra TOPIC_LIST <<< "${KAFKA_TOPICS}" # 변경: .env에 정의된 토픽 목록 사용
for TOPIC in "${TOPIC_LIST[@]}"; do
    kafka-topics --create --if-not-exists \
        --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
        --topic "$TOPIC" \
        --partitions "${KAFKA_TOPIC_PARTITIONS}" \
        --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
        --config "retention.ms=${KAFKA_TOPIC_RETENTION_MS}"
    echo "생성 완료: $TOPIC"
done


echo "=== 토픽 생성 완료 ==="
kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" --list
