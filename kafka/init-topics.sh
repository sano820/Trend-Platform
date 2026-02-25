#!/bin/bash
set -e

echo "Kafka 준비 대기중..."
until kafka-broker-api-versions --bootstrap-server kafka:9092 2>/dev/null; do
    echo "Kafka 미준비. 5초 대기..."
    sleep 5
done

echo "=== 토픽 생성 시작 ==="

for TOPIC in blog.raw blog.cleaned blog.error; do
    kafka-topics --create --if-not-exists \
        --bootstrap-server kafka:9092 \
        --topic "$TOPIC" \
        --partitions 3 \
        --replication-factor 1 \
        --config retention.ms=604800000
    echo "생성 완료: $TOPIC"
done

echo "=== 토픽 생성 완료 ==="
kafka-topics --bootstrap-server kafka:9092 --list
