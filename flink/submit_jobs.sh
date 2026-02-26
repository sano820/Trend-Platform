#!/usr/bin/env bash
set -euo pipefail

JOBMANAGER_URL="${JOBMANAGER_URL:-http://flink-jobmanager:8081}"

echo "[submitter] Waiting for JobManager at ${JOBMANAGER_URL}..."
for i in {1..30}; do
  if curl -sSf "${JOBMANAGER_URL}/overview" >/dev/null; then
    echo "[submitter] JobManager is up."
    break
  fi
  echo "[submitter] Not ready yet, retrying... (${i}/30)"
  sleep 2
done

echo "[submitter] Submitting jobs..."
/opt/flink/bin/flink run -py /opt/flink/usrlib/traffic_10m_to_redis_job.py
/opt/flink/bin/flink run -py /opt/flink/usrlib/top_tokens_10m_to_redis_job.py

echo "[submitter] Done."
