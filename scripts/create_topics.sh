#!/usr/bin/env bash
set -euo pipefail

BROKERS="${REDPANDA_BROKERS:-localhost:19092}"
RAW_TOPIC="${RAW_TOPIC:-raw.trades}"
DLQ_TOPIC="${DLQ_TOPIC:-dead-letter.trades}"
PARTITIONS="${PARTITIONS:-3}"
RETENTION_RAW_MS="${RETENTION_RAW_MS:-604800000}"
RETENTION_DLQ_MS="${RETENTION_DLQ_MS:-2592000000}"

echo "Creating topics via rpk..."

rpk topic create "$RAW_TOPIC" \
    --partitions "$PARTITIONS" \
    --replicas 1 \
    -c "retention.ms=$RETENTION_RAW_MS" \
    --brokers "$BROKERS" || echo "Topic $RAW_TOPIC may already exist"

rpk topic create "$DLQ_TOPIC" \
    --partitions "$PARTITIONS" \
    --replicas 1 \
    -c "retention.ms=$RETENTION_DLQ_MS" \
    --brokers "$BROKERS" || echo "Topic $DLQ_TOPIC may already exist"

echo "Topics:"
rpk topic list --brokers "$BROKERS"
