#!/bin/bash

REDIS_PASSWORD="redisdevpwd123"
METRICS_FILE="$1"

if [ -z "$METRICS_FILE" ] || [ "$METRICS_FILE" = "null" ]; then
    echo "No metrics file provided. Skipping restore."
else
    echo "Restoring metrics-storage-redis from: $METRICS_FILE"

    docker stop metrics-storage-redis

    docker run --rm \
        -v redis-metrics-data:/data \
        -v "$(pwd)":/backup \
        alpine sh -c "rm -f /data/appendonly.aof /data/dump.rdb && cp /backup/$METRICS_FILE /data/dump.rdb"

    docker start metrics-storage-redis
    echo "Metrics storage restored."
fi

echo "Restore completed."
