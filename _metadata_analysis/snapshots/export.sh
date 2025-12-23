docker exec metrics-storage-redis \
  redis-cli -h 10.15.0.22 -a "$REDIS_PASSWORD" SAVE

docker cp metrics-storage-redis:/data/dump.rdb ./metrics_backup.rdb
