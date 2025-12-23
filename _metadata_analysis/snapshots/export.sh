docker exec metrics-storage-redis \
  redis-cli -h 10.15.0.22 -a "redisdevpwd123" SAVE

docker cp metrics-storage-redis:/data/dump.rdb ./metrics_backup.rdb
