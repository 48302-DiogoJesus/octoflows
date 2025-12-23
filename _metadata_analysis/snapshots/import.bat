@echo off
set REDIS_PASSWORD=redisdevpwd123
set METRICS_FILE=%1

echo Restoring metrics-storage-redis from: %METRICS_FILE%

docker stop metrics-storage-redis

docker run --rm ^
  -v redis-metrics-data:/data ^
  -v "%cd%":/backup ^
  alpine sh -c "rm -f /data/appendonly.aof /data/dump.rdb && cp /backup/%METRICS_FILE% /data/dump.rdb"

docker start metrics-storage-redis
echo Metrics storage restored.

echo Restore completed.
pause
