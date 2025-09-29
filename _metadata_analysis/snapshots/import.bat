@echo off
set REDIS_PASSWORD=redisdevpwd123
set METRICS_FILE=%1

if "%METRICS_FILE%"=="null" goto skip_metrics
if "%METRICS_FILE%"=="" goto skip_metrics

echo Restoring metrics-storage-redis from: %METRICS_FILE%

docker stop metrics-storage-redis

docker run --rm ^
  -v redis-metrics-data:/data ^
  -v "%cd%":/backup ^
  alpine sh -c "rm -f /data/appendonly.aof /data/dump.rdb && cp /backup/%METRICS_FILE% /data/dump.rdb"

docker start metrics-storage-redis
echo Metrics storage restored.

:skip_metrics
echo Restore completed.
pause
