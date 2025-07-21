@echo off

set REDIS_PASSWORD=redisdevpwd123
set INTERMEDIATE_FILE=%1
set METRICS_FILE=%2

echo Redis Restore Script
echo ====================

if "%INTERMEDIATE_FILE%"=="null" goto skip_intermediate
if "%INTERMEDIATE_FILE%"=="" goto skip_intermediate

echo Restoring intermediate-storage-redis from: %INTERMEDIATE_FILE%
docker stop intermediate-storage-redis
docker run --rm -v redis-intermediate-data:/data -v "%cd%":/backup alpine sh -c "rm -f /data/dump.rdb && cp /backup/%INTERMEDIATE_FILE% /data/dump.rdb"
docker start intermediate-storage-redis
echo Intermediate storage restored.

:skip_intermediate
if "%METRICS_FILE%"=="null" goto skip_metrics
if "%METRICS_FILE%"=="" goto skip_metrics

echo Restoring metrics-storage-redis from: %METRICS_FILE%
docker stop metrics-storage-redis
docker run --rm -v redis-metrics-data:/data -v "%cd%":/backup alpine sh -c "rm -f /data/dump.rdb && cp /backup/%METRICS_FILE% /data/dump.rdb"
docker start metrics-storage-redis
echo Metrics storage restored.

:skip_metrics
echo Restore completed.
pause