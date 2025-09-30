@echo off

set REDIS_PASSWORD=redisdevpwd123

echo Backing up metrics-storage-redis...
docker exec metrics-storage-redis redis-cli -a %REDIS_PASSWORD% SAVE
docker run --rm -v redis-metrics-data:/source -v "%cd%":/backup alpine cp /source/dump.rdb /backup/metrics_backup.rdb

echo Backups completed:
echo - metrics_backup.rdb