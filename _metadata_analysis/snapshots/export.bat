@echo off

set REDIS_PASSWORD=redisdevpwd123
set BACKUP_DATE=%date:~-4,4%%date:~-10,2%%date:~-7,2%

echo Creating Redis backups...

echo Backing up intermediate-storage-redis...
docker exec intermediate-storage-redis redis-cli -a %REDIS_PASSWORD% SAVE
docker run --rm -v redis-intermediate-data:/source -v "%cd%":/backup alpine cp /source/dump.rdb /backup/intermediate_backup_%BACKUP_DATE%.rdb

echo Backing up metrics-storage-redis...
docker exec metrics-storage-redis redis-cli -a %REDIS_PASSWORD% SAVE
docker run --rm -v redis-metrics-data:/source -v "%cd%":/backup alpine cp /source/dump.rdb /backup/metrics_backup_%BACKUP_DATE%.rdb

echo Backups completed:
echo - intermediate_backup_%BACKUP_DATE%.rdb
echo - metrics_backup_%BACKUP_DATE%.rdb