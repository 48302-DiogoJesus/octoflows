@REM Intermediate Storage (no persistence)
docker run -d --name intermediate-storage-redis -p 6379:6379 -v redis-intermediate-data:/data redis redis-server --appendonly yes --appendfsync everysec --requirepass "redisdevpwd123"

@REM Metrics Storage (persistent)
docker run -d --name metrics-storage-redis -p 6380:6379 -v redis-metrics-data:/data redis redis-server --appendonly yes --appendfsync everysec --requirepass "redisdevpwd123"

@REM DEBUG: Authenticate redis CLI
redis-cli -a redisdevpwd123