@REM Intermediate Storage
docker run -d --name intermediate-storage-redis -p 6379:6379 -v redis-intermediate-data:/data redis redis-server --appendonly no --appendfsync no --requirepass "redisdevpwd123"

@REM Metrics Storage
docker run -d --name metrics-storage-redis -p 6380:6379 -v redis-metrics-data:/data redis redis-server --appendonly no --appendfsync no --requirepass "redisdevpwd123"

@REM DEBUG: Authenticate redis CLI
redis-cli -a redisdevpwd123