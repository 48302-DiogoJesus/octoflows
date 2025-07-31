#!/bin/bash

# Start Intermediate Storage (no persistence)
docker run -d --name intermediate-storage-redis -p 6379:6379 -v redis-intermediate-data:/data redis redis-server --appendonly no --save 900 1 --save 300 10 --save 60 10000 --requirepass "redisdevpwd123"

# Start Metrics Storage (persistent)
docker run -d --name metrics-storage-redis -p 6380:6379 -v redis-metrics-data:/data redis redis-server --appendonly no --save 900 1 --save 300 10 --save 60 10000 --requirepass "redisdevpwd123"

echo "Redis containers started."
echo "To connect to Redis CLI, use: redis-cli -a redisdevpwd123"
