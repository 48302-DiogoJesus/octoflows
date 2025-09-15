#!/bin/bash

# Start Intermediate Storage
docker run -d --name intermediate-storage-redis -p 6379:6379 -v redis-intermediate-data:/data redis redis-server --appendonly yes --appendfsync no --requirepass "redisdevpwd123"

# Start Metrics Storage
docker run -d --name metrics-storage-redis -p 6380:6379 -v redis-metrics-data:/data redis redis-server --appendonly yes --appendfsync no --requirepass "redisdevpwd123"

echo "Redis containers started."
echo "To connect to Redis CLI, use: redis-cli -a redisdevpwd123"
