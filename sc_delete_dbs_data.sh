#!/bin/bash

# Flush all data from Redis instances
redis-cli -h 146.193.41.54 -p 6379 -a "redisdevpwd123" FLUSHALL && \
redis-cli -h 146.193.41.54 -p 6380 -a "redisdevpwd123" FLUSHALL
