#!/bin/bash

# Flush all data from Redis instances
redis-cli -p 6379 -a "redisdevpwd123" FLUSHALL && \
redis-cli -p 6380 -a "redisdevpwd123" FLUSHALL
