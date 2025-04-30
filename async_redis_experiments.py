import time
import asyncio
import redis
from redis.asyncio import Redis as AsyncRedis

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6380
OPERATIONS_COUNT = 1000
KEY_PREFIX = 'test_key'
VALUE_SIZE = 100  # bytes

def generate_value(size):
    return 'x' * size

def sync_redis_operations():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password='redisdevpwd123')
    value = generate_value(VALUE_SIZE)
    
    # Test SET operations
    start_time = time.time()
    for i in range(OPERATIONS_COUNT):
        r.set(f"{KEY_PREFIX}_{i}", value)
    set_time = time.time() - start_time
    
    # Test GET operations
    start_time = time.time()
    for i in range(OPERATIONS_COUNT):
        r.get(f"{KEY_PREFIX}_{i}")
    get_time = time.time() - start_time
    
    # Cleanup
    for i in range(OPERATIONS_COUNT):
        r.delete(f"{KEY_PREFIX}_{i}")
    
    return set_time, get_time

async def async_redis_operations():
    r = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, password='redisdevpwd123')
    value = generate_value(VALUE_SIZE)
    
    # Test SET operations
    start_time = time.time()
    for i in range(OPERATIONS_COUNT):
        await r.set(f"{KEY_PREFIX}_{i}", value)
    set_time = time.time() - start_time
    
    # Test GET operations
    start_time = time.time()
    for i in range(OPERATIONS_COUNT):
        await r.get(f"{KEY_PREFIX}_{i}")
    get_time = time.time() - start_time
    
    # Cleanup
    for i in range(OPERATIONS_COUNT):
        await r.delete(f"{KEY_PREFIX}_{i}")
    
    return set_time, get_time

async def async_redis_operations_bulk():
    r = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, password='redisdevpwd123')
    value = generate_value(VALUE_SIZE)
    
    # Test SET operations with pipeline
    start_time = time.time()
    async with r.pipeline() as pipe:
        for i in range(OPERATIONS_COUNT):
            pipe.set(f"{KEY_PREFIX}_{i}", value)
        await pipe.execute()
    set_time = time.time() - start_time
    
    # Test GET operations with pipeline
    start_time = time.time()
    async with r.pipeline() as pipe:
        for i in range(OPERATIONS_COUNT):
            pipe.get(f"{KEY_PREFIX}_{i}")
        await pipe.execute()
    get_time = time.time() - start_time
    
    # Cleanup
    async with r.pipeline() as pipe:
        for i in range(OPERATIONS_COUNT):
            pipe.delete(f"{KEY_PREFIX}_{i}")
        await pipe.execute()
    
    return set_time, get_time

def print_results(title, set_time, get_time):
    print(f"\n{title} Results:")
    print(f"Total SET operations: {OPERATIONS_COUNT} in {set_time:.4f} seconds")
    print(f"SET ops/sec: {OPERATIONS_COUNT/set_time:.2f}")
    print(f"Total GET operations: {OPERATIONS_COUNT} in {get_time:.4f} seconds")
    print(f"GET ops/sec: {OPERATIONS_COUNT/get_time:.2f}")

def main():
    print(f"Comparing Redis sync vs async performance with {OPERATIONS_COUNT} operations")
    
    # Synchronous test
    set_time, get_time = sync_redis_operations()
    print_results("Synchronous", set_time, get_time)
    
    # Asynchronous test (sequential)
    set_time, get_time = asyncio.run(async_redis_operations())
    print_results("Asynchronous (sequential)", set_time, get_time)
    
    # Asynchronous test (bulk/pipelined)
    set_time, get_time = asyncio.run(async_redis_operations_bulk())
    print_results("Asynchronous (bulk/pipelined)", set_time, get_time)

if __name__ == "__main__":
    main()