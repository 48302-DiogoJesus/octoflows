import os
import sys
import redis

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


client = redis.Redis(
    host='localhost',
    port=6380,
    password='redisdevpwd123',
    decode_responses=False
)

for item in client.scan_iter():
    print(item.decode())