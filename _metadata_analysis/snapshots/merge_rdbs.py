import subprocess
import time
import sys
import redis
import os
import platform
from pathlib import Path

def run(cmd, check=True):
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and check:
        print("Error:", result.stderr.strip())
        raise subprocess.CalledProcessError(result.returncode, cmd)
    return result

def wait_for_redis(port, timeout=20):
    print(f"⏳ Waiting for Redis on port {port}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = redis.Redis(host="localhost", port=port)
            r.ping()
            print(f"✅ Redis ready on port {port}")
            return True
        except redis.ConnectionError:
            time.sleep(0.5)
    raise RuntimeError(f"Redis on port {port} did not start in time")

def copy_all_keys(src_port, dest_port):
    src = redis.Redis(host="localhost", port=src_port)
    dest = redis.Redis(host="localhost", port=dest_port)
    print(f"🔁 Copying keys from {src_port} → {dest_port}...")
    cursor = 0
    count = 0
    while True:
        cursor, keys = src.scan(cursor=cursor, count=1000)
        for key in keys:
            try:
                dump = src.dump(key)
                ttl = src.ttl(key)
                if dump:
                    if ttl < 0:
                        ttl = 0
                    dest.restore(key, ttl * 1000, dump, replace=True)
                    count += 1
            except redis.exceptions.ResponseError as e:
                # Skip existing or problematic keys
                print(f"⚠️  {e}")
        if cursor == 0:
            break
    print(f"✅ Copied {count} keys from Redis:{src_port}")

def normalize_path_for_docker(path: str) -> str:
    """Convert Windows path (C:\\Users\\me\\file.rdb) → Docker-compatible (/c/Users/me/file.rdb)."""
    path = Path(path).resolve()
    drive, tail = os.path.splitdrive(str(path))
    if platform.system().lower().startswith("win"):
        docker_path = f"/{drive[0].lower()}{tail.replace(os.sep, '/')}"
        return docker_path
    return str(path)

def merge_rdbs(rdb1, rdb2, dest_port=6380):
    temp_ports = [6381, 6382]
    rdb_files = [rdb1, rdb2]
    containers = []

    # Check Docker
    try:
        run(["docker", "info"], check=True)
    except Exception:
        print("❌ Docker is not running. Please start Docker Desktop.")
        sys.exit(1)

    for port, rdb in zip(temp_ports, rdb_files):
        container_name = f"redis_temp_{port}"
        containers.append(container_name)

        docker_rdb_path = normalize_path_for_docker(rdb)

        # Start temporary Redis with the provided RDB file
        run([
            "docker", "run", "-d",
            "--name", container_name,
            "-p", f"{port}:6379",
            "-v", f"{docker_rdb_path}:/data/dump.rdb",
            "redis:7",
        ])
        wait_for_redis(port)

    # Copy from each temp Redis instance into the destination Redis
    for port in temp_ports:
        copy_all_keys(port, dest_port)

    # Cleanup containers
    for container in containers:
        run(["docker", "rm", "-f", container], check=False)

    print("Merge complete!")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python <script_name> first.rdb second.rdb")
        sys.exit(1)

    merge_rdbs(sys.argv[1], sys.argv[2])
