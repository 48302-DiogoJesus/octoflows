import requests
import asyncio
import time
from collections import defaultdict
from typing import Any, Optional, Tuple

from src.storage.metrics.metrics_types import DAGResourceUsageMetrics

DOCKER_API = "http://localhost:2375"
SAMPLE_INTERVAL = 1  # seconds

class DockerContainerUsageMonitor:
    _dag_data: dict[str, dict[str, Any]] = {}
    _tasks: dict[str, asyncio.Task] = {}
    # Cache container limits to handle removed containers
    _container_limits_cache: dict[str, Tuple[int, float]] = {}

    @staticmethod
    def _get_containers():
        resp = requests.get(f"{DOCKER_API}/containers/json")
        return resp.json()

    @staticmethod
    def _get_container_limits(container_id) -> Optional[Tuple[int, float]]:
        """Get container limits, return None if container is gone (404)"""
        try:
            resp = requests.get(f"{DOCKER_API}/containers/{container_id}/json")
            resp.raise_for_status()  # Raises an exception for 4xx/5xx status codes
            data = resp.json()
            mem_limit = data["HostConfig"]["Memory"]
            cpu_quota = data["HostConfig"]["CpuQuota"]
            cpu_period = data["HostConfig"]["CpuPeriod"]
            num_cpus = cpu_quota / cpu_period if cpu_quota > 0 else 1
            
            # Cache the limits for this container
            DockerContainerUsageMonitor._container_limits_cache[container_id] = (mem_limit, num_cpus)
            return mem_limit, num_cpus
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Container was removed, try to get from cache
                if container_id in DockerContainerUsageMonitor._container_limits_cache:
                    return DockerContainerUsageMonitor._container_limits_cache[container_id]
                else:
                    # Container was removed before we could cache its limits
                    return None
            else:
                # Re-raise other HTTP errors
                raise

    @staticmethod
    async def _monitor_dag(dag_id):
        data = DockerContainerUsageMonitor._dag_data[dag_id]
        start_time = time.time()
        data["start_time"] = start_time
        memory_seconds = defaultdict(float)
        
        while not data["stop"]:
            # Get containers for this DAG
            containers = DockerContainerUsageMonitor._get_containers()
            for c in containers:
                name = c["Names"][0]
                if dag_id in name:
                    cid = c["Id"]
                    limits = DockerContainerUsageMonitor._get_container_limits(cid)
                    if limits is not None:
                        mem_limit, num_cpus = limits
                        # Lambda-like cost: allocated memory × 1s + CPU × 1s
                        memory_seconds[cid] += mem_limit * SAMPLE_INTERVAL
                    # If limits is None, container was removed - skip but keep accumulated data
                    
            await asyncio.sleep(SAMPLE_INTERVAL)
            
        end_time = time.time()
        data["end_time"] = end_time
        data["memory_seconds"] = memory_seconds

    @staticmethod
    def start_monitoring(dag_id):
        if dag_id in DockerContainerUsageMonitor._tasks:
            raise RuntimeError(f"Monitoring already started for DAG {dag_id}")
        DockerContainerUsageMonitor._dag_data[dag_id] = {"stop": False}
        DockerContainerUsageMonitor._tasks[dag_id] = asyncio.create_task(DockerContainerUsageMonitor._monitor_dag(dag_id))

    @staticmethod
    async def stop_monitoring(dag_id):
        if dag_id not in DockerContainerUsageMonitor._tasks:
            raise RuntimeError(f"No monitoring task for DAG {dag_id}")
        # Signal the coroutine to stop
        DockerContainerUsageMonitor._dag_data[dag_id]["stop"] = True
        # Wait for coroutine to finish
        await DockerContainerUsageMonitor._tasks[dag_id]
        data = DockerContainerUsageMonitor._dag_data.pop(dag_id)
        DockerContainerUsageMonitor._tasks.pop(dag_id)

        runtime = data["end_time"] - data["start_time"]
        total_cpu_seconds = 0
        total_memory_bytes = 0
        
        for cid, mem_sec in data["memory_seconds"].items():
            # Try to get limits, fallback to cache if container was removed
            limits = DockerContainerUsageMonitor._get_container_limits(cid)
            if limits is not None:
                mem_limit, num_cpus = limits
                total_cpu_seconds += num_cpus * runtime
                total_memory_bytes += mem_limit * runtime
            # If container was removed and not in cache, we skip it
            # but the accumulated memory_seconds data is still preserved

        # Clean up cache entries for this DAG's containers
        containers_to_remove = []
        for cid in DockerContainerUsageMonitor._container_limits_cache:
            if any(dag_id in name for name in [cid]):  # This is a simplified check
                containers_to_remove.append(cid)
        for cid in containers_to_remove:
            DockerContainerUsageMonitor._container_limits_cache.pop(cid, None)

        # weighted sum to balance memory influence in the cost with the cpu influence
        total_cost = total_cpu_seconds + total_memory_bytes / (1024**3)

        return DAGResourceUsageMetrics(
            master_dag_id=dag_id,
            run_time_seconds=runtime,
            cpu_seconds=total_cpu_seconds,
            memory_bytes=total_memory_bytes,
            cost=total_cost
        )