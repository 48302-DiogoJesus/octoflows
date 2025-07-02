from typing import Optional, List, Literal
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.redis_storage import RedisStorage
from src.storage.in_memory_storage import InMemoryStorage
from src.workers.docker_worker import DockerWorker
from src.workers.local_worker import LocalWorker
from src.storage.metrics.metrics_storage import MetricsStorage
from src.planning.simple_planner_algorithm import SimplePlannerAlgorithm
from src.planning.first_planner_algorithm import FirstPlannerAlgorithm
from src.planning.second_planner_algorithm import SecondPlannerAlgorithm
import sys

def get_planner_from_sys_argv():
    supported_planners = ["simple", "first", "second"]
    
    if len(sys.argv) < 2:
        print(f"Usage: python <script.py> <planner_type: {supported_planners}>")
        sys.exit(1)
        
    planner_type = sys.argv[1]
    if planner_type not in supported_planners:
        print(f"Unknown planner type: {planner_type}")
        sys.exit(1)

    print(f"Using planner type: {planner_type}")

    if planner_type == "simple":
        return SimplePlannerAlgorithm.Config(
            sla="avg",
            all_flexible_workers=False,
            worker_resource_configuration=TaskWorkerResourceConfiguration(cpus=2, memory_mb=512),
        )
    elif planner_type == "first":
        return FirstPlannerAlgorithm.Config(
            sla="avg",
            worker_resource_configuration=TaskWorkerResourceConfiguration(cpus=2, memory_mb=512),
        )
    elif planner_type == "second":
        return SecondPlannerAlgorithm.Config(
            sla="avg",
            available_worker_resource_configurations=[
                TaskWorkerResourceConfiguration(cpus=2, memory_mb=512),
                TaskWorkerResourceConfiguration(cpus=3, memory_mb=1024)
            ]
        )
    else:
        raise ValueError(f"Unhandled planner type: {planner_type}")

# STORAGE CONFIGS
_REDIS_INTERMEDIATE_STORAGE_CONFIG = RedisStorage.Config(
    host="localhost",
    port=6379,
    password="redisdevpwd123"
)

_REDIS_METRICS_STORAGE_CONFIG = RedisStorage.Config(
    host="localhost",
    port=6380,
    password="redisdevpwd123"
)

_IN_MEMORY_STORAGE_CONFIG = InMemoryStorage.Config()

_IN_MEMORY_CONFIG = _IN_MEMORY_STORAGE_CONFIG

# WORKER CONFIGS
_LOCAL_WORKER_CONFIG = LocalWorker.Config(
    intermediate_storage_config=_REDIS_INTERMEDIATE_STORAGE_CONFIG,
    metadata_storage_config=_REDIS_METRICS_STORAGE_CONFIG,
)

_DOCKER_WORKER_CONFIG = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=_REDIS_INTERMEDIATE_STORAGE_CONFIG,
    metrics_storage_config=MetricsStorage.Config(storage_config=_REDIS_METRICS_STORAGE_CONFIG),
    planner_config=get_planner_from_sys_argv()
)

WORKER_CONFIG = _DOCKER_WORKER_CONFIG