from src.planning.optimizations.preload import PreLoadOptimization
from src.planning.optimizations.prewarm import PreWarmOptimization
from src.planning.optimizations.taskdup import TaskDupOptimization
from src.planning.optimizations.wukong_optimizations import WukongOptimizations
from src.planning.sla import Percentile, SLA
from src.storage.redis_storage import RedisStorage
from src.workers.docker_worker import DockerWorker
from src.storage.metrics.metrics_storage import MetricsStorage
from src.planning.uniform_planner import UniformPlanner
from src.planning.non_uniform_planner import NonUniformPlanner
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.wukong_planner import WUKONGPlanner
import sys
import os

def get_planner_from_sys_argv():
    supported_planners = ["wukong", "wukong-opt", "simple", "uniform", "non-uniform"]
    
    if len(sys.argv) < 2:
        print(f"Usage: python <script.py> <planner_type: {supported_planners}>")
        sys.exit(-1)
    
    script_name = os.path.basename(sys.argv[0])

    planner_type = sys.argv[1]
    if planner_type not in supported_planners:
        print(f"Unknown planner type: {planner_type}")
        sys.exit(-1)

    montage_min_worker_resource_config = TaskWorkerResourceConfiguration(cpus=3, memory_mb=8192)
    min_resources = montage_min_worker_resource_config if script_name == "montage.py" else TaskWorkerResourceConfiguration(cpus=3, memory_mb=512)

    sla: SLA
    sla_str: str = sys.argv[2]
    if sla_str != "average":
        if int(sla_str) not in range(1, 101):
            print(f"Invalid SLA: {sla_str}. Accepted: 'average' or 0-100 (for percentile)")
            sys.exit(-1)
        sla = Percentile(int(sla_str))
    else:
        sla = "average"

    if planner_type == "wukong":
        return WUKONGPlanner.Config(
            sla=sla, # won't be used
            worker_resource_configurations=[min_resources],
            optimizations=[],
        )
    elif planner_type == "wukong-opt":
        return WUKONGPlanner.Config(
            sla=sla,
            worker_resource_configurations=[min_resources],
            optimizations=[
                WukongOptimizations.configured(
                    task_clustering_fan_outs=True, 
                    task_clustering_fan_ins=True, 
                    delayed_io=True, 
                    large_output_b=5 * 1024 * 1024 # 5MB
                ) 
            ],
        )
    elif planner_type == "simple":
        return UniformPlanner.Config(
            sla=sla,
            worker_resource_configurations=[min_resources],
            optimizations=[],
        )
    elif planner_type == "uniform":
        return UniformPlanner.Config(
            sla=sla,
            worker_resource_configurations=[min_resources],
            optimizations=[PreLoadOptimization, TaskDupOptimization],
        )
    elif planner_type == "non-uniform":
        return NonUniformPlanner.Config(
            sla=sla,
            worker_resource_configurations=[
                min_resources,
                min_resources.clone(cpus=min_resources.cpus, memory_mb=min_resources.memory_mb * 2),
                min_resources.clone(cpus=min_resources.cpus, memory_mb=min_resources.memory_mb * 4),
            ],
            optimizations=[PreLoadOptimization, TaskDupOptimization, PreWarmOptimization]
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

# WORKER CONFIGS
WORKER_CONFIG = DockerWorker.Config(
    external_docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=_REDIS_INTERMEDIATE_STORAGE_CONFIG,
    metrics_storage_config=MetricsStorage.Config(storage_config=_REDIS_METRICS_STORAGE_CONFIG),
    planner_config=get_planner_from_sys_argv()
)