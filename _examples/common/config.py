"""
Centralized configuration for all workflow examples.
This module provides pre-configured worker and storage configurations
that should be used by all workflow examples to maintain consistency.
"""
from typing import Optional, List
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

# Import worker config functions
from .worker_config import (
    get_local_worker_config,
    get_docker_worker_config,
    get_redis_storage_config,
    IN_MEMORY_STORAGE_CONFIG,
    REDIS_INTERMEDIATE_STORAGE_CONFIG,
    REDIS_METRICS_STORAGE_CONFIG
)

# Default storage configurations
REDIS_INTERMEDIATE_CONFIG = get_redis_storage_config(port=6379)
REDIS_METRICS_CONFIG = get_redis_storage_config(port=6380)
IN_MEMORY_CONFIG = IN_MEMORY_STORAGE_CONFIG

# Default worker configurations
LOCAL_WORKER_CONFIG = get_local_worker_config(
    intermediate_storage_config=REDIS_INTERMEDIATE_CONFIG
)

DOCKER_WORKER_CONFIG = get_docker_worker_config(
    planner_type="first",
    intermediate_storage_config=REDIS_INTERMEDIATE_CONFIG,
    metrics_storage_config=REDIS_METRICS_CONFIG,
    docker_gateway_address="http://localhost:5000",
    worker_resource_configuration=TaskWorkerResourceConfiguration(cpus=2, memory_mb=512)
)

def get_worker_config(
    worker_type: str = "docker",
    planner_type: Optional[str] = None,
    **kwargs
):
    """
    Get a worker configuration with the specified settings.
    
    Args:
        worker_type: Type of worker ("docker" or "local")
        planner_type: Type of planner to use ("first", "second", or "simple")
        **kwargs: Additional arguments to pass to the worker configuration
        
    Returns:
        A worker configuration object
    """
    if worker_type.lower() == "local":
        return LOCAL_WORKER_CONFIG
    
    # For Docker workers, allow overriding planner type and other settings
    worker_config = DOCKER_WORKER_CONFIG
    
    if planner_type is not None:
        worker_config = get_docker_worker_config(
            planner_type=planner_type,
            intermediate_storage_config=REDIS_INTERMEDIATE_CONFIG,
            metrics_storage_config=REDIS_METRICS_CONFIG,
            docker_gateway_address="http://localhost:5000",
            **kwargs
        )
    
    return worker_config
