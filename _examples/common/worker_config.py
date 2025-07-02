from typing import List, Optional, Union, Dict, Any, Type
from src.workers.docker_worker import DockerWorker
from src.workers.local_worker import LocalWorker
from src.storage.redis_storage import RedisStorage
from src.storage.in_memory_storage import InMemoryStorage
from src.planning.first_planner_algorithm import FirstPlannerAlgorithm
from src.planning.second_planner_algorithm import SecondPlannerAlgorithm
from src.planning.simple_planner_algorithm import SimplePlannerAlgorithm
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.metrics.metrics_storage import MetricsStorage

# Common storage configurations
def get_redis_storage_config(port: int = 6379) -> RedisStorage.Config:
    """Get a Redis storage configuration.
    
    Args:
        port: The port number for the Redis server
        
    Returns:
        RedisStorage.Config: Configured Redis storage configuration
    """
    return RedisStorage.Config(
        host="localhost",
        port=port,
        password="redisdevpwd123"
    )

# Default storage configurations
REDIS_INTERMEDIATE_STORAGE_CONFIG = get_redis_storage_config(port=6379)
REDIS_METRICS_STORAGE_CONFIG = get_redis_storage_config(port=6380)
IN_MEMORY_STORAGE_CONFIG = InMemoryStorage.Config()

def get_local_worker_config(
    intermediate_storage_config: RedisStorage.Config = REDIS_INTERMEDIATE_STORAGE_CONFIG,
    metadata_storage_config: Optional[RedisStorage.Config] = None
) -> LocalWorker.Config:
    """
    Get a LocalWorker configuration with the specified storage settings.
    
    Args:
        intermediate_storage_config: Configuration for intermediate storage
        metadata_storage_config: Configuration for metadata storage. If None, uses intermediate_storage_config
        
    Returns:
        LocalWorker.Config: Configured local worker configuration
    """
    if metadata_storage_config is None:
        metadata_storage_config = intermediate_storage_config
        
    return LocalWorker.Config(
        intermediate_storage_config=intermediate_storage_config,
        metadata_storage_config=metadata_storage_config,
    )

def get_docker_worker_config(
    planner_type: str = "first",
    intermediate_storage_config: RedisStorage.Config = REDIS_INTERMEDIATE_STORAGE_CONFIG,
    metrics_storage_config: RedisStorage.Config = REDIS_METRICS_STORAGE_CONFIG,
    docker_gateway_address: str = "http://localhost:5000",
    worker_resource_configuration: Optional[TaskWorkerResourceConfiguration] = None,
    available_worker_resource_configurations: Optional[List[TaskWorkerResourceConfiguration]] = None,
    all_flexible_workers: Optional[bool] = None,
    **planner_kwargs: Any
) -> DockerWorker.Config:
    """
    Get a DockerWorker configuration with the specified settings.
    
    Args:
        planner_type: Type of planner to use ("first", "second", or "simple")
        intermediate_storage_config: Configuration for intermediate storage
        metrics_storage_config: Configuration for metrics storage
        docker_gateway_address: Address of the Docker gateway
        worker_resource_configuration: Resource configuration for the worker
        available_worker_resource_configurations: List of resource configs (for 'second' planner)
        all_flexible_workers: Whether all workers are flexible (for 'simple' planner)
        **planner_kwargs: Additional arguments to pass to the planner configuration
            
    Returns:
        DockerWorker.Config: Configured Docker worker configuration
    """
    planner_type = planner_type.lower()
    
    # Set default resource configuration if not provided
    if worker_resource_configuration is None:
        worker_resource_configuration = TaskWorkerResourceConfiguration(cpus=2, memory_mb=512)
    
    # Start with base configuration
    base_config = {
        "sla": "avg",
        "worker_resource_configuration": worker_resource_configuration
    }
    
    # Handle planner-specific configurations
    if planner_type == "second":
        if available_worker_resource_configurations is None:
            available_worker_resource_configurations = [
                TaskWorkerResourceConfiguration(cpus=2, memory_mb=512),
                TaskWorkerResourceConfiguration(cpus=3, memory_mb=1024)
            ]
        base_config["available_worker_resource_configurations"] = available_worker_resource_configurations
    elif planner_type == "simple" and all_flexible_workers is not None:
        base_config["all_flexible_workers"] = all_flexible_workers
    
    # Merge with any additional planner kwargs
    planner_config = {**base_config, **planner_kwargs}
    
    # Select the appropriate planner class based on the type
    planner_class = {
        "first": FirstPlannerAlgorithm,
        "second": SecondPlannerAlgorithm,
        "simple": SimplePlannerAlgorithm
    }.get(planner_type, FirstPlannerAlgorithm)
    
    # Filter out None values from the config
    filtered_config = {k: v for k, v in planner_config.items() if v is not None}
    
    return DockerWorker.Config(
        docker_gateway_address=docker_gateway_address,
        intermediate_storage_config=intermediate_storage_config,
        metrics_storage_config=MetricsStorage.Config(storage_config=metrics_storage_config),
        planner_config=planner_class.Config(**filtered_config)
    )

def get_storage_config(storage_type: str = "redis", **kwargs: Any) -> Union[RedisStorage.Config, InMemoryStorage.Config]:
    """
    Get a storage configuration.
    
    Args:
        storage_type: Type of storage ("redis" or "memory")
        **kwargs: Additional arguments to pass to the storage configuration
            For Redis: host, port, password
            
    Returns:
        Storage configuration object (RedisStorage.Config or InMemoryStorage.Config)
        
    Raises:
        ValueError: If an unsupported storage type is provided
    """
    if storage_type.lower() == "redis":
        return RedisStorage.Config(
            host=kwargs.get("host", "localhost"),
            port=kwargs.get("port", 6379),
            password=kwargs.get("password", "redisdevpwd123")
        )
    elif storage_type.lower() == "memory":
        return InMemoryStorage.Config()
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")
