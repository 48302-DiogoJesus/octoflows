from dataclasses import dataclass

from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration


@dataclass
class FullDAGPrepareTime:
    download_time_ms: float
    create_subdags_time_ms: float # time to create a subdag
    size_bytes: int

@dataclass
class TaskOutputMetrics:
    size_bytes: int
    time_ms: float
    normalized_time_ms: float


@dataclass
class TaskInvocationMetrics:
    task_id: str
    time_ms: float


@dataclass
class TaskInputMetrics:
    hardcoded_input_size_bytes: int = 0  # known ahead of time (not "lazy", not DAGTasks)
    downloadable_input_size_bytes: int = 0  # size of inputs that need to be downloaded
    input_download_time_ms: float = 0  # time to download all inputs
    normalized_input_download_time_ms: float = 0  # download time normalized by input size


@dataclass
class TaskMetrics:
    worker_resource_configuration: TaskWorkerResourceConfiguration
    started_at_timestamp_s: float  # time at which the task started being processed by a worker
    
    input_metrics: TaskInputMetrics

    execution_time_ms: float
    execution_time_per_input_byte_ms: float

    update_dependency_counters_time_ms: float
    
    output_metrics: TaskOutputMetrics
    
    total_invocations_count: int
    total_invocation_time_ms: float  # time to do all invocations
