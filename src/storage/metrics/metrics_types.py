from dataclasses import dataclass

from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration


@dataclass
class FullDAGPrepareTime:
    download_time_ms: float
    create_subdags_time_ms: float # time to create a subdag
    size_bytes: int


@dataclass
class TaskInputMetrics:
    task_id: str
    size_bytes: int
    time_ms: float
    normalized_time_ms: float


@dataclass
class TaskHardcodedInputMetrics:
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
class TaskMetrics:
    worker_resource_configuration: TaskWorkerResourceConfiguration
    started_at_timestamp_s: float # time at which the task started being processed by a worker
    input_metrics: list[TaskInputMetrics]
    hardcoded_input_metrics: list[TaskHardcodedInputMetrics] # known ahead of time (not "lazy", not DAGTasks)
    total_input_download_time_ms: float # time to download all inputs (improves if we download inputs in parallel => this wouldn't be visible just with the input_metrics)
    execution_time_ms: float
    execution_time_per_input_byte_ms: float
    update_dependency_counters_time_ms: float
    output_metrics: TaskOutputMetrics
    total_invocations_count: int
    total_invocation_time_ms: float # time to do all invocations
