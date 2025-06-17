from dataclasses import dataclass, field
from typing import Literal

from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration


@dataclass
class FullDAGPrepareTime:
    download_time_ms: float
    create_subdags_time_ms: float # time to create a subdag
    size_bytes: int

@dataclass
class TaskInputDownloadMetrics:
    size_bytes: int
    time_ms: float

@dataclass
class TaskInputMetrics:
    hardcoded_input_size_bytes: int = 0  # known ahead of time (not "lazy", not DAGTasks)

    tp_total_time_waiting_for_inputs_ms: float = -1  # task-path time to download ALL inputs (can be -1 if was preloaded or if it won't download any inputs). This time includes time waiting for preload to finish

    input_download_metrics: dict[str, TaskInputDownloadMetrics] = field(default_factory=dict) # how much time each input took to download (only inputs that were already available will have time_ms=-1)

@dataclass
class TaskOutputMetrics:
    size_bytes: int
    time_ms: float | Literal[-1]

@dataclass
class TaskMetrics:
    worker_resource_configuration: TaskWorkerResourceConfiguration
    started_at_timestamp_s: float  # time at which the task started being processed by a worker
    
    input_metrics: TaskInputMetrics

    execution_time_ms: float
    execution_time_per_input_byte_ms: float

    update_dependency_counters_time_ms: float | Literal[-1]
    
    output_metrics: TaskOutputMetrics
    
    total_invocations_count: int
    total_invocation_time_ms: float | Literal[-1]
