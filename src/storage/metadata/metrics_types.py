from abc import ABC
from dataclasses import dataclass, field
from typing import Literal

from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration

# NOTE: Prefix "tp" means "task-path", which indicates time spent doing something while handling a task (SYNC)
    # pre-load isn't included in this time, but waiting for pending preloads is

@dataclass
class UserDAGSubmissionMetrics:
    dag_submission_time_ms: float

@dataclass
class FullDAGPrepareTime:
    download_time_ms: float
    create_subdags_time_ms: float # time to create a subdag
    serialized_size_bytes: int

@dataclass
class TaskInputDownloadMetrics:
    serialized_size_bytes: int # for download time prediction
    time_ms: float | None

@dataclass
class TaskInputMetrics:
    hardcoded_input_size_bytes: int = 0  # known ahead of time (not "lazy", not DAGTasks)

    tp_total_time_waiting_for_inputs_ms: float | None = None  # task-path time to download ALL inputs (can be None if was preloaded or if it won't download any inputs). This time includes time waiting for preload to finish

    input_download_metrics: dict[str, TaskInputDownloadMetrics] = field(default_factory=dict) # how much time each input took to download (only inputs that were already available will have time_ms=None)

@dataclass
class TaskOutputMetrics:
    serialized_size_bytes: int # for upload time prediction
    tp_time_ms: float | None

@dataclass
class TaskOptimizationMetrics(ABC): pass

@dataclass
class TaskMetrics:
    worker_resource_configuration: TaskWorkerResourceConfiguration
    started_at_timestamp_s: float  # time at which the task started being processed by a worker
    
    input_metrics: TaskInputMetrics

    tp_execution_time_ms: float
    execution_time_per_input_byte_ms: float | None

    update_dependency_counters_time_ms: float | None
    
    output_metrics: TaskOutputMetrics
    
    total_invocations_count: int
    total_invocation_time_ms: float | None

    planner_used_name: str | None
    optimization_metrics: list[TaskOptimizationMetrics]


@dataclass
class WorkerStartupMetrics:
    master_dag_id: str
    resource_configuration: TaskWorkerResourceConfiguration
    start_time_ms: float
    initial_task_ids: list[str]
    end_time_ms: float | None = None
    state: Literal["warm", "cold"] | None = None


@dataclass
class DAGResourceUsageMetrics:
    master_dag_id: str
    run_time_seconds: float
    cpu_seconds: float
    gb_seconds: float
