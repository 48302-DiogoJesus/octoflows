import asyncio
import atexit
from dataclasses import dataclass
from enum import Enum
import time

import cloudpickle
import concurrent
from src.storage.storage import Storage

@dataclass
class TaskInputMetrics:
    task_id: str
    size: float
    time_ms: float

@dataclass
class TaskOutputMetrics:
    size: float
    time_ms: float

@dataclass
class TaskInvocationMetrics:
    task_id: str
    time_ms: float

@dataclass
class TaskMetrics:
    worker_id: str
    started_at_timestamp: float # time at which the task started being processed by a worker
    input_metrics: list[TaskInputMetrics]
    total_input_download_time_ms: float # time to download all inputs (improves if we download inputs in parallel => this wouldn't be visible just with the input_metrics)
    execution_time_ms: float
    update_dependency_counters_time_ms: float
    output_metrics: TaskOutputMetrics
    downstream_invocation_times: list[TaskInvocationMetrics] | None # Can be None if no downstream task was ready
    total_invocation_time_ms: float # time to do all invocations

class MetricsStorage:
    KEY_PREFIX = "metrics-storage-"

    # class UploadStrategy(Enum):
    #     BEFORE_SHUTDOWN = 1
    #     AFTER_EACH_TASK = 2
    #     PERIODIC = 3 # requires user to specify interval
    #     AFTER_N_METRICS = 4 # uses a queue

    @dataclass
    class Config:
        storage_config: Storage.Config
        # upload_strategy: "MetricsStorage.UploadStrategy"

        def create_instance(self) -> "MetricsStorage":
            return MetricsStorage(self.storage_config)

    def __init__(self, storage_config: Storage.Config) -> None:
        self.storage = storage_config.create_instance()
        self.cached_metrics: dict[str, TaskMetrics] = {}

    def store_task_metrics(self, task_id: str, metrics: TaskMetrics):
        print(f"Caching metrics for task {task_id}: {len(metrics.input_metrics)}")
        self.cached_metrics[task_id] = metrics

    def flush(self):
        print("Flushing metrics to storage...")
        start = time.time()

        for key, metrics in self.cached_metrics.items():
            self.storage.set(
                f"{self.KEY_PREFIX}{key}", 
                cloudpickle.dumps(metrics)
            )
        
        end = time.time()
        print(f"Flushed {len(self.cached_metrics)} metrics to storage in {end - start:.4f} seconds")
        self.cached_metrics = {}