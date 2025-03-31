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
    execution_time_ms: float
    input_metrics: list[TaskInputMetrics]
    output_metrics: TaskOutputMetrics
    downstream_invocation_times: list[TaskInvocationMetrics] | None # Can be None if no downstream task was ready

class MetricsStorage:
    KEY_PREFIX = "metrics-storage-"

    class UploadStrategy(Enum):
        BEFORE_SHUTDOWN = 1
        AFTER_EACH_TASK = 2
        PERIODIC = 3 # requires user to specify interval
        AFTER_N_METRICS = 4 # uses a queue

    @dataclass
    class Config:
        storage_config: Storage.Config
        upload_strategy: "MetricsStorage.UploadStrategy"

        def create_instance(self) -> "MetricsStorage":
            return MetricsStorage(self.storage_config)

    def __init__(self, storage_config: Storage.Config) -> None:
        self.storage = storage_config.create_instance()
        self.cached_metrics: dict[str, TaskMetrics] = {}

    def store_task_metrics(self, task_id: str, metrics: TaskMetrics):
        self.cached_metrics[task_id] = metrics

    def flush(self):
        print("Flushing metrics to storage...")
        start = time.time()

        [
            self.storage.set(
                f"{self.KEY_PREFIX}{key}", 
                cloudpickle.dumps(metrics)
            )
            for key, metrics in self.cached_metrics.items()
        ]
        
        end = time.time()
        print(f"Flushed {len(self.cached_metrics)} metrics to storage in {end - start:.4f} seconds")
        self.cached_metrics = {}