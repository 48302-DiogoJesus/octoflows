import asyncio
from dataclasses import dataclass
import time

import cloudpickle
from src.storage.metrics.metrics_types import FullDAGPrepareTime, TaskMetrics
from src.storage.storage import Storage
from src.utils.logger import create_logger

logger = create_logger(__name__)

class MetricsStorage():
    TASK_METRICS_KEY_PREFIX = "metrics-storage-tasks-"
    DAG_METRICS_KEY_PREFIX = "metrics-storage-dag-"

    @dataclass
    class Config:
        storage_config: Storage.Config

        def create_instance(self) -> "MetricsStorage":
            return MetricsStorage(self.storage_config)

    def __init__(self, storage_config: Storage.Config) -> None:
        self.storage = storage_config.create_instance()
        self.cached_metrics: dict[str, TaskMetrics | FullDAGPrepareTime] = {}

    async def keys(self, pattern: str) -> list:
        return await self.storage.keys(pattern)

    async def get(self, key: str) -> TaskMetrics | FullDAGPrepareTime | None:
        return cloudpickle.loads(await self.storage.get(key))
    
    async def mget(self, keys: list[str]) -> list[TaskMetrics | FullDAGPrepareTime]:
        return [cloudpickle.loads(m) for m in await self.storage.mget(keys)]

    def store_task_metrics(self, task_id: str, metrics: TaskMetrics):
        # logger.info(f"Caching metrics for task {task_id}: {len(metrics.input_metrics)}")
        self.cached_metrics[f"{self.TASK_METRICS_KEY_PREFIX}{task_id}"] = metrics

    def store_dag_download_time(self, id: str, dag_download_metrics: FullDAGPrepareTime):
        # logger.info(f"Caching download time for root node {id}: {dag_download_metrics.download_time_ms} ms, {dag_download_metrics.size_bytes} bytes")
        self.cached_metrics[f"{self.DAG_METRICS_KEY_PREFIX}{id}"] = dag_download_metrics
    
    async def flush(self):
        start = time.time()
        if len(self.cached_metrics) == 0: return

        coroutines = []
        keys_to_remove = []
        for key, metrics in self.cached_metrics.items():
            coroutines.append(self.storage.set(key, cloudpickle.dumps(metrics)))
            # remove from self.cached_metrics
            keys_to_remove.append(key)

        if len(coroutines) > 0: await asyncio.gather(*coroutines)
        for key in keys_to_remove: self.cached_metrics.pop(key, None)
        
        end = time.time()
        logger.info(f"Flushed {len(self.cached_metrics)} metrics to storage in {end - start:.4f} seconds")


BASELINE_MEMORY_MB = 512 # for normalization