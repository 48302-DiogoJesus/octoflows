import hashlib
import json
from typing import Literal
import uuid
from dataclasses import dataclass
import time
import asyncio
import cloudpickle
from src.storage.metadata.metrics_types import UserDAGSubmissionMetrics, FullDAGPrepareTime, TaskMetrics, WorkerStartupMetrics
from src.storage.storage import Storage
from src.utils.logger import create_logger

logger = create_logger(__name__)

class MetadataStorage():
    TASK_MD_KEY_PREFIX = "md-storage-tasks+"
    DAG_MD_KEY_PREFIX = "md-storage-dag+"
    PLAN_KEY_PREFIX = "md-storage-plan+"
    WORKER_STARTUP_PREFIX = "md-storage-worker-startup+"
    USER_DAG_SUBMISSION_PREFIX = "md-storage-user-dag-submission+"
    DAG_RESOURCE_USAGE_PREFIX = "md-storage-dag-resource-usage+"

    storage: Storage
    lock: asyncio.Lock

    @dataclass
    class Config:
        storage_config: Storage.Config

        def create_instance(self) -> "MetadataStorage":
            return MetadataStorage(self.storage_config)

    def __init__(self, storage_config: Storage.Config) -> None:
        from src.planning.abstract_dag_planner import AbstractDAGPlanner
        self.storage = storage_config.create_instance()
        self.cached_metrics: dict[str, TaskMetrics | FullDAGPrepareTime | AbstractDAGPlanner.PlanOutput | WorkerStartupMetrics | UserDAGSubmissionMetrics] = {}
        self.lock = asyncio.Lock()

    async def store_dag_submission_time(self, master_dag_id: str, user_dag_submission_metrics: UserDAGSubmissionMetrics):
        async with self.lock:
            self.cached_metrics[f"{self.USER_DAG_SUBMISSION_PREFIX}{master_dag_id}"] = user_dag_submission_metrics

    async def store_task_metrics(self, task_id: str, metrics: TaskMetrics):
        async with self.lock:
            self.cached_metrics[f"{self.TASK_MD_KEY_PREFIX}{task_id}"] = metrics

    async def store_dag_download_time(self, master_dag_id: str, dag_download_metrics: FullDAGPrepareTime):
        unique_id = uuid.uuid4().hex # required because there can be {N} DAG downloads for a single DAG instance
        async with self.lock:
            self.cached_metrics[f"{self.DAG_MD_KEY_PREFIX}{master_dag_id}{unique_id}"] = dag_download_metrics
    
    async def store_plan(self, master_dag_id: str, plan):
        async with self.lock:
            self.cached_metrics[f"{self.PLAN_KEY_PREFIX}{master_dag_id}"] = plan

    async def store_dag_resource_usage_metrics(self, metrics):
        async with self.lock:
            self.cached_metrics[f"{self.DAG_RESOURCE_USAGE_PREFIX}{metrics.master_dag_id}"] = metrics

    async def store_invoker_worker_startup_metrics(self, metrics: WorkerStartupMetrics, task_ids: list[str]):
        """ direct upload to storage so that the INVOKED can find it and complete the missing fields """
        task_ids_hash = hashlib.sha256(json.dumps(task_ids).encode('utf-8')).hexdigest()
        await self.storage.set(f"{self.WORKER_STARTUP_PREFIX}{metrics.master_dag_id}_{task_ids_hash}", cloudpickle.dumps(metrics))

    async def update_invoked_worker_startup_metrics(self, end_time_ms: float, worker_state: Literal["warm", "cold"], task_ids: list[str], master_dag_id: str):
        task_ids_hash = hashlib.sha256(json.dumps(task_ids).encode('utf-8')).hexdigest()
        wsm: WorkerStartupMetrics = cloudpickle.loads(await self.storage.get(f"{self.WORKER_STARTUP_PREFIX}{master_dag_id}_{task_ids_hash}"))
        wsm.end_time_ms = end_time_ms
        wsm.state = worker_state
        async with self.lock:
            self.cached_metrics[f"{self.WORKER_STARTUP_PREFIX}{master_dag_id}_{task_ids_hash}"] = wsm

    async def flush(self):
        async with self.lock:
            start = time.time()
            len_before_flush = len(self.cached_metrics)
            if len_before_flush == 0: return

            keys_to_remove = []
            for key, metrics in self.cached_metrics.items():
                await self.storage.set(key, cloudpickle.dumps(metrics))
                # remove from self.cached_metrics
                keys_to_remove.append(key)

            for key in keys_to_remove: self.cached_metrics.pop(key, None)
            
            end = time.time()
            logger.info(f"Flushed {len_before_flush} metrics to storage in {end - start:.4f} seconds")

    async def close_connection(self):
        await self.storage.close_connection()


BASELINE_MEMORY_MB = 2048 # reference value for normalization