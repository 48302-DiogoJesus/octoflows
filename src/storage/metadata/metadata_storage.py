import hashlib
import json
from typing import Literal
import uuid
from dataclasses import dataclass
import time
import asyncio
import cloudpickle
from src.storage.metadata.metrics_types import UserDAGSubmissionMetrics, EndWorkerMetrics, TaskMetrics, WorkerStartupMetrics
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
        self.cached_metrics: dict[str, TaskMetrics | EndWorkerMetrics | AbstractDAGPlanner.PlanOutput | WorkerStartupMetrics | UserDAGSubmissionMetrics] = {}
        self.lock = asyncio.Lock()

    async def store_dag_submission_time(self, master_dag_id: str, user_dag_submission_metrics: UserDAGSubmissionMetrics):
        async with self.lock:
            self.cached_metrics[f"{self.USER_DAG_SUBMISSION_PREFIX}{master_dag_id}"] = user_dag_submission_metrics

    async def store_task_metrics(self, task_id: str, metrics: TaskMetrics):
        async with self.lock:
            self.cached_metrics[f"{self.TASK_MD_KEY_PREFIX}{task_id}"] = metrics

    async def store_workflow_end_metrics(self, master_dag_id: str, dag_download_metrics: EndWorkerMetrics):
        unique_id = uuid.uuid4().hex # required because there can be {N} DAG downloads for a single DAG instance
        async with self.lock:
            self.cached_metrics[f"{self.DAG_MD_KEY_PREFIX}{master_dag_id}{unique_id}"] = dag_download_metrics
    
    async def store_plan(self, master_dag_id: str, plan):
        async with self.lock:
            self.cached_metrics[f"{self.PLAN_KEY_PREFIX}{master_dag_id}"] = plan

    async def store_invoker_worker_startup_metrics(self, metrics: WorkerStartupMetrics, task_ids: list[str]):
        """ direct upload to storage so that the INVOKED can find it and complete the missing fields """
        task_ids_hash = hashlib.sha256(json.dumps(task_ids).encode('utf-8')).hexdigest()
        await self.storage.set(f"{self.WORKER_STARTUP_PREFIX}{metrics.master_dag_id}_{task_ids_hash}", cloudpickle.dumps(metrics))

    async def update_invoked_worker_startup_metrics(self, end_time_s: float, worker_state: Literal["warm", "cold"], was_prewarmed: bool, task_ids: list[str], master_dag_id: str):
        task_ids_hash = hashlib.sha256(json.dumps(task_ids).encode('utf-8')).hexdigest()
        storage_key = f"{self.WORKER_STARTUP_PREFIX}{master_dag_id}_{task_ids_hash}"
        wsm: WorkerStartupMetrics = cloudpickle.loads(await self.storage.get(storage_key))
        wsm.end_timestamp_s = end_time_s
        wsm.state = worker_state
        wsm.was_prewarmed = was_prewarmed
        async with self.lock: self.cached_metrics[storage_key] = wsm

    async def flush(self):
        start = time.time()
        async with self.lock:
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