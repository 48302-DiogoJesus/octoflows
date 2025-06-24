import hashlib
import json
from typing import Literal
import uuid
import asyncio
from dataclasses import dataclass
import time

import cloudpickle
from src.storage.metrics.metrics_types import UserDAGSubmissionMetrics, FullDAGPrepareTime, TaskMetrics, WorkerStartupMetrics
from src.storage.storage import Storage
from src.utils.logger import create_logger

logger = create_logger(__name__)

class MetricsStorage():
    TASK_METRICS_KEY_PREFIX = "metrics-storage-tasks-"
    DAG_METRICS_KEY_PREFIX = "metrics-storage-dag-"
    PLAN_KEY_PREFIX = "metrics-storage-plan-"
    WORKER_STARTUP_PREFIX = "metrics-storage-worker-startup-"
    USER_DAG_SUBMISSION_PREFIX = "metrics-storage-user-dag-submission-"

    @dataclass
    class Config:
        storage_config: Storage.Config

        def create_instance(self) -> "MetricsStorage":
            return MetricsStorage(self.storage_config)

    def __init__(self, storage_config: Storage.Config) -> None:
        from src.planning.abstract_dag_planner import AbstractDAGPlanner
        self.storage = storage_config.create_instance()
        self.cached_metrics: dict[str, TaskMetrics | FullDAGPrepareTime | AbstractDAGPlanner.PlanOutput | WorkerStartupMetrics | UserDAGSubmissionMetrics] = {}

    async def keys(self, pattern: str) -> list:
        return await self.storage.keys(pattern)

    async def get(self, key: str) -> TaskMetrics | FullDAGPrepareTime | None:
        return cloudpickle.loads(await self.storage.get(key))
    
    async def mget(self, keys: list[str]) -> list[TaskMetrics | FullDAGPrepareTime]:
        return [cloudpickle.loads(m) for m in await self.storage.mget(keys)]

    def store_dag_submission_time(self, master_dag_id: str, user_dag_submission_metrics: UserDAGSubmissionMetrics):
        self.cached_metrics[f"{self.USER_DAG_SUBMISSION_PREFIX}{master_dag_id}"] = user_dag_submission_metrics

    def store_task_metrics(self, task_id: str, metrics: TaskMetrics):
        self.cached_metrics[f"{self.TASK_METRICS_KEY_PREFIX}{task_id}"] = metrics

    def store_dag_download_time(self, master_dag_id: str, dag_download_metrics: FullDAGPrepareTime):
        unique_id = uuid.uuid4().hex # required because there can be {N} DAG downloads for a single DAG instance
        self.cached_metrics[f"{self.DAG_METRICS_KEY_PREFIX}{master_dag_id}{unique_id}"] = dag_download_metrics
    
    def store_plan(self, id: str, plan):
        self.cached_metrics[f"{self.PLAN_KEY_PREFIX}{id}"] = plan

    async def store_invoker_worker_startup_metrics(self, metrics: WorkerStartupMetrics, task_ids: list[str]):
        """ direct upload to storage so that the INVOKED can find it and complete the missing fields """
        task_ids_hash = hashlib.sha256(json.dumps(task_ids).encode('utf-8')).hexdigest()
        await self.storage.set(f"{self.WORKER_STARTUP_PREFIX}{metrics.master_dag_id}_{task_ids_hash}", cloudpickle.dumps(metrics))

    async def update_invoked_worker_startup_metrics(self, end_time_ms: float, worker_state: Literal["warm", "cold"], task_ids: list[str], master_dag_id: str):
        task_ids_hash = hashlib.sha256(json.dumps(task_ids).encode('utf-8')).hexdigest()
        wsm: WorkerStartupMetrics = cloudpickle.loads(await self.storage.get(f"{self.WORKER_STARTUP_PREFIX}{master_dag_id}_{task_ids_hash}"))
        wsm.end_time_ms = end_time_ms
        wsm.state = worker_state
        self.cached_metrics[f"{self.WORKER_STARTUP_PREFIX}{master_dag_id}_{task_ids_hash}"] = wsm

    async def flush(self):
        start = time.time()
        len_before_flush = len(self.cached_metrics)
        if len_before_flush == 0: return

        keys_to_remove = []
        async with self.storage.batch() as batch:
            for key, metrics in self.cached_metrics.items():
                await batch.set(key, cloudpickle.dumps(metrics))
                # remove from self.cached_metrics
                keys_to_remove.append(key)
            await batch.execute()

        for key in keys_to_remove: self.cached_metrics.pop(key, None)
        
        end = time.time()
        logger.info(f"Flushed {len_before_flush} metrics to storage in {end - start:.4f} seconds")


BASELINE_MEMORY_MB = 512 # reference value for normalization