import asyncio
import base64
from dataclasses import dataclass, field
from itertools import groupby
import json
import random
import time
import uuid
import aiohttp
import cloudpickle
import os
from typing import Any

from src.dag import dag
from src.planning.optimizations.prewarm import PreWarmOptimization
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.utils.logger import create_logger
from src.workers.worker import Worker
from src.utils.utils import calculate_data_structure_size_bytes, get_consistent_gateway_for_worker_id

logger = create_logger(__name__)

class DockerWorker(Worker):
    @dataclass
    class Config(Worker.Config):
        external_docker_gateway_addresses: list[tuple[str, int]] = field(default_factory=list)
        container_monitoring_addresses: list[tuple[str, int]] = field(default_factory=list)
      
        def create_instance(self) -> "DockerWorker": 
            super().create_instance()
            return DockerWorker(self)

    docker_config: Config

    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, config: Config):
        super().__init__(config)
        self.docker_config = config
        # self.ARTIFICIAL_NETWORK_LATENCY_S = 0.030 # ~15 ms each way (request, response)
        self.ARTIFICIAL_NETWORK_LATENCY_S = 0 # ~15 ms each way (request, response)
        self.MAX_DAG_SIZE_BYTES = 300 * 1024 # 300KB
        self.MAX_DAG_CACHED_RESULTS_BYTES = 250 * 1024 # 250KB
        # On linux, docker containers don't have access to host.docker.internal. They can just call localhost, on Windows they have to use host.docker.internal
        self.is_docker_host_linux = os.getenv("HOST_OS") == "linux"

    async def _simulate_network_latency(self) -> None:
        if self.ARTIFICIAL_NETWORK_LATENCY_S > 0:
            await asyncio.sleep(self.ARTIFICIAL_NETWORK_LATENCY_S)

    async def delegate(self, subdags: list[dag.SubDAG], fulldag: dag.FullDAG, called_by_worker: bool = True):
        from src.storage.metadata.metrics_types import WorkerStartupMetrics
        if not subdags: raise Exception("DockerWorker.delegate() received an empty list of subdags!")

        # pre-calculate pre-warmed workers
        prewarmed_worker_ids = set()
        for node in fulldag._all_nodes.values():
            prewarm_opt = node.try_get_optimization(PreWarmOptimization)
            if prewarm_opt:
                for _, config in prewarm_opt.target_resource_configs:
                    if config.worker_id:
                        prewarmed_worker_ids.add(config.worker_id)

        relevant_cached_results: dict[str, bytes] = {} 
        aggregated_results_size_bytes = 0

        for subdag in subdags:
            for utask in subdag.root_node.upstream_nodes:
                if utask.cached_result is None: continue
                internal_id = utask.id.get_internal_id()
                if internal_id in relevant_cached_results: continue
                res_obj = utask.cached_result.result
                serialized_result = cloudpickle.dumps(res_obj)
                res_size = calculate_data_structure_size_bytes(serialized_result)
                if aggregated_results_size_bytes + res_size < self.MAX_DAG_CACHED_RESULTS_BYTES:
                    aggregated_results_size_bytes += res_size
                    relevant_cached_results[internal_id] = serialized_result

        tasks_with_worker_id_by_gateway: dict[tuple[str, int], dict[str, list[dag.SubDAG]]] = {}
        tasks_without_worker_id: list[dag.SubDAG] = []
        for subdag in subdags:
            worker_id = subdag.root_node.worker_config.worker_id
            if worker_id is None:
                tasks_without_worker_id.append(subdag)
            else:
                if worker_id in prewarmed_worker_ids:
                    # Consistent routing logic
                    gateway = get_consistent_gateway_for_worker_id(worker_id, self.docker_config.external_docker_gateway_addresses)
                else:
                    gateway = ("localhost", 5000) if called_by_worker else random.choice(self.docker_config.external_docker_gateway_addresses)
                tasks_with_worker_id_by_gateway.setdefault(gateway, {}).setdefault(worker_id, []).append(subdag)

        fulldag_size_below_threshold = False
        if self.docker_config.optimized_dag:
            fulldag_size = calculate_data_structure_size_bytes(self.docker_config.optimized_dag)
            fulldag_size_below_threshold = fulldag_size < self.MAX_DAG_SIZE_BYTES

        async def make_worker_request(session: aiohttp.ClientSession, gateway_address: tuple[str, int], worker_id: str | None, worker_subdags: list[dag.SubDAG]):
            target_config = worker_subdags[0].root_node.worker_config
            root_task_ids = [sd.root_node.id.get_internal_id() for sd in worker_subdags]
            
            # Metric Logging
            await self.metadata_storage.store_invoker_worker_startup_metrics(
                WorkerStartupMetrics(
                    master_dag_id=worker_subdags[0].master_dag_id,
                    start_time_ms=time.time() * 1000,
                    resource_configuration=target_config,
                    initial_task_ids=root_task_ids
                ),
                task_ids=root_task_ids
            )

            payload = {
                "resource_configuration": base64.b64encode(cloudpickle.dumps(target_config)).decode('utf-8'),
                "dag_id": worker_subdags[0].master_dag_id,
                "fulldag": self.docker_config.optimized_dag if self.docker_config.optimized_dag and fulldag_size_below_threshold else None,
                "task_ids": base64.b64encode(cloudpickle.dumps([sd.root_node.id for sd in worker_subdags])).decode('utf-8'),
                "relevant_cached_results": base64.b64encode(cloudpickle.dumps(relevant_cached_results)).decode('utf-8'), # Already dict of b64 strings
                "config": base64.b64encode(cloudpickle.dumps(self.docker_config)).decode('utf-8'),
            }

            url = f"http://{gateway_address[0]}:{gateway_address[1]}/job"
            async with session.post(url, json=payload) as response:
                if response.status != 202:
                    text = await response.text()
                    raise Exception(f"Worker {worker_id} at {url} failed with {response.status}: {text}")
                return response.status

        # Execute requests
        async with aiohttp.ClientSession() as session:
            http_tasks = []
            
            for gateway, worker_map in tasks_with_worker_id_by_gateway.items():
                for worker_id, s_dags in worker_map.items():
                    http_tasks.append(make_worker_request(session, gateway, worker_id, s_dags))
            
            for subdag in tasks_without_worker_id:
                http_tasks.append(make_worker_request(session, ("localhost", 5000), None, [subdag]))
            
            await asyncio.gather(*http_tasks)

    async def warmup(self, dag_id: str, resource_configurations: list[TaskWorkerResourceConfiguration]):
        # await self._simulate_network_latency()

        # 1. Grouping by gateway (logic remains the same)
        gateway_to_rc_map: dict[tuple[str, int], list[TaskWorkerResourceConfiguration]] = {}
        for rc in resource_configurations:
            assigned_gateway = get_consistent_gateway_for_worker_id(rc.worker_id or str(uuid.uuid4()), self.docker_config.external_docker_gateway_addresses)
            gateway_to_rc_map.setdefault(assigned_gateway, []).append(rc)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for gateway_address, rcs in gateway_to_rc_map.items():
                tasks.append(self._send_warmup_request(
                    session, 
                    f"http://{gateway_address[0]}:{gateway_address[1]}/warmup",
                    {
                        "dag_id": dag_id,
                        "resource_configurations": base64.b64encode(cloudpickle.dumps(rcs)).decode('utf-8')
                    }
                ))

            await asyncio.gather(*tasks)

    async def _send_warmup_request(self, session: aiohttp.ClientSession, url: str, payload: dict):
        try:
            async with session.post(url, json=payload) as response:
                if response.status != 202:
                    text = await response.text()
                    logger.error(f"Warmup to {url} failed ({response.status}): {text}")
                else:
                    logger.info(f"Warmup to {url} successful")
        except Exception as e:
            logger.error(f"Network error during warmup to {url}: {e}")
