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
        if len(subdags) == 0: raise Exception("DockerWorker.delegate() received an empty list of subdags to delegate!")
        
        subdags.sort(key=lambda sd: sd.root_node.worker_config.worker_id or "", reverse=True)
        
        relevant_cached_results: dict[str, Any] = {}
        aggregated_results_size_bytes = 0
        for subdag in subdags:
            rn = subdag.root_node
            # Go through all tasks and add as much results as possible without exceeding {MAX_DAG_CACHED_RESULTS_BYTES}
            for utask in rn.upstream_nodes:
                if utask.cached_result is None: continue
                serialized_result = cloudpickle.dumps(utask.cached_result.result)
                utask_result_size = calculate_data_structure_size_bytes(serialized_result)
                if aggregated_results_size_bytes + utask_result_size < self.MAX_DAG_CACHED_RESULTS_BYTES:
                    aggregated_results_size_bytes += utask_result_size
                    relevant_cached_results[utask.id.get_internal_id()] = serialized_result
                
        # Separate tasks with None worker_id from those with specific worker_ids
        tasks_with_worker_id_by_gateway: dict[tuple[str, int], dict[str, list[dag.SubDAG]]] = {}
        tasks_without_worker_id: list[dag.SubDAG] = []
        
        def _is_worker_id_prewarmed(worker_id: str):
            for node in fulldag._all_nodes.values():
                prewarm_opt = node.try_get_optimization(PreWarmOptimization)
                if not prewarm_opt: continue
                if any([trc[1].worker_id == worker_id for trc in prewarm_opt.target_resource_configs]): return True
            return False

        for subdag in subdags:
            worker_id = subdag.root_node.worker_config.worker_id
            if worker_id is None:
                tasks_without_worker_id.append(subdag)
            else:
                if not called_by_worker:
                    assigned_gateway = get_consistent_gateway_for_worker_id(worker_id, self.docker_config.external_docker_gateway_addresses)
                else:
                    # Is worker was prewarmed, route it to the same gateway as the prewarm request, else route to local
                    assigned_gateway = ("localhost", 5000) if not _is_worker_id_prewarmed(worker_id) \
                        else get_consistent_gateway_for_worker_id(worker_id, self.docker_config.external_docker_gateway_addresses)
                tasks_with_worker_id_by_gateway.setdefault(assigned_gateway, {}).setdefault(worker_id, []).append(subdag)
        
        http_tasks = []
        # An individual request will result in a new worker/container, so 1 request per worker
        async def make_worker_request(session: aiohttp.ClientSession, gateway_address: tuple[str, int], worker_id: str | None, worker_subdags: list[dag.SubDAG]):
            _worker_subdags: list[dag.SubDAG] = worker_subdags
            targetWorkerResourcesConfig = _worker_subdags[0].root_node.worker_config

            logger.info(f"Invoking docker gateway ({gateway_address[0]}:{gateway_address[1]}) | CPUs: {targetWorkerResourcesConfig.cpus} | Memory: {targetWorkerResourcesConfig.memory_mb} | Worker ID: {worker_id} | Root Tasks: {[subdag.root_node.id.get_internal_id() for subdag in _worker_subdags]}")
            await self.metadata_storage.store_invoker_worker_startup_metrics(
                WorkerStartupMetrics(
                    master_dag_id=_worker_subdags[0].master_dag_id,
                    start_time_ms=time.time() * 1000,
                    resource_configuration=targetWorkerResourcesConfig,
                    state=None,
                    end_time_ms=None,
                    initial_task_ids=[subdag.root_node.id.get_internal_id() for subdag in _worker_subdags]
                ),
                task_ids=[subdag.root_node.id.get_internal_id() for subdag in _worker_subdags]
            )

            fulldag_size_below_threshold = False
            if self.docker_config.optimized_dag:
                fulldag_size = calculate_data_structure_size_bytes(self.docker_config.optimized_dag)
                fulldag_size_below_threshold = fulldag_size < self.MAX_DAG_SIZE_BYTES
            
            http_body_data = {
                "resource_configuration": targetWorkerResourcesConfig,
                "dag_id": _worker_subdags[0].master_dag_id,
                # if dag size is below 200KB, send the dag in the invocation, else, send the ID and the worker has to fetch it from storage
                "fulldag": self.docker_config.optimized_dag if self.docker_config.optimized_dag and fulldag_size_below_threshold else None,
                # "fulldag": None,
                "task_ids": base64.b64encode(cloudpickle.dumps([subdag.root_node.id for subdag in _worker_subdags])).decode('utf-8'),
                "relevant_cached_results": base64.b64encode(cloudpickle.dumps(relevant_cached_results)).decode('utf-8'),
                "config": base64.b64encode(cloudpickle.dumps(self.docker_config)).decode('utf-8'),
            }
            http_body_data_serialized = cloudpickle.dumps(http_body_data)

            async with await session.post(
                f"http://{gateway_address[0]}:{gateway_address[1]}" + "/job",
                data=http_body_data_serialized,
                headers={'Content-Type': 'application/octet-stream'}
            ) as response:
                if response.status != 202:
                    text = await response.text()
                    raise Exception(f"Failed to invoke worker: {text}")
                return response.status
        
        # await self._simulate_network_latency()

        async with aiohttp.ClientSession() as session:
            for gateway, worker_id_to_dags in tasks_with_worker_id_by_gateway.items():
                for worker_id, subdags in worker_id_to_dags.items():
                    http_tasks.append(make_worker_request(session, gateway, worker_id, subdags))
            
            # Create individual tasks for each subdag with worker_id = None
            for subdag in tasks_without_worker_id:
                assigned_gateway = random.choice(self.docker_config.external_docker_gateway_addresses) if not called_by_worker else ("localhost", 5000)
                http_tasks.append(make_worker_request(session, assigned_gateway, None, [subdag]))
            
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
                    cloudpickle.dumps({
                        "dag_id": dag_id,
                        "resource_configurations": rcs
                    })
                ))

            await asyncio.gather(*tasks)

    async def _send_warmup_request(self, session: aiohttp.ClientSession, url: str, payload: bytes):
        try:
            async with session.post(url, data=payload, headers={'Content-Type': 'application/octet-stream'}) as response:
                if response.status != 202:
                    text = await response.text()
                    logger.error(f"Warmup to {url} failed ({response.status}): {text}")
                else:
                    logger.info(f"Warmup to {url} successful")
        except Exception as e:
            logger.error(f"Network error during warmup to {url}: {e}")
