from collections.abc import Iterable
import asyncio
import base64
from dataclasses import dataclass
import json
import time
from typing import Any
import uuid
import cloudpickle
import aiohttp
from abc import ABC, abstractmethod

from src.resource_configuration import ResourceConfiguration
from src.utils.logger import create_logger
import src.dag as dag
import src.dag_task_node as dag_task_node
import src.storage.storage as storage_module

logger = create_logger(__name__)

class Worker(ABC):
    @dataclass
    class Config(ABC):
        intermediate_storage_config: storage_module.Storage.Config
        metadata_storage_config: storage_module.Storage.Config | None = None
        
        @abstractmethod
        def create_instance(self) -> "Worker": pass

    intermediate_storage: storage_module.Storage
    worker_id: str

    def __init__(self, config: Config):
        self.worker_id = str(uuid.uuid4())
        self.intermediate_storage = config.intermediate_storage_config.create_instance()
        self.metadata_storage = self.intermediate_storage if not config.metadata_storage_config else config.metadata_storage_config.create_instance()

    async def start_executing(self, subdag: dag.DAG):
        if not subdag.root_node: raise Exception(f"AbstractWorker expected a subdag with only 1 root node. Got {len(subdag.root_node)}")
        task = subdag.root_node

        try:
            while True:
                # 1. DOWNLOAD DEPENDENCIES
                self.log(task.id.get_full_id_in_dag(subdag), f"1) Grabbing Dependencies | Dynamic Task: {task.fan_out_idx != -1}...")
                task_dependencies: dict[str, Any] = {}
                # Dynamic fan-outs
                if task.fan_out_idx != -1:
                    if len(task.upstream_nodes) != 1: raise Exception(f"task: {task.id.get_full_id_in_dag(subdag)} Dynamic fan-out tasks can only have 1 upstream task. Got {len(task.upstream_nodes)}")
                    dependency_task = task.upstream_nodes[0]
                    logger.info(f"DYNAMIC FAN-OUT: Grabbing {dependency_task.id.get_full_id_in_dag(subdag)}.output[{task.fan_out_idx}]")
                    task_output = self.intermediate_storage.get(dependency_task.id.get_full_id_in_dag(subdag))
                    if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
                    task_dependencies[dependency_task.id.get_full_id()] = cloudpickle.loads(task_output)[task.fan_out_idx]
                else:
                    logger.info(f"STATIC FAN-OUT: Grabbing {len(task.upstream_nodes)} upstream tasks")
                    for dependency_task in task.upstream_nodes:
                        if dependency_task.is_dynamic_fan_out_representative:
                            fanout_ids = self.metadata_storage.get(f"dynamic-fanout-ids-{dependency_task.id.get_full_id_in_dag(subdag)}")
                            if fanout_ids is None: raise Exception(f"[BUG] Dynamic fan-out ids for {dependency_task.id.get_full_id_in_dag(subdag)}'s are not available")
                            fanout_ids: list[str] = cloudpickle.loads(fanout_ids) # type: ignore
                            # Dynamic Fan-outs: "reduce" phase
                            aggregated_outputs = []
                            for fanout_id in fanout_ids:
                                task_output = self.intermediate_storage.get(fanout_id)
                                if task_output is None: raise Exception(f"[BUG] Dynamic fan-out task {fanout_id}'s data is not available")
                                aggregated_outputs.append(cloudpickle.loads(task_output))
                            task_dependencies[dependency_task.id.get_full_id()] = aggregated_outputs
                        else:
                            task_output = self.intermediate_storage.get(dependency_task.id.get_full_id_in_dag(subdag))
                            if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
                            task_dependencies[dependency_task.id.get_full_id()] = cloudpickle.loads(task_output)
                
                # 2. EXECUTE TASK
                self.log(task.id.get_full_id_in_dag(subdag), f"2) Executing...")
                task_result = task.invoke(dependencies=task_dependencies)
                self.log(task.id.get_full_id_in_dag(subdag), f"3) Done! Writing output to storage...")
                self.intermediate_storage.set(task.id.get_full_id_in_dag(subdag), cloudpickle.dumps(task_result))

                if len(task.downstream_nodes) == 0: 
                    self.log(task.id.get_full_id_in_dag(subdag), f"Last Task finished. Shutting down worker...")
                    break

                # 3. HANDLE FAN-OUT (1-1 or 1-N)
                self.log(task.id.get_full_id_in_dag(subdag), f"4) Handle Fan-Out {task.id.get_full_id_in_dag(subdag)} => {[t.id.get_full_id_in_dag(subdag) for t in task.downstream_nodes]}")
                ready_downstream: list[dag_task_node.DAGTaskNode] = []
                
                for downstream_task in task.downstream_nodes:
                    dc_key = f"dependency-counter-{downstream_task.id.get_full_id_in_dag(subdag)}"
                    dependencies_met = self.metadata_storage.atomic_increment_and_get(dc_key)

                    downstream_task_total_dependencies = 0
                    for unode in subdag.get_node_by_id(downstream_task.id).upstream_nodes:
                        if unode.is_dynamic_fan_out_representative:
                            fan_out_task_ids = self.metadata_storage.get(f"dynamic-fanout-ids-{unode.id.get_full_id_in_dag(subdag)}")
                            if fan_out_task_ids is None: break # A fan-out that this DT depends on was not created yet. {downstream_task} is def. not ready
                            fan_out_task_ids = cloudpickle.loads(fan_out_task_ids)
                            downstream_task_total_dependencies += len(fan_out_task_ids)
                        else:
                            downstream_task_total_dependencies += 1

                    self.log(task.id.get_full_id_in_dag(subdag), f"Incremented DC of {dc_key} ({dependencies_met}/{downstream_task_total_dependencies})")
                    if dependencies_met == downstream_task_total_dependencies:
                        if not downstream_task.is_dynamic_fan_out_representative:
                            ready_downstream.append(downstream_task)
                        else:
                            if not isinstance(task_result, Iterable):
                                raise Exception(f"Task {task.id.get_full_id_in_dag(subdag)} returned a non-iterable result but one of its downstream tasks ({downstream_task.id.get_full_id_in_dag(subdag)}) expected an iterable.")
                            dynamic_fanout_size = len(task_result) # type: ignore
                            if dynamic_fanout_size == 0: 
                                raise Exception(f"Task {task.id.get_full_id_in_dag(subdag)} returned an empty iterable but one of its downstream tasks ({downstream_task.id.get_full_id_in_dag(subdag)}) expected a non-empty iterable.")
                            logger.info(f"Splitting {downstream_task.id.get_full_id_in_dag(subdag)} into {dynamic_fanout_size} tasks")
                            fanout_task_ids = [] # use list to keep the order
                            # Dynamic Fan-outs: "map" phase
                            for fo_idx in range(dynamic_fanout_size):
                                node = dag_task_node.DAGTaskNode(
                                        downstream_task.func_code, downstream_task.func_args, downstream_task.func_kwargs, 
                                        # task_id=f"{downstream_task.id.task_id}-{fo_idx}",
                                        dynamic_fan_out_representative_id=downstream_task.id,
                                        fan_out_idx=fo_idx,
                                        fan_out_size=dynamic_fanout_size
                                    )
                                fanout_task_ids.append(node.id.get_full_id_in_dag(subdag))
                                node.upstream_nodes = downstream_task.upstream_nodes
                                node.downstream_nodes = downstream_task.downstream_nodes
                                ready_downstream.append(node)
                            self.metadata_storage.set(f"dynamic-fanout-ids-{downstream_task.id.get_full_id_in_dag(subdag)}", cloudpickle.dumps(fanout_task_ids))

                # Delegate Downstream Tasks Execution
                ## 1 Task ?: the same worker continues with it
                if len(ready_downstream) == 0:
                    self.log(task.id.get_full_id_in_dag(subdag), f"No ready downstream tasks found. Shutting down worker...")
                    break  # Give up

                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                continuation_task = ready_downstream[0] # choose the first task
                tasks_to_delegate = ready_downstream[1:]
                coroutines = []
                
                for t in tasks_to_delegate:
                    self.log(task.id.get_full_id_in_dag(subdag), f"Delegating downstream task: {t}")
                    coroutines.append(self.delegate(
                        subdag.create_subdag(t), 
                        resource_configuration=ResourceConfiguration.medium(), 
                        called_by_worker=True
                    ))
                
                await asyncio.gather(*coroutines) # wait for the delegations to be accepted

                # Continue with one task in this worker
                self.log(task.id.get_full_id_in_dag(subdag), f"Continuing with first of multiple downstream tasks: {continuation_task}")
                task = continuation_task # type: ignore downstream_task_id)
        except Exception as e:
            self.log(task.id.get_full_id_in_dag(subdag), f"Error: {str(e)}") # type: ignore
            raise e

        # Cleanup
        self.log(task.id.get_full_id_in_dag(subdag), f"Worker shut down!")

    @abstractmethod
    async def delegate(self, subdag: dag.DAG, resource_configuration: ResourceConfiguration, called_by_worker: bool = False): 
        """
        {called_by_worker}: indicates if it's a worker invoking another worker, or the Client beggining the execution
        """
        pass

    @staticmethod
    def store_full_dag(metadata_storage: storage_module.Storage, dag: dag.DAG):
        metadata_storage.set(f"dag-{dag.master_dag_id}", cloudpickle.dumps(dag))

    def get_full_dag(self, dag_id: str) -> dag.DAG:
        dag = self.metadata_storage.get(f"dag-{dag_id}")
        if dag is None: raise Exception(f"Could not find DAG with id {dag_id}")
        return cloudpickle.loads(dag)

    @staticmethod
    async def wait_for_result_of_task(intermediate_storage: storage_module.Storage, task: dag_task_node.DAGTaskNode, dag: dag.DAG, polling_interval_s: float = 1.0):
        start_time = time.time()
        # Poll Storage for final result. Asynchronous wait
        task_id = task.id.get_full_id_in_dag(dag)
        is_task_dynamic_fan_out = task.is_dynamic_fan_out_representative
        first_iter = True
        while True:
            if not first_iter: await asyncio.sleep(polling_interval_s)
            first_iter = False

            if is_task_dynamic_fan_out:
                fanout_task_ids = intermediate_storage.get(f"dynamic-fanout-ids-{task_id}")
                if fanout_task_ids is None: continue
                final_result_ids = cloudpickle.loads(fanout_task_ids)
                fanout_tasks_ready = intermediate_storage.exists(*final_result_ids)
                if fanout_tasks_ready != len(final_result_ids): continue # not all tasks are ready
                aggregated_result = []
                for final_result_id in final_result_ids:
                    final_result = intermediate_storage.get(final_result_id)
                    aggregated_result.append(cloudpickle.loads(final_result)) # type: ignore
                end_time = time.time()
                logger.info(f"Dynamic Fan-Out Final Result Ready: ({task_id}) => Size: {len(aggregated_result)} | Type: ({type(aggregated_result)}) | Time: {end_time - start_time}s")
                return aggregated_result
            else:
                final_result = intermediate_storage.get(task_id)
                if final_result is not None:
                    final_result = cloudpickle.loads(final_result) # type: ignore
                    end_time = time.time()
                    logger.info(f"Final Result Ready: ({task_id}) => Size: {len(str(final_result))} | Type: ({type(final_result)}) | Time: {end_time - start_time}s")
                    return final_result

    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        logger.info(f"Worker({self.worker_id}) Task({task_id}) | {message}")

class LocalWorker(Worker):
    @dataclass
    class Config(Worker.Config):
        def create_instance(self) -> "LocalWorker":
            return LocalWorker(self)

    local_config: Config

    """
    Processes DAG tasks
    continuing with single downstream tasks and spawning new workers (coroutines) for branches.
    """
    def __init__(self, config: Config):
       super().__init__(config)
       self.local_config = config
    
    async def delegate(self, subdag: dag.DAG, resource_configuration: ResourceConfiguration = ResourceConfiguration.small(), called_by_worker: bool = True):
        await asyncio.create_task(self.start_executing(subdag))

class DockerWorker(Worker):
    @dataclass
    class Config(Worker.Config):
        docker_gateway_address: str = "http://localhost:5000"
        
        def create_instance(self) -> "DockerWorker":
            return DockerWorker(self)

    docker_config: Config

    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, config: Config):
        super().__init__(config)
        self.docker_config = config

    async def delegate(self, subdag: dag.DAG, resource_configuration: ResourceConfiguration = ResourceConfiguration.small(), called_by_worker: bool = True):
        '''
        Each invocation is done inside a new Coroutine without blocking the owner Thread
        '''
        gateway_address = "http://host.docker.internal:5000" if called_by_worker else self.docker_config.docker_gateway_address
        self.log(subdag.root_node.id.get_full_id_in_dag(subdag), f"Invoking docker gateway ({gateway_address})")
        async with aiohttp.ClientSession() as session:
            async with await session.post(
                gateway_address + "/job", 
                data=json.dumps({
                    "resource_configuration": base64.b64encode(cloudpickle.dumps(resource_configuration)).decode('utf-8'),
                    "dag_id": subdag.master_dag_id,
                    "task_id": base64.b64encode(cloudpickle.dumps(subdag.root_node.id)).decode('utf-8'),
                    "config": base64.b64encode(cloudpickle.dumps(self.docker_config)).decode('utf-8'),
                }),
                headers={'Content-Type': 'application/json'}
            ) as response:
                if response.status != 202:
                    text = await response.text()
                    raise Exception(f"Failed to invoke worker: {text}")