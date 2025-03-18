import asyncio
import base64
from dataclasses import dataclass
import json
from types import CoroutineType
from typing import Any
import uuid
import cloudpickle
import aiohttp
from abc import ABC, abstractmethod

import src.dag as dag
import src.dag_task_node as dag_task_node
import src.storage.storage as intermediate_storage_module

class Worker(ABC):
    @dataclass
    class Config(ABC):
        intermediate_storage_config: intermediate_storage_module.Storage.Config
        
        @abstractmethod
        def create_instance(self) -> "Worker": pass

    intermediate_storage: intermediate_storage_module.Storage
    worker_id: str

    def __init__(self, config: Config):
        self.worker_id = str(uuid.uuid4())[:4]
        self.intermediate_storage = config.intermediate_storage_config.create_instance()

    async def start_executing(self, subdag: dag.DAG):
        if not subdag.root_node: raise Exception(f"AbstractWorker expected a subdag with only 1 root node. Got {len(subdag.root_nodes)}")
        task = subdag.root_node

        try:
            while True:
                # 1. DOWNLOAD DEPENDENCIES
                self.log(task.id.get_full_id_in_dag(subdag), f"1) Grabbing Dependencies...")
                task_dependencies: dict[str, Any] = {}
                for dependency_task in task.upstream_nodes:
                    task_output = self.intermediate_storage.get(subdag.get_dag_task_id(dependency_task))
                    if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
                    task_dependencies[dependency_task.id.get_full_id()] = cloudpickle.loads(task_output) # type: ignore
                
                # 2. EXECUTE TASK
                self.log(task.id.get_full_id_in_dag(subdag), f"2) Executing...")
                task_result = task.invoke(dependencies=task_dependencies)
                self.intermediate_storage.set(subdag.get_dag_task_id(task), cloudpickle.dumps(task_result))

                if len(task.downstream_nodes) == 0: 
                    self.log(task.id.get_full_id_in_dag(subdag), f"Last Task finished. Shutting down worker...")
                    break

                # 3. HANDLE FAN-OUT (1-1 or 1-N)
                self.log(task.id.get_full_id_in_dag(subdag), f"3) Handle Fan-Out {task.id.get_full_id_in_dag(subdag)} => [{[t.id.get_full_id_in_dag(subdag) for t in task.downstream_nodes]}]")
                ready_downstream: list[dag_task_node.DAGTaskNode] = []
                for downstream_task in task.downstream_nodes:
                    downstream_task_total_dependencies = len(subdag.get_node_by_id(downstream_task.id).upstream_nodes)
                    if downstream_task_total_dependencies == 1: # {task} was the only dependency
                        ready_downstream.append(downstream_task)
                    else:
                        dc_key = f"dependency-counter-{downstream_task.id.get_full_id_in_dag(subdag)}"
                        dependencies_met = self.intermediate_storage.atomic_increment_and_get(dc_key)
                        self.log(task.id.get_full_id_in_dag(subdag), f"Incremented DC of {dc_key} ({dependencies_met}/{downstream_task_total_dependencies})")
                        if dependencies_met == downstream_task_total_dependencies:
                            ready_downstream.append(downstream_task)
                
                # Delegate Downstream Tasks Execution
                ## 1 Task ?: the same worker continues with it
                if len(ready_downstream) == 1:
                    task = subdag.get_node_by_id(ready_downstream[0].id) # type: ignore downstream_task_id)
                    continue
                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                elif len(ready_downstream) > 1:
                    continuation_task = ready_downstream[0] # choose the first task
                    tasks_to_delegate = ready_downstream[1:]
                    
                    for task in tasks_to_delegate:
                        asyncio.create_task(self.delegate(subdag.create_subdag(task)))
                    
                    # Continue with one task in this worker
                    self.log(task.id.get_full_id_in_dag(subdag), f"Continuing with first of multiple downstream tasks: {continuation_task}")
                    task = subdag.get_node_by_id(ready_downstream[0].id) # type: ignore downstream_task_id)
                    continue
                else:
                    self.log(task.id.get_full_id_in_dag(subdag), f"No ready downstream tasks found. Shutting down worker...")
                    break  # Give up
        except Exception as e:
            self.log(task.id.get_full_id_in_dag(subdag), f"Error: {str(e)}") # type: ignore
            raise e

        # Cleanup
        self.log(task.id.get_full_id_in_dag(subdag), f"Worker shut down!")

    @abstractmethod
    async def delegate(self, subdag: dag.DAG): pass

    @staticmethod
    async def wait_for_result_of_task(intermediate_storage: intermediate_storage_module.Storage, task_id: str, polling_interval_s: float = 1.0):
        # Poll Storage for final result. Asynchronous wait
        while True:
            final_result = intermediate_storage.get(task_id)
            if final_result is not None:
                final_result = cloudpickle.loads(final_result) # type: ignore
                print(f"Final Result Ready: ({task_id}) => {final_result} | Type: ({type(final_result)})")
                return final_result
            await asyncio.sleep(polling_interval_s)

    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        print(f"Worker({self.worker_id}) Task({task_id}) | {message}", flush=True)

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
    
    async def delegate(self, subdag: dag.DAG):
        await self.start_executing(subdag)

class DockerWorker(Worker):
    @dataclass
    class Config(Worker.Config):
        docker_gateway_address: str
        
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

    async def delegate(self, subdag: dag.DAG):
        '''
        Each invocation is done inside a new Coroutine without blocking the owner Thread
        '''
        self.log(subdag.root_node.id.get_full_id_in_dag(subdag), f"Invoking docker gateway for subsubdag starting at: {subdag.root_node}")
        async with aiohttp.ClientSession() as session:
            print(f"Invoking docker gateway for subsubdag starting at: {subdag.root_node}")
            async with await session.post(
                self.docker_config.docker_gateway_address + "/job", 
                data=json.dumps({
                    "resource_configuration": {
                        "cpus": 1,
                        "memory": 128,
                    },
                    "subdag": base64.b64encode(cloudpickle.dumps(subdag)).decode('utf-8'),
                    "config": base64.b64encode(cloudpickle.dumps(self.docker_config)).decode('utf-8'),
                }),
                headers={'Content-Type': 'application/json'}
            ) as response:
                if response.status != 202:
                    text = await response.text()
                    raise Exception(f"Failed to invoke worker: {text}")