import asyncio
import threading
from typing import Any
import uuid
import cloudpickle
import requests
import aiohttp
from abc import ABC, abstractmethod

import src.dag as dag
import src.intermediate_storage as intermediate_storage
import src.dag_task_node as dag_task_node

# TODO: ?
class Worker(ABC):
    pass

class LocalCoroutineWorker(Worker):
    """
    Processes DAG tasks
    continuing with single downstream tasks and spawning new workers (coroutines) for branches.
    """
    def __init__(self, subdag: dag.DAG):
        self.shutdown_flag = asyncio.Event()
        # Tracks all Workers created by this worker and those workers, recursivelly
        self.subdag = subdag
        self.worker_id = str(uuid.uuid4())[:3]  # Use first 8 chars of a UUID for readable IDs
        if not subdag.root_node: raise Exception(f"LocalCoroutineWorker expected a subdag with only 1 root node. Got {len(subdag.root_nodes)}")
    
    def start_executing(self):
        asyncio.create_task(self._start_executing())

    async def _start_executing(self):
        task = self.subdag.root_node

        try:
            while not self.shutdown_flag.is_set():
                # Should be none on the first call. Not None if the same Virtual Worker executes multiple tasks
                if self.shutdown_flag.is_set(): return

                # 1. DOWNLOAD DEPENDENCIES
                self.log(task.task_id, f"({task.task_id}) 1) Grabbing Dependencies...")
                task_dependencies: dict[str, Any] = {}
                for dependency_task in task.upstream_nodes:
                    task_output = intermediate_storage.IntermediateStorage.get(dependency_task.task_id)
                    if task_output is None: raise Exception(f"[BUG] Task {dependency_task.task_id}'s data is not available")
                    task_dependencies[dependency_task.task_id] = cloudpickle.loads(task_output) # type: ignore
                
                # 2. EXECUTE TASK
                self.log(task.task_id, f"({task.task_id}) 2) Executing...")
                task_result = task.invoke(dependencies=task_dependencies)
                upload_result = intermediate_storage.IntermediateStorage.set(task.task_id, cloudpickle.dumps(task_result))
                if not upload_result: raise Exception(f"[BUG] Task {task.task_id}'s data could not be uploaded to Redis (set() failed)")

                if self.shutdown_flag.is_set(): return

                if len(task.downstream_nodes) == 0: 
                    self.log(task.task_id, f"Last Task finished. Shutting down executor...")
                    self.shutdown_flag.set()
                    return

                # 3. HANDLE FAN-OUT (1-1 or 1-N)
                self.log(task.task_id, f"({task.task_id}) 3) Handle Fan-Out {task.task_id} => [{[t.task_id for t in task.downstream_nodes]}]")
                ready_downstream: list[dag_task_node.DAGTaskNode] = []
                for downstream_task in task.downstream_nodes:
                    downstream_task_total_dependencies = len(self.subdag.get_node_by_id(downstream_task.task_id).upstream_nodes)
                    if downstream_task_total_dependencies == 1: # {task} was the only dependency
                        ready_downstream.append(downstream_task)
                    else:
                        dependencies_met = intermediate_storage.IntermediateStorage.increment_and_get(f"dependency-counter-{downstream_task}")
                        self.log(task.task_id, f"Incremented DC of {downstream_task.task_id} ({dependencies_met}/{downstream_task_total_dependencies})")
                        if dependencies_met == downstream_task_total_dependencies:
                            ready_downstream.append(downstream_task)
                
                if self.shutdown_flag.is_set(): return

                # Delegate Downstream Tasks Execution
                ## 1 Task ?: the same worker continues with it
                if len(ready_downstream) == 1:
                    task = self.subdag.get_node_by_id(ready_downstream[0].task_id) # type: ignore downstream_task_id)
                    continue
                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                elif len(ready_downstream) > 1:
                    continuation_task = ready_downstream[0] # choose the first task
                    tasks_to_delegate = ready_downstream[1:]
                    
                    for task in tasks_to_delegate:
                        self._parallelize(self.subdag.create_subdag(task))
                    
                    # Continue with one task in this worker
                    self.log(task.task_id, f"Continuing with first of multiple downstream tasks: {continuation_task}")
                    task = self.subdag.get_node_by_id(ready_downstream[0].task_id) # type: ignore downstream_task_id)
                    continue
                else:
                    return  # Give up
        except Exception as e:
            self.log(task.task_id, f"Error: {str(e)}") # type: ignore
            raise e

    def _parallelize(self, subsubdag: dag.DAG):
        LocalCoroutineWorker(subsubdag).start_executing()

    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        print(f"VW[{self.worker_id}] T({task_id}) => {message}")

class FlaskProcessExecutor(Worker):
    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, subdag: dag.DAG, flask_server_address: str):
        self.shutdown_flag = asyncio.Event()
        self.flask_server_address = flask_server_address
        # Tracks all Workers created by this worker and those workers, recursivelly
        self.subdag = subdag
        self.worker_id = str(uuid.uuid4())[:3]  # Use first 8 chars of a UUID for readable IDs
        if not subdag.root_node: raise Exception(f"FlaskProcessExecutor expected a subdag with only 1 root node. Got {len(subdag.root_nodes)}")
    
    async def start_executing(self):
        task = self.subdag.root_node

        try:
            while not self.shutdown_flag.is_set():
                # Should be none on the first call. Not None if the same Virtual Worker executes multiple tasks
                if self.shutdown_flag.is_set(): return

                # 1. DOWNLOAD DEPENDENCIES
                self.log(task.task_id, f"({task.task_id}) 1) Grabbing Dependencies...")
                task_dependencies: dict[str, Any] = {}
                for dependency_task in task.upstream_nodes:
                    task_output = intermediate_storage.IntermediateStorage.get(dependency_task.task_id)
                    if task_output is None: raise Exception(f"[BUG] Task {dependency_task.task_id}'s data is not available")
                    task_dependencies[dependency_task.task_id] = cloudpickle.loads(task_output) # type: ignore
                
                # 2. EXECUTE TASK
                self.log(task.task_id, f"({task.task_id}) 2) Executing...")
                task_result = task.invoke(dependencies=task_dependencies)
                upload_result = intermediate_storage.IntermediateStorage.set(task.task_id, cloudpickle.dumps(task_result))
                if not upload_result: raise Exception(f"[BUG] Task {task.task_id}'s data could not be uploaded to Redis (set() failed)")

                if self.shutdown_flag.is_set(): return

                if len(task.downstream_nodes) == 0: 
                    self.log(task.task_id, f"Last Task finished. Shutting down executor...")
                    self.shutdown_flag.set()
                    return

                # 3. HANDLE FAN-OUT (1-1 or 1-N)
                self.log(task.task_id, f"({task.task_id}) 3) Handle Fan-Out {task.task_id} => [{[t.task_id for t in task.downstream_nodes]}]")
                ready_downstream: list[dag_task_node.DAGTaskNode] = []
                for downstream_task in task.downstream_nodes:
                    downstream_task_total_dependencies = len(self.subdag.get_node_by_id(downstream_task.task_id).upstream_nodes)
                    if downstream_task_total_dependencies == 1: # {task} was the only dependency
                        ready_downstream.append(downstream_task)
                    else:
                        dependencies_met = intermediate_storage.IntermediateStorage.increment_and_get(f"dependency-counter-{downstream_task}")
                        self.log(task.task_id, f"Incremented DC of {downstream_task.task_id} ({dependencies_met}/{downstream_task_total_dependencies})")
                        if dependencies_met == downstream_task_total_dependencies:
                            ready_downstream.append(downstream_task)
                
                if self.shutdown_flag.is_set(): return

                # Delegate Downstream Tasks Execution
                ## 1 Task ?: the same worker continues with it
                if len(ready_downstream) == 1:
                    task = self.subdag.get_node_by_id(ready_downstream[0].task_id) # type: ignore downstream_task_id)
                    continue
                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                elif len(ready_downstream) > 1:
                    continuation_task = ready_downstream[0] # choose the first task
                    tasks_to_delegate = ready_downstream[1:]
                    
                    for task in tasks_to_delegate:
                        self.parallelize(self.subdag.create_subdag(task))
                    
                    # Continue with one task in this worker
                    self.log(task.task_id, f"Continuing with first of multiple downstream tasks: {continuation_task}")
                    task = self.subdag.get_node_by_id(ready_downstream[0].task_id) # type: ignore downstream_task_id)
                    continue
                else:
                    return  # Give up
        except Exception as e:
            self.log(task.task_id, f"Error: {str(e)}") # type: ignore
            raise e

    def parallelize_self(self):
        self.parallelize(self.subdag)

    def parallelize(self, subsubdag: dag.DAG):
        '''
        Each invocation is done inside a new Coroutine without blocking the owner Thread
        '''
        print(f"Invoking remote executor for subsubdag with {len(subsubdag.root_nodes)} root nodes | First Node: {subsubdag.root_node}")
        async def __parallelize():
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.flask_server_address, data=cloudpickle.dumps(subsubdag), headers={'Content-Type': 'application/octet-stream'}
                ) as response:
                    if response.status != 202:
                        text = await response.text()
                        raise Exception(f"Failed to invoke executor: {text}")
        asyncio.create_task(__parallelize())

    def _parallelize_locally(self, subsubdag: dag.DAG):
        pass # makes sense to implement?

    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        print(f"VW[{self.worker_id}] T({task_id}) => {message}")