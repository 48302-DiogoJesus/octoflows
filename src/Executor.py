import asyncio
from typing import Any
import uuid
import cloudpickle
import aiohttp
from abc import ABC, abstractmethod

import src.dag as dag
import src.intermediate_storage as intermediate_storage
import src.dag_task_node as dag_task_node

class AbstractExecutor(ABC):
    shutdown_flag: asyncio.Event
    subdag: dag.DAG
    executor_id: str   # Use first 8 chars of a UUID for readable IDs

    def __init__(self, subdag: dag.DAG):
        self.shutdown_flag = asyncio.Event()
        self.subdag = subdag
        self.executor_id = str(uuid.uuid4())[:3]  # Use first 8 chars of a UUID for readable IDs
        if not subdag.root_node: raise Exception(f"AbstractExecutor expected a subdag with only 1 root node. Got {len(subdag.root_nodes)}")

    async def start_executing(self):
        task = self.subdag.root_node

        try:
            while not self.shutdown_flag.is_set():
                # 1. DOWNLOAD DEPENDENCIES
                self.log(task.id.get_full_id(), f"1) Grabbing Dependencies...")
                task_dependencies: dict[str, Any] = {}
                for dependency_task in task.upstream_nodes:
                    task_output = intermediate_storage.IntermediateStorage.get(dependency_task.id.get_full_id())
                    if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id()}'s data is not available")
                    task_dependencies[dependency_task.id.get_full_id()] = cloudpickle.loads(task_output) # type: ignore
                
                # 2. EXECUTE TASK
                self.log(task.id.get_full_id(), f"2) Executing...")
                task_result = task.invoke(dependencies=task_dependencies)
                upload_result = intermediate_storage.IntermediateStorage.set(task.id.get_full_id(), cloudpickle.dumps(task_result))
                if not upload_result: raise Exception(f"[BUG] Task {task.id.get_full_id()}'s data could not be uploaded to Redis (set() failed)")

                if self.shutdown_flag.is_set(): break

                if len(task.downstream_nodes) == 0: 
                    self.log(task.id.get_full_id(), f"Last Task finished. Shutting down executor...")
                    self.shutdown_flag.set()
                    break

                # 3. HANDLE FAN-OUT (1-1 or 1-N)
                self.log(task.id.get_full_id(), f"3) Handle Fan-Out {task.id.get_full_id()} => [{[t.id.get_full_id() for t in task.downstream_nodes]}]")
                ready_downstream: list[dag_task_node.DAGTaskNode] = []
                for downstream_task in task.downstream_nodes:
                    downstream_task_total_dependencies = len(self.subdag.get_node_by_id(downstream_task.id).upstream_nodes)
                    if downstream_task_total_dependencies == 1: # {task} was the only dependency
                        ready_downstream.append(downstream_task)
                    else:
                        dependencies_met = intermediate_storage.IntermediateStorage.increment_and_get(f"dependency-counter-{downstream_task}")
                        self.log(task.id.get_full_id(), f"Incremented DC of {downstream_task.id.get_full_id()} ({dependencies_met}/{downstream_task_total_dependencies})")
                        if dependencies_met == downstream_task_total_dependencies:
                            ready_downstream.append(downstream_task)
                
                if self.shutdown_flag.is_set(): break

                # Delegate Downstream Tasks Execution
                ## 1 Task ?: the same executor continues with it
                if len(ready_downstream) == 1:
                    task = self.subdag.get_node_by_id(ready_downstream[0].id) # type: ignore downstream_task_id)
                    continue
                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                elif len(ready_downstream) > 1:
                    continuation_task = ready_downstream[0] # choose the first task
                    tasks_to_delegate = ready_downstream[1:]
                    
                    for task in tasks_to_delegate:
                        asyncio.create_task(self.delegate(self.subdag.create_subdag(task)))
                    
                    # Continue with one task in this executor
                    self.log(task.id.get_full_id(), f"Continuing with first of multiple downstream tasks: {continuation_task}")
                    task = self.subdag.get_node_by_id(ready_downstream[0].id) # type: ignore downstream_task_id)
                    continue
                else:
                    self.log(task.id.get_full_id(), f"No ready downstream tasks found. Shutting down executor...")
                    break  # Give up
        except Exception as e:
            self.log(task.id.get_full_id(), f"Error: {str(e)}") # type: ignore
            raise e

        # Cleanup
        self.log(task.id.get_full_id(), f"Executor shut down!")

    @abstractmethod
    async def delegate(self, subsubdag: dag.DAG): pass

    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        print(f"Executor({self.executor_id}) Task({task_id}) | {message}")

class LocalExecutor(AbstractExecutor):
    """
    Processes DAG tasks
    continuing with single downstream tasks and spawning new workers (coroutines) for branches.
    """
    def __init__(self, subdag: dag.DAG):
       super().__init__(subdag)
    
    async def delegate(self, subsubdag: dag.DAG):
        asyncio.create_task(LocalExecutor(subsubdag).start_executing())

class FlaskExecutor(AbstractExecutor):
    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, subdag: dag.DAG, flask_server_address: str):
        self.flask_server_address = flask_server_address
        super().__init__(subdag)

    async def parallelize_self(self):
        ''' Delegate this subdag to a remote worker. This should be called by the Client to initiate a DAG execution '''
        await self.delegate(self.subdag)

    async def delegate(self, subsubdag: dag.DAG):
        '''
        Each invocation is done inside a new Coroutine without blocking the owner Thread
        '''
        self.log(subsubdag.root_node.id.get_full_id(), f"Invoking remote executor for subsubdag with {len(subsubdag.root_nodes)} root nodes | First Node: {subsubdag.root_node}")
        async with aiohttp.ClientSession() as session:
            async with await session.post(
                self.flask_server_address, data=cloudpickle.dumps(subsubdag), headers={'Content-Type': 'application/octet-stream'}
            ) as response:
                if response.status != 202:
                    text = await response.text()
                    raise Exception(f"Failed to invoke executor: {text}")