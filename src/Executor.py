import asyncio
from typing import Any
import uuid
import cloudpickle
import requests

import src.dag as dag
import src.intermediate_storage as intermediate_storage
import src.dag_task_node as dag_task_node

class VirtualWorker:
    """
    Processes DAG tasks
    continuing with single downstream tasks and spawning new workers (coroutines) for branches.
    """
    def __init__(self, executor: "Executor", subsubdag: dag.DAG):
        self.executor = executor
        self.subsubdag = subsubdag
        self.worker_id = str(uuid.uuid4())[:3]  # Use first 8 chars of a UUID for readable IDs
        if not subsubdag.root_node: raise Exception(f"VirtualWorker expected a subdag with only 1 root node. Got {len(subsubdag.root_nodes)}")
    
    async def execute_task(self, task: dag_task_node.DAGTaskNode | None = None):
        try:
            # Should be none on the first call. Not None if the same Virtual Worker executes multiple tasks
            if task is None:
                task = self.subsubdag.root_node

            if self.executor.shutdown_flag.is_set(): return

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

            if self.executor.shutdown_flag.is_set(): return

            if len(task.downstream_nodes) == 0: 
                self.log(task.task_id, f"Last Task finished. Shutting down executor...")
                self.executor.shutdown_flag.set()
                return

            # 3. HANDLE FAN-OUT (1-1 or 1-N)
            self.log(task.task_id, f"({task.task_id}) 3) Handle Fan-Out {task.task_id} => [{[t.task_id for t in task.downstream_nodes]}]")
            ready_downstream: list[dag_task_node.DAGTaskNode] = []
            for downstream_task in task.downstream_nodes:
                downstream_task_total_dependencies = len(self.subsubdag.get_node_by_id(downstream_task.task_id).upstream_nodes)
                if downstream_task_total_dependencies == 1: # {task} was the only dependency
                    ready_downstream.append(downstream_task)
                else:
                    dependencies_met = intermediate_storage.IntermediateStorage.increment_and_get(f"dependency-counter-{downstream_task}")
                    self.log(task.task_id, f"Incremented DC of {downstream_task.task_id} ({dependencies_met}/{downstream_task_total_dependencies})")
                    if dependencies_met == downstream_task_total_dependencies:
                        ready_downstream.append(downstream_task)
            
            if self.executor.shutdown_flag.is_set(): return

            # Delegate Downstream Tasks Execution
            ## 1 Task ?: the same worker continues with it
            if len(ready_downstream) == 1:
                await self.execute_task(self.subsubdag.get_node_by_id(ready_downstream[0].task_id)) # type: ignore downstream_task_id)
            ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
            elif len(ready_downstream) > 1:
                continuation_task = ready_downstream[0] # choose the first task
                tasks_to_delegate = ready_downstream[1:]
                
                for task in tasks_to_delegate:
                    self.executor._parallelize_subdag(self.subsubdag.create_subdag(task))
                
                # Continue with one task in this worker
                self.log(task.task_id, f"Continuing with first of multiple downstream tasks: {continuation_task}")
                await self.execute_task(continuation_task)
        except Exception as e:
            self.log(task.task_id, f"Error: {str(e)}") # type: ignore
            raise e
    
    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        print(f"W[{self.worker_id}] T({task_id}) => {message}")


from abc import ABC, abstractmethod

class Executor(ABC):
    subdag: dag.DAG
    workers: list[asyncio.Task]
    shutdown_flag: asyncio.Event

    def __init__(self, subdag: dag.DAG):
        self.subdag = subdag
        self.workers: list[asyncio.Task] = []
        self.shutdown_flag = asyncio.Event()

    @abstractmethod
    async def start_executing(self):
        pass

    @abstractmethod
    def _parallelize_subdag(self, subsubdag: dag.DAG):
        # Dispatch the subdag to execute on a local process/thread
        pass

class SameProcessExecutor(Executor):
    """
    Uses local workers (asyncio coroutines) to execute tasks
    Waits for the completion of all workers
    """
    def __init__(self, subdag: dag.DAG):
        super().__init__(subdag)

    async def start_executing(self):
        # Create 1 worker per root node
        for root_node in self.subdag.root_nodes:
            self._parallelize_subdag(self.subdag.create_subdag(root_node))
        
        print(f"Launched {len(self.subdag.root_nodes)} workers. Waiting for workers to shutdown...")
        await asyncio.gather(*self.workers)
        print(f"All workers finished. Executor Shutdown")

    def _parallelize_subdag(self, subsubdag: dag.DAG):
        self.workers.append(
            asyncio.create_task(VirtualWorker(self, subsubdag).execute_task())
        )

class FlaskProcessExecutor(Executor):
    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, subdag: dag.DAG, flask_server_address: str):
        super().__init__(subdag)
        self.flask_server_address = flask_server_address

    async def start_executing(self):
        # Create 1 worker per root node
        for root_node in self.subdag.root_nodes:
            self._parallelize_subdag(self.subdag.create_subdag(root_node))
        
        print(f"Launched {len(self.subdag.root_nodes)} workers. Exiting Executor")

    def _parallelize_subdag(self, subsubdag: dag.DAG):
        print(f"Invoking remote executor for subsubdag with {len(subsubdag.root_nodes)} root nodes | First Node: {subsubdag.root_node}")
        response = requests.post(
            self.flask_server_address,
            data=cloudpickle.dumps(subsubdag),
            headers={'Content-Type': 'application/octet-stream'}
        )
        if response.status_code != 200: raise Exception(f"Failed to invoke executor: {response.text}")