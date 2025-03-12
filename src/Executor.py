import asyncio
import threading
import time
from typing import Any
import uuid
import cloudpickle

import src.dag as dag
import src.intermediate_storage as intermediate_storage
import src.dag_task_node as dag_task_node

class VirtualWorker:
    """
    Processes DAG tasks
    continuing with single downstream tasks and spawning new workers (coroutines) for branches.
    """
    def __init__(self, executor: "Executor", subdag: dag.DAG):
        self.executor = executor
        self.subdag = subdag
        self.worker_id = str(uuid.uuid4())[:3]  # Use first 8 chars of a UUID for readable IDs
    
    async def execute_task(self, task: dag_task_node.DAGTaskNode):
        try:
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
                downstream_task_total_dependencies = len(self.subdag.get_node_by_id(downstream_task.task_id).upstream_nodes)
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
                await self.execute_task(self.subdag.get_node_by_id(ready_downstream[0].task_id)) # type: ignore downstream_task_id)
            ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
            elif len(ready_downstream) > 1:
                continuation_task = ready_downstream[0] # choose the first task
                tasks_to_delegate = ready_downstream[1:]
                
                for task in tasks_to_delegate:
                    self.executor._spawn_new_virtual_worker(task)
                
                # Continue with one task in this worker
                self.log(task.task_id, f"Continuing with first of multiple downstream tasks: {continuation_task}")
                await self.execute_task(continuation_task)
        except Exception as e:
            self.log(task.task_id, f"Error: {str(e)}")
            raise e
    
    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        print(f"W[{self.worker_id}] T({task_id}) => {message}")


class Executor:
    """
    Asynchronous executor for DAG tasks.
    Manages the task execution flow and coordinates workers.
    The actual execution is delegated to InternalWorker instances.
    """
    def __init__(self, dag: dag.DAG):
        self.dag = dag
        self.workers: list[asyncio.Task] = []
        self.shutdown_flag = asyncio.Event()

    async def start_executing(self):
        # Create 1 worker per root node
        for root_node in self.dag.root_nodes:
            self._spawn_new_virtual_worker(root_node)
        
        print(f"Launched {len(self.dag.root_nodes)} workers. Waiting for workers to shutdown...")
        await asyncio.gather(*self.workers)
        print(f"All workers finished. Executor Shutdown")

    def _spawn_new_virtual_worker(self, root_node: dag_task_node.DAGTaskNode):
        self.workers.append(
            asyncio.create_task(VirtualWorker(self, self.dag.create_subdag(root_nodes=[root_node])).execute_task(root_node))
        )