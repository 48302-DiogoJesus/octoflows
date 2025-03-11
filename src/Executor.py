import asyncio
import threading
import time
from typing import Any
import uuid
import cloudpickle

import src.intermediate_storage as intermediate_storage
import src.dag as dag
import src.dag_task_node as dag_task_node

class InternalWorker:
    """
    A worker that processes DAG tasks in a depth-first manner,
    continuing with single downstream tasks and spawning new workers for branches.
    """
    def __init__(self, executor: "Executor", subdag: dag.DAG):
        self.executor = executor
        self.subdag = subdag
        self.worker_id = str(uuid.uuid4())[:3]  # Use first 8 chars of a UUID for readable IDs
    
    def execute_task(self, task: dag_task_node.DAGTaskNode) -> asyncio.Task:
        return asyncio.create_task(self._execute_task(task))

    async def _execute_task(self, task: dag_task_node.DAGTaskNode):
        try:
            # Execute the node. If this function was called, it means all its dependencies are in remote Storage
            task_dependencies: dict[str, Any] = {}
            for dependency_task in task.upstream_nodes:
                task_output = intermediate_storage.IntermediateStorage.get(dependency_task.task_id)
                if task_output is None: raise Exception(f"[BUG] Task {dependency_task.task_id}'s data is not available")
                task_dependencies[dependency_task.task_id] = cloudpickle.loads(task_output) # type: ignore
            
            self.log(task.task_id, f"({task.task_id}) Executing...")
            task_result = task.execute(dependencies=task_dependencies)
            upload_result = intermediate_storage.IntermediateStorage.set(task.task_id, cloudpickle.dumps(task_result))
            if not upload_result: raise Exception(f"[BUG] Task {task.task_id}'s data could not be uploaded to Redis (set() failed)")
            self.log(task.task_id, f"({task.task_id}) Executed!")

            ready_downstream: list[dag_task_node.DAGTaskNode] = []
            for downstream_task in task.downstream_nodes:
                downstream_task_total_dependencies = len(self.subdag.get_node_by_id(downstream_task.task_id).upstream_nodes)
                if downstream_task_total_dependencies == 1: # {task} was the only dependency
                    self.log(task.task_id, f"1-1 READY {downstream_task.task_id}")
                    ready_downstream.append(downstream_task)
                else:
                    dependencies_met = intermediate_storage.IntermediateStorage.increment_and_get(f"dependency-counter-{downstream_task}")
                    if dependencies_met == downstream_task_total_dependencies:
                        self.log(task.task_id, f"FAN-IN READY {downstream_task.task_id} ({dependencies_met}/{downstream_task_total_dependencies})")
                        ready_downstream.append(downstream_task)
            
            # Delegate Downstream Tasks Execution
            ## 1 Task ?: the same worker continues with it
            if len(ready_downstream) == 1:
                await self.execute_task(self.subdag.get_node_by_id(ready_downstream[0].task_id)) # type: ignore downstream_task_id)
            ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
            elif len(ready_downstream) > 1:
                continuation_task = ready_downstream[0] # choose the first task
                tasks_to_delegate = ready_downstream[1:]
                
                for task in tasks_to_delegate:
                    InternalWorker(self.executor, subdag=dag.DAG(master_dag_id=self.subdag.master_dag_id, sink_node=self.subdag.sink_node, root_nodes=[task])).execute_task(self.subdag.get_node_by_id(task.task_id))
                
                # Continue with one task in this worker
                self.log(task.task_id, f"Continuing with first of multiple downstream tasks: {continuation_task}")
                await self.execute_task(continuation_task)
                
        except Exception as e:
            self.log(task.task_id, f"Error")
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
        self.shutdown_flag = threading.Event()

    async def execute(self):
        """Start executing the DAG from root nodes."""
        # storage_pubsub_topic = f"{self.dag.master_dag_id}-final-result"
        # await intermediate_storage.IntermediateStorage.subscribe_to_channel(storage_pubsub_topic, callback=self.ready_queue.put_nowait,wait_for_result=False)

        # Create 1 worker per root node
        for root_node in self.dag.root_nodes:
            InternalWorker(self, dag.DAG(master_dag_id=self.dag.master_dag_id, sink_node=self.dag.sink_node, root_nodes=[root_node])).execute_task(root_node)
        
        print(f"Launched {len(self.dag.root_nodes)} workers. Waiting for the shutdown signal...")
        self.shutdown_flag.wait()
        print(f"DAG Executor Finishing. Doesn't mean master DAG is complete!!")

        # TODO: worker cleanup code

        # await intermediate_storage.IntermediateStorage.unsubscribe_from_channel(storage_pubsub_topic, wait_for_result=True)
        # print(f"Unsubscribed from ready_queue channel")