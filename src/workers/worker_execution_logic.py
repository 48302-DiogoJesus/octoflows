import asyncio
from types import CoroutineType
from typing import Any
from abc import ABC

from src.utils.logger import create_logger
from src.utils.timer import Timer

logger = create_logger(__name__)

class WorkerExecutionLogic(ABC):
    @staticmethod
    async def wel_on_worker_ready(planner, intermediate_storage, dag, this_worker_id: str | None):
        pass

    @staticmethod
    async def wel_before_task_handling(planner, this_worker, metadata_storage, subdag, current_task, is_dupping: bool):
        pass

    @staticmethod
    async def wel_override_handle_inputs(planner, intermediate_storage, task, subdag, upstream_tasks_without_cached_results: list, worker_resource_config, task_dependencies: dict[str, Any]) -> tuple[list, list[str], CoroutineType | None] | None:
        """
        returns (
            tasks_to_fetch (on default implementation, fetch ALL tasks that don't have cached results),
            input_metrics,
            wait_until_coroutine (so that the caller can fetch the tasks in parallel)
        )
        """
        return (upstream_tasks_without_cached_results, [], None)

    @staticmethod
    async def wel_before_task_execution(planner, this_worker, metadata_storage, subdag, current_task, is_dupping: bool):
        pass

    @staticmethod
    async def wel_override_should_upload_output(planner, current_task, subdag, this_worker, metadata_storage, is_dupping: bool) -> bool | None:
        """
        return value indicates if the task result was uploaded or not 
        """
        from src.dag_task_node import DAGTaskNode
        _task: DAGTaskNode = current_task

        # only upload if necessary
        return subdag.sink_node.id.get_full_id() == _task.id.get_full_id() or any(dt.worker_config.worker_id is None or dt.worker_config.worker_id != this_worker.my_resource_configuration.worker_id for dt in _task.downstream_nodes)

    @staticmethod
    async def wel_override_handle_downstream(planner, fulldag, current_task, this_worker, downstream_tasks_ready, subdag, is_dupping: bool) -> list | None:
        from src.workers.worker import Worker
        from src.dag_task_node import DAGTaskNode

        _downstream_tasks_ready: list[DAGTaskNode] = downstream_tasks_ready
        _current_task: DAGTaskNode = current_task
        _this_worker: Worker = this_worker
        my_continuation_tasks: list[DAGTaskNode] = []
        other_continuation_tasks: list[DAGTaskNode] = []
        coroutines = []
        total_invocation_time_timer = Timer()
        for task in _downstream_tasks_ready:
            task_resource_config = task.worker_config
            if task_resource_config.worker_id is None:
                if len(my_continuation_tasks) == 0:
                    my_continuation_tasks.append(task)
                else:
                    # delegate all other DS tasks to other workers
                    other_continuation_tasks.append(task)
            elif task_resource_config.worker_id == _this_worker.my_resource_configuration.worker_id:
                my_continuation_tasks.append(task)
            else: # diff. worker id
                requires_launching_worker = True
                for dunode in task.upstream_nodes:
                    dunode_resource_config = dunode.worker_config
                    if dunode_resource_config.worker_id is None:
                        pass # can't reuse these, flexible workers don't subscribe to TASK_READY events
                    elif dunode_resource_config.worker_id == task_resource_config.worker_id:
                        # => We know that the worker for the downstream task was already launched, meaning
                        #   we don't need to launch a new worker, only send the READY event and the appropriate 
                        #   worker will handle it
                        requires_launching_worker = False
                        break
                
                if requires_launching_worker: other_continuation_tasks.append(task)
        
        total_invocations_count = len(other_continuation_tasks)

        if len(other_continuation_tasks) > 0:
            logger.info(f"W({_this_worker.my_resource_configuration.worker_id}) Delegating {len(other_continuation_tasks)} tasks to other workers...")
            coroutines.append(_this_worker.delegate([subdag.create_subdag(t) for t in other_continuation_tasks], fulldag, called_by_worker=True))
            await asyncio.gather(*coroutines) # wait for the delegations to be accepted

        for my_task in my_continuation_tasks:
            logger.info(f"W({_this_worker.my_resource_configuration.worker_id}) I will execute {my_task.id.get_full_id()}...")

        _current_task.metrics.total_invocation_time_ms = total_invocation_time_timer.stop() if len(_downstream_tasks_ready) > 0 else None
        _current_task.metrics.total_invocations_count = total_invocations_count
        return my_continuation_tasks