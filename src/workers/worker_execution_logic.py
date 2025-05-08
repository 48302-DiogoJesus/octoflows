import asyncio
from itertools import groupby
from typing import Any

import cloudpickle

from src.dag import dag
from src.dag_task_node import TASK_COMPLETION_EVENT_PREFIX, DAGTaskNode
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, TaskInputMetrics, TaskInvocationMetrics
from src.storage.storage import Storage
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class WorkerExecutionLogic():
    @staticmethod
    async def override_handle_inputs(intermediate_storage: Storage, task: DAGTaskNode, subdag: dag.SubDAG, worker_resource_config: TaskWorkerResourceConfiguration | None) -> tuple[dict[str, Any], list[TaskInputMetrics], float]:
        task_dependencies: dict[str, Any] = {}
        _input_metrics: list[TaskInputMetrics] = []
        dependency_download_timer = Timer()
        upstream_tasks_without_cached_results = []

        for task in task.upstream_nodes:
            if task.cached_result is None:
                upstream_tasks_without_cached_results.append(task)
            else:
                task_dependencies[task.id.get_full_id()] = task.cached_result.result
                _input_metrics.append(
                    TaskInputMetrics(
                        task_id=task.id.get_full_id(),
                        size_bytes=calculate_data_structure_size(task.cached_result.result),
                        time_ms=0,
                        normalized_time_ms=0
                    )
                )

        async def _fetch_dependency_data(dependency_task, subdag, intermediate_storage):
            fotimer = Timer()
            task_output = await intermediate_storage.get(dependency_task.id.get_full_id_in_dag(subdag))
            if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
            input_fetch_time = fotimer.stop()
            loaded_data = cloudpickle.loads(task_output)
            return (
                dependency_task.id.get_full_id(),
                loaded_data,
                TaskInputMetrics(
                    task_id=dependency_task.id.get_full_id_in_dag(subdag),
                    size_bytes=calculate_data_structure_size(loaded_data),
                    time_ms=input_fetch_time,
                    normalized_time_ms=input_fetch_time * (worker_resource_config.memory_mb / BASELINE_MEMORY_MB) if worker_resource_config else 0
                )
            )

        # Concurrently fetch dependencies
        fetch_coroutines = [_fetch_dependency_data(dependency_task, subdag, intermediate_storage) for dependency_task in upstream_tasks_without_cached_results]
        results = await asyncio.gather(*fetch_coroutines)

        for task_id, data, metrics in results:
            task_dependencies[task_id] = data
            _input_metrics.append(metrics)
        
        _total_input_download_time_ms = dependency_download_timer.stop()
        return (task_dependencies, _input_metrics, _total_input_download_time_ms)
    
    @staticmethod
    async def override_handle_execution(task: DAGTaskNode, task_dependencies: dict[str, Any]) -> tuple[Any, float]:
        exec_timer = Timer()
        task_result = task.invoke(dependencies=task_dependencies)
        return (task_result, exec_timer.stop())

    @staticmethod
    async def override_handle_output(task_result: Any, task: DAGTaskNode, subdag: dag.SubDAG, intermediate_storage: Storage, metadata_storage: Storage) -> float: 
        output_upload_timer = Timer()
        task_result_serialized = cloudpickle.dumps(task_result)
        await intermediate_storage.set(task.id.get_full_id_in_dag(subdag), task_result_serialized)
        task_result_output_time_ms = output_upload_timer.stop()
        #! Can be optimized, don't need to always be sending this
        await metadata_storage.publish(f"{TASK_COMPLETION_EVENT_PREFIX}{task.id.get_full_id_in_dag(subdag)}", b"1")
        return task_result_output_time_ms

    @staticmethod
    async def override_handle_downstream(this_worker, downstream_tasks_ready: list[DAGTaskNode], subdag: dag.SubDAG) -> tuple[list[DAGTaskNode], int, float]:
        from src.workers.worker import Worker
        _this_worker: Worker = this_worker
        my_continuation_tasks = []
        other_continuation_tasks: list[DAGTaskNode] = []
        coroutines = []
        total_invocation_time_timer = Timer()
        for task in downstream_tasks_ready:
            if task.get_annotation(TaskWorkerResourceConfiguration).worker_id == _this_worker.my_resource_configuration.worker_id:
                my_continuation_tasks.append(task)
            else:
                other_continuation_tasks.append(task)
        total_invocations_count = len(other_continuation_tasks)

        if len(other_continuation_tasks) > 0:
            coroutines.append(_this_worker.delegate([subdag.create_subdag(t) for t in other_continuation_tasks], called_by_worker=True))
            await asyncio.gather(*coroutines) # wait for the delegations to be accepted

        total_invocation_time_ms = total_invocation_time_timer.stop()
        return (my_continuation_tasks, total_invocations_count, total_invocation_time_ms)