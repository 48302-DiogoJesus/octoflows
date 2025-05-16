import asyncio
from typing import Any
import cloudpickle

from src.dag.dag import FullDAG, SubDAG
from src.storage.events import TASK_COMPLETION_EVENT_PREFIX
from src.storage.storage import Storage
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size

logger = create_logger(__name__)

class WorkerExecutionLogic():
    @staticmethod
    async def override_on_worker_ready(intermediate_storage: Storage, task, dag: FullDAG, this_worker_id: str):
        pass

    async def override_handle_inputs(self, intermediate_storage: Storage, task, subdag: SubDAG, worker_resource_config) -> tuple[dict[str, Any], list, float]:
        from src.storage.metrics.metrics_types import TaskInputMetrics
        from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
        from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB
        _worker_resource_config: TaskWorkerResourceConfiguration = worker_resource_config
        task_dependencies: dict[str, Any] = {}
        _input_metrics: list[TaskInputMetrics] = []
        dependency_download_timer = Timer()
        upstream_tasks_without_cached_results = []

        for t in task.upstream_nodes:
            if t.cached_result is None:
                upstream_tasks_without_cached_results.append(t)
            else:
                task_dependencies[t.id.get_full_id()] = t.cached_result.result
                _input_metrics.append(
                    TaskInputMetrics(
                        task_id=t.id.get_full_id_in_dag(subdag),
                        size_bytes=calculate_data_structure_size(t.cached_result.result),
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
                    normalized_time_ms=input_fetch_time * (_worker_resource_config.memory_mb / BASELINE_MEMORY_MB) if _worker_resource_config else 0
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
    
    async def override_handle_execution(self, task, task_dependencies: dict[str, Any]) -> tuple[Any, float]:
        exec_timer = Timer()
        task_result = task.invoke(dependencies=task_dependencies)
        return (task_result, exec_timer.stop())

    async def override_handle_output(self, task_result: Any, task, subdag: SubDAG, intermediate_storage: Storage, metadata_storage: Storage) -> float: 
        output_upload_timer = Timer()
        task_result_serialized = cloudpickle.dumps(task_result)
        await intermediate_storage.set(task.id.get_full_id_in_dag(subdag), task_result_serialized)
        task_result_output_time_ms = output_upload_timer.stop()
        #! Can be optimized, don't need to always be sending this
        receivers = await metadata_storage.publish(f"{TASK_COMPLETION_EVENT_PREFIX}{task.id.get_full_id_in_dag(subdag)}", b"1")
        logger.info(f"Receivers for completion of task {task.id.get_full_id_in_dag(subdag)}: {receivers}")
        return task_result_output_time_ms

    async def override_handle_downstream(self, this_worker, downstream_tasks_ready, subdag: SubDAG) -> tuple[list, int, float]:
        from src.workers.worker import Worker
        from src.dag_task_node import DAGTaskNode
        from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

        _downstream_tasks_ready: list[DAGTaskNode] = downstream_tasks_ready

        _this_worker: Worker = this_worker
        my_continuation_tasks: list[DAGTaskNode] = []
        other_continuation_tasks: list[DAGTaskNode] = []
        coroutines = []
        total_invocation_time_timer = Timer()
        for task in _downstream_tasks_ready:
            if task.get_annotation(TaskWorkerResourceConfiguration).worker_id == _this_worker.my_resource_configuration.worker_id:
                my_continuation_tasks.append(task)
            else:
                requires_launching_worker = True
                for dunode in task.upstream_nodes:
                    if dunode.get_annotation(TaskWorkerResourceConfiguration).worker_id == task.get_annotation(TaskWorkerResourceConfiguration).worker_id:
                        # => We know that the worker for the downstream task was already launched meaning
                        #   we don't need to launch a new worker, only send the READY event and the appropriate 
                        #   worker will handle it
                        requires_launching_worker = False
                        break
                
                if requires_launching_worker: other_continuation_tasks.append(task)
        
        total_invocations_count = len(other_continuation_tasks)

        if len(other_continuation_tasks) > 0:
            logger.info(_this_worker.my_resource_configuration.worker_id, f"Delegating {len(other_continuation_tasks)} tasks to other workers...")
            coroutines.append(_this_worker.delegate([subdag.create_subdag(t) for t in other_continuation_tasks], called_by_worker=True))
            await asyncio.gather(*coroutines) # wait for the delegations to be accepted

        for my_task in my_continuation_tasks:
            logger.info(f"Worker({_this_worker.my_resource_configuration.worker_id}) I will execute {my_task.id.get_full_id()}...")

        total_invocation_time_ms = total_invocation_time_timer.stop()
        return (my_continuation_tasks, total_invocations_count, total_invocation_time_ms)
    
WorkerExecutionLogicSingleton = WorkerExecutionLogic()