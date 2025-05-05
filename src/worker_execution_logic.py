import asyncio
from typing import Any

import cloudpickle

from src.dag import dag
from src.dag_task_node import TASK_COMPLETION_EVENT_PREFIX, DAGTaskNode
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, TaskInputMetrics, TaskInvocationMetrics
from src.storage.storage import Storage
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

class WorkerExecutionLogic():
    @staticmethod
    async def override_handle_inputs(intermediate_storage: Storage, task: DAGTaskNode, subdag: dag.SubDAG, worker_resource_config: TaskWorkerResourceConfiguration | None) -> tuple[dict[str, Any], list[TaskInputMetrics], float]:
        task_dependencies: dict[str, Any] = {}
        _input_metrics: list[TaskInputMetrics] = []
        dependency_download_timer = Timer()

        async def _fetch_dependency_data(dependency_task, subdag, intermediate_storage):
            fotimer = Timer()
            task_output = await intermediate_storage.get(dependency_task.id.get_full_id_in_dag(subdag))
            if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
            loaded_data = cloudpickle.loads(task_output)
            return (
                dependency_task.id.get_full_id(),
                loaded_data,
                TaskInputMetrics(
                    task_id=dependency_task.id.get_full_id_in_dag(subdag),
                    size_bytes=calculate_data_structure_size(loaded_data),
                    time_ms=fotimer.stop(),
                    normalized_time_ms=fotimer.stop() * (worker_resource_config.memory_mb / BASELINE_MEMORY_MB) if worker_resource_config else 0
                )
            )

        # Concurrently fetch dependencies
        fetch_coroutines = [_fetch_dependency_data(dependency_task, subdag, intermediate_storage) for dependency_task in task.upstream_nodes]
        results = await asyncio.gather(*fetch_coroutines)

        # Process results
        task_dependencies = {}
        _input_metrics = []
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
    async def override_handle_downstream(worker, downstream_tasks_ready: list[DAGTaskNode], task: DAGTaskNode, subdag: dag.SubDAG, my_worker_resources: TaskWorkerResourceConfiguration) -> tuple[DAGTaskNode | None, int, float]:
        from src.worker import Worker
        _worker: Worker = worker
        tasks_w_matching_resource_tasks = [t for t in downstream_tasks_ready if t.try_get_annotation(TaskWorkerResourceConfiguration) == my_worker_resources]
        # Pick a task that requires the same resources as this worker
        continuation_task = tasks_w_matching_resource_tasks[0] if len(tasks_w_matching_resource_tasks) > 0 else None
        # Delegate all other tasks
        tasks_to_delegate = downstream_tasks_ready if not continuation_task else [t for t in downstream_tasks_ready if t != continuation_task]
        coroutines = []
        total_invocation_time_timer = Timer()
        total_invocations_count = len(tasks_to_delegate)
        for t in tasks_to_delegate:
            _worker.log(task.id.get_full_id_in_dag(subdag), f"Delegating downstream task: {t}")
            coroutines.append(_worker.delegate(subdag.create_subdag(t), called_by_worker=True))
        
        await asyncio.gather(*coroutines) # wait for the delegations to be accepted

        total_invocation_time_ms = total_invocation_time_timer.stop()
        return (continuation_task, total_invocations_count, total_invocation_time_ms)