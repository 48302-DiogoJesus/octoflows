import asyncio
from typing import Any

import cloudpickle

from src.dag import dag
from src.dag_task_node import DAGTaskNode
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, TaskInputMetrics, TaskInvocationMetrics
from src.storage.storage import Storage
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size
from src.worker_resource_configuration import TaskWorkerResourceConfiguration

class WorkerExecutionLogic():
    @staticmethod
    async def override_handle_inputs(intermediate_storage: Storage, task: DAGTaskNode, subdag: dag.SubDAG, worker_resource_config: TaskWorkerResourceConfiguration | None) -> tuple[dict[str, Any], list[TaskInputMetrics], float]:
        task_dependencies: dict[str, Any] = {}
        _input_metrics: list[TaskInputMetrics] = []
        dependency_download_timer = Timer()
        for dependency_task in task.upstream_nodes:
            fotimer = Timer()
            task_output = await intermediate_storage.get(dependency_task.id.get_full_id_in_dag(subdag))
            if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
            task_dependencies[dependency_task.id.get_full_id()] = cloudpickle.loads(task_output)
            _input_metrics.append(TaskInputMetrics(
                task_id=dependency_task.id.get_full_id_in_dag(subdag),
                size_bytes=calculate_data_structure_size(task_dependencies[dependency_task.id.get_full_id()]),
                time_ms=fotimer.stop(),
                normalized_time_ms=fotimer.stop() * (worker_resource_config.memory_mb / BASELINE_MEMORY_MB) if worker_resource_config else 0
            ))
        
        _total_input_download_time_ms = dependency_download_timer.stop()
        return (task_dependencies, _input_metrics, _total_input_download_time_ms)
    
    @staticmethod
    async def override_handle_execution(task: DAGTaskNode, task_dependencies: dict[str, Any]) -> tuple[Any, float]:
        exec_timer = Timer()
        task_result = task.invoke(dependencies=task_dependencies)
        return (task_result, exec_timer.stop())

    @staticmethod
    async def override_handle_output(task_result: Any, task: DAGTaskNode, subdag: dag.SubDAG, intermediate_storage: Storage) -> float: 
        output_upload_timer = Timer()
        task_result_serialized = cloudpickle.dumps(task_result)
        await intermediate_storage.set(task.id.get_full_id_in_dag(subdag), task_result_serialized)
        return output_upload_timer.stop()

    @staticmethod
    async def override_handle_downstream(worker, downstream_tasks_ready: list[DAGTaskNode], task: DAGTaskNode, subdag: dag.SubDAG) -> tuple[DAGTaskNode, list[TaskInvocationMetrics], float]:
        from src.planning.dag_planner import SimpleDAGPlanner
        from src.worker import Worker
        _worker: Worker = worker
        continuation_task = downstream_tasks_ready[0] # choose the first task
        tasks_to_delegate = downstream_tasks_ready[1:]
        coroutines = []
        total_invocation_time_timer = Timer()
        downstream_invocation_times = []
        for t in tasks_to_delegate:
            workerResourcesConfig = t.get_annotation(TaskWorkerResourceConfiguration)
            # ! TODO: don't have a ref. to planner here
            casted_planner: SimpleDAGPlanner = self.planner # type: ignore
            if workerResourcesConfig is None and len(casted_planner.config.available_worker_resource_configurations) > 0:
                workerResourcesConfig = casted_planner.config.available_worker_resource_configurations[0]
                
            _worker.log(task.id.get_full_id_in_dag(subdag), f"Delegating downstream task: {t} with resources: {workerResourcesConfig}")
            delegate_invoke_timer = Timer()
            coroutines.append(_worker.delegate(
                subdag.create_subdag(t),
                resource_configuration=workerResourcesConfig,
                called_by_worker=True
            ))
            downstream_invocation_times.append(TaskInvocationMetrics(task_id=t.id.get_full_id_in_dag(subdag), time_ms=delegate_invoke_timer.stop()))
        
        await asyncio.gather(*coroutines) # wait for the delegations to be accepted

        total_invocation_time_ms = total_invocation_time_timer.stop()
        return (continuation_task, downstream_invocation_times, total_invocation_time_ms)