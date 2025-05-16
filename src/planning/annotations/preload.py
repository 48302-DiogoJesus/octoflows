import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar
import cloudpickle

from src.dag import dag
from src.dag_task_annotation import TaskAnnotation
from src.dag_task_node import _CachedResultWrapper, DAGTaskNode, DAGTaskNodeId
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.events import TASK_COMPLETION_EVENT_PREFIX
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, TaskInputMetrics
from src.storage.storage import Storage
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size

@dataclass
class PreLoad(TaskAnnotation):
    """ Indicates that the upstream dependencies of a task annotated with this annotation should be downloaded as soon as possible """

    # for upstream tasks
    preloading_complete_events: dict[str, asyncio.Event] = {}
    # Flag that indicates if starting new preloading for upstream tasks of this task is allowed or not
    allow_new_preloads = True
    
    _onworker_ready_executed: ClassVar[bool] = False
    _classlock: ClassVar[asyncio.Lock] = asyncio.Lock()
    _instancelock: asyncio.Lock = asyncio.Lock()

    def __init__(self, task: DAGTaskNode) -> None:
        self.task = task

    def on_preload_task_completed_builder(self, intermediate_storage: Storage, dag: dag.FullDAG):
        async def _callback(_: dict):
            async with self._instancelock:
                if not self.allow_new_preloads: return
                self.preloading_complete_events[self.task.id.get_full_id()] = asyncio.Event()
            
            task_output = cloudpickle.loads(await intermediate_storage.get(self.task.id.get_full_id_in_dag(dag)))
            # Store the result so that its visible to other coroutines
            dag.get_node_by_id(self.task.id).cached_result = _CachedResultWrapper(task_output)
            
            async with self._instancelock:
                self.preloading_complete_events[self.task.id.get_full_id()].set()
        return _callback

    @staticmethod
    async def override_on_worker_ready(intermediate_storage: Storage, task: DAGTaskNode, dag: dag.FullDAG, this_worker_id: str):
        # Only executes once, even if there are multiple tasks with this annotation
        async with PreLoad._classlock:
            if PreLoad._onworker_ready_executed: return
            PreLoad._onworker_ready_executed = True

        _nodes_to_visit = dag.root_nodes
        visited_nodes = set()
        while _nodes_to_visit:
            current_node = _nodes_to_visit.pop(0)
            if current_node.id.get_full_id() in visited_nodes: continue
            visited_nodes.add(current_node.id.get_full_id())
            for downstream_node in current_node.downstream_nodes:
                if downstream_node.id.get_full_id() not in visited_nodes: _nodes_to_visit.append(downstream_node)
            # My Logic
            if current_node.get_annotation(TaskWorkerResourceConfiguration).worker_id != this_worker_id: continue
            preload_annotation = current_node.try_get_annotation(PreLoad)
            if not preload_annotation: continue
            for unode in current_node.upstream_nodes:
                if unode.get_annotation(TaskWorkerResourceConfiguration).worker_id == this_worker_id: continue
                await intermediate_storage.subscribe(
                    f"{TASK_COMPLETION_EVENT_PREFIX}{unode.id.get_full_id_in_dag(dag)}", 
                    preload_annotation.on_preload_task_completed_builder(intermediate_storage, dag, unode.id)
                )

    async def override_handle_inputs(self, intermediate_storage: Storage, task: DAGTaskNode, subdag: dag.SubDAG, worker_resource_config: TaskWorkerResourceConfiguration | None) -> tuple[dict[str, Any], list[TaskInputMetrics], float]:
        task_dependencies: dict[str, Any] = {}
        _input_metrics: list[TaskInputMetrics] = []
        dependency_download_timer = Timer()
        upstream_tasks_not_preloading_nor_cached = []
        
        __tasks_preloading_coroutines: dict[str, asyncio.Task] = {}
        async with self._instancelock:
            self.allow_new_preloads = False
            for utask_id, preloading_event in self.preloading_complete_events.items():
                __tasks_preloading_coroutines[utask_id] = asyncio.create_task(preloading_event.wait())

        for t in self.task.upstream_nodes:
            if t.cached_result is None and t.id.get_full_id() not in __tasks_preloading_coroutines:
                upstream_tasks_not_preloading_nor_cached.append(t)
                
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

        # Fetch data that is not being preloaded
        fetch_coroutines = [_fetch_dependency_data(ut, subdag, intermediate_storage) for ut in upstream_tasks_not_preloading_nor_cached]
        results = await asyncio.gather(*fetch_coroutines)
        for task_id, data, metrics in results:
            task_dependencies[task_id] = data
            _input_metrics.append(metrics)
        
        # Wait for preloading to finish
        await asyncio.gather(*__tasks_preloading_coroutines.values())
        
        # Grab cached + preloaded data
        for t in self.task.upstream_nodes:
            if t.cached_result:
                if t.id.get_full_id() in __tasks_preloading_coroutines: continue
                task_dependencies[t.id.get_full_id()] = t.cached_result.result
                _input_metrics.append(
                    TaskInputMetrics(
                        task_id=t.id.get_full_id_in_dag(subdag),
                        size_bytes=calculate_data_structure_size(t.cached_result.result),
                        time_ms=0,
                        normalized_time_ms=0
                    )
                )

        _total_input_download_time_ms = dependency_download_timer.stop()
        return (task_dependencies, _input_metrics, _total_input_download_time_ms)

    def clone(self): return PreLoad(self.task)