import asyncio
from dataclasses import dataclass, field
from types import CoroutineType
from typing import Any
import cloudpickle
from src.dag.dag import FullDAG, SubDAG
from src.dag_task_annotation import TaskAnnotation
from src.dag_task_node import _CachedResultWrapper, DAGTaskNode
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.events import TASK_COMPLETION_EVENT_PREFIX
from src.storage.storage import Storage
from src.utils.coroutine_tags import COROTAG_PRELOAD
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.storage.metrics.metrics_types import TaskInputDownloadMetrics
from src.utils.logger import create_logger
from src.utils.utils import calculate_data_structure_size
from src.utils.timer import Timer

logger = create_logger(__name__)

@dataclass
class PreLoadOptimization(TaskAnnotation, WorkerExecutionLogic):
    """ Indicates that the upstream dependencies of a task annotated with this annotation should be downloaded as soon as possible """

    # for upstream tasks
    preloading_complete_events: dict[str, asyncio.Event] = field(default_factory=dict)
    # Flag that indicates if starting new preloading for upstream tasks of this task is allowed or not
    allow_new_preloads: bool = True
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def clone(self): return PreLoadOptimization()

    @staticmethod
    async def override_on_worker_ready(intermediate_storage: Storage, dag: FullDAG, this_worker_id: str | None):
        if this_worker_id is None: 
            return # Flexible workers can't look ahead for their tasks to see if they have preload

        # Only executes once, even if there are multiple tasks with this annotation
        def _on_preload_task_completed_builder(dependent_task: DAGTaskNode, upstream_task: DAGTaskNode, annotation: PreLoadOptimization, intermediate_storage: Storage, dag: FullDAG):
            async def _callback(_: dict):
                async with annotation._lock:
                    await intermediate_storage.unsubscribe(f"{TASK_COMPLETION_EVENT_PREFIX}{upstream_task.id.get_full_id_in_dag(dag)}")
                    if not annotation.allow_new_preloads: return
                    annotation.preloading_complete_events[upstream_task.id.get_full_id()] = asyncio.Event()
                logger.info(f"[PRELOADING - STARTED] Task: {upstream_task.id.get_full_id()}")
                
                _timer = Timer()
                serialized_data = await intermediate_storage.get(upstream_task.id.get_full_id_in_dag(dag))
                time_to_fetch_ms = _timer.stop()
                deserialized_task_output = cloudpickle.loads(serialized_data)
                dependent_task.metrics.input_metrics.input_download_metrics[upstream_task.id.get_full_id()] = TaskInputDownloadMetrics(
                    serialized_size_bytes=calculate_data_structure_size(serialized_data),
                    deserialized_size_bytes=calculate_data_structure_size(deserialized_task_output),
                    time_ms=time_to_fetch_ms
                )

                # Store the result so that its visible to other coroutines
                dag.get_node_by_id(upstream_task.id).cached_result = _CachedResultWrapper(deserialized_task_output)
                
                async with annotation._lock:
                    annotation.preloading_complete_events[upstream_task.id.get_full_id()].set()
                    logger.info(f"[PRELOADING - DONE] Task: {upstream_task.id.get_full_id()}")

            return _callback

        _nodes_to_visit = dag.root_nodes
        visited_nodes = set()
        while _nodes_to_visit:
            current_node = _nodes_to_visit.pop(0)
            if current_node.id.get_full_id() in visited_nodes: continue
            visited_nodes.add(current_node.id.get_full_id())
            for downstream_node in current_node.downstream_nodes:
                if downstream_node.id.get_full_id() not in visited_nodes: _nodes_to_visit.append(downstream_node)
            # MY Logic
            # 1) tasks assigned to me
            if current_node.get_annotation(TaskWorkerResourceConfiguration).worker_id != this_worker_id: continue
            preload_annotation = current_node.try_get_annotation(PreLoadOptimization)
            # 2) that should preload
            if not preload_annotation: continue
            for unode in current_node.upstream_nodes:
                if unode.get_annotation(TaskWorkerResourceConfiguration).worker_id == this_worker_id: continue
                logger.info(f"[PRELOADING - SUBSCRIBING] Task: {unode.id.get_full_id()} | Dependent task: {current_node.id.get_full_id()}")
                await intermediate_storage.subscribe(
                    f"{TASK_COMPLETION_EVENT_PREFIX}{unode.id.get_full_id_in_dag(dag)}", 
                    _on_preload_task_completed_builder(current_node, unode, preload_annotation, intermediate_storage, dag),
                    coroutine_tag=COROTAG_PRELOAD
                )

    @staticmethod
    async def override_handle_inputs(intermediate_storage: Storage, task: DAGTaskNode, subdag: SubDAG, upstream_tasks_without_cached_results: list, worker_resource_config: TaskWorkerResourceConfiguration | None, task_dependencies: dict[str, Any]) -> tuple[list, list[str], CoroutineType | None]:
        """
        returns (
            tasks_to_fetch (on default implementation, fetch ALL tasks that don't have cached results),
            wait_until_coroutine (so that the caller can fetch the tasks in parallel)
        )
        """
        upstream_tasks_to_fetch = []
        
        preload_annotation = task.try_get_annotation(PreLoadOptimization)
        __tasks_preloading_coroutines: dict[str, CoroutineType] = {}
        if preload_annotation:
            async with preload_annotation._lock:
                preload_annotation.allow_new_preloads = False
                for utask_id, preloading_event in preload_annotation.preloading_complete_events.items():
                    logger.info(f"[HANDLE_INPUTS - IS PRELOADING] Task: {utask_id} | Dependent task: {task.id.get_full_id()}")
                    __tasks_preloading_coroutines[utask_id] = preloading_event.wait()

        for t in task.upstream_nodes:
            if t.cached_result:
                await intermediate_storage.unsubscribe(f"{TASK_COMPLETION_EVENT_PREFIX}{t.id.get_full_id_in_dag(subdag)}")
            elif t.cached_result is None and t.id.get_full_id() not in __tasks_preloading_coroutines:
                logger.info(f"[HANDLE_INPUTS - NEED FETCHING] Task: {t.id.get_full_id()} | Dependent task: {task.id.get_full_id()}")
                # unsubscribe because we are going to fetch it, in the future it won't matter
                await intermediate_storage.unsubscribe(f"{TASK_COMPLETION_EVENT_PREFIX}{t.id.get_full_id_in_dag(subdag)}")
                upstream_tasks_to_fetch.append(t)

        async def _wait_all_preloads_coroutine():
            await asyncio.gather(*__tasks_preloading_coroutines.values()) # Wait for all preloading to finish for this task
            # Grab preloaded data results
            for t in task.upstream_nodes:
                if t.id.get_full_id() not in __tasks_preloading_coroutines: continue
                if not t.cached_result: raise Exception(f"ERROR: Task {t.id.get_full_id()} was preloading. After preload, it doesn't have a cached result!!")
                task_dependencies[t.id.get_full_id()] = t.cached_result.result

        upstream_tasks_i_fetch = list(__tasks_preloading_coroutines.keys())

        return (upstream_tasks_to_fetch, upstream_tasks_i_fetch, _wait_all_preloads_coroutine())
