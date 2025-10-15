import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable
import cloudpickle
from src.dag.dag import FullDAG, SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import _CachedResultWrapper, DAGTaskNode, DAGTaskNodeId
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.events import TASK_COMPLETED_EVENT_PREFIX
from src.storage.storage import Storage
from src.utils.coroutine_tags import COROTAG_PRELOAD
from src.storage.metadata.metrics_types import TaskInputDownloadMetrics
from src.utils.logger import create_logger
from src.utils.utils import calculate_data_structure_size_bytes
from src.utils.timer import Timer
from src.storage.metadata.metrics_types import TaskOptimizationMetrics

logger = create_logger(__name__)

@dataclass
class PreLoadOptimization(TaskOptimization):
    """ 
    Indicates that the upstream dependencies of a task annotated with this 
    annotation should be downloaded as soon as possible, in parallel. 
    """

    @dataclass
    class OptimizationMetrics(TaskOptimizationMetrics):
        preloaded: DAGTaskNodeId

    # for upstream tasks
    preloading_subscription_ids: dict[str, str] = field(default_factory=dict) # dependent task + upstream task -> subscription id
    # Flag that indicates if starting new preloading for upstream tasks of this task is allowed or not
    allow_new_preloads: bool = True
    
    # New state management for concurrent preloads
    # A lock to protect access to shared state like `allow_new_preloads` and `_active_preloads`
    _state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # A dictionary to track currently running preload tasks
    _active_preloads: dict[str, asyncio.Task] = field(default_factory=dict) # upstream task full_id -> Task

    @property
    def name(self): return "PreLoad"

    def clone(self): return PreLoadOptimization()

    @staticmethod
    def planning_assignment_logic(planner, dag: FullDAG, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]): 
        for node in topo_sorted_nodes:
            # Skip if node already has PreLoad annotation. Either added by this planner or the user
            if node.try_get_optimization(PreLoadOptimization): continue 
            
            resource_config: TaskWorkerResourceConfiguration = node.worker_config
            if resource_config.worker_id is None: continue # flexible workers can't have preload

            # Only apply preload to nodes that depend on at least 2 tasks from other workers
            if len([un for un in node.upstream_nodes if un.worker_config.worker_id is None or un.worker_config.worker_id != resource_config.worker_id]) >= 2:
                node.add_optimization(PreLoadOptimization())

    async def _start_preloading_if_not_running(
        self, 
        upstream_task: DAGTaskNode, 
        dependent_task: DAGTaskNode, 
        intermediate_storage: Storage, 
        metadata_storage: Storage, 
        dag: FullDAG
    ):
        async with self._state_lock:
            if not self.allow_new_preloads: return
            if upstream_task.cached_result is not None: return
            
            upstream_id = upstream_task.id.get_full_id()
            if upstream_id in self._active_preloads: return # Already preloading

            logger.info(f"[PRELOADING - QUEUED] Task: {upstream_id} for dependent: {dependent_task.id.get_full_id()}")

            # Create the task but don't await it here.
            preload_task = asyncio.create_task(
                self._perform_preloading(upstream_task, dependent_task, self, intermediate_storage, metadata_storage, dag),
                name=f"{COROTAG_PRELOAD}_{upstream_id}"
            )
            self._active_preloads[upstream_id] = preload_task
    
    @staticmethod
    async def _perform_preloading(
        upstream_task: DAGTaskNode, 
        dependent_task: DAGTaskNode, 
        annotation: 'PreLoadOptimization', 
        intermediate_storage: Storage, 
        metadata_storage: Storage, 
        dag: FullDAG
    ):
        """ The core logic for downloading a single dependency. Runs concurrently for multiple dependencies. """
        upstream_id_in_dag = upstream_task.id.get_full_id_in_dag(dag)
        upstream_full_id = upstream_task.id.get_full_id()
        
        # Unsubscribe from the completion event since we are now handling it.
        # This is safe to do outside the lock.
        subscription_key = f"{dependent_task.id.get_full_id()}{upstream_full_id}"
        subscription_id = annotation.preloading_subscription_ids.pop(subscription_key, None)
        if subscription_id:
            await metadata_storage.unsubscribe(f"{TASK_COMPLETED_EVENT_PREFIX}{upstream_id_in_dag}", subscription_id)

        try:
            if upstream_task.cached_result is not None: return # Double-check

            logger.info(f"[PRELOADING - STARTED] Task: {upstream_full_id}")
            dependent_task.metrics.optimization_metrics.append(PreLoadOptimization.OptimizationMetrics(preloaded=upstream_task.id))

            _timer = Timer()
            serialized_data: Any = await intermediate_storage.get(upstream_id_in_dag)
            time_to_fetch_ms = _timer.stop()
            deserialized_task_output = cloudpickle.loads(serialized_data)
            upstream_task.cached_result = _CachedResultWrapper(deserialized_task_output)

            dependent_task.metrics.input_metrics.input_download_metrics[
                upstream_full_id
            ] = TaskInputDownloadMetrics(
                serialized_size_bytes=calculate_data_structure_size_bytes(serialized_data),
                time_ms=time_to_fetch_ms
            )

            logger.info(f"[PRELOADING - DONE] Task: {upstream_full_id}")

        except Exception as e:
            logger.error(f"[PRELOADING - FAILED] Task: {upstream_full_id}. Error: {e}")
        finally:
            # Always remove the task from the active list when done.
            async with annotation._state_lock:
                annotation._active_preloads.pop(upstream_full_id, None)

    @staticmethod
    async def wel_on_worker_ready(worker, dag: FullDAG, this_worker_id: str | None):
        from src.workers.worker import Worker
        _worker: Worker = worker
        
        if this_worker_id is None: 
            return # Flexible workers can't look ahead for their tasks to see if they have preload

        def _on_preload_task_completed_builder(dependent_task: DAGTaskNode, upstream_task: DAGTaskNode, annotation: PreLoadOptimization, intermediate_storage: Storage, metadata_storage: Storage, dag: FullDAG):
            async def _callback(_: dict, subscription_id: str | None = None):
                await annotation._start_preloading_if_not_running(upstream_task, dependent_task, intermediate_storage, metadata_storage, dag)
            return _callback

        _nodes_to_visit = dag.root_nodes
        visited_nodes = set()
        while _nodes_to_visit:
            current_node = _nodes_to_visit.pop(0)
            if current_node.id.get_full_id() in visited_nodes: continue
            visited_nodes.add(current_node.id.get_full_id())
            
            for downstream_node in current_node.downstream_nodes:
                if downstream_node.id.get_full_id() not in visited_nodes: _nodes_to_visit.append(downstream_node)
            
            if current_node.worker_config.worker_id != this_worker_id: continue
            
            preload_optimization = current_node.try_get_optimization(PreLoadOptimization)
            if not preload_optimization: continue
            
            for unode in current_node.upstream_nodes:
                if unode.worker_config.worker_id == this_worker_id: continue

                if await _worker.intermediate_storage.exists(unode.id.get_full_id_in_dag(dag)):
                    logger.info(f"[PRELOADING - ALREADY EXISTS] Task: {unode.id.get_full_id()} | Dependent task: {current_node.id.get_full_id()}")
                    # Use the helper to start the preload immediately
                    asyncio.create_task(preload_optimization._start_preloading_if_not_running(unode, current_node, _worker.intermediate_storage, _worker.metadata_storage.storage, dag))
                else:
                    subscription_id = await _worker.metadata_storage.storage.subscribe(
                        f"{TASK_COMPLETED_EVENT_PREFIX}{unode.id.get_full_id_in_dag(dag)}", 
                        _on_preload_task_completed_builder(current_node, unode, preload_optimization, _worker.intermediate_storage, _worker.metadata_storage.storage, dag),
                        coroutine_tag=COROTAG_PRELOAD
                    )
                    logger.info(f"[PRELOADING - SUBSCRIBED] Task: {unode.id.get_full_id()} | Dependent task: {current_node.id.get_full_id()}")
                    preload_optimization.preloading_subscription_ids[f"{current_node.id.get_full_id()}{unode.id.get_full_id()}"] = subscription_id

    @staticmethod
    async def wel_override_handle_inputs(worker, task: DAGTaskNode, subdag: SubDAG, upstream_tasks_without_cached_results: list) -> tuple[list, list[str], Awaitable[Any] | None]:
        from src.workers.worker import Worker
        _worker: Worker = worker

        upstream_tasks_to_fetch = []
        wait_coro = None
        
        preload_optimization = task.try_get_optimization(PreLoadOptimization)
        if preload_optimization:
            tasks_to_wait_for = []
            async with preload_optimization._state_lock:
                # Stop any new preloads from starting
                logger.info(f"[PRELOAD - HANDLE_INPUTS] No more preloading allowed for {task.id.get_full_id()}")
                preload_optimization.allow_new_preloads = False
                # Get a snapshot of currently running preload tasks for this optimization instance
                tasks_to_wait_for = list(preload_optimization._active_preloads.values())

            if tasks_to_wait_for:
                logger.info(f"[PRELOAD - HANDLE_INPUTS] Waiting for {len(tasks_to_wait_for)} active preloads to complete...")
                # Create a coroutine that waits for all active preloads to finish
                wait_coro = asyncio.gather(*tasks_to_wait_for)

        # This part of the logic runs after the wait_coro (if any) is awaited by the caller.
        # Here, we determine which inputs are *still* missing after preloading has finished.
        for t in task.upstream_nodes:
            subscription_id = preload_optimization.preloading_subscription_ids.get(f"{task.id.get_full_id()}{t.id.get_full_id()}") if preload_optimization else None
            if subscription_id is not None:
                # Unsubscribe from any remaining events; they are no longer needed.
                await _worker.metadata_storage.storage.unsubscribe(f"{TASK_COMPLETED_EVENT_PREFIX}{t.id.get_full_id_in_dag(subdag)}", subscription_id=subscription_id)

            if not t.cached_result:
                logger.info(f"[HANDLE_INPUTS - NEED FETCHING] Task: {t.id.get_full_id()} | Dependent task: {task.id.get_full_id()}")
                upstream_tasks_to_fetch.append(t)

        return (
            upstream_tasks_to_fetch,
            [], 
            wait_coro
        )