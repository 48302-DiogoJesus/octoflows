import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, ClassVar
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

MAX_GLOBAL_CONCURRENT_PRELOADS = 4

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
    preloading_subscription_ids: dict[str, str] = field(default_factory=dict)
    allow_new_preloads: bool = True
    
    _state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _active_preloads: dict[str, asyncio.Task] = field(default_factory=dict)

    _global_semaphore: ClassVar[asyncio.Semaphore | None] = None

    @classmethod
    def get_semaphore(cls) -> asyncio.Semaphore:
        if cls._global_semaphore is None:
            cls._global_semaphore = asyncio.Semaphore(MAX_GLOBAL_CONCURRENT_PRELOADS)
        return cls._global_semaphore

    @property
    def name(self): return "PreLoad"

    def clone(self): return PreLoadOptimization()

    @staticmethod
    def planning_assignment_logic(planner, dag: FullDAG, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]): 
        for node in topo_sorted_nodes:
            if node.try_get_optimization(PreLoadOptimization): continue 
            
            resource_config: TaskWorkerResourceConfiguration = node.worker_config
            if resource_config.worker_id is None: continue 

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
            
            upstream_id = upstream_task.id.get_internal_id()
            if upstream_id in self._active_preloads: return 
            logger.info(f"[PRELOADING - QUEUED] Task: {upstream_id} for dependent: {dependent_task.id.get_internal_id()}")

            self._active_preloads[upstream_id] = asyncio.create_task(
                self._perform_preloading(upstream_task, dependent_task, self, intermediate_storage, metadata_storage, dag),
                name=f"{COROTAG_PRELOAD}_{upstream_id}"
            )
    
    @staticmethod
    async def _perform_preloading(
        upstream_task: DAGTaskNode, 
        dependent_task: DAGTaskNode, 
        annotation: 'PreLoadOptimization', 
        intermediate_storage: Storage, 
        metadata_storage: Storage, 
        dag: FullDAG
    ):
        upstream_id_in_dag = upstream_task.id.get_remote_id(dag)
        upstream_full_id = upstream_task.id.get_internal_id()
        
        # fire-and-forget unsubscribe
        subscription_key = f"{dependent_task.id.get_internal_id()}{upstream_full_id}"
        subscription_id = annotation.preloading_subscription_ids.pop(subscription_key, None)
        if subscription_id: asyncio.create_task(metadata_storage.unsubscribe(f"{TASK_COMPLETED_EVENT_PREFIX}{upstream_id_in_dag}", subscription_id))

        try:
            sem = PreLoadOptimization.get_semaphore()
            async with sem:
                # Re-check conditions after acquiring semaphore (we might have waited a long time)
                if upstream_task.cached_result is not None: return 
                if not annotation.allow_new_preloads: return 

                logger.info(f"[PRELOADING - STARTED] Task: {upstream_full_id}")
                
                _timer = Timer() 
                serialized_data: Any = await intermediate_storage.get(upstream_id_in_dag)
                time_to_fetch_ms = _timer.stop()
                deserialized_task_output = await asyncio.to_thread(cloudpickle.loads, serialized_data)
                upstream_task.cached_result = _CachedResultWrapper(deserialized_task_output)

                dependent_task.metrics.optimization_metrics.append(PreLoadOptimization.OptimizationMetrics(preloaded=upstream_task.id))
                dependent_task.metrics.input_metrics.input_download_metrics[upstream_full_id] = TaskInputDownloadMetrics(
                    serialized_size_bytes=calculate_data_structure_size_bytes(serialized_data),
                    time_ms=time_to_fetch_ms
                )

                logger.info(f"[PRELOADING - DONE] Task: {upstream_full_id}")
        except Exception as e:
            logger.error(f"[PRELOADING - FAILED] Task: {upstream_full_id}. Error: {e}")
        finally:
            async with annotation._state_lock:
                annotation._active_preloads.pop(upstream_full_id, None)

    @staticmethod
    async def wel_on_worker_ready(worker, dag: FullDAG):
        from src.workers.worker import Worker
        _worker: Worker = worker
        
        if _worker.is_flex(): return 

        def _on_preload_task_completed_builder(dependent_task: DAGTaskNode, upstream_task: DAGTaskNode, annotation: PreLoadOptimization, intermediate_storage: Storage, metadata_storage: Storage, dag: FullDAG):
            async def _callback(_: dict, subscription_id: str | None = None):
                await annotation._start_preloading_if_not_running(upstream_task, dependent_task, intermediate_storage, metadata_storage, dag)
            return _callback

        candidates_to_check = []
        
        _nodes_to_visit = dag.root_nodes[:]
        visited_nodes = set()
        
        while _nodes_to_visit:
            current_node = _nodes_to_visit.pop(0)
            if current_node.id.get_internal_id() in visited_nodes: continue
            visited_nodes.add(current_node.id.get_internal_id())
            
            for downstream_node in current_node.downstream_nodes:
                if downstream_node.id.get_internal_id() not in visited_nodes: 
                    _nodes_to_visit.append(downstream_node)
            
            # Skip tasks not for me
            if current_node.worker_config.worker_id != _worker.my_resource_configuration.worker_id: continue
            # Skip tasks that are already done (if the node tracks this state locally)
            if current_node.cached_result is not None: continue 

            preload_optimization = current_node.try_get_optimization(PreLoadOptimization)
            if not preload_optimization: continue
            
            for unode in current_node.upstream_nodes:
                if unode.worker_config.worker_id == _worker.my_resource_configuration.worker_id: continue
                candidates_to_check.append((current_node, unode, preload_optimization))

        if not candidates_to_check: return

        keys_to_check = [unode.id.get_remote_id(dag) for _, unode, _ in candidates_to_check]
        existence_results = await _worker.intermediate_storage.exists_many(keys_to_check)
        
        for (current_node, unode, preload_opt), exists in zip(candidates_to_check, existence_results):
            if exists:
                logger.info(f"[PRELOADING - ALREADY EXISTS] Task: {unode.id.get_internal_id()}")
                asyncio.create_task(preload_opt._start_preloading_if_not_running(
                    unode, current_node, _worker.intermediate_storage, _worker.metadata_storage.storage, dag
                ))
            else:
                subscription_id = await _worker.metadata_storage.storage.subscribe(
                    f"{TASK_COMPLETED_EVENT_PREFIX}{unode.id.get_remote_id(dag)}", 
                    _on_preload_task_completed_builder(current_node, unode, preload_opt, _worker.intermediate_storage, _worker.metadata_storage.storage, dag),
                    coroutine_tag=COROTAG_PRELOAD,
                    debug_worker_id=_worker.debug_worker_id
                )
                
                logger.info(f"[PRELOADING - SUBSCRIBED] Task: {unode.id.get_internal_id()}")
                preload_opt.preloading_subscription_ids[f"{current_node.id.get_internal_id()}{unode.id.get_internal_id()}"] = subscription_id

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
                logger.info(f"[PRELOAD - HANDLE_INPUTS] No more preloading allowed for {task.id.get_internal_id()}")
                preload_optimization.allow_new_preloads = False
                tasks_to_wait_for = list(preload_optimization._active_preloads.values())

            if tasks_to_wait_for:
                logger.info(f"[PRELOAD - HANDLE_INPUTS] Waiting for {len(tasks_to_wait_for)} active preloads to complete...")
                wait_coro = asyncio.gather(*tasks_to_wait_for)

        if preload_optimization:
            unsubscribe_tasks = []
            for t in task.upstream_nodes:
                subscription_id = preload_optimization.preloading_subscription_ids.get(f"{task.id.get_internal_id()}{t.id.get_internal_id()}")
                if subscription_id is not None:
                     unsubscribe_tasks.append(
                         _worker.metadata_storage.storage.unsubscribe(f"{TASK_COMPLETED_EVENT_PREFIX}{t.id.get_remote_id(subdag)}", subscription_id=subscription_id)
                     )
            
            if unsubscribe_tasks:
                async def _unsubscribe_background(): await asyncio.gather(*unsubscribe_tasks)
                asyncio.create_task(_unsubscribe_background())

        for t in task.upstream_nodes:
            if not t.cached_result:
                logger.info(f"[HANDLE_INPUTS - NEED FETCHING] Task: {t.id.get_internal_id()} | Dependent task: {task.id.get_internal_id()}")
                upstream_tasks_to_fetch.append(t)

        return (
            upstream_tasks_to_fetch,
            [], 
            wait_coro
        )