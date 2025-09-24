import asyncio
from dataclasses import dataclass, field
from types import CoroutineType
from typing import Any
import cloudpickle
from src.dag.dag import FullDAG, SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import _CachedResultWrapper, DAGTaskNode
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.events import TASK_COMPLETED_EVENT_PREFIX
from src.storage.storage import Storage
from src.utils.coroutine_tags import COROTAG_PRELOAD
from src.storage.metrics.metrics_types import TaskInputDownloadMetrics
from src.utils.logger import create_logger
from src.utils.utils import calculate_data_structure_size_bytes
from src.utils.timer import Timer

logger = create_logger(__name__)

@dataclass
class PreLoadOptimization(TaskOptimization):
    """ Indicates that the upstream dependencies of a task annotated with this annotation should be downloaded as soon as possible """

    MIN_DEPENDENCIES_TO_APPLY_OPTIMIZATION = 10

    # for upstream tasks
    preloading_complete_events: dict[str, asyncio.Event] = field(default_factory=dict)
    # Flag that indicates if starting new preloading for upstream tasks of this task is allowed or not
    allow_new_preloads: bool = True
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def name(self): return "PreLoad"

    def clone(self): return PreLoadOptimization()

    @staticmethod
    def planning_assignment_logic(planner, dag: FullDAG, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]): 
        from src.planning.abstract_dag_planner import AbstractDAGPlanner
        from src.planning.predictions.predictions_provider import PredictionsProvider
        _planner: AbstractDAGPlanner = planner
        _predictions_provider: PredictionsProvider = predictions_provider
        iteration = 0

        for node in topo_sorted_nodes:
            if len(node.upstream_nodes) >= PreLoadOptimization.MIN_DEPENDENCIES_TO_APPLY_OPTIMIZATION:
                node.add_optimization(PreLoadOptimization())

        while True:
            iteration += 1
            
            # Calculate current node timings and find critical path
            updated_nodes_info = _planner._calculate_workflow_timings(topo_sorted_nodes, _predictions_provider, _planner.config.sla)
            critical_path_nodes, critical_path_time = _planner._find_critical_path(dag, updated_nodes_info)
            initial_critical_path_node_ids = { node.id.get_full_id() for node in critical_path_nodes }
            
            # logger.info(f"CRITICAL PATH | Nodes: {len(critical_path_nodes)} | Node IDs: {[node.id.get_full_id() for node in critical_path_nodes]} | Predicted Completion Time: {critical_path_time} ms")

            # Try to optimize nodes in the current critical path with PreLoad
            nodes_optimized_this_iteration = 0
            
            for node in critical_path_nodes:
                if node.try_get_optimization(PreLoadOptimization): 
                    # Skip if node already has PreLoad annotation. Either added by this planner or the user
                    continue 
                
                resource_config: TaskWorkerResourceConfiguration = node.worker_config
                if resource_config.worker_id is None: continue # flexible workers can't have preload

                # Only apply preload to nodes that depend on > 1 tasks AND at least 1 of them is from different worker id
                if len(node.upstream_nodes) == 0 or len([un for un in node.upstream_nodes if un.worker_config.worker_id is None or un.worker_config.worker_id != resource_config.worker_id]) == 0:
                    continue

                # logger.info(f"Trying to assign 'PreLoad' annotation to critical path node: {node_id}")
                
                # Add PreLoad annotation temporarily
                node.add_optimization(PreLoadOptimization())

                # Recalculate timings with this optimization
                updated_nodes_info = _planner._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, _planner.config.sla)
                new_critical_path_nodes, new_critical_path_time = _planner._find_critical_path(dag, updated_nodes_info)
                new_critical_path_node_ids = { node.id.get_full_id() for node in new_critical_path_nodes }

                # Check if optimization improved performance
                if new_critical_path_time < critical_path_time:
                    # Optimization helped - keep it
                    nodes_optimized_this_iteration += 1
                    
                    # Check if we introduced a new critical path (different set of nodes)
                    if initial_critical_path_node_ids != new_critical_path_node_ids:
                        # logger.info(f"New critical path introduced. Old: {critical_path_node_ids} | New: {new_critical_path_node_ids}")
                        break  # Start new iteration with the new critical path
                    else:
                        # Same critical path, continue optimizing it
                        critical_path_nodes = new_critical_path_nodes
                        critical_path_time = new_critical_path_time
                        initial_critical_path_node_ids = new_critical_path_node_ids
                        continue
                else:
                    # Optimization didn't help, revert it
                    node.remove_optimization(PreLoadOptimization)

            # logger.info(f"Optimized {nodes_optimized_this_iteration} nodes in iteration {iteration}")
            
            # If no optimization was applied in this iteration, we're done
            if nodes_optimized_this_iteration == 0:
                # logger.info(f"No further optimizations possible on current critical path. Algorithm completed after {iteration} iterations.")
                break
            
            # If we optimized nodes but didn't introduce a new critical path, we're also done
            # (this happens when we've optimized all optimizable nodes in the current critical path)
            updated_nodes_info = _planner._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, _planner.config.sla)
            current_critical_path_nodes, _ = _planner._find_critical_path(dag, updated_nodes_info)
            current_critical_path_node_ids = { node.id.get_full_id() for node in current_critical_path_nodes }
            
            if initial_critical_path_node_ids == current_critical_path_node_ids:
                # logger.info(f"Critical path unchanged after optimizations. Algorithm completed after {iteration} iterations.")
                break
                
            # Prevent infinite loops
            if iteration > 100:
                logger.warning(f"Maximum iterations reached. Stopping algorithm.")
                break
        return

    @staticmethod
    async def wel_on_worker_ready(planner, intermediate_storage: Storage, dag: FullDAG, this_worker_id: str | None):
        if this_worker_id is None: 
            return # Flexible workers can't look ahead for their tasks to see if they have preload

        # Only executes once, even if there are multiple tasks with this annotation
        def _on_preload_task_completed_builder(dependent_task: DAGTaskNode, upstream_task: DAGTaskNode, annotation: PreLoadOptimization, intermediate_storage: Storage, dag: FullDAG):
            async def _callback(_: dict, subscription_id: str | None = None):
                async with annotation._lock:
                    if subscription_id is not None:
                        await intermediate_storage.unsubscribe(f"{TASK_COMPLETED_EVENT_PREFIX}{upstream_task.id.get_full_id_in_dag(dag)}", subscription_id)
                    if not annotation.allow_new_preloads: return
                    annotation.preloading_complete_events[upstream_task.id.get_full_id()] = asyncio.Event()
                logger.info(f"[PRELOADING - STARTED] Task: {upstream_task.id.get_full_id()}")
                
                _timer = Timer()
                serialized_data = await intermediate_storage.get(upstream_task.id.get_full_id_in_dag(dag))
                time_to_fetch_ms = _timer.stop()
                deserialized_task_output = cloudpickle.loads(serialized_data)
                dependent_task.metrics.input_metrics.input_download_metrics[upstream_task.id.get_full_id()] = TaskInputDownloadMetrics(
                    serialized_size_bytes=calculate_data_structure_size_bytes(serialized_data),
                    deserialized_size_bytes=calculate_data_structure_size_bytes(deserialized_task_output),
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
            if current_node.worker_config.worker_id != this_worker_id: continue
            preload_optimization = current_node.try_get_optimization(PreLoadOptimization)
            # 2) that should preload
            if not preload_optimization: continue
            for unode in current_node.upstream_nodes:
                if unode.worker_config.worker_id == this_worker_id: continue
                logger.info(f"[PRELOADING - SUBSCRIBING] Task: {unode.id.get_full_id()} | Dependent task: {current_node.id.get_full_id()}")
                await intermediate_storage.subscribe(
                    f"{TASK_COMPLETED_EVENT_PREFIX}{unode.id.get_full_id_in_dag(dag)}", 
                    _on_preload_task_completed_builder(current_node, unode, preload_optimization, intermediate_storage, dag),
                    coroutine_tag=COROTAG_PRELOAD
                )

    @staticmethod
    async def wel_override_handle_inputs(planner, intermediate_storage: Storage, task: DAGTaskNode, subdag: SubDAG, upstream_tasks_without_cached_results: list, worker_resource_config: TaskWorkerResourceConfiguration | None, task_dependencies: dict[str, Any]) -> tuple[list, list[str], CoroutineType | None]:
        """
        returns (
            tasks_to_fetch (on default implementation, fetch ALL tasks that don't have cached results),
            wait_until_coroutine (so that the caller can fetch the tasks in parallel)
        )
        """
        upstream_tasks_to_fetch = []
        
        preload_optimization = task.try_get_optimization(PreLoadOptimization)
        __tasks_preloading_coroutines: dict[str, CoroutineType] = {}
        if preload_optimization:
            async with preload_optimization._lock:
                preload_optimization.allow_new_preloads = False
                for utask_id, preloading_event in preload_optimization.preloading_complete_events.items():
                    logger.info(f"[HANDLE_INPUTS - IS PRELOADING] Task: {utask_id} | Dependent task: {task.id.get_full_id()}")
                    __tasks_preloading_coroutines[utask_id] = preloading_event.wait()

        for t in task.upstream_nodes:
            if t.cached_result:
                await intermediate_storage.unsubscribe(f"{TASK_COMPLETED_EVENT_PREFIX}{t.id.get_full_id_in_dag(subdag)}", subscription_id=None)
            elif t.cached_result is None and t.id.get_full_id() not in __tasks_preloading_coroutines:
                logger.info(f"[HANDLE_INPUTS - NEED FETCHING] Task: {t.id.get_full_id()} | Dependent task: {task.id.get_full_id()}")
                # unsubscribe because we are going to fetch it, in the future it won't matter
                await intermediate_storage.unsubscribe(f"{TASK_COMPLETED_EVENT_PREFIX}{t.id.get_full_id_in_dag(subdag)}", subscription_id=None)
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
