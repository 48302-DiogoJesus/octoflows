import asyncio
from dataclasses import dataclass
from types import CoroutineType
from typing import Any
import uuid

import cloudpickle

from src.dag.dag import FullDAG, SubDAG
from src.dag_task_node import _CachedResultWrapper, DAGTaskNode
from src.planning.annotations.preload import PreLoad
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.dag_planner import DAGPlanner
from src.planning.metadata_access.metadata_access import MetadataAccess
from src.storage.events import TASK_COMPLETION_EVENT_PREFIX
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB
from src.storage.metrics.metrics_types import TaskInputMetrics
from src.storage.storage import Storage
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size
from src.workers.worker_execution_logic import WorkerExecutionLogic

logger = create_logger(__name__, prefix="PLANNING")

class UniformWorkersPlanner(DAGPlanner, WorkerExecutionLogic):
    @dataclass
    class Config(DAGPlanner.Config):
        worker_resource_configuration: TaskWorkerResourceConfiguration

        def create_instance(self) -> "UniformWorkersPlanner":
            return UniformWorkersPlanner(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def plan(self, dag, metadata_access: MetadataAccess):
        """
        dag: dag.DAG

        This planning algorithm:
        - Assigns the best resource config to each node in the DAG
        - Finds the critical path
        - Simulates downgrading resource configs of tasks outside the critical path without affecting the critical path significantly

        """
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        topo_sorted_nodes = self._topological_sort(dag)

        if not metadata_access.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving uniform resources ({self.config.worker_resource_configuration}) to all nodes")
            # No Metadata recorded for previous runs of the same DAG structure => give intermediate resources to all nodes
            # Assign worker resources and ids
            for node in topo_sorted_nodes: 
                unique_resources = self.config.worker_resource_configuration.clone()
                node.add_annotation(unique_resources)
                if len(node.upstream_nodes) == 0:
                    # Give each root node a unique worker id
                    unique_resources.worker_id = uuid.uuid4().hex
                else:
                    # Use same worker id as its first upstream node
                    unique_resources.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id
            self._visualize_plan(dag)
            return
        
        logger.info(f"Starting DAG Planning Algorithm")
        
        algorithm_start_time = Timer()

        #* Give same resources to all nodes and reuse worker ids
        for node in topo_sorted_nodes:
            resource_config = self.config.worker_resource_configuration.clone()
            node.add_annotation(resource_config)
            if len(node.upstream_nodes) == 0:
                # Give each root node a unique worker id
                resource_config.worker_id = uuid.uuid4().hex
            else:
                # Use same worker id as its first upstream node
                resource_config.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id

        #* Initial planning with Best Resources for all nodes
        nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, metadata_access, self.config.worker_resource_configuration, self.config.sla)
        critical_path_nodes, critical_path_time = self._find_critical_path(dag, nodes_info)
        critical_path_node_ids = { node.id.get_full_id() for node in critical_path_nodes }
        
        logger.info(f"CRITICAL PATH | Nodes: {len(critical_path_nodes)} | Predicted Completion Time: {critical_path_time} ms")

        nodes_outside_critical_path = [node for node in topo_sorted_nodes if node.id.get_full_id() not in critical_path_node_ids]
        nodes_with_preload = 0
        preload_simulation_timer = Timer()
        #* Simulate downgrading resources for nodes NOT on the critical path without creating a new critical path
        for node in nodes_outside_critical_path:
            node_id = node.id.get_full_id()
            # Only TRY preload annotation to nodes that depend on > 1 tasks AND at least 1 of them is from different worker ids
            if len(node.upstream_nodes) == 0 or len([un for un in node.upstream_nodes if un.get_annotation(TaskWorkerResourceConfiguration).worker_id != node.get_annotation(TaskWorkerResourceConfiguration).worker_id]) == 0:
                continue

            node.add_annotation(PreLoad())

            # Recalculate timings with this optimization
            temp_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
            _, new_critical_path_time = self._find_critical_path(dag, temp_nodes_info)

            if new_critical_path_time != critical_path_time:
                node.remove_annotation(PreLoad) # REVERT: because this changes the critical path
            else:
                # print(f"Node: {node_id[-6:]} | Downgraded Resources: {original_config.memory_mb} => {node_to_resource_config[node_id].memory_mb}")
                nodes_with_preload += 1

        logger.info(f"Assigned the 'PreLoad' annotation to {nodes_with_preload} nodes in {preload_simulation_timer.stop():.3f} ms")
            
        unique_worker_ids = {}
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            if resource_config.worker_id not in unique_worker_ids: unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1

        logger.info(f"CRITICAL PATH | Nodes: {len(critical_path_nodes)} | Predicted Completion Time: {critical_path_time} ms")
        logger.info(f"Number of unique workers: {len(unique_worker_ids)}")
        logger.info(f"Completed in {algorithm_start_time.stop():.3f} ms")

        # DEBUG: Plan Visualization
        updated_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
        self._visualize_plan(dag, updated_nodes_info, critical_path_node_ids)
        self.validate_plan(_dag.root_nodes)
        # !!! FOR QUICK TESTING ONLY. REMOVE LATER !!!
        exit()

    @staticmethod
    async def override_on_worker_ready(intermediate_storage: Storage, dag: FullDAG, this_worker_id: str):
        # Only executes once, even if there are multiple tasks with this annotation
        def _on_preload_task_completed_builder(upstream_task: DAGTaskNode, annotation: PreLoad, intermediate_storage: Storage, dag: FullDAG):
            async def _callback(_: dict):
                async with annotation._lock:
                    if not annotation.allow_new_preloads: return
                    annotation.preloading_complete_events[upstream_task.id.get_full_id()] = asyncio.Event()
                
                logger.info(f"[PRELOADING - STARTED] Task: {upstream_task.id.get_full_id()}")

                task_output = cloudpickle.loads(await intermediate_storage.get(upstream_task.id.get_full_id_in_dag(dag)))
                # Store the result so that its visible to other coroutines
                dag.get_node_by_id(upstream_task.id).cached_result = _CachedResultWrapper(task_output)
                
                async with annotation._lock:
                    annotation.preloading_complete_events[upstream_task.id.get_full_id()].set()
                    logger.info(f"[PRELOADING - DONE] Task: {upstream_task.id.get_full_id()}")

                await intermediate_storage.unsubscribe(f"{TASK_COMPLETION_EVENT_PREFIX}{upstream_task.id.get_full_id_in_dag(dag)}")
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
            if current_node.get_annotation(TaskWorkerResourceConfiguration).worker_id != this_worker_id: continue
            preload_annotation = current_node.try_get_annotation(PreLoad)
            if not preload_annotation: continue
            for unode in current_node.upstream_nodes:
                if unode.get_annotation(TaskWorkerResourceConfiguration).worker_id == this_worker_id: continue
                logger.info(f"[PRELOADING - SUBSCRIBING] Task: {unode.id.get_full_id()} | Dependent task: {current_node.id.get_full_id()}")
                await intermediate_storage.subscribe(
                    f"{TASK_COMPLETION_EVENT_PREFIX}{unode.id.get_full_id_in_dag(dag)}", 
                    _on_preload_task_completed_builder(unode, preload_annotation, intermediate_storage, dag)
                )

    @staticmethod
    async def override_handle_inputs(intermediate_storage: Storage, task: DAGTaskNode, subdag: SubDAG, worker_resource_config: TaskWorkerResourceConfiguration | None) -> tuple[dict[str, Any], list[TaskInputMetrics], float]:
        task_dependencies: dict[str, Any] = {}
        _input_metrics: list[TaskInputMetrics] = []
        dependency_download_timer = Timer()
        upstream_tasks_to_fetch = []
        
        preload_annotation = task.try_get_annotation(PreLoad)
        __tasks_preloading_coroutines: dict[str, CoroutineType] = {}
        if preload_annotation:
            async with preload_annotation._lock:
                preload_annotation.allow_new_preloads = False
                for utask_id, preloading_event in preload_annotation.preloading_complete_events.items():
                    logger.info(f"[HANDLE_INPUTS - IS PRELOADING] Task: {utask_id} | Dependent task: {task.id.get_full_id()}")
                    __tasks_preloading_coroutines[utask_id] = preloading_event.wait()

        for t in task.upstream_nodes:
            if t.cached_result is None and t.id.get_full_id() not in __tasks_preloading_coroutines:
                logger.info(f"[HANDLE_INPUTS - NEED FETCHING] Task: {t.id.get_full_id()} | Dependent task: {task.id.get_full_id()}")
                # unsubscribe because we are going to fetch it, in the future it won't matter
                await intermediate_storage.unsubscribe(f"{TASK_COMPLETION_EVENT_PREFIX}{t.id.get_full_id_in_dag(subdag)}")
                upstream_tasks_to_fetch.append(t)
                
        async def _fetch_dependency_data(dependency_task, subdag, intermediate_storage):
            fotimer = Timer()
            task_output = await intermediate_storage.get(dependency_task.id.get_full_id_in_dag(subdag))
            if task_output is None: raise Exception(f"[ERROR] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
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
        fetch_coroutines = [_fetch_dependency_data(ut, subdag, intermediate_storage) for ut in upstream_tasks_to_fetch]
        results = await asyncio.gather(*fetch_coroutines)
        for task_id, data, metrics in results:
            task_dependencies[task_id] = data
            _input_metrics.append(metrics)
        
        # Wait for preloading to finish
        await asyncio.gather(*__tasks_preloading_coroutines.values())
        
        # Grab cached + preloaded data
        for t in task.upstream_nodes:
            if t.cached_result:
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