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

class SecondAlgorithm(DAGPlanner, WorkerExecutionLogic):
    @dataclass
    class Config(DAGPlanner.Config):
        available_worker_resource_configurations: list[TaskWorkerResourceConfiguration]

        def __post_init__(self):
            """
            Sort the available_resource_configurations by memory_mb
            Best config first
            """
            self.available_worker_resource_configurations.sort(key=lambda x: x.memory_mb, reverse=True)

        def create_instance(self) -> "SecondAlgorithm":
            return SecondAlgorithm(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def plan(self, dag, metadata_access: MetadataAccess):
        """
        The second algorithm should use non-uniform workers. It would first find the critical path by predicting
        times of all workflow tasks running on the best worker configuration. Then, it would downgrade resources
        on the other paths as much as possible without introducing a new critical path. After attributing tasks to workers
        (at plan-time, not run-time), this algorithm would then simulate using optimizations to further improve resource
        efficiency and reduce makespan.

        - Assign best resources to all tasks, find CP
        - Simulate downgrading resources on non-critical path tasks (w/o introducing a new CP)
        - Simulate using optimizations
        """
        
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        topo_sorted_nodes = self._topological_sort(dag)

        if len(self.config.available_worker_resource_configurations) == 1:
            # If only one resource config is available, use it for all nodes
            for node in topo_sorted_nodes:
                unique_resources = self.config.available_worker_resource_configurations[0].clone()
                node.add_annotation(unique_resources)
                if len(node.upstream_nodes) == 0:
                    unique_resources.worker_id = uuid.uuid4().hex
                else:
                    unique_resources.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id
            self._visualize_plan(dag)
            return

        middle_resource_config = self.config.available_worker_resource_configurations[len(self.config.available_worker_resource_configurations) // 2]
        
        if not metadata_access.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving intermediate resources ({middle_resource_config}) to all nodes")
            for node in topo_sorted_nodes: 
                unique_resources = middle_resource_config.clone()
                node.add_annotation(unique_resources)
                if len(node.upstream_nodes) == 0:
                    unique_resources.worker_id = uuid.uuid4().hex
                else:
                    unique_resources.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id
            self._visualize_plan(dag)
            return
        
        algorithm_start_time = Timer()
        best_resource_config = self.config.available_worker_resource_configurations[0]
        
        # Step 1: Assign best resources to all nodes and find initial critical path
        logger.info("=== Step 1: Initial assignment with best resources ===")
        for node in topo_sorted_nodes:
            resource_config = best_resource_config.clone()
            node.add_annotation(resource_config)
            if len(node.upstream_nodes) == 0:
                resource_config.worker_id = uuid.uuid4().hex
            else:
                resource_config.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id

        # Calculate initial critical path with best resources
        nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, metadata_access, best_resource_config, self.config.sla)
        critical_path_nodes, critical_path_time = self._find_critical_path(dag, nodes_info)
        critical_path_node_ids = {node.id.get_full_id() for node in critical_path_nodes}
        
        logger.info(f"Initial Critical Path | Nodes: {len(critical_path_nodes)} | Node IDs: {[node.id.get_full_id() for node in critical_path_nodes]} | Time: {critical_path_time} ms")

        # Step 2: Downgrade resources on non-critical paths without introducing new critical path
        logger.info("=== Step 2: Downgrading resources on non-critical paths ===")
        nodes_outside_critical_path = [node for node in topo_sorted_nodes if node.id.get_full_id() not in critical_path_node_ids]
        successful_downgrades = 0
        
        for node in nodes_outside_critical_path:
            node_id = node.id.get_full_id()
            original_config = node.get_annotation(TaskWorkerResourceConfiguration)
            best_downgrade = None
            
            # Try each resource config from second best to worst (skip the best one which is already assigned)
            for resource_config in self.config.available_worker_resource_configurations[1:]:
                # Clone and try to reuse existing worker if possible
                test_resource_config = resource_config.clone()
                
                # Try to reuse worker from upstream nodes with same resource configuration
                reusable_worker_id_found = False
                for unode in node.upstream_nodes:
                    unode_resources = unode.get_annotation(TaskWorkerResourceConfiguration)
                    if (unode_resources.cpus == test_resource_config.cpus and 
                        unode_resources.memory_mb == test_resource_config.memory_mb):
                        test_resource_config.worker_id = unode_resources.worker_id
                        reusable_worker_id_found = True
                        break
                
                # If no reusable worker found, create new worker id
                if not reusable_worker_id_found:
                    test_resource_config.worker_id = uuid.uuid4().hex
                
                # Temporarily assign this resource config
                node.add_annotation(test_resource_config)
                
                # Recalculate timings with this configuration
                new_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
                new_critical_path_nodes, new_critical_path_time = self._find_critical_path(dag, new_nodes_info)
                
                # Check if this downgrade introduces a new critical path or increases makespan
                if new_critical_path_time <= critical_path_time:
                    best_downgrade = test_resource_config
                    # Continue to find the lowest resource config that doesn't hurt performance
                else:
                    # This downgrade hurts performance, revert and stop trying lower configs
                    break
            
            if best_downgrade:
                # note: no need to add annotation because it was added in the loop above
                successful_downgrades += 1
                logger.info(f"Downgraded node {node_id}: {original_config.memory_mb}MB -> {best_downgrade.memory_mb}MB")
            else:
                # Restore original config if no downgrade was beneficial
                node.add_annotation(original_config)

        logger.info(f"Successfully downgraded {successful_downgrades} out of {len(nodes_outside_critical_path)} non-critical path nodes")

        # Step 3: Apply preload optimizations to improve resource efficiency and reduce makespan
        logger.info("=== Step 3: Applying preload optimizations ===")
        iteration = 0
        total_preload_optimizations = 0
        while True:
            iteration += 1
            logger.info(f"=== Preload Optimization Iteration {iteration} ===")
            
            # Recalculate current critical path with current resource assignments
            current_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
            current_critical_path_nodes, current_critical_path_time = self._find_critical_path(dag, current_nodes_info)
            current_critical_path_node_ids = {node.id.get_full_id() for node in current_critical_path_nodes}
            
            logger.info(f"Current Critical Path | Nodes: {len(current_critical_path_nodes)} | Time: {current_critical_path_time} ms")

            # Try to optimize nodes in the current critical path with PreLoad
            optimized_any_node = False
            nodes_optimized_this_iteration = 0
            
            for node in current_critical_path_nodes:
                node_id = node.id.get_full_id()
                
                if node.try_get_annotation(PreLoad):
                    continue  # Skip if node already has PreLoad annotation
                    
                # Only try to apply preload to nodes that depend on > 1 tasks AND at least 1 of them is from different worker ids
                if (len(node.upstream_nodes) == 0 or 
                    len([un for un in node.upstream_nodes 
                        if un.get_annotation(TaskWorkerResourceConfiguration).worker_id != 
                            node.get_annotation(TaskWorkerResourceConfiguration).worker_id]) == 0):
                    continue

                logger.info(f"Trying to assign 'PreLoad' annotation to critical path node: {node_id}")
                
                # Add PreLoad annotation temporarily
                node.add_annotation(PreLoad())

                # Recalculate timings with this optimization
                new_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
                new_critical_path_nodes, new_critical_path_time = self._find_critical_path(dag, new_nodes_info)
                new_critical_path_node_ids = {node.id.get_full_id() for node in new_critical_path_nodes}

                # Check if optimization improved performance
                if new_critical_path_time < current_critical_path_time:
                    # Optimization helped - keep it
                    logger.info(f"PreLoad optimization successful for node {node_id}: {current_critical_path_time} -> {new_critical_path_time} ms")
                    optimized_any_node = True
                    nodes_optimized_this_iteration += 1
                    total_preload_optimizations += 1
                    
                    # Check if we introduced a new critical path (different set of nodes)
                    if current_critical_path_node_ids != new_critical_path_node_ids:
                        logger.info(f"New critical path introduced. Old: {current_critical_path_node_ids} | New: {new_critical_path_node_ids}")
                        break  # Start new iteration with the new critical path
                    else:
                        # Same critical path, continue optimizing it
                        current_critical_path_nodes = new_critical_path_nodes
                        current_critical_path_time = new_critical_path_time
                        current_critical_path_node_ids = new_critical_path_node_ids
                        continue
                else:
                    # Optimization didn't help, revert it
                    logger.info(f"PreLoad optimization not beneficial for node {node_id}: {current_critical_path_time} -> {new_critical_path_time} ms, reverting")
                    node.remove_annotation(PreLoad)

            logger.info(f"Optimized {nodes_optimized_this_iteration} nodes in iteration {iteration}")
            
            # If no optimization was applied in this iteration, we're done
            if not optimized_any_node:
                logger.info(f"No further optimizations possible on current critical path. Algorithm completed after {iteration} iterations.")
                break
            
            # If we optimized nodes but didn't introduce a new critical path, we're also done
            final_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
            final_critical_path_nodes, _ = self._find_critical_path(dag, final_nodes_info)
            final_critical_path_node_ids = {node.id.get_full_id() for node in final_critical_path_nodes}
            
            if current_critical_path_node_ids == final_critical_path_node_ids:
                logger.info(f"Critical path unchanged after optimizations. Algorithm completed after {iteration} iterations.")
                break
                
            # Prevent infinite loops
            if iteration > 100:
                logger.warning(f"Maximum iterations reached. Stopping algorithm.")
                break

        # Final statistics and logging
        final_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, final_nodes_info)
        final_critical_path_node_ids = {node.id.get_full_id() for node in final_critical_path_nodes}
        
        # Calculate resource distribution
        resource_distribution = {}
        unique_worker_ids = {}
        nodes_with_preload = 0
        
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            config_key = f"CPU:{resource_config.cpus},Memory:{resource_config.memory_mb}MB"
            if config_key not in resource_distribution:
                resource_distribution[config_key] = 0
            resource_distribution[config_key] += 1
            
            if resource_config.worker_id not in unique_worker_ids:
                unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1
            
            if node.try_get_annotation(PreLoad):
                nodes_with_preload += 1

        logger.info(f"=== FINAL RESULTS ===")
        logger.info(f"FINAL CRITICAL PATH | Nodes: {len(final_critical_path_nodes)} | Node IDs: {[node.id.get_full_id() for node in final_critical_path_nodes]} | Predicted Completion Time: {final_critical_path_time} ms")
        logger.info(f"Total PreLoad optimizations applied: {total_preload_optimizations}")
        logger.info(f"Nodes with PreLoad annotation: {nodes_with_preload}")
        logger.info(f"Successfully downgraded resources: {successful_downgrades} nodes")
        logger.info(f"Task Resource distribution: {resource_distribution}")
        logger.info(f"Number of unique workers: {len(unique_worker_ids)}")
        logger.info(f"Algorithm completed in {algorithm_start_time.stop():.3f} ms after {iteration} iterations")

        # DEBUG: Plan Visualization
        self._visualize_plan(dag, final_nodes_info, final_critical_path_node_ids)
        self.validate_plan(_dag.root_nodes)
        # !!! FOR QUICK TESTING ONLY. REMOVE LATER !!!
        # exit()

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