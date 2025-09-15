from dataclasses import dataclass
from types import CoroutineType
from typing import Any
import uuid

from src.dag.dag import FullDAG, SubDAG
from src.planning.annotations.preload import PreLoadOptimization
from src.planning.annotations.prewarm import PreWarmOptimization
from src.planning.annotations.taskdup import TaskDupOptimization, DUPPABLE_TASK_MAX_EXEC_TIME_MS, DUPPABLE_TASK_MAX_INPUT_SIZE
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.storage.storage import Storage
from src.utils.logger import create_logger

logger = create_logger(__name__, prefix="PLANNING")

class SecondPlannerAlgorithm(AbstractDAGPlanner):
    @dataclass
    class Config(AbstractDAGPlanner.Config):
        available_worker_resource_configurations: list[TaskWorkerResourceConfiguration]

        def __post_init__(self):
            """
            Sort the available_resource_configurations by memory_mb
            Best config first
            """
            self.available_worker_resource_configurations.sort(key=lambda x: x.memory_mb, reverse=True)

        def create_instance(self) -> "SecondPlannerAlgorithm": return SecondPlannerAlgorithm(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def get_description(self) -> str: 
        return \
            """
            The second algorithm should use non-uniform workers. It would first find the critical path by predicting
            times of all workflow tasks running on the best worker configuration. Then, it would downgrade resources
            on the other paths as much as possible without introducing a new critical path. After attributing tasks to workers
            (at plan-time, not run-time), it simulates using preload optimization to reduce makespan by masking dependency download time.

            - Assign best resources to all tasks, find CP
            - Simulate downgrading resources on non-critical path tasks (w/o introducing a new CP)
            - Simulate using optimizations (preload)
            - Uses task dupping optimization
            """

    def internal_plan(self, dag, predictions_provider: PredictionsProvider):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        topo_sorted_nodes = self._topological_sort(dag)

        if len(self.config.available_worker_resource_configurations) == 1:
            # If only one resource config is available, use it for all nodes
            for node in topo_sorted_nodes:
                unique_resources = self.config.available_worker_resource_configurations[0].clone()
                node.worker_config = unique_resources
                if len(node.upstream_nodes) == 0:
                    unique_resources.worker_id = uuid.uuid4().hex
                else:
                    unique_resources.worker_id = node.upstream_nodes[0].worker_config.worker_id
            self._store_plan_image(dag)
            # self._store_plan_as_json(dag)
            return

        middle_resource_config = self.config.available_worker_resource_configurations[len(self.config.available_worker_resource_configurations) // 2]
        
        if not predictions_provider.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving intermediate resources ({middle_resource_config}) to all nodes")
            for node in topo_sorted_nodes: 
                unique_resources = middle_resource_config.clone()
                node.worker_config = unique_resources
                unique_resources.worker_id = None # note: ALL workers will be "flexible"
            self._store_plan_image(dag)
            # self._store_plan_as_json(dag)
            return
        
        best_resource_config = self.config.available_worker_resource_configurations[0]
        
        # Step 1: Assign best resources to all nodes and find initial critical path
        # logger.info("=== Step 1: Initial assignment with best resources ===")
        for node in topo_sorted_nodes:
            resource_config = best_resource_config.clone()
            node.worker_config = resource_config
            
            # #!! for debugging pre-warm
            # resource_config.worker_id = uuid.uuid4().hex
            # continue #!! for debugging pre-warm
            
            if len(node.upstream_nodes) == 0:
                resource_config.worker_id = uuid.uuid4().hex
            else:
                # Count worker usage among downstream nodes of upstream nodes
                same_level_worker_usage = {}
                for upstream_node in node.upstream_nodes:
                    worker_id = upstream_node.worker_config.worker_id
                    if not worker_id: continue
                    same_level_worker_usage[worker_id] = 0

                for upstream_node in node.upstream_nodes:
                    # Get all downstream nodes of this upstream node
                    for downstream_node in upstream_node.downstream_nodes:
                        if downstream_node.id.get_full_id() == node.id.get_full_id(): continue
                        downstream_worker_id = downstream_node.worker_config.worker_id
                        if not downstream_worker_id: continue
                        same_level_worker_usage[downstream_worker_id] = same_level_worker_usage.get(downstream_worker_id, 0) + 1

                # Get the most used worker ID that doesn't exceed MAX_FAN_OUT_SIZE_W_SAME_WORKER
                best_worker_id = None
                best_usage = -1
                for worker_id, usage in same_level_worker_usage.items():
                    if usage > best_usage and usage < AbstractDAGPlanner.MAX_FAN_OUT_SIZE_W_SAME_WORKER:
                        best_worker_id = worker_id
                        best_usage = usage
                
                # If no suitable worker found, create a new one
                resource_config.worker_id = best_worker_id if best_worker_id else uuid.uuid4().hex

        # Calculate initial critical path with best resources
        updated_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, predictions_provider, best_resource_config, self.config.sla)
        critical_path_nodes, critical_path_time = self._find_critical_path(dag, updated_nodes_info)
        critical_path_node_ids = {node.id.get_full_id() for node in critical_path_nodes}
        
        # logger.info(f"Initial Critical Path | Nodes: {len(critical_path_nodes)} | Node IDs: {[node.id.get_full_id() for node in critical_path_nodes]} | Time: {critical_path_time} ms")

        # Step 2: Downgrade resources on non-critical paths without introducing new critical path
        # logger.info("=== Step 2: Downgrading resources on non-critical paths ===")
        worker_ids_outside_critical_path: set[str] = set()
        for node in topo_sorted_nodes:
            node_worker_id = node.worker_config.worker_id
            if not node_worker_id: continue
            if node.id.get_full_id() not in critical_path_node_ids and all(node_worker_id != cpnode.worker_config.worker_id for cpnode in critical_path_nodes):
                worker_ids_outside_critical_path.add(node_worker_id)

        nodes_outside_critical_path = [node for node in topo_sorted_nodes if node.id.get_full_id() not in critical_path_node_ids]
        successful_worker_resources_downgrades = 0

        # For each worker outside critical path, simulate downgrading resources without introducing a new critical path
        for worker_id in worker_ids_outside_critical_path:
            last_successful_configs: dict[str, TaskWorkerResourceConfiguration] = {}
            
            # Store initial configurations as the first "successful" state
            for node in nodes_outside_critical_path:
                if node.worker_config.worker_id == worker_id:
                    last_successful_configs[node.id.get_full_id()] = node.worker_config
            
            for simul_resource_config in self.config.available_worker_resource_configurations[1:]:
                new_res_config = simul_resource_config.clone()
                new_res_config.worker_id = worker_id # use same worker_id, just change the resource config
                
                for node in nodes_outside_critical_path:
                    if node.worker_config.worker_id != worker_id: continue
                    node.worker_config = new_res_config
                
                # Recalculate timings with this configuration
                new_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, predictions_provider, self.config.sla)
                new_critical_path_nodes, new_critical_path_time = self._find_critical_path(dag, new_nodes_info)
                
                # If downgrading doesn't change the critical path, allow it, else: reverse it
                if new_critical_path_time == critical_path_time:
                    successful_worker_resources_downgrades += 1
                    # Update last successful configs to current state
                    for node in nodes_outside_critical_path:
                        if node.worker_config.worker_id == worker_id:
                            last_successful_configs[node.id.get_full_id()] = node.worker_config
                else:
                    # This downgrade hurts performance, REVERT to last successful state and stop trying lower configs
                    for node in nodes_outside_critical_path:
                        if node.worker_config.worker_id != worker_id: continue
                        node.worker_config = last_successful_configs[node.id.get_full_id()]
                    break # try downgrading the next worker

        logger.info(f"Successfully downgraded {successful_worker_resources_downgrades} out of {len(nodes_outside_critical_path)} non-critical path nodes")

        # OPTIMIZATION: PRE-LOAD
        # logger.info("=== Step 3: Applying preload optimizations ===")
        iteration = 0
        total_preload_optimizations = 0
        while True:
            iteration += 1
            # logger.info(f"=== Preload Optimization Iteration {iteration} ===")
            
            # Recalculate current critical path with current resource assignments
            updated_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, predictions_provider, self.config.sla)
            current_critical_path_nodes, current_critical_path_time = self._find_critical_path(dag, updated_nodes_info)
            initial_critical_path_node_ids = {node.id.get_full_id() for node in current_critical_path_nodes}
            
            # logger.info(f"Current Critical Path | Nodes: {len(current_critical_path_nodes)} | Time: {current_critical_path_time} ms")

            # Try to optimize nodes in the current critical path with PreLoad
            nodes_optimized_this_iteration = 0
            
            for node in current_critical_path_nodes:
                if node.try_get_annotation(PreLoadOptimization):
                    # Skip if node already has PreLoad annotation. Either added by this planner or the user
                    continue 
                    
                resource_config = node.worker_config

                if resource_config.worker_id is None: continue # flexible workers can't have preload

                # Only try to apply preload to nodes that depend on > 1 tasks AND at least 1 of them is from different worker ids
                if (len(node.upstream_nodes) == 0 or 
                    len([un for un in node.upstream_nodes 
                        if un.worker_config.worker_id != 
                            resource_config.worker_id]) == 0):
                    continue

                # logger.info(f"Trying to assign 'PreLoad' annotation to critical path node: {node_id}")
                
                # Add PreLoad annotation temporarily
                node.add_annotation(PreLoadOptimization())

                # Recalculate timings with this optimization
                new_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, predictions_provider, self.config.sla)
                new_critical_path_nodes, new_critical_path_time = self._find_critical_path(dag, new_nodes_info)
                new_critical_path_node_ids = {node.id.get_full_id() for node in new_critical_path_nodes}

                # Check if optimization improved performance
                if new_critical_path_time < current_critical_path_time:
                    # Optimization helped - keep it
                    # logger.info(f"PreLoad optimization successful for node {node_id}: {current_critical_path_time} -> {new_critical_path_time} ms")
                    nodes_optimized_this_iteration += 1
                    total_preload_optimizations += 1
                    
                    # Check if we introduced a new critical path (different set of nodes)
                    if initial_critical_path_node_ids != new_critical_path_node_ids:
                        # logger.info(f"New critical path introduced. Old: {current_critical_path_node_ids} | New: {new_critical_path_node_ids}")
                        break  # Start new iteration with the new critical path
                    else:
                        # Same critical path, continue optimizing it
                        current_critical_path_nodes = new_critical_path_nodes
                        current_critical_path_time = new_critical_path_time
                        initial_critical_path_node_ids = new_critical_path_node_ids
                        continue
                else:
                    # Optimization didn't help, revert it
                    # logger.info(f"PreLoad optimization not beneficial for node {node_id}: {current_critical_path_time} -> {new_critical_path_time} ms, reverting")
                    node.remove_annotation(PreLoadOptimization)

            # logger.info(f"Optimized {nodes_optimized_this_iteration} nodes in iteration {iteration}")
            
            # If no optimization was applied in this iteration, we're done
            if nodes_optimized_this_iteration == 0:
                # logger.info(f"No further optimizations possible on current critical path. Algorithm completed after {iteration} iterations.")
                break
            
            # If we optimized nodes but didn't introduce a new critical path, we're also done
            updated_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, predictions_provider, self.config.sla)
            final_critical_path_nodes, _ = self._find_critical_path(dag, updated_nodes_info)
            final_critical_path_node_ids = {node.id.get_full_id() for node in final_critical_path_nodes}
            
            if initial_critical_path_node_ids == final_critical_path_node_ids:
                # logger.info(f"Critical path unchanged after optimizations. Algorithm completed after {iteration} iterations.")
                break
                
            # Prevent infinite loops
            if iteration > 100:
                logger.warning(f"Maximum iterations reached. Stopping algorithm.")
                break

        # OPTIMIZATION: PRE-WARM
        total_prewarm_optimizations = 0
        
        # For each node that has a cold start
        for my_node_id, node_info in updated_nodes_info.items():
            if node_info.worker_startup_state != "cold": continue
            if len(node_info.node_ref.upstream_nodes) == 0: continue # ignore root nodes

            # Calculate sum of execution times of tasks with same worker config that start after this node
            my_worker_config = node_info.node_ref.worker_config
            sum_exec_times = 0
            
            # Get tasks with same resources that start after this node (because they could also benefit from pre-warm)
            for other_node_id, other_node_info in updated_nodes_info.items():
                if other_node_id == my_node_id: continue
                other_worker_config = other_node_info.node_ref.worker_config
                if other_worker_config.cpus != my_worker_config.cpus or other_worker_config.memory_mb != my_worker_config.memory_mb: continue
                if other_node_info.earliest_start_ms > node_info.earliest_start_ms:
                    sum_exec_times += other_node_info.tp_exec_time_ms
            

            if not (node_info.tp_worker_startup_time_ms > 0.15 * sum_exec_times):
                # don't apply pre-warm if the startup time is not significant when compared to the time that worker will be executing
                continue
            
            # Find the best node to add pre-warm annotation to
            best_node = None
            best_start_time = -1
            time_until_worker_goes_cold_ms = AbstractDAGPlanner.TIME_UNTIL_WORKER_GOES_COLD_S * 1000
            
            for other_node_id, other_node_info in updated_nodes_info.items():
                if other_node_id == my_node_id: continue
                
                # time at which the worker config I need would be available if I were to add pre-warm annotation to this node
                my_worker_potential_ready_if_prewarmed = other_node_info.earliest_start_ms + node_info.tp_worker_startup_time_ms
                # avoid the worker being ready but cold by the time we need it
                min_prewarm_time = max(0, node_info.earliest_start_ms - time_until_worker_goes_cold_ms + time_until_worker_goes_cold_ms / 3)
                max_prewarm_time = max(0, node_info.earliest_start_ms)
                is_in_optimal_prewarm_window = min_prewarm_time < my_worker_potential_ready_if_prewarmed < max_prewarm_time
                
                if is_in_optimal_prewarm_window and (best_node is None or other_node_info.earliest_start_ms > best_start_time):
                    best_node = other_node_info.node_ref
                    best_start_time = other_node_info.earliest_start_ms
            
            # Add pre-warm annotation to the best node found
            if best_node is not None:
                annotation = best_node.try_get_annotation(PreWarmOptimization)
                if not annotation: annotation = best_node.add_annotation(PreWarmOptimization([]))
                # allow multiple pre-warms for the same worker config (only makes sense with local docker implementation. Lambda implementation)
                
                annotation.target_resource_configs.append(my_worker_config)
                # recomputing node timings is required because after adding `PreWarm` annotation, other tasks "cold" starts may become "warm"
                #  and the next iteration of this "pre-warm annotation assignment" algorithm needs to know the updated state ("cold" | "warm")
                updated_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, predictions_provider, self.config.sla)
                total_prewarm_optimizations += 1

        # OPTIMIZATION: TASK-DUP
        total_duppable_tasks = 0
        for node_info in updated_nodes_info.values():
            if node_info.node_ref.try_get_annotation(TaskDupOptimization): 
                # Skip if node already has TaskDup annotation. Cloud have been added by the user
                continue
            if len(node_info.node_ref.downstream_nodes) == 0: continue
            if node_info.deserialized_input_size > DUPPABLE_TASK_MAX_INPUT_SIZE: continue
            if node_info.tp_exec_time_ms > DUPPABLE_TASK_MAX_EXEC_TIME_MS: continue
            node_info.node_ref.add_annotation(TaskDupOptimization())
            total_duppable_tasks += 1

        # Final statistics and logging
        final_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, predictions_provider, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, final_nodes_info)
        final_critical_path_node_ids = {node.id.get_full_id() for node in final_critical_path_nodes}
        
        # Calculate resource distribution
        resource_distribution = {}
        unique_worker_ids: dict[str, int] = {}
        nodes_with_preload = 0
        
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.worker_config
            config_key = f"CPU:{resource_config.cpus},Memory:{resource_config.memory_mb}MB"
            if config_key not in resource_distribution:
                resource_distribution[config_key] = 0
            resource_distribution[config_key] += 1
            
            if resource_config.worker_id is None: continue
            if resource_config.worker_id not in unique_worker_ids:
                unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1
            
            if node.try_get_annotation(PreLoadOptimization):
                nodes_with_preload += 1

        prediction_samples_used = AbstractDAGPlanner.PlanPredictionSampleCounts(
            previous_instances=predictions_provider.nr_of_previous_instances,
            # note: data from ALL workflow instances
            for_download_speed=len(predictions_provider.cached_download_speeds),
            for_upload_speed=len(predictions_provider.cached_upload_speeds),
            # note: only related to instances from same workflow type
            for_execution_time=sum(map(len, predictions_provider.cached_execution_time_per_byte.values())),
            for_output_size=sum(map(len, predictions_provider.cached_deserialized_io_ratios.values()))
        )

        logger.info(f"=== FINAL RESULTS ===")
        logger.info(f"Critical Path | Nr. Nodes: {len(final_critical_path_nodes)}, Predicted Completion Time: {final_critical_path_time / 1000:.2f}s")
        logger.info(f"Number of PreLoad optimizations: {total_preload_optimizations} | Number of PreWarm optimizations: {total_prewarm_optimizations} | Number of duppable tasks: {total_duppable_tasks}/{len(updated_nodes_info)}")
        logger.info(f"Number of unique workers: {len(unique_worker_ids)}")
        logger.info(f"Successfully downgraded resources for {successful_worker_resources_downgrades}/{len(_dag._all_nodes)} nodes")
        logger.info(f"Worker Resource Configuration Distribution: {resource_distribution}")
        # logger.info(f"Prediction samples used: {prediction_samples_used}")

        return AbstractDAGPlanner.PlanOutput(
            self.__class__.__name__, 
            self.config.sla,
            final_nodes_info, 
            final_critical_path_node_ids, 
            prediction_samples_used
        )

    @staticmethod
    async def wel_on_worker_ready(intermediate_storage: Storage, dag: FullDAG, this_worker_id: str | None):
        from src.planning.annotations.preload import PreLoadOptimization
        await PreLoadOptimization.wel_on_worker_ready(intermediate_storage, dag, this_worker_id)

    @staticmethod
    async def wel_before_task_handling(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task, is_dupping: bool = False):
        from src.planning.annotations.prewarm import PreWarmOptimization
        await PreWarmOptimization.wel_before_task_handling(this_worker, metadata_storage, subdag, current_task, is_dupping)
        from src.planning.annotations.taskdup import TaskDupOptimization
        await TaskDupOptimization.wel_before_task_handling(this_worker, metadata_storage, subdag, current_task, is_dupping)

    @staticmethod
    async def wel_before_task_execution(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task, is_dupping: bool):
        from src.planning.annotations.taskdup import TaskDupOptimization
        await TaskDupOptimization.wel_before_task_execution(this_worker, metadata_storage, subdag, current_task, is_dupping)

    @staticmethod
    async def wel_override_handle_inputs(intermediate_storage: Storage, task, subdag: SubDAG, upstream_tasks_without_cached_results: list, worker_resource_config, task_dependencies: dict[str, Any]) -> tuple[list, list[str], CoroutineType | None]:
        """
        returns (
            tasks_to_fetch: list[task] (on default implementation, fetch ALL tasks that don't have cached results),
            wait_until_coroutine: list[TaskInputMetrics] (so that the caller can fetch the tasks in parallel)
        )
        """
        res = await PreLoadOptimization.wel_override_handle_inputs(intermediate_storage, task, subdag, upstream_tasks_without_cached_results, worker_resource_config, task_dependencies)
        return res

    @staticmethod
    async def wel_override_should_upload_output(current_task, subdag: SubDAG, this_worker, metadata_storage: Storage, is_dupping: bool) -> bool:
        from src.planning.annotations.taskdup import TaskDupOptimization
        res = await TaskDupOptimization.wel_override_should_upload_output(current_task, subdag, this_worker, metadata_storage, is_dupping)
        return res

    @staticmethod
    async def wel_override_handle_downstream(current_task, this_worker, downstream_tasks_ready, subdag: SubDAG, is_dupping: bool) -> list:
        from src.planning.annotations.taskdup import TaskDupOptimization
        res = await TaskDupOptimization.wel_override_handle_downstream(current_task, this_worker, downstream_tasks_ready, subdag, is_dupping)
        return res