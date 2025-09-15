from dataclasses import dataclass
from types import CoroutineType
from typing import Any
import uuid

from src.dag.dag import FullDAG, SubDAG
from src.planning.annotations.preload import PreLoadOptimization
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.annotations.taskdup import TaskDupOptimization, DUPPABLE_TASK_MAX_EXEC_TIME_MS, DUPPABLE_TASK_MAX_INPUT_SIZE
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.storage.storage import Storage
from src.utils.logger import create_logger

logger = create_logger(__name__, prefix="PLANNING")

class FirstPlannerAlgorithm(AbstractDAGPlanner):
    @dataclass
    class Config(AbstractDAGPlanner.Config):
        worker_resource_configuration: TaskWorkerResourceConfiguration

        def create_instance(self) -> "FirstPlannerAlgorithm": return FirstPlannerAlgorithm(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def get_description(self) -> str: 
        return \
            """
            The first algorithm would target uniform Lambda
            workers. It would use the MetadataAccess API to predict the longest workflow path (critical path). Then it would
            simulate using the pre-load optimization on this path. If optimizing the critical path made it shorter than the
            second longest path, the algorithm would repeat the process for this new critical path. This would be repeated
            until we can't optimize the critical path any further. TaskDup optimization is also used.
            """

    def internal_plan(self, dag, predictions_provider: PredictionsProvider):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        topo_sorted_nodes = self._topological_sort(dag)

        if not predictions_provider.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving uniform resources ({self.config.worker_resource_configuration}) to all nodes")
            # Assign worker resources and ids
            for node in topo_sorted_nodes: 
                unique_resources = self.config.worker_resource_configuration.clone()
                unique_resources.worker_id = None # note: ALL workers will be "flexible"
                node.worker_config = unique_resources
            self._store_plan_image(dag)
            # self._store_plan_as_json(dag)
            return
        
        # Step 1: Assign best resources to all nodes and find initial critical path + assign worker IDs
        # logger.info("=== Step 1: Initial assignment with best resources ===")
        for node in topo_sorted_nodes:
            resource_config = self.config.worker_resource_configuration.clone()
            node.worker_config = resource_config
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

        # OPTIMIZATION: PRE-LOAD
        iteration = 0
        total_preload_optimizations = 0
        while True:
            iteration += 1
            
            # Calculate current node timings and find critical path
            updated_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, predictions_provider, self.config.worker_resource_configuration, self.config.sla)
            critical_path_nodes, critical_path_time = self._find_critical_path(dag, updated_nodes_info)
            initial_critical_path_node_ids = { node.id.get_full_id() for node in critical_path_nodes }
            
            # logger.info(f"CRITICAL PATH | Nodes: {len(critical_path_nodes)} | Node IDs: {[node.id.get_full_id() for node in critical_path_nodes]} | Predicted Completion Time: {critical_path_time} ms")

            # Try to optimize nodes in the current critical path with PreLoad
            nodes_optimized_this_iteration = 0
            
            for node in critical_path_nodes:
                if node.try_get_annotation(PreLoadOptimization): 
                    # Skip if node already has PreLoad annotation. Either added by this planner or the user
                    continue 
                
                resource_config: TaskWorkerResourceConfiguration = node.worker_config
                if resource_config.worker_id is None: continue # flexible workers can't have preload

                # Only apply preload to nodes that depend on > 1 tasks AND at least 1 of them is from different worker id
                if len(node.upstream_nodes) == 0 or len([un for un in node.upstream_nodes if un.worker_config.worker_id is None or un.worker_config.worker_id != resource_config.worker_id]) == 0:
                    continue

                # logger.info(f"Trying to assign 'PreLoad' annotation to critical path node: {node_id}")
                
                # Add PreLoad annotation temporarily
                node.add_annotation(PreLoadOptimization())

                # Recalculate timings with this optimization
                updated_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, predictions_provider, self.config.worker_resource_configuration, self.config.sla)
                new_critical_path_nodes, new_critical_path_time = self._find_critical_path(dag, updated_nodes_info)
                new_critical_path_node_ids = { node.id.get_full_id() for node in new_critical_path_nodes }

                # Check if optimization improved performance
                if new_critical_path_time < critical_path_time:
                    # Optimization helped - keep it
                    nodes_optimized_this_iteration += 1
                    total_preload_optimizations += 1
                    
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
                    node.remove_annotation(PreLoadOptimization)

            # logger.info(f"Optimized {nodes_optimized_this_iteration} nodes in iteration {iteration}")
            
            # If no optimization was applied in this iteration, we're done
            if nodes_optimized_this_iteration == 0:
                # logger.info(f"No further optimizations possible on current critical path. Algorithm completed after {iteration} iterations.")
                break
            
            # If we optimized nodes but didn't introduce a new critical path, we're also done
            # (this happens when we've optimized all optimizable nodes in the current critical path)
            updated_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, predictions_provider, self.config.worker_resource_configuration, self.config.sla)
            current_critical_path_nodes, _ = self._find_critical_path(dag, updated_nodes_info)
            current_critical_path_node_ids = { node.id.get_full_id() for node in current_critical_path_nodes }
            
            if initial_critical_path_node_ids == current_critical_path_node_ids:
                # logger.info(f"Critical path unchanged after optimizations. Algorithm completed after {iteration} iterations.")
                break
                
            # Prevent infinite loops
            if iteration > 100:
                logger.warning(f"Maximum iterations reached. Stopping algorithm.")
                break
        
        # OPTIMIZATION: TASK-DUP
        total_duppable_tasks = 0
        for node_info in updated_nodes_info.values():
            if node_info.node_ref.try_get_annotation(TaskDupOptimization): 
                # Skip if node already has TaskDup annotation. Cloud have been added by the user
                continue
            if len(node_info.node_ref.downstream_nodes) == 0: continue
            if node_info.tp_exec_time_ms > DUPPABLE_TASK_MAX_EXEC_TIME_MS: continue
            if node_info.deserialized_input_size > DUPPABLE_TASK_MAX_INPUT_SIZE: continue
            node_info.node_ref.add_annotation(TaskDupOptimization())
            total_duppable_tasks += 1

        # Final statistics
        final_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, predictions_provider, self.config.worker_resource_configuration, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, final_nodes_info)
        final_critical_path_node_ids = { node.id.get_full_id() for node in final_critical_path_nodes }
            
        unique_worker_ids: dict[str, int] = {}
        for my_node_id, node in _dag._all_nodes.items():
            resource_config = node.worker_config
            if resource_config.worker_id is None: continue
            if resource_config.worker_id not in unique_worker_ids: unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1

        prediction_samples_used = AbstractDAGPlanner.PlanPredictionSampleCounts(
            # note: data from ALL workflow instances
            previous_instances=predictions_provider.nr_of_previous_instances,
            for_download_speed=len(predictions_provider.cached_download_speeds),
            for_upload_speed=len(predictions_provider.cached_upload_speeds),
            # note: only related to instances from same workflow type
            for_execution_time=sum(map(len, predictions_provider.cached_execution_time_per_byte.values())),
            for_output_size=sum(map(len, predictions_provider.cached_deserialized_io_ratios.values()))
        )

        logger.info(f"=== FINAL RESULTS ===")
        logger.info(f"Critical Path Nodes Count: {len(final_critical_path_nodes)} | Predicted Completion Time: {final_critical_path_time / 1000:.2f}s | Unique workers: {len(unique_worker_ids)}")
        logger.info(f"Number of PreLoad optimizations: {total_preload_optimizations} | Number of duppable tasks: {total_duppable_tasks}/{len(updated_nodes_info)}")
        logger.info(f"Worker Resource Configuration (same for all tasks): (cpus={self.config.worker_resource_configuration.cpus}, memory={self.config.worker_resource_configuration.memory_mb})")
        logger.info(f"Prediction samples used: {prediction_samples_used}")

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