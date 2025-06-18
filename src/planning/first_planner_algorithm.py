from dataclasses import dataclass
from types import CoroutineType
from typing import Any
import uuid

from src.dag.dag import FullDAG, SubDAG
from src.planning.annotations.preload import PreLoadOptimization
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.metadata_access.metadata_access import MetadataAccess
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
            until we can't optimize the critical path any further.
            """

    def internal_plan(self, dag, metadata_access: MetadataAccess):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        topo_sorted_nodes = self._topological_sort(dag)

        if not metadata_access.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving uniform resources ({self.config.worker_resource_configuration}) to all nodes")
            # No Metadata recorded for previous runs of the same DAG structure => give intermediate resources to all nodes
            # Assign worker resources and ids
            for node in topo_sorted_nodes: 
                unique_resources = self.config.worker_resource_configuration.clone()
                unique_resources.worker_id = None # note: ALL workers will be "flexible"
                node.add_annotation(unique_resources)
            self._store_plan_image(dag)
            return
        
        # Give same resources to all nodes and assign worker ids
        for node in topo_sorted_nodes:
            resource_config = self.config.worker_resource_configuration.clone()
            node.add_annotation(resource_config)
            if len(node.upstream_nodes) == 0:
                # Give each root node a unique worker id
                resource_config.worker_id = uuid.uuid4().hex
            else:
                # Use same worker id as its first upstream node
                resource_config.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id

        iteration = 0
        total_preload_optimizations = 0
        while True:
            iteration += 1
            
            # Calculate current node timings and find critical path
            nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, metadata_access, self.config.worker_resource_configuration, self.config.sla)
            critical_path_nodes, critical_path_time = self._find_critical_path(dag, nodes_info)
            critical_path_node_ids = { node.id.get_full_id() for node in critical_path_nodes }
            
            # logger.info(f"CRITICAL PATH | Nodes: {len(critical_path_nodes)} | Node IDs: {[node.id.get_full_id() for node in critical_path_nodes]} | Predicted Completion Time: {critical_path_time} ms")

            # Try to optimize nodes in the current critical path with PreLoad
            optimized_any_node = False
            nodes_optimized_this_iteration = 0
            
            for node in critical_path_nodes:
                node_id = node.id.get_full_id()
                
                if node.try_get_annotation(PreLoadOptimization): continue # Skip if node already has PreLoad annotation
                
                resource_config: TaskWorkerResourceConfiguration = node.get_annotation(TaskWorkerResourceConfiguration)
                if resource_config.worker_id is None: continue # flexible workers can't have preload

                # Only apply preload to nodes that depend on > 1 tasks AND at least 1 of them is from different worker id
                if len(node.upstream_nodes) == 0 or len([un for un in node.upstream_nodes if un.get_annotation(TaskWorkerResourceConfiguration).worker_id is None or un.get_annotation(TaskWorkerResourceConfiguration).worker_id != resource_config.worker_id]) == 0:
                    continue

                # logger.info(f"Trying to assign 'PreLoad' annotation to critical path node: {node_id}")
                
                # Add PreLoad annotation temporarily
                node.add_annotation(PreLoadOptimization())

                # Recalculate timings with this optimization
                new_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, metadata_access, self.config.worker_resource_configuration, self.config.sla)
                new_critical_path_nodes, new_critical_path_time = self._find_critical_path(dag, new_nodes_info)
                new_critical_path_node_ids = { node.id.get_full_id() for node in new_critical_path_nodes }

                # Check if optimization improved performance
                if new_critical_path_time < critical_path_time:
                    # Optimization helped - keep it
                    # logger.info(f"PreLoad optimization successful for node {node_id}: {critical_path_time} -> {new_critical_path_time} ms")
                    optimized_any_node = True
                    nodes_optimized_this_iteration += 1
                    total_preload_optimizations += 1
                    
                    # Check if we introduced a new critical path (different set of nodes)
                    if critical_path_node_ids != new_critical_path_node_ids:
                        # logger.info(f"New critical path introduced. Old: {critical_path_node_ids} | New: {new_critical_path_node_ids}")
                        break  # Start new iteration with the new critical path
                    else:
                        # Same critical path, continue optimizing it
                        critical_path_nodes = new_critical_path_nodes
                        critical_path_time = new_critical_path_time
                        critical_path_node_ids = new_critical_path_node_ids
                        continue
                else:
                    # Optimization didn't help, revert it
                    # logger.info(f"PreLoad optimization not beneficial for node {node_id}: {critical_path_time} -> {new_critical_path_time} ms, reverting")
                    node.remove_annotation(PreLoadOptimization)

            # logger.info(f"Optimized {nodes_optimized_this_iteration} nodes in iteration {iteration}")
            
            # If no optimization was applied in this iteration, we're done
            if not optimized_any_node:
                # logger.info(f"No further optimizations possible on current critical path. Algorithm completed after {iteration} iterations.")
                break
            
            # If we optimized nodes but didn't introduce a new critical path, we're also done
            # (this happens when we've optimized all optimizable nodes in the current critical path)
            current_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, metadata_access, self.config.worker_resource_configuration, self.config.sla)
            current_critical_path_nodes, _ = self._find_critical_path(dag, current_nodes_info)
            current_critical_path_node_ids = { node.id.get_full_id() for node in current_critical_path_nodes }
            
            if critical_path_node_ids == current_critical_path_node_ids:
                # logger.info(f"Critical path unchanged after optimizations. Algorithm completed after {iteration} iterations.")
                break
                
            # Prevent infinite loops
            if iteration > 100:
                logger.warning(f"Maximum iterations reached. Stopping algorithm.")
                break

        # Final statistics
        final_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, metadata_access, self.config.worker_resource_configuration, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, final_nodes_info)
        final_critical_path_node_ids = { node.id.get_full_id() for node in final_critical_path_nodes }
            
        unique_worker_ids: dict[str, int] = {}
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            if resource_config.worker_id is None: continue
            if resource_config.worker_id not in unique_worker_ids: unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1

        prediction_samples_used = AbstractDAGPlanner.PlanPredictionSampleCounts(
            # note: data from ALL workflow instances
            for_download_speed=len(metadata_access.cached_download_speeds),
            for_upload_speed=len(metadata_access.cached_upload_speeds),
            # note: only related to instances from same workflow type
            for_execution_time=sum(map(len, metadata_access.cached_execution_time_per_byte.values())),
            for_output_size=sum(map(len, metadata_access.cached_io_ratios.values()))
        )

        logger.info(f"=== FINAL RESULTS ===")
        logger.info(f"Critical Path | Nr. Nodes: {len(final_critical_path_nodes)}, Predicted Completion Time: {final_critical_path_time} ms")
        logger.info(f"Number of PreLoad optimizations: {total_preload_optimizations}")
        logger.info(f"Number of unique workers: {len(unique_worker_ids)}")
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
    async def override_on_worker_ready(intermediate_storage: Storage, dag: FullDAG, this_worker_id: str | None):
        from src.planning.annotations.preload import PreLoadOptimization
        await PreLoadOptimization.override_on_worker_ready(intermediate_storage, dag, this_worker_id)

    @staticmethod
    async def override_handle_inputs(intermediate_storage: Storage, task, subdag: SubDAG, upstream_tasks_without_cached_results: list, worker_resource_config, task_dependencies: dict[str, Any]) -> tuple[list, CoroutineType | None]:
        """
        returns (
            tasks_to_fetch: list[task] (on default implementation, fetch ALL tasks that don't have cached results),
            wait_until_coroutine: list[TaskInputMetrics] (so that the caller can fetch the tasks in parallel)
        )
        """
        res = await PreLoadOptimization.override_handle_inputs(intermediate_storage, task, subdag, upstream_tasks_without_cached_results, worker_resource_config, task_dependencies)
        return res