from dataclasses import dataclass
from types import CoroutineType
from typing import Any
import uuid

from src.dag.dag import FullDAG, SubDAG
from src.planning.annotations.preload import PreLoadOptimization
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.storage.storage import Storage
from src.utils.logger import create_logger

logger = create_logger(__name__, prefix="PLANNING")

class SimplePlannerAlgorithm(AbstractDAGPlanner):
    @dataclass
    class Config(AbstractDAGPlanner.Config):
        all_flexible_workers: bool
        worker_resource_configuration: TaskWorkerResourceConfiguration

        def create_instance(self) -> "SimplePlannerAlgorithm": return SimplePlannerAlgorithm(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def get_description(self) -> str: 
        return \
            """
            This algorithm just assigns the same resources to all tasks (uniform workers), without using optimizations and without considering the critical path.
            It can be parametrized to control whether workers should be strict or flexible.
            """

    def internal_plan(self, dag, predictions_provider: PredictionsProvider):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        topo_sorted_nodes = self._topological_sort(dag)

        if not predictions_provider.has_required_predictions():
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
            
            if not self.config.all_flexible_workers:
                if len(node.upstream_nodes) == 0:
                    # Give each root node a unique worker id
                    resource_config.worker_id = uuid.uuid4().hex
                else:
                    # Use same worker id as its first upstream node
                    resource_config.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id

        # Final statistics
        final_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, predictions_provider, self.config.worker_resource_configuration, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, final_nodes_info)
        final_critical_path_node_ids = { node.id.get_full_id() for node in final_critical_path_nodes }
            
        unique_worker_ids: dict[str, int] = {}
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            if resource_config.worker_id is None: continue
            if resource_config.worker_id not in unique_worker_ids: unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1

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
        logger.info(f"Critical Path | Nr. Nodes: {len(final_critical_path_nodes)}, Predicted Completion Time: {final_critical_path_time} ms")
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