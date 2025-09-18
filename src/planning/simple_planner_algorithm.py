from dataclasses import dataclass
import uuid

from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.utils.logger import create_logger

logger = create_logger(__name__, prefix="PLANNING")

class SimplePlannerAlgorithm(AbstractDAGPlanner):
    @dataclass
    class Config(AbstractDAGPlanner.BaseConfig):
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

        assert isinstance(self.config, SimplePlannerAlgorithm.Config)

        if not predictions_provider.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving uniform resources ({self.config.worker_resource_configuration}) to all nodes")
            # No Metadata recorded for previous runs of the same DAG structure => give intermediate resources to all nodes
            # Assign worker resources and ids
            for node in topo_sorted_nodes: 
                unique_resources = self.config.worker_resource_configuration.clone()
                unique_resources.worker_id = None # note: ALL workers will be "flexible"
                node.worker_config = unique_resources
            # self._store_plan_image(dag)
            # self._store_plan_as_json(dag)
            return
        
        # Give same resources to all nodes + assign worker ids to all tasks
        for node in topo_sorted_nodes:
            resource_config = self.config.worker_resource_configuration.clone()
            node.worker_config = resource_config
            if len(node.upstream_nodes) == 0:
                resource_config.worker_id = uuid.uuid4().hex
            else:
                # Tries to assign the same worker id to multiple downstream nodes
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

        # Final statistics
        final_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, predictions_provider, self.config.worker_resource_configuration, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, final_nodes_info)
        final_critical_path_node_ids = { node.id.get_full_id() for node in final_critical_path_nodes }
            
        unique_worker_ids: dict[str, int] = {}
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.worker_config
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
        logger.info(f"Critical Path Nodes Count: {len(final_critical_path_nodes)} | Predicted Completion Time: {final_critical_path_time / 1000:.2f}s | Unique workers: {len(unique_worker_ids)}")
        logger.info(f"Number of unique workers: {len(unique_worker_ids)}")
        logger.info(f"Worker Resource Configuration (same for all tasks): (cpus={self.config.worker_resource_configuration.cpus}, memory={self.config.worker_resource_configuration.memory_mb})")
        # logger.info(f"Prediction samples used: {prediction_samples_used}")

        return AbstractDAGPlanner.PlanOutput(
            self.__class__.__name__, 
            self.config.sla,
            final_nodes_info, 
            final_critical_path_node_ids, 
            prediction_samples_used
        )
