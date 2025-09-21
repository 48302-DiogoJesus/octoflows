from dataclasses import dataclass
from src.planning.optimizations.taskdup import TaskDupOptimization
from src.planning.optimizations.preload import PreLoadOptimization
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.utils.logger import create_logger

logger = create_logger(__name__, prefix="PLANNING")

class UniformPlanner(AbstractDAGPlanner):
    @dataclass
    class Config(AbstractDAGPlanner.BaseConfig):
        def create_instance(self) -> "UniformPlanner": 
            super().create_instance()
            return UniformPlanner(self)

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
        assert isinstance(self.config, UniformPlanner.Config)

        topo_sorted_nodes = self._topological_sort(dag)

        worker_resources = self.config.worker_resource_configurations[0]

        if not predictions_provider.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving uniform resources ({worker_resources}) to all nodes")
            # Assign worker resources and ids
            for node in topo_sorted_nodes: 
                unique_resources = worker_resources.clone()
                unique_resources.worker_id = None # note: ALL workers will be "flexible"
                node.worker_config = unique_resources
            # self._store_plan_image(dag)
            # self._store_plan_as_json(dag)
            return
        
        # Step 1: Assign uniform resources to all nodes
        # logger.info("=== Step 1: Initial assignment with best resources ===")
        self._basic_worker_id_assignment(dag, worker_resources, topo_sorted_nodes)

        nodes_info = self._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, self.config.sla)

        # OPTIMIZATIONS
        for optimization in self.config.optimizations:
            optimization.planning_assignment_logic(self, dag, predictions_provider, nodes_info, topo_sorted_nodes)

        optimizations_count: dict[str, int] = {}
        for node_info in nodes_info.values():
            for optimization in node_info.node_ref.optimizations: 
                optimizations_count[optimization.__class__.__name__] = optimizations_count.get(optimization.__class__.__name__, 0) + 1

        # Final statistics
        nodes_info = self._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, nodes_info)
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
        logger.info(f"Optimizations: {optimizations_count}")
        logger.info(f"Worker Resource Configuration (same for all tasks): (cpus={worker_resources.cpus}, memory={worker_resources.memory_mb})")
        # logger.info(f"Prediction samples used: {prediction_samples_used}")

        return AbstractDAGPlanner.PlanOutput(
            self.planner_name, 
            self.config.sla,
            nodes_info,
            final_critical_path_node_ids,
            prediction_samples_used
        )
