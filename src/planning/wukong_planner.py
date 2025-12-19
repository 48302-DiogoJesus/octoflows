from dataclasses import dataclass

import cloudpickle
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.utils.logger import create_logger
from src.utils.utils import calculate_data_structure_size_bytes
from src.workers.worker_execution_logic import WorkerExecutionLogic

logger = create_logger(__name__, prefix="PLANNING")

class WUKONGPlanner(AbstractDAGPlanner, WorkerExecutionLogic):
    @dataclass
    class Config(AbstractDAGPlanner.BaseConfig):

        def create_instance(self) -> "WUKONGPlanner":
            super().create_instance()
            return WUKONGPlanner(self)
        
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def get_description(self) -> str:
        return \
            """
            - Uses uniform resources (specified in config)
            - Assigns flexible workers to all tasks
            - Doesn't use predictions
            - Doesn't use optimizations
            """

    def internal_plan(self, dag, predictions_provider: PredictionsProvider):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        topo_sorted_nodes = self._topological_sort(_dag)

        assert isinstance(self.config, WUKONGPlanner.Config)

        for node in topo_sorted_nodes:
            resource_config = self.config.worker_resource_configurations[0].clone()
            resource_config.worker_id = None # "flexible worker"
            node.worker_config = resource_config

        for optimization in self.config.optimizations:
            optimization.planning_assignment_logic(self, _dag, predictions_provider, {}, topo_sorted_nodes)

        hardcoded_sizes_cache = {}
        for obj_id, (_, obj_data) in _dag._hardcoded_data_ids.items():
            hardcoded_sizes_cache[obj_id] = calculate_data_structure_size_bytes(cloudpickle.dumps(obj_data))
        
        nodes_info = self._calculate_workflow_timings(_dag, topo_sorted_nodes, predictions_provider, self.config.sla, hardcoded_sizes_cache)

        optimizations: dict[str, int] = {}
        for node_info in nodes_info.values():
            for optimization in node_info.node_ref.optimizations: 
                optimizations[optimization.__class__.__name__] = optimizations.get(optimization.__class__.__name__, 0) + 1

        logger.info(f"=== FINAL RESULTS ===")
        logger.info(f"Optimizations: {optimizations}")


        if not predictions_provider.has_required_predictions():
            return None
        else:
            return AbstractDAGPlanner.PlanOutput(
                self.planner_name, 
                self.config.sla,
                {},
                set(),
                AbstractDAGPlanner.PlanPredictionSampleCounts(
                    previous_instances=0,
                    for_download_speed=0,
                    for_upload_speed=0,
                    for_execution_time=0,
                    for_output_size=0
                )
            )
