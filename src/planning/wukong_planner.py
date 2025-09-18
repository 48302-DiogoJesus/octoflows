from dataclasses import dataclass
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.utils.logger import create_logger
from src.workers.worker_execution_logic import WorkerExecutionLogic
import uuid

logger = create_logger(__name__, prefix="PLANNING")

class WUKONGPlanner(AbstractDAGPlanner, WorkerExecutionLogic):
    @dataclass
    class Config(AbstractDAGPlanner.BaseConfig):
        worker_resource_configuration: TaskWorkerResourceConfiguration

        def create_instance(self) -> "WUKONGPlanner":
            return WUKONGPlanner(self)
        
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def get_description(self) -> str:
        return \
            """
            - Uses uniform resources
            - Assigns flexible workers to all tasks
            - Doesn't use predictions
            """

    def internal_plan(self, dag, predictions_provider: PredictionsProvider):

        topo_sorted_nodes = self._topological_sort(dag)

        assert isinstance(self.config, WUKONGPlanner.Config)

        for node in topo_sorted_nodes:
            resource_config = self.config.worker_resource_configuration.clone()
            resource_config.worker_id = None # "flexible worker"
            node.worker_config = resource_config

        final_nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, predictions_provider, self.config.worker_resource_configuration, self.config.sla)
        final_critical_path_nodes, _ = self._find_critical_path(dag, final_nodes_info)
        final_critical_path_node_ids = { node.id.get_full_id() for node in final_critical_path_nodes }

        return AbstractDAGPlanner.PlanOutput(
            self.planner_name, 
            self.config.sla,
            final_nodes_info,
            final_critical_path_node_ids,
            AbstractDAGPlanner.PlanPredictionSampleCounts(
                previous_instances=0,
                for_download_speed=0,
                for_upload_speed=0,
                for_execution_time=0,
                for_output_size=0
            )
        )
