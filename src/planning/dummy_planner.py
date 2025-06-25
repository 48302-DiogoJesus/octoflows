from dataclasses import dataclass
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.utils.logger import create_logger
from src.workers.worker_execution_logic import WorkerExecutionLogic

logger = create_logger(__name__, prefix="PLANNING")

class DummyDAGPlanner(AbstractDAGPlanner, WorkerExecutionLogic):
    @dataclass
    class Config(AbstractDAGPlanner.Config):
        def create_instance(self) -> "DummyDAGPlanner":
            return DummyDAGPlanner(self)
        
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def internal_plan(self, dag, predictions_provider: PredictionsProvider): return None

    def get_description(self) -> str:
        return \
            """
            Does nothing
            """
