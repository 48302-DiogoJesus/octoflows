
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, TypeAlias

from src.storage.metrics import metrics_storage
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.worker_resource_configuration import TaskWorkerResourceConfiguration

@dataclass
class Percentile:
    value: int

SLA: TypeAlias = Literal["avg"] | Percentile

logger = create_logger(__name__)

class DAGPlanner(ABC):
    @staticmethod
    @abstractmethod
    def plan(dag, metrics_storage_config: metrics_storage.MetricsStorage.Config, available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA): 
        """
        dag: dag.DAG
        metrics_storage_config: MetricsStorage.Config ??

        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        pass

class DummyDAGPlanner(DAGPlanner):
    @staticmethod
    def plan(dag, metrics_storage_config: metrics_storage.MetricsStorage.Config, available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA):
        from src.dag import DAG
        _dag: DAG = dag
        for node_id, node in _dag._all_nodes.items():
            logger.info(f"Adding annotation to node: {node_id}")
            node.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory=128))
        return

class SimpleDAGPlanner(DAGPlanner):
    @staticmethod
    def plan(dag, metrics_storage_config: metrics_storage.MetricsStorage.Config, available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA):
        """
        dag: dag.DAG

        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        from src.dag import DAG
        _dag: DAG = dag
        logger.info("Starting DAG Planning Algorithm")
        algorithm_start_time = Timer()
        if sla == "avg":
            logger.info("Using average SLA")
            # Handle average case
        elif isinstance(sla, Percentile):
            logger.info(f"Using percentile SLA: {sla.value}")
        else:
            raise ValueError(f"Invalid SLA given: {sla}")
        
        # TODO: Handle planning
        logger.info(f"Planning completed in {algorithm_start_time.stop():.3f} ms")

        pass