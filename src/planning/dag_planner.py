
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, TypeAlias

from src.planning.metadata_access.metadata_access import MetadataAccess
from src.storage.metrics import metrics_storage
from src.planning.sla import SLA, Percentile
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class DAGPlanner(ABC):
    @staticmethod
    @abstractmethod
    def plan(dag, metadata_access: MetadataAccess, available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA): 
        """
        dag: dag.DAG
        metadata_access: MetadataAccess

        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        pass

class DummyDAGPlanner(DAGPlanner):
    @staticmethod
    def plan(dag, metadata_access: MetadataAccess, available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
        for node_id, node in _dag._all_nodes.items():
            node.add_annotation(TaskWorkerResourceConfiguration(cpus=2, memory_mb=256))
        return

class SimpleDAGPlanner(DAGPlanner):
    @staticmethod
    def plan(dag, metadata_access: MetadataAccess, available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA):
        """
        dag: dag.DAG

        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
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