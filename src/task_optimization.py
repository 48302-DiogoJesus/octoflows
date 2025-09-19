from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class TaskOptimization(ABC):
    @staticmethod
    @abstractmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list) -> dict : pass