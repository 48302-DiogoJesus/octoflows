from typing import Any, Awaitable
from abc import abstractmethod
from dataclasses import dataclass
from src.workers.worker_execution_logic import WorkerExecutionLogic

@dataclass
class TaskOptimization(WorkerExecutionLogic):
    @property
    @abstractmethod
    def name(self) -> str: pass

    @staticmethod
    @abstractmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list): pass

    @staticmethod
    async def wel_on_worker_ready(worker, dag): pass

    @staticmethod
    async def wel_before_task_handling(worker, task, subdag): pass

    @staticmethod
    async def wel_override_handle_inputs(worker, task, subdag, upstream_tasks_without_cached_results: list) -> tuple[list, list[str], Awaitable[Any] | None] | None: return None
    
    @staticmethod
    async def wel_before_task_execution(worker, task, subdag): pass
    
    @staticmethod
    async def wel_override_should_upload_output(worker, task, subdag) -> bool | None: return None
    
    @staticmethod
    async def wel_update_dependency_counters(worker,  task, subdag) -> list | None: return None

    @staticmethod
    async def wel_override_handle_downstream(worker, task, fulldag, subdag, downstream_tasks_ready) -> list | None: return None