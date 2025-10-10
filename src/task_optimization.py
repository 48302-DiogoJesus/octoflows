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
    async def wel_on_worker_ready(planner, intermediate_storage, metadata_storage, dag, this_worker_id: str | None, this_worker): pass

    @staticmethod
    async def wel_before_task_handling(planner, this_worker, metadata_storage, subdag, current_task, is_dupping: bool): pass

    @staticmethod
    async def wel_override_handle_inputs(planner, intermediate_storage, metadata_storage, task, subdag, upstream_tasks_without_cached_results: list, worker_resource_config, task_dependencies: dict) -> tuple[list, list[str], Awaitable[Any] | None] | None: return None
    
    @staticmethod
    async def wel_before_task_execution(planner, this_worker, metadata_storage, subdag, current_task, is_dupping: bool): pass
    
    @staticmethod
    async def wel_override_should_upload_output(planner, current_task, subdag, this_worker, metadata_storage, is_dupping: bool) -> bool | None: return None
    
    @staticmethod
    async def wel_update_dependency_counters(planner, this_worker, metadata_storage, subdag, current_task, is_dupping: bool) -> list | None: return None

    @staticmethod
    async def wel_override_handle_downstream(planner, fulldag, current_task, this_worker, downstream_tasks_ready, subdag) -> list | None: return None