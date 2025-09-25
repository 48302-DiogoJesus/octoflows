import asyncio
from dataclasses import dataclass, field
import time
from src.dag.dag import SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import DAGTaskNode
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger
from src.utils.errors import CancelCurrentWorkerLoopException

logger = create_logger(__name__)

# if task execution time exceeds this, don't allow dupping. Short tasks are better for dupping
DUPPABLE_TASK_MAX_EXEC_TIME_MS: float = 2_000
# if task input size exceeds 10MB, don't allow dupping
DUPPABLE_TASK_MAX_INPUT_SIZE: int = 10 * 1024 * 1024
DUPPABLE_TASK_STARTED_PREFIX = "taskdup-task-started-"
DUPPABLE_TASK_CANCELLATION_PREFIX = "taskdup-cancellation-"
DUPPABLE_TASK_TIME_SAVED_THRESHOLD_MS = 1_500 # the least amount of time we need to save to justify duplication

@dataclass
class TaskDupOptimization(TaskOptimization, WorkerExecutionLogic):
    """ 
    Indicates that this task can be duplicated IF NEEDED (decided at runtime)
    """

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    
    @property
    def name(self) -> str: return "TaskDup"

    def clone(self): return TaskDupOptimization()

    @staticmethod
    async def _check_cancellation_flag(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode):
        # CHECK CANCELLATION FLAG
        # if there is NOT ANY downstream task that will(fixed worker_id)/could(flexible worker) be executed by this worker, I don't need to execute locally, since someone else is executing it already and will notify the others who need it
        # if I do have at least 1 task that will need this locally, execute it 
        #   (avoids complex logic waiting for the remote worker to finish it + download time (because it's produced locally))
        from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
        has_downstream_task_to_execute_locally = any([n for n in current_task.downstream_nodes if n.worker_config.worker_id is None or n.worker_config.worker_id == this_worker.my_resource_configuration.worker_id])
        
        if not has_downstream_task_to_execute_locally:
            if await metadata_storage.exists(f"{DUPPABLE_TASK_CANCELLATION_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}"): 
                raise CancelCurrentWorkerLoopException(f"Task {current_task.id.get_full_id()} is being dupped by another worker, aborting branch with {len(current_task.downstream_nodes)} downstream tasks")

    @staticmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]):
        for node_info in nodes_info.values():
            if node_info.node_ref.try_get_optimization(TaskDupOptimization): 
                # Skip if node already has TaskDup annotation. Cloud have been added by the user
                continue
            if len(node_info.node_ref.downstream_nodes) == 0: continue
            if node_info.tp_exec_time_ms > DUPPABLE_TASK_MAX_EXEC_TIME_MS: continue
            if node_info.deserialized_input_size > DUPPABLE_TASK_MAX_INPUT_SIZE: continue
            node_info.node_ref.add_optimization(TaskDupOptimization())
        return

    @staticmethod
    async def wel_before_task_handling(planner, this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode, is_dupping: bool):
        is_duppable = current_task.try_get_optimization(TaskDupOptimization) is not None
        # if not is_dupping and is_duppable: await TaskDupOptimization._check_cancellation_flag(this_worker, metadata_storage, subdag, current_task)
        if is_duppable: await metadata_storage.set(f"{DUPPABLE_TASK_STARTED_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}", time.time())

    @staticmethod
    async def wel_before_task_execution(planner, this_worker, metadata_storage: Storage, subdag: SubDAG, current_task, is_dupping: bool):
        is_duppable = current_task.try_get_optimization(TaskDupOptimization) is not None
        # set the cancellation flag to notify other workers to not execute this task. if all inputs are available, then we can dup the task. Warn others that they MAY NOT need to execute it
        if is_dupping: await metadata_storage.set(f"{DUPPABLE_TASK_CANCELLATION_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}", 1)

        # if not is_dupping and is_duppable: await TaskDupOptimization._check_cancellation_flag(this_worker, metadata_storage, subdag, current_task)

    @staticmethod
    async def wel_override_should_upload_output(planner, current_task, subdag: SubDAG, this_worker, metadata_storage: Storage, is_dupping: bool) -> bool:
        from src.dag_task_node import DAGTaskNode
        _task: DAGTaskNode = current_task

        # if not is_dupping and _task.try_get_optimization(TaskDupOptimization) is not None: await TaskDupOptimization._check_cancellation_flag(this_worker, metadata_storage, subdag, _task)

        has_any_downstream_from_another_worker = any(dt.worker_config.worker_id is None or dt.worker_config.worker_id != this_worker.my_resource_configuration.worker_id for dt in _task.downstream_nodes)

        return has_any_downstream_from_another_worker or subdag.sink_node.id.get_full_id() == _task.id.get_full_id() or this_worker is None

    @staticmethod
    async def wel_override_handle_downstream(planner, fulldag, current_task, this_worker, downstream_tasks_ready, subdag: SubDAG, is_dupping: bool):
        if is_dupping: raise CancelCurrentWorkerLoopException("This task was dupped, don't continue branch")
        return None
