import asyncio
from dataclasses import dataclass
import time
from typing import Any
import cloudpickle
from abc import ABC, abstractmethod

from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.storage.events import TASK_COMPLETION_EVENT_PREFIX, TASK_READY_EVENT_PREFIX
from src.storage.metrics.metrics_types import TaskMetrics, TaskOutputMetrics, TaskInputMetrics
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.metrics import metrics_storage
from src.utils.logger import create_logger
import src.dag.dag as dag
import src.dag_task_node as dag_task_node
import src.storage.storage as storage_module
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.storage.prefixes import DEPENDENCY_COUNTER_PREFIX
from src.storage.prefixes import DAG_PREFIX

logger = create_logger(__name__)

class Worker(ABC, WorkerExecutionLogic):
    @dataclass
    class Config(ABC):
        intermediate_storage_config: storage_module.Storage.Config
        metadata_storage_config: storage_module.Storage.Config | None = None
        metrics_storage_config: metrics_storage.MetricsStorage.Config | None = None
        planner_config: AbstractDAGPlanner.Config | None = None

        @abstractmethod
        def create_instance(self) -> "Worker": pass

    intermediate_storage: storage_module.Storage

    def __init__(self, config: Config):
        self.intermediate_storage = config.intermediate_storage_config.create_instance()
        self.metadata_storage = self.intermediate_storage if not config.metadata_storage_config else config.metadata_storage_config.create_instance()
        self.metrics_storage = config.metrics_storage_config.create_instance() if config.metrics_storage_config else None
        self.planner = config.planner_config.create_instance() if config.planner_config else None

    async def execute_branch(self, subdag: dag.SubDAG, my_worker_id: str) -> tuple[list[str], bool]:
        """
        Note: {my_worker_id} can't be None. for flexible worker it will be prefixed "flexible-"
        """
        if not subdag.root_node: raise Exception(f"AbstractWorker expected a subdag with only 1 root node. Got {len(subdag.root_node)}")
        current_task = subdag.root_node
        self.my_resource_configuration: TaskWorkerResourceConfiguration = current_task.get_annotation(TaskWorkerResourceConfiguration)
        # To help understand locality decisions afterwards, at the dashboard
        _my_resource_configuration_with_flexible_worker_id = self.my_resource_configuration.clone()
        _my_resource_configuration_with_flexible_worker_id.worker_id = my_worker_id
        
        tasks_executed_by_this_coroutine = []
        other_coroutines_i_launched = []
        task_metrics: dict[str, TaskMetrics] = {}
        sink_task_executed = False

        try:
            while True:
                current_task_metrics = TaskMetrics(
                    worker_resource_configuration=_my_resource_configuration_with_flexible_worker_id,
                    started_at_timestamp_s=time.time(),
                    input_metrics=TaskInputMetrics(),
                    execution_time_ms=0,
                    execution_time_per_input_byte_ms=0,
                    update_dependency_counters_time_ms=0,
                    output_metrics=None,  # type: ignore
                    total_invocations_count=0,
                    total_invocation_time_ms=0
                )
                task_metrics[current_task.id.get_full_id_in_dag(subdag)] = current_task_metrics

                #* 1) DOWNLOAD TASK DEPENDENCIES
                self.log(current_task.id.get_full_id(), f"1) Grabbing {len(current_task.upstream_nodes)} upstream tasks...")
                task_dependencies: dict[str, Any] = {}
                # Grab outputs that are already cached locally
                for t in current_task.upstream_nodes:
                    if t.cached_result:
                        task_dependencies[t.id.get_full_id()] = t.cached_result.result
                        current_task_metrics.input_metrics.downloadable_input_size_bytes += calculate_data_structure_size(t.cached_result.result)
                
                upstream_tasks_without_cached_results: list[dag_task_node.DAGTaskNode] = []
                for t in current_task.upstream_nodes:
                    if t.cached_result is None:
                        upstream_tasks_without_cached_results.append(t)

                _download_dependencies_timer = Timer()

                if self.planner:
                    # self.log(self.my_resource_configuration.worker_id, "PLANNER.HANDLE_INPUTS()")
                    tasks_to_fetch, wait_until_coroutine = await self.planner.override_handle_inputs(self.intermediate_storage, current_task, subdag, upstream_tasks_without_cached_results, self.my_resource_configuration, task_dependencies)
                else:
                    # self.log(self.my_resource_configuration.worker_id, "WEL.HANDLE_INPUTS()")
                    tasks_to_fetch, wait_until_coroutine = await WorkerExecutionLogic.override_handle_inputs(self.intermediate_storage, current_task, subdag, upstream_tasks_without_cached_results, self.my_resource_configuration, task_dependencies)

                if tasks_to_fetch:
                    # Prepare batch operations for fetching task data
                    task_keys = [dependency_task.id.get_full_id_in_dag(subdag) for dependency_task in tasks_to_fetch]
                    
                    # Use batching to fetch all task data at once
                    async with self.intermediate_storage.batch() as batch:
                        # Queue all get operations with result keys
                        for i, task_key in enumerate(task_keys):
                            await batch.get(task_key, result_key=f"task_{i}")
                        
                        # Execute all operations at once
                        fetch_results = await batch.execute()
                    
                    # Process the fetched data
                    for i, (dependency_task, task_output) in enumerate(zip(tasks_to_fetch, fetch_results)):
                        if task_output is None:
                            raise Exception(f"[ERROR] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
                        
                        serialized_data = cloudpickle.loads(task_output)
                        task_dependencies[dependency_task.id.get_full_id()] = serialized_data
                        current_task_metrics.input_metrics.downloadable_input_size_bytes += calculate_data_structure_size(serialized_data)

                # Handle wait_until_coroutine if present
                if wait_until_coroutine:
                    wait_result = await wait_until_coroutine
                    current_task_metrics.input_metrics.downloadable_input_size_bytes += wait_result

                current_task_metrics.input_metrics.input_download_time_ms = _download_dependencies_timer.stop() if len(current_task.upstream_nodes) > 0 else 0
                # Store raw values, normalization will be done during prediction

                # METADATA: Register the size of hardcoded arguments as well
                for func_arg in current_task.func_args:
                    if isinstance(func_arg, dag_task_node.DAGTaskNodeId): continue
                    current_task_metrics.input_metrics.hardcoded_input_size_bytes += calculate_data_structure_size(func_arg)

                for func_kwarg in current_task.func_kwargs.values():
                    if isinstance(func_kwarg, dag_task_node.DAGTaskNodeId): continue
                    current_task_metrics.input_metrics.hardcoded_input_size_bytes += calculate_data_structure_size(func_kwarg)
                    
                #* 2) EXECUTE TASK
                self.log(current_task.id.get_full_id(), f"2) Executing Task...")
                if self.planner:
                    # self.log(self.my_resource_configuration.worker_id, "PLANNER.HANDLE_EXECUTION()")
                    task_result, task_execution_time_ms = await self.planner.override_handle_execution(current_task, task_dependencies)
                else:
                    # self.log(self.my_resource_configuration.worker_id, "WEL.HANDLE_EXECUTION()")
                    task_result, task_execution_time_ms = await WorkerExecutionLogic.override_handle_execution(current_task, task_dependencies)

                tasks_executed_by_this_coroutine.append(current_task.id.get_full_id())

                current_task_metrics.execution_time_ms = task_execution_time_ms
                # normalize based on the memory used. Calculate "per input size byte"
                total_input_size = current_task_metrics.input_metrics.downloadable_input_size_bytes + current_task_metrics.input_metrics.hardcoded_input_size_bytes
                current_task_metrics.execution_time_per_input_byte_ms = current_task_metrics.execution_time_ms / total_input_size \
                    if total_input_size > 0 else 0  # 0, not to influence predictions, using current_task_metrics.execution_time_ms would be incorrect
                
                #* 3) HANDLE TASK OUTPUT
                self.log(current_task.id.get_full_id(), f"3) Handling Task Output...")
                if self.planner:
                    # self.log(self.my_resource_configuration.worker_id, "PLANNER.HANDLE_OUTPUT()")
                    output_upload_time_ms = await self.planner.override_handle_output(task_result, current_task, subdag, self.intermediate_storage, self.metadata_storage, self.my_resource_configuration.worker_id)
                else:
                    # self.log(self.my_resource_configuration.worker_id, "WEL.HANDLE_OUTPUT()")
                    output_upload_time_ms = await WorkerExecutionLogic.override_handle_output(task_result, current_task, subdag, self.intermediate_storage, self.metadata_storage, self.my_resource_configuration.worker_id)

                current_task_metrics.output_metrics = TaskOutputMetrics(
                    size_bytes=calculate_data_structure_size(task_result),
                    time_ms=output_upload_time_ms
                )

                if current_task.id.get_full_id() == subdag.sink_node.id.get_full_id():
                    sink_task_executed = True
                    self.log(current_task.id.get_full_id(), f"Sink task finished. Shutting down worker...")
                    break

                # Update Dependency Counters of Downstream Tasks
                updating_dependency_counters_timer = Timer()
                downstream_tasks_ready: list[dag_task_node.DAGTaskNode] = []
                async with self.metadata_storage.batch() as batch:
                    for downstream_task in current_task.downstream_nodes:
                        await batch.atomic_increment_and_get(f"{DEPENDENCY_COUNTER_PREFIX}{downstream_task.id.get_full_id_in_dag(subdag)}")
                    results = await batch.execute()
                    
                for downstream_task, dependencies_met in zip(current_task.downstream_nodes, results):
                    downstream_task_total_dependencies = len(subdag.get_node_by_id(downstream_task.id).upstream_nodes)
                    self.log(current_task.id.get_full_id(), f"Incremented DC of {downstream_task.id.get_full_id()} ({dependencies_met}/{downstream_task_total_dependencies})")
                    if dependencies_met == downstream_task_total_dependencies:
                        await self.metadata_storage.publish(f"{TASK_READY_EVENT_PREFIX}{downstream_task.id.get_full_id_in_dag(subdag)}", b"1")
                        downstream_tasks_ready.append(downstream_task)
                current_task_metrics.update_dependency_counters_time_ms = updating_dependency_counters_timer.stop()

                self.log(current_task.id.get_full_id(), f"4) Handle Fan-Out to {len(current_task.downstream_nodes)} tasks...")

                if len(downstream_tasks_ready) == 0:
                    self.log(current_task.id.get_full_id(), f"No ready downstream tasks found. Shutting down worker...")
                    break # Give up

                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                #* 4) HANDLE DOWNSTREAM TASKS
                self.log(current_task.id.get_full_id(), f"5) Handling Task Output...")
                if self.planner:
                    # self.log(self.my_resource_configuration.worker_id, "PLANNER.HANDLE_DOWNSTREAM()")
                    my_continuation_tasks, total_invocations_count, total_invocation_time_ms = await self.planner.override_handle_downstream(self, downstream_tasks_ready, subdag)
                else:
                    # self.log(self.my_resource_configuration.worker_id, "WEL.HANDLE_DOWNSTREAM()")
                    my_continuation_tasks, total_invocations_count, total_invocation_time_ms = await WorkerExecutionLogic.override_handle_downstream(self, downstream_tasks_ready, subdag)

                current_task_metrics.total_invocations_count = total_invocations_count
                current_task_metrics.total_invocation_time_ms = total_invocation_time_ms

                # Continue with one task in this worker
                if len(my_continuation_tasks) == 0:
                    self.log(current_task.id.get_full_id(), f"No continuation task found with the same resource configuration... Shutting down worker...")
                    break

                self.log(current_task.id.get_full_id(), f"Continuing with first of multiple downstream tasks: {my_continuation_tasks}")
                current_task = my_continuation_tasks[0]
                if len(my_continuation_tasks) > 1:
                    my_other_tasks = my_continuation_tasks[1:]
                    # Execute other tasks on coroutines in this worker
                    for t in my_other_tasks:
                        other_coroutines_i_launched.append(asyncio.create_task(self.execute_branch(subdag.create_subdag(t), my_worker_id), name=f"start_executing_immediate_followup(task={t.id.get_full_id()})"))
        except Exception as e:
            self.log(current_task.id.get_full_id(), f"Error: {str(e)}") # type: ignore
            raise e

        # Wait for my other coroutines executing other tasks
        if len(other_coroutines_i_launched) > 0: 
            self.log(current_task.id.get_full_id(), f"Worker waiting for coroutines: {[t.get_name() for t in other_coroutines_i_launched]}")
            await asyncio.gather(*other_coroutines_i_launched)

        if self.metrics_storage:
            for tid, tm in task_metrics.items(): 
                self.metrics_storage.store_task_metrics(tid, tm)

        self.log(current_task.id.get_full_id(), f"Worker shut down!")

        return tasks_executed_by_this_coroutine, sink_task_executed

    @abstractmethod
    async def delegate(self, subdags: list[dag.SubDAG], called_by_worker: bool = False): 
        """
        {called_by_worker}: indicates if it's a worker invoking another worker, or the Client beggining the execution
        """
        pass

    @staticmethod
    async def store_full_dag(metadata_storage: storage_module.Storage, dag: dag.FullDAG):
        await metadata_storage.set(f"{DAG_PREFIX}{dag.master_dag_id}", cloudpickle.dumps(dag))

    async def get_full_dag(self, dag_id: str) -> tuple[int, dag.FullDAG]:
        serialized_dag = await self.metadata_storage.get(f"{DAG_PREFIX}{dag_id}")
        if serialized_dag is None: raise Exception(f"Could not find DAG with id {dag_id}")
        deserialized = cloudpickle.loads(serialized_dag)
        if not isinstance(deserialized, dag.FullDAG): raise Exception("Error: fulldag is not a DAG instance")
        return (calculate_data_structure_size(deserialized), deserialized)

    @staticmethod
    async def wait_for_result_of_task(metadata_storage: storage_module.Storage, intermediate_storage: storage_module.Storage, task: dag_task_node.DAGTaskNode, dag: dag.FullDAG):
        # Poll Storage for final result. Asynchronous wait
        channel = f"{TASK_COMPLETION_EVENT_PREFIX}{task.id.get_full_id_in_dag(dag)}"
        # Use an event to signal when we've received our message
        message_received = asyncio.Event()
        result = None
        start_time = Timer()
        total_wait_time_ms = 0
        
        def callback(msg: dict):
            nonlocal result, total_wait_time_ms
            message_received.set()
            total_wait_time_ms = start_time.stop()
        
        await metadata_storage.subscribe(channel, callback)
        
        try:
            await message_received.wait()
            final_task_id = task.id.get_full_id_in_dag(dag)
            final_result = await intermediate_storage.get(final_task_id)
            if final_result is not None:
                final_result = cloudpickle.loads(final_result) # type: ignore
                logger.info(f"Final Result Ready: ({final_task_id}) => Size: {calculate_data_structure_size(final_result)} | Type: ({type(final_result)}) | Time: {total_wait_time_ms} ms")
                return final_result
        finally:
            await metadata_storage.unsubscribe(channel)


    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        logger.info(f"Worker({self.my_resource_configuration.worker_id}) Task({task_id}) | {message}")
