import asyncio
from dataclasses import dataclass
import time
import cloudpickle
from abc import ABC, abstractmethod

from src.planning.dag_planner import DAGPlanner
from src.storage.events import TASK_COMPLETION_EVENT_PREFIX, TASK_READY_EVENT_PREFIX
from src.storage.metrics.metrics_types import TaskHardcodedInputMetrics, TaskMetrics, TaskOutputMetrics
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size, get_method_overridden
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.metrics import metrics_storage
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB
from src.utils.logger import create_logger
import src.dag.dag as dag
import src.dag_task_node as dag_task_node
import src.storage.storage as storage_module
from src.workers.worker_execution_logic import WorkerExecutionLogic, WorkerExecutionLogicSingleton

logger = create_logger(__name__)

class Worker(ABC, WorkerExecutionLogic):
    @dataclass
    class Config(ABC):
        intermediate_storage_config: storage_module.Storage.Config
        metadata_storage_config: storage_module.Storage.Config | None = None
        metrics_storage_config: metrics_storage.MetricsStorage.Config | None = None
        planner_config: DAGPlanner.Config | None = None

        @abstractmethod
        def create_instance(self) -> "Worker": pass

    intermediate_storage: storage_module.Storage

    def __init__(self, config: Config):
        self.intermediate_storage = config.intermediate_storage_config.create_instance()
        self.metadata_storage = self.intermediate_storage if not config.metadata_storage_config else config.metadata_storage_config.create_instance()
        self.metrics_storage = config.metrics_storage_config.create_instance() if config.metrics_storage_config else None
        self.planner = config.planner_config.create_instance() if config.planner_config else None

    async def start_executing(self, subdag: dag.SubDAG) -> list[str]:
        if not subdag.root_node: raise Exception(f"AbstractWorker expected a subdag with only 1 root node. Got {len(subdag.root_node)}")
        current_task = subdag.root_node
        tasks_executed_by_this_coroutine = []

        try:
            while True:
                self.my_resource_configuration: TaskWorkerResourceConfiguration = current_task.get_annotation(TaskWorkerResourceConfiguration)
                task_metrics = TaskMetrics(
                    worker_resource_configuration=self.my_resource_configuration,
                    started_at_timestamp = time.time(),
                    input_metrics = [],
                    hardcoded_input_metrics = [],
                    total_input_download_time_ms = 0,
                    execution_time_ms = 0,
                    normalized_execution_time_per_input_byte_ms = 0,
                    update_dependency_counters_time_ms = 0,
                    output_metrics = None, # type: ignore
                    total_invocations_count=0,
                    total_invocation_time_ms=0
                )
                other_coroutines_i_launched = []

                #* 1) DOWNLOAD TASK DEPENDENCIES
                self.log(current_task.id.get_full_id(), f"1) Grabbing {len(current_task.upstream_nodes)} upstream tasks...")
                override_handle_inputs = None
                task_dependencies = { }
                for annotation in current_task.annotations:
                    override_handle_inputs = get_method_overridden(annotation.__class__, WorkerExecutionLogic.override_handle_inputs)
                    if override_handle_inputs: 
                        self.log(self.my_resource_configuration.worker_id, "ANNOTATION.HANDLE_INPUTS()")
                        task_dependencies, task_metrics.input_metrics, task_metrics.total_input_download_time_ms = await override_handle_inputs(self.intermediate_storage, current_task, subdag, self.my_resource_configuration)
                
                if override_handle_inputs is None:
                    self.log(self.my_resource_configuration.worker_id, "WEL.HANDLE_INPUTS()")
                    task_dependencies, task_metrics.input_metrics, task_metrics.total_input_download_time_ms = await WorkerExecutionLogicSingleton.override_handle_inputs(self.intermediate_storage, current_task, subdag, self.my_resource_configuration)

                # METADATA: Register the size of hardcoded arguments as well
                for func_arg in current_task.func_args:
                    if isinstance(func_arg, dag_task_node.DAGTaskNodeId): continue
                    task_metrics.hardcoded_input_metrics.append(TaskHardcodedInputMetrics(size_bytes=calculate_data_structure_size(func_arg)))

                for func_kwarg in current_task.func_kwargs.values():
                    if isinstance(func_kwarg, dag_task_node.DAGTaskNodeId): continue
                    task_metrics.hardcoded_input_metrics.append(TaskHardcodedInputMetrics(size_bytes=calculate_data_structure_size(func_kwarg)))
                
                #* 2) EXECUTE TASK
                self.log(current_task.id.get_full_id(), f"2) Executing Task...")
                override_handle_execution = None
                task_execution_time_ms = -1
                task_result = None
                for annotation in current_task.annotations:
                    override_handle_execution = get_method_overridden(annotation.__class__, WorkerExecutionLogic.override_handle_execution)
                    if override_handle_execution: 
                        self.log(self.my_resource_configuration.worker_id, "ANNOTATION.HANDLE_EXECUTION()")
                        task_result, task_execution_time_ms = await override_handle_execution(current_task, task_dependencies)
                
                if override_handle_execution is None:
                    self.log(self.my_resource_configuration.worker_id, "WEL.HANDLE_EXECUTION()")
                    task_result, task_execution_time_ms = await WorkerExecutionLogicSingleton.override_handle_execution(current_task, task_dependencies)

                tasks_executed_by_this_coroutine.append(current_task.id.get_full_id())

                task_metrics.execution_time_ms = task_execution_time_ms
                # normalize based on the memory used. Calculate "per input size byte"
                total_input_size = sum(m.size_bytes for m in task_metrics.input_metrics) + sum(m.size_bytes for m in task_metrics.hardcoded_input_metrics)
                task_metrics.normalized_execution_time_per_input_byte_ms = task_metrics.execution_time_ms \
                    * (task_metrics.worker_resource_configuration.memory_mb / BASELINE_MEMORY_MB)  \
                    / total_input_size if task_metrics.worker_resource_configuration and total_input_size > 0 else 0 # 0, not to influence predictions, using task_metrics.execution_time_ms would be incorrect
                
                #* 3) HANDLE TASK OUTPUT
                self.log(current_task.id.get_full_id(), f"3) Handling Task Output...")
                override_handle_output = None
                output_upload_time_ms = -1
                for annotation in current_task.annotations:
                    override_handle_output = get_method_overridden(annotation.__class__, WorkerExecutionLogic.override_handle_output)
                    if override_handle_output: 
                        self.log(self.my_resource_configuration.worker_id, "ANNOTATION.HANDLE_OUTPUT()")
                        output_upload_time_ms = await override_handle_output(task_result, current_task, subdag, self.intermediate_storage, self.metadata_storage)
                
                if override_handle_output is None:
                    self.log(self.my_resource_configuration.worker_id, "WEL.HANDLE_OUTPUT()")
                    output_upload_time_ms = await WorkerExecutionLogicSingleton.override_handle_output(task_result, current_task, subdag, self.intermediate_storage, self.metadata_storage)

                task_metrics.output_metrics = TaskOutputMetrics(
                    size_bytes=calculate_data_structure_size(task_result),
                    time_ms=output_upload_time_ms,
                    normalized_time_ms=output_upload_time_ms * (task_metrics.worker_resource_configuration.memory_mb / BASELINE_MEMORY_MB) if task_metrics.worker_resource_configuration else 0
                )

                if current_task.id.get_full_id() == subdag.sink_node.id.get_full_id():
                    self.log(current_task.id.get_full_id(), f"Sink task finished. Shutting down worker...")
                    if self.metrics_storage: self.metrics_storage.store_task_metrics(current_task.id.get_full_id_in_dag(subdag), task_metrics)
                    break

                # Update Dependency Counters of Downstream Tasks
                updating_dependency_counters_timer = Timer()
                downstream_tasks_ready: list[dag_task_node.DAGTaskNode] = []
                for downstream_task in current_task.downstream_nodes:
                    dc_key = f"dependency-counter-{downstream_task.id.get_full_id_in_dag(subdag)}"
                    dependencies_met = await self.metadata_storage.atomic_increment_and_get(dc_key)
                    downstream_task_total_dependencies = len(subdag.get_node_by_id(downstream_task.id).upstream_nodes)
                    self.log(current_task.id.get_full_id(), f"Incremented DC of {dc_key} ({dependencies_met}/{downstream_task_total_dependencies})")
                    if dependencies_met == downstream_task_total_dependencies:
                        await self.metadata_storage.publish(f"{TASK_READY_EVENT_PREFIX}{downstream_task.id.get_full_id_in_dag(subdag)}", b"1")
                        downstream_tasks_ready.append(downstream_task)
                task_metrics.update_dependency_counters_time_ms = updating_dependency_counters_timer.stop()

                self.log(current_task.id.get_full_id(), f"4) Handle Fan-Out {current_task.id.get_full_id_in_dag(subdag)} => {[t.id.get_full_id_in_dag(subdag) for t in current_task.downstream_nodes]}")

                if len(downstream_tasks_ready) == 0:
                    self.log(current_task.id.get_full_id(), f"No ready downstream tasks found. Shutting down worker...")
                    if self.metrics_storage: self.metrics_storage.store_task_metrics(current_task.id.get_full_id_in_dag(subdag), task_metrics)
                    break # Give up

                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                #* 4) HANDLE DOWNSTREAM TASKS
                self.log(current_task.id.get_full_id(), f"5) Handling Task Output...")
                override_handle_downstream = None
                total_invocations_count = -1
                total_invocation_time_ms = -1
                my_continuation_tasks = []
                for annotation in current_task.annotations:
                    override_handle_downstream = get_method_overridden(annotation.__class__, WorkerExecutionLogic.override_handle_downstream)
                    if override_handle_downstream: 
                        self.log(self.my_resource_configuration.worker_id, "ANNOTATION.HANDLE_DOWNSTREAM()")
                        my_continuation_tasks, total_invocations_count, total_invocation_time_ms = await override_handle_downstream(self, downstream_tasks_ready, subdag)
                
                if override_handle_downstream is None:
                    self.log(self.my_resource_configuration.worker_id, "WEL.HANDLE_DOWNSTREAM()")
                    my_continuation_tasks, total_invocations_count, total_invocation_time_ms = await WorkerExecutionLogicSingleton.override_handle_downstream(self, downstream_tasks_ready, subdag)

                task_metrics.total_invocations_count = total_invocations_count
                task_metrics.total_invocation_time_ms = total_invocation_time_ms

                if self.metrics_storage: self.metrics_storage.store_task_metrics(current_task.id.get_full_id_in_dag(subdag), task_metrics)

                # Continue with one task in this worker
                if len(my_continuation_tasks) == 0:
                    self.log(current_task.id.get_full_id(), f"No continuation task found with the same resource configuration... Shutting down worker...")
                    if self.metrics_storage: self.metrics_storage.store_task_metrics(current_task.id.get_full_id_in_dag(subdag), task_metrics)
                    break

                self.log(current_task.id.get_full_id(), f"Continuing with first of multiple downstream tasks: {my_continuation_tasks}")
                current_task = my_continuation_tasks[0]
                if len(my_continuation_tasks) > 1:
                    my_other_tasks = my_continuation_tasks[1:]
                    # Execute other tasks on coroutines in this worker
                    for t in my_other_tasks: 
                        other_coroutines_i_launched.append(asyncio.create_task(self.start_executing(subdag.create_subdag(t))))
        except Exception as e:
            self.log(current_task.id.get_full_id(), f"Error: {str(e)}") # type: ignore
            raise e

        # Cleanup
        self.log(current_task.id.get_full_id(), f"Worker shut down!")
        if self.metrics_storage:
            await self.metrics_storage.flush()

        # Wait for my other coroutines executing other tasks
        if len(other_coroutines_i_launched) > 0: await asyncio.gather(*other_coroutines_i_launched)

        return tasks_executed_by_this_coroutine

    @abstractmethod
    async def delegate(self, subdags: list[dag.SubDAG], called_by_worker: bool = False): 
        """
        {called_by_worker}: indicates if it's a worker invoking another worker, or the Client beggining the execution
        """
        pass

    @staticmethod
    async def store_full_dag(metadata_storage: storage_module.Storage, dag: dag.FullDAG):
        await metadata_storage.set(f"dag-{dag.master_dag_id}", cloudpickle.dumps(dag))

    async def get_full_dag(self, dag_id: str) -> tuple[int, dag.FullDAG]:
        serialized_dag = await self.metadata_storage.get(f"dag-{dag_id}")
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
