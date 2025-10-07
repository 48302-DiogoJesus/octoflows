import asyncio
from dataclasses import dataclass
import time
from typing import Any
import cloudpickle
from abc import ABC, abstractmethod
import uuid

from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.storage.events import TASK_COMPLETED_EVENT_PREFIX
from src.storage.metadata.metrics_types import TaskInputDownloadMetrics, TaskOutputMetrics
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size_bytes
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.utils.logger import create_logger
import src.dag.dag as dag
import src.dag_task_node as dag_task_node
import src.storage.storage as storage_module
from src.storage.prefixes import DAG_PREFIX
from src.storage.metadata.metadata_storage import MetadataStorage
from src.utils.errors import CancelCurrentWorkerLoopException

logger = create_logger(__name__)

class TaskOutputNotAvailableException(Exception):
    def __init__(self, worker_id: str, task_id: str, required_by_task_id: str):
        message = f"W({worker_id}) Task {task_id}'s data is not available yet! Required by {required_by_task_id}"
        super().__init__(message)

class Worker(ABC):

    @dataclass
    class Config(ABC):
        planner_config: AbstractDAGPlanner.BaseConfig
        intermediate_storage_config: storage_module.Storage.Config
        metadata_storage_config: MetadataStorage.Config
        optimized_dag: str | None = None

        @abstractmethod
        def create_instance(self) -> "Worker": pass

    intermediate_storage: storage_module.Storage
    config: Config

    def __init__(self, config: Config):
        self.intermediate_storage = config.intermediate_storage_config.create_instance()
        self.metadata_storage = config.metadata_storage_config.create_instance()
        self.planner = config.planner_config.create_instance()
        self.config = config

    async def execute_branch(self, subdag: dag.SubDAG, fulldag: dag.FullDAG, my_worker_id: str, is_dupping: bool = False) -> None:
        """
        Note: {my_worker_id} can't be None. for flexible worker it will be prefixed "flexible-"

        {is_dupping} indicates this task is being dupped, meaning it won't execute downstream and it won't check for dup cancelation flag
        """
        if not subdag.root_node: raise Exception(f"AbstractWorker expected a subdag with only 1 root node. Got {len(subdag.root_node)}")
        current_task = subdag.root_node
        tasks_executed_by_this_coroutine: list[dag_task_node.DAGTaskNode] = []
        other_coroutines_i_launched = []

        branch_id = uuid.uuid4().hex
        try:
            while True:
                if not is_dupping:
                    self.my_resource_configuration: TaskWorkerResourceConfiguration = current_task.worker_config
                    # To help understand locality decisions afterwards, at the dashboard
                    _my_resource_configuration_with_flexible_worker_id = self.my_resource_configuration.clone()
                    _my_resource_configuration_with_flexible_worker_id.worker_id = my_worker_id
                    self.my_worker_id = my_worker_id
                    # not when dupping, otherwise would override our real id by the worker_id assigned to the duppable task
                    current_task.metrics.worker_resource_configuration = _my_resource_configuration_with_flexible_worker_id # type: ignore
                    current_task.metrics.started_at_timestamp_s = time.time()
                    current_task.metrics.planner_used_name = self.planner.planner_name if self.planner else None

                if self.my_resource_configuration.worker_id is not None:
                    assert self.my_worker_id == self.my_resource_configuration.worker_id

                if not await current_task.is_handling.set_if_not_set():
                    # avoid duplicate execution on same worker
                    raise CancelCurrentWorkerLoopException("Task is/was already being handled by this worker on another coroutine. Aborting")
                
                await self.planner.wel_before_task_handling(self.planner, self, self.metadata_storage.storage, subdag, current_task, is_dupping)
                
                # don't store metrics for dupped tasks
                if not is_dupping: tasks_executed_by_this_coroutine.append(current_task)

                #* 1) DOWNLOAD TASK DEPENDENCIES
                self.log(current_task.id.get_full_id() + "++" + branch_id, f"1) Grabbing {len(current_task.upstream_nodes)} upstream tasks...", is_dupping)
                _download_dependencies_timer = Timer()
                task_dependencies: dict[str, Any] = {}

                # METADATA: Register the size of hardcoded arguments that won't be downloaded!
                for func_arg in current_task.func_args:
                    if isinstance(func_arg, dag_task_node.DAGTaskNodeId): continue
                    if isinstance(func_arg, dag.HardcodedDependencyId): continue # these are accounted for in the normal download metrics
                    current_task.metrics.input_metrics.hardcoded_input_size_bytes += calculate_data_structure_size_bytes(cloudpickle.dumps(func_arg))

                for func_kwarg in current_task.func_kwargs.values():
                    if isinstance(func_kwarg, dag_task_node.DAGTaskNodeId): continue
                    if isinstance(func_kwarg, dag.HardcodedDependencyId): continue # these are accounted for in the normal download metrics
                    current_task.metrics.input_metrics.hardcoded_input_size_bytes += calculate_data_structure_size_bytes(cloudpickle.dumps(func_kwarg))

                upstream_tasks_without_cached_results: list[dag_task_node.DAGTaskNode] = []
                for t in current_task.upstream_nodes:
                    if t.cached_result is None:
                        upstream_tasks_without_cached_results.append(t)
                    else:
                        self.log(current_task.id.get_full_id() + "++" + branch_id, f"Input {t.id.get_full_id()} was in cache!", is_dupping)

                # Always fetch hardcoded inputs that are not present locally
                # {subdag.cached_hardcoded_data_map} stored hardcoded data only once, to avoid repetition (tasks store references to the storage_ids until they need to be executed; then they replace that by the cached value or grab it from storage and then cache it)
                non_repeated_storage_ids = { arg.storage_id for arg in current_task.func_args if isinstance(arg, dag.HardcodedDependencyId) and arg.storage_id not in subdag.cached_hardcoded_data_map }
                non_repeated_storage_ids |= { arg.storage_id for arg in current_task.func_kwargs.values() if isinstance(arg, dag.HardcodedDependencyId) and arg.storage_id not in subdag.cached_hardcoded_data_map }

                time_spent_downloading: dict[str, float] = {}

                async def _fetch_and_cache(storage_id: str):
                    self.log(current_task.id.get_full_id() + "++" + branch_id, f"Fetching hardcoded dependency: {storage_id}", is_dupping)

                    timer = Timer()
                    data_serialized = await self.intermediate_storage.get(storage_id)
                    download_time = timer.stop()
                    if data_serialized is None: raise TaskOutputNotAvailableException(self.my_worker_id, storage_id, current_task.id.get_full_id())
                    data = cloudpickle.loads(data_serialized)
                    subdag.cached_hardcoded_data_map[storage_id] = data
                    time_spent_downloading[storage_id] = download_time

                # Launch all downloads concurrently
                await asyncio.gather(*(_fetch_and_cache(sid) for sid in non_repeated_storage_ids))

                new_func_args = []
                for arg in current_task.func_args:
                    if isinstance(arg, dag.HardcodedDependencyId):
                        d = subdag.cached_hardcoded_data_map[arg.storage_id]
                        new_func_args.append(d)
                        current_task.metrics.input_metrics.input_download_metrics[arg.storage_id] = TaskInputDownloadMetrics(
                            serialized_size_bytes=calculate_data_structure_size_bytes(cloudpickle.dumps(d)),
                            time_ms=time_spent_downloading.get(arg.storage_id, None) # note: could be None if was cached
                        )
                    else:
                        new_func_args.append(arg) # don't store input_metrics for non hardcoded inputs. that will be done later

                new_func_kwargs = {}
                for key, arg in current_task.func_kwargs.items():
                    if isinstance(arg, dag.HardcodedDependencyId):
                        d = subdag.cached_hardcoded_data_map[arg.storage_id]
                        new_func_kwargs[key] = d
                        current_task.metrics.input_metrics.input_download_metrics[arg.storage_id] = TaskInputDownloadMetrics(
                            serialized_size_bytes=calculate_data_structure_size_bytes(cloudpickle.dumps(d)),
                            time_ms=time_spent_downloading.get(arg.storage_id, None) # note: could be None if was cached
                        )
                    else:
                        new_func_kwargs[key] = arg # don't store input_metrics for non hardcoded inputs. that will be done later

                current_task.func_args = tuple(new_func_args)
                current_task.func_kwargs = new_func_kwargs

                res = await self.planner.wel_override_handle_inputs(self.planner, self.intermediate_storage, self.metadata_storage.storage, current_task, subdag, upstream_tasks_without_cached_results, self.my_resource_configuration, task_dependencies)
                assert res is not None
                tasks_to_fetch, tasks_fetched_by_handler, wait_until_coroutine = res

                tasks_with_cached_results = list(
                    set([un.id.get_full_id() for un in current_task.upstream_nodes]) - 
                    set([t.id.get_full_id() for t in tasks_to_fetch]) - 
                    set(tasks_fetched_by_handler)
                )

                # Grab outputs that are already cached locally
                for t in current_task.upstream_nodes:
                    if t.id.get_full_id() not in tasks_with_cached_results: continue
                    if not t.cached_result: continue
                    
                    task_dependencies[t.id.get_full_id()] = t.cached_result.result

                    current_task.metrics.input_metrics.input_download_metrics[t.id.get_full_id()] = TaskInputDownloadMetrics(
                        serialized_size_bytes=calculate_data_structure_size_bytes(cloudpickle.dumps(t.cached_result.result)),
                        time_ms=None
                    )
                
                if tasks_to_fetch:
                    async def _fetch_task_result(utask):
                        _timer = Timer()
                        serialized_task_result = await self.intermediate_storage.get(utask.id.get_full_id_in_dag(subdag))
                        if serialized_task_result is None: 
                            raise TaskOutputNotAvailableException(self.my_worker_id, utask.id.get_full_id(), current_task.id.get_full_id())

                        time_to_fetch_ms = _timer.stop()
                        deserialized_result = cloudpickle.loads(serialized_task_result)

                        # Store results
                        task_dependencies[utask.id.get_full_id()] = deserialized_result
                        current_task.metrics.input_metrics.input_download_metrics[utask.id.get_full_id()] = TaskInputDownloadMetrics(
                            serialized_size_bytes=calculate_data_structure_size_bytes(serialized_task_result),
                            time_ms=time_to_fetch_ms
                        )

                    await asyncio.gather(*(_fetch_task_result(utask) for utask in tasks_to_fetch))

                # Handle wait_until_coroutine if present
                if wait_until_coroutine is not None: 
                    logger.info(f"[WAIT UNTIL] Waiting for wait_until_coroutine to complete for task: {current_task.id.get_full_id()}")
                    await wait_until_coroutine
                    logger.info(f"[WAIT UNTIL] Complete for task: {current_task.id.get_full_id()}")
                    # wait_until_coroutine may have fetched some results, we need to use them
                    for utask in current_task.upstream_nodes:
                        utask_id = utask.id.get_full_id()
                        if utask_id not in task_dependencies and utask.cached_result is not None:
                            task_dependencies[utask_id] = utask.cached_result.result

                current_task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms = _download_dependencies_timer.stop() if len(current_task.upstream_nodes) > 0 and any([t.worker_config.worker_id != self.my_worker_id for t in current_task.upstream_nodes]) else None

                # Store raw values, normalization will be done during prediction
                await self.planner.wel_before_task_execution(self.planner, self, self.metadata_storage, subdag, current_task, is_dupping)

                #* 2) EXECUTE TASK
                self.log(current_task.id.get_full_id() + "++" + branch_id, f"2) Executing Task...", is_dupping)
                exec_timer = Timer()
                deserialized_task_result = await asyncio.to_thread(current_task.invoke, dependencies=task_dependencies)
                task_execution_time_ms = exec_timer.stop()

                current_task.metrics.tp_execution_time_ms = task_execution_time_ms
                # normalize based on the memory used. Calculate "per input size byte"
                total_input_size = current_task.metrics.input_metrics.hardcoded_input_size_bytes + sum([input_metric.serialized_size_bytes for input_metric in current_task.metrics.input_metrics.input_download_metrics.values()])
                current_task.metrics.execution_time_per_input_byte_ms = current_task.metrics.tp_execution_time_ms / total_input_size \
                    if total_input_size > 0 else None

                #* 3) HANDLE TASK OUTPUT
                serialized_task_result = cloudpickle.dumps(deserialized_task_result)
                current_task.metrics.output_metrics = TaskOutputMetrics(
                    serialized_size_bytes=calculate_data_structure_size_bytes(serialized_task_result),
                    tp_time_ms=None
                )

                self.log(current_task.id.get_full_id() + "++" + branch_id, f"3) Handling Task Output...", is_dupping)
                upload_output = await self.planner.wel_override_should_upload_output(self.planner, current_task, subdag, self, self.metadata_storage, is_dupping)
                
                if upload_output:
                    output_upload_timer = Timer()
                    await self.intermediate_storage.set(current_task.id.get_full_id_in_dag(subdag), serialized_task_result)
                    current_task.metrics.output_metrics.tp_time_ms = output_upload_timer.stop()
                    current_task.upload_complete.set()
                    self.log(current_task.id.get_full_id() + "++" + branch_id, f"Uploaded Output", is_dupping)
                else:
                    self.log(current_task.id.get_full_id() + "++" + branch_id, f"Won't upload output", is_dupping)

                if not is_dupping:
                    receivers = await self.metadata_storage.storage.publish(f"{TASK_COMPLETED_EVENT_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}", b"1")
                    self.log(current_task.id.get_full_id() + "++" + branch_id, f"Published COMPLETED event for {current_task.id.get_full_id()} | {receivers}, is_dupping receivers")

                if current_task.id.get_full_id() == subdag.sink_node.id.get_full_id():
                    self.log(current_task.id.get_full_id() + "++" + branch_id, f"Sink task finished. Shutting down worker...", is_dupping)
                    break

                # Update Dependency Counters of Downstream Tasks
                downstream_tasks_ready = await self.planner.wel_update_dependency_counters(self.planner, self, self.metadata_storage, subdag, current_task, is_dupping)
                assert downstream_tasks_ready is not None
                
                self.log(current_task.id.get_full_id() + "++" + branch_id, f"4) Handle Downstream to {len(current_task.downstream_nodes)} tasks...", is_dupping)

                if len(downstream_tasks_ready) == 0:
                    self.log(current_task.id.get_full_id() + "++" + branch_id, f"No ready downstream tasks found. Shutting down worker...", is_dupping)
                    break # Give up

                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                #* 4) HANDLE DOWNSTREAM TASKS
                self.log(current_task.id.get_full_id() + "++" + branch_id, f"5) Handling Downstream Tasks...", is_dupping)
                my_continuation_tasks = await self.planner.wel_override_handle_downstream(self.planner, fulldag, current_task, self, downstream_tasks_ready, subdag)
                assert my_continuation_tasks is not None

                # Continue with one task in this worker
                if len(my_continuation_tasks) == 0:
                    self.log(current_task.id.get_full_id() + "++" + branch_id, f"No continuation tasks found... Shutting down, is_dupping worker...")
                    break

                self.log(current_task.id.get_full_id() + "++" + branch_id, f"Continuing with first of multiple downstream tasks: {my_continuation_tasks}", is_dupping)
                current_task = my_continuation_tasks[0]
                # note: at the end of a taskdup, it may execute other tasks, those are not dupped, hence "is_dupping=False"
                is_dupping = False
                if len(my_continuation_tasks) > 1:
                    my_other_tasks = my_continuation_tasks[1:]
                    # Execute other tasks on separate coroutines in this worker
                    for t in my_other_tasks:
                        other_coroutines_i_launched.append(
                            asyncio.create_task(
                                self.execute_branch(subdag.create_subdag(t), fulldag, my_worker_id, is_dupping=False), 
                                name=f"start_executing_immediate_followup(task={t.id.get_full_id()})")
                            )
        except CancelCurrentWorkerLoopException as e:
            self.log(current_task.id.get_full_id() + "++" + branch_id, f"CancelCurrentWorkerLoopException: {str(e)}", is_dupping)
            pass
        except TaskOutputNotAvailableException as e:
            if is_dupping:
                # CANCEL dupping because at least 1 of the dependencies of duppable task may not be available
                self.log(current_task.id.get_full_id() + "++" + branch_id, f"TaskOutputNotAvailableException: {str(e)}") # type: ignor, is_duppinge
                pass
            else:
                raise e
        except Exception as e:
            self.log(current_task.id.get_full_id() + "++" + branch_id, f"ExecuteBranch Error: {str(e)}") # type: ignor, is_duppinge
            raise e
        finally:
            # await current_task.is_handling.clear()
            pass

        # Wait for my other coroutines executing other tasks
        if len(other_coroutines_i_launched) > 0: 
            self.log(current_task.id.get_full_id() + "++" + branch_id, f"Worker waiting for coroutines: {[t.get_name() for t in other_coroutines_i_launched]}", is_dupping)
            await asyncio.gather(*other_coroutines_i_launched)

        for task_executed in tasks_executed_by_this_coroutine: 
            await self.metadata_storage.store_task_metrics(task_executed.id.get_full_id_in_dag(subdag), task_executed.metrics)

        self.log(current_task.id.get_full_id() + "++" + branch_id, f"Worker shut down!", is_dupping)
        # print the names of the coroutines that are still running in the program in a single print
        this_coro = asyncio.current_task()
        logger.info(f"W({self.my_resource_configuration.worker_id}) Coroutines still running: {[t.get_name() for t in asyncio.all_tasks() if not this_coro or t.get_name() != this_coro.get_name()]}")
        return None

    @abstractmethod
    async def delegate(self, subdags: list[dag.SubDAG], fulldag: dag.FullDAG, called_by_worker: bool = False): 
        """
        {called_by_worker}: indicates if it's a worker invoking another worker, or the Client beggining the execution
        """
        pass

    @abstractmethod
    async def warmup(self, dag_id: str, resource_configurations: list[TaskWorkerResourceConfiguration]):
        """
        Warmup the worker with the given resource configuration by doing a "special" invocation
        """
        pass

    @staticmethod
    async def store_full_dag(metadata_storage: storage_module.Storage, dag: dag.FullDAG):
        await metadata_storage.set(f"{DAG_PREFIX}{dag.master_dag_id}", cloudpickle.dumps(dag))

    async def get_full_dag(self, dag_id: str) -> tuple[int, dag.FullDAG]:
        serialized_dag = await self.metadata_storage.storage.get(f"{DAG_PREFIX}{dag_id}")
        if serialized_dag is None: raise Exception(f"Could not find DAG with id {dag_id}")
        deserialized_dag = cloudpickle.loads(serialized_dag)
        if not isinstance(deserialized_dag, dag.FullDAG): raise Exception("Error: fulldag is not a DAG instance")
        return (calculate_data_structure_size_bytes(serialized_dag), deserialized_dag)

    @staticmethod
    async def wait_for_result_of_task(metadata_storage: storage_module.Storage, intermediate_storage: storage_module.Storage, task: dag_task_node.DAGTaskNode, dag: dag.FullDAG, timer: Timer) -> tuple[Any, float]:
        # Poll Storage for final result. Asynchronous wait
        channel = f"{TASK_COMPLETED_EVENT_PREFIX}{task.id.get_full_id_in_dag(dag)}"
        # Use an event to signal when we've received our message
        message_received = asyncio.Event()
        result = None
        time_of_final_notification = -1
        
        def callback(msg: dict, _):
            nonlocal result
            message_received.set()
        
        await metadata_storage.subscribe(channel, callback, coroutine_tag="wait_for_result_of_task")
        
        try:
            await message_received.wait()
            time_of_final_notification = timer.stop()

            final_task_id = task.id.get_full_id_in_dag(dag)
            final_result = await intermediate_storage.get(final_task_id)
            if final_result is not None:
                final_result = cloudpickle.loads(final_result) # type: ignore
                return (final_result, time_of_final_notification)
            else:
                return (None, -1)
        finally:
            await metadata_storage.unsubscribe(channel, None)


    def log(self, task_id: str, message: str, is_dupping: bool = False):
        """Log a message with worker ID prefix."""
        curr_coro = asyncio.current_task()
        coro_name = curr_coro.get_name() if curr_coro else "Unknown"
        logger.info(f"Coro({coro_name}) Dupping({is_dupping}) W({self.my_resource_configuration.worker_id}) T({task_id}) | {message}")
