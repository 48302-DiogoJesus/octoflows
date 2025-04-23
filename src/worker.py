from pympler import asizeof
from collections.abc import Iterable
import asyncio
import base64
from dataclasses import dataclass, field
import json
import pickle
import time
from typing import Any
import uuid
import cloudpickle
import aiohttp
from abc import ABC, abstractmethod

from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size
from src.worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.metrics import metrics_storage
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, TaskHardcodedInputMetrics, TaskMetrics, TaskInputMetrics, TaskOutputMetrics, TaskInvocationMetrics
from src.utils.logger import create_logger
import src.dag.dag as dag
import src.dag_task_node as dag_task_node
import src.storage.storage as storage_module

logger = create_logger(__name__)

class Worker(ABC):
    @dataclass
    class Config(ABC):
        intermediate_storage_config: storage_module.Storage.Config
        metadata_storage_config: storage_module.Storage.Config | None = None
        metrics_storage_config: metrics_storage.MetricsStorage.Config | None = None
        available_resource_configurations: list[TaskWorkerResourceConfiguration] = field(default_factory=list)
        
        def __post_init__(self):
            """
            Sort the available_resource_configurations by memory_mb
            Greatest {memory_mb} first
            """
            self.available_resource_configurations.sort(key=lambda x: x.memory_mb, reverse=True)

        @abstractmethod
        def create_instance(self) -> "Worker": pass

    intermediate_storage: storage_module.Storage
    worker_id: str

    def __init__(self, config: Config):
        self.worker_id = str(uuid.uuid4())
        self.intermediate_storage = config.intermediate_storage_config.create_instance()
        self.metadata_storage = self.intermediate_storage if not config.metadata_storage_config else config.metadata_storage_config.create_instance()
        self.metrics_storage = config.metrics_storage_config.create_instance() if config.metrics_storage_config else None
        self.available_resource_configurations = config.available_resource_configurations

    async def start_executing(self, subdag: dag.SubDAG):
        if not subdag.root_node: raise Exception(f"AbstractWorker expected a subdag with only 1 root node. Got {len(subdag.root_node)}")
        task = subdag.root_node

        try:
            while True:
                workerResourceConfig: TaskWorkerResourceConfiguration | None = task.get_annotation(TaskWorkerResourceConfiguration)
                task_metrics = TaskMetrics(
                    worker_id = self.worker_id,
                    worker_resource_configuration=workerResourceConfig,
                    started_at_timestamp = time.time(),
                    input_metrics = [],
                    hardcoded_input_metrics = [],
                    total_input_download_time_ms = 0,
                    execution_time_ms = 0,
                    normalized_execution_time_per_input_byte_ms = 0,
                    update_dependency_counters_time_ms = 0,
                    output_metrics = None, # type: ignore
                    downstream_invocation_times = None,
                    total_invocation_time_ms=0
                )
                # 1. DOWNLOAD DEPENDENCIES
                self.log(task.id.get_full_id_in_dag(subdag), f"1) Grabbing Dependencies")
                task_dependencies: dict[str, Any] = {}
                dependency_download_timer = Timer()
                # Dynamic fan-outs
                logger.info(f"STATIC FAN-OUT: Grabbing {len(task.upstream_nodes)} upstream tasks: {[tt.id for tt in task.upstream_nodes]}")
                for dependency_task in task.upstream_nodes:
                    fotimer = Timer()
                    task_output = self.intermediate_storage.get(dependency_task.id.get_full_id_in_dag(subdag))
                    if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id_in_dag(subdag)}'s data is not available")
                    task_dependencies[dependency_task.id.get_full_id()] = cloudpickle.loads(task_output)
                    task_metrics.input_metrics.append(TaskInputMetrics(
                        task_id=dependency_task.id.get_full_id_in_dag(subdag),
                        size_bytes=calculate_data_structure_size(task_dependencies[dependency_task.id.get_full_id()]),
                        time_ms=fotimer.stop(),
                        normalized_time_ms=fotimer.stop() * (task_metrics.worker_resource_configuration.memory_mb / BASELINE_MEMORY_MB) if task_metrics.worker_resource_configuration else 0
                    ))
                
                # Register the size of hardcoded arguments as well
                for func_arg in task.func_args:
                    if isinstance(func_arg, dag_task_node.DAGTaskNodeId): continue
                    task_metrics.hardcoded_input_metrics.append(TaskHardcodedInputMetrics(size_bytes=calculate_data_structure_size(func_arg)))

                for func_kwarg in task.func_kwargs.values():
                    if isinstance(func_kwarg, dag_task_node.DAGTaskNodeId): continue
                    task_metrics.hardcoded_input_metrics.append(TaskHardcodedInputMetrics(size_bytes=calculate_data_structure_size(func_kwarg)))

                task_metrics.total_input_download_time_ms = dependency_download_timer.stop()
                # 2. EXECUTE TASK
                exec_timer = Timer()
                self.log(task.id.get_full_id_in_dag(subdag), f"2) Executing...")
                task_result = task.invoke(dependencies=task_dependencies)
                task_metrics.execution_time_ms = exec_timer.stop()
                total_input_size = sum(m.size_bytes for m in task_metrics.input_metrics) + sum(m.size_bytes for m in task_metrics.hardcoded_input_metrics)
                """
                normalize based on the memory used
                calculate "per input size byte", using a scaling factor of 60%
                """
                task_metrics.normalized_execution_time_per_input_byte_ms = task_metrics.execution_time_ms \
                    * (task_metrics.worker_resource_configuration.memory_mb / BASELINE_MEMORY_MB)  \
                    / total_input_size if task_metrics.worker_resource_configuration else 0 # 0, not to influence predictions, using task_metrics.execution_time_ms would be incorrect
                self.log(task.id.get_full_id_in_dag(subdag), f"3) Done! Writing output to storage...")
                output_upload_timer = Timer()
                task_result_serialized = cloudpickle.dumps(task_result)
                self.intermediate_storage.set(task.id.get_full_id_in_dag(subdag), task_result_serialized)
                task_metrics.output_metrics = TaskOutputMetrics(
                    size_bytes=calculate_data_structure_size(task_result),
                    time_ms=output_upload_timer.stop(),
                    normalized_time_ms=output_upload_timer.stop() * (task_metrics.worker_resource_configuration.memory_mb / BASELINE_MEMORY_MB) if task_metrics.worker_resource_configuration else 0
                )

                if len(task.downstream_nodes) == 0: 
                    self.log(task.id.get_full_id_in_dag(subdag), f"Last Task finished. Shutting down worker...")
                    if self.metrics_storage: self.metrics_storage.store_task_metrics(task.id.get_full_id_in_dag(subdag), task_metrics)
                    break

                # 3. HANDLE FAN-OUT (1-1 or 1-N)
                self.log(task.id.get_full_id_in_dag(subdag), f"4) Handle Fan-Out {task.id.get_full_id_in_dag(subdag)} => {[t.id.get_full_id_in_dag(subdag) for t in task.downstream_nodes]}")
                ready_downstream: list[dag_task_node.DAGTaskNode] = []
                
                updating_dependency_counters_timer = Timer()

                for downstream_task in task.downstream_nodes:
                    dc_key = f"dependency-counter-{downstream_task.id.get_full_id_in_dag(subdag)}"
                    dependencies_met = self.metadata_storage.atomic_increment_and_get(dc_key)
                    downstream_task_total_dependencies = len(subdag.get_node_by_id(downstream_task.id).upstream_nodes)
                    self.log(task.id.get_full_id_in_dag(subdag), f"Incremented DC of {dc_key} ({dependencies_met}/{downstream_task_total_dependencies})")
                    if dependencies_met == downstream_task_total_dependencies: ready_downstream.append(downstream_task)

                task_metrics.update_dependency_counters_time_ms = updating_dependency_counters_timer.stop()

                # Delegate Downstream Tasks Execution
                if len(ready_downstream) == 0:
                    self.log(task.id.get_full_id_in_dag(subdag), f"No ready downstream tasks found. Shutting down worker...")
                    if self.metrics_storage: self.metrics_storage.store_task_metrics(task.id.get_full_id_in_dag(subdag), task_metrics)
                    break  # Give up

                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                continuation_task = ready_downstream[0] # choose the first task
                tasks_to_delegate = ready_downstream[1:]
                coroutines = []
                total_invocation_time_timer = Timer()

                task_metrics.downstream_invocation_times = []
                for t in tasks_to_delegate:
                    workerResourcesConfig = t.get_annotation(TaskWorkerResourceConfiguration)
                    if workerResourcesConfig is None and len(self.available_resource_configurations) > 0:
                        workerResourcesConfig = self.available_resource_configurations[0]
                        
                    self.log(task.id.get_full_id_in_dag(subdag), f"Delegating downstream task: {t} with resources: {workerResourcesConfig}")
                    delegate_invoke_timer = Timer()
                    coroutines.append(self.delegate(
                        subdag.create_subdag(t),
                        resource_configuration=workerResourcesConfig,
                        called_by_worker=True
                    ))
                    task_metrics.downstream_invocation_times.append(TaskInvocationMetrics(task_id=t.id.get_full_id_in_dag(subdag), time_ms=delegate_invoke_timer.stop()))
                
                await asyncio.gather(*coroutines) # wait for the delegations to be accepted
                task_metrics.total_invocation_time_ms = total_invocation_time_timer.stop()

                if self.metrics_storage: self.metrics_storage.store_task_metrics(task.id.get_full_id_in_dag(subdag), task_metrics)

                # Continue with one task in this worker
                self.log(task.id.get_full_id_in_dag(subdag), f"Continuing with first of multiple downstream tasks: {continuation_task}")
                task = continuation_task # type: ignore downstream_task_id)
        except Exception as e:
            self.log(task.id.get_full_id_in_dag(subdag), f"Error: {str(e)}") # type: ignore
            raise e

        # Cleanup
        self.log(task.id.get_full_id_in_dag(subdag), f"Worker shut down!")
        if self.metrics_storage:
            self.metrics_storage.flush()

    @abstractmethod
    async def delegate(self, subdag: dag.SubDAG, resource_configuration: TaskWorkerResourceConfiguration | None, called_by_worker: bool = False): 
        """
        {called_by_worker}: indicates if it's a worker invoking another worker, or the Client beggining the execution
        """
        pass

    @staticmethod
    def store_full_dag(metadata_storage: storage_module.Storage, dag: dag.FullDAG):
        metadata_storage.set(f"dag-{dag.master_dag_id}", cloudpickle.dumps(dag))

    def get_full_dag(self, dag_id: str) -> tuple[int, dag.FullDAG]:
        serialized_dag = self.metadata_storage.get(f"dag-{dag_id}")
        if serialized_dag is None: raise Exception(f"Could not find DAG with id {dag_id}")
        deserialized = cloudpickle.loads(serialized_dag)
        if not isinstance(deserialized, dag.FullDAG): raise Exception("Error: fulldag is not a DAG instance")
        return (calculate_data_structure_size(deserialized), deserialized)

    @staticmethod
    async def wait_for_result_of_task(intermediate_storage: storage_module.Storage, task: dag_task_node.DAGTaskNode, dag: dag.FullDAG, polling_interval_s: float = 1.0):
        start_time = Timer()
        # Poll Storage for final result. Asynchronous wait
        task_id = task.id.get_full_id_in_dag(dag)
        first_iter = True
        while True:
            if not first_iter: await asyncio.sleep(polling_interval_s)
            first_iter = False
            final_result = intermediate_storage.get(task_id)
            if final_result is not None:
                final_result = cloudpickle.loads(final_result) # type: ignore
                logger.info(f"Final Result Ready: ({task_id}) => Size: {calculate_data_structure_size(final_result)} | Type: ({type(final_result)}) | Time: {start_time.stop()} ms")
                return final_result

    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        logger.info(f"Worker({self.worker_id}) Task({task_id}) | {message}")

class LocalWorker(Worker):
    @dataclass
    class Config(Worker.Config):
        def create_instance(self) -> "LocalWorker":
            return LocalWorker(self)

    local_config: Config

    """
    Processes DAG tasks
    continuing with single downstream tasks and spawning new workers (coroutines) for branches.
    """
    def __init__(self, config: Config):
       super().__init__(config)
       self.local_config = config
    
    async def delegate(self, subdag: dag.SubDAG, resource_configuration: TaskWorkerResourceConfiguration | None, called_by_worker: bool = True):
        await asyncio.create_task(self.start_executing(subdag))

class DockerWorker(Worker):
    @dataclass
    class Config(Worker.Config):
        docker_gateway_address: str = "http://localhost:5000"
        
        def create_instance(self) -> "DockerWorker":
            return DockerWorker(self)

    docker_config: Config

    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, config: Config):
        super().__init__(config)
        self.docker_config = config

    async def delegate(self, subdag: dag.SubDAG, resource_configuration: TaskWorkerResourceConfiguration | None, called_by_worker: bool = True):
        '''
        Each invocation is done inside a new Coroutine without blocking the owner Thread
        '''
        if not resource_configuration: 
            raise Exception("Resource configuration is required for DockerWorker to delegation!")

        gateway_address = "http://host.docker.internal:5000" if called_by_worker else self.docker_config.docker_gateway_address
        self.log(subdag.root_node.id.get_full_id_in_dag(subdag), f"Invoking docker gateway ({gateway_address}) | Resource Configuration: {resource_configuration}")
        async with aiohttp.ClientSession() as session:
            async with await session.post(
                gateway_address + "/job", 
                data=json.dumps({
                    "resource_configuration": base64.b64encode(cloudpickle.dumps(resource_configuration)).decode('utf-8'),
                    "dag_id": subdag.master_dag_id,
                    "task_id": base64.b64encode(cloudpickle.dumps(subdag.root_node.id)).decode('utf-8'),
                    "config": base64.b64encode(cloudpickle.dumps(self.docker_config)).decode('utf-8'),
                }),
                headers={'Content-Type': 'application/json'}
            ) as response:
                if response.status != 202:
                    text = await response.text()
                    raise Exception(f"Failed to invoke worker: {text}")