import math
from typing import Literal
import numpy as np
from src.planning.sla import SLA
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, MetricsStorage
from src.storage.metrics.metrics_types import TaskMetrics, WorkerStartupMetrics
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class PredictionsProvider:
    MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION = 20

    nr_of_previous_instances: int = 0

    # Changed to store tuples with resource configuration: (bytes/ms, cpus, memory_mb)
    cached_upload_speeds: list[tuple[float, float, int]] = [] # (bytes/ms, cpus, memory_mb)
    cached_download_speeds: list[tuple[float, float, int]] = [] # (bytes/ms, cpus, memory_mb)
    cached_deserialized_io_ratios: dict[str, list[float]] = {} # i/o for each function_name
    cached_serialized_io_ratios: dict[str, list[float]] = {} # i/o for each function_name
    # Value: dict[function_name, list[tuple[normalized_execution_time_ms / input_size_bytes, cpus, memory_mb, input_size_bytes, total_input_size_bytes]]]
    cached_execution_time_per_byte: dict[str, list[tuple[float, float, int, int]]] = {}
    # Value: dict[function_name, list[tuple[startup_time, cpus, memory_mb]]]
    cached_worker_cold_start_times: list[tuple[float, float, int]] = []
    # Value: dict[function_name, list[tuple[startup_time, cpus, memory_mb]]]
    cached_worker_warm_start_times: list[tuple[float, float, int]] = []

    _cached_prediction_data_transfer_times: dict[str, float] = {}
    _cached_prediction_execution_times: dict[str, float] = {}
    _cached_prediction_output_sizes: dict[str, int] = {}
    _cached_prediction_startup_times: dict[str, float] = {}

    def __init__(self, nr_of_dag_nodes: int, dag_structure_hash: str, metrics_storage: MetricsStorage):
        self.nr_of_dag_nodes = nr_of_dag_nodes
        self.dag_structure_hash = dag_structure_hash
        self.metrics_storage = metrics_storage

    async def load_metrics_from_storage(self, planner_name: str):
        from src.planning.abstract_dag_planner import AbstractDAGPlanner

        generic_metrics_keys = await self.metrics_storage.keys(f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}*")
        if not generic_metrics_keys: return # No metrics found
        worker_startup_metrics_keys = await self.metrics_storage.keys(f"{MetricsStorage.WORKER_STARTUP_PREFIX}*")
        timer = Timer()
        same_workflow_same_planner_type_metrics: dict[str, TaskMetrics] = {}

        # Goes to redis
        generic_metrics_values = await self.metrics_storage.mget(generic_metrics_keys)
        for key, metrics in zip(generic_metrics_keys, generic_metrics_values): # type: ignore
            if not isinstance(metrics, TaskMetrics): raise Exception(f"Deserialized value is not of type TaskMetrics: {type(metrics)}")
            task_id = key.decode('utf-8')
            if self.dag_structure_hash in task_id:
                _, _ , master_dag_id = self._split_task_id(task_id)
                plan_output: AbstractDAGPlanner.PlanOutput | None = await self.metrics_storage.get(f"{MetricsStorage.PLAN_KEY_PREFIX}{master_dag_id}") # type: ignore
                if plan_output and plan_output.planner_name != planner_name:
                    continue #! Note: it's not even collecting upload/download information (this is for easier planner comparison only)
                same_workflow_same_planner_type_metrics[task_id] = metrics
            
            # Store upload/download speeds with resource configuration
            # DOWNLOAD SPEEDS
            for input_metric in metrics.input_metrics.input_download_metrics.values():
                if input_metric.time_ms is None: continue # it can be None if the input was present at the worker
                # Normalize the download speed based on memory
                self.cached_download_speeds.append((
                    input_metric.serialized_size_bytes / input_metric.time_ms,
                    metrics.worker_resource_configuration.cpus,
                    metrics.worker_resource_configuration.memory_mb
                ))

            # UPLOAD SPEEDS
            if metrics.output_metrics.tp_time_ms: # it can be None if the output was present at the worker, for example
                # Normalize the upload speed based on memory
                self.cached_upload_speeds.append((
                    metrics.output_metrics.serialized_size_bytes / metrics.output_metrics.tp_time_ms,
                    metrics.worker_resource_configuration.cpus,
                    metrics.worker_resource_configuration.memory_mb
                ))

        worker_startup_metrics: list[WorkerStartupMetrics] = await self.metrics_storage.mget(worker_startup_metrics_keys) # type: ignore
        for wsm in worker_startup_metrics:
            if wsm.end_time_ms is None: continue
            if wsm.state == "cold": self.cached_worker_cold_start_times.append((wsm.end_time_ms - wsm.start_time_ms, wsm.resource_configuration.cpus, wsm.resource_configuration.memory_mb))
            elif wsm.state == "warm": self.cached_worker_warm_start_times.append((wsm.end_time_ms - wsm.start_time_ms, wsm.resource_configuration.cpus, wsm.resource_configuration.memory_mb))

        # Doesn't go to Redis
        for task_id, metrics in same_workflow_same_planner_type_metrics.items():
            function_name, _, _ = self._split_task_id(task_id)
            if metrics.execution_time_per_input_byte_ms is None: continue
            
            if function_name not in self.cached_execution_time_per_byte: self.cached_execution_time_per_byte[function_name] = []
            self.cached_execution_time_per_byte[function_name].append((
                metrics.execution_time_per_input_byte_ms,
                metrics.worker_resource_configuration.cpus,
                metrics.worker_resource_configuration.memory_mb,
                metrics.input_metrics.hardcoded_input_size_bytes + sum([input_metric.deserialized_size_bytes for input_metric in metrics.input_metrics.input_download_metrics.values()])
            ))

            # I/O RATIO
            if function_name not in self.cached_deserialized_io_ratios:
                self.cached_deserialized_io_ratios[function_name] = []
            input_size = metrics.input_metrics.hardcoded_input_size_bytes + sum([input_metric.deserialized_size_bytes for input_metric in metrics.input_metrics.input_download_metrics.values()])
            output_size = metrics.output_metrics.deserialized_size_bytes
            self.cached_deserialized_io_ratios[function_name].append(output_size / input_size if input_size > 0 else 0)

            if function_name not in self.cached_serialized_io_ratios:
                self.cached_serialized_io_ratios[function_name] = []
            input_size = metrics.input_metrics.hardcoded_input_size_bytes + sum([input_metric.serialized_size_bytes for input_metric in metrics.input_metrics.input_download_metrics.values()])
            output_size = metrics.output_metrics.serialized_size_bytes
            self.cached_serialized_io_ratios[function_name].append(output_size / input_size if input_size > 0 else 0)

        self.nr_of_previous_instances = int(len(same_workflow_same_planner_type_metrics) / self.nr_of_dag_nodes)
        logger.info(f"Loaded {len(generic_metrics_values)} metadata entries in {timer.stop()}ms")

    def has_required_predictions(self) -> bool:
        # print("cached upload speeds len: ", len(self.cached_upload_speeds))
        # print("cached download speeds len: ", len(self.cached_download_speeds))
        # # change cached io ratios to count the number of ratios for all function_names
        # print("cached io ratios len: ", sum(len(ratios) for ratios in self.cached_io_ratios.values()))
        # print("cached execution time per byte len: ", sum(len(ratios) for ratios in self.cached_execution_time_per_byte.values()))
        # print("cached worker cold start times len: ", len(self.cached_worker_cold_start_times))
        # print("cached worker warm start times len: ", len(self.cached_worker_warm_start_times))
        #* Needs to have at least SOME history for the SAME TYPE of workflow
        return len(self.cached_deserialized_io_ratios) > 0 or len(self.cached_serialized_io_ratios) > 0 or len(self.cached_execution_time_per_byte) > 0

    def predict_output_size(self, function_name: str, input_size: int , sla: SLA, deserialized: bool = True, allow_cached: bool = True) -> int:
        """
        Returns:
            Predicted output size in bytes
        """
        if input_size < 0: raise ValueError("Input size cannot be negative")
        if deserialized and function_name not in self.cached_deserialized_io_ratios: raise ValueError(f"Function {function_name} not found in metadata")
        if not deserialized and function_name not in self.cached_serialized_io_ratios: raise ValueError(f"Function {function_name} not found in metadata")
        prediction_key = f"{function_name}-{input_size}-{sla}-{deserialized}"
        if allow_cached and prediction_key in self._cached_prediction_output_sizes: 
            return self._cached_prediction_output_sizes[prediction_key]
        function_io_ratios = self.cached_deserialized_io_ratios[function_name] if deserialized else self.cached_serialized_io_ratios[function_name]
        if len(function_io_ratios) == 0: return 0

        if sla == "avg":
            ratio = np.mean(function_io_ratios)
        else:
            ratio = np.percentile(function_io_ratios, 100 - sla.value)
        
        # Logarithmic scaling: output grows slower than input
        # base_output = input_size * ratio
        # scaled_output = base_output * math.log(input_size + 1) / math.log(input_size * 2 + 1)
        res = math.ceil(input_size * ratio)

        self._cached_prediction_output_sizes[prediction_key] = res
        return res

    def predict_data_transfer_time(self, type: Literal['upload', 'download'], data_size_bytes: int, resource_config: TaskWorkerResourceConfiguration, sla: SLA, allow_cached: bool = True) -> float:
        """Predict data transfer time for upload/download given data size and resources.
        
        Args:
            type: 'upload' or 'download'
            data_size_bytes: Size of data to transfer in bytes
            resource_config: Worker resource configuration (CPUs + RAM)
            sla: Either "avg" for mean prediction or percentile (0-100)
        
        Returns:
            Predicted data transfer time in milliseconds
        """
        if sla != "avg" and (sla.value < 0 or sla.value > 100): raise ValueError("SLA must be 'avg' or between 0 and 100")
        if data_size_bytes == 0: return 0
        
        cached_data = self.cached_upload_speeds if type == 'upload' else self.cached_download_speeds
        if len(cached_data) == 0: return 0
        
        prediction_key = f"{type}-{data_size_bytes}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_data_transfer_times: 
            return self._cached_prediction_data_transfer_times[prediction_key]
        
        # Filter samples by exact resource match
        matching_samples = [
            speed for speed, cpus, memory_mb in cached_data
            if cpus == resource_config.cpus and memory_mb == resource_config.memory_mb
        ]

        # logger.info(f"Found {len(matching_samples)} exact resource matches for {type}")
        
        if len(matching_samples) >= self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            # Direct prediction using exact resource matches (no memory scaling needed)
            if sla == "avg": normalized_speed_bytes_per_ms = np.mean(matching_samples)
            else: normalized_speed_bytes_per_ms = np.percentile(matching_samples, 100 - sla.value)
            if normalized_speed_bytes_per_ms <= 0: raise ValueError(f"No data available for {type}")
            res = data_size_bytes / normalized_speed_bytes_per_ms
        else:
            # Insufficient exact matches - use memory scaling model with baseline normalization
            # First, normalize all samples to baseline memory configuration
            baseline_normalized_samples = [speed * (BASELINE_MEMORY_MB / memory_mb) ** 0.5 for speed, cpus, memory_mb in cached_data]
            
            if sla == "avg":  baseline_speed_bytes_per_ms = np.mean(baseline_normalized_samples)
            else: baseline_speed_bytes_per_ms = np.percentile(baseline_normalized_samples, 100 - sla.value)
            
            if baseline_speed_bytes_per_ms <= 0: raise ValueError(f"No data available for {type}")
            
            # Scale from baseline to target resource configuration
            actual_speed = baseline_speed_bytes_per_ms * (resource_config.memory_mb / BASELINE_MEMORY_MB) ** 0.5
            res = data_size_bytes / actual_speed
            
        self._cached_prediction_data_transfer_times[prediction_key] = res # type: ignore
        return res # type: ignore

    def predict_worker_startup_time(self, resource_config: TaskWorkerResourceConfiguration, state: Literal['cold', 'warm'], sla: SLA, allow_cached: bool = True) -> float:
        """Predict worker startup time given resource configuration and state."""
        samples = self.cached_worker_cold_start_times if state == "cold" else self.cached_worker_warm_start_times
        if sla != "avg" and (sla.value < 0 or sla.value > 100): raise ValueError("SLA must be 'avg' or between 0 and 100")
        
        if len(samples) == 0: return 0
        
        prediction_key = f"{state}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_startup_times: 
            return self._cached_prediction_startup_times[prediction_key]
        
        # Filter samples by exact resource match
        matching_samples = [
            startup_time for startup_time, cpus, memory_mb in samples
            if cpus == resource_config.cpus and memory_mb == resource_config.memory_mb
        ]

        # logger.info(f"Found {len(matching_samples)} exact resource matches for {state}")
        
        if len(matching_samples) >= self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            # Direct prediction using exact resource matches (no memory scaling needed)
            if sla == "avg": startup_time = np.mean(matching_samples)
            else: startup_time = np.percentile(matching_samples, 100 - sla.value)
            if startup_time <= 0: raise ValueError(f"No data available for predicting '{state}' worker startup time")
            res = startup_time
        else:
            # Insufficient exact matches - use memory scaling model with baseline normalization
            # First, normalize all samples to baseline memory configuration
            baseline_normalized_samples = [startup_time * (BASELINE_MEMORY_MB / memory_mb) ** 0.5 for startup_time, cpus, memory_mb in samples]
            if sla == "avg":  startup_time = np.mean(baseline_normalized_samples)
            else: startup_time = np.percentile(baseline_normalized_samples, 100 - sla.value)
            
            if startup_time <= 0: raise ValueError(f"No data available for predicting '{state}' worker startup time")
            
            # Scale from baseline to target resource configuration
            actual_startup_time = startup_time * (resource_config.memory_mb / BASELINE_MEMORY_MB) ** 0.5
            res = actual_startup_time
        
        self._cached_prediction_startup_times[prediction_key] = res # type: ignore
        return res # type: ignore

    def predict_execution_time(
        self,
        function_name: str,
        input_size: int,
        resource_config: TaskWorkerResourceConfiguration,
        sla: SLA,
        allow_cached: bool = True,
        size_scaling_factor: float = 1
    ) -> float:
        """Predict execution time for a function given input size and resources.
        
        size_scaling_factor: Exponent for input size scaling (default 0.8 for sublinear growth)
            - 1.0 = linear scaling (original behavior)
            - 0.5 = square root scaling (very slow growth)
            - 0.8 = moderate sublinear scaling (recommended)
        
        Returns:
            Predicted execution time in milliseconds
            None if no data is available
        """
        if sla != "avg" and (sla.value < 0 or sla.value > 100): 
            raise ValueError("SLA must be 'avg' or between 0 and 100")
        if function_name not in self.cached_execution_time_per_byte: 
            raise ValueError(f"Function {function_name} not found in metadata")
        
        prediction_key = f"{function_name}-{input_size}-{resource_config}-{sla}-{size_scaling_factor}"
        if allow_cached and prediction_key in self._cached_prediction_execution_times: 
            return self._cached_prediction_execution_times[prediction_key]
        
        # Get all samples for this function
        all_samples = self.cached_execution_time_per_byte[function_name]
        if len(all_samples) == 0: 
            return 0
        
        # Filter samples by exact resource match
        matching_samples = [
            normalized_time for normalized_time, cpus, memory_mb, total_input_size_bytes in all_samples
            if cpus == resource_config.cpus and memory_mb == resource_config.memory_mb
        ]

        # logger.info(f"Found {len(matching_samples)} exact resource matches for function {function_name} | nr samples: {len(all_samples)}")
        
        def _select_samples_by_input_size(samples_with_sizes: list[tuple[float, int]], input_size: int) -> list[float]:
            if not samples_with_sizes: return []
            
            if len(samples_with_sizes) <= 5: return [value for value, _ in samples_with_sizes] # use all samples
            
            # calculate distances from input_size and sort by distance
            samples_with_distances = [(abs(size - input_size), value, idx) for idx, (value, size) in enumerate(samples_with_sizes)]
            samples_with_distances.sort()
            
            target_count = min(
                20, # max samples
                max(
                    len(samples_with_sizes) // 5, # 20% of total samples 
                    1 # to avoid 0
                )
            )
            return [value for _, value, _ in samples_with_distances[:target_count]]
            # return [value for _, value, _ in samples_with_distances]

        if len(matching_samples) >= self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            # Get the full samples (with input sizes) for the matching resource config
            full_matching_samples = [
                (t, size) for t, cpus, memory_mb, size in all_samples
                if cpus == resource_config.cpus and memory_mb == resource_config.memory_mb
            ]
            
            # Select samples based on input size similarity
            selected_times = _select_samples_by_input_size(full_matching_samples, input_size)
            
            # Calculate prediction using the selected samples
            if sla == "avg": 
                ms_per_byte = np.mean(selected_times)
            else: 
                ms_per_byte = np.percentile(selected_times, 100 - sla.value)
            
            if ms_per_byte <= 0: 
                raise ValueError(f"No valid data available for function {function_name}")
                
            # Apply sublinear scaling to input size
            res = ms_per_byte * (input_size ** size_scaling_factor)
        else:
            # Insufficient exact matches - use memory scaling model with baseline normalization
            # Normalize samples to baseline memory configuration and select by input size
            samples_with_sizes = [
                (t * (memory_mb / BASELINE_MEMORY_MB) ** 0.5, total_input_size_bytes)
                for t, cpus, memory_mb, total_input_size_bytes in all_samples
            ]
            
            # Select samples based on input size similarity
            baseline_normalized_samples = _select_samples_by_input_size(samples_with_sizes, input_size)
            
            if sla == "avg":
                baseline_ms_per_byte = np.mean(baseline_normalized_samples) 
            else: 
                baseline_ms_per_byte = np.percentile(baseline_normalized_samples, 100 - sla.value)
            if baseline_ms_per_byte <= 0:  
                raise ValueError(f"No data available for function {function_name}")
            # Apply sublinear scaling to input size and memory scaling
            res = (baseline_ms_per_byte * (input_size ** size_scaling_factor) * 
                   (BASELINE_MEMORY_MB / resource_config.memory_mb) ** 0.5)
        
        self._cached_prediction_execution_times[prediction_key] = res # type: ignore
        return res # type: ignore

    def _split_task_id(self, task_id: str) -> tuple[str, str, str]:
        """ returns [function_name, task_id, master_dag_id] """
        task_id = task_id.removeprefix(MetricsStorage.TASK_METRICS_KEY_PREFIX)
        splits = task_id.split("+", maxsplit=1)
        function_name = splits[0]
        splits_2 = splits[1].split("+", maxsplit=1)
        task_id = splits_2[0]
        master_dag_id = splits_2[1]
        return function_name, task_id, master_dag_id