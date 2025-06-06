import math
from typing import Literal
import numpy as np
from src.planning.sla import SLA
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, MetricsStorage
from src.storage.metrics.metrics_types import TaskMetrics
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class MetadataAccess:
    # Changed to store tuples with resource configuration: (bytes/ms, cpus, memory_mb)
    cached_upload_speeds: list[tuple[float, float, int]] = [] # (bytes/ms, cpus, memory_mb)
    cached_download_speeds: list[tuple[float, float, int]] = [] # (bytes/ms, cpus, memory_mb)
    cached_io_ratios: dict[str, list[float]] = {} # i/o for each function_name
    # Value: dict[function_name, list[tuple[normalized_execution_time_ms / input_size_bytes, cpus, memory_mb]]]
    cached_execution_time_per_byte: dict[str, list[tuple[float, float, int]]] = {}

    _cached_prediction_data_transfer_times: dict[str, float] = {}
    _cached_prediction_execution_times: dict[str, float] = {}
    _cached_prediction_output_sizes: dict[str, int] = {}

    def __init__(self, dag_structure_hash: str, metrics_storage: MetricsStorage):
        self.dag_structure_hash = dag_structure_hash
        self.metrics_storage = metrics_storage

    async def load_metrics_from_storage(self):
        generic_metrics_keys = await self.metrics_storage.keys(f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}*")
        if not generic_metrics_keys: return # No metrics found
        timer = Timer()
        task_specific_metrics: dict[str, TaskMetrics] = {}

        # Goes to redis
        generic_metrics_values = await self.metrics_storage.mget(generic_metrics_keys)
        for key, metrics in zip(generic_metrics_keys, generic_metrics_values): # type: ignore
            if not isinstance(metrics, TaskMetrics): raise Exception(f"Deserialized value is not of type TaskMetrics: {type(metrics)}")
            task_id = key.decode('utf-8')
            if self.dag_structure_hash in task_id: task_specific_metrics[task_id] = metrics
            
            # Store upload/download speeds with resource configuration
            if metrics.worker_resource_configuration:
                # UPLOAD SPEEDS
                if metrics.output_metrics.time_ms > 0: # it can be 0 if the input was present at the worker (locality)
                    # Normalize the upload speed based on memory
                    normalized_time_ms = metrics.output_metrics.time_ms * (metrics.worker_resource_configuration.memory_mb / BASELINE_MEMORY_MB) ** 0.7
                    upload_speed = metrics.output_metrics.size_bytes / normalized_time_ms if normalized_time_ms > 0 else 0
                    if upload_speed > 0:
                        self.cached_upload_speeds.append((
                            upload_speed,
                            metrics.worker_resource_configuration.cpus,
                            metrics.worker_resource_configuration.memory_mb
                        ))
                # DOWNLOAD SPEEDS
                if metrics.input_metrics.input_download_time_ms > 0: # it can be 0 if the input was present at the worker (locality)
                    # Normalize the download speed based on memory
                    normalized_time_ms = metrics.input_metrics.input_download_time_ms * (metrics.worker_resource_configuration.memory_mb / BASELINE_MEMORY_MB) ** 0.7
                    download_speed = metrics.input_metrics.downloadable_input_size_bytes / normalized_time_ms if normalized_time_ms > 0 else 0
                    if download_speed > 0:
                        self.cached_download_speeds.append((
                            download_speed,
                            metrics.worker_resource_configuration.cpus,
                            metrics.worker_resource_configuration.memory_mb
                        ))

        # Doesn't go to Redis
        for task_id, metrics in task_specific_metrics.items():
            function_name = self._split_task_id(task_id)[0]
            if metrics.worker_resource_configuration:
                if metrics.execution_time_per_input_byte_ms == 0: continue
                if function_name not in self.cached_execution_time_per_byte: self.cached_execution_time_per_byte[function_name] = []
                # Store tuple of (normalized_time, cpus, memory_mb)
                self.cached_execution_time_per_byte[function_name].append((
                    metrics.execution_time_per_input_byte_ms,
                    metrics.worker_resource_configuration.cpus,
                    metrics.worker_resource_configuration.memory_mb
                ))

            # I/O RATIO
            if function_name not in self.cached_io_ratios:
                self.cached_io_ratios[function_name] = []
            input_size = metrics.input_metrics.downloadable_input_size_bytes + metrics.input_metrics.hardcoded_input_size_bytes
            output = metrics.output_metrics.size_bytes
            self.cached_io_ratios[function_name].append(output / input_size if input_size > 0 else 0)

        logger.info(f"Loaded {len(generic_metrics_values)} metadata entries in {timer.stop()}ms")

    def has_required_predictions(self) -> bool:
        return len(self.cached_upload_speeds) > 0 and len(self.cached_download_speeds) > 0 and len(self.cached_io_ratios) > 0 and len(self.cached_execution_time_per_byte) > 0

    def predict_output_size(self, function_name: str, input_size: int , sla: SLA, allow_cached: bool = False) -> int | None:
        """
        Returns:
            Predicted output size in bytes
            None if no data is available
        """
        if input_size < 0: raise ValueError("Input size cannot be negative")
        if function_name not in self.cached_io_ratios: raise ValueError(f"Function {function_name} not found in metadata")
        prediction_key = f"{function_name}-{input_size}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_output_sizes: 
            return self._cached_prediction_output_sizes[prediction_key]
        function_io_ratios = self.cached_io_ratios[function_name]
        if len(function_io_ratios) == 0: raise ValueError(f"No data available for function {function_name}")

        if sla == "avg":
            ratio = np.mean(function_io_ratios)
        else:
            if sla.value < 0 or sla.value > 100: raise ValueError("SLA must be between 0 and 100")
            ratio = np.percentile(function_io_ratios, 100 - sla.value)
        
        res = math.ceil(input_size * ratio)
        self._cached_prediction_output_sizes[prediction_key] = res
        return res

    def predict_data_transfer_time(self, type: Literal['upload', 'download'], data_size_bytes: int, resource_config: TaskWorkerResourceConfiguration, sla: SLA, allow_cached: bool = False) -> float | None:
        """Predict data transfer time for upload/download given data size and resources.
        
        Args:
            type: 'upload' or 'download'
            data_size_bytes: Size of data to transfer in bytes
            resource_config: Worker resource configuration (CPUs + RAM)
            sla: Either "avg" for mean prediction or percentile (0-100)
        
        Returns:
            Predicted data transfer time in milliseconds
            None if no data is available
        """
        if sla != "avg" and (sla.value < 0 or sla.value > 100): raise ValueError("SLA must be 'avg' or between 0 and 100")
        if data_size_bytes == 0: return 0
        
        cached_data = self.cached_upload_speeds if type == 'upload' else self.cached_download_speeds
        if len(cached_data) == 0: raise ValueError(f"No data available for {type}")
        
        prediction_key = f"{type}-{data_size_bytes}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_data_transfer_times: 
            return self._cached_prediction_data_transfer_times[prediction_key]
        
        # Filter samples by exact resource match
        matching_samples = [
            speed for speed, cpus, memory_mb in cached_data
            if cpus == resource_config.cpus and memory_mb == resource_config.memory_mb
        ]

        logger.info(f"Found {len(matching_samples)} exact resource matches for {type}")
        
        if len(matching_samples) >= 2:
            # Direct prediction using exact resource matches (no memory scaling needed)
            if sla == "avg": normalized_speed_bytes_per_ms = np.mean(matching_samples)
            else: normalized_speed_bytes_per_ms = np.percentile(matching_samples, 100 - sla.value)
            if normalized_speed_bytes_per_ms <= 0: raise ValueError(f"No data available for {type}")
            res = data_size_bytes / normalized_speed_bytes_per_ms
        else:
            # Insufficient exact matches - use memory scaling model with baseline normalization
            # First, normalize all samples to baseline memory configuration
            baseline_normalized_samples = [speed * (BASELINE_MEMORY_MB / memory_mb) ** 0.7 for speed, cpus, memory_mb in cached_data]
            if sla == "avg":  baseline_speed_bytes_per_ms = np.mean(baseline_normalized_samples)
            else: baseline_speed_bytes_per_ms = np.percentile(baseline_normalized_samples, 100 - sla.value)
            
            if baseline_speed_bytes_per_ms <= 0: raise ValueError(f"No data available for {type}")
            
            # Scale from baseline to target resource configuration
            actual_speed = baseline_speed_bytes_per_ms * (resource_config.memory_mb / BASELINE_MEMORY_MB) ** 0.7
            res = data_size_bytes / actual_speed
        
        self._cached_prediction_data_transfer_times[prediction_key] = res # type: ignore
        return res # type: ignore

    def predict_execution_time(
        self,
        function_name: str,
        input_size: int,
        resource_config: TaskWorkerResourceConfiguration,
        sla: SLA,
        allow_cached: bool = False
    ) -> float | None:
        """Predict execution time for a function given input size and resources.
        
        Args:
            function_name: Name of the function to predict
            input_size: Size of input data in bytes
            resource_config: Worker resource configuration (CPUs + RAM)
            sla: Either "avg" for mean prediction or percentile (0-100)
        
        Returns:
            Predicted execution time in milliseconds
            None if no data is available
        """
        if sla != "avg" and (sla.value < 0 or sla.value > 100): raise ValueError("SLA must be 'avg' or between 0 and 100")
        if function_name not in self.cached_execution_time_per_byte: raise ValueError(f"Function {function_name} not found in metadata")
        
        prediction_key = f"{function_name}-{input_size}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_execution_times: 
            return self._cached_prediction_execution_times[prediction_key]
        
        # Get all samples for this function
        all_samples = self.cached_execution_time_per_byte[function_name]
        if len(all_samples) == 0: raise ValueError(f"No data available for function {function_name}")
        
        # Filter samples by exact resource match
        matching_samples = [
            normalized_time for normalized_time, cpus, memory_mb in all_samples
            if cpus == resource_config.cpus and memory_mb == resource_config.memory_mb
        ]

        logger.info(f"Found {len(matching_samples)} exact resource matches for function {function_name}")
        
        if len(matching_samples) >= 2:
            # Direct prediction using exact resource matches (no memory scaling needed)
            if sla == "avg": ms_per_byte = np.mean(matching_samples)
            else: ms_per_byte = np.percentile(matching_samples, 100 - sla.value)
            if ms_per_byte <= 0: raise ValueError(f"No data available for function {function_name}")
            res = ms_per_byte * input_size
        else:
            # Insufficient exact matches - use memory scaling model with baseline normalization
            # First, normalize all samples to baseline memory configuration
            baseline_normalized_samples = [t * (memory_mb / BASELINE_MEMORY_MB) ** 0.5 for t, cpus, memory_mb in all_samples]
            
            if sla == "avg": baseline_ms_per_byte = np.mean(baseline_normalized_samples) 
            else: baseline_ms_per_byte = np.percentile(baseline_normalized_samples, 100 - sla.value)
            if baseline_ms_per_byte <= 0:  raise ValueError(f"No data available for function {function_name}")
            res = baseline_ms_per_byte * input_size * (BASELINE_MEMORY_MB / resource_config.memory_mb) ** 0.5
        
        self._cached_prediction_execution_times[prediction_key] = res # type: ignore
        return res # type: ignore

    def _split_task_id(self, task_id: str) -> tuple[str, str, str]:
        """ returns [function_name, task_id, dag_id] """
        task_id = task_id.removeprefix(MetricsStorage.TASK_METRICS_KEY_PREFIX)
        splits = task_id.split("-", maxsplit=1)
        function_name = splits[0]
        splits_2 = splits[1].split("_")
        task_id = splits_2[0]
        dag_id = splits_2[1]
        return function_name, task_id, dag_id