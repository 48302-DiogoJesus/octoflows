import math
from typing import Literal
import numpy as np
from src.planning.sla import SLA
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, TaskMetrics, MetricsStorage
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class MetadataAccess:
    cached_upload_speeds: list[float] = [] # bytes/ms
    cached_download_speeds: list[float] = [] # bytes/ms
    cached_io_ratios: dict[str, list[float]] = {} # i/o for each function_name
    # Value: dict[memory_mb, list[normalized_execution_time_ms / input_size_bytes]]
    cached_execution_time_per_byte: dict[str, list[float]] = {}

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
            # UPLOAD SPEEDS
            if metrics.output_metrics.normalized_time_ms > 0: # it can be 0 if the input was present at the worker (locality)
                self.cached_upload_speeds.append(metrics.output_metrics.size_bytes / metrics.output_metrics.normalized_time_ms)
            # DOWNLOAD SPEEDS
            for input_metric in metrics.input_metrics:
                if input_metric.normalized_time_ms > 0: # it can be 0 if the input was present at the worker (locality)
                    self.cached_download_speeds.append(input_metric.size_bytes / input_metric.normalized_time_ms)

        # Doesn't go to Redis
        for task_id, metrics in task_specific_metrics.items():
            function_name = self._split_task_id(task_id)[0]
            if metrics.worker_resource_configuration:
                if metrics.normalized_execution_time_per_input_byte_ms == 0: continue
                if function_name not in self.cached_execution_time_per_byte: self.cached_execution_time_per_byte[function_name] = []
                self.cached_execution_time_per_byte[function_name].append(metrics.normalized_execution_time_per_input_byte_ms)

            # I/O RATIO
            if function_name not in self.cached_io_ratios:
                self.cached_io_ratios[function_name] = []
            input = sum(input_metric.size_bytes for input_metric in metrics.input_metrics) + sum(h_input_metric.size_bytes for h_input_metric in metrics.hardcoded_input_metrics)
            output = metrics.output_metrics.size_bytes
            self.cached_io_ratios[function_name].append(output / input if input > 0 else 0)

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
        """
        Returns:
            Predicted data transfer time in milliseconds
            None if no data is available
        """
        cached_data = self.cached_upload_speeds if type == 'upload' else self.cached_download_speeds
        if len(cached_data) == 0: raise ValueError(f"No data available for {type}")
        if data_size_bytes == 0: return 0
        prediction_key = f"{type}-{data_size_bytes}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_data_transfer_times: 
            return self._cached_prediction_data_transfer_times[prediction_key]
        if sla == "avg":
            normalized_speed_bytes_per_ms = np.mean(cached_data)
        else:
            if sla.value < 0 or sla.value > 100: 
                raise ValueError("SLA must be between 0 and 100")
            normalized_speed_bytes_per_ms = np.percentile(cached_data, 100 - sla.value)
        
        if normalized_speed_bytes_per_ms <= 0: raise ValueError(f"No data available for {type}")
        
        res = (data_size_bytes / normalized_speed_bytes_per_ms) * (BASELINE_MEMORY_MB / resource_config.memory_mb) 
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
        normalized_ms_per_byte_for_function = self.cached_execution_time_per_byte[function_name]
        if len(normalized_ms_per_byte_for_function) == 0: raise ValueError(f"No data available for function {function_name}")

        prediction_key = f"{function_name}-{input_size}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_execution_times: 
            return self._cached_prediction_execution_times[prediction_key]
        
        if sla == "avg":
            normalized_ms_per_byte = np.mean(normalized_ms_per_byte_for_function)
        else:
            if sla.value < 0 or sla.value > 100:
                raise ValueError("SLA must be between 0 and 100")
            normalized_ms_per_byte = np.percentile(normalized_ms_per_byte_for_function, 100 - sla.value)
        
        if normalized_ms_per_byte <= 0: raise ValueError(f"No data available for function {function_name}")
        
        res = normalized_ms_per_byte * input_size * (BASELINE_MEMORY_MB / resource_config.memory_mb)
        self._cached_prediction_execution_times[prediction_key] = res # type: ignore
        return res # type: ignore

    def _calculate_weighted_times(
        self,
        metrics_list: list[TaskMetrics],
        resource_config: TaskWorkerResourceConfiguration,
    ) -> list[float]:
        """Calculate resource-adjusted normalized execution times."""
        weighted_times = []
        
        for metrics in metrics_list:
            total_input = sum(m.size_bytes for m in metrics.input_metrics) + sum(m.size_bytes for m in metrics.hardcoded_input_metrics)
            
            if total_input <= 0: continue

            assert metrics.worker_resource_configuration # metrics_list only contains metrics with worker_resource_configuration
            hist_cpu = metrics.worker_resource_configuration.cpus
            hist_mem = metrics.worker_resource_configuration.memory_mb
            
            cpu_factor = hist_cpu / resource_config.cpus if resource_config.cpus > 0 else 1
            mem_factor = np.sqrt(hist_mem / resource_config.memory_mb) if resource_config.memory_mb > 0 else 1
            resource_factor = 0.7 * cpu_factor + 0.3 * mem_factor  # Weighted combination
            
            # Normalize execution time and apply resource adjustment
            normalized_time = (metrics.execution_time_ms / total_input) * resource_factor
            weighted_times.append(normalized_time)
        
        return weighted_times
        
    def _split_task_id(self, task_id: str) -> tuple[str, str, str]:
        """ returns [function_name, task_id, dag_id] """
        task_id = task_id.removeprefix(MetricsStorage.TASK_METRICS_KEY_PREFIX)
        splits = task_id.split("-", maxsplit=1)
        function_name = splits[0]
        splits_2 = splits[1].split("_")
        task_id = splits_2[0]
        dag_id = splits_2[1]
        return function_name, task_id, dag_id