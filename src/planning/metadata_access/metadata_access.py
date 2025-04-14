from typing import Literal
import numpy as np
from src.planning.sla import SLA
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, TaskMetrics, MetricsStorage
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class MetadataAccess:
    cached_upload_speeds: list[float] = [] # bytes/ms
    cached_download_speeds: list[float] = [] # bytes/ms
    cached_io_ratios: dict[str, list[float]] = {} # i/o for each function_name
    # Value: dict[memory_mb, list[normalized_execution_time_ms / input_size_bytes]]
    cached_execution_time_per_byte: dict[str, list[float]] = {}

    def __init__(self, dag_structure_hash: str, metrics_storage: MetricsStorage):
        self.metrics_storage = metrics_storage
        # Grabs metrics for the same workflow structure (uses hashing)
        metrics_keys = self.metrics_storage.keys(f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}*{dag_structure_hash}")
        if not metrics_keys: return # No metrics found
        timer = Timer()
        for key in metrics_keys: # type: ignore
            metrics = self.metrics_storage.get(key)
            if not metrics: raise Exception(f"Key: {key.decode('utf-8')} has no value")
            if not isinstance(metrics, TaskMetrics): raise Exception(f"Deserialized value is not of type TaskMetrics: {type(metrics)}")
            
            task_id = key.decode('utf-8')
            function_name = self._split_task_id(task_id)[0]
            if metrics.worker_resource_configuration:
                total_input_size = sum(i.size_bytes for i in metrics.input_metrics) + sum(h.size_bytes for h in metrics.hardcoded_input_metrics)
                if metrics.normalized_execution_time_ms == 0: continue
                if function_name not in self.cached_execution_time_per_byte: self.cached_execution_time_per_byte[function_name] = []
                self.cached_execution_time_per_byte[function_name].append(metrics.normalized_execution_time_ms / total_input_size)

            # I/O RATIO
            if function_name not in self.cached_io_ratios:
                self.cached_io_ratios[function_name] = []
            input = sum(input_metric.size_bytes for input_metric in metrics.input_metrics) + sum(h_input_metric.size_bytes for h_input_metric in metrics.hardcoded_input_metrics)
            output = metrics.output_metrics.size_bytes
            self.cached_io_ratios[function_name].append(output / input if input > 0 else 0)
            # UPLOAD SPEEDS
            if metrics.output_metrics.time_ms > 0:
                self.cached_upload_speeds.append(metrics.output_metrics.size_bytes / metrics.output_metrics.normalized_time_ms)
            # DOWNLOAD SPEEDS
            for input_metric in metrics.input_metrics:
                if input_metric.time_ms > 0:
                    self.cached_download_speeds.append(input_metric.size_bytes / input_metric.normalized_time_ms)

    def predict_output_size(self, function_name: str, input_size: int , sla: SLA) -> float | None:
        """
        Returns:
            Predicted output size in bytes
            None if no data is available
        """
        if input_size < 0: raise ValueError("Input size cannot be negative")
        if function_name not in self.cached_io_ratios: return None
        
        function_io_ratios = self.cached_io_ratios[function_name]
        if sla == "avg":
            ratio = np.mean(function_io_ratios)
        else:
            if sla.value < 0 or sla.value > 100: raise ValueError("SLA must be between 0 and 100")
            ratio = np.percentile(function_io_ratios, 100 - sla.value)
        
        return input_size * ratio # type: ignore

    def predict_data_transfer_time(self, type: Literal['upload', 'download'], data_size_bytes: int, resource_config: TaskWorkerResourceConfiguration, sla: SLA) -> float | None:
        """
        Returns:
            Predicted data transfer time in milliseconds
            None if no data is available
        """
        cached_data = self.cached_upload_speeds if type == 'upload' else self.cached_download_speeds

        if sla == "avg":
            normalized_speed_bytes_per_ms = np.mean(cached_data)
        else:
            if sla.value < 0 or sla.value > 100: 
                raise ValueError("SLA must be between 0 and 100")
            normalized_speed_bytes_per_ms = np.percentile(cached_data, 100 - sla.value)
        
        if normalized_speed_bytes_per_ms <= 0: return None
        
        return (data_size_bytes / normalized_speed_bytes_per_ms) * (BASELINE_MEMORY_MB / resource_config.memory_mb) # type: ignore

    def predict_execution_time(
        self,
        function_name: str,
        input_size: int,
        resource_config: TaskWorkerResourceConfiguration,
        sla: SLA,
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
        normalized_ms_per_byte_for_function = self.cached_execution_time_per_byte.get(function_name, [])
        
        if not normalized_ms_per_byte_for_function: return None
        
        if sla == "avg":
            ms_per_byte = np.mean(normalized_ms_per_byte_for_function)
        else:
            if sla.value < 0 or sla.value > 100:
                raise ValueError("SLA must be between 0 and 100")
            ms_per_byte = np.percentile(normalized_ms_per_byte_for_function, 100 - sla.value)
        
        if ms_per_byte <= 0: return None
        
        return (ms_per_byte * input_size) * (BASELINE_MEMORY_MB / resource_config.memory_mb) # type: ignore

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