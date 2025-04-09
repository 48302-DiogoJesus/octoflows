import numpy as np
from src.planning.dag_planner import SLA
from src.storage.metrics import metrics_storage
from src.worker_resource_configuration import TaskWorkerResourceConfiguration


class MetadataAccess:
    cached_data_transfer_speeds: list[float] # bytes/ms
    cached_io_ratios: dict[str, list[float]] # i/o for each function_name
    cached_task_metrics: dict[str, metrics_storage.TaskMetrics]

    def __init__(self, metrics_storage: metrics_storage.MetricsStorage) -> None:
        # TODO: Grab and cache metrics from metrics_storage
        self.metrics_storage = metrics_storage
        pass

    def predict_remote_download_time(self, data_size_bytes: int, sla: SLA) -> float:
        if sla == "avg":
            # Calculate average speed when SLA is "avg"
            speed_bytes_per_ms = np.mean(self.cached_data_transfer_speeds)
        else:
            # Existing percentile-based calculation
            if sla.value < 0 or sla.value > 100: 
                raise ValueError("SLA must be between 0 and 100")
            speed_bytes_per_ms = np.percentile(self.cached_data_transfer_speeds, 100 - sla.value)
        
        if speed_bytes_per_ms <= 0:
            return float('inf')
        
        return data_size_bytes / speed_bytes_per_ms

    def predict_output_size(self, function_name: str, input_size: int , sla: SLA) -> float:
        if input_size < 0: raise ValueError("Input size cannot be negative")
        if function_name not in self.cached_io_ratios:
            raise ValueError(f"Empty I/O ratio data for function: {function_name}")
        
        function_io_ratios = self.cached_io_ratios[function_name]
        if sla == "avg":
            # Calculate average ratio when SLA is "avg"
            ratio = np.mean(function_io_ratios)
        else:
            if sla.value < 0 or sla.value > 100: raise ValueError("SLA must be between 0 and 100")
            ratio = np.percentile(function_io_ratios, 100 - sla.value)
        
        return input_size * ratio

    def predict_execution_time(
        self,
        function_name: str,
        input_size: int,
        resource_config: TaskWorkerResourceConfiguration,
        sla: SLA,
    ) -> float:
        """Predict execution time for a function given input size and resources.
        
        Args:
            function_name: Name of the function to predict
            input_size: Size of input data in bytes
            resource_config: Worker resource configuration (CPUs + RAM)
            sla: Either "avg" for mean prediction or percentile (0-100)
        
        Returns:
            Predicted execution time in milliseconds
        """
        # Input validation
        if sla != "avg" and (sla.value < 0 or sla.value > 100): raise ValueError("SLA must be 'avg' or between 0 and 100")

        # Collect relevant metrics for the function
        relevant_metrics: list[metrics_storage.TaskMetrics] = []
        for task_id, metrics in self.cached_task_metrics.items():
            fn_name, _, _ = self._split_task_id(task_id)
            if fn_name == function_name and metrics.worker_resource_configuration is not None:
                relevant_metrics.append(metrics)
        
        if not relevant_metrics:
            raise ValueError(f"No historical data found for function {function_name}")

        weighted_times = self._calculate_weighted_times(relevant_metrics, resource_config)
        
        if not weighted_times: raise ValueError("No valid input size data available for prediction")

        if sla == "avg":
            predicted_normalized = np.mean(weighted_times)
        else:
            percentile = 100 - sla.value
            predicted_normalized = np.percentile(weighted_times, percentile)

        return predicted_normalized * input_size

    def _calculate_weighted_times(
        self,
        metrics_list: list[metrics_storage.TaskMetrics],
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
        task_id = task_id.removeprefix(metrics_storage.MetricsStorage.TASK_METRICS_KEY_PREFIX)
        splits = task_id.split("-", maxsplit=1)
        function_name = splits[0]
        splits_2 = splits[1].split("_")
        task_id = splits_2[0]
        dag_id = splits_2[1]
        return function_name, task_id, dag_id