import math
from typing import Literal
import numpy as np
import cloudpickle

from src.planning.sla import SLA
from src.storage.metadata.metadata_storage import BASELINE_MEMORY_MB, MetadataStorage
from src.storage.metadata.metrics_types import TaskMetrics, WorkerStartupMetrics
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class PredictionsProvider:
    MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION = 5

    nr_of_previous_instances: int = 0

    # Changed to store tuples with resource configuration: (bytes/ms, cpus, memory_mb)
    cached_upload_speeds: list[tuple[float, int, float, int]] = [] # (bytes/ms, total_bytes, cpus, memory_mb)
    cached_download_speeds: list[tuple[float, int, float, int]] = [] # (bytes/ms, total_bytes, cpus, memory_mb)
    cached_serialized_io_ratios: dict[str, list[tuple[float, int]]] = {} # (i/o ratio, input_size) for each function_name
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

    def __init__(self, nr_of_dag_nodes: int, dag_structure_hash: str, metadata_storage: MetadataStorage):
        self.nr_of_dag_nodes = nr_of_dag_nodes
        self.dag_structure_hash = dag_structure_hash
        self.metadata_storage = metadata_storage

    async def load_metrics_from_storage(self, planner_name: str):
        timer = Timer()
        generic_metrics_keys = await self.metadata_storage.storage.keys(f"{MetadataStorage.TASK_MD_KEY_PREFIX}*")
        if not generic_metrics_keys: return # No metrics found
        worker_startup_metrics_keys = await self.metadata_storage.storage.keys(f"{MetadataStorage.WORKER_STARTUP_PREFIX}*")
        same_workflow_same_planner_type_metrics: dict[str, TaskMetrics] = {}

        # Goes to redis
        generic_metrics_values = await self.metadata_storage.storage.mget(generic_metrics_keys)
        for key, metrics in zip(generic_metrics_keys, generic_metrics_values): # type: ignore
            metrics = cloudpickle.loads(metrics)
            if not isinstance(metrics, TaskMetrics): raise Exception(f"Deserialized value is not of type TaskMetrics: {type(metrics)}")
            task_id = key.decode('utf-8')
            if self.dag_structure_hash not in task_id: continue # only metrics grabbed from the same DAG are used
            if metrics.planner_used_name != planner_name: continue # only metrics grabbed from the same planner are used
 
            same_workflow_same_planner_type_metrics[task_id] = metrics 

            # Store upload/download speeds with resource configuration
            # DOWNLOAD SPEEDS
            for input_metric in metrics.input_metrics.input_download_metrics.values():
                if input_metric.time_ms is None or input_metric.serialized_size_bytes == -1: continue # it can be None if the input was present at the worker
                # Normalize the download speed based on memory
                self.cached_download_speeds.append((
                    input_metric.serialized_size_bytes / input_metric.time_ms,
                    input_metric.serialized_size_bytes,
                    metrics.worker_resource_configuration.cpus,
                    metrics.worker_resource_configuration.memory_mb
                ))

            # UPLOAD SPEEDS
            if metrics.output_metrics.tp_time_ms and metrics.output_metrics.serialized_size_bytes != -1: # it can be None if the output was present at the worker, for example
                # Normalize the upload speed based on memory
                self.cached_upload_speeds.append((
                    metrics.output_metrics.serialized_size_bytes / metrics.output_metrics.tp_time_ms,
                    metrics.output_metrics.serialized_size_bytes,
                    metrics.worker_resource_configuration.cpus,
                    metrics.worker_resource_configuration.memory_mb
                ))

        worker_startup_metrics = await self.metadata_storage.storage.mget(worker_startup_metrics_keys)
        for wsm in worker_startup_metrics:
            wsm = cloudpickle.loads(wsm)
            if not isinstance(wsm, WorkerStartupMetrics): raise Exception(f"Deserialized value is not of type WorkerStartupMetrics: {type(wsm)}")
            if self.dag_structure_hash not in wsm.master_dag_id: continue # only metrics grabbed from the same DAG are used
            if wsm.end_time_ms is None: continue
            if wsm.state == "cold": self.cached_worker_cold_start_times.append((wsm.end_time_ms - wsm.start_time_ms, wsm.resource_configuration.cpus, wsm.resource_configuration.memory_mb))
            elif wsm.state == "warm": self.cached_worker_warm_start_times.append((wsm.end_time_ms - wsm.start_time_ms, wsm.resource_configuration.cpus, wsm.resource_configuration.memory_mb))

        # Doesn't go to Redis
        for task_id, metrics in same_workflow_same_planner_type_metrics.items():
            function_name, _, dag_id = self._split_task_id(task_id)
            if metrics.execution_time_per_input_byte_ms is None: continue
            
            # EXECUTION TIME P/ BYTE
            if function_name not in self.cached_execution_time_per_byte: self.cached_execution_time_per_byte[function_name] = []
            self.cached_execution_time_per_byte[function_name].append((
                metrics.execution_time_per_input_byte_ms,
                metrics.worker_resource_configuration.cpus,
                metrics.worker_resource_configuration.memory_mb,
                metrics.input_metrics.hardcoded_input_size_bytes + sum([input_metric.serialized_size_bytes for input_metric in metrics.input_metrics.input_download_metrics.values()])
            ))

            # I/O RATIO
            if function_name not in self.cached_serialized_io_ratios:
                self.cached_serialized_io_ratios[function_name] = []
            input_size = metrics.input_metrics.hardcoded_input_size_bytes + sum([input_metric.serialized_size_bytes for input_metric in metrics.input_metrics.input_download_metrics.values()])
            output_size = metrics.output_metrics.serialized_size_bytes
            self.cached_serialized_io_ratios[function_name].append((output_size / input_size if input_size > 0 else 0, input_size))

        self.nr_of_previous_instances = int(len(same_workflow_same_planner_type_metrics) / self.nr_of_dag_nodes)
        logger.info(f"Loaded {len(same_workflow_same_planner_type_metrics.keys())}/{len(generic_metrics_keys)} useful metrics in {timer.stop()}ms")

    def has_required_predictions(self) -> bool:
        # print("cached upload speeds len: ", len(self.cached_upload_speeds))
        # print("cached download speeds len: ", len(self.cached_download_speeds))
        # # change cached io ratios to count the number of ratios for all function_names
        # print("cached io ratios len: ", sum(len(ratios) for ratios in self.cached_io_ratios.values()))
        # print("cached execution time per byte len: ", sum(len(ratios) for ratios in self.cached_execution_time_per_byte.values()))
        # print("cached worker cold start times len: ", len(self.cached_worker_cold_start_times))
        # print("cached worker warm start times len: ", len(self.cached_worker_warm_start_times))
        #* Needs to have at least SOME history for the SAME TYPE of workflow
        return len(self.cached_serialized_io_ratios) > 0 or len(self.cached_execution_time_per_byte) > 0

    def predict_output_size(self, function_name: str, input_size: int , sla: SLA, allow_cached: bool = True) -> int:
        if input_size < 0: raise ValueError("Input size cannot be negative")
        if function_name not in self.cached_serialized_io_ratios: return input_size

        function_io_ratios = self.cached_serialized_io_ratios[function_name]
        if len(function_io_ratios) == 0: return 0

        prediction_key = f"{function_name}-{input_size}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_output_sizes: 
            return self._cached_prediction_output_sizes[prediction_key]

        selected_ratios = self._select_related_samples(input_size, function_io_ratios, sla)
        if sla == "average": ratio = np.average(selected_ratios)
        else: ratio = np.percentile(selected_ratios, sla.value)

        res = math.ceil(input_size * ratio)
        self._cached_prediction_output_sizes[prediction_key] = res
        return res

    def predict_data_transfer_time(
        self,
        type: Literal['upload', 'download'],
        data_size_bytes: int,
        resource_config: TaskWorkerResourceConfiguration,
        sla: SLA,
        allow_cached: bool = True
    ) -> float:
        """Predict data transfer time for upload/download given data size and resources.
        
        Args:
            type: 'upload' or 'download'
            data_size_bytes: Size of data to transfer in bytes
            resource_config: Worker resource configuration (CPUs + RAM)
            sla: Either "average" for mean prediction or percentile (0-100)
            allow_cached: Whether to use cached predictions
    
        Note: Not using the `related_samples` tecnique here because when the value to predict is too low, the values will include bootstraping/cold-start times massively overestimating the prediction
        """
        if sla != "average" and (sla.value < 0 or sla.value > 100):
            raise ValueError("SLA must be 'average' or between 0 and 100")
        if data_size_bytes == 0: return 0
        
        all_samples = self.cached_upload_speeds if type == 'upload' else self.cached_download_speeds
        if len(all_samples) == 0: return 0
        
        prediction_key = f"{type}-{data_size_bytes}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_data_transfer_times: 
            return self._cached_prediction_data_transfer_times[prediction_key]

        # Filter samples by exact resource match
        _matching_samples = [
            (speed, total_bytes) for speed, total_bytes, cpus, memory_mb in all_samples
            if cpus == resource_config.cpus and memory_mb == resource_config.memory_mb
        ]

        download_k_base = 1.13
        upload_k_base = 1.04

        prediction_key = ""
        if len(_matching_samples) >= self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            matching_samples = self._select_related_samples(data_size_bytes, _matching_samples, sla, min_samples=60)
            # matching_samples = [speed for speed, _ in _matching_samples]

            adaptive_exponent = self._adaptive_scaling_exponent(
                data_size_bytes, 
                [total_bytes for _, total_bytes in _matching_samples], 
                sla, 
                k_base=download_k_base if type == 'download' else upload_k_base,
                alpha=0.5
            )

            if sla == "average":
                speed_bytes_per_ms = np.average(matching_samples)
            else:
                speed_bytes_per_ms = np.percentile(matching_samples, sla.value)
            
            if speed_bytes_per_ms <= 0:
                raise ValueError(f"No data available for {type} or invalid speed data")
            
            res = (data_size_bytes ** adaptive_exponent) / speed_bytes_per_ms
        else:
            _baseline_normalized_samples = [
                (speed * (BASELINE_MEMORY_MB / memory_mb) ** 0.3, total_bytes)
                for speed, total_bytes, cpus, memory_mb in all_samples
            ]

            baseline_normalized_samples = self._select_related_samples(data_size_bytes, _baseline_normalized_samples, sla, min_samples=60)

            adaptive_exponent = self._adaptive_scaling_exponent(data_size_bytes, [total_bytes for _, total_bytes in _baseline_normalized_samples], sla, 
                k_base=download_k_base if type == 'download' else upload_k_base,
                alpha=0.5
            )
            
            if sla == "average":
                baseline_speed_bytes_per_ms = np.average(baseline_normalized_samples)
            else:
                baseline_speed_bytes_per_ms = np.percentile(baseline_normalized_samples, sla.value)
            
            if baseline_speed_bytes_per_ms <= 0:
                raise ValueError(f"No data available for {type} or invalid baseline speed data")
            
            scaled_speed_bytes_per_ms = baseline_speed_bytes_per_ms * (resource_config.memory_mb / BASELINE_MEMORY_MB) ** 0.3
            res = (data_size_bytes ** adaptive_exponent) / scaled_speed_bytes_per_ms

        self._cached_prediction_data_transfer_times[prediction_key] = float(res)
        return float(res)

    def predict_worker_startup_time(self, resource_config: TaskWorkerResourceConfiguration, state: Literal['cold', 'warm'], sla: SLA, allow_cached: bool = True) -> float:
        """Predict worker startup time given resource configuration and state."""
        samples = self.cached_worker_cold_start_times if state == "cold" else self.cached_worker_warm_start_times
        if sla != "average" and (sla.value < 0 or sla.value > 100): raise ValueError("SLA must be 'average' or between 0 and 100")
        
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
            if sla == "average": startup_time = np.average(matching_samples)
            else: startup_time = np.percentile(matching_samples, sla.value)
            if startup_time <= 0: raise ValueError(f"No data available for predicting '{state}' worker startup time")
            res = startup_time
        else:
            # Insufficient exact matches - use memory scaling model with baseline normalization
            # First, normalize all samples to baseline memory configuration
            baseline_normalized_samples = [startup_time * (BASELINE_MEMORY_MB / memory_mb) ** 0.3 for startup_time, cpus, memory_mb in samples]
            if sla == "average":  startup_time = np.average(baseline_normalized_samples)
            else: startup_time = np.percentile(baseline_normalized_samples, sla.value)
            
            if startup_time <= 0: raise ValueError(f"No data available for predicting '{state}' worker startup time")
            
            # Scale from baseline to target resource configuration
            actual_startup_time = startup_time * (resource_config.memory_mb / BASELINE_MEMORY_MB) ** 0.3
            res = actual_startup_time
        
        self._cached_prediction_startup_times[prediction_key] = res # type: ignore
        return res # type: ignore

    def predict_execution_time(
        self,
        function_name: str,
        input_size: int,
        resource_config: TaskWorkerResourceConfiguration,
        sla: SLA,
        allow_cached: bool = True
    ) -> float:
        """Predict execution time for a function given input size and resources.
        
        Returns:
            Predicted execution time in milliseconds
            None if no data is available
        """
        if sla != "average" and (sla.value < 0 or sla.value > 100): 
            raise ValueError("SLA must be 'average' or between 0 and 100")
        if function_name not in self.cached_execution_time_per_byte: 
            return 0
        
        # Get all samples for this function
        all_samples = self.cached_execution_time_per_byte[function_name]
        if len(all_samples) == 0: 
            return 0
        
        prediction_key = f"{function_name}-{input_size}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_execution_times: 
            return self._cached_prediction_execution_times[prediction_key]
        
        # Filter samples by exact resource match
        _matching_samples = [
            (time_per_byte, total_input_size_bytes) for time_per_byte, cpus, memory_mb, total_input_size_bytes in all_samples
            if cpus == resource_config.cpus and memory_mb == resource_config.memory_mb
        ]

        # logger.info(f"Found {len(matching_samples)} exact resource matches for function {function_name} | nr samples: {len(all_samples)}")
        prediction_key = ""
        if len(_matching_samples) >= self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            # Select samples based on input size similarity
            matching_samples = self._select_related_samples(input_size, _matching_samples, sla)
            
            # Calculate prediction using the selected samples
            if sla == "average": 
                ms_per_byte = np.average(matching_samples)
            else: 
                ms_per_byte = np.percentile(matching_samples, sla.value)
            
            if ms_per_byte <= 0: 
                raise ValueError(f"No valid data available for function {function_name}")
                
            res = ms_per_byte * input_size
        else:
            # Insufficient exact matches - use memory scaling model with baseline normalization
            # Normalize samples to baseline memory configuration and select by input size
            samples_w_normalized_time_per_byte = [
                (time_per_byte * (memory_mb / BASELINE_MEMORY_MB) ** 0.3, total_input_size_bytes)
                for time_per_byte, cpus, memory_mb, total_input_size_bytes in all_samples
            ]
            
            # Select samples based on input size similarity
            baseline_normalized_samples = self._select_related_samples(input_size, samples_w_normalized_time_per_byte, sla)
            
            if sla == "average":
                baseline_ms_per_byte = np.average(baseline_normalized_samples) 
            else: 
                baseline_ms_per_byte = np.percentile(baseline_normalized_samples, sla.value)
            if baseline_ms_per_byte <= 0:  
                raise ValueError(f"No data available for function {function_name}")
            
            res = baseline_ms_per_byte * input_size * (BASELINE_MEMORY_MB / resource_config.memory_mb) ** 0.3
        
        self._cached_prediction_execution_times[prediction_key] = float(res)
        return float(res)

    def _adaptive_scaling_exponent(self, value_for_which_to_predict, samples: list[int | float], sla: SLA, k_base=0.8, alpha=0.5):
        """
        Compute adaptive scaling exponent for input size prediction.

        Args:
            value_for_which_to_predict: value to predict for
            samples: list of sample values used for prediction
            k_base: nominal power-law exponent
            alpha: smoothing factor (higher = faster transition to k_base)

        Returns:
            adaptive exponent (float)
        """
        # Use median sample size to reduce outlier influence
        if sla == "average": S_sample = np.average(samples)
        else: S_sample = np.percentile(samples, sla.value)
        ratio = np.abs(np.log(value_for_which_to_predict / S_sample))
        k_eff = 1 - (1 - k_base) * (1 - np.exp(-alpha * ratio)) # Smoothly interpolate between 1 and k_base
        return k_eff

    def _select_related_samples(self, reference_value: int, all_samples: list[tuple[float, int]], sla: SLA, max_samples = 100, min_samples = 6) -> list[float]:
        """
        to_predict: int
        all_samples: [(value_to_use_for_prediction, input_that_generated_the_value)]
        """
        if not all_samples: return []
        
        if sla == "average": baseline_of_observed_values = np.average([size for _, size in all_samples])
        else: baseline_of_observed_values = np.percentile([size for _, size in all_samples], sla.value)
        
        # Try increasing thresholds from 5% to 100% in 5% steps
        for threshold_percent in range(5, 101, 5):
            threshold = threshold_percent / 100.0
            distance_threshold = abs(baseline_of_observed_values * threshold)
            
            below_samples = []
            above_samples = []
            exact_samples = []
            
            # Collect samples within current threshold
            for idx, (value, size) in enumerate(all_samples):
                signed_distance = size - reference_value
                abs_distance = abs(signed_distance)
                
                if abs_distance <= distance_threshold:
                    if signed_distance < 0:
                        below_samples.append((abs_distance, value, size, idx))
                    elif signed_distance > 0:
                        above_samples.append((abs_distance, value, size, idx))
                    else:  # signed_distance == 0
                        exact_samples.append((0, value, size, idx))
            
            total_within_threshold = len(below_samples) + len(above_samples) + len(exact_samples)
            
            # If we have enough samples at this threshold, proceed with selection
            if total_within_threshold >= min_samples:
                # Sort by distance (closest first)
                below_samples.sort()
                above_samples.sort()
                
                remaining_budget = max_samples - len(exact_samples)
                if remaining_budget <= 0:
                    return [value for _, value, _, _ in exact_samples[:max_samples]]
                
                max_per_side = remaining_budget // 2
                
                selected_below = below_samples[:min(max_per_side, len(below_samples))]
                selected_above = above_samples[:min(max_per_side, len(above_samples))]
                total_selected = len(exact_samples) + len(selected_below) + len(selected_above)
                
                # If we're under the max_samples limit, try to take more from whichever side has remaining samples
                if total_selected < max_samples:
                    remaining_budget = max_samples - total_selected
                    
                    available_below = len(below_samples) - len(selected_below)
                    if available_below > 0:
                        additional_below = min(remaining_budget, available_below)
                        selected_below.extend(below_samples[len(selected_below):len(selected_below) + additional_below])
                        remaining_budget -= additional_below
                    
                    if remaining_budget > 0:
                        available_above = len(above_samples) - len(selected_above)
                        additional_above = min(remaining_budget, available_above)
                        selected_above.extend(above_samples[len(selected_above):len(selected_above) + additional_above])
                
                # Combine and return values
                all_selected = exact_samples + selected_below + selected_above
                return [value for _, value, _, _ in all_selected]
        
        # If we reach here, even at 100% threshold we don't have enough samples
        # Sort all samples by distance to reference_value and take the closest ones
        samples_with_distance = []
        for value, size in all_samples:
            abs_distance = abs(size - reference_value)
            samples_with_distance.append((abs_distance, value, size))
        
        # Sort by distance (closest first) and take up to min_samples. Since the closest samples are very far (more than 100% of the max observed value), pick a few samples (min_samples)
        samples_with_distance.sort()
        closest_samples = samples_with_distance[:min_samples]
        
        return [value for _, value, _ in closest_samples]

    def _split_task_id(self, task_id: str) -> tuple[str, str, str]:
        """ returns [function_name, task_id, master_dag_id] """
        task_id = task_id.removeprefix(MetadataStorage.TASK_MD_KEY_PREFIX)
        splits = task_id.split("+", maxsplit=1)
        function_name = splits[0]
        splits_2 = splits[1].split("+", maxsplit=1)
        task_id = splits_2[0]
        master_dag_id = splits_2[1]
        return function_name, task_id, master_dag_id