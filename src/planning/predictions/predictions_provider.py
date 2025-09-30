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
    MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION = 20

    nr_of_previous_instances: int = 0

    # Changed to store tuples with resource configuration: (bytes/ms, cpus, memory_mb)
    cached_upload_speeds: list[tuple[float, int, float, int]] = [] # (bytes/ms, total_bytes, cpus, memory_mb)
    cached_download_speeds: list[tuple[float, int, float, int]] = [] # (bytes/ms, total_bytes, cpus, memory_mb)
    cached_deserialized_io_ratios: dict[str, list[tuple[float, int]]] = {} # (i/o ratio, input_size) for each function_name
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
                if input_metric.time_ms is None: continue # it can be None if the input was present at the worker
                # Normalize the download speed based on memory
                self.cached_download_speeds.append((
                    input_metric.serialized_size_bytes / input_metric.time_ms,
                    input_metric.serialized_size_bytes,
                    metrics.worker_resource_configuration.cpus,
                    metrics.worker_resource_configuration.memory_mb
                ))

            # UPLOAD SPEEDS
            if metrics.output_metrics.tp_time_ms: # it can be None if the output was present at the worker, for example
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
            function_name, _, _ = self._split_task_id(task_id)
            if metrics.execution_time_per_input_byte_ms is None: continue
            
            # EXECUTION TIME P/ BYTE
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
            self.cached_deserialized_io_ratios[function_name].append((output_size / input_size if input_size > 0 else 0, input_size))

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
        return len(self.cached_deserialized_io_ratios) > 0 or len(self.cached_serialized_io_ratios) > 0 or len(self.cached_execution_time_per_byte) > 0

    def predict_output_size(
        self,
        function_name: str,
        input_size: int,
        sla: SLA,
        deserialized: bool = True,
        allow_cached: bool = True
    ) -> int:
        """
        Predict output size from input size using weighted neighbor regression.
        Uses I/O ratio samples and applies a locally weighted fit for smoother estimates.
        """
        if input_size < 0:
            raise ValueError("Input size cannot be negative")

        ratios_dict = self.cached_deserialized_io_ratios if deserialized else self.cached_serialized_io_ratios
        if function_name not in ratios_dict:
            return input_size  # No data → fallback to identity scaling

        prediction_key = f"outsize-{function_name}-{input_size}-{sla}-{deserialized}"
        if allow_cached and prediction_key in self._cached_prediction_output_sizes:
            return self._cached_prediction_output_sizes[prediction_key]

        samples = ratios_dict[function_name]  # (ratio, input_size)
        if not samples:
            return input_size

        # Compute weighted neighbor prediction
        predicted_ratio = self._weighted_percentile_prediction(
            x_query=input_size,
            samples=samples,
            value_index=0,
            x_index=1,
            sla=sla
        )

        res = max(1, math.ceil(input_size * predicted_ratio))
        self._cached_prediction_output_sizes[prediction_key] = res
        return res

    def predict_data_transfer_time(
        self,
        type: Literal['upload', 'download'],
        data_size_bytes: int,
        resource_config: TaskWorkerResourceConfiguration,
        sla: SLA,
        allow_cached: bool = True,
        scaling_exponent: float = 0.85
    ) -> float:
        """
        Predict transfer time using size-speed relationship.
        Uses memory-aware normalization and nearest-neighbor smoothing.
        """
        if data_size_bytes <= 0:
            return 0.0
        if sla != "average" and not (0 <= sla.value <= 100):
            raise ValueError("SLA must be 'average' or between 0 and 100")

        cached_data = self.cached_upload_speeds if type == "upload" else self.cached_download_speeds
        if not cached_data:
            return 0.0

        prediction_key = f"transfer-{type}-{data_size_bytes}-{resource_config}-{sla}-{scaling_exponent}"
        if allow_cached and prediction_key in self._cached_prediction_data_transfer_times:
            return self._cached_prediction_data_transfer_times[prediction_key]

        # Exact resource matches first
        matching = [(speed, total_bytes) for speed, total_bytes, cpus, mem in cached_data
                    if cpus == resource_config.cpus and mem == resource_config.memory_mb]

        if len(matching) < self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            # Fallback: normalize memory to baseline
            matching = [
                (speed * (BASELINE_MEMORY_MB / mem) ** 0.4, total_bytes)
                for speed, total_bytes, cpus, mem in cached_data
            ]

        predicted_speed = self._weighted_percentile_prediction(
            x_query=data_size_bytes,
            samples=matching,
            value_index=0,
            x_index=1,
            sla=sla
        )

        if predicted_speed <= 0:
            return 0.0

        base_time = data_size_bytes / predicted_speed
        result = float(abs(base_time) ** scaling_exponent)
        self._cached_prediction_data_transfer_times[prediction_key] = result
        return result

    def predict_worker_startup_time(
        self,
        resource_config: TaskWorkerResourceConfiguration,
        state: Literal['cold', 'warm'],
        sla: SLA,
        allow_cached: bool = True
    ) -> float:
        """
        Predict worker startup time using weighted neighbor memory scaling.
        """
        samples = self.cached_worker_cold_start_times if state == "cold" else self.cached_worker_warm_start_times
        if not samples:
            return 0.0
        if sla != "average" and not (0 <= sla.value <= 100):
            raise ValueError("SLA must be 'average' or between 0 and 100")

        prediction_key = f"startup-{state}-{resource_config}-{sla}"
        if allow_cached and prediction_key in self._cached_prediction_startup_times:
            return self._cached_prediction_startup_times[prediction_key]

        matching = [(time, mem) for time, cpus, mem in samples
                    if cpus == resource_config.cpus and mem == resource_config.memory_mb]

        if len(matching) < self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            # Fallback: normalize memory to baseline
            matching = [(time * (BASELINE_MEMORY_MB / mem) ** 0.4, BASELINE_MEMORY_MB)
                        for time, _, mem in samples]

        predicted_time = self._weighted_percentile_prediction(
            x_query=resource_config.memory_mb,
            samples=matching,
            value_index=0,
            x_index=1,
            sla=sla
        )

        if len(matching) < self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            # Scale back to target memory
            predicted_time *= (resource_config.memory_mb / BASELINE_MEMORY_MB) ** 0.4

        self._cached_prediction_startup_times[prediction_key] = float(predicted_time)
        return float(predicted_time)

    def predict_execution_time(
        self,
        function_name: str,
        input_size: int,
        resource_config: TaskWorkerResourceConfiguration,
        sla: SLA,
        allow_cached: bool = True,
        size_scaling_factor: float = 0.8
    ) -> float:
        """
        Predict execution time using weighted neighbor regression with size scaling.
        """
        if function_name not in self.cached_execution_time_per_byte:
            return 0.0
        if sla != "average" and not (0 <= sla.value <= 100):
            raise ValueError("SLA must be 'average' or between 0 and 100")

        prediction_key = f"exec-{function_name}-{input_size}-{resource_config}-{sla}-{size_scaling_factor}"
        if allow_cached and prediction_key in self._cached_prediction_execution_times:
            return self._cached_prediction_execution_times[prediction_key]

        all_samples = self.cached_execution_time_per_byte[function_name]
        if not all_samples:
            return 0.0

        matching = [(tpb, total_input) for tpb, cpus, mem, total_input in all_samples
                    if cpus == resource_config.cpus and mem == resource_config.memory_mb]

        if len(matching) < self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            # Normalize to baseline memory
            matching = [
                (tpb * (mem / BASELINE_MEMORY_MB) ** 0.4, total_input)
                for tpb, cpus, mem, total_input in all_samples
            ]

        predicted_tpb = self._weighted_percentile_prediction(
            x_query=input_size,
            samples=matching,
            value_index=0,
            x_index=1,
            sla=sla
        )

        if predicted_tpb <= 0:
            return 0.0

        result = predicted_tpb * (input_size ** size_scaling_factor)
        if len(matching) < self.MIN_SAMPLES_OF_SAME_RESOURCE_CONFIGURATION:
            result *= (BASELINE_MEMORY_MB / resource_config.memory_mb) ** 0.4

        self._cached_prediction_execution_times[prediction_key] = float(result)
        return float(result)

    # ==============================
    # 🔧 Utility: Weighted Prediction
    # ==============================
    def _weighted_percentile_prediction(
        self,
        x_query: float,
        samples: list[tuple],
        value_index: int,
        x_index: int,
        sla: SLA,
        bandwidth: float = 0.25
    ) -> float:
        if not samples:
            return 0.0

        xs = np.array([s[x_index] for s in samples], dtype=float)
        ys = np.array([s[value_index] for s in samples], dtype=float)

        scale = np.std(xs) if np.std(xs) > 0 else max(1.0, np.mean(xs))
        h = bandwidth * scale

        weights = np.exp(-0.5 * ((xs - x_query) / h) ** 2)
        weights /= np.sum(weights) if np.sum(weights) > 0 else len(weights)

        if sla == "average":
            return float(np.sum(weights * ys))
        else:
            order = np.argsort(ys)
            sorted_y = ys[order]
            sorted_w = weights[order]
            cum_w = np.cumsum(sorted_w)
            cutoff = sla.value / 100.0

            idx = np.searchsorted(cum_w, cutoff, side="right")
            idx = min(idx, len(sorted_y) - 1)  # <- clamp to last index
            return float(sorted_y[idx])


    def _select_related_samples(self, reference_value: int, all_samples: list[tuple[float, int]], sla: SLA, max_samples = 100, min_samples = 6) -> tuple[list[float], list[tuple[float, int, float]]]:
        """
        reference_value: int
        all_samples: [(value, comparable_to_reference)]
        """
        if not all_samples:
            return [], []
        
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
                    return [value for _, value, _, _ in exact_samples[:max_samples]], [(value, total, threshold) for _, value, total, _ in exact_samples[:max_samples]]
                
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
                return [value for _, value, _, _ in all_selected], [(value, total, threshold) for _, value, total, _ in all_selected]
        
        # If we reach here, even at 100% threshold we don't have enough samples
        # Sort all samples by distance to reference_value and take the closest ones
        samples_with_distance = []
        for value, size in all_samples:
            abs_distance = abs(size - reference_value)
            samples_with_distance.append((abs_distance, value, size))
        
        # Sort by distance (closest first) and take up to min_samples. Since the closest samples are very far (more than 100% of the max observed value), pick a few samples (min_samples)
        samples_with_distance.sort()
        closest_samples = samples_with_distance[:min_samples]
        
        return [value for _, value, _ in closest_samples], [(value, size, 1.0) for _, value, size in closest_samples]

    def _split_task_id(self, task_id: str) -> tuple[str, str, str]:
        """ returns [function_name, task_id, master_dag_id] """
        task_id = task_id.removeprefix(MetadataStorage.TASK_MD_KEY_PREFIX)
        splits = task_id.split("+", maxsplit=1)
        function_name = splits[0]
        splits_2 = splits[1].split("+", maxsplit=1)
        task_id = splits_2[0]
        master_dag_id = splits_2[1]
        return function_name, task_id, master_dag_id