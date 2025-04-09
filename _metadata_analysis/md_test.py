import os
import sys
import time
from typing import Dict, List, Literal, Optional, Union
from matplotlib import pyplot as plt
import numpy as np
import redis
import cloudpickle
import seaborn as sns


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.metrics.metrics_storage import MetricsStorage, TaskMetrics

client = redis.Redis(
    host='localhost',
    port=6380,
    password='redisdevpwd123',
    decode_responses=False
)

def plot_combined_speed_analysis(data):
    """Combined visualization showing both percentile curves and histograms"""
    # Create figure with 2 rows and 3 columns
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Comprehensive Transfer Speed Analysis', fontsize=16, y=1.02)
    
    # Common parameters
    percentiles = np.arange(101)
    histogram_percentiles = [10, 25, 50, 75, 90]
    
    # --- Top row: Percentile plots ---
    ax_top = axes[0, col]
    dir_percentiles = np.percentile(data, percentiles)
    ax_top.plot(percentiles, dir_percentiles)
    ax_top.set_title('Percentiles')
    ax_top.set_xlabel('Percentile')
    ax_top.set_ylabel('Speed (bytes/ms)')
    ax_top.grid(True)
    
    # --- Bottom row: Histograms ---
    ax_bottom = axes[1]
    sns.histplot(data, bins=50, ax=ax_bottom, alpha=0.6, kde=True, stat='density')
    
    # Mark percentiles on histogram
    percentiles_values = np.percentile(data, histogram_percentiles)
    for p_val, p in zip(histogram_percentiles, percentiles_values):
        ax_bottom.axvline(p, color='k', linestyle='--', alpha=0.7)
        ax_bottom.text(p, ax_bottom.get_ylim()[1]*0.9, f'P{p_val}', 
                        rotation=90, va='top', ha='right', backgroundcolor='w')
    
    ax_bottom.set_title(f'{direction.capitalize()} Speed Distribution')
    ax_bottom.set_xlabel('Speed (bytes/ms)')
    ax_bottom.set_ylabel('Density')
    ax_bottom.grid(True)
    
    plt.tight_layout()
    # plt.show()
    plt.savefig('data_transfer_percentiles.png')

def plot_percentiles(values: list[float], 
                    save_path: str = 'percentiles.png',
                    percentiles: list[float] = [10, 25, 50, 75, 90]) -> None:
    """
    Simple percentile visualization with boxplot and histogram
    
    Args:
        values: List of numerical values to analyze
        save_path: Where to save the image (default: 'percentiles.png')
        percentiles: Which percentiles to highlight (default: [10, 25, 50, 75, 90])
    """
    if not values:
        raise ValueError("Input list cannot be empty")
    
    # Calculate percentiles
    percentile_values = np.percentile(values, percentiles)
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle('Percentile Analysis', fontsize=14)
    
    # Boxplot
    ax1.boxplot(values, vert=False, showfliers=False)
    ax1.set_title('Boxplot')
    ax1.set_xlabel('Values')
    ax1.grid(True, alpha=0.3)
    
    # Histogram with percentile markers
    ax2.hist(values, bins=30, alpha=0.7, edgecolor='black')
    ax2.set_title('Histogram with Percentiles')
    ax2.set_xlabel('Values')
    ax2.set_ylabel('Frequency')
    ax2.grid(True, alpha=0.3)
    
    # Add percentile lines and labels to histogram
    colors = plt.cm.viridis(np.linspace(0, 1, len(percentiles)))
    for p, val, color in zip(percentiles, percentile_values, colors):
        ax2.axvline(val, color=color, linestyle='--', linewidth=1.5)
        ax2.text(val, ax2.get_ylim()[1]*0.9, f'P{p}: {val:.2f}',
                rotation=90, va='top', ha='right', color=color)
    
    plt.tight_layout()
    plt.savefig(save_path)
    plt.show()

### PREDICTION FUNCTIONS ###
def predict_function_output_size(
    io_ratios: dict[str, list[float]], 
    function_name: str,
    input_size: float, 
    sla: float = 50,
) -> float:
    """
    Predicts output size based on historical I/O ratios with specified confidence level.
    
    Args:
        io_ratios: Dictionary mapping function names to lists of historical I/O ratios
        function_name: Name of the function to predict for
        input_size: Size of input data in bytes
        sla: Confidence level (0-100) where higher means more conservative prediction
        
    Returns:
        Predicted output size in bytes
    """
    if function_name not in io_ratios:
        raise ValueError(f"No I/O ratio data available for function: {function_name}")
    if not io_ratios[function_name]:
        raise ValueError(f"Empty I/O ratio data for function: {function_name}")
    if sla < 0 or sla > 100:
        raise ValueError("SLA must be between 0 and 100")
    if input_size < 0:
        raise ValueError("Input size cannot be negative")
    
    # Calculate the ratio at the requested percentile
    percentile = 100 - sla
    ratio = np.percentile(io_ratios[function_name], percentile)
    
    return input_size * ratio

def predict_data_transfer_time(
    speeds: dict, 
    data_size: float, 
    direction: Literal['upload', 'download'], 
    sla: float = 50,
) -> float:
    if direction not in speeds or not speeds[direction]: raise ValueError(f"No speed data available for direction: {direction}")
    if sla < 0 or sla > 100: raise ValueError("SLA must be between 0 and 100")
    
    percentile = 100 - sla
    speed = np.percentile(speeds[direction], percentile)
    
    if speed <= 0:
        return float('inf')
    
    return data_size / speed

def predict_execution_time(
    task_metrics: list[tuple[str, TaskMetrics]],
    function_name: str,
    input_size: float,
    resource_config: TaskWorkerResourceConfiguration,
    sla: float = 50,
    use_all_resources: bool = True,
    similarity_threshold: float = 0.1
) -> float:
    """
    Enhanced execution time prediction that considers all historical data with weighted adjustments
    
    Args:
        task_metrics: List of historical task metrics
        function_name: Name of the function to predict
        input_size: Size of input data in bytes
        resource_config: Worker resource configuration
        sla: Confidence level (0-100)
        use_all_resources: Whether to use data from all resource configs (with adjustments)
        similarity_threshold: Threshold for considering resources similar (default 10%)
        
    Returns:
        Predicted execution time in milliseconds
    """
    if sla < 0 or sla > 100:
        raise ValueError("SLA must be between 0 and 100")
    
    # Collect all relevant metrics for the function
    relevant_metrics = []
    for task_id, metrics in task_metrics:
        fn_name, _, _ = _split_task_id(task_id)
        if fn_name == function_name and metrics.worker_resource_configuration:
            relevant_metrics.append(metrics)
    
    if not relevant_metrics:
        raise ValueError(f"No historical data found for function {function_name}")
    
    # Calculate normalized execution times with resource adjustment factors
    weighted_times = []
    
    for metrics in relevant_metrics:
        total_input = sum(m.size_bytes for m in metrics.input_metrics) + \
                     sum(m.size_bytes for m in metrics.hardcoded_input_metrics)
        
        if total_input <= 0:
            continue
            
        # Calculate resource adjustment factors
        hist_cpu = metrics.worker_resource_configuration.cpus
        hist_mem = metrics.worker_resource_configuration.memory_mb
        
        # CPU adjustment (inverse relationship - more CPUs should mean faster)
        cpu_factor = hist_cpu / resource_config.cpus if resource_config.cpus > 0 else 1
        
        # Memory adjustment (complex relationship - we'll use square root for diminishing returns)
        mem_factor = np.sqrt(hist_mem / resource_config.memory_mb) if resource_config.memory_mb > 0 else 1
        
        # Combine factors (weighted average)
        resource_factor = 0.7 * cpu_factor + 0.3 * mem_factor
        
        # Calculate normalized time with resource adjustment
        normalized_time = (metrics.execution_time_ms / total_input) * resource_factor
        weighted_times.append(normalized_time)
    
    if not weighted_times:
        raise ValueError("No valid input size data available for prediction")
    
    # Calculate the normalized time at the requested percentile
    percentile = 100 - sla
    predicted_normalized = np.percentile(weighted_times, percentile)
    
    return predicted_normalized * input_size

### INTERACTIVE PREDICTIONS ###
def interactive_data_transfer_speed_prediction(speeds: dict):
    """Interactive console interface for transfer time prediction"""
    print("\nTransfer Time Predictor")
    print("----------------------")
    
    while True:
        try:
            print("\nOptions:")
            print("1. Predict transfer time")
            print("2. Exit")
            choice = input("Enter your choice (1-2): ").strip()
            
            if choice == '2':
                break
                
            if choice != '1':
                print("Invalid choice, please try again")
                continue
                
            # Get prediction parameters
            data_size = float(input("Enter data size in bytes: "))
            direction = input("Enter direction (upload/download): ").lower().strip()
            if direction not in ['upload', 'download']:
                print("Invalid direction, must be 'upload' or 'download'")
                continue
                
            confidence = float(input("Enter confidence percentile (0-100): "))
            if confidence < 0 or confidence > 100:
                print("Confidence must be between 0 and 100")
                continue
                
            # Calculate and display prediction
            time_ms = predict_data_transfer_time(speeds, data_size, direction, confidence)
            
            if time_ms == float('inf'):
                print("Prediction: Transfer speed at this percentile is 0 (infinite time)")
            else:
                print(f"\nPredicted transfer time at {confidence}th percentile:")
                print(f"- Direction: {direction}")
                print(f"- Data size: {data_size:,} bytes")
                print(f"- Time: {time_ms:.2f} ms")
                print(f"- Equivalent to {time_ms/1000:.2f} seconds")
                
        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

def interactive_io_prediction(io_ratios: dict[str, list[float]]):
    """Interactive console interface for I/O size prediction"""
    print("\nI/O Size Predictor")
    print("-----------------")
    
    while True:
        try:
            print("\nOptions:")
            print("1. Predict output size")
            print("2. Exit")
            choice = input("Enter your choice (1-2): ").strip()
            
            if choice == '2':
                break
                
            if choice != '1':
                print("Invalid choice, please try again")
                continue
                
            # Get prediction parameters
            function_name = input("Enter function name: ").strip()
            input_size = float(input("Enter input size in bytes: "))
            confidence = float(input("Enter confidence percentile (0-100): "))
            if confidence < 0 or confidence > 100:
                print("Confidence must be between 0 and 100")
                continue
                
            # Calculate and display prediction
            output_size = predict_function_output_size(io_ratios, function_name, input_size, confidence)
            
            print(f"\nPredicted output size at {confidence}th percentile:")
            print(f"- Function: {function_name}")
            print(f"- Input size: {input_size:,} bytes")
            print(f"- Output size: {output_size:,.2f} bytes")
            print(f"- I/O ratio: {output_size/input_size:.4f}")
                
        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

def interactive_execution_time_prediction(task_metrics: list[tuple[str, TaskMetrics]]):
    """Interactive console interface for execution time prediction"""
    print("\nExecution Time Predictor")
    print("-----------------------")
    
    while True:
        try:
            print("\nOptions:")
            print("1. Predict execution time")
            print("2. Exit")
            choice = input("Enter your choice (1-2): ").strip()
            
            if choice == '2':
                break
                
            if choice != '1':
                print("Invalid choice, please try again")
                continue
                
            # Get prediction parameters
            function_name = input("Enter function name: ").strip()
            input_size = float(input("Enter input size in bytes: "))
            cpu = float(input("Enter CPU cores: "))
            memory = int(input("Enter memory (MB): "))
            confidence = float(input("Enter confidence percentile (0-100): "))
            
            if confidence < 0 or confidence > 100:
                print("Confidence must be between 0 and 100")
                continue
                
            # Create resource configuration
            resource_config = TaskWorkerResourceConfiguration(cpus=cpu, memory_mb=memory)
            
            # Calculate and display prediction
            time_ms = predict_execution_time(task_metrics, function_name, input_size, resource_config, confidence)
            
            print(f"\nPredicted execution time at {confidence}th percentile:")
            print(f"- Function: {function_name}")
            print(f"- Input size: {input_size:,} bytes")
            print(f"- CPU cores: {cpu}")
            print(f"- Memory: {memory} MB")
            print(f"- Time: {time_ms:.2f} ms")
            print(f"- Equivalent to {time_ms/1000:.2f} seconds")
                
        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

### VISUALIZATION ###
def print_percentile_values(data: list):
    """Prints percentile values for upload, download, and all transfers"""
    percentiles = [25, 50, 75, 90]
    
    print("\nPercentiles")
    print("=" * 65)

    percentile_values = np.percentile(data, percentiles)
    
    print("-" * 65)
    print(f"{'Percentile':<12} | {'Value':>15} | {'Interpretation':<20} | {'Confidence':<15}")
    print("-" * 65)
    
    for p, val in zip(percentiles, percentile_values):
        interpretation = {
            25: "Lower quartile",
            50: "Median",
            75: "Upper quartile",
            90: "Best 10%"
        }[p]
        
        confidence = {
            25: "75% confidence",
            50: "50% confidence",
            75: "25% confidence",
            90: "10% confidence"
        }[p]
        
        print(f"{f'P{p}':<12} | {val:>15.2f} | {interpretation:<20} | {confidence:<15}")

### DATA PREPARATION and UTILS ###
def _extract_task_io_speeds(metrics: List[tuple[str, TaskMetrics]]) -> tuple[list[float], list[float]]:
    """ returns (upload_speeds, download_speeds) """
    upload_speeds: list[float] = []
    download_speeds: list[float] = []
    
    for _, task_metric in metrics:
        # Extract download speeds (input metrics)
        for input_metric in task_metric.input_metrics:
            if input_metric.time_ms > 0:
                speed = input_metric.size_bytes / input_metric.time_ms
                download_speeds.append(speed)
        
        # Extract upload speeds (output metrics)
        if task_metric.output_metrics.time_ms > 0:
            speed = task_metric.output_metrics.size_bytes / task_metric.output_metrics.time_ms
            upload_speeds.append(speed)
    
    return (upload_speeds, download_speeds)

# Input => Output relationship (consider hardcoded inputs)
def _extract_task_io(task_metrics: list[tuple[str, TaskMetrics]]) -> dict[str, list[float]]:
    function_metrics: dict[str, list[float]] = {}
    
    for task_id, metrics in task_metrics:
        function_name, _, _ = _split_task_id(task_id)
        if function_name not in function_metrics:
            function_metrics[function_name] = []

        input = sum(input_metric.size_bytes for input_metric in metrics.input_metrics) + sum(h_input_metric.size_bytes for h_input_metric in metrics.hardcoded_input_metrics)
        output = metrics.output_metrics.size_bytes
        io_ratio = output / input if input > 0 else 0
        function_metrics[function_name].append(io_ratio)
    
    return function_metrics

def _split_task_id(task_id: str) -> tuple[str, str, str]:
    """ returns [function_name, task_id, dag_id] """
    task_id = task_id.removeprefix(MetricsStorage.TASK_METRICS_KEY_PREFIX)
    splits = task_id.split("-", maxsplit=1)
    function_name = splits[0]
    splits_2 = splits[1].split("_")
    task_id = splits_2[0]
    dag_id = splits_2[1]
    return function_name, task_id, dag_id

if __name__ == "__main__":
    # Get all keys
    keys = client.keys(f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}*")

    task_metrics: list[tuple[str, TaskMetrics]] = []
    start = time.time()

    # Deserialize each value using cloudpickle
    for key in keys:
        serialized_value = client.get(key)
        if serialized_value:
            deserialized: TaskMetrics = cloudpickle.loads(serialized_value) # type: ignore
            if not isinstance(deserialized, TaskMetrics):
                raise Exception(f"Deserialized value is not of type TaskMetrics: {type(deserialized)}")
            task_id = key.decode('utf-8')
            task_metrics.append((task_id, deserialized))
        else:
            print(f"Key: {key.decode('utf-8')} has no value")

    print(f"Got {len(task_metrics)} task metrics in {time.time() - start:.4f} seconds")

    # upload_speeds, download_speeds = _extract_task_io_speeds(task_metrics)
    # print("Task Speeds")
    # print_percentile_values(upload_speeds)
    # print_percentile_values(download_speeds)
    
    # tasks_io = _extract_task_io(task_metrics)
    # print("Task I/O")
    # first_task_io = tasks_io[list(tasks_io.keys())[0]]
    # print_percentile_values(first_task_io)
    # plot_percentiles(first_task_io)


    