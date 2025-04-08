from collections import defaultdict
import os
import statistics
import sys
import time
from typing import List, Literal, Optional
from matplotlib import pyplot as plt
import numpy as np
import redis
import cloudpickle
import seaborn as sns

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.storage.metrics.metrics_storage import MetricsStorage, TaskMetrics

client = redis.Redis(
    host='localhost',
    port=6380,
    password='redisdevpwd123',
    decode_responses=False
)

def split_task_id(task_id: str) -> tuple[str, str, str]:
    """ returns [function_name, task_id, dag_id] """
    task_id = task_id.removeprefix(MetricsStorage.TASK_METRICS_KEY_PREFIX)
    splits = task_id.split("-", maxsplit=1)
    function_name = splits[0]
    splits_2 = splits[1].split("_")
    task_id = splits_2[0]
    dag_id = splits_2[1]
    return function_name, task_id, dag_id

# Input => Output relationship (consider hardcoded inputs)
def extract_task_io(task_metrics: list[tuple[str, TaskMetrics]]) -> dict[str, tuple[int, int]]:
    function_metrics: dict[str, tuple[int, int]] = {}
    
    for task_id, metrics in task_metrics:
        function_name, _, _ = split_task_id(task_id)
        function_metrics[function_name] = (
            sum(input_metric.size_bytes for input_metric in metrics.input_metrics) + sum(h_input_metric.size_bytes for h_input_metric in metrics.hardcoded_input_metrics),
            metrics.output_metrics.size_bytes
        )
    
    return function_metrics

# Input
def extract_task_io_speeds(metrics: List[tuple[str, TaskMetrics]]):
    upload_speeds = []
    download_speeds = []
    
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
    
    return {
        'upload': upload_speeds,
        'download': download_speeds
    }

def print_percentile_values(speeds):
    """Prints percentile values for upload, download, and all transfers"""
    percentiles = [10, 25, 50, 75, 90]
    
    print("\nTransfer Speed Percentiles (bytes/ms)")
    print("=" * 45)
    
    for direction in ['download', 'upload']:
        if not speeds[direction]:
            continue
            
        data = speeds[direction]
        percentile_values = np.percentile(data, percentiles)
        
        print(f"\n{direction.capitalize()} Speeds:")
        print("-" * 40)
        print(f"{'Percentile':<12} | {'Speed (bytes/ms)':>15} | {'Interpretation':<20}")
        print("-" * 40)
        
        for p, val in zip(percentiles, percentile_values):
            interpretation = {
                10: "Worst 10%",
                25: "Lower quartile",
                50: "Median",
                75: "Upper quartile",
                90: "Best 10%"
            }[p]
            print(f"{f'P{p}':<12} | {val:>15.2f} | {interpretation:<20}")

def plot_combined_speed_analysis(speeds):
    """Combined visualization showing both percentile curves and histograms"""
    # Create figure with 2 rows and 3 columns
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Comprehensive Transfer Speed Analysis', fontsize=16, y=1.02)
    
    # Common parameters
    percentiles = np.arange(101)
    histogram_percentiles = [10, 25, 50, 75, 90]
    colors = {'download': 'b', 'upload': 'r' }
    
    for col, direction in enumerate(['download', 'upload']):
        if not speeds[direction]:
            continue
            
        data = speeds[direction]
        
        # --- Top row: Percentile plots ---
        ax_top = axes[0, col]
        dir_percentiles = np.percentile(data, percentiles)
        ax_top.plot(percentiles, dir_percentiles, colors[direction] + '-')
        ax_top.set_title(f'{direction.capitalize()} Speed Percentiles')
        ax_top.set_xlabel('Percentile')
        ax_top.set_ylabel('Speed (bytes/ms)')
        ax_top.grid(True)
        
        # --- Bottom row: Histograms ---
        ax_bottom = axes[1, col]
        sns.histplot(data, bins=50, ax=ax_bottom, color=colors[direction], 
                    alpha=0.6, kde=True, stat='density')
        
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

def predict_transfer_time(
    speeds: dict, 
    data_size: float, 
    direction: Literal['upload', 'download'], 
    sla: float = 50,
) -> float:
    if direction not in speeds or not speeds[direction]:
        raise ValueError(f"No speed data available for direction: {direction}")
    
    if sla < 0 or sla > 100:
        raise ValueError("SLA must be between 0 and 100")
    
    percentile = 100 - sla
    speed = np.percentile(speeds[direction], percentile)
    
    if speed <= 0:
        return float('inf')
    
    return data_size / speed

def interactive_prediction(speeds: dict):
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
            time_ms = predict_transfer_time(speeds, data_size, direction, confidence)
            
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

if __name__ == "__main__":
    # Get all keys
    keys = client.keys('metrics-storage-tasks-*')

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

    tasks_speeds = extract_task_io_speeds(task_metrics)
    print("Task Speeds")
    print(tasks_speeds)
    
    tasks_io = extract_task_io(task_metrics)
    print("Task I/O")
    print(tasks_io)

    # plot_combined_speed_analysis(speeds)

    # print_percentile_values(speeds)
    # interactive_prediction(speeds)
    