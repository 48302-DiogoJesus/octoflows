import os
import statistics
import sys
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
    task_id = task_id.removeprefix(MetricsStorage.KEY_PREFIX)
    splits = task_id.split("-", maxsplit=1)
    function_name = splits[0]
    splits_2 = splits[1].split("_")
    task_id = splits_2[0]
    dag_id = splits_2[1]
    return function_name, task_id, dag_id

def predict_remote_transfer_time(
    metrics: list[TaskMetrics],
    data_size: float,
    sla: Optional[Literal["median", "avg"]] = "median",
    percentile_value: Optional[float] = None
) -> float:
    """
    Predicts the time to transfer data of a given size based on historical metrics.
    
    Args:
        metrics: List of historical task metrics to analyze
        data_size: Size of data to predict transfer time for (in same units as input/output metrics)
        sla: Either "median", "avg", or None (if using percentile_value)
        percentile_value: Optional percentile value (e.g., 95 for 95th percentile)
    
    Returns:
        Predicted transfer time in milliseconds
    
    Raises:
        ValueError: If no metrics are provided or invalid parameters are given
    """
    if not metrics:
        raise ValueError("No metrics provided for prediction")
    
    # Collect all transfer speeds (size/time) from both inputs and outputs
    transfer_speeds = []
    
    for task_metric in metrics:
        # Add input transfer speeds
        for input_metric in task_metric.input_metrics:
            if input_metric.time_ms > 0:  # Avoid division by zero
                transfer_speeds.append(input_metric.size / input_metric.time_ms)
        
        # Add output transfer speed
        if task_metric.output_metrics.time_ms > 0:
            transfer_speeds.append(task_metric.output_metrics.size / task_metric.output_metrics.time_ms)
    
    if not transfer_speeds:
        raise ValueError("No transfer metrics available in the provided data")
    
    # Calculate the appropriate speed based on SLA
    if percentile_value is not None:
        if percentile_value < 0 or percentile_value > 100:
            raise ValueError("Percentile value must be between 0 and 100")
        speed = np.percentile(transfer_speeds, percentile_value)
    elif sla == "median":
        speed = statistics.median(transfer_speeds)
    elif sla == "avg":
        speed = statistics.mean(transfer_speeds)
    else:
        raise ValueError("Invalid SLA parameter. Must be 'median', 'avg', or use percentile_value")
    
    # Handle case where speed is 0 (shouldn't happen due to checks above)
    if speed <= 0:
        return float('inf')
    
    # Calculate predicted time
    return data_size / speed

# Function to extract transfer speeds from TaskMetrics
def extract_transfer_speeds(metrics: List[TaskMetrics]):
    upload_speeds = []
    download_speeds = []
    all_speeds = []
    
    for task_metric in metrics:
        # Extract download speeds (input metrics)
        for input_metric in task_metric.input_metrics:
            if input_metric.time_ms > 0:
                speed = input_metric.size / input_metric.time_ms
                download_speeds.append(speed)
                all_speeds.append(speed)
        
        # Extract upload speeds (output metrics)
        if task_metric.output_metrics.time_ms > 0:
            speed = task_metric.output_metrics.size / task_metric.output_metrics.time_ms
            upload_speeds.append(speed)
            all_speeds.append(speed)
    
    return {
        'upload': upload_speeds,
        'download': download_speeds,
        'all': all_speeds
    }

if __name__ == "__main__":
    # Get all keys
    keys = client.keys('*')

    task_metrics: list[TaskMetrics] = []

    # Deserialize each value using cloudpickle
    for key in keys:
        serialized_value = client.get(key)
        if serialized_value:
            deserialized: TaskMetrics = cloudpickle.loads(serialized_value) # type: ignore
            if not isinstance(deserialized, TaskMetrics):
                raise Exception(f"Deserialized value is not of type TaskMetrics: {type(deserialized)}")
            # task_id = key.decode('utf-8')
            task_metrics.append(deserialized)
        else:
            print(f"Key: {key.decode('utf-8')} has no value")

    print(f"Got {len(task_metrics)} task metrics")
    # res1 = predict_remote_transfer_time(task_metrics, 500, sla="median")
    # res2 = predict_remote_transfer_time(task_metrics, 500, sla="avg")
    # res3 = predict_remote_transfer_time(task_metrics, 500, sla=None, percentile_value=95)
    
    # print("Res1: ", res1, "ms")
    # print("Res2: ", res2, "ms")
    # print("Res3: ", res3, "ms")

    # Extract speeds
    speeds = extract_transfer_speeds(task_metrics)

    # Create a larger figure with just the single plot
    plt.figure(figsize=(12, 8))

    # Visualize percentiles
    percentiles = [5, 25, 50, 75, 95]
    percentile_values = np.percentile(speeds['all'], percentiles)

    # Create the histogram with KDE
    sns.histplot(speeds['all'], bins=30, kde=True)

    # Add the percentile lines with clearer labels
    for i, p in enumerate(percentiles):
        plt.axvline(percentile_values[i], color=f'C{i}', linestyle='--', 
                    linewidth=2, label=f'{p}th percentile: {percentile_values[i]:.2f} bytes/ms')

    # Add descriptive annotations for each percentile
    annotations = [
        "5th: Very conservative estimate (95% confidence)",
        "25th: Conservative estimate (75% confidence)",
        "50th: Median speed (typical case)",
        "75th: Optimistic estimate (25% confidence)",
        "95th: Very optimistic estimate (5% confidence)"
    ]

    # Add annotations to the right side of the plot
    for i, p in enumerate(percentiles):
        y_pos = 0.9 - (i * 0.05)
        plt.annotate(annotations[i], xy=(0.65, y_pos), xycoords='axes fraction',
                    bbox=dict(boxstyle="round,pad=0.3", fc=f"C{i}", alpha=0.2))

    # Make the title and labels larger and more descriptive
    plt.title('Transfer Speeds Distribution with Percentile Markers', fontsize=16)
    plt.xlabel('Transfer Speed (bytes/ms)', fontsize=14)
    plt.ylabel('Frequency', fontsize=14)
    plt.legend(loc='upper left', fontsize=12)

    # Add a grid for better readability
    plt.grid(True, alpha=0.3)

    # Adjust layout and save
    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    plt.savefig('transfer_speeds_percentiles.png', dpi=300)
    