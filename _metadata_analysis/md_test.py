import os
import sys
import redis
import cloudpickle

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.storage.metrics.metrics_storage import MetricsStorage, TaskMetrics  # Make sure to install this package first: pip install cloudpickle

client = redis.Redis(
    host='localhost',
    port=6380,
    password='redisdevpwd123',
    decode_responses=False  # Keep as False since we're handling serialized data
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

def print_task_metrics(task_id: str, m: TaskMetrics):
    function_name, task_id, dag_id = split_task_id(task_id)
    print(f"> Function Name: {function_name} | DAG ID: {dag_id} | Task ID: {task_id}")
    print(f"  Worker ID: {m.worker_id}")
    print(f"  Execution Time (ms): {m.execution_time_ms}")
    print(f"  Input Metrics (len: {len(m.input_metrics)})")
    for input_metric in m.input_metrics:
        print(f"    Task ID: {input_metric.task_id}")
        print(f"    Execution Time (ms): {input_metric.time_ms}")
        print(f"    Size: {input_metric.size}")
    print(f"  Output Metrics")
    print(f"    Execution Time (ms): {m.output_metrics.time_ms}")
    print(f"    Size: {m.output_metrics.size}")
    print(f"  Downstream Invocations: {len(m.downstream_invocation_times) if m.downstream_invocation_times else 'None'}")
    if m.downstream_invocation_times:
        for downstream_invocation in m.downstream_invocation_times:
            print(f"    Task ID: {downstream_invocation.task_id}")
            print(f"    Invocation Time (ms): {downstream_invocation.time_ms}")

if __name__ == "__main__":
    # Get all keys
    keys = client.keys('*')

    # Deserialize each value using cloudpickle
    for key in keys:
        serialized_value = client.get(key)
        if serialized_value:
            deserialized: TaskMetrics = cloudpickle.loads(serialized_value) # type: ignore
            if not isinstance(deserialized, TaskMetrics):
                raise Exception(f"Deserialized value is not of type TaskMetrics: {type(deserialized)}")
            task_id = key.decode('utf-8')
            print_task_metrics(task_id, deserialized)
        else:
            print(f"Key: {key.decode('utf-8')} has no value")