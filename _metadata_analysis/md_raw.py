import os
import sys
import redis
import cloudpickle

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.storage.metrics.metrics_storage import FullDAGPrepareTime, MetricsStorage, TaskMetrics

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

def print_task_metrics(task_id: str, m: TaskMetrics):
    print(f"> Task Id: {task_id}")
    print(f"\tWorker Id: {m.worker_id}")
    print(f"\tWorker Resource Configuration: {m.worker_resource_configuration}")
    print(f"\tStarted At Timestamp: {m.started_at_timestamp}")
    print(f"\tInput Metrics Len: {len(m.input_metrics)} | Sum: {sum(input_metric.size_bytes for input_metric in m.input_metrics)} bytes")
    print(f"\tHardcoded Input Metrics Len: {len(m.hardcoded_input_metrics)} | Sum: {sum(h_input_metric.size_bytes for h_input_metric in m.hardcoded_input_metrics)} bytes")
    print(f"\tTotal Input Download Time: {m.total_input_download_time_ms} ms")
    print(f"\tExecution Time: {m.execution_time_ms} ms")
    print(f"\tUpdate Dependency Counters Time: {m.update_dependency_counters_time_ms} ms")
    print(f"\tOutput Metrics: {m.output_metrics}")
    print(f"\tDownstream Invocation Times: {m.downstream_invocation_times}")
    print(f"\tTotal Invocation Time: {m.total_invocation_time_ms} ms")

def get_all_task_metrics() -> list[tuple[str, TaskMetrics]]:
    keys = client.keys('metrics-storage-tasks-*')

    task_metrics: list[tuple[str, TaskMetrics]] = []

    # Deserialize each value using cloudpickle
    for key in keys:
        serialized_value = client.get(key)
        if serialized_value:
            deserialized: TaskMetrics = cloudpickle.loads(serialized_value) # type: ignore
            if not isinstance(deserialized, TaskMetrics): raise Exception(f"Deserialized value is not of type TaskMetrics: {type(deserialized)}")
            task_id = key.decode('utf-8')
            task_metrics.append((task_id, deserialized))
        else:
            print(f"Key: {key.decode('utf-8')} has no value")
    
    return task_metrics

def get_all_dag_prepare_metrics() -> list[FullDAGPrepareTime]:
    keys = client.keys('metrics-storage-dag-*')

    task_metrics: list[FullDAGPrepareTime] = []

    # Deserialize each value using cloudpickle
    for key in keys:
        serialized_value = client.get(key)
        if serialized_value:
            deserialized: FullDAGPrepareTime = cloudpickle.loads(serialized_value) # type: ignore
            if not isinstance(deserialized, FullDAGPrepareTime): raise Exception(f"Deserialized value is not of type TaskMetrics: {type(deserialized)}")
            task_metrics.append(deserialized)
        else:
            print(f"Key: {key.decode('utf-8')} has no value")
    
    return task_metrics

def print_dag_prepare_metrics(dag_prep_metric: FullDAGPrepareTime):
    print(f"> DAG Fetch Time")
    print(f"\t> Download Time: {dag_prep_metric.download_time_ms} ms")
    print(f"\tDownload Time: {dag_prep_metric.create_subdag_time_ms} ms")
    print(f"\tSize: {dag_prep_metric.size_bytes} bytes")

if __name__ == "__main__":
    task_metrics = get_all_task_metrics()
    print(f"Got {len(task_metrics)} task metrics")
    for task_metric in task_metrics:
        print_task_metrics(task_metric[0], task_metric[1])

    dag_prepare_metrics = get_all_dag_prepare_metrics()
    print(f"Got {len(dag_prepare_metrics)} dag prepare metrics")
    for dag_prepare_metric in dag_prepare_metrics:
        print_dag_prepare_metrics(dag_prepare_metric)