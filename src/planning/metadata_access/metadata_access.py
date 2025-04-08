from src.planning.dag_planner import SLA
from src.storage.metrics import metrics_storage
from src.worker_resource_configuration import TaskWorkerResourceConfiguration


class MetadataAccess:
    def __init__(self, metrics_storage: metrics_storage.MetricsStorage) -> None:
        # TODO: Receive access to metrics_storage
        self.metrics_storage = metrics_storage
        pass

    def predict_remote_download_time(self, data_size: int , sla: SLA) -> int:
        return -1

    def predict_output_size(self, task_id: str, input_size: int , sla: SLA) -> int:
        return -1

    def predict_execution_time(self, task_id: str, input_size: int, available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA) -> list[tuple[TaskWorkerResourceConfiguration, float]]:
        return []