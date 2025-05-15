from dataclasses import dataclass

from src.dag_task_annotation import TaskAnnotation

@dataclass
class TaskWorkerResourceConfiguration(TaskAnnotation):
    """ MANDATORY annotation that doesn't conflict with other annotations """
    cpus: float
    memory_mb: int
    worker_id: str = ""

    def clone(self): return TaskWorkerResourceConfiguration(self.cpus, self.memory_mb, self.worker_id)