from dataclasses import dataclass


@dataclass
class TaskWorkerResourceConfiguration:
    """ MANDATORY annotation that doesn't conflict with other annotations """
    cpus: float
    memory_mb: int
    worker_id: str | None = None
    def clone(self): return TaskWorkerResourceConfiguration(self.cpus, self.memory_mb, self.worker_id)