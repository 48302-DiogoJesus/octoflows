from dataclasses import dataclass


@dataclass
class TaskWorkerResourceConfiguration:
    cpus: float
    memory_mb: int
    worker_id: str | None = None
    def clone(self): return TaskWorkerResourceConfiguration(self.cpus, self.memory_mb, self.worker_id)