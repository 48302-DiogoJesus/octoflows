from dataclasses import dataclass


@dataclass
class TaskWorkerResourceConfiguration:
    cpus: float
    memory_mb: int
    worker_id: str | None = None
    def clone(self, cpus: float | None = None, memory_mb: int | None = None): 
        return TaskWorkerResourceConfiguration(cpus if cpus else self.cpus, memory_mb if memory_mb else self.memory_mb, self.worker_id)