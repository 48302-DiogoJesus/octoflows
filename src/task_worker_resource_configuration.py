from dataclasses import dataclass, field


@dataclass
class TaskWorkerResourceConfiguration:
    memory_mb: int
    worker_id: str | None = None
    cpus: float = field(init=False)

    def __post_init__(self):
        """
        Calculate CPUs based on memory using AWS Lambda rules:
        ~1792 MB = 1 vCPU, max 6 vCPUs, rounded to 2 decimals.
        """
        self.cpus = round(min(self.memory_mb / 1792.0, 6.0), 2)

    def clone(self, memory_mb: int | None = None):
        return TaskWorkerResourceConfiguration(
            memory_mb=memory_mb if memory_mb is not None else self.memory_mb,
            worker_id=self.worker_id,
        )