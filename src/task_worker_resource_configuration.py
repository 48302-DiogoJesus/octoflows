from dataclasses import dataclass, field


@dataclass
class TaskWorkerResourceConfiguration:
    memory_mb: int
    worker_id: str | None = None

    @property
    def cpus(self) -> float:
        """
        AWS Lambda CPU allocation rule:
        ~1792 MB = 1 vCPU. Max 6 vCPUs.
        Rounded to 2 decimal places.
        """
        if self.memory_mb == -1: return -1
        vcpus = self.memory_mb / 1792.0
        return round(min(vcpus, 6.0), 2)

    def clone(self, memory_mb: int | None = None):
        return TaskWorkerResourceConfiguration(
            memory_mb=memory_mb if memory_mb is not None else self.memory_mb,
            worker_id=self.worker_id,
        )