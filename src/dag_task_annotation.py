from abc import ABC
from dataclasses import dataclass

from src.workers.worker_execution_logic import WorkerExecutionLogic


@dataclass
class TaskAnnotation(ABC, WorkerExecutionLogic): pass