import asyncio
from dataclasses import dataclass

from src.dag_task_annotation import TaskAnnotation
from src.dag_task_node import DAGTaskNode

@dataclass
class PreLoad(TaskAnnotation):
    """ Indicates that the upstream dependencies of a task annotated with this annotation should be downloaded as soon as possible """

    # for upstream tasks
    preloading_complete_events: dict[str, asyncio.Event] = {}
    # Flag that indicates if starting new preloading for upstream tasks of this task is allowed or not
    allow_new_preloads = True
    _lock: asyncio.Lock = asyncio.Lock()

    def clone(self): return PreLoad()