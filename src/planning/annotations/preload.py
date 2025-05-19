import asyncio
from dataclasses import dataclass, field

from src.dag_task_annotation import TaskAnnotation

@dataclass
class PreLoad(TaskAnnotation):
    """ Indicates that the upstream dependencies of a task annotated with this annotation should be downloaded as soon as possible """

    # for upstream tasks
    preloading_complete_events: dict[str, asyncio.Event] = field(default_factory=dict)
    # Flag that indicates if starting new preloading for upstream tasks of this task is allowed or not
    allow_new_preloads: bool = True
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def clone(self): return PreLoad()
