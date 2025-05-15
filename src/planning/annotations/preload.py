from dataclasses import dataclass

from src.dag_task_annotation import TaskAnnotation

@dataclass
class PreLoad(TaskAnnotation):
    """ Indicates that the upstream dependencies of a task annotated with this annotation should be downloaded as soon as possible """

    def clone(self): return PreLoad()