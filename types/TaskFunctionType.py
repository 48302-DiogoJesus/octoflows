from typing import Protocol, Union
from generics import RType, TType
from ..task import TaskNode

class TaskFunctionType(Protocol[TType, RType]):
    def __call__(self, arg: Union[TType, TaskNode[TType]]) -> TaskNode[RType]: ...