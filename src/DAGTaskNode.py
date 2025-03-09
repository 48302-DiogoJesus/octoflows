from dataclasses import dataclass
from functools import wraps
from typing import Callable, Generic, TypeVar
import uuid

import cloudpickle

from .IntermediateStorage import IntermediateStorage

R = TypeVar('R')

@dataclass
class DAGTaskNodeId:
    value: str

class DAGTaskNode(Generic[R]):
    def __init__(self, func: Callable[..., R], args: tuple, kwargs: dict):
        self.task_id = f"{func.__name__}-{str(uuid.uuid4())[:4]}"
        self.func_name = func.__name__
        self.func_code = func
        self.func_args = args
        self.func_kwargs = kwargs
        self.downstream_nodes: list[str] = []
        self.upstream_nodes: list[str] = []
        self._register_dependencies()
        
    def _register_dependencies(self):
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                self.upstream_nodes.append(arg.task_id)

        for _, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                self.upstream_nodes.append(value.task_id)

        """Register this task as a downstream task of all its dependencies."""
        # Check args for DAGNode instances
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                arg.downstream_nodes.append(self.task_id)
                
        # Check kwargs for DAGNode instances
        for _, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                value.downstream_nodes.append(self.task_id)

    def is_ready(self):
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNodeId) and not IntermediateStorage.exists(arg.value):
                return False
        for _, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNodeId) and not IntermediateStorage.exists(value.value):
                return False
        return True

    def execute(self):
        final_func_args = []
        final_func_kwargs = {}

        if not self.is_ready(): raise Exception(f"Task {self.task_id} is not ready to be executed!")

        for arg in self.func_args:
            if isinstance(arg, DAGTaskNodeId):
                task_output = IntermediateStorage.get(arg.value)
                if task_output is None: raise Exception(f"[BUG] Task {arg.value}'s data is not available, despite is_ready()")
                task_output = cloudpickle.loads(task_output) # type: ignore
                final_func_args.append(task_output)
            else:
                final_func_args.append(arg)

        for key, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNodeId):
                task_output = IntermediateStorage.get(value.value)
                if task_output is None: raise Exception(f"[BUG] Task {value.value}'s data is not available, despite is_ready()")
                task_output = cloudpickle.loads(task_output) # type: ignore
                final_func_kwargs[key] = task_output
            else:
                final_func_kwargs[key] = value

        print(f"Executing task {self.task_id} with args {final_func_args} and kwargs {final_func_kwargs}")

        result = self.func_code(*tuple(final_func_args), **final_func_kwargs)
        upload_result = IntermediateStorage.set(self.task_id, cloudpickle.dumps(result))
        if not upload_result: raise Exception(f"[BUG] Task {self.task_id}'s data could not be uploaded to Redis (set() failed)")

    def __repr__(self):
        return f"DAGTaskNode({self.func_name}, id={self.task_id})"


def DAGTask(func: Callable[..., R]) -> Callable[..., DAGTaskNode[R]]:
    """Decorator to convert a function into a DAG node task."""
    @wraps(func)
    def wrapper(*args, **kwargs) -> DAGTaskNode[R]:
        return DAGTaskNode[R](func, args, kwargs)
    return wrapper