from dataclasses import dataclass
from functools import wraps
from typing import Callable, Generic, TypeVar
import uuid

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
        self.downstream_nodes: list['DAGTaskNode'] = []
        self._register_dependencies()
        
    def _register_dependencies(self):
        """Register this task as a downstream task of all its dependencies."""
        # Check args for DAGNode instances
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                arg.downstream_nodes.append(self)
                
        # Check kwargs for DAGNode instances
        for _, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                value.downstream_nodes.append(self)

    def __repr__(self):
        return f"DAGTaskNode({self.func_name}, id={self.task_id})"


def DAGTask(func: Callable[..., R]) -> Callable[..., DAGTaskNode[R]]:
    """Decorator to convert a function into a DAG node task."""
    @wraps(func)
    def wrapper(*args, **kwargs) -> DAGTaskNode[R]:
        return DAGTaskNode[R](func, args, kwargs)
    return wrapper