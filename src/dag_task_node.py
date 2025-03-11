from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Generic, TypeVar
import uuid

import src.dag_task_node as dag_task_node

R = TypeVar('R')

@dataclass
class DAGTaskNodeId:
    value: str

class DAGTaskNode(Generic[R]):
    def __init__(self, func: Callable[..., R], args: tuple, kwargs: dict):
        self.task_id = f"{func.__name__}-{str(uuid.uuid4())[:3]}"
        self.func_name = func.__name__
        self.func_code = func
        self.func_args = args
        self.func_kwargs = kwargs
        self.downstream_nodes: list[DAGTaskNode] = []
        self.upstream_nodes: list[DAGTaskNode] = []
        self._register_dependencies()
        self._convert_node_func_args_to_ids()
        
    def _register_dependencies(self):
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                self.upstream_nodes.append(arg)

        for _, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                self.upstream_nodes.append(value)

        """Register this task as a downstream task of all its dependencies."""
        # Check args for DAGNode instances
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                arg.downstream_nodes.append(self)
                
        # Check kwargs for DAGNode instances
        for _, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                value.downstream_nodes.append(self)

    def execute(self, dependencies: dict[str, Any]):
        final_func_args = []
        final_func_kwargs = {}

        for arg in self.func_args:
            if isinstance(arg, DAGTaskNodeId):
                if arg.value not in dependencies: raise Exception(f"[BUG] Output of {arg.value} not in dependencies")
                final_func_args.append(dependencies[arg.value])
            else:
                final_func_args.append(arg)

        for key, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNodeId):
                if value.value not in dependencies: raise Exception(f"[BUG] Output of {value.value} not in dependencies")
                final_func_kwargs[key] = dependencies[value.value]
            else:
                final_func_kwargs[key] = value

        # print(f"Executing task {self.task_id} with args {final_func_args} and kwargs {final_func_kwargs}")

        return self.func_code(*tuple(final_func_args), **final_func_kwargs)

    def _convert_node_func_args_to_ids(self):
        """
        Convert all DAGTaskNode references in {func_args} and {func_kwargs} to DAGTaskNodeId to save space, as they are stored in {upstream_nodes} and {downstream_nodes}
        """
        # Convert func_args
        new_args = []
        for arg in self.func_args:
            if isinstance(arg, dag_task_node.DAGTaskNode):
                new_args.append(dag_task_node.DAGTaskNodeId(arg.task_id))
            else:
                new_args.append(arg)
        
        # Convert func_kwargs
        new_kwargs = {}
        for key, value in self.func_kwargs.items():
            if isinstance(value, dag_task_node.DAGTaskNode):
                new_kwargs[key] = dag_task_node.DAGTaskNodeId(value.task_id)
            else:
                new_kwargs[key] = value

        self.func_args = tuple(new_args)
        self.func_kwargs = new_kwargs

    def __repr__(self):
        return f"DAGTaskNode({self.func_name}, id={self.task_id})"


def DAGTask(func: Callable[..., R]) -> Callable[..., DAGTaskNode[R]]:
    """Decorator to convert a function into a DAG node task."""
    @wraps(func)
    def wrapper(*args, **kwargs) -> DAGTaskNode[R]:
        return DAGTaskNode[R](func, args, kwargs)
    return wrapper