import copy
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Generic, TypeVar
import uuid

import cloudpickle

import src.intermediate_storage as intermediate_storage
import src.dag_task_node as dag_task_node

R = TypeVar('R')

@dataclass
class DAGTaskNodeId:
    function_name: str
    task_id: str
    dag_id: str | None

    def __init__(self, function_name: str, task_id: str | None = None, dag_id=None):
        self.function_name = function_name
        self.task_id = task_id or str(uuid.uuid4())[:3]
        self.dag_id = dag_id

    def get_full_id(self) -> str: 
        dag_id_str = "" if self.dag_id is None else f"-{self.dag_id}"
        return f"{self.function_name}{dag_id_str}-{self.task_id}"

class DAGTaskNode(Generic[R]):
    def __init__(self, func: Callable[..., R], args: tuple, kwargs: dict):
        self.id: DAGTaskNodeId = DAGTaskNodeId(func.__name__, task_id=None, dag_id=None)
        self.func_name = func.__name__
        self.func_code = func
        self.func_args = args
        self.func_kwargs = kwargs
        self.downstream_nodes: list[DAGTaskNode] = []
        self.upstream_nodes: list[DAGTaskNode] = []
        self._register_dependencies()
        
    def _register_dependencies(self):
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                self.upstream_nodes.append(arg)
                arg.downstream_nodes.append(self)

        for _, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                self.upstream_nodes.append(value)
                value.downstream_nodes.append(self)

    def visualize_dag(self, open_after: bool = True):
        import src.dag as dag
        dag.DAG.visualize(sink_node=self, open_after=open_after)

    def clone(self, cloned_nodes: dict[str, "DAGTaskNode"] | None = None) -> "DAGTaskNode":
        if cloned_nodes is None:
            cloned_nodes = {}

        # If this node has already been cloned, return the cloned version
        if self.id.task_id in cloned_nodes:
            return cloned_nodes[self.id.task_id]

        cloned_node = copy.deepcopy(self) # needs to be deepcopy
        cloned_nodes[self.id.task_id] = cloned_node

        # Clone the upstream and downstream nodes
        cloned_node.upstream_nodes = [node.clone(cloned_nodes) for node in self.upstream_nodes]
        cloned_node.downstream_nodes = [node.clone(cloned_nodes) for node in self.downstream_nodes]

        # Clone the arguments and keyword arguments
        cloned_node.func_args = tuple(
            arg.clone(cloned_nodes) if isinstance(arg, DAGTaskNode) else arg
            for arg in self.func_args
        )
        cloned_node.func_kwargs = {
            key: value.clone(cloned_nodes) if isinstance(value, DAGTaskNode) else value
            for key, value in self.func_kwargs.items()
        }

        return cloned_node

    def compute(self, local=False) -> R:
        import src.dag as dag
        dag_representation = dag.DAG(sink_node=self)
        res = None
        if local: 
            res = dag_representation.start_local_execution() # type: ignore
        else: 
            res = dag_representation.start_remote_execution() # type: ignore
        return res

    def invoke(self, dependencies: dict[str, Any]):
        final_func_args = []
        final_func_kwargs = {}

        for arg in self.func_args:
            if isinstance(arg, DAGTaskNodeId):
                if arg.get_full_id() not in dependencies: raise Exception(f"[BUG] Output of {arg.get_full_id()} not in dependencies")
                final_func_args.append(dependencies[arg.get_full_id()])
            else:
                final_func_args.append(arg)

        for key, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNodeId):
                if value.get_full_id() not in dependencies: raise Exception(f"[BUG] Output of {value.get_full_id()} not in dependencies")
                final_func_kwargs[key] = dependencies[value.get_full_id()]
            else:
                final_func_kwargs[key] = value

        # print(f"Executing task {self.task_id} with args {final_func_args} and kwargs {final_func_kwargs}")

        return self.func_code(*tuple(final_func_args), **final_func_kwargs)

    def _try_convert_node_func_args_to_ids(self):
        """
        Convert all DAGTaskNode references in {func_args} and {func_kwargs} to DAGTaskNodeId to save space, as they are stored in {upstream_nodes} and {downstream_nodes}
        """
        # Convert func_args
        new_args = []
        for arg in self.func_args:
            if isinstance(arg, dag_task_node.DAGTaskNode):
                new_args.append(arg.id)
            else:
                new_args.append(arg)
        
        # Convert func_kwargs
        new_kwargs = {}
        for key, value in self.func_kwargs.items():
            if isinstance(value, dag_task_node.DAGTaskNode):
                new_kwargs[key] = value.id
            else:
                new_kwargs[key] = value

        self.func_args = tuple(new_args)
        self.func_kwargs = new_kwargs

    def __repr__(self):
        return f"DAGTaskNode({self.func_name}, id={self.id})"


def DAGTask(func: Callable[..., R]) -> Callable[..., DAGTaskNode[R]]:
    """Decorator to convert a function into a DAG node task."""
    @wraps(func)
    def wrapper(*args, **kwargs) -> DAGTaskNode[R]:
        return DAGTaskNode[R](func, args, kwargs)
    return wrapper