from dataclasses import dataclass
from functools import wraps
import uuid
from graphviz import Digraph
from typing import Callable, Generic, Optional, TypeVar
import cloudpickle

R = TypeVar("R")

@dataclass
class TaskNodeInfo(Generic[R]):
    task_id: str
    func_name: str
    func_code: Callable[..., R]  # cloudpickle serialized
    func_args: tuple  # cloudpickle serialized
    func_kwargs: dict  # cloudpickle serialized
    computed: bool
    result: R | None

class TaskNode(Generic[R]):
    def __init__(self, func: Callable[..., R], args: tuple | None = None, kwargs: dict | None = None):
        self.info = TaskNodeInfo[R](
            task_id=str(uuid.uuid4())[:8],
            func_name=func.__name__,
            func_code=func,
            func_args=args or (),
            func_kwargs=kwargs or {},
            computed=False,
            result=None
        )
        print("TaskNode.__init__()")
    
    def serialize(self) -> bytes:
        return cloudpickle.dumps(self.info)

    @classmethod
    def deserialize(cls, serializedInfo: bytes):
        instance = cls.__new__(cls)
        cls.info = cloudpickle.loads(serializedInfo)
        return instance
    
    def compute(self) -> R:
        # If already computed, return cached result
        if self._computed:
            return self.info.result
            
        # Compute all dependencies first
        resolved_args = [arg.compute() if isinstance(arg, TaskNode) else arg for arg in self.info.func_args]
        resolved_kwargs = {key: val.compute() if isinstance(val, TaskNode) else val for key, val in self.info.func_kwargs.items()}
                
        # Compute this node
        self._result = self.info.func_code(*resolved_args, **resolved_kwargs)
        self._computed = True
        return self.info.result
    
    def dag_json(self, visited=None):
        if visited is None:
            visited = set()
            
        if self.task_id in visited:
            return { "task_id": self.task_id }
            
        visited.add(self.task_id)
        
        # Process args to find dependencies
        dag_args = [arg.dag_json(visited) if isinstance(arg, TaskNode) else str(arg) for arg in self.func_args]
        dag_kwargs = {key: val.dag_json(visited) if isinstance(val, TaskNode) else str(val) for key, val in self.func_kwargs.items()}
        
        # Create node representation
        node = {
            "task_id": self.task_id,
            "function": self.func_name,
            "args": dag_args,
            "kwargs": dag_kwargs
        }
        
        return node
    
    def dag_visualize(self, filename='dag_output', openImageAfter=True):
        graph = Digraph(format='png')
        visited = set()

        def build_graph(node: TaskNode):
            if node.task_id in visited: return
            visited.add(node.task_id)
            
            graph.node(node.task_id, label=f"{node.func_name}()")
            
            for arg in node.func_args:
                if isinstance(arg, TaskNode):
                    graph.edge(arg.task_id, node.task_id)
                    build_graph(arg)
            
            for key, val in node.func_kwargs.items():
                if isinstance(val, TaskNode):
                    graph.edge(val.task_id, node.task_id, label=key)
                    build_graph(val)
        
        build_graph(self)
        graph.render(filename, view=openImageAfter)

# Task Decorator
def Task(func: Callable[..., R]) -> Callable[..., TaskNode[R]]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> TaskNode[R]:
        return TaskNode[R](func, args, kwargs)
    return wrapper