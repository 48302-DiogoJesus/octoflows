from functools import wraps
import uuid
from graphviz import Digraph
from typing import Callable, Generic, TypeVar

R = TypeVar("R")

class TaskNode(Generic[R]):
    def __init__(self, func: Callable[..., R], args: tuple | None = None, kwargs: dict | None = None):
        self.task_id = str(uuid.uuid4())[:8]
        self.func = func
        self.func_name = func.__name__
        self.args = args or []
        self.kwargs = kwargs or {}
        self._result: R
        self._computed = False
        
    def compute(self) -> R:
        # If already computed, return cached result
        if self._computed:
            return self._result
            
        # Compute all dependencies first
        resolved_args = [arg.compute() if isinstance(arg, TaskNode) else arg for arg in self.args]
        resolved_kwargs = {key: val.compute() if isinstance(val, TaskNode) else val for key, val in self.kwargs.items()}
                
        # Compute this node
        self._result = self.func(*resolved_args, **resolved_kwargs)
        self._computed = True
        return self._result
    
    def dag_json(self, visited=None):
        if visited is None:
            visited = set()
            
        if self.task_id in visited:
            return { "task_id": self.task_id }
            
        visited.add(self.task_id)
        
        # Process args to find dependencies
        dag_args = [arg.dag_json(visited) if isinstance(arg, TaskNode) else str(arg) for arg in self.args]
        dag_kwargs = {key: val.dag_json(visited) if isinstance(val, TaskNode) else str(val) for key, val in self.kwargs.items()}
        
        # Create node representation
        node = {
            "task_id": self.task_id,
            "function": self.func_name,
            "args": dag_args,
            "kwargs": dag_kwargs
        }
        
        return node
    
    def dag_visualize(self, filename='dag_output'):
        graph = Digraph(format='png')
        visited = set()

        def build_graph(node: TaskNode):
            if node.task_id in visited: return
            visited.add(node.task_id)
            
            graph.node(node.task_id, label=f"{node.func_name}()")
            
            for arg in node.args:
                if isinstance(arg, TaskNode):
                    graph.edge(arg.task_id, node.task_id)
                    build_graph(arg)
            
            for key, val in node.kwargs.items():
                if isinstance(val, TaskNode):
                    graph.edge(val.task_id, node.task_id, label=key)
                    build_graph(val)
        
        build_graph(self)
        graph.render(filename, view=True)  # Save and open the DAG

# Task Decorator
def task(func: Callable[..., R]) -> Callable[..., TaskNode[R]]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> TaskNode[R]:
        return TaskNode(func, args, kwargs)
    return wrapper
