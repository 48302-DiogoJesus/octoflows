import ast
import copy
from dataclasses import dataclass
from functools import wraps
import inspect
import subprocess
import sys
import time
from typing import Any, Callable, Generic, TypeVar
import uuid

R = TypeVar('R')
from src.utils.logger import create_logger

logger = create_logger(__name__)

@dataclass
class DAGTaskNodeId:
    function_name: str
    task_id: str

    def __init__(self, function_name: str, task_id: str | None = None):
        self.function_name = function_name
        self.task_id = task_id or str(uuid.uuid4())[:4]

    def get_full_id(self) -> str: 
        return f"{self.function_name}-{self.task_id}"
    
    # can't be typed because or circular import error......
    def get_full_id_in_dag(self, dag: Any) -> str: 
        return f"{self.function_name}-{self.task_id}_{dag.master_dag_id}"

# Needed to distinguish a result=None (if R allows it) from NO result
@dataclass
class _CachedResultWrapper(Generic[R]):
    result: R

class DAGTaskNode(Generic[R]):
    def __init__(self, func: Callable[..., R], args: tuple, kwargs: dict):
        self.id: DAGTaskNodeId = DAGTaskNodeId(func.__name__, task_id=None)
        self.func_name = func.__name__
        self.func_code = func
        self.func_args = args
        self.func_kwargs = kwargs
        self.downstream_nodes: list[DAGTaskNode] = []
        self.upstream_nodes: list[DAGTaskNode] = []
        self.cached_result: _CachedResultWrapper[R] | None = None
        self._register_dependencies()
        self.third_party_libs: set[str] = self._find_third_party_libraries()
        
    def _register_dependencies(self):
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                self.upstream_nodes.append(arg)
                arg.downstream_nodes.append(self)
            elif isinstance(arg, list) and all(isinstance(item, DAGTaskNode) for item in arg):
                for item in arg:
                    self.upstream_nodes.append(item)
                    item.downstream_nodes.append(self)

        for _, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                self.upstream_nodes.append(value)
                value.downstream_nodes.append(self)
            elif isinstance(value, list) and all(isinstance(item, DAGTaskNode) for item in value):
                for item in value:
                    self.upstream_nodes.append(item)
                    item.downstream_nodes.append(self)

    def _find_third_party_libraries(self) -> set[str]:
        func_file_path = inspect.getfile(self.func_code)
        with open(func_file_path, "r") as file:
            tree = ast.parse(file.read(), filename=func_file_path)

        imports = set[str]()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names: imports.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module
                if module: imports.add(module)

        return {
            module for module in imports
            if not (hasattr(sys, "stdlib_module_names") and module in sys.stdlib_module_names)
        }

    """ config: worker.Worker.Config """
    def compute(self, config, open_dashboard: bool = False) -> R:
        import src.dag as dag
        from src.worker import Worker
        _config: Worker.Config = config
        _start_time = time.time()
        dag_representation = dag.DAG(sink_node=self)
        logger.info(f"Created DAG in {time.time() - _start_time:.4f} seconds")
        return dag_representation.compute(_config, open_dashboard)

    def visualize_dag(self, open_after: bool = True):
        import src.dag as dag
        dag.DAG.visualize(sink_node=self, open_after=open_after)

    def clone(self, cloned_nodes: dict[str, "DAGTaskNode"] | None = None) -> "DAGTaskNode":
        # _clone_start_time = time.time()
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
        cloned_node.func_args = []
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                cloned_node.func_args.append(arg.clone(cloned_nodes))
            elif isinstance(arg, list) and all(isinstance(item, DAGTaskNode) for item in arg):
                cloned_node.func_args.append([item.clone(cloned_nodes) for item in arg])
            else:
                cloned_node.func_args.append(arg)
        
        cloned_node.func_kwargs = {}
        for key, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                cloned_node.func_kwargs[key] = value.clone(cloned_nodes)
            elif isinstance(value, list) and all(isinstance(item, DAGTaskNode) for item in value):
                cloned_node.func_kwargs[key] = [item.clone(cloned_nodes) for item in value]
            else:
                cloned_node.func_kwargs[key] = value

        # _clone_end_time = time.time()
        # print(f"Cloned {self.func_name} in {(_clone_end_time - _clone_start_time):.4f} seconds")
        return cloned_node    

    def invoke(self, dependencies: dict[str, Any]):
        self._try_install_third_party_libs()

        final_func_args = []
        final_func_kwargs = {}

        for arg in self.func_args:
            if isinstance(arg, DAGTaskNodeId):
                if arg.get_full_id() not in dependencies: raise Exception(f"[BUG] Output of {arg.get_full_id()} not in dependencies")
                final_func_args.append(dependencies[arg.get_full_id()])
            elif isinstance(arg, list) and all(isinstance(item, DAGTaskNodeId) for item in arg):
                final_func_args.append([dependencies[item.get_full_id()] for item in arg])
            else:
                final_func_args.append(arg)

        for key, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNodeId):
                if value.get_full_id() not in dependencies: raise Exception(f"[BUG] Output of {value.get_full_id()} not in dependencies")
                final_func_kwargs[key] = dependencies[value.get_full_id()]
            elif isinstance(value, list) and all(isinstance(item, DAGTaskNodeId) for item in value):
                final_func_kwargs[key] = [dependencies[item.get_full_id()] for item in value]
            else:
                final_func_kwargs[key] = value

        # print(f"Executing task {self.id.get_full_id()} with args {final_func_args} and kwargs {final_func_kwargs}")

        res = self.func_code(*tuple(final_func_args), **final_func_kwargs)
        self.cached_result = _CachedResultWrapper(res)
        return res

    def _try_convert_node_func_args_to_ids(self):
        """
        Convert all DAGTaskNode references in {func_args} and {func_kwargs} to DAGTaskNodeId to save space, as they are stored in {upstream_nodes} and {downstream_nodes}
        """
        # Convert func_args
        new_args = []
        for arg in self.func_args:
            if isinstance(arg, DAGTaskNode):
                new_args.append(arg.id)
            elif isinstance(arg, list) and all(isinstance(item, DAGTaskNode) for item in arg):
                new_args.append([item.id for item in arg])
            else:
                new_args.append(arg)
        
        # Convert func_kwargs
        new_kwargs = {}
        for key, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNode):
                new_kwargs[key] = value.id
            elif isinstance(value, list) and all(isinstance(item, DAGTaskNode) for item in value):
                new_kwargs[key] = [item.id for item in value]
            else:
                new_kwargs[key] = value

        self.func_args = tuple(new_args)
        self.func_kwargs = new_kwargs

    def __repr__(self):
        return f"DAGTaskNode({self.func_name}, id={self.id})"

    def _try_install_third_party_libs(self):
        missing_modules = []
        for module in self.third_party_libs:
            try:
                __import__(module)
                # print(f"({module}) already installed")
            except ImportError:
                # print(f"({module}) not found, will install")
                missing_modules.append(module)
        
        if missing_modules:
            logger.info(f"Installing missing modules: {', '.join(missing_modules)}")
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_modules)
            
            for module in missing_modules:
                try:
                    __import__(module)
                    # print(f"({module}) successfully installed")
                except ImportError:
                    logger.error(f"Warning: Failed to import {module} after installation")

def DAGTask(func: Callable[..., R]) -> Callable[..., DAGTaskNode[R]]:
    """Decorator to convert a function into a DAG node task."""
    @wraps(func)
    def wrapper(*args, **kwargs) -> DAGTaskNode[R]:
        return DAGTaskNode[R](func, args, kwargs)
    return wrapper