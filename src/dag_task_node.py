from abc import ABC
import ast
import asyncio
import copy
from dataclasses import dataclass
from functools import wraps
import inspect
import subprocess
import sys
import time
from typing import Any, Callable, Generic, Type, TypeAlias, TypeVar, Union, get_args, get_origin
import uuid

from src.dag_task_annotation import TaskAnnotation
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.utils.timer import Timer

R = TypeVar('R')
S = TypeVar('S')
from src.utils.logger import create_logger

logger = create_logger(__name__)


@dataclass
class DAGTaskNodeId:
    function_name: str
    task_id: str

    def __init__(self, function_name: str, task_id: str | None = None):
        self.function_name = function_name
        self.task_id = task_id or str(uuid.uuid4())

    def get_full_id(self) -> str:
        return f"{self.function_name}-{self.task_id}"
    
    # can't be typed because of circular import error.......
    def get_full_id_in_dag(self, dag: Any) -> str:
        return f"{self.function_name}-{self.task_id}_{dag.master_dag_id}"

# Needed to distinguish a result=None (if R allows it) from "NO result yet"
# @dataclass
# class _CachedResultWrapper(Generic[R]):
#     result: R

class DAGTaskNode(Generic[R]):
    def __init__(self, func: Callable[..., R], args: tuple, kwargs: dict):
        self.id: DAGTaskNodeId = DAGTaskNodeId(func.__name__)
        self.func_name = func.__name__
        self.func_code = func
        self.func_args = args
        self.func_kwargs = kwargs
        self.downstream_nodes: list[DAGTaskNode] = []
        self.upstream_nodes: list[DAGTaskNode] = []
        # Initialized with a dummy worker config annotation for local worker
        self.annotations: list[TaskAnnotation] = [TaskWorkerResourceConfiguration(-1, -1)]
        # self.cached_result: _CachedResultWrapper[R] | None = None
        self._register_dependencies()
        self.third_party_libs: set[str] = self._find_third_party_libraries(exlude_libs=set(["src", "tests"]))

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

    def _find_third_party_libraries(self, exlude_libs: set[str] = set()) -> set[str]:
        func_file_path = inspect.getfile(self.func_code)
        with open(func_file_path, "r") as file:
            tree = ast.parse(file.read(), filename=func_file_path)

        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names: 
                    imports.add(alias.name.split('.')[0])  # Only take top-level package
            elif isinstance(node, ast.ImportFrom):
                module = node.module
                if module: 
                    imports.add(module.split('.')[0])  # Only take top-level package

        return {
            module for module in imports
            if module not in exlude_libs and not (hasattr(sys, "stdlib_module_names") and module in sys.stdlib_module_names)
        }

    """ config: worker.Worker.Config """
    def compute(self, config, open_dashboard: bool = False) -> R:
        import src.dag.dag as dag
        from src.worker import Worker
        _config: Worker.Config = config
        timer = Timer()
        dag_representation = dag.FullDAG(sink_node=self)
        logger.info(f"Created DAG with {len(dag_representation._all_nodes)} nodes in {timer.stop():.3f} ms")
        return asyncio.run(dag_representation.compute(_config, open_dashboard))

    async def compute_async(self, config, open_dashboard: bool = False) -> R:
        import src.dag.dag as dag
        from src.worker import Worker
        _config: Worker.Config = config
        timer = Timer()
        dag_representation = dag.FullDAG(sink_node=self)
        logger.info(f"Created DAG with {len(dag_representation._all_nodes)} nodes in {timer.stop():.3f} ms")
        res = await dag_representation.compute(_config, open_dashboard)
        return res

    def visualize_dag(self, output_file="dag_graph.png", open_after: bool = True):
        import src.dag.dag as dag
        dag.FullDAG.visualize(sink_node=self, output_file=output_file, open_after=open_after)

    def clone(self, cloned_nodes: dict[str, "DAGTaskNode"] | None = None) -> "DAGTaskNode":
        # _clone_start_time = time.time()
        if cloned_nodes is None:
            cloned_nodes = {}

        # If this node has already been cloned, return the cloned version
        if self.id.task_id in cloned_nodes:
            return cloned_nodes[self.id.task_id]

        cloned_node = copy.copy(self)
        cloned_nodes[self.id.task_id] = cloned_node

        # Clone the upstream and downstream nodes
        cloned_node.annotations = copy.copy(self.annotations)
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
        # self.cached_result = _CachedResultWrapper(res)
        return res

    T = TypeVar('T', bound='TaskAnnotation')

    def add_annotation(self, annotation: TaskAnnotation):
        for existing in self.annotations:
            if type(existing) is type(annotation): 
                self.annotations.remove(existing) # replace annotation
                break
        
        self.annotations.append(annotation)
        return True

    def try_get_annotation(self, annotation_type: Type[T]) -> T | None:
        for annotation in self.annotations:
            if isinstance(annotation, annotation_type):
                return annotation
        return None
    
    def get_annotation(self, annotation_type: Type[T]) -> T:
        for annotation in self.annotations:
            if isinstance(annotation, annotation_type):
                return annotation
        raise ValueError(f"Mandatory annotation of type {annotation_type} not found on task {self.id}")

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
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_modules)
            except subprocess.CalledProcessError as e:
                logger.warning(f"Failed to install {', '.join(missing_modules)}: {e}")
            
            for module in missing_modules:
                try:
                    __import__(module)
                    # print(f"({module}) successfully installed")
                except ImportError:
                    logger.error(f"Warning: Failed to import {module} after installation")

def DAGTask(func: Callable[..., R]) -> Callable[..., DAGTaskNode[R]]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> DAGTaskNode[R]:
        return DAGTaskNode[R](func, args, kwargs)
    return wrapper