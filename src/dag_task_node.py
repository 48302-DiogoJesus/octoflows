import ast
import asyncio
import copy
from dataclasses import dataclass
from functools import wraps
import inspect
import sys
from typing import Any, Callable, Type, TypeVar
import uuid

from src.task_optimization import TaskOptimization
from src.utils.atomic_flag import AtomicFlag
from src.utils.timer import Timer
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.utils.utils import calculate_data_structure_size_bytes

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
        return f"{self.function_name}+{self.task_id}"
    
    # can't be typed because of circular import error.......
    def get_full_id_in_dag(self, dag: Any) -> str:
        return f"{self.function_name}+{self.task_id}+{dag.master_dag_id}"

# Needed to distinguish a result=None (if R allows it) from "NO result yet"
@dataclass
class _CachedResultWrapper:
    result: Any

class DAGTaskNode:

    def __init__(self, func: Callable, args: tuple, kwargs: dict):
        from src.storage.metadata.metrics_types import TaskMetrics, TaskInputMetrics, TaskOutputMetrics
        from src.planning.abstract_dag_planner import AbstractDAGPlanner
        self.id: DAGTaskNodeId = DAGTaskNodeId(func.__name__)
        self.func_name = func.__name__
        # after the DAG is optimized, this will be set to None and the code will be stored in a map on the DAG structure
        self.func_code = func
        self.func_args = args
        self.worker_config = TaskWorkerResourceConfiguration(memory_mb=-1, worker_id=None)
        self.func_kwargs = kwargs
        self.downstream_nodes: list[DAGTaskNode] = []
        self.upstream_nodes: list[DAGTaskNode] = []
        self.metrics: TaskMetrics = TaskMetrics(
            worker_resource_configuration=self.worker_config,
            started_at_timestamp_s=0,
            input_metrics=TaskInputMetrics(input_download_metrics={}),
            tp_execution_time_ms=0,
            execution_time_per_input_byte_ms=None,
            update_dependency_counters_time_ms=None,
            output_metrics=TaskOutputMetrics(serialized_size_bytes=-1, tp_time_ms=None),
            total_invocations_count=0,
            total_invocation_time_ms=None,
            planner_used_name=None,
            optimization_metrics=[]
        )
        self.duppable_tasks_predictions: dict[str, AbstractDAGPlanner.DuppableTaskPrediction] = {}
        self.optimizations: list[TaskOptimization] = []
        # Initialized with a dummy worker config annotation for local worker
        #! Don't clone this on the clone() function to avoid sending large data on invocation to other workers
        self.cached_result: _CachedResultWrapper | None = None
        self.upload_complete: asyncio.Event = asyncio.Event()
        self.completed_event: asyncio.Event = asyncio.Event()
        self.is_handling: AtomicFlag = AtomicFlag() # used to prevent multiple invocations of the same task in the same worker
        self._register_dependencies()
        self.third_party_libs: set[str] = self._find_third_party_libraries(exlude_libs=set(["src", "tests", "_examples", "common"]))

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
        assert self.func_code

        func_file_path = inspect.getfile(self.func_code)
        with open(func_file_path, "r") as file:
            tree = ast.parse(file.read(), filename=func_file_path)

        top_level_imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names: 
                    top_level_imports.add(alias.name.split('.')[0])  # Only take top-level package
            elif isinstance(node, ast.ImportFrom):
                module = node.module
                if module:
                    top_level_imports.add(module.split('.')[0])  # Only take top-level package

        return {
            module for module in top_level_imports
            if module not in exlude_libs and not (hasattr(sys, "stdlib_module_names") and module in sys.stdlib_module_names)
        }

    """ config: worker.Worker.Config """
    def compute(self, config, dag_name: str = "unnamed-dag", download_result: bool = True, open_dashboard: bool = False) -> Any:
        import src.dag.dag as dag
        from src.workers.worker import Worker
        _config: Worker.Config = config
        timer = Timer()
        dag_representation = dag.FullDAG(sink_node=self)
        logger.info(f"Created DAG with {len(dag_representation._all_nodes)} nodes in {timer.stop():.3f} ms")
        return asyncio.run(dag_representation.compute(_config, dag_name, download_result, open_dashboard))

    async def compute_async(self, config, dag_name: str = "unnamed-dag", download_result: bool = True, open_dashboard: bool = False) -> Any:
        import src.dag.dag as dag
        from src.workers.worker import Worker
        _config: Worker.Config = config
        timer = Timer()
        dag_representation = dag.FullDAG(sink_node=self)
        logger.info(f"Created DAG with {len(dag_representation._all_nodes)} nodes in {timer.stop():.3f} ms")
        res = await dag_representation.compute(_config, dag_name, download_result, open_dashboard)
        return res

    def visualize_dag(self, output_file="dag_graph", open_after: bool = True):
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
        cloned_node.optimizations = copy.copy(self.optimizations)
        cloned_node.upstream_nodes = [node.clone(cloned_nodes) for node in self.upstream_nodes]
        cloned_node.downstream_nodes = [node.clone(cloned_nodes) for node in self.downstream_nodes]
        # cloned_node.cached_result = self.cached_result

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
        # self._try_install_third_party_libs()

        final_func_args = []
        final_func_kwargs = {}

        for arg in self.func_args:
            if isinstance(arg, DAGTaskNodeId):
                if arg.get_full_id() not in dependencies: raise Exception(f"[ERROR] Output of {arg.get_full_id()} not in dependencies")
                final_func_args.append(dependencies[arg.get_full_id()])
            elif isinstance(arg, list) and all(isinstance(item, DAGTaskNodeId) for item in arg):
                final_func_args.append([dependencies[item.get_full_id()] for item in arg])
            else:
                final_func_args.append(arg)

        for key, value in self.func_kwargs.items():
            if isinstance(value, DAGTaskNodeId):
                if value.get_full_id() not in dependencies: raise Exception(f"[ERROR] Output of {value.get_full_id()} not in dependencies")
                final_func_kwargs[key] = dependencies[value.get_full_id()]
            elif isinstance(value, list) and all(isinstance(item, DAGTaskNodeId) for item in value):
                final_func_kwargs[key] = [dependencies[item.get_full_id()] for item in value]
            else:
                final_func_kwargs[key] = value

        # print(f"Executing task {self.id.get_full_id()} with args {final_func_args} and kwargs {final_func_kwargs}")

        # res = function_code(*tuple(final_func_args), **final_func_kwargs)
        res = self.func_code(*tuple(final_func_args), **final_func_kwargs)
        self.cached_result = _CachedResultWrapper(res)
        self.completed_event.set()
        return res

    T = TypeVar('T', bound='TaskOptimization')

    def add_optimization(self, optimization: T) -> T:
        for existing in self.optimizations:
            if type(existing) is type(optimization): 
                self.optimizations.remove(existing) # replace annotation
                break
        
        self.optimizations.append(optimization)
        return optimization

    def remove_optimization(self, optimization_type: Type[T]):
        for optimization in self.optimizations:
            if isinstance(optimization, optimization_type):
                self.optimizations.remove(optimization)
    
    def try_get_optimization(self, optimization_type: Type[T]) -> T | None:
        for optimization in self.optimizations:
            if isinstance(optimization, optimization_type):
                return optimization
        return None
    
    def get_optimization(self, optimization_type: Type[T]) -> T:
        for optimization in self.optimizations:
            if isinstance(optimization, optimization_type):
                return optimization
        raise ValueError(f"Mandatory optimization of type {optimization_type} not found on task {self.id}")

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

        self.func_args = new_args
        self.func_kwargs = new_kwargs

    def __repr__(self):
        return f"DAGTaskNode({self.func_name}, id={self.id})"

    # def _try_install_third_party_libs(self):
    #     '''
    #     Commented out because correct way would be to gather dependencies of the entire workflow and installing them before deserializting code on the workers
    #     '''
    #     missing_modules = []
    #     for module in self.third_party_libs:
    #         try:
    #             __import__(module)
    #             # print(f"({module}) already installed")
    #         except ImportError:
    #             print(f"({module}) not found, will install")
    #             missing_modules.append(module)
        
    #     if missing_modules:
    #         logger.info(f"Installing missing modules: {', '.join(missing_modules)}")
    #         try:
    #             subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_modules)
    #         except subprocess.CalledProcessError as e:
    #             logger.warning(f"Failed to install {', '.join(missing_modules)}: {e}")
            
    #         for module in missing_modules:
    #             try:
    #                 __import__(module)
    #                 # print(f"({module}) successfully installed")
    #             except ImportError:
    #                 logger.error(f"Warning: Failed to import {module} after installation")
    # '''

def DAGTask(func_or_params=None, forced_optimizations: list[TaskOptimization] = []) -> Callable[..., DAGTaskNode]:
    def decorator(func: Callable[..., Any]) -> Callable[..., DAGTaskNode]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> DAGTaskNode:
            node = DAGTaskNode(func, args, kwargs)
            for annotation in forced_optimizations:  node.add_optimization(annotation)
            return node
        return wrapper
    
    # @DAGTaskFlexible(forced_optimizations={...})
    if func_or_params is None:
        return decorator # type: ignore
    # @DAGTaskFlexible
    elif callable(func_or_params):
        return decorator(func_or_params)
    else:
        raise ValueError("Invalid usage of 'DAGTask' decorator")