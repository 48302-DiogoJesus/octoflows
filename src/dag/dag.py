from abc import ABC
import asyncio
import hashlib
import time
import uuid
import graphviz

from src.dag.dag_errors import NoRootNodesError, MultipleSinkNodesError
from src.utils.logger import create_logger
import src.dag_task_node as dag_task_node
import src.visualization.vis as vis
from src.utils.utils import calculate_data_structure_size
from src.utils.timer import Timer
from src.planning.abstract_dag_planner import AbstractDAGPlanner

logger = create_logger(__name__)

class GenericDAG(ABC):
    _all_nodes: dict[str, dag_task_node.DAGTaskNode]
    root_nodes: list[dag_task_node.DAGTaskNode]
    sink_node: dag_task_node.DAGTaskNode
    master_dag_structure_hash: str
    master_dag_id: str
    dag_name: str

    def get_node_by_id(self, node_id: dag_task_node.DAGTaskNodeId) -> dag_task_node.DAGTaskNode: 
        return self._all_nodes[node_id.get_full_id()]
    
    def create_subdag(self, root_node: dag_task_node.DAGTaskNode) -> "SubDAG":
        return SubDAG(master_dag_id=self.master_dag_id, master_dag_structure_hash=self.master_dag_structure_hash, root_node=root_node)

class SubDAG(GenericDAG):
    def __init__(self, master_dag_id: str, master_dag_structure_hash: str, root_node: dag_task_node.DAGTaskNode):
        self.root_nodes = [root_node]
        self.root_node = root_node
        self._all_nodes, self.sink_node = self._find_all_nodes_from_root(self.root_node)
        self.master_dag_structure_hash = master_dag_structure_hash
        self.master_dag_id = master_dag_id

    @staticmethod
    def _find_all_nodes_from_root(root_node: dag_task_node.DAGTaskNode) -> tuple[dict[str, dag_task_node.DAGTaskNode], dag_task_node.DAGTaskNode]:
        """Build the complete graph by traversing from root nodes downward and find the sink node."""
        all_nodes: dict[str, dag_task_node.DAGTaskNode] = {}
        sink_node = None  # Will store the last visited node with no downstreams
        
        def visit(node: dag_task_node.DAGTaskNode):
            nonlocal sink_node
            if node.id.get_full_id() in all_nodes:
                return
            all_nodes[node.id.get_full_id()] = node
            
            if not node.downstream_nodes:
                sink_node = node
            
            for arg in node.downstream_nodes:
                visit(arg)
        
        visit(root_node)

        return all_nodes, sink_node # type: ignore


class FullDAG(GenericDAG):
    def __init__(self, sink_node: dag_task_node.DAGTaskNode):
        self.sink_node = sink_node.clone() # clone all nodes behind the sink node
        self._all_nodes, self.root_nodes = self._find_all_nodes_and_root_nodes_from_sink(self.sink_node)
        if len(self.root_nodes) == 0: raise NoRootNodesError(self.sink_node.id.get_full_id())
        self._check_for_fake_sink_nodes_references(self._all_nodes, self.sink_node)
        self._optimize_task_metadata()
        
        self.master_dag_structure_hash = self.get_structure_hash()
        self.master_dag_id = f"{(time.time() * 1000):.0f}.{sink_node.func_name}.{str(uuid.uuid4())}.{self.master_dag_structure_hash}" # type: ignore

    async def compute(self, config, dag_name: str, open_dashboard: bool = False):
        from src.workers.worker import Worker
        from src.workers.local_worker import LocalWorker
        from src.storage.in_memory_storage import InMemoryStorage
        from src.planning.predictions.predictions_provider import PredictionsProvider
        from src.storage.metrics.metrics_types import UserDAGSubmissionMetrics
        
        _wk_config: Worker.Config = config
        wk: Worker = _wk_config.create_instance()
        self.dag_name = dag_name

        if wk.planner:
            if not wk.metrics_storage: raise Exception("You specified a Planner but not MetricsStorage!")
            predictions_provider = PredictionsProvider(len(self._all_nodes), self.master_dag_structure_hash, wk.metrics_storage)
            planner_name = wk.planner.__class__.__name__
            await predictions_provider.load_metrics_from_storage(planner_name=planner_name)
            logger.info(f"[PLANNING] Planner Selected: {planner_name}")
            plan_result = wk.planner.plan(self, predictions_provider)
            if plan_result:
                wk.metrics_storage.store_plan(self.master_dag_id, plan_result)

        if not isinstance(wk, LocalWorker):
            # ! Need to STORE after PLANNING because after the full dag is stored on redis, all workers will use that!
            _ = await Worker.store_full_dag(wk.metadata_storage, self)

        if open_dashboard:
            if isinstance(_wk_config.intermediate_storage_config, InMemoryStorage.Config):
                raise Exception("Can't use dashboard when using in-memory storage!")
            vis.DAGVisualizationDashboard.start(self, _wk_config)
        
        _start_time = Timer()
        logger.info(f"Invoking {len(self.root_nodes)} initial workers...")
        asyncio.create_task(wk.delegate([self.create_subdag(root_node) for root_node in self.root_nodes], called_by_worker=False), name="delegate_initial_workers")
        if wk.metrics_storage: wk.metrics_storage.store_dag_submission_time(self.master_dag_id, UserDAGSubmissionMetrics(time.time() * 1000))

        logger.info(f"Awaiting result of: {self.sink_node.id.get_full_id_in_dag(self)}")
        res = await Worker.wait_for_result_of_task(
            wk.metadata_storage,
            wk.intermediate_storage,
            self.sink_node,
            self
        )
        logger.info(f"Final Result Ready: ({self.sink_node.id.get_full_id_in_dag(self)}) => Size: {calculate_data_structure_size(res)} | Type: ({type(res)}) | Time: {_start_time.stop()} ms")

        if wk.metrics_storage: await wk.metrics_storage.flush()

        return res

    def get_structure_hash(self) -> str:
        """ Create a hash from the DAG structure (only including function_names and their relationships) """
        hasher = hashlib.sha256()
        
        for node in sorted(self._all_nodes.values(), key=lambda n: n.func_name):
            hasher.update(node.func_name.encode())
            for downstream in sorted(node.downstream_nodes, key=lambda n: n.func_name):
                hasher.update(downstream.func_name.encode())
                
        return hasher.hexdigest()

    def _optimize_task_metadata(self):
        ''' Reduce the {DAGTaskNode} by just their IDs to serve as placeholders for the future data '''
        for _, node in self._all_nodes.items(): # Use list() to create a copy to allow mutations while iterating
            # Optimize memory by replacing {DAGTaskNode} instances with their IDs (Note: Needs to be done after ALL IDs are replaced)
            # Convert func_args
            optimized_args = []
            for arg in node.func_args:
                if isinstance(arg, dag_task_node.DAGTaskNode):
                    # previous for loop adds the master dag id, we need that update
                    optimized_args.append(arg.id)
                elif isinstance(arg, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in arg):
                    optimized_args.append([item.id for item in arg])
                else:
                    optimized_args.append(arg)
            
            # Convert func_kwargs
            optimized_kwargs = {}
            for key, value in node.func_kwargs.items():
                if isinstance(value, dag_task_node.DAGTaskNode):
                    optimized_kwargs[key] = value.id
                elif isinstance(value, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in value):
                    optimized_kwargs[key] = [item.id for item in value]
                else:
                    optimized_kwargs[key] = value

            node.func_args = tuple(optimized_args)
            node.func_kwargs = optimized_kwargs

    @staticmethod
    def _find_all_nodes_and_root_nodes_from_sink(sink_node: dag_task_node.DAGTaskNode) -> tuple[dict[str, dag_task_node.DAGTaskNode], list[dag_task_node.DAGTaskNode]]:
        """
        Traverse the DAG from the sink node and return:
        1. A dictionary of all nodes in the DAG.
        2. A list of root nodes (nodes with no upstream dependencies).
        """
        all_nodes: dict[str, dag_task_node.DAGTaskNode] = {}
        root_nodes: list[dag_task_node.DAGTaskNode] = []

        def visit(node: dag_task_node.DAGTaskNode):
            # Skip if the node has already been visited
            if node.id.get_full_id() in all_nodes:
                return
            
            # Add the node to all_nodes
            all_nodes[node.id.get_full_id()] = node
            
            # If the node has no upstream nodes, it's a root node
            if not node.upstream_nodes:
                root_nodes.append(node)
            
            # Recursively visit all upstream nodes
            for upstream_node in node.upstream_nodes:
                visit(upstream_node)

        # Start traversal from the sink node
        visit(sink_node)
        
        return all_nodes, root_nodes

    @staticmethod
    def _check_for_fake_sink_nodes_references(all_nodes: dict[str, dag_task_node.DAGTaskNode], real_sink_node: dag_task_node.DAGTaskNode):
        # _find_all_nodes_and_root_nodes_from_sink() should NOT find sink nodes, but some valid nodes may point (DOWNESTREAM ONLY) to the fake sink nodes (no downstream nodes)
        def recursive_find_invalid_sink_nodes(node: dag_task_node.DAGTaskNode, visited: set[str] | None = None):
            if visited is None:
                visited = set()
            
            node_id = node.id.get_full_id()
            if node_id in visited: return
            visited.add(node_id)
            
            if not node.downstream_nodes and node_id != real_sink_node.id.get_full_id():
                raise MultipleSinkNodesError(node.func_name)
            
            # Recursively check downstream nodes
            for downstream_node in node.downstream_nodes:
                recursive_find_invalid_sink_nodes(downstream_node, visited)
    
        for node in all_nodes.values():
            recursive_find_invalid_sink_nodes(node)
   
    @classmethod
    def visualize(cls, sink_node: dag_task_node.DAGTaskNode, output_file="dag_graph.png", open_after=True):
        # Create a new directed graph
        dot = graphviz.Digraph(
            comment="DAG Visualization",
            format="png",
            engine="dot"  # Use dot layout for directed graphs
        )
        dot.attr(rankdir="LR")  # Layout from left to right
        
        all_nodes, root_nodes = cls._find_all_nodes_and_root_nodes_from_sink(sink_node)
        cls._check_for_fake_sink_nodes_references(all_nodes, sink_node)
        
        def print_argument(arg):
            if isinstance(arg, dag_task_node.DAGTaskNode) or isinstance(arg, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in arg):
                return "º"
            else:
                if isinstance(arg, str):
                    return f"hardcoded_str_len: {len(str(arg))}"
                elif isinstance(arg, int) or isinstance(arg, float):
                    return f"hardcoded_num: {arg}"
                elif isinstance(arg, bool):
                    return f"hardcoded_bool: {arg}"
                else:
                    return f"hardcoded_data_len: {len(str(arg))}"

        # Add nodes
        for node_id, node in all_nodes.items():
            dependency_strs = []
            for arg in node.func_args:
                if isinstance(arg, dag_task_node.DAGTaskNode):
                    # dependency_strs.append(str(arg.id.get_full_id()))
                    dependency_strs.append(print_argument(arg))
                elif isinstance(arg, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in arg):
                    # dependency_strs.append(str([item.id.get_full_id() for item in arg]))
                    dependency_strs.append(print_argument(arg))
                else:
                    dependency_strs.append(print_argument(arg))

            for key, value in node.func_kwargs.items():
                if isinstance(value, dag_task_node.DAGTaskNode):
                    # dependency_strs.append(f"{key}={value.id.get_full_id()}")
                    dependency_strs.append(f"{key}={print_argument(value)}")
                elif isinstance(value, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in value):
                    # dependency_strs.append(f"{key}={[item.id.get_full_id() for item in value]}")
                    dependency_strs.append(f"{key}={[print_argument(item) for item in value]}")
                else:
                    dependency_strs.append(f"{key}={print_argument(value)}")
            
            # Create the node label
            label = f"{node.id.get_full_id()}({', '.join(dependency_strs)})"
            
            # Determine node style based on whether it's a root or sink node
            node_style = "filled"
            node_color = "lightblue"

            # Add the node to the graph
            dot.node(node_id, label=label, shape="box", style=node_style, fillcolor=node_color)
        
        # Add edges
        for node_id, node in all_nodes.items():
            for downstream_node in node.downstream_nodes:
                dot.edge(node_id, downstream_node.id.get_full_id())
        
        # Render the graph to a file and open it
        dot.render(filename=output_file, cleanup=True, view=open_after)