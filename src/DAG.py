import asyncio
import time
from typing import Any
import uuid
import graphviz

from src.utils.logger import create_logger
import src.dag_task_node as dag_task_node
import src.visualization.vis as vis

logger = create_logger(__name__)

class DAG:
    def __init__(self, sink_node: dag_task_node.DAGTaskNode | None = None, master_dag_id: str | None = None, root_node: dag_task_node.DAGTaskNode | None = None):
        """Create a DAG from sink node (node with no downstream tasks)."""
        self.master_dag_id = master_dag_id or str(uuid.uuid4())
        # SUB-DAG (Stop searching for nodes at "fake" root nodes)
        if root_node:
            self.root_nodes = None
            self.root_node = root_node
            self._all_nodes, self.sink_node = DAG._find_all_nodes_from_root(self.root_node)
        # FULL DAG (Find real root nodes)
        else:
            if not sink_node: raise Exception("Sink node can't be None if no root nodes are provided!")
            self.sink_node = sink_node.clone() # clone all nodes behind the sink node
            self._all_nodes, self.root_nodes = DAG._find_all_nodes_and_root_node_from_sink(self.sink_node)
            if len(self.root_nodes) == 0: raise Exception(f"[BUG] DAG with sink node: {self.sink_node.id.get_full_id()} has 0 root notes!")
            self.root_node = self.root_nodes[0]
            DAG._check_for_fake_sink_nodes_references(self._all_nodes, self.sink_node)
            self._optimize_task_metadata()

    async def compute(self, config, open_dashboard: bool = False):
        from src.worker import Worker
        from src.storage.in_memory_storage import InMemoryStorage
        from src.resource_configuration import ResourceConfiguration
        _wk_config: Worker.Config = config
        wk: Worker = _wk_config.create_instance()
        if self.root_nodes is None: raise Exception("Expected complete DAG, but 'root_nodes == None'")
        Worker.store_full_dag(wk.metadata_storage, self)

        if open_dashboard:
            if isinstance(_wk_config.intermediate_storage_config, InMemoryStorage.Config):
                raise Exception("Can't use dashboard when using in-memory storage!")
            vis.DAGVisualizationDashboard.start(self, _wk_config)
        
        logger.info(f"Invoking {len(self.root_nodes)} initial workers...")
        for root_node in self.root_nodes:
            asyncio.create_task(wk.delegate(
                self.create_subdag(root_node),
                resource_configuration=ResourceConfiguration.small(),
                called_by_worker=False
            ))

        #! "await" is needed here
        logger.info(f"Awaiting result of: {self.sink_node.id.get_full_id_in_dag(self)}")
        res = await Worker.wait_for_result_of_task(
            wk.intermediate_storage, # type: ignore
            self.sink_node,
            self,
            polling_interval_s=0.1
        )

        if wk.metrics_storage_config: wk.metrics_storage_config.flush()
        return res
    
    def create_subdag(self, root_node: dag_task_node.DAGTaskNode) -> "DAG":
        return DAG(self.sink_node, master_dag_id=self.master_dag_id, root_node=root_node)

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

    def _find_sink_node_from_roots(self, root_nodes: list[dag_task_node.DAGTaskNode]):
        def dfs(node):
            if len(node.downstream_nodes) == 0:  # Sink node found
                return node
            for child in node.downstream_nodes:
                result = dfs(child)
                if result:
                    return result
            return None

        for root in root_nodes:
            sink = dfs(root)
            if sink:
                return sink

        raise Exception("Cloud not find sink node from root nodes")
    
    @staticmethod
    def _find_all_nodes_and_root_node_from_sink(sink_node: dag_task_node.DAGTaskNode) -> tuple[dict[str, dag_task_node.DAGTaskNode], list[dag_task_node.DAGTaskNode]]:
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
                raise Exception(f"[ClientError] Invalid DAG! There can only be one sink node. Found more that 1 node with 0 downstream tasks (function_name={node.func_name})!")
            
            # Recursively check downstream nodes
            for downstream_node in node.downstream_nodes:
                recursive_find_invalid_sink_nodes(downstream_node, visited)
    
        for node in all_nodes.values():
            recursive_find_invalid_sink_nodes(node)

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

    def get_node_by_id(self, node_id: dag_task_node.DAGTaskNodeId) -> dag_task_node.DAGTaskNode: 
        return self._all_nodes[node_id.get_full_id()]
    
    def _get_node_by_task_id(self, task_id: str) -> Any:
        for node in self._all_nodes.values():
            if node.id.task_id == task_id:
                return node
        return None
    
    @classmethod
    def visualize(cls, sink_node: dag_task_node.DAGTaskNode, output_file="dag_graph.png", open_after=True):
        # Create a new directed graph
        dot = graphviz.Digraph(
            comment="DAG Visualization",
            format="png",
            engine="dot"  # Use dot layout for directed graphs
        )
        dot.attr(rankdir="LR")  # Layout from left to right
        
        all_nodes, root_nodes = cls._find_all_nodes_and_root_node_from_sink(sink_node)
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

            if node.is_dynamic_fan_out_representative:
                node_color = "lightyellow"
            
            # Add the node to the graph
            dot.node(node_id, label=label, shape="box", style=node_style, fillcolor=node_color)
        
        # Add edges
        for node_id, node in all_nodes.items():
            for downstream_node in node.downstream_nodes:
                dot.edge(node_id, downstream_node.id.get_full_id())
        
        # Render the graph to a file and open it
        dot.render(filename=output_file, cleanup=True, view=open_after)