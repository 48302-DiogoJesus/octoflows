import asyncio
import time
from typing import Any
import uuid
import cloudpickle
import graphviz

import src.intermediate_storage as intermediate_storage
import src.dag_task_node as dag_task_node
import src.executor as executor

class DAG:
    _FINAL_RESULT_POLLING_TIME_S = 0.2

    def __init__(self, sink_node: dag_task_node.DAGTaskNode, master_dag_id: str | None = None, root_nodes: list[dag_task_node.DAGTaskNode] | None = None):
        """Create a DAG from sink node (node with no downstream tasks)."""
        self.master_dag_id = master_dag_id or str(uuid.uuid4())[:4]
        # SUB-DAG (Stop searching for nodes at "fake" root nodes)
        if root_nodes:
            self.root_nodes: list[dag_task_node.DAGTaskNode] = root_nodes
            self.sink_node = self._find_sink_node_from_roots(self.root_nodes)#.clone() # subdag should already be iterating on clones and not the original decorated tasks
            self.all_nodes: dict[str, dag_task_node.DAGTaskNode] = DAG._find_all_nodes_from_sink(self.sink_node)
        # FULL DAG (Find real root nodes)
        else:
            self.sink_node = sink_node.clone()
            self.all_nodes: dict[str, dag_task_node.DAGTaskNode] = DAG._find_all_nodes_from_sink(self.sink_node)
            self.root_nodes: list[dag_task_node.DAGTaskNode] = DAG._find_root_nodes(self.all_nodes)
        # Find nodes by going backwards until root nodes
        # Add the DAG id to each task
        self._optimize_task_metadata()

        if len(self.root_nodes) == 0: raise Exception(f"[BUG] DAG with sink node: {sink_node.id.get_full_id()} has 0 root notes!")
        self.root_node = self.root_nodes[0]
    
    def create_subdag(self, root_node: dag_task_node.DAGTaskNode) -> "DAG":
        return DAG(self.sink_node, master_dag_id=self.master_dag_id, root_nodes=[root_node])

    # User interface must be synchronous
    def start_docker_execution(self):
        async def internal():
            for root_node in self.root_nodes:
                asyncio.create_task(executor.DockerExecutor(self.create_subdag(root_node), 'http://localhost:5000/').parallelize_self())
            res = await self._wait_for_final_result()
            return res
        return asyncio.run(internal())

    # User interface must be synchronous
    def start_local_execution(self):
        async def internal():
            leaf_executors: list[executor.LocalExecutor] = []
            
            for root_node in self.root_nodes:
                ex = executor.LocalExecutor(self.create_subdag(root_node))
                asyncio.create_task(ex.start_executing())
                leaf_executors.append(ex)
            
            res = await self._wait_for_final_result()
            for ex in leaf_executors: ex.shutdown_flag.set()
            return res
        return asyncio.run(internal())

    def get_dag_task_id(self, dag_task_node: dag_task_node.DAGTaskNode):
        return f"{dag_task_node.id.get_full_id()}-{self.master_dag_id}"

    async def _wait_for_final_result(self):
        # Asynchronously poll Storage for final result
        while True:
            final_result = intermediate_storage.IntermediateStorage.get(self.get_dag_task_id(self.sink_node))
            if final_result is not None:
                final_result = cloudpickle.loads(final_result) # type: ignore
                print(f"Final Result Ready: ({self.sink_node.id.get_full_id()}) => {final_result} | Type: ({type(final_result)})")
                return final_result
            await asyncio.sleep(self._FINAL_RESULT_POLLING_TIME_S)

    def _optimize_task_metadata(self):
        ''' Reduce the {DAGTaskNode} by just their IDs to serve as placeholders for the future data '''
        for _, node in list(self.all_nodes.items()): # Use list() to create a copy to allow mutations while iterating
            # Optimize memory by replacing {DAGTaskNode} instances with their IDs (Note: Needs to be done after ALL IDs are replaced)
            # Convert func_args
            new_args = []
            for arg in node.func_args:
                if isinstance(arg, dag_task_node.DAGTaskNode):
                    # previous for loop adds the master dag id, we need that update
                    new_args.append(node.id)
                elif isinstance(arg, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in arg):
                    new_args.append([item.id for item in arg])
                else:
                    new_args.append(arg)
            
            # Convert func_kwargs
            new_kwargs = {}
            for key, value in node.func_kwargs.items():
                if isinstance(value, dag_task_node.DAGTaskNode):
                    new_kwargs[key] = value.id
                elif isinstance(value, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in value):
                    new_kwargs[key] = [item.id for item in value]
                else:
                    new_kwargs[key] = value

            node.func_args = tuple(new_args)
            node.func_kwargs = new_kwargs

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
    
    @classmethod
    def _find_root_nodes(cls, all_nodes: dict[str, dag_task_node.DAGTaskNode]):
        """Identify root nodes (nodes with no upstream dependencies)."""
        rns = list(all_nodes.values())
        for node in all_nodes.values():
            for dependent_node in node.downstream_nodes:
                for rn in rns:
                    if rn.id.get_full_id() == dependent_node.id.get_full_id():
                        rns.remove(rn)
        return rns

    @classmethod
    def _find_all_nodes_from_sink(cls, sink_node: dag_task_node.DAGTaskNode) -> dict[str, dag_task_node.DAGTaskNode]:
        """Build the complete graph by traversing from sink node upward."""
        all_nodes: dict[str, dag_task_node.DAGTaskNode] = {}
        
        def visit(node: dag_task_node.DAGTaskNode):
            if node.id.get_full_id() in all_nodes: return
            all_nodes[node.id.get_full_id()] = node
            for arg in node.upstream_nodes: visit(arg)
        
        visit(sink_node)
        return all_nodes
    
    def get_node_by_id(self, node_id: dag_task_node.DAGTaskNodeId) -> dag_task_node.DAGTaskNode: 
        return self.all_nodes[node_id.get_full_id()]
    
    def _get_node_by_task_id(self, task_id: str) -> Any:
        for node in self.all_nodes.values():
            if node.id.task_id == task_id:
                return node
        return None
    
    @classmethod
    def visualize(cls, sink_node: dag_task_node.DAGTaskNode, output_file="dag_graph.png", highlight_roots=True, highlight_sink=True, open_after=True):
        # Create a new directed graph
        dot = graphviz.Digraph(
            comment="DAG Visualization",
            format="png",
            engine="dot"  # Use dot layout for directed graphs
        )
        dot.attr(rankdir="LR")  # Layout from left to right
        
        all_nodes = cls._find_all_nodes_from_sink(sink_node)
        root_nodes = cls._find_root_nodes(all_nodes)
        # Add nodes
        for node_id, node in all_nodes.items():
            # Create a label showing function name and args
            # for dependency in node.upstream_nodes:
            #     dependency_strs.append(str(dependency.task_id))
            
            dependency_strs = []
            for arg in node.func_args:
                if isinstance(arg, dag_task_node.DAGTaskNode):
                    # dependency_strs.append(str(arg.id.get_full_id()))
                    dependency_strs.append("_")
                elif isinstance(arg, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in arg):
                    # dependency_strs.append(str([item.id.get_full_id() for item in arg]))
                    dependency_strs.append(str(["_" for item in arg]))
                else:
                    dependency_strs.append(str(arg))

            for key, value in node.func_kwargs.items():
                if isinstance(value, dag_task_node.DAGTaskNode):
                    # dependency_strs.append(f"{key}={value.id.get_full_id()}")
                    dependency_strs.append(f"{key}=_")
                elif isinstance(value, list) and all(isinstance(item, dag_task_node.DAGTaskNode) for item in value):
                    # dependency_strs.append(f"{key}={[item.id.get_full_id() for item in value]}")
                    dependency_strs.append(f"{key}=[_ for item in value]")
                else:
                    dependency_strs.append(f"{key}={value}")
            
            # Create the node label
            label = f"{node.id.get_full_id()}({', '.join(dependency_strs)})"
            
            # Determine node style based on whether it's a root or sink node
            node_style = "filled"
            node_color = "lightblue"
            
            if highlight_roots and node in root_nodes:
                node_color = "lightgreen"  # Root nodes in green
            
            if highlight_sink and node.id.get_full_id() == sink_node.id.get_full_id():
                node_color = "lightcoral"  # Sink node in red
            
            # Add the node to the graph
            dot.node(node_id, label=label, shape="box", style=node_style, fillcolor=node_color)
        
        # Add edges
        for node_id, node in all_nodes.items():
            for downstream_node in node.downstream_nodes:
                dot.edge(node_id, downstream_node.id.get_full_id())
        
        # Render the graph to a file and open it
        dot.render(filename=output_file.split('.')[0], cleanup=True, view=open_after)
    
    def serialize(self):
        return cloudpickle.dumps(self)

    @classmethod
    def from_serialized(cls, serialized_dag: bytes):
        return cloudpickle.loads(serialized_dag)