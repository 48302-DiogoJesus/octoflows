import asyncio
import threading
import time
from typing import Callable
import uuid
import cloudpickle
import graphviz
import os
import subprocess
import platform
import requests

import src.intermediate_storage as intermediate_storage
import src.dag_task_node as dag_task_node
import src.executor as executor

class DAG:
    _FINAL_RESULT_POLLING_TIME_S = 0.1

    def __init__(self, sink_node: dag_task_node.DAGTaskNode, master_dag_id: str | None = None, root_nodes: list[dag_task_node.DAGTaskNode] | None = None):
        """Create a DAG from sink node (node with no downstream tasks)."""
        self.master_dag_id = master_dag_id or str(uuid.uuid4())[:4]
        self.sink_node = sink_node
        # SUB-DAG (Stop searching for nodes at "fake" root nodes)
        if root_nodes: 
            self.root_nodes: list[dag_task_node.DAGTaskNode] = root_nodes
            # Find nodes by going backwards until root nodes
            self.all_nodes: dict[str, dag_task_node.DAGTaskNode] = self._find_all_nodes_from_roots()
        # FULL DAG (Find real root nodes)
        else:
            # Find nodes by going backwards until root nodes
            self.all_nodes: dict[str, dag_task_node.DAGTaskNode] = self._find_all_nodes_from_sink()
            self.root_nodes: list[dag_task_node.DAGTaskNode] = self._identify_root_nodes()
    
    def create_subdag(self, root_nodes: list[dag_task_node.DAGTaskNode]) -> "DAG":
        return DAG(self.sink_node, master_dag_id=self.master_dag_id, root_nodes=root_nodes)

    # User interface must be synchronous
    def start_remote_execution(self, wait_for_final_result=False):
        # TODO: move out of here so that RemoteExecutor can use it as well
        def _invoke_remote_executor(subgraph: DAG):
            print(f"Invoking remote executor for subgraph with {len(subgraph.root_nodes)} root nodes | First Node: {subgraph.root_nodes[0].task_id}")
            response = requests.post(
                'http://localhost:5000/',
                data=cloudpickle.dumps(subgraph),
                headers={'Content-Type': 'application/octet-stream'}
            )
            if response.status_code != 200: raise Exception(f"Failed to invoke executor: {response.text}")

        # Invoke 1 new Executor per root node
        for root_node in self.root_nodes:
            _invoke_remote_executor(DAG(self.sink_node, master_dag_id=self.master_dag_id, root_nodes=[root_node]))
            
        if wait_for_final_result: 
            res = asyncio.run(self._wait_for_final_result())
            print(f"Got final result: {res}")
            return res

    # User interface must be synchronous
    def start_local_execution(self, wait_for_final_result=False):
        async def internal():
            coroutines: list[asyncio.Task] = []
            
            ex = executor.Executor(self)
            coroutines.append(asyncio.create_task(ex.start_executing()))
            
            if wait_for_final_result:
                wait_final_result_coroutine = asyncio.create_task(self._wait_for_final_result())
                coroutines.append(wait_final_result_coroutine)
                res = await wait_final_result_coroutine
                ex.shutdown_flag.set()
                return res
            
            await asyncio.gather(*coroutines)
            return None
        return asyncio.run(internal())

    async def _wait_for_final_result(self):
        # Asynchronously poll Storage for final result
        while True:
            final_result = intermediate_storage.IntermediateStorage.get(self.sink_node.task_id)
            if final_result is not None:
                return cloudpickle.loads(final_result) # type: ignore
            await asyncio.sleep(self._FINAL_RESULT_POLLING_TIME_S)

    def _identify_root_nodes(self):
        """Identify root nodes (nodes with no upstream dependencies)."""
        rns = list(self.all_nodes.values())
        for node in self.all_nodes.values():
            for dependent_node in node.downstream_nodes:
                for rn in rns:
                    if rn.task_id == dependent_node.task_id:
                        rns.remove(rn)
        return rns

    def _find_all_nodes_from_sink(self) -> dict[str, dag_task_node.DAGTaskNode]:
        """Build the complete graph by traversing from sink node upward."""
        all_nodes = {}
        
        def visit(node: dag_task_node.DAGTaskNode):
            if node.task_id in all_nodes: return
            all_nodes[node.task_id] = node
            for arg in node.upstream_nodes: visit(arg)
        
        visit(self.sink_node)
        return all_nodes
    
    def _find_all_nodes_from_roots(self):
        all_nodes = {}
        
        def visit(node: dag_task_node.DAGTaskNode):
            if node.task_id in all_nodes: return
            all_nodes[node.task_id] = node
            for arg in node.downstream_nodes: visit(arg)
        
        for root_node in self.root_nodes: visit(root_node)
        return all_nodes
    
    def get_node_by_id(self, node_id: str) -> dag_task_node.DAGTaskNode: return self.all_nodes[node_id]

    def visualize(self, output_file="dag_graph.png", highlight_roots=True, highlight_sink=True, open_after=True):
        # Create a new directed graph
        dot = graphviz.Digraph(
            comment="DAG Visualization",
            format="png",
            engine="dot"  # Use dot layout for directed graphs
        )
        dot.attr(rankdir="LR")  # Layout from left to right
        
        # Add nodes
        for node_id, node in self.all_nodes.items():
            # Create a label showing function name and args
            # for dependency in node.upstream_nodes:
            #     dependency_strs.append(str(dependency.task_id))
            
            dependency_strs = []
            for arg in node.func_args:
                if isinstance(arg, dag_task_node.DAGTaskNodeId):
                    dependency_strs.append(str(arg.value))
                else:
                    dependency_strs.append(str(arg))

            for key, value in node.func_kwargs.items():
                if isinstance(value, dag_task_node.DAGTaskNodeId):
                    dependency_strs.append(f"{key}={value.value}")
                else:
                    dependency_strs.append(f"{key}={value}")
            
            # Create the node label
            label = f"{node.task_id}({', '.join(dependency_strs)})"
            
            # Determine node style based on whether it's a root or sink node
            node_style = "filled"
            node_color = "lightblue"
            
            if highlight_roots and node in self.root_nodes:
                node_color = "lightgreen"  # Root nodes in green
            
            if highlight_sink and node.task_id == self.sink_node.task_id:
                node_color = "lightcoral"  # Sink node in red
            
            # Add the node to the graph
            dot.node(node_id, label=label, shape="box", style=node_style, fillcolor=node_color)
        
        # Add edges
        for node_id, node in self.all_nodes.items():
            for downstream_node in node.downstream_nodes:
                dot.edge(node_id, downstream_node.task_id)
        
        # Render the graph to a file and open it
        dot.render(filename=output_file.split('.')[0], cleanup=True, view=open_after)
    
    def serialize(self):
        return cloudpickle.dumps(self)

    @classmethod
    def from_serialized(cls, serialized_dag: bytes):
        return cloudpickle.loads(serialized_dag)