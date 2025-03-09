import cloudpickle
import graphviz
import os
import subprocess
import platform

from .DAGTaskNode import DAGTaskNode, DAGTaskNodeId


class DAG:
    """A class to represent a directed acyclic graph of tasks."""
    
    def __init__(self, sink_node: DAGTaskNode):
        """Create a DAG from sink node (node with no downstream tasks)."""
        self.sink_node = sink_node
        self.all_nodes: dict[str, DAGTaskNode] = {}
        self.root_nodes: list[DAGTaskNode] = []
        self._build_graph()
        self._identify_root_nodes()
        self._convert_node_func_args_to_ids()
    
    def _identify_root_nodes(self):
        """Identify root nodes (nodes with no upstream dependencies)."""
        # First, collect all nodes that are dependencies of other nodes
        dependency_nodes = set()
        for node in self.all_nodes.values():
            for arg in node.func_args:
                if isinstance(arg, DAGTaskNode):
                    dependency_nodes.add(arg.task_id)
            
            for _, value in node.func_kwargs.items():
                if isinstance(value, DAGTaskNode):
                    dependency_nodes.add(value.task_id)
        
        # Root nodes are all nodes that aren't dependencies of other nodes
        self.root_nodes = [
            node for node_id, node in self.all_nodes.items()
            if node_id not in dependency_nodes
        ]

    def _build_graph(self):
        """Build the complete graph by traversing from sink node upward."""
        visited: set[str] = set()
        
        def visit(node: DAGTaskNode):
            if node.task_id in visited:
                return
            visited.add(node.task_id)
            self.all_nodes[node.task_id] = node
            
            # Visit dependencies (nodes in args and kwargs)
            for arg in node.func_args:
                if isinstance(arg, DAGTaskNode):
                    visit(arg)
                    
            for _, value in node.func_kwargs.items():
                if isinstance(value, DAGTaskNode):
                    visit(value)
        
        visit(self.sink_node)
    
    def _convert_node_func_args_to_ids(self):
        """
        Convert all DAGTaskNode references in {func_args} and {func_kwargs} to DAGTaskNodeId to save space
        """
        for node in self.all_nodes.values():
            # Convert func_args
            new_args = []
            for arg in node.func_args:
                if isinstance(arg, DAGTaskNode):
                    new_args.append(DAGTaskNodeId(arg.task_id))
                else:
                    new_args.append(arg)
            node.func_args = tuple(new_args)
            
            # Convert func_kwargs
            new_kwargs = {}
            for key, value in node.func_kwargs.items():
                if isinstance(value, DAGTaskNode):
                    new_kwargs[key] = DAGTaskNodeId(value.task_id)
                else:
                    new_kwargs[key] = value
            node.func_kwargs = new_kwargs
    
    def get_node_by_id(self, node_id: str) -> DAGTaskNode | None:
        """Retrieve a node by its ID."""
        return self.all_nodes.get(node_id)

    def visualize(self, output_file="dag_graph.png", highlight_roots=True, highlight_sink=True):
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
            args_strs = []
            
            # Format function arguments
            for arg in node.func_args:
                if isinstance(arg, DAGTaskNodeId):
                    dep_node = self.get_node_by_id(arg.value)
                    args_strs.append(dep_node.task_id if dep_node else f"Unknown({arg.value})")
                else:
                    args_strs.append(str(arg))
            
            # Format function keyword arguments
            for key, value in node.func_kwargs.items():
                if isinstance(value, DAGTaskNodeId):
                    dep_node = self.get_node_by_id(value.value)
                    kwargs_str = f"{key}={dep_node.task_id if dep_node else f'Unknown({value.value})'}"
                else:
                    kwargs_str = f"{key}={value}"
                args_strs.append(kwargs_str)
            
            # Create the node label
            label = f"{node.task_id}({', '.join(args_strs)})"
            
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
        
        # Render the graph to a file
        try:
            # Render and save the graph
            dot.render(filename=output_file.split('.')[0], cleanup=True)
            
            # Get the full path to the rendered file
            rendered_file = f"{output_file.split('.')[0]}.png"
            abs_path = os.path.abspath(rendered_file)
            
            # Open the file with the default image viewer
            if platform.system() == 'Darwin':  # macOS
                subprocess.run(['open', abs_path], check=True)
            elif platform.system() == 'Windows':
                os.startfile(abs_path)
            else:  # Linux and others
                subprocess.run(['xdg-open', abs_path], check=True)
            
            return f"Graph visualization saved to {abs_path}"
        except Exception as e:
            return f"Error rendering graph: {str(e)}"
    
    def serialize(self):
        return cloudpickle.dumps(self)

    @classmethod
    def from_serialized(cls, serialized_dag: bytes):
        return cloudpickle.loads(serialized_dag)
