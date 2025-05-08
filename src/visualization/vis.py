import asyncio
import tempfile
import base64
from collections import deque
import sys
import cloudpickle
import streamlit as st
import graphviz
import time
import subprocess
import atexit
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.logger import create_logger
import src.storage.storage as storage

logger = create_logger(__name__)

class DAGVisualizationDashboard:
    def __init__(self, dag, worker_config):
        from src.workers import worker
        _worker_config: worker.Worker.Config = worker_config
        self.dag = dag
        self.intermediate_storage: storage.Storage = _worker_config.intermediate_storage_config.create_instance()

    @staticmethod
    def start(dag, worker_config):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.dag') as dag_file, \
            tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.config') as config_file:
            
            dag_file.write(base64.b64encode(cloudpickle.dumps(dag)).decode('utf-8'))
            config_file.write(base64.b64encode(cloudpickle.dumps(worker_config)).decode('utf-8'))
            
            dag_path = dag_file.name
            config_path = config_file.name
        
        current_script = os.path.abspath(__file__)
        dashboard_process = subprocess.Popen(
            ["streamlit", "run", current_script, config_path, dag_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        def cleanup():
            # try:
            #     if os.path.exists(dag_path):
            #         os.unlink(dag_path)
            #     if os.path.exists(config_path):
            #         os.unlink(config_path)
            # except Exception as e:
            #     print(f"Error deleting temp files: {e}")
            
            if dashboard_process.poll() is None: 
                time.sleep(3) # Give the dashboard time to show it's completed
                try:
                    dashboard_process.terminate()
                    dashboard_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    dashboard_process.kill()
                    
        atexit.register(cleanup)

    def render_graphviz(self):
        """Render the DAG using Graphviz with left-to-right layout"""
        self.dag: FullDAG = self.dag # type it without introducing a circular dependency
        
        # Create a new directed graph
        graph = graphviz.Digraph()
        graph.attr(rankdir="LR")  # Left to right layout
        graph.attr(size="8,5")
        graph.attr(ratio="fill")
        graph.attr(fontname="Arial")
        graph.attr(fontsize="12")
        
        # Add nodes to the graph
        for name, node in self.dag._all_nodes.items():
            is_completed = asyncio.run(self.intermediate_storage.get(node.id.get_full_id_in_dag(self.dag))) is not None
            
            # Set node attributes based on completion status
            if is_completed:
                graph.node(name, 
                           label=f"{name}\n(Completed)",
                           style="filled", 
                           fillcolor="#4CAF50", 
                           fontcolor="white",
                           shape="box",
                           margin="0.2")
            else:
                graph.node(name, 
                           label=f"{name}\n(Pending)",
                           style="filled", 
                           fillcolor="#9E9E9E", 
                           fontcolor="white",
                           shape="box",
                           margin="0.2")
        
        # Add edges to the graph
        for name, node in self.dag._all_nodes.items():
            for downstream in node.downstream_nodes:
                downstream_id = downstream.id.get_full_id()
                graph.edge(name, downstream_id)
        
        return graph

    def run_dashboard(self):
        st.set_page_config(layout="wide", page_title="Real-time DAG Visualization")
        st.title("Real-time DAG Visualization")
        
        # Add sidebar with controls
        with st.sidebar:
            st.header("Controls")
            refresh_rate = st.slider("Refresh rate (seconds)", min_value=1, max_value=10, value=3)
            
            st.markdown("---")
            st.write("DAG Progress:")
            
            # Calculate progress
            completed = 0
            total = len(self.dag._all_nodes)
            for name, node in self.dag._all_nodes.items():
                if asyncio.run(self.intermediate_storage.get(node.id.get_full_id_in_dag(self.dag))) is not None:
                    completed += 1
            
            # Display progress bar
            st.progress(completed/total if total > 0 else 0)
            st.write(f"**{completed}/{total}** tasks completed")
        
        # Add explanation text
        st.write("This dashboard shows the DAG execution status in real-time. Green nodes have completed processing, while gray nodes are pending.")
        
        # Function to update the graph
        def update_graph():
            self._check_completed_tasks()
            
            # Render the graph using Graphviz
            graph = self.render_graphviz()
            graph.engine = "dot"
            st.graphviz_chart(graph, use_container_width=True)

        while True:
            update_graph()
            st.rerun()
            time.sleep(refresh_rate)

    def _check_completed_tasks(self):
        assert self.dag.root_nodes is not None
        visited = set()
        queue = deque(self.dag.root_nodes)

        while queue:
            node = queue.popleft()
            if node.id.get_full_id() in visited:
                continue

            if asyncio.run(self.intermediate_storage.get(node.id.get_full_id_in_dag(self.dag))) is not None:
                visited.add(node.id.get_full_id())

                for downstream in node.downstream_nodes:
                    queue.append(downstream)

if __name__ == "__main__":
    from src.workers.worker import Worker
    from src.dag.dag import FullDAG
    import sys
    import base64
    import cloudpickle
    
    if len(sys.argv) != 3:
        raise Exception("Usage: python script.py <config_path> <dag_path>")
    
    try:
        # Read and decode the config file
        with open(sys.argv[1], 'r') as config_file:
            config = cloudpickle.loads(base64.b64decode(config_file.read()))
        
        # Read and decode the DAG file
        with open(sys.argv[2], 'r') as dag_file:
            dag = cloudpickle.loads(base64.b64decode(dag_file.read()))
        
        if not isinstance(config, Worker.Config):
            raise Exception("Error: config is not a Worker.Config instance")
        if not isinstance(dag, FullDAG):
            raise Exception("Error: dag is not a DAG instance")
        
        DAGVisualizationDashboard(dag, config).run_dashboard()
    
    except FileNotFoundError as e:
        raise Exception(f"Temp file not found: {e}")
    except Exception as e:
        raise Exception(f"Error loading dashboard data: {e}")