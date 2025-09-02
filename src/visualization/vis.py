import asyncio
import tempfile
import base64
from collections import deque
import sys
import threading
import cloudpickle
import streamlit as st
import time
import subprocess
import atexit
import os
import nest_asyncio
import signal

nest_asyncio.apply()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.logger import create_logger
import src.storage.storage as storage

logger = create_logger(__name__)

REFRESH_RATE_S = 3

class DAGVisualizationDashboard:
    def __init__(self, dag, worker_config):
        from src.workers import worker
        _worker_config: worker.Worker.Config = worker_config
        self.dag = dag
        self.intermediate_storage: storage.Storage = _worker_config.intermediate_storage_config.create_instance()
        self._lock = threading.RLock()
        self.completed_tasks = set()
        self.all_tasks_completed = False  # Add flag to track completion status

    @staticmethod
    def start(dag, worker_config):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.dag') as dag_file, \
            tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.config') as config_file:
            
            dag_file.write(base64.b64encode(cloudpickle.dumps(dag)).decode('utf-8'))
            config_file.write(base64.b64encode(cloudpickle.dumps(worker_config)).decode('utf-8'))
            
            dag_path = dag_file.name
            config_path = config_file.name
        
        current_script = os.path.abspath(__file__)
        # Start dashboard as a completely independent process
        dashboard_process = subprocess.Popen(
            ["streamlit", "run", current_script, config_path, dag_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            # Make it independent from parent process
            start_new_session=True,
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None # type: ignore
        )
        
        # Don't register cleanup - let the dashboard run independently
        return dashboard_process

    def render_agraph(self):
        from streamlit_agraph import agraph, Node, Edge, Config
        self.dag: FullDAG = self.dag # type it without introducing a circular dependency
        
        nodes = []
        edges = []
        node_levels = {}
        visited = set()
        
        async def update_completed_tasks():
            # Skip checking if all tasks are already completed
            if self.all_tasks_completed:
                return
                
            async with self.intermediate_storage.batch() as batch:
                for node_id, node in self.dag._all_nodes.items():
                    if node_id in self.completed_tasks: continue
                    await batch.exists(node.id.get_full_id_in_dag(self.dag), result_key=node_id)
 
                await batch.execute()
 
                newly_completed = []
                for node_id in self.dag._all_nodes.keys():
                    if batch.get_result(node_id) and node_id not in self.completed_tasks:
                        newly_completed.append(node_id)
                        self.completed_tasks.add(node_id)
                
                # Propagate completion to upstream tasks
                if newly_completed:
                    self._propagate_completion_upstream(newly_completed)
                
                # Check if all tasks are now completed
                total_tasks = len(self.dag._all_nodes)
                if len(self.completed_tasks) >= total_tasks:
                    self.all_tasks_completed = True
                    logger.info("All DAG tasks completed. Stopping completion checks.")
        
        asyncio.run(update_completed_tasks())

        def traverse_dag(node, level=0):
            """ Recursively traverse DAG from root nodes """
            node_id = node.id.get_full_id()
            
            if node_id in visited:
                return  # Prevents duplicate processing

            visited.add(node_id)
            node_levels[node_id] = level

            # Determine if task is completed
            is_completed = node_id in self.completed_tasks
            
            # Set node color based on completion status
            if is_completed:
                node_color = "#4CAF50"  # Green for completed
            else:
                node_color = "#9E9E9E"  # Gray for pending
                
            label = node.func_name
                
            nodes.append(Node(
                id=node_id, 
                label=label,
                title=f"task_id: {node_id}\nstatus: {'Completed' if is_completed else 'Pending'}",
                size=25,
                color=node_color,
                shape="box",
                font={"color": "white", "size": 12, "face": "Arial"},
                level=level
            ))
        
            # Process downstream nodes
            for downstream in node.downstream_nodes:
                downstream_id = downstream.id.get_full_id()
                
                edges.append(Edge(
                    source=node_id, 
                    target=downstream_id, 
                    arrow="to",
                    color="#888888",  # Gray for regular edges
                    width=1
                ))
                traverse_dag(downstream, level + 1)
                
        # Start traversal from all root nodes
        assert self.dag.root_nodes
        for root in self.dag.root_nodes:
            traverse_dag(root, level=0)

        # Graph configuration
        config = Config(
            width="100%", # type: ignore
            height=600,
            directed=True,
            physics=False,
            hierarchical=True,
            hierarchical_sort_method="directed"
        )

        return agraph(nodes=nodes, edges=edges, config=config)

    def run_dashboard(self):
        st.set_page_config(layout="wide", page_title="Real-time DAG Visualization")
        st.title("Real-time DAG Visualization")
        
        # Add sidebar with controls
        with st.sidebar:
            st.header("Controls")
            
            # Close button to stop the dashboard
            if st.button("🚪 Close Dashboard", type="primary"):
                os.kill(os.getpid(), signal.SIGTERM)
                st.stop()
            
            st.markdown("---")
            
            st.markdown("---")
            st.write("DAG Progress:")
            
            # Calculate progress
            completed = 0
            total = len(self.dag._all_nodes)
            for name, node in self.dag._all_nodes.items():
                if node.id.get_full_id() in self.completed_tasks: completed += 1
            
            # Display progress bar
            progress_value = completed/total if total > 0 else 0
            st.progress(progress_value)
            st.write(f"**{completed}/{total}** tasks completed")
            
            # Show completion status
            if self.all_tasks_completed:
                st.success("✅ DAG execution completed!")
            
            # Show legend
            st.markdown("---")
            st.write("Legend:")
            st.markdown("🟢 **Green**: Completed tasks")
            st.markdown("⚪ **Gray**: Pending tasks")
        
        # Add explanation text
        explanation = "This dashboard shows the DAG execution status in real-time. Green nodes have completed processing, while gray nodes are pending."
        if self.all_tasks_completed:
            explanation += " **All tasks have been completed.**"
        st.write(explanation)
        
        # Create two columns for the graph
        col1, col2 = st.columns([4, 1])
        
        with col1:
            self._check_completed_tasks()
            selected_node = self.render_agraph()
            
            # Display selected node information
            if selected_node:
                st.write(f"**Selected Node:** {selected_node}")
        
        with col2:
            # Additional node details can go here
            st.write("**Node Details**")
            if hasattr(st.session_state, 'last_selected') and st.session_state.last_selected:
                node_id = st.session_state.last_selected
                if node_id in self.dag._all_nodes:
                    node = self.dag._all_nodes[node_id]
                    st.write(f"Function: {node.func_name}")
                    st.write(f"Status: {'Completed' if node.id.get_full_id() in self.completed_tasks else 'Pending'}")
                    
                    # Show upstream and downstream connections
                    upstream = [up.id.get_full_id() for up in getattr(node, 'upstream_nodes', [])]
                    downstream = [down.id.get_full_id() for down in node.downstream_nodes]
                    
                    if upstream:
                        st.write("**Upstream:**")
                        for up in upstream:
                            st.write(f"- {up}")
                    
                    if downstream:
                        st.write("**Downstream:**")
                        for down in downstream:
                            st.write(f"- {down}")
        
        # Store selected node in session state
        if selected_node:
            st.session_state.last_selected = selected_node
        
        # Only refresh if not all tasks are completed
        if not self.all_tasks_completed:
            time.sleep(REFRESH_RATE_S)
            st.rerun()

    def _propagate_completion_upstream(self, newly_completed):
        """
        When tasks become completed, mark all upstream tasks (dependencies) as completed as well.
        This ensures that if a task is done, all tasks it depends on are also marked as done.
        """
        tasks_to_process = deque(newly_completed)
        
        while tasks_to_process:
            current_task_id = tasks_to_process.popleft()
            
            if current_task_id in self.dag._all_nodes:
                current_node = self.dag._all_nodes[current_task_id]
                
                # Get upstream nodes (dependencies)
                upstream_nodes = getattr(current_node, 'upstream_nodes', [])
                
                for upstream_node in upstream_nodes:
                    upstream_id = upstream_node.id.get_full_id()
                    
                    # If upstream task is not already completed, mark it as completed
                    if upstream_id not in self.completed_tasks:
                        self.completed_tasks.add(upstream_id)
                        # Continue propagating upstream
                        tasks_to_process.append(upstream_id)

    def _check_completed_tasks(self):
        assert self.dag.root_nodes is not None
        visited = set()
        queue = deque(self.dag.root_nodes)

        while queue:
            node = queue.popleft()
            if node.id.get_full_id() in visited:
                continue

            if node.id.get_full_id() in self.completed_tasks:
                visited.add(node.id.get_full_id())

                for downstream in node.downstream_nodes:
                    queue.append(downstream)

# Note: Streamlit will re-run the entire script on every rerun() call. That's why I'm using session_state to avoid re-initializing the dashboard
if __name__ == "__main__":
    if 'initialized' not in st.session_state:
        st.session_state.initialized = False

    if not st.session_state.initialized:
        st.session_state.initialized = True
        from src.workers.worker import Worker
        from src.dag.dag import FullDAG
        import sys
        import base64
        import cloudpickle
        
        if len(sys.argv) != 3:
            raise Exception("Usage: python script.py <config_path> <dag_path>")
        
        try:
            with open(sys.argv[1], 'r') as config_file:
                config = cloudpickle.loads(base64.b64decode(config_file.read()))
            with open(sys.argv[2], 'r') as dag_file:
                dag = cloudpickle.loads(base64.b64decode(dag_file.read()))
            
            if not isinstance(config, Worker.Config):
                raise Exception("Error: config is not a Worker.Config instance")
            if not isinstance(dag, FullDAG):
                raise Exception("Error: dag is not a DAG instance")
            
            st.session_state.dashboard = DAGVisualizationDashboard(dag, config)
        
        except Exception as e:
            raise Exception(f"Error loading dashboard: {e}")

    st.session_state.dashboard.run_dashboard()