import streamlit as st
import redis
import cloudpickle
from typing import Dict, List, Set, Optional, Tuple, Union
import pandas as pd
import plotly.express as px
from datetime import datetime
import hashlib
import colorsys

# Import necessary modules from the project
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage.prefixes import DAG_PREFIX
from src.storage.metrics.metrics_storage import MetricsStorage
from src.dag.dag import FullDAG

# Redis connection setup
def get_redis_connection(port: int = 6379):
    return redis.Redis(
        host='localhost',
        port=port,
        password='redisdevpwd123',
        decode_responses=False
    )

def get_workflow_types(dag_redis_6379, dag_redis_6780) -> Dict[str, Set[Tuple[bytes, int]]]:
    """
    Get all unique workflow types (sink_task.id.function_name) from both Redis instances
    Returns a dictionary mapping workflow types to sets of DAG keys
    """
    workflow_types: Dict[str, Set[Tuple[bytes, int]]] = {}
    
    # Check both Redis instances
    for redis_instance, port in [(dag_redis_6379, 6379), (dag_redis_6780, 6780)]:
        try:
            dag_keys = [key for key in redis_instance.keys() if key.decode('utf-8').startswith(DAG_PREFIX)]
            
            for dag_key in dag_keys:
                try:
                    dag_data = redis_instance.get(dag_key)
                    dag: FullDAG = cloudpickle.loads(dag_data)
                    
                    # Get the sink task's function name
                    sink_func_name = dag.sink_node.func_name
                    
                    # Add to our workflow types dictionary with port information
                    if sink_func_name not in workflow_types:
                        workflow_types[sink_func_name] = set()
                    # Store as tuple of (key, port) to identify which Redis instance to use later
                    # The key is bytes, port is int
                    workflow_types[sink_func_name].add((dag_key, port))
                    
                except Exception as e:
                    print(f"Error processing DAG {dag_key} from port {port}: {e}")
        except Exception as e:
            print(f"Error accessing Redis on port {port}: {e}")
    
    return workflow_types

def format_bytes(size: float) -> str:
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"

def get_color_for_workflow(workflow_name: str) -> str:
    """Generate a consistent color for each workflow type"""
    # Create a hash of the workflow name
    hash_obj = hashlib.md5(workflow_name.encode())
    hash_int = int(hash_obj.hexdigest(), 16)
    
    # Generate a hue value that is more spaced out
    hue = (hash_int % 360)  # Full hue spectrum (0-359 degrees)
    saturation = 0.7  # Keep colors vibrant
    lightness = 0.6   # Slightly lighter for better visibility
    
    # Convert HSL to RGB (values between 0-1)
    r, g, b = colorsys.hls_to_rgb(hue / 360, lightness, saturation)
    
    # Scale to 0-255 and format as RGB
    return f"rgb({int(r * 255)},{int(g * 255)},{int(b * 255)})"

def main():
    # Configure page layout for better visualization
    st.set_page_config(layout="wide")
    st.title("DAG Workflow Dashboard")
    
    # Connect to both Redis instances
    dag_redis_6379 = get_redis_connection(6379)
    dag_redis_6780 = get_redis_connection(6780)
    
    # Initialize workflow types in session state if not already loaded
    if 'workflow_types' not in st.session_state:
        st.session_state.workflow_types = get_workflow_types(dag_redis_6379, dag_redis_6780)
    
    workflow_types = st.session_state.workflow_types
    
    if not workflow_types:
        st.warning("No DAGs found in Redis")
        st.stop()
    
    # Add a refresh button to force reload workflow types
    if st.sidebar.button("🔄 Refresh Workflow Types"):
        # Clear the session state and reload
        st.session_state.workflow_types = get_workflow_types(dag_redis_6379, dag_redis_6780)
        st.rerun()
    
    # Sidebar for workflow type selection
    st.sidebar.title("Workflow Filter")
    
    # Create a dropdown to select workflow type
    selected_workflow = st.sidebar.selectbox(
        "Select Workflow Type",
        options=["All"] + sorted(list(workflow_types.keys())),
        index=0
    )
    
    # Get DAG keys based on selection
    dag_entries = []
    if selected_workflow == "All":
        for keys in workflow_types.values():
            dag_entries.extend(keys)
    else:
        dag_entries = list(workflow_types[selected_workflow])
    
    # Helper function to get the appropriate Redis connection based on port
    def get_redis_conn(port: int) -> redis.Redis:
        return dag_redis_6379 if port == 6379 else dag_redis_6780
    
    # Display workflow type statistics
    st.sidebar.subheader("Workflow Statistics")
    workflow_stats = []
    for workflow, keys in workflow_types.items():
        workflow_stats.append({
            "Workflow Type": workflow,
            "Count": len(keys),
            "Color": get_color_for_workflow(workflow)
        })
    
    # Create a bar chart of workflow counts
    if workflow_stats:
        df_workflows = pd.DataFrame(workflow_stats)
        fig = px.bar(
            df_workflows, 
            x="Workflow Type", 
            y="Count",
            color="Workflow Type",
            color_discrete_map={row["Workflow Type"]: row["Color"] for _, row in df_workflows.iterrows()},
            title="Workflow Type Distribution"
        )
        fig.update_layout(showlegend=False)
        st.sidebar.plotly_chart(fig, use_container_width=True)
    
    # Main content area
    st.header(f"DAGs for Workflow: {selected_workflow if selected_workflow != 'All' else 'All Workflows'}")
    
    # Display DAGs in a table
    dags_data = []
    for dag_entry in dag_entries:
        dag_key, port = dag_entry
        try:
            redis_conn = get_redis_conn(port)
            dag_data = redis_conn.get(dag_key)  # type: ignore
            dag: FullDAG = cloudpickle.loads(dag_data)
            
            # Basic DAG information
            dags_data.append({
                "DAG ID": f"{dag_key.decode('utf-8') if isinstance(dag_key, bytes) else dag_key} (port {port})",
                "Workflow Type": dag.sink_node.func_name,
                "Total Nodes": len(dag._all_nodes),
                "Root Nodes": len(dag.root_nodes),
                "Sink Node ID": dag.sink_node.id.get_full_id(),
                "Master DAG ID": dag.master_dag_id.split('_')[0],  # Just show the timestamp part
                "Structure Hash": dag.master_dag_structure_hash[:8] + "...",  # Shorten the hash for display
                "Has Upstream": len(dag.sink_node.upstream_nodes) > 0,
                "Has Downstream": len(dag.sink_node.downstream_nodes) > 0
            })
        except Exception as e:
            print(f"Error processing DAG {dag_key}: {e}")
    
    if dags_data:
        df_dags = pd.DataFrame(dags_data)
        
        # Add some basic statistics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total DAGs", len(df_dags))
        with col2:
            unique_workflows = int(df_dags['Workflow Type'].nunique())
            st.metric("Unique Workflow Types", unique_workflows)
        
        # Display the DAGs table
        st.dataframe(
            df_dags,
            column_config={
                "DAG ID": "DAG ID",
                "Workflow Type": "Workflow Type",
                "Number of Nodes": "# Nodes",
                "Root Nodes": "# Root Nodes",
                "Sink Node": "Sink Node ID",
                "Created At": "Creation Time"
            },
            hide_index=True,
            use_container_width=True
        )
        
    else:
        st.info("No DAGs found for the selected workflow type.")

if __name__ == "__main__":
    main()