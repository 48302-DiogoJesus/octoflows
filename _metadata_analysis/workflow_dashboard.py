import streamlit as st
import redis
import cloudpickle
from typing import Dict, List, Set, Optional, Tuple, Union
import pandas as pd
import plotly.express as px
from datetime import datetime
import hashlib
import colorsys
from dataclasses import dataclass

# Import necessary modules from the project
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage.metrics.metrics_types import TaskMetrics
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.storage.prefixes import DAG_PREFIX
from src.storage.metrics.metrics_storage import MetricsStorage
from src.dag.dag import FullDAG
from src.storage.metrics.metrics_types import FullDAGPrepareTime

# Redis connection setup
def get_redis_connection(port: int = 6379):
    return redis.Redis(
        host='localhost',
        port=port,
        password='redisdevpwd123',
        decode_responses=False
    )

@dataclass
class WorkflowInstanceTaskInfo:
    task_id: str
    metrics: TaskMetrics

@dataclass
class WorkflowInstanceInfo:
    plan: AbstractDAGPlanner.PlanOutput | None
    download_time: FullDAGPrepareTime | None
    tasks: List[WorkflowInstanceTaskInfo]

@dataclass
class WorkflowInfo:
    type: str
    dag: FullDAG
    instances: List[WorkflowInstanceInfo]

def get_workflow_types(intermediate_storage_conn: redis.Redis, metrics_storage_conn: redis.Redis) -> Dict[str, WorkflowInfo]:
    workflow_types: Dict[str, WorkflowInfo] = {}
    
    try:
        redis_instance = intermediate_storage_conn
        all_dag_keys = [key for key in redis_instance.keys() if key.decode('utf-8').startswith(DAG_PREFIX)] # type: ignore
        
        for dag_key in all_dag_keys:
            try:
                dag_data = redis_instance.get(dag_key)
                dag: FullDAG = cloudpickle.loads(dag_data) # type: ignore

                plan_data = metrics_storage_conn.get(f"{MetricsStorage.PLAN_KEY_PREFIX}{dag.master_dag_id}")
                plan_output: AbstractDAGPlanner.PlanOutput | None = cloudpickle.loads(plan_data) if plan_data else None # type: ignore

                download_time_data = metrics_storage_conn.get(f"{MetricsStorage.DAG_METRICS_KEY_PREFIX}{dag.master_dag_id}")
                download_time: FullDAGPrepareTime | None = cloudpickle.loads(download_time_data) if download_time_data else None # type: ignore

                tasks_data = metrics_storage_conn.mget([f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}{t.id.get_full_id_in_dag(dag)}" for t in dag._all_nodes.values()])
                tasks: List[WorkflowInstanceTaskInfo] = [WorkflowInstanceTaskInfo(t.id.get_full_id_in_dag(dag), cloudpickle.loads(task_data)) for t, task_data in zip(dag._all_nodes.values(), tasks_data)] # type: ignore  

                if dag.dag_name not in workflow_types: workflow_types[dag.dag_name] = WorkflowInfo(dag.dag_name, dag, [])
                workflow_types[dag.dag_name].instances.append(WorkflowInstanceInfo(plan_output, download_time, tasks))
            except Exception as e:
                print(f"Error processing DAG {dag_key}: {e}")
    except Exception as e:
        print(f"Error accessing Redis: {e}")
    
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
    intermediate_storage_conn = get_redis_connection(6379)
    metrics_storage_conn = get_redis_connection(6380)
    
    # Initialize workflow types in session state if not already loaded
    if 'workflow_types' not in st.session_state:
        st.session_state.workflow_types = get_workflow_types(intermediate_storage_conn, metrics_storage_conn)
    
    workflow_types = st.session_state.workflow_types
    
    if not workflow_types:
        st.warning("No DAGs found in Redis")
        st.stop()
    
    # Add a refresh button to force reload workflow types
    if st.sidebar.button("🔄 Refresh Workflow Types"):
        # Clear the session state and reload
        st.session_state.workflow_types = get_workflow_types(intermediate_storage_conn, metrics_storage_conn)
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
    workflow_instances = []
    if selected_workflow == "All":
        for keys in workflow_types.values():
            workflow_instances.extend(keys.instances)
    else:
        workflow_instances = list(workflow_types[selected_workflow].instances)
    
    st.sidebar.subheader("Workflow Statistics")
    workflow_stats = []
    for workflow, keys in workflow_types.items():
        workflow_stats.append({
            "Workflow Type": workflow,
            "Count": len(keys.instances),
            "Color": get_color_for_workflow(workflow)
        })
    
    # Bar Chart of workflow counts
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
    
    st.header(selected_workflow if selected_workflow != 'All' else 'All Workflows')

    st.metric("Workflow Instances", len(workflow_instances))
    if selected_workflow != 'All':
        st.metric("Workflow Tasks", len(workflow_types[selected_workflow].dag._all_nodes))
    
if __name__ == "__main__":
    main()