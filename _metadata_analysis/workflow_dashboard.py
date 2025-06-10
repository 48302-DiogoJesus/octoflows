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
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage.metrics.metrics_types import TaskMetrics
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.storage.prefixes import DAG_PREFIX
from src.storage.metrics.metrics_storage import MetricsStorage
from src.dag.dag import FullDAG
from src.storage.metrics.metrics_types import FullDAGPrepareTime

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

def get_workflows_information(intermediate_storage_conn: redis.Redis, metrics_storage_conn: redis.Redis) -> Dict[str, WorkflowInfo]:
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
    st.title("Workflow Execution Analysis Dashboard")
    
    # Connect to both Redis instances
    intermediate_storage_conn = get_redis_connection(6379)
    metrics_storage_conn = get_redis_connection(6380)
    
    # Initialize workflow types in session state if not already loaded
    if 'workflow_types' not in st.session_state:
        st.session_state.workflow_types = get_workflows_information(intermediate_storage_conn, metrics_storage_conn)
    
    workflow_types = st.session_state.workflow_types
    
    if not workflow_types:
        st.warning("No DAGs found in Redis")
        st.stop()
    
    # Add a refresh button to force reload workflow types
    if st.sidebar.button("🔄 Refresh Workflow Types"):
        # Clear the session state and reload
        st.session_state.workflow_types = get_workflows_information(intermediate_storage_conn, metrics_storage_conn)
        st.rerun()
    
    # Sidebar for workflow type selection
    st.sidebar.title("Workflow Filter")
    
    # Create a dropdown to select workflow type
    selected_workflow = st.sidebar.selectbox(
        "Select Workflow Type",
        options=["All"] + sorted(list(workflow_types.keys())),
        index=0
    )
    
    # Get all unique planner names from the selected workflow instances
    all_planners = set()
    if selected_workflow == "All":
        for workflow in workflow_types.values():
            for instance in workflow.instances:
                if instance.plan and instance.plan.planner_name:
                    all_planners.add(instance.plan.planner_name)
    else:
        for instance in workflow_types[selected_workflow].instances:
            if instance.plan and instance.plan.planner_name:
                all_planners.add(instance.plan.planner_name)
    
    selected_planner = st.sidebar.selectbox(
        "Select Planner",
        options=["All"] + sorted(list(all_planners)),
        index=0
    )
    
    # Filter workflow instances based on selection (workflow type + planner)
    matching_workflow_instances = []
    if selected_workflow == "All":
        for workflow in workflow_types.values():
            for instance in workflow.instances:
                if selected_planner == "All" or (instance.plan and instance.plan.planner_name == selected_planner):
                    matching_workflow_instances.append(instance)
    else:
        for instance in workflow_types[selected_workflow].instances:
            if selected_planner == "All" or (instance.plan and instance.plan.planner_name == selected_planner):
                matching_workflow_instances.append(instance)
    
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

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Workflow Instances", len(matching_workflow_instances))
    if selected_workflow != 'All':
        with col2:
            st.metric("Workflow Tasks", len(workflow_types[selected_workflow].dag._all_nodes))
    
    # Show metrics based on selection
    if selected_workflow != 'All' and matching_workflow_instances:
        if selected_planner != 'All':
            # Show average times for specific planner
            st.subheader("Average Times")
            
            # Initialize metrics
            total_execution_time = 0.0
            total_download_time = 0.0
            total_upload_time = 0.0
            total_makespan = 0.0
            valid_instances = 0
            
            # Calculate totals
            for instance in matching_workflow_instances:
                if not instance.plan or not instance.tasks:
                    continue
                    
                valid_instances += 1
                
                # Calculate per-task metrics
                instance_execution_time = sum(task.metrics.execution_time_ms for task in instance.tasks)
                instance_download_time = sum(task.metrics.input_metrics.input_download_time_ms for task in instance.tasks)
                instance_upload_time = sum(task.metrics.output_metrics.time_ms for task in instance.tasks)
                
                # Calculate makespan for this instance
                task_timings = []
                for task in instance.tasks:
                    task_start = task.metrics.started_at_timestamp_s * 1000  # Convert to ms
                    task_end = task_start
                    task_end += task.metrics.input_metrics.input_download_time_ms
                    task_end += task.metrics.execution_time_ms
                    task_end += task.metrics.total_invocation_time_ms
                    if hasattr(task.metrics, 'output_metrics') and task.metrics.output_metrics:
                        task_end += task.metrics.output_metrics.time_ms
                    task_timings.append((task_start, task_end))
                
                if task_timings:
                    min_start = min(start for start, _ in task_timings)
                    max_end = max(end for _, end in task_timings)
                    instance_makespan = max_end - min_start
                    total_makespan += instance_makespan
                
                total_execution_time += instance_execution_time
                total_download_time += instance_download_time
                total_upload_time += instance_upload_time
            
            # Calculate averages if we have valid instances
            if valid_instances > 0:
                avg_execution = total_execution_time / (valid_instances * 1000)  # Convert to seconds
                avg_download = total_download_time / (valid_instances * 1000)     # Convert to seconds
                avg_upload = total_upload_time / (valid_instances * 1000)         # Convert to seconds
                avg_makespan = total_makespan / (valid_instances * 1000)          # Convert to seconds
                
                # Display metrics in columns
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Avg Execution Time", f"{avg_execution:.2f}s")
                with col2:
                    st.metric("Avg Download Time", f"{avg_download:.2f}s")
                with col3:
                    st.metric("Avg Upload Time", f"{avg_upload:.2f}s")
                with col4:
                    st.metric("Avg Makespan", f"{avg_makespan:.2f}s")
        else:
            # Show comparison chart for all planners of this workflow type
            st.subheader("Performance Comparison by Planner")
            
            # Group instances by planner
            planner_metrics = {}
            
            for instance in workflow_types[selected_workflow].instances:
                if not instance.plan or not instance.plan.planner_name or not instance.tasks:
                    continue
                    
                planner_name = instance.plan.planner_name
                if planner_name not in planner_metrics:
                    planner_metrics[planner_name] = {
                        'execution_times': [],
                        'download_times': [],
                        'upload_times': [],
                        'makespans': [],
                        'instance_count': 0
                    }
                
                # Calculate metrics for this instance
                instance_execution = sum(task.metrics.execution_time_ms for task in instance.tasks) / 1000  # to seconds
                instance_download = sum(task.metrics.input_metrics.input_download_time_ms for task in instance.tasks) / 1000
                instance_upload = sum(task.metrics.output_metrics.time_ms for task in instance.tasks) / 1000
                
                # Calculate makespan for this instance
                task_timings = []
                for task in instance.tasks:
                    task_start = task.metrics.started_at_timestamp_s * 1000  # Convert to ms
                    task_end = task_start
                    task_end += task.metrics.input_metrics.input_download_time_ms
                    task_end += task.metrics.execution_time_ms
                    task_end += task.metrics.total_invocation_time_ms
                    if hasattr(task.metrics, 'output_metrics') and task.metrics.output_metrics:
                        task_end += task.metrics.output_metrics.time_ms
                    task_timings.append((task_start, task_end))
                
                instance_makespan = 0
                if task_timings:
                    min_start = min(start for start, _ in task_timings)
                    max_end = max(end for _, end in task_timings)
                    instance_makespan = (max_end - min_start) / 1000  # Convert to seconds
                
                planner_metrics[planner_name]['execution_times'].append(instance_execution)
                planner_metrics[planner_name]['download_times'].append(instance_download)
                planner_metrics[planner_name]['upload_times'].append(instance_upload)
                planner_metrics[planner_name]['makespans'].append(instance_makespan)
                planner_metrics[planner_name]['instance_count'] += 1
            
            # Calculate averages for each planner
            chart_data = []
            for planner, metrics in planner_metrics.items():
                if metrics['instance_count'] > 0:
                    chart_data.append({
                        'Planner': planner,
                        'Metric': 'Execution Time',
                        'Seconds': sum(metrics['execution_times']) / metrics['instance_count']
                    })
                    chart_data.append({
                        'Planner': planner,
                        'Metric': 'Download Time',
                        'Seconds': sum(metrics['download_times']) / metrics['instance_count']
                    })
                    chart_data.append({
                        'Planner': planner,
                        'Metric': 'Upload Time',
                        'Seconds': sum(metrics['upload_times']) / metrics['instance_count']
                    })
                    chart_data.append({
                        'Planner': planner,
                        'Metric': 'Makespan',
                        'Seconds': sum(metrics['makespans']) / metrics['instance_count']
                    })
            
            if chart_data:
                df = pd.DataFrame(chart_data)
                
                # Create grouped bar chart
                fig = px.bar(
                    df, 
                    x='Planner', 
                    y='Seconds',
                    color='Metric',
                    barmode='group',
                    title=f'Average Times by Planner for {selected_workflow}',
                    labels={'Seconds': 'Time (seconds)'},
                    color_discrete_map={
                        'Execution Time': '#1f77b4',
                        'Download Time': '#ff7f0e',
                        'Upload Time': '#2ca02c',
                        'Makespan': '#d62728'
                    }
                )
                
                # Improve layout
                fig.update_layout(
                    xaxis_title='Planner',
                    yaxis_title='Time (seconds)',
                    legend_title='Metric',
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No valid planner data available for comparison.")
    
if __name__ == "__main__":
    main()