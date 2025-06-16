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
        all_dag_keys = [key for key in intermediate_storage_conn.keys() if key.decode('utf-8').startswith(DAG_PREFIX)] # type: ignore
        
        for dag_key in all_dag_keys:
            try:
                dag_data = intermediate_storage_conn.get(dag_key)
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

def calculate_prediction_error(actual, predicted):
    """Calculate relative prediction error percentage"""
    if actual == 0 and predicted == 0:
        return 0
    if actual == 0:
        return float('inf')
    return abs(actual - predicted) / actual * 100

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
    matching_workflow_instances: list[WorkflowInstanceInfo] = []
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
    
    st.header(selected_workflow if selected_workflow != 'All' else 'All Workflow Instances')

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
                    st.metric("Avg Time Executing Tasks", f"{avg_execution:.2f}s")
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
            
            # Add sample count analysis section
            st.subheader("Sample Count Analysis")
            
            # Prepare data for sample count analysis
            sample_data = []
            error_summary_data = []
            
            for instance in workflow_types[selected_workflow].instances:
                if not instance.plan or not instance.plan.prediction_sample_counts or not instance.tasks:
                    continue
                
                # Calculate actual vs predicted metrics
                actual_download = sum(task.metrics.input_metrics.input_download_time_ms / 1000 for task in instance.tasks)  # in seconds
                actual_execution = sum(task.metrics.execution_time_ms / 1000 for task in instance.tasks)  # in seconds
                actual_upload = sum(task.metrics.output_metrics.time_ms / 1000 for task in instance.tasks)  # in seconds
                actual_output_size = sum(task.metrics.output_metrics.size_bytes for task in instance.tasks if hasattr(task.metrics, 'output_metrics') and task.metrics.output_metrics)  # in bytes
                
                # Calculate makespan
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
                
                actual_makespan = 0
                if task_timings:
                    min_start = min(start for start, _ in task_timings)
                    max_end = max(end for _, end in task_timings)
                    actual_makespan = (max_end - min_start) / 1000  # Convert to seconds
                
                if instance.plan and instance.plan.nodes_info:
                    # Calculate total metrics for non-makespan related stats
                    predicted_download = sum(info.download_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_execution = sum(info.exec_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_upload = sum(info.upload_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_output_size = sum(info.output_size for info in instance.plan.nodes_info.values())  # in bytes
                    
                    # Calculate makespan using critical path analysis
                    # Initialize earliest start and finish times for each node
                    earliest_start = {node_id: 0.0 for node_id in instance.plan.nodes_info}
                    earliest_finish = {node_id: 0.0 for node_id in instance.plan.nodes_info}
                    
                    # Process nodes in topological order
                    for node_id, info in instance.plan.nodes_info.items():
                        # Total time for this node (download + exec + upload)
                        node_duration = (info.download_time + info.exec_time + info.upload_time) / 1000  # in seconds
                        
                        # Find the latest finishing upstream node
                        max_upstream_finish = 0.0
                        for upstream_node in info.node_ref.upstream_nodes:
                            upstream_id = upstream_node.id.get_full_id()
                            if upstream_id in earliest_finish:
                                max_upstream_finish = max(max_upstream_finish, earliest_finish[upstream_id])
                        
                        # Update this node's start and finish times
                        earliest_start[node_id] = max_upstream_finish
                        earliest_finish[node_id] = max_upstream_finish + node_duration
                    
                    # The makespan is the maximum finish time of all nodes
                    predicted_makespan = max(earliest_finish.values()) if earliest_finish else 0.0
                    
                    # Calculate prediction errors
                    download_error = calculate_prediction_error(actual_download, predicted_download)
                    execution_error = calculate_prediction_error(actual_execution, predicted_execution)
                    upload_error = calculate_prediction_error(actual_upload, predicted_upload)
                    output_size_error = calculate_prediction_error(actual_output_size, predicted_output_size)
                    makespan_error = calculate_prediction_error(actual_makespan, predicted_makespan)
                    
                    # Time differences (actual - predicted)
                    download_diff = actual_download - predicted_download
                    execution_diff = actual_execution - predicted_execution
                    upload_diff = actual_upload - predicted_upload
                    makespan_diff = actual_makespan - predicted_makespan
                    
                    sample_data.append({
                        'Planner': instance.plan.planner_name,
                        'Download Samples': instance.plan.prediction_sample_counts.for_download_speed,
                        'Execution Samples': instance.plan.prediction_sample_counts.for_execution_time,
                        'Upload Samples': instance.plan.prediction_sample_counts.for_upload_speed,
                        'Output Size Samples': getattr(instance.plan.prediction_sample_counts, 'for_output_size', 0),
                        'Download Error %': download_error,
                        'Execution Error %': execution_error,
                        'Upload Error %': upload_error,
                        'Output Size Error %': output_size_error,
                        'Makespan Error %': makespan_error,
                        'Instance': f"{instance.plan.planner_name}_{len(sample_data)}",
                        'Actual Output Size': actual_output_size,
                        'Predicted Output Size': predicted_output_size,
                        'Download Diff (s)': download_diff,
                        'Execution Diff (s)': execution_diff,
                        'Upload Diff (s)': upload_diff,
                        'Makespan Diff (s)': makespan_diff
                    })
                    
                    # Add to error summary data
                    error_summary_data.append({
                        'Planner': instance.plan.planner_name,
                        'Metric': 'Download Time',
                        'Error %': download_error,
                        'Diff (s)': download_diff
                    })
                    error_summary_data.append({
                        'Planner': instance.plan.planner_name,
                        'Metric': 'Execution Time',
                        'Error %': execution_error,
                        'Diff (s)': execution_diff
                    })
                    error_summary_data.append({
                        'Planner': instance.plan.planner_name,
                        'Metric': 'Upload Time',
                        'Error %': upload_error,
                        'Diff (s)': upload_diff
                    })
                    error_summary_data.append({
                        'Planner': instance.plan.planner_name,
                        'Metric': 'Makespan',
                        'Error %': makespan_error,
                        'Diff (s)': makespan_diff
                    })
            
            if sample_data:
                df_samples = pd.DataFrame(sample_data)
                df_errors = pd.DataFrame(error_summary_data)
                
                # Create tabs for different visualizations
                tab1, tab2 = st.tabs(["Prediction Errors", "Error vs Samples"])
                
                with tab1:
                    st.write("### Prediction Error Analysis")
                    
                    # Calculate average errors by planner and metric
                    error_stats = df_errors.groupby(['Planner', 'Metric']).agg({
                        'Error %': 'mean',
                        'Diff (s)': 'mean'
                    }).reset_index()
                    
                    # Pivot the data for better visualization
                    error_pivot = error_stats.pivot(
                        index='Planner',
                        columns='Metric',
                        values=['Error %', 'Diff (s)']
                    ).reset_index()
                    
                    # Flatten multi-index columns
                    error_pivot.columns = [' '.join(col).strip() for col in error_pivot.columns.values]
                    
                    # Show the error statistics table
                    st.write("#### Average Prediction Errors")
                    st.dataframe(
                        error_pivot.style.format({
                            'Error % Download Time': '{:.2f}%',
                            'Error % Execution Time': '{:.2f}%',
                            'Error % Upload Time': '{:.2f}%',
                            'Error % Makespan': '{:.2f}%',
                            'Diff (s) Download Time': '{:.3f}s',
                            'Diff (s) Execution Time': '{:.3f}s',
                            'Diff (s) Upload Time': '{:.3f}s',
                            'Diff (s) Makespan': '{:.3f}s'
                        }),
                        use_container_width=True
                    )
                    
                    # Create bar charts for error visualization
                    st.write("#### Error Percentage by Metric")
                    fig_error_pct = px.bar(
                        error_stats,
                        x='Metric',
                        y='Error %',
                        color='Planner',
                        barmode='group',
                        title='Average Prediction Error Percentage by Metric',
                        labels={'Error %': 'Error Percentage (%)'},
                        height=500
                    )
                    st.plotly_chart(fig_error_pct, use_container_width=True)
                    
                    st.write("#### Time Difference by Metric")
                    fig_error_diff = px.bar(
                        error_stats,
                        x='Metric',
                        y='Diff (s)',
                        color='Planner',
                        barmode='group',
                        title='Average Time Difference (Actual - Predicted)',
                        labels={'Diff (s)': 'Time Difference (seconds)'},
                        height=500
                    )
                    st.plotly_chart(fig_error_diff, use_container_width=True)
                
                with tab2:
                    st.write("### Prediction Error vs Sample Count")
                    
                    # Let user select which metric to analyze
                    metric = st.radio(
                        "Select Metric to Analyze:",
                        ["Download", "Execution", "Upload", "Output Size"],
                        horizontal=True
                    ).lower()
                    
                    # Create scatter plot of error vs sample count
                    metric_display = metric.replace('_', ' ').title()
                    samples_col = f"{metric_display} Samples"
                    error_col = f"{metric_display} Error %"
                    
                    fig_error = px.scatter(
                        df_samples,
                        x=samples_col,
                        y=error_col,
                        color='Planner',
                        title=f'{metric_display} Prediction Error vs Sample Count',
                        hover_data=['Instance', 'Actual Output Size', 'Predicted Output Size'] if 'output_size' in metric else ['Instance'],
                        trendline='lowess',
                        labels={
                            samples_col: f"Number of {metric.replace('_', ' ')} samples",
                            error_col: f"Prediction Error %"
                        }
                    )
                    fig_error.update_traces(
                        marker=dict(size=10, line=dict(width=1, color='DarkSlateGrey')),
                        selector=dict(mode='markers')
                    )
                    st.plotly_chart(fig_error, use_container_width=True)
            else:
                st.warning("No sample count data available for analysis.")
        
        # Add instance comparison table at the end of the page
        st.subheader("Instance Comparison")
        
        # Prepare data for the instance comparison table
        instance_data = []
        for idx, instance in enumerate(matching_workflow_instances):
            if not instance.plan or not instance.tasks:
                continue
                
            # Calculate actual metrics
            actual_download = sum(task.metrics.input_metrics.input_download_time_ms / 1000 for task in instance.tasks)  # in seconds
            actual_execution = sum(task.metrics.execution_time_ms / 1000 for task in instance.tasks)  # in seconds
            actual_upload = sum(task.metrics.output_metrics.time_ms / 1000 for task in instance.tasks)  # in seconds
            
            # Calculate actual makespan
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
            
            actual_makespan = 0
            if task_timings:
                min_start = min(start for start, _ in task_timings)
                max_end = max(end for _, end in task_timings)
                actual_makespan = (max_end - min_start) / 1000  # Convert to seconds
            
            # Get predicted metrics if available
            predicted_download = predicted_execution = predicted_upload = predicted_makespan = 0
            if instance.plan and instance.plan.nodes_info:
                predicted_download = sum(info.download_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                predicted_execution = sum(info.exec_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                predicted_upload = sum(info.upload_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                
                # Calculate predicted makespan using critical path analysis
                earliest_finish = {node_id: 0.0 for node_id in instance.plan.nodes_info}
                for node_id, info in instance.plan.nodes_info.items():
                    node_duration = (info.download_time + info.exec_time + info.upload_time) / 1000
                    max_upstream_finish = 0.0
                    for upstream_node in info.node_ref.upstream_nodes:
                        upstream_id = upstream_node.id.get_full_id()
                        if upstream_id in earliest_finish:
                            max_upstream_finish = max(max_upstream_finish, earliest_finish[upstream_id])
                    earliest_finish[node_id] = max_upstream_finish + node_duration
                predicted_makespan = max(earliest_finish.values()) if earliest_finish else 0.0
            
            # Calculate differences and percentages with sample counts
            def format_metric(actual, predicted, samples=None):
                if predicted == 0 and actual == 0:
                    base = "0.00s (0.00%)"
                else:
                    diff = actual - predicted
                    pct_diff = (diff / predicted * 100) if predicted != 0 else float('inf')
                    sign = "+" if diff >= 0 else "-"
                    base = f"{predicted:.2f}s → {actual:.2f}s ({sign}{abs(diff):.2f}s, {sign}{abs(pct_diff):.1f}%)"
                
                if samples is not None:
                    return f"{base}\n(samples: {samples})"
                return base
            
            # Get sample counts if available
            sample_counts = instance.plan.prediction_sample_counts if instance.plan and hasattr(instance.plan, 'prediction_sample_counts') else None
            
            # Get the master_dag_id from the workflow's DAG object
            workflow_info = workflow_types.get(selected_workflow)
            master_dag_id = workflow_info.dag.master_dag_id if workflow_info and hasattr(workflow_info.dag, 'master_dag_id') else 'N/A'
            
            instance_data.append({
                'Workflow Type': selected_workflow,
                'Planner': instance.plan.planner_name if instance.plan else 'N/A',
                'Master DAG ID': master_dag_id,
                'Makespan': format_metric(actual_makespan, predicted_makespan, 
                                       sample_counts.for_execution_time if sample_counts else None),
                'Execution Time': format_metric(actual_execution, predicted_execution, 
                                            sample_counts.for_execution_time if sample_counts else None),
                'Download Time': format_metric(actual_download, predicted_download, 
                                           sample_counts.for_download_speed if sample_counts else None),
                'Upload Time': format_metric(actual_upload, predicted_upload, 
                                         sample_counts.for_upload_speed if sample_counts else None),
                '_sample_count': sample_counts.for_execution_time if sample_counts else 0,
            })

# ... (rest of the code remains the same)

        if instance_data:
            # Create a DataFrame for the table
            df_instances = pd.DataFrame(instance_data)
            
            # Sort by sample count in descending order
            df_instances = df_instances.sort_values('_sample_count', ascending=False)
            
            # Remove the temporary sample count column before display
            df_instances = df_instances.drop(columns=['_sample_count'])
            
            # Reorder columns to put Master DAG ID first
            columns = ['Master DAG ID'] + [col for col in df_instances.columns if col != 'Master DAG ID']
            df_instances = df_instances[columns]
            
            # Display the table with better formatting
            st.dataframe(
                df_instances,
                column_config={
                    'Workflow Type': "Workflow Type",
                    'Makespan': "Makespan (Predicted → Actual)",
                    'Execution Time': "Execution Time (Predicted → Actual)",
                    'Download Time': "Download Time (Predicted → Actual)",
                    'Upload Time': "Upload Time (Predicted → Actual)",
                },
                use_container_width=True,
                height=min(400, 35 * (len(df_instances) + 1)),  # Dynamic height based on number of rows
                hide_index=True,
                column_order=[
                    'Workflow Type', 
                    'Planner',
                    'Master DAG ID',
                    'Makespan', 
                    'Execution Time', 
                    'Download Time', 
                    'Upload Time'
                ]
            )
            
            # Add a download button for the data
            csv = df_instances.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name=f"{selected_workflow}_{selected_planner}_comparison.csv",
                mime='text/csv',
            )
        else:
            st.warning("No instance data available for the selected filters.")
    
if __name__ == "__main__":
    main()