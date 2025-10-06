import math
import plotly.subplots as sp
import plotly.graph_objects as go
import streamlit as st
import redis
import cloudpickle
from typing import Dict, List
import pandas as pd
import numpy as np
import plotly.express as px
import hashlib
import colorsys
from dataclasses import dataclass
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage.metadata.metadata_storage import MetadataStorage
from src.storage.metadata.metrics_types import TaskMetrics, UserDAGSubmissionMetrics
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.storage.prefixes import DAG_PREFIX
from src.dag.dag import FullDAG
from src.storage.metadata.metrics_types import FullDAGPrepareTime, WorkerStartupMetrics, DAGResourceUsageMetrics
from src.utils.timer import Timer
from src.planning.optimizations.preload import PreLoadOptimization
from src.planning.optimizations.taskdup import TaskDupOptimization
from src.planning.optimizations.prewarm import PreWarmOptimization

def get_redis_connection(port: int = 6379):
    return redis.Redis(
        host='localhost',
        port=port,
        password='redisdevpwd123',
        decode_responses=False
    )

@dataclass
class WorkflowInstanceTaskInfo:
    global_task_id: str
    internal_task_id: str
    metrics: TaskMetrics
    input_size_downloaded_bytes: int
    output_size_uploaded_bytes: int
    
    optimization_preloads_done: int
    optimization_task_dups_done: int
    optimization_prewarms_done: int

@dataclass
class WorkflowInstanceInfo:
    master_dag_id: str
    plan: AbstractDAGPlanner.PlanOutput | None
    dag: FullDAG
    dag_download_stats: List[FullDAGPrepareTime]
    start_time_ms: float
    total_worker_startup_time_ms: float
    total_workers: int
    tasks: List[WorkflowInstanceTaskInfo]
    resource_usage: DAGResourceUsageMetrics
    resource_usage_cost: float
    total_transferred_data_bytes: int
    total_inputs_downloaded_bytes: float
    total_outputs_uploaded_bytes: float
    warm_starts_count: int
    cold_starts_count: int

@dataclass
class WorkflowInfo:
    type: str
    representative_dag: FullDAG
    instances: List[WorkflowInstanceInfo]


def get_workflows_information(metadata_storage_conn: redis.Redis) -> tuple[List[WorkerStartupMetrics], Dict[str, WorkflowInfo]]:
    workflow_types: Dict[str, WorkflowInfo] = {
        "All": WorkflowInfo("All", None, []) # type: ignore
    }
    worker_startup_metrics: List[WorkerStartupMetrics] = []

    def scan_keys(conn, pattern: str):
        """Efficiently scan Redis keys matching pattern."""
        cursor = 0
        keys = []
        while True:
            cursor, batch = conn.scan(cursor=cursor, match=pattern, count=1000)
            keys.extend(batch)
            if cursor == 0:
                break
        return keys

    try:
        # Scan DAG keys instead of using keys()
        all_dag_keys = scan_keys(metadata_storage_conn, f"{DAG_PREFIX}*")
        print(f"Found {len(all_dag_keys)} workflow instances")
        from typing import Any

        for dag_key in all_dag_keys:
            try:
                dag_data = metadata_storage_conn.get(dag_key)
                if not dag_data: continue
                dag: FullDAG = cloudpickle.loads(dag_data)  # type: ignore

                # Plan output
                plan_key = f"{MetadataStorage.PLAN_KEY_PREFIX}{dag.master_dag_id}"
                plan_data: Any = metadata_storage_conn.get(plan_key)
                plan_output: AbstractDAGPlanner.PlanOutput | None = cloudpickle.loads(plan_data) if plan_data else None
                if plan_output is not None and plan_output.nodes_info:
                    predicted_makespan_s = plan_output.nodes_info[dag.sink_node.id.get_full_id()].task_completion_time_ms / 1000
                    if predicted_makespan_s > 500:
                        print(f"Discard workflow with predicted makespan of {predicted_makespan_s}")
                        continue

                # DAG download stats
                download_keys_pattern = f"{MetadataStorage.DAG_MD_KEY_PREFIX}{dag.master_dag_id}*"
                download_keys = scan_keys(metadata_storage_conn, download_keys_pattern)
                download_data: Any = metadata_storage_conn.mget(download_keys) if download_keys else []
                dag_download_stats: List[FullDAGPrepareTime] = [cloudpickle.loads(d) for d in download_data]

                # Tasks data
                task_keys = [
                    f"{MetadataStorage.TASK_MD_KEY_PREFIX}{t.id.get_full_id_in_dag(dag)}"
                    for t in dag._all_nodes.values()
                ]
                tasks_data: Any = metadata_storage_conn.mget(task_keys) if task_keys else []
                tasks: List[WorkflowInstanceTaskInfo] = [
                    WorkflowInstanceTaskInfo(
                        t.id.get_full_id_in_dag(dag), 
                        t.id.get_full_id(), 
                        cloudpickle.loads(td),
                        -1,
                        -1,
                        0,
                        0,
                        0
                    )
                    for t, td in zip(dag._all_nodes.values(), tasks_data) if td
                ]
                total_inputs_downloaded = 0
                total_outputs_uploaded = 0
                for task in tasks:
                    tm = task.metrics
                    task.input_size_downloaded_bytes = sum([m.serialized_size_bytes for m in tm.input_metrics.input_download_metrics.values() if m.time_ms is not None])
                    task.output_size_uploaded_bytes = tm.output_metrics.serialized_size_bytes if tm.output_metrics.tp_time_ms is not None else 0
                    total_inputs_downloaded += task.input_size_downloaded_bytes
                    total_outputs_uploaded += task.output_size_uploaded_bytes

                    if tm.optimization_metrics:
                        task.optimization_preloads_done = len([om for om in tm.optimization_metrics if isinstance(om, PreLoadOptimization.OptimizationMetrics)])
                        task.optimization_task_dups_done = len([om for om in tm.optimization_metrics if isinstance(om, TaskDupOptimization.OptimizationMetrics)])
                        task.optimization_prewarms_done = len([om for om in tm.optimization_metrics if isinstance(om, PreWarmOptimization.OptimizationMetrics)])
                    
                # DAG submission metrics
                submission_key = f"{MetadataStorage.USER_DAG_SUBMISSION_PREFIX}{dag.master_dag_id}"
                submission_data: Any = metadata_storage_conn.get(submission_key)
                if not submission_data: continue
                dag_submission_metrics: UserDAGSubmissionMetrics = cloudpickle.loads(submission_data)

                # Worker startup metrics
                worker_keys_pattern = f"{MetadataStorage.WORKER_STARTUP_PREFIX}{dag.master_dag_id}*"
                worker_keys = scan_keys(metadata_storage_conn, worker_keys_pattern)
                worker_data: Any = metadata_storage_conn.mget(worker_keys)
                this_workflow_wsm: List[WorkerStartupMetrics] = [cloudpickle.loads(d) for d in worker_data]
                worker_startup_metrics.extend(this_workflow_wsm)

                #DEBUG: Understand effectiveness of prewarm
                for task in tasks:
                    for om in task.metrics.optimization_metrics:
                        if not isinstance(om, PreWarmOptimization.OptimizationMetrics): continue
                        target_worker_id = om.resource_config.worker_id
                        worker_startup_metrics = [m for m in this_workflow_wsm if m.resource_configuration.worker_id == target_worker_id]
                        if not len(worker_startup_metrics): continue
                        this_worker_startup_metrics = worker_startup_metrics[0]
                        print(f"worker_start_time-prewarm happened at: {(this_worker_startup_metrics.start_time_ms / 1000) - om.absolute_trigger_timestamp_s} | Worker state: {this_worker_startup_metrics.state}")

                #DEBUG
                if plan_output and "opt" in plan_output.planner_name:
                        print(f"Planner name: {plan_output.planner_name} | Workflow name: {dag.dag_name} | Dups: {sum([t.optimization_task_dups_done for t in tasks])} | Preloads: {sum([t.optimization_preloads_done for t in tasks])} | Prewarms: {sum([t.optimization_prewarms_done for t in tasks])}")

                warm_starts_count = len([m for m in this_workflow_wsm if m.state == "warm"])
                cold_starts_count = len([m for m in this_workflow_wsm if m.state == "cold"])

                total_worker_startup_time_ms = 0
                for metric in this_workflow_wsm:
                    if not metric.end_time_ms: continue
                    total_worker_startup_time_ms += metric.end_time_ms - metric.start_time_ms

                total_workers = len(this_workflow_wsm)

                # Resource usage metrics
                resource_usage_key = f"{MetadataStorage.DAG_RESOURCE_USAGE_PREFIX}{dag.master_dag_id}"
                resource_usage_data: Any = metadata_storage_conn.get(resource_usage_key)
                resource_usage: DAGResourceUsageMetrics = cloudpickle.loads(resource_usage_data)

                total_transferred_data_bytes = total_inputs_downloaded + total_outputs_uploaded

                # Update workflow_types dict
                if dag.dag_name not in workflow_types:
                    workflow_types[dag.dag_name] = WorkflowInfo(dag.dag_name, dag, [])
                instance_info = WorkflowInstanceInfo(
                        dag.master_dag_id,
                        plan_output,
                        dag,
                        dag_download_stats,
                        dag_submission_metrics.dag_submission_time_ms,
                        total_worker_startup_time_ms,
                        total_workers,
                        tasks,
                        resource_usage,
                        resource_usage.cost,
                        total_transferred_data_bytes,
                        total_inputs_downloaded,
                        total_outputs_uploaded,
                        warm_starts_count,
                        cold_starts_count
                    )
                workflow_types[dag.dag_name].instances.append(instance_info)
                workflow_types["All"].instances.append(instance_info)
            except Exception as e:
                print(f"Error processing DAG {dag_key}: {e}")
                raise e
    except Exception as e:
        print(f"Error accessing Redis: {e}")
        raise e

    return worker_startup_metrics, workflow_types

def format_bytes(size: float) -> tuple[float, str, str]:
    """Convert bytes to human-readable format"""
    # returns [value, unit, formatted_string]
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return size, unit, f"{size:.2f} {unit}"
        size /= 1024.0
    return size, 'TB', f"{size:.2f} TB"

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
    st.title("Planning Analysis Dashboard")
    
    # Connect to both Redis instances
    metadata_storage_conn = get_redis_connection(6380)
    
    # Initialize workflow types in session state if not already loaded
    if 'workflow_types' not in st.session_state:
        timer = Timer()
        st.session_state.worker_startup_metrics, st.session_state.workflow_types = get_workflows_information(metadata_storage_conn)
        print(f"Time to load workflow information: {(timer.stop() / 1_000):.2f} s")
    
    workflow_types = st.session_state.workflow_types
    
    if not workflow_types:
        st.warning("No DAGs found in Redis")
        st.stop()
    
    # Sidebar for workflow type selection
    st.sidebar.title("Workflow Filter")
    
    # Create a dropdown to select workflow type
    selected_workflow = st.sidebar.selectbox(
        "Select Workflow Type",
        options=sorted(list(workflow_types.keys())),
        index=0
    )
    
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
    
    st.header(selected_workflow)

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Workflow Instances", len(workflow_types[selected_workflow].instances))
    if selected_workflow != 'All':
        with col2:
            st.metric("Workflow Tasks", len(workflow_types[selected_workflow].representative_dag._all_nodes))
    
    instance_data = []
    for idx, instance in enumerate(workflow_types[selected_workflow].instances):
        if not instance.plan or not instance.tasks:
            continue
            
        # Calculate actual metrics
        actual_total_download = sum([sum([input_metric.time_ms / 1000 for input_metric in task.metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms is not None]) for task in instance.tasks])
        actual_execution = sum(task.metrics.tp_execution_time_ms / 1000 for task in instance.tasks)  # in seconds
        actual_total_upload = sum(task.metrics.output_metrics.tp_time_ms / 1000 for task in instance.tasks if task.metrics.output_metrics.tp_time_ms is not None)  # in seconds
        actual_invocation = sum(task.metrics.total_invocation_time_ms / 1000 for task in instance.tasks if task.metrics.total_invocation_time_ms is not None)  # in seconds
        actual_dependency_update = sum(task.metrics.update_dependency_counters_time_ms / 1000 for task in instance.tasks if task.metrics.update_dependency_counters_time_ms is not None)  # in seconds
        actual_input_size = sum([sum([input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values()]) + task.metrics.input_metrics.hardcoded_input_size_bytes for task in instance.tasks])  # in bytes
        actual_output_size = sum([task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks])  # in bytes
        actual_total_worker_startup_time_s = instance.total_worker_startup_time_ms / 1000  # in seconds
        
        # Calculate actual makespan
        from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
        common_resources: TaskWorkerResourceConfiguration | None = None
        for task in instance.tasks:
            if common_resources is None: common_resources = task.metrics.worker_resource_configuration
            elif common_resources.cpus != task.metrics.worker_resource_configuration.cpus or common_resources.memory_mb != task.metrics.worker_resource_configuration.memory_mb: 
                # print(f"Found a diff resource config. Prev: {common_resources} New: {task.metrics.worker_resource_configuration}")
                common_resources = None
        
        sink_task_metrics = [t for t in instance.tasks if t.internal_task_id == instance.dag.sink_node.id.get_full_id()][0].metrics
        sink_task_ended_timestamp_ms = (sink_task_metrics.started_at_timestamp_s * 1000) + (sink_task_metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + (sink_task_metrics.tp_execution_time_ms or 0) + (sink_task_metrics.output_metrics.tp_time_ms or 0) + (sink_task_metrics.total_invocation_time_ms or 0)
        actual_makespan_s = (sink_task_ended_timestamp_ms - instance.start_time_ms) / 1000
        actual_data_transferred = instance.total_transferred_data_bytes
        actual_total_downloadable_input_size_bytes = instance.total_inputs_downloaded_bytes
        actual_total_uploadable_output_size_bytes = instance.total_outputs_uploaded_bytes

        # Get predicted metrics if available
        predicted_total_downloadable_input_size_bytes = predicted_total_download = predicted_execution = predicted_total_upload = predicted_makespan_s = 0
        predicted_total_uploadable_output_size_bytes = predicted_input_size_bytes = predicted_output_size = predicted_total_worker_startup_time_s = 0
        if instance.plan and instance.plan.nodes_info:
            predicted_total_download = sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values())  # in seconds
            predicted_execution = sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values())  # in seconds
            predicted_total_upload = sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values())  # in seconds
            predicted_input_size_bytes = sum(info.serialized_input_size for info in instance.plan.nodes_info.values())  # in bytes
            predicted_output_size = sum(info.serialized_output_size for info in instance.plan.nodes_info.values())  # in bytes
            predicted_makespan_s = instance.plan.nodes_info[instance.dag.sink_node.id.get_full_id()].task_completion_time_ms / 1000
            workers_accounted_for = set()
            predicted_total_worker_startup_time_s = 0
            for info in instance.plan.nodes_info.values():
                if info.node_ref.worker_config.worker_id is None or info.node_ref.worker_config.worker_id not in workers_accounted_for:
                    predicted_total_worker_startup_time_s += info.tp_worker_startup_time_ms / 1000
                    workers_accounted_for.add(info.node_ref.worker_config.worker_id)
            predicted_total_downloadable_input_size_bytes = instance.plan.total_downloadable_input_size_bytes
            predicted_total_uploadable_output_size_bytes = instance.plan.total_uploadable_output_size_bytes
        
        # Store actual values for SLA comparison
        instance_metrics = {
            'makespan': actual_makespan_s,
            'execution': actual_execution,
            'download': actual_total_download,
            'upload': actual_total_upload,
            'input_size': actual_input_size,
            'output_size': actual_output_size,
            'worker_startup': actual_total_worker_startup_time_s
        }
        
        def convert_bytes_to_GB(bytes):
            return bytes / (1024 * 1024 * 1024)

        # Calculate differences and percentages with sample counts
        def format_metric(actual, predicted, metric_name=None, samples=None):
            # Store the actual value for SLA comparison
            if metric_name is not None:
                instance_metrics[metric_name] = actual
            
            # Format the predicted vs actual comparison
            if predicted == 0 and actual == 0:
                comparison = "0.000s (0.000%)"
            else:
                diff = actual - predicted
                pct_diff = (diff / predicted * 100) if predicted != 0 else float('inf')
                sign = "+" if diff >= 0 else ""
                comparison = f"{predicted:.3f}s → {actual:.3f}s ({sign}{diff:.3f}s, {sign}{pct_diff:.1f}%)"
            
            # Add sample count if provided
            if samples is not None:
                comparison = f"{comparison}\n(samples: {samples})"
            
            return comparison
        
        # Get sample counts if available
        sample_counts = instance.plan.prediction_sample_counts if instance.plan and hasattr(instance.plan, 'prediction_sample_counts') else None
        
        def format_size_metric(actual, predicted, metric_name=None, samples=None):
            # Store the actual value for SLA comparison
            if metric_name is not None:
                instance_metrics[metric_name] = actual
                
            formatted_actual = format_bytes(actual)[2]
            formatted_predicted = format_bytes(predicted)[2]
            
            # Format the predicted vs actual comparison
            if predicted == 0 and actual == 0:
                comparison = f"{formatted_predicted} → {formatted_actual} (0.0%)"
            else:
                diff = actual - predicted
                pct_diff = (diff / predicted * 100) if predicted != 0 else float('inf')
                sign = "+" if diff >= 0 else ""
                comparison = f"{formatted_predicted} → {formatted_actual} ({sign}{pct_diff:.1f}%)"
            
            # Add sample count if provided
            if samples is not None:
                comparison = f"{comparison}\n(samples: {samples})"
                
            return comparison
            
        # Format SLA for display and store for later use
        sla_percentile = None
        sla_value = 'N/A'
        if instance.plan:
            if instance.plan.sla == 'average':
                sla_value = 'average'
                sla_percentile = None
            else:
                sla_value = f'p{instance.plan.sla.value}'
                sla_percentile = instance.plan.sla.value
        
        # Calculate total DAG download time across all downloads
        # total_download_time = sum(stat.download_time_ms for stat in instance.dag_download_stats)
        # dag_download_time = f"{total_download_time / 1000:.3f}s"

        unique_worker_ids = set()
        for task_metrics in instance.tasks:
            unique_worker_ids.add(task_metrics.metrics.worker_resource_configuration.worker_id)
        

        is_wukong_instance = 'wukong' in instance.plan.planner_name.lower()  if instance.plan else None
        # Store the instance data with metrics for SLA comparison
        instance_data.append({
            'Workflow Type': selected_workflow,
            'Planner': instance.plan.planner_name if instance.plan else 'N/A',
            'Resources': f"{common_resources.cpus} CPUs {common_resources.memory_mb} MB" if common_resources else 'Non-Uniform',
            'SLA': sla_value,
            '_sla_percentile': sla_percentile,
            'Master DAG ID': instance.master_dag_id,
            'Makespan': format_metric(actual_makespan_s, 0 if is_wukong_instance else predicted_makespan_s, 'makespan',
                                sample_counts.for_execution_time if sample_counts else None),
            'Total Execution Time': format_metric(actual_execution, 0 if is_wukong_instance else predicted_execution, 'execution',
                                        sample_counts.for_execution_time if sample_counts else None),
            'Total Downloaded Data': f"{0 if is_wukong_instance else format_bytes(predicted_total_downloadable_input_size_bytes)[2]} -> {format_bytes(actual_total_downloadable_input_size_bytes)[2]}",
            'Total Download Time': format_metric(actual_total_download, 0 if is_wukong_instance else predicted_total_download, 'download',
                                    sample_counts.for_download_speed if sample_counts else None),
            'Total Uploaded Data': f"{0 if is_wukong_instance else format_bytes(predicted_total_uploadable_output_size_bytes)[2]} -> {format_bytes(actual_total_uploadable_output_size_bytes)[2]}",
            'Total Upload Time': format_metric(actual_total_upload, 0 if is_wukong_instance else predicted_total_upload, 'upload',
                                    sample_counts.for_upload_speed if sample_counts else None),
            'Total Input Size': format_size_metric(actual_input_size, 0 if is_wukong_instance else predicted_input_size_bytes, 'input_size',
                                    sample_counts.for_output_size if sample_counts else None),
            'Total Output Size': format_size_metric(actual_output_size, 0 if is_wukong_instance else predicted_output_size, 'output_size',
                                    sample_counts.for_output_size if sample_counts else None),
            'Total Data Transferred': format_bytes(actual_data_transferred)[2],
            'Total Task Invocation Time': f"{actual_invocation:.2f}s",
            'Total Dependency Counter Update Time': f"{actual_dependency_update:.2f}s",
            # 'Total DAG Download Time': dag_download_time,
            'Total Worker Startup Time': format_metric(actual_total_worker_startup_time_s, 0 if is_wukong_instance else predicted_total_worker_startup_time_s, 'worker_startup') + f" (Workers: {len(unique_worker_ids)})",
            'Run Time': f"{instance.resource_usage.run_time_seconds:.2f}",
            'CPU Time': f"{instance.resource_usage.cpu_seconds:.2f}",
            'Memory Usage': f"{convert_bytes_to_GB(instance.resource_usage.memory_bytes):.2f} GB",
            'Resources Cost': f"{instance.resource_usage_cost:.2f}",
            'Warm/Cold Starts': f"{instance.warm_starts_count}/{instance.cold_starts_count}",
            '_actual_worker_startup': actual_total_worker_startup_time_s,
            '_actual_invocation': actual_invocation,
            '_actual_dependency_update': actual_dependency_update,
            '_sample_count': sample_counts.for_execution_time if sample_counts else 0,
            '_metrics': instance_metrics
        })

    if instance_data:
        # Create a DataFrame for the table
        df_instances = pd.DataFrame(instance_data)
        
        # Ensure we're working with a pandas DataFrame
        if not isinstance(df_instances, pd.DataFrame):
            df_instances = pd.DataFrame(df_instances)
        
        # Calculate SLA metrics across all instances for each metric type
        if len(df_instances) > 0 and '_metrics' in df_instances.columns:
            # Get all metrics from all instances
            all_metrics = {
                'makespan': [],
                'execution': [],
                'download': [],
                'upload': [],
                'input_size': [],
                'output_size': [],
                'worker_startup': []
            }
            
            # Collect all metrics
            for _, row in df_instances.iterrows():
                for metric, value in row['_metrics'].items():
                    all_metrics[metric].append(value)
            
            # Sort instances by start time to ensure we're looking at previous instances correctly
            if not df_instances.empty and 'Master DAG ID' in df_instances.columns:
                # Create a list to store all previous metrics for each metric type
                all_previous_metrics = {
                    'makespan': [],
                    'execution': [],
                    'download': [],
                    'upload': [],
                    'input_size': [],
                    'output_size': [],
                    'worker_startup': []
                }
                
                # Create a new column to store the formatted values
                formatted_values = {col: [None] * len(df_instances) for col in [
                    'Makespan', 'Total Execution Time', 'Total Download Time',
                    'Total Upload Time', 'Total Input Size', 'Total Output Size',
                    'Total Worker Startup Time'
                ]}
                
                # Process each instance in order
                for idx, row in df_instances.iterrows():
                    metrics = row['_metrics']
                    sla_pct = row['_sla_percentile']
                    sla_value = row['SLA']
                    
                    # Helper function to format SLA comparison for this instance
                    def format_with_sla(metric_name: str, value: float, unit: str = 's', current_metrics: Dict[str, float] = None) -> str:
                        try:
                            if not current_metrics or metric_name not in current_metrics or pd.isna(current_metrics[metric_name]):
                                return f"{value:.3f}{unit}"
                            
                            sla = float(current_metrics[metric_name])
                            offset = value - sla
                            sign = "+" if offset >= 0 else ""
                            meets_sla = value <= sla
                            emoji = "✅" if meets_sla else "❌"
                            
                            # Get the current cell value (contains the predicted vs actual comparison)
                            current_value = str(df_instances.at[idx, col_name])
                            
                            # Format the SLA information
                            if unit == 's':
                                sla_info = f"{emoji} SLA: {sla:.3f}s (offset: {sign}{abs(offset):.3f}s)"
                            else:  # for sizes
                                sla_info = f"{emoji} SLA: {format_bytes(sla)[2]} (offset: {sign}{format_bytes(abs(offset))[2]})"
                            
                            return f"{current_value}\n{sla_info}"
                        except Exception as e:
                            print(f"Error formatting SLA for {metric_name}: {e}")
                            return str(value)
                    
                    # Calculate SLA based on previous instances
                    current_sla_metrics = {}
                    if sla_pct is not None and all_previous_metrics['makespan']:  # Only calculate if we have previous data
                        for metric in all_previous_metrics.keys():
                            if not all_previous_metrics[metric]:
                                continue
                                
                            try:
                                if sla_value == "average":
                                    sla_value = float(np.average(all_previous_metrics[metric]))
                                else:  # specific percentile
                                    sla_value = float(np.percentile(all_previous_metrics[metric], sla_pct))
                                current_sla_metrics[metric] = sla_value
                            except (TypeError, ValueError) as e:
                                print(f"Error calculating SLA for {metric}: {e}")
                    
                    # Update each metric with SLA comparison if we have SLA data
                    metric_columns = {
                        'Makespan': ('makespan', 's'),
                        'Total Execution Time': ('execution', 's'),
                        'Total Download Time': ('download', 's'),
                        'Total Upload Time': ('upload', 's'),
                        'Total Input Size': ('input_size', 'b'),
                        'Total Output Size': ('output_size', 'b'),
                        'Total Worker Startup Time': ('worker_startup', 's')
                    }
                    
                    for col_name, (metric_name, unit) in metric_columns.items():
                        if col_name in df_instances.columns and metric_name in metrics:
                            try:
                                metric_value = float(metrics[metric_name])
                                
                                if current_sla_metrics:  # Only add SLA if we have previous data to compare with
                                    formatted = format_with_sla(metric_name, metric_value, unit, current_sla_metrics)
                                else:
                                    formatted = str(df_instances.at[idx, col_name])
                                    
                                if formatted is not None:
                                    formatted_values[col_name][idx] = formatted
                            except (ValueError, TypeError) as e:
                                print(f"Error processing {metric_name}: {e}")
                                formatted_values[col_name][idx] = str(metrics.get(metric_name, 'N/A'))
                    
                    # Add current instance's metrics to the history for the next iteration
                    for metric_name in all_previous_metrics.keys():
                        if metric_name in metrics:
                            all_previous_metrics[metric_name].append(float(metrics[metric_name]))
                
                # Update the dataframe with the formatted values
                for col_name, values in formatted_values.items():
                    if col_name in df_instances.columns:
                        df_instances[col_name] = values
        
        # Sort by sample count in descending order
        if '_sample_count' in df_instances.columns:
            df_instances = df_instances.sort_values('_sample_count', ascending=False)
        
        # Remove the temporary columns before display
        cols_to_drop = [col for col in [
            '_sample_count', 
            '_actual_invocation', 
            '_actual_dependency_update',
            '_actual_worker_startup',
            '_metrics',
            '_sla_percentile'
        ] if col in df_instances.columns]
        
        if cols_to_drop:
            df_instances = df_instances.drop(columns=cols_to_drop)
        
        # Reorder columns to put Master DAG ID first
        if 'Master DAG ID' in df_instances.columns:
            columns = ['Master DAG ID'] + [col for col in df_instances.columns if col != 'Master DAG ID']
            df_instances = df_instances[columns]

        TAB_RAW, TAB_PREDICTIONS, TAB_ACTUAL_VALUES = st.tabs(["Raw", "Predictions", "Actual Values"])

        with TAB_RAW:
            # Add planner filter dropdown
            if isinstance(df_instances, pd.DataFrame) and 'Planner' in df_instances.columns:
                # Get unique planners using a method that works with both pandas and numpy arrays
                planner_values = df_instances['Planner'].values if hasattr(df_instances['Planner'], 'values') else df_instances['Planner']
                if hasattr(planner_values, 'tolist'):
                    planner_values = planner_values.tolist()
                
                # Convert to a set to get unique values, then back to a list
                unique_planners = list(set(str(p) for p in planner_values if pd.notna(p) and p is not None))
                all_planners = ['All'] + sorted(unique_planners)
                
                selected_planner = st.selectbox(
                    'Filter by Planner:',
                    all_planners,
                    index=0
                )
                
                # Filter by selected planner if not 'All'
                if selected_planner != 'All':
                    df_instances = df_instances[df_instances['Planner'].astype(str) == selected_planner]
            
            # Display the instance comparison table with row count
            st.markdown(f"### Instance Comparison ({len(df_instances)} instances)")
            
            # Display the table
            st.dataframe(
                df_instances,
                column_config={
                    'Workflow Type': "Workflow Type",
                    'Resources': "Resources",
                    'SLA': "SLA",
                    'Makespan': "Makespan (Predicted → Actual)",
                    'Total Execution Time': "Total Execution Time (Predicted → Actual)",
                    'Total Downloaded Data': "Total Downloaded Data (Predicted → Actual)",
                    'Total Download Time': "Total Download Time (Predicted → Actual)",
                    'Total Uploaded Data': "Total Uploaded Data (Predicted → Actual)",
                    'Total Upload Time': "Total Upload Time (Predicted → Actual)",
                    'Total Input Size': "Total Input Size (Predicted → Actual)",
                    'Total Output Size': "Total Output Size (Predicted → Actual)",
                    'Total Data Transferred': "Total Data Transferred",
                    'Total Task Invocation Time': "Total Task Invocation Time",
                    'Total Dependency Counter Update Time': "Total Dependency Counter Update Time",
                    # 'Total DAG Download Time': "Total DAG Download Time",
                    'Total Worker Startup Time': "Total Worker Startup Time (Predicted → Actual)",
                    'Run Time': "Run Time",
                    'CPU Time': "CPU Time",
                    'Memory Usage': "Memory Usage",
                    'Resources Cost': "Resources Cost",
                    'Warm/Cold Starts': "Warm/Cold Starts",
                },
                use_container_width=True,
                height=min(400, 35 * (len(df_instances) + 1)),
                hide_index=True,
                column_order=[
                    'Workflow Type', 
                    'Planner',
                    'Resources',
                    'SLA',
                    'Master DAG ID',
                    'Makespan', 
                    'Total Execution Time', 
                    'Total Downloaded Data',
                    'Total Download Time',
                    'Total Uploaded Data',
                    'Total Upload Time',
                    'Total Input Size',
                    'Total Output Size',
                    'Total Data Transferred',
                    'Total Task Invocation Time',
                    'Total Dependency Counter Update Time',
                    # 'Total DAG Download Time',
                    'Total Worker Startup Time',
                    'Run Time',
                    'CPU Time',
                    'Memory Usage',
                    'Resources Cost',
                    'Warm/Cold Starts',
                ]
            )

        # st.markdown("---")
        with TAB_PREDICTIONS:
            # Add comparison bar chart for predicted vs actual metrics
            st.markdown("### Reality vs Predictions")
            
            # Calculate averages for the comparison
            metrics_data = []
            for instance in workflow_types[selected_workflow].instances:
                if not instance.plan or not instance.tasks:
                    continue
                if 'wukong' in instance.plan.planner_name.lower():
                    continue
                    
                # Calculate actual metrics
                actual_makespan_s = (
                        max([
                            (task.metrics.started_at_timestamp_s * 1000) + (task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + (task.metrics.tp_execution_time_ms or 0) + (task.metrics.output_metrics.tp_time_ms or 0) + (task.metrics.total_invocation_time_ms or 0) for task in instance.tasks
                        ]) - instance.start_time_ms
                    ) / 1000
                actual_execution = sum(task.metrics.tp_execution_time_ms / 1000 for task in instance.tasks)
                actual_total_download = sum([sum([input_metric.time_ms / 1000 for input_metric in task.metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms is not None]) for task in instance.tasks])
                actual_total_upload = sum(task.metrics.output_metrics.tp_time_ms / 1000 for task in instance.tasks if task.metrics.output_metrics.tp_time_ms is not None)
                actual_input_size = sum([sum([input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values()]) + task.metrics.input_metrics.hardcoded_input_size_bytes for task in instance.tasks])
                actual_output_size = sum([task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks])
                actual_worker_startup_time_s = sum([metric.end_time_ms - metric.start_time_ms for metric in st.session_state.worker_startup_metrics if metric.master_dag_id == instance.master_dag_id and metric.end_time_ms is not None])

                # Get predicted metrics if available
                predicted_makespan_s = predicted_execution = predicted_total_download = predicted_total_upload = predicted_input_size_bytes = predicted_output_size = predicted_worker_startup_time_s = 0 # initialize them outside
                if instance.plan and instance.plan.nodes_info:
                    predicted_makespan_s = instance.plan.nodes_info[instance.dag.sink_node.id.get_full_id()].task_completion_time_ms / 1000
                    predicted_total_download = sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values())
                    predicted_execution = sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values())
                    predicted_total_upload = sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values())
                    predicted_input_size_bytes = sum(info.serialized_input_size for info in instance.plan.nodes_info.values())
                    predicted_output_size = sum(info.serialized_output_size for info in instance.plan.nodes_info.values())
                    workers_accounted_for = set()
                    predicted_worker_startup_time_s = 0
                    for info in instance.plan.nodes_info.values():
                        if info.node_ref.worker_config.worker_id is None or info.node_ref.worker_config.worker_id not in workers_accounted_for:
                            predicted_worker_startup_time_s += info.tp_worker_startup_time_ms / 1000
                            workers_accounted_for.add(info.node_ref.worker_config.worker_id)
                
                metrics_data.append({
                    'makespan_actual': actual_makespan_s,
                    'makespan_predicted': predicted_makespan_s,
                    'execution_actual': actual_execution,
                    'execution_predicted': predicted_execution,
                    'download_actual': actual_total_download,
                    'download_predicted': predicted_total_download,
                    'upload_actual': actual_total_upload,
                    'upload_predicted': predicted_total_upload,
                    'input_size_actual': actual_input_size,
                    'input_size_predicted': predicted_input_size_bytes,
                    'output_size_actual': actual_output_size,
                    'output_size_predicted': predicted_output_size,
                    'worker_startup_time_actual': actual_worker_startup_time_s,
                    'worker_startup_time_predicted': predicted_worker_startup_time_s,
                })
            
            if metrics_data:
                # Group metrics by planner
                planner_metrics = {}
                instances_clone = workflow_types[selected_workflow].instances.copy()
                for instance in instances_clone:
                    if not instance.plan or not instance.tasks:
                        continue
                    if 'wukong' in instance.plan.planner_name.lower():
                        continue
                        
                    # Get metrics for this instance
                    actual_makespan_s = (
                        max([
                            (task.metrics.started_at_timestamp_s * 1000) + 
                            (task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + 
                            (task.metrics.tp_execution_time_ms or 0) + 
                            (task.metrics.output_metrics.tp_time_ms or 0) + 
                            (task.metrics.total_invocation_time_ms or 0) 
                            for task in instance.tasks
                        ]) - instance.start_time_ms
                    ) / 1000
                    
                    actual_execution = sum(task.metrics.tp_execution_time_ms / 1000 for task in instance.tasks)
                    actual_total_download = sum([sum([input_metric.time_ms / 1000 for input_metric in task.metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms is not None]) for task in instance.tasks])
                    actual_total_upload = sum(task.metrics.output_metrics.tp_time_ms / 1000 for task in instance.tasks if task.metrics.output_metrics.tp_time_ms is not None)
                    actual_input_size = sum([sum([input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values()]) + task.metrics.input_metrics.hardcoded_input_size_bytes for task in instance.tasks])
                    actual_output_size = sum([task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks])
                    actual_worker_startup_time_s = sum([(metric.end_time_ms - metric.start_time_ms) / 1000 for metric in st.session_state.worker_startup_metrics if metric.master_dag_id == instance.master_dag_id and metric.end_time_ms is not None])

                    # Get predicted metrics
                    predicted_makespan_s = predicted_execution = predicted_total_download = predicted_total_upload = predicted_input_size_bytes = predicted_output_size = predicted_worker_startup_time_s = 0
                    if instance.plan and instance.plan.nodes_info:
                        predicted_makespan_s = instance.plan.nodes_info[instance.dag.sink_node.id.get_full_id()].task_completion_time_ms / 1000
                        predicted_total_download = sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values())
                        predicted_execution = sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values())
                        predicted_total_upload = sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values())
                        predicted_input_size_bytes = sum(info.serialized_input_size for info in instance.plan.nodes_info.values())
                        predicted_output_size = sum(info.serialized_output_size for info in instance.plan.nodes_info.values())
                        workers_accounted_for = set()
                        predicted_worker_startup_time_s = 0
                        for info in instance.plan.nodes_info.values():
                            if info.node_ref.worker_config.worker_id is None or info.node_ref.worker_config.worker_id not in workers_accounted_for:
                                predicted_worker_startup_time_s += info.tp_worker_startup_time_ms / 1000
                                workers_accounted_for.add(info.node_ref.worker_config.worker_id)
                    
                        planner_name = instance.plan.planner_name
                        if planner_name not in planner_metrics:
                            planner_metrics[planner_name] = []
                        planner_metrics[planner_name].append({
                            'makespan_actual': actual_makespan_s,
                            'makespan_predicted': predicted_makespan_s,
                            'execution_actual': actual_execution,
                            'execution_predicted': predicted_execution,
                            'download_actual': actual_total_download,
                            'download_predicted': predicted_total_download,
                            'upload_actual': actual_total_upload,
                            'upload_predicted': predicted_total_upload,
                            'input_size_actual': actual_input_size,
                            'input_size_predicted': predicted_input_size_bytes,
                            'output_size_actual': actual_output_size,
                            'output_size_predicted': predicted_output_size,
                            'worker_startup_time_actual': actual_worker_startup_time_s,
                            'worker_startup_time_predicted': predicted_worker_startup_time_s,
                        })

                # Calculate averages for each planner
                planner_median_metrics = {}
                for planner_name, planner_data in planner_metrics.items():
                    if not planner_data:
                        continue
                    if 'wukong' in planner_name.lower():
                        continue
                        
                    planner_median_metrics[planner_name] = {
                        'Makespan (s)': {
                            'actual': np.median([m['makespan_actual'] for m in planner_data]),
                            'predicted': np.median([m['makespan_predicted'] for m in planner_data])
                        },
                        'Execution Time (s)': {
                            'actual': np.median([m['execution_actual'] for m in planner_data]),
                            'predicted': np.median([m['execution_predicted'] for m in planner_data])
                        },
                        'Download Time (s)': {
                            'actual': np.median([m['download_actual'] for m in planner_data]),
                            'predicted': np.median([m['download_predicted'] for m in planner_data])
                        },
                        'Upload Time (s)': {
                            'actual': np.median([m['upload_actual'] for m in planner_data]),
                            'predicted': np.median([m['upload_predicted'] for m in planner_data])
                        },
                        'Input Size (bytes)': {
                            'actual': np.median([m['input_size_actual'] for m in planner_data]),
                            'predicted': np.median([m['input_size_predicted'] for m in planner_data])
                        },
                        'Output Size (bytes)': {
                            'actual': np.median([m['output_size_actual'] for m in planner_data]),
                            'predicted': np.median([m['output_size_predicted'] for m in planner_data])
                        },
                        'Worker Startup Time (s)': {
                            'actual': np.median([m['worker_startup_time_actual'] for m in planner_data]),
                            'predicted': np.median([m['worker_startup_time_predicted'] for m in planner_data])
                        },
                    }
                
                # Create a dropdown to select planner
                if planner_median_metrics:
                    plot_data = []
                    for planner_name, metrics in planner_median_metrics.items():
                        for metric_name, values in metrics.items():
                            plot_data.append({
                                'Planner': planner_name,
                                'Metric': metric_name,
                                'Type': 'Actual',
                                'Value': values['actual']
                            })
                            plot_data.append({
                                'Planner': planner_name,
                                'Metric': metric_name,
                                'Type': 'Predicted',
                                'Value': values['predicted']
                            })

                    df_plot = pd.DataFrame(plot_data)

                    # Create overlapped bars: use facet for metric, color for Actual/Predicted, x=Planner
                    fig = px.bar(
                        df_plot,
                        x='Planner',
                        y='Value',
                        color='Type',
                        facet_col='Metric',   # One small multiple per metric, horizontally
                        barmode='overlay',    # Overlap Actual & Predicted on top of each other
                        opacity=0.6,          # Slight transparency to see overlap
                        title='Comparison of Actual vs Predicted Metrics Across Planners',
                        color_discrete_map={'Actual': '#1f77b4', 'Predicted': '#ff7f0e'}
                    )

                    # Layout improvements
                    fig.update_layout(
                        height=600,
                        legend_title='',
                        plot_bgcolor='rgba(0,0,0,0)',
                        yaxis_type='log'  # keep log scale if still useful
                    )

                    # Rotate x labels for clarity
                    fig.update_xaxes(tickangle=-45)

                    # Add labels on top of bars
                    fig.update_traces(
                        texttemplate='%{y:.2f}',
                        textposition='outside',
                        textfont_size=9
                    )

                    st.plotly_chart(fig, use_container_width=True)
            
            # Add prediction accuracy evolution chart
            # st.markdown("### Prediction Accuracy Evolution")
            
            # Collect all data
            data = []

            for workflow_name, workflow in workflow_types.items():
                for instance in workflow.instances:
                    if not instance.plan or not instance.tasks:
                        continue
                    planner_name = instance.plan.planner_name.lower()
                    if 'wukong' in planner_name:
                        continue

                    # Actual metrics
                    actual_metrics = {
                        'makespan': (
                            max([
                                (task.metrics.started_at_timestamp_s * 1000) +
                                (task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) +
                                (task.metrics.tp_execution_time_ms or 0) +
                                (task.metrics.output_metrics.tp_time_ms or 0) +
                                (task.metrics.total_invocation_time_ms or 0)
                                for task in instance.tasks
                            ]) - instance.start_time_ms
                        ) / 1000,
                        'execution': sum(task.metrics.tp_execution_time_ms or 0 for task in instance.tasks) / 1000,
                        'download': sum(sum(input_metric.time_ms for input_metric in task.metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms is not None) for task in instance.tasks) / 1000,
                        'upload': sum(task.metrics.output_metrics.tp_time_ms or 0 for task in instance.tasks) / 1000,
                        'input_size': sum(sum(input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values()) + 
                                        (task.metrics.input_metrics.hardcoded_input_size_bytes or 0) for task in instance.tasks),
                        'output_size': sum(task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks if hasattr(task.metrics, 'output_metrics')),
                        'worker_startup_time': sum(
                            (metric.end_time_ms - metric.start_time_ms) / 1000 
                            for metric in st.session_state.worker_startup_metrics 
                            if metric.master_dag_id == instance.master_dag_id and metric.end_time_ms is not None
                        )
                    }

                    # Predicted metrics
                    predicted_metrics = {}
                    if instance.plan and instance.plan.nodes_info:
                        sink_node = instance.dag.sink_node.id.get_full_id()
                        workers_accounted_for = set()
                        predicted_worker_startup_time_s = 0
                        for info in instance.plan.nodes_info.values():
                            worker_id = info.node_ref.worker_config.worker_id
                            if worker_id is not None and worker_id not in workers_accounted_for:
                                predicted_worker_startup_time_s += info.tp_worker_startup_time_ms / 1000
                                workers_accounted_for.add(worker_id)
                        predicted_metrics = {
                            'makespan': instance.plan.nodes_info[sink_node].task_completion_time_ms / 1000,
                            'execution': sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                            'download': sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                            'upload': sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                            'input_size': sum(info.serialized_input_size for info in instance.plan.nodes_info.values()),
                            'output_size': sum(info.serialized_output_size for info in instance.plan.nodes_info.values()),
                            'worker_startup_time': predicted_worker_startup_time_s
                        }

                    # Append to data with relative error calculated
                    for metric in actual_metrics:
                        actual = actual_metrics[metric]
                        predicted = predicted_metrics[metric]
                        # Calculate absolute relative error: |predicted - actual| / actual * 100
                        relative_error = (abs(predicted - actual) / actual * 100) if actual != 0 else 0
                        
                        data.append({
                            'planner': planner_name,
                            'metric': metric,
                            'actual': actual,
                            'predicted': predicted,
                            'relative_error': relative_error
                        })

            # Create DataFrame and prepare for plotting
            df = pd.DataFrame(data)

            # Group by metric only, taking the median relative error across all planners
            error_summary = df.groupby('metric')['relative_error'].median().reset_index()

            # Define the metric order
            metric_order = ['makespan', 'execution', 'download', 'upload', 'input_size', 'output_size', 'worker_startup_time']
            # Filter and sort metrics in the defined order
            error_summary['metric'] = pd.Categorical(error_summary['metric'], categories=metric_order, ordered=True)
            error_summary = error_summary.sort_values('metric')

            # Create the bar chart
            fig = go.Figure()

            # Add a single bar series for all metrics
            fig.add_trace(go.Bar(
                x=error_summary['metric'],
                y=error_summary['relative_error'],
                text=[f'{val:.1f}%' for val in error_summary['relative_error']],
                textposition='auto',
                marker_color='steelblue'
            ))

            # Update layout
            fig.update_layout(
                title='Median Absolute Relative Error (%) by Metric',
                xaxis_title='Metric',
                yaxis_title='Median Absolute Relative Error (%)',
                height=500,
                showlegend=False
            )

            # Display the chart
            st.plotly_chart(fig, use_container_width=True)
            
        with TAB_ACTUAL_VALUES:
            # Prepare data for all metrics comparison
            metrics_data = []
            
            for instance in workflow_types[selected_workflow].instances:
                if not instance.plan or not instance.tasks: 
                    continue
                
                # Calculate all metrics for this instance
                sink_task_metrics = [t for t in instance.tasks if t.internal_task_id == instance.dag.sink_node.id.get_full_id()][0].metrics
                
                # Calculate makespan
                sink_task_ended_timestamp_ms = (sink_task_metrics.started_at_timestamp_s * 1000) + \
                                            (sink_task_metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + \
                                            (sink_task_metrics.tp_execution_time_ms or 0) + \
                                            (sink_task_metrics.output_metrics.tp_time_ms or 0) + \
                                            (sink_task_metrics.total_invocation_time_ms or 0)
                
                # Calculate all metrics
                # Calculate average memory allocation per task (in MB)
                total_memory_mb = sum(
                    task.metrics.worker_resource_configuration.memory_mb 
                    for task in instance.tasks
                )
                
                input_size_value = sum(task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks)
                input_size_value, input_size_unit, _ = format_bytes(input_size_value)
                output_size_value = sum(task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks)
                output_size_value, output_size_unit, _ = format_bytes(output_size_value)
                total_data_value = instance.total_transferred_data_bytes
                total_data_value, total_data_unit, _ = format_bytes(total_data_value)
                
                instance_metrics = {
                    'Makespan [s]': (sink_task_ended_timestamp_ms - instance.start_time_ms) / 1000,
                    'Execution Time [s]': sum(task.metrics.tp_execution_time_ms / 1000 for task in instance.tasks),
                    'Total Time Waiting for Inputs [s]': sum(
                        (task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) / 1000 
                        for task in instance.tasks
                    ),
                    'Download Time [s]': sum(
                        sum(input_metric.time_ms / 1000 
                            for input_metric in task.metrics.input_metrics.input_download_metrics.values() 
                            if input_metric.time_ms is not None)
                        for task in instance.tasks
                    ),
                    'Upload Time [s]': sum(
                        task.metrics.output_metrics.tp_time_ms / 1000
                        for task in instance.tasks
                        if task.metrics.output_metrics.tp_time_ms is not None
                    ),
                    # f'Input Size [{input_size_unit}]': input_size_value,  # Convert to MB
                    # f'Output Size [{output_size_unit}]': output_size_value,  # Convert to MB
                    f'Total Data Transferred': total_data_value,  # Convert to MB
                    'Worker Startup Time [s]': instance.total_worker_startup_time_ms / 1000,
                    'Resource Usage': instance.resource_usage_cost
                }
                
                # Add all metrics to the data list
                for metric_name, value in instance_metrics.items():
                    metrics_data.append({
                        'Metric': metric_name,
                        'Value': value,
                        'Planner': instance.plan.planner_name if instance.plan else 'No Planner',
                        'Instance ID': instance.master_dag_id.split('-')[0]
                    })
            
            st.markdown("### Metrics Comparison")
            if metrics_data:
                # Convert metrics_data to DataFrame
                df_metrics = pd.DataFrame(metrics_data)

                # Create a list of (df_for_metric, metric_name) for each metric
                all_metrics_to_plot = []
                for metric in df_metrics['Metric'].unique():
                    df_metric = df_metrics[df_metrics['Metric'] == metric]
                    all_metrics_to_plot.append((df_metric, metric))

                # Plot in 3 columns per row
                import math
                num_cols = 3
                num_rows = math.ceil(len(all_metrics_to_plot) / num_cols)

                for i in range(num_rows):
                    cols = st.columns(num_cols)
                    for j in range(num_cols):
                        idx = i * num_cols + j
                        if idx >= len(all_metrics_to_plot):
                            break
                        df_plot, metric_name = all_metrics_to_plot[idx]
                        if df_plot.empty:
                            cols[j].write(f"No data for {metric_name}")
                            continue

                        fig = px.box(
                            df_plot,
                            x='Planner',
                            y='Value',
                            color='Planner',
                            points="all",
                            hover_data=['Instance ID'],
                            title=f"{metric_name} by Planner",
                            category_orders={"Planner": sorted(df_plot['Planner'].unique())}
                        )
                        fig.update_layout(
                            plot_bgcolor='rgba(0,0,0,0)',
                            boxmode='group',
                            height=500,
                            legend_title="Planner",
                            xaxis_title="Planner",
                            yaxis_title="Value"
                        )
                        cols[j].plotly_chart(fig, use_container_width=True)


                # ---------------------                        
                # Add pie charts for time distribution by activity for each planner
                st.markdown("### Time Breakdown Analysis")
                
                # Define the time metrics we want to include in the pie charts
                time_metrics = ['Worker Startup Time [s]', 'Total Time Waiting for Inputs [s]', 'Execution Time [s]', 'Upload Time [s]']
                
                # Get unique planners and filter for time-related metrics
                planner_names = sorted(df_metrics['Planner'].unique())
                time_metrics_df = df_metrics[df_metrics['Metric'].isin(time_metrics)]
                
                # Create columns for the pie charts - one per planner
                cols = st.columns(len(planner_names) if len(planner_names) > 0 else 1)
                
                # for idx, planner_name in enumerate(planner_names):
                #     with cols[idx % len(cols)]:
                #         # Get metrics for this planner
                #         planner_data = time_metrics_df[time_metrics_df['Planner'] == planner_name]
                        
                #         # Create a dictionary with the time data
                #         time_data = {}
                #         for metric in time_metrics:
                #             metric_data = planner_data[planner_data['Metric'] == metric]
                #             time_data[metric] = metric_data['Value'].mean() if not metric_data.empty else 0
                        
                #         # Create a DataFrame for the pie chart
                #         df_pie = pd.DataFrame({
                #             'Activity': list(time_data.keys()),
                #             'Time (s)': list(time_data.values())
                #         })
                        
                #         # Create the pie chart with legend
                #         fig_pie = px.pie(
                #             df_pie, 
                #             values='Time (s)', 
                #             names='Activity',
                #             title=f'{planner_name} Time Distribution',
                #             color='Activity'
                #         )
                        
                #         # Update layout for better visualization
                #         fig_pie.update_traces(
                #             textposition='inside',
                #             textinfo='percent+label',
                #             hovertemplate='%{label}: %{value:.2f}s (%{percent})',
                #             textfont_size=12,
                #             # Enable click-to-toggle behavior
                #             customdata=df_pie['Activity'],
                #             selector=dict(type='pie')
                #         )
                        
                #         fig_pie.update_layout(
                #             showlegend=True,
                #             legend=dict(
                #                 title='Legend',
                #                 orientation='h',
                #                 yanchor='bottom',
                #                 y=-0.2,
                #                 xanchor='center',
                #                 x=0.5
                #             ),
                #             margin=dict(t=30, b=80, l=10, r=10),
                #             height=450,
                #             title_x=0.5,
                #             title_font_size=14,
                #             # Enable click-to-toggle functionality
                #             clickmode='event+select'
                #         )
                        
                #         # Add click-to-toggle functionality
                #         fig_pie.update_traces(
                #             hovertemplate='%{label}: %{value:.2f}s (%{percent})<extra></extra>',
                #             selector=dict(type='pie')
                #         )
                        
                #         st.plotly_chart(fig_pie, use_container_width=True, key=f"pie_{planner_name}")
                
                # STACKED BAR CHART of time breakdown
                bar_data = []

                for planner_name in planner_names:
                    planner_data = time_metrics_df[time_metrics_df['Planner'] == planner_name]
                    time_data = {}
                    for metric in time_metrics:
                        metric_data = planner_data[planner_data['Metric'] == metric]
                        time_data[metric] = metric_data['Value'].mean() if not metric_data.empty else 0
                    bar_data.append({'Planner': planner_name, **time_data})

                df_bar = pd.DataFrame(bar_data)

                # Create stacked bar chart
                fig_bar = px.bar(
                    df_bar,
                    x='Planner',
                    y=time_metrics,
                    title="Time Distribution",
                    labels={'value': 'Time (s)', 'Planner': 'Planner'}
                )

                # Update layout
                fig_bar.update_layout(
                    barmode='stack',
                    xaxis_title='Planner',
                    yaxis_title='Time (s)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    height=500,
                    legend_title='Activity'
                )

                st.plotly_chart(fig_bar, use_container_width=True)
            
            # Calculate metrics by planner type
            # Collect metrics per planner
            planner_metrics = {}

            for instance in workflow_types[selected_workflow].instances:
                if not instance.plan or not instance.tasks:
                    continue
                
                planner = instance.plan.planner_name if instance.plan else 'Unknown'
                if planner not in planner_metrics:
                    # store lists to compute median later
                    planner_metrics[planner] = {
                        'makespan': [],
                        'execution': [],
                        'download': [],
                        'upload': [],
                        'input_size': [],
                        'output_size': [],
                        'data_transferred': [],
                        'data_size_uploaded': [],
                        'data_size_downloaded': [],
                        'invocation': [],
                        'dependency_update': [],
                        'dag_download': [],
                        'worker_startup': [],
                        'resource_usage': [],
                        'warm_starts': [],
                        'cold_starts': []
                    }
                
                metrics = planner_metrics[planner]
                
                sink_task_metrics = [t for t in instance.tasks if t.internal_task_id == instance.dag.sink_node.id.get_full_id()][0].metrics
                sink_task_ended_timestamp_ms = (
                    sink_task_metrics.started_at_timestamp_s * 1000
                    + (sink_task_metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0)
                    + (sink_task_metrics.tp_execution_time_ms or 0)
                    + (sink_task_metrics.output_metrics.tp_time_ms or 0)
                    + (sink_task_metrics.total_invocation_time_ms or 0)
                )
                actual_makespan_s = (sink_task_ended_timestamp_ms - instance.start_time_ms) / 1000

                metrics['makespan'].append(actual_makespan_s)
                metrics['execution'].append(sum(task.metrics.tp_execution_time_ms / 1000 for task in instance.tasks))
                metrics['download'].append(sum(
                    sum(input_metric.time_ms / 1000 for input_metric in task.metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms is not None)
                    for task in instance.tasks
                ))
                metrics['upload'].append(sum(
                    task.metrics.output_metrics.tp_time_ms / 1000
                    for task in instance.tasks
                    if task.metrics.output_metrics.tp_time_ms is not None
                ))
                metrics['input_size'].append(sum(
                    sum(input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values())
                    for task in instance.tasks
                ))
                metrics['output_size'].append(sum(
                    task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks
                ))
                metrics['data_transferred'].append(instance.total_transferred_data_bytes)
                metrics['invocation'].append(sum(
                    task.metrics.total_invocation_time_ms / 1000
                    for task in instance.tasks
                    if task.metrics.total_invocation_time_ms is not None
                ))
                metrics['dependency_update'].append(sum(
                    task.metrics.update_dependency_counters_time_ms / 1000
                    for task in instance.tasks
                    if hasattr(task.metrics, 'update_dependency_counters_time_ms') and task.metrics.update_dependency_counters_time_ms is not None
                ))
                metrics['worker_startup'].append(instance.total_worker_startup_time_ms / 1000)
                metrics['resource_usage'].append(instance.resource_usage_cost)
                metrics['data_size_uploaded'].append(sum(task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks))
                metrics['data_size_downloaded'].append(sum(
                    sum(input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms is not None)
                    for task in instance.tasks
                ))
                metrics['warm_starts'].append(instance.warm_starts_count)
                metrics['cold_starts'].append(instance.cold_starts_count)

            # Compute median per planner

            # Prepare plot data with median and std
            plot_data = []
            network_data = []  # keep this array intact

            for planner_name, metrics in planner_metrics.items():
                metric_names = [
                    ('Makespan (s)', 'makespan'),
                    ('Execution Time (s)', 'execution'),
                    ('Download Time (s)', 'download'),
                    ('Upload Time (s)', 'upload'),
                    ('Data Transferred (GB)', 'data_transferred'),
                    ('Task Invocation Time (s)', 'invocation'),
                    ('Dependency Counter Update Time (s)', 'dependency_update'),
                    ('Worker Startup Time (s)', 'worker_startup'),
                    ('Resource Usage', 'resource_usage'),
                    ('Data Size Uploaded (MB)', 'data_size_uploaded'),
                    ('Data Size Downloaded (MB)', 'data_size_downloaded'),
                    ('Warm Starts', 'warm_starts'),
                    ('Cold Starts', 'cold_starts'),
                ]
                
                for display_name, key in metric_names:
                    median_val = np.median(metrics[key])
                    
                    # Convert units where needed
                    if display_name == 'Data Transferred (GB)':
                        median_val /= 1024**3
                    if display_name in ['Data Size Uploaded (MB)', 'Data Size Downloaded (MB)']:
                        median_val /= 1024**2
                    
                    plot_data.append({
                        'Planner': planner_name,
                        'Metric': display_name,
                        'Value': median_val,
                    })

                    # Keep network_data intact
                    if display_name == 'Data Size Uploaded (MB)':
                        network_data.append({
                            'Planner': planner_name,
                            'Type': 'Upload (MB)',
                            'Value': median_val
                        })
                    if display_name == 'Data Size Downloaded (MB)':
                        network_data.append({
                            'Planner': planner_name,
                            'Type': 'Download (MB)',
                            'Value': median_val
                        })

            df_plot = pd.DataFrame(plot_data)
            sorted_planners = sorted(df_plot['Planner'].unique())

            # Create bar chart
            fig = px.bar(
                df_plot,
                x='Metric',
                y='Value',
                color='Planner',
                barmode='group',
                title='All Metrics Comparison (Median)',
                labels={'Value': 'Value', 'Metric': 'Metric'},
                category_orders={'Planner': sorted_planners}
            )

            # Add median value above bars
            fig.update_traces(
                texttemplate='%{y:.2f}',  # this will always match the actual bar height
                textposition='outside',
            )

            fig.update_layout(
                xaxis_title='Metric',
                yaxis_title='Value',
                legend_title='Planner',
                plot_bgcolor='rgba(0,0,0,0)',
                yaxis_type='log',
                height=800,
                xaxis={'categoryorder':'total descending'}
            )

            st.plotly_chart(fig, use_container_width=True)

            st.markdown("### Resource Usage")
            ######### Resource Usage plot
            # Collect resource usage metrics per planner
            resource_data = []

            for instance in workflow_types[selected_workflow].instances:
                if not instance.plan or not instance.tasks:
                    continue
                
                planner = instance.plan.planner_name if instance.plan else 'Unknown'
                usage = instance.resource_usage  # assuming it has run_time_seconds, cpu_seconds, memory_bytes, cost

                resource_data.append({
                    'Planner': planner,
                    'Metric': 'Run Time (s)',
                    'Value': usage.run_time_seconds
                })
                resource_data.append({
                    'Planner': planner,
                    'Metric': 'CPU Time (s)',
                    'Value': usage.cpu_seconds
                })
                resource_data.append({
                    'Planner': planner,
                    'Metric': 'Memory (GB)',
                    'Value': usage.memory_bytes / (1024 * 1024 * 1024)  # convert to MB
                })
                resource_data.append({
                    'Planner': planner,
                    'Metric': 'Cost',
                    'Value': instance.resource_usage_cost
                })


            ######## Network I/O
            df_network = pd.DataFrame(network_data)

            # Sort planners alphabetically (or use custom order)
            sorted_planners = sorted(df_network['Planner'].unique())

            # Create side-by-side bar chart
            fig = px.bar(
                df_network,
                x='Planner',
                y='Value',
                color='Type',
                barmode='group',  # side-by-side bars
                title='Network I/O (Median)',
                labels={'Value': 'MB', 'Planner': 'Planner', 'Type': 'I/O Type'},
                category_orders={'Planner': sorted_planners}  # consistent order
            )

            # Layout improvements
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                yaxis_title='Data (MB)',
                height=500
            )

            # Show values on top of bars
            fig.update_traces(
                texttemplate='%{y:.2f}',
                textposition='outside',
                textfont_size=9
            )

            st.plotly_chart(fig, use_container_width=True)

            df_resource = pd.DataFrame(resource_data)

            # Plot each metric separately
            metrics = ['Run Time (s)', 'CPU Time (s)', 'Memory (GB)', 'Cost']

            cols = st.columns(2)

            for i, metric in enumerate(metrics):
                df_metric = df_resource[df_resource['Metric'] == metric]
                
                fig = px.box(
                    df_metric,
                    x='Planner',
                    y='Value',
                    color='Planner',
                    points='all',
                    title=f'{metric} Distribution',
                    labels={'Value': metric, 'Planner': 'Planner'},
                    category_orders={'Planner': sorted_planners}
                )
                
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    height=400,
                    showlegend=False
                )
                
                # Place the plot in the correct column
                cols[i % 2].plotly_chart(fig, use_container_width=True)
                
                # Move to a new row after 2 plots
                if i % 2 == 1:
                    cols = st.columns(2)

            planner_start_stats = []
            for instance in workflow_types[selected_workflow].instances:
                if not instance.plan:
                    continue
                planner = instance.plan.planner_name

                warm = instance.warm_starts_count
                cold = instance.cold_starts_count
                total = warm + cold
                if total == 0:
                    continue

                warm_pct = warm / total * 100
                cold_pct = cold / total * 100

                planner_start_stats.append({
                    "Planner": planner,
                    "Warm": warm,
                    "Cold": cold,
                    "Warm %": warm_pct,
                    "Cold %": cold_pct
                })

            # Convert to DataFrame
            df_starts = pd.DataFrame(planner_start_stats)

            # Group by planner: take mean counts + mean percentages
            df_agg = df_starts.groupby("Planner").agg({
                "Warm": "mean",
                "Cold": "mean",
                "Warm %": "mean",
                "Cold %": "mean"
            }).reset_index()

            # Melt for stacked bar (percentages)
            df_melted = df_agg.melt(
                id_vars="Planner",
                value_vars=["Warm %", "Cold %"],
                var_name="Start Type",
                value_name="Percentage"
            )

            # Create stacked percentage bar chart
            fig = px.bar(
                df_melted,
                x="Planner",
                y="Percentage",
                color="Start Type",
                barmode="stack",
                text=df_melted["Percentage"].round(1).astype(str) + "%",
                title="Warm vs Cold Starts per Planner (Mean % + Counts)"
            )

            fig.update_traces(textposition="inside")

            # Add annotations for mean counts above each bar
            for i, row in df_agg.iterrows():
                fig.add_annotation(
                    x=row["Planner"],
                    y=105,  # slightly above the 100% bar
                    text=f"Avg Warm={row['Warm']:.1f}, Cold={row['Cold']:.1f} p/ instance",
                    showarrow=False,
                    font=dict(size=12, color="black")
                )

            # Layout tweaks
            fig.update_layout(
                xaxis_title="Planner",
                yaxis_title="Mean Percentage of Starts",
                legend_title="Start Type",
                plot_bgcolor="rgba(0,0,0,0)",
                height=650,
                yaxis=dict(range=[0, 115])  # leave space for labels above bars
            )

            st.plotly_chart(fig, use_container_width=True)

            #########
            st.markdown("### SLA")

            # Define the metrics we want to track
            metrics_list = [
                "makespan",
                "execution",
                "download",
                "upload",
                "input_size",
                "output_size",
                "worker_startup_time"
            ]

            results = []

            for workflow_name, workflow_info in st.session_state.workflow_types.items():
                # Sort instances by start time
                sorted_instances = sorted(workflow_info.instances, key=lambda inst: inst.start_time_ms)

                # Keep track of history for each metric
                history = {metric: [] for metric in metrics_list}

                for inst in sorted_instances:
                    if not inst.plan or not inst.tasks:
                        continue
                    sla = inst.plan.sla
                    if sla == "average": continue  # skip "average"

                    sla_value = sla.value  # percentile 1-100

                    # --- Compute actual metrics like before ---
                    actual_makespan = (
                        max([
                            (task.metrics.started_at_timestamp_s * 1000) +
                            (task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) +
                            (task.metrics.tp_execution_time_ms or 0) +
                            (task.metrics.output_metrics.tp_time_ms or 0) +
                            (task.metrics.total_invocation_time_ms or 0)
                            for task in inst.tasks
                        ]) - inst.start_time_ms
                    ) / 1000

                    actual_execution = sum(task.metrics.tp_execution_time_ms / 1000 for task in inst.tasks)
                    actual_download = sum(
                        sum(im.time_ms for im in task.metrics.input_metrics.input_download_metrics.values() if im.time_ms) 
                        for task in inst.tasks
                    ) / 1000
                    actual_upload = sum(task.metrics.output_metrics.tp_time_ms or 0 for task in inst.tasks) / 1000
                    actual_input_size = sum(
                        sum(im.serialized_size_bytes for im in task.metrics.input_metrics.input_download_metrics.values()) +
                        task.metrics.input_metrics.hardcoded_input_size_bytes
                        for task in inst.tasks
                    )
                    actual_output_size = sum(task.metrics.output_metrics.serialized_size_bytes for task in inst.tasks)
                    actual_worker_startup = inst.total_worker_startup_time_ms / 1000

                    actuals = {
                        "makespan": actual_makespan,
                        "execution": actual_execution,
                        "download": actual_download,
                        "upload": actual_upload,
                        "input_size": actual_input_size,
                        "output_size": actual_output_size,
                        "worker_startup_time": actual_worker_startup
                    }

                    # Compare to historical percentile for each metric
                    for metric in metrics_list:
                        prev_values = history[metric]
                        if prev_values:
                            threshold = np.percentile(prev_values, sla_value)
                            success = actuals[metric] <= threshold
                            results.append({
                                "SLA": sla_value,
                                "Metric": metric,
                                "Success": success
                            })

                        # Update history
                        history[metric].append(actuals[metric])

            # Convert to DataFrame and compute success rate
            df = pd.DataFrame(results)
            if not df.empty:
                # Per-metric success rate
                summary = df.groupby(["SLA", "Metric"])["Success"].mean().reset_index()
                summary["SuccessRate"] = summary["Success"] * 100

                # Overall success rate across all metrics
                overall = df.groupby("SLA")["Success"].mean().reset_index()
                overall["SuccessRate"] = overall["Success"] * 100
                overall["Metric"] = "Overall"

                # Merge
                summary = pd.concat([summary, overall], ignore_index=True)

                # Convert SLA numeric to categorical label
                summary["SLA_label"] = "P" + summary["SLA"].astype(str)

                # Bar chart
                fig = px.bar(
                    summary,
                    x="SLA_label",
                    y="SuccessRate",
                    color="Metric",
                    barmode="group",
                    title="SLA Fulfillment Rate Across Metrics (with Overall)",
                    text="SuccessRate"
                )

                fig.update_layout(
                    xaxis_title="SLA Percentile",
                    yaxis_title="Fulfillment Rate (%)",
                    yaxis=dict(range=[0, 100]),
                    legend_title="Metric"
                )
                fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write("No percentile SLA data available to plot.")
    else:
        st.warning("No instance data available for the selected filters.")

if __name__ == "__main__":
    main()