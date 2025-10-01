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
    total_transferred_data_bytes: int
    total_inputs_downloaded_bytes: float
    total_outputs_uploaded_bytes: float

@dataclass
class WorkflowInfo:
    type: str
    representative_dag: FullDAG
    instances: List[WorkflowInstanceInfo]


def get_workflows_information(metadata_storage_conn: redis.Redis) -> tuple[List[WorkerStartupMetrics], Dict[str, WorkflowInfo]]:
    workflow_types: Dict[str, WorkflowInfo] = {}
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
                        -1
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

                total_worker_startup_time_ms = 0
                for metric in this_workflow_wsm:
                    if not metric.end_time_ms: continue
                    total_worker_startup_time_ms += metric.end_time_ms - metric.start_time_ms

                total_workers = len(this_workflow_wsm)

                # Resource usage metrics
                resource_usage_key = f"{MetadataStorage.DAG_RESOURCE_USAGE_PREFIX}{dag.master_dag_id}"
                resource_usage_data: Any = metadata_storage_conn.get(resource_usage_key)
                resource_usage: DAGResourceUsageMetrics = cloudpickle.loads(resource_usage_data)

                total_transferred_data_bytes = 0
                for task in tasks:
                    for im in task.metrics.input_metrics.input_download_metrics.values(): 
                        if im.time_ms is None: continue # did not download from storage
                        total_transferred_data_bytes += im.serialized_size_bytes
                    if task.metrics.output_metrics.tp_time_ms is None: continue # did not upload to storage
                    total_transferred_data_bytes += task.metrics.output_metrics.serialized_size_bytes

                # Update workflow_types dict
                if dag.dag_name not in workflow_types:
                    workflow_types[dag.dag_name] = WorkflowInfo(dag.dag_name, dag, [])
                workflow_types[dag.dag_name].instances.append(
                    WorkflowInstanceInfo(
                        dag.master_dag_id,
                        plan_output,
                        dag,
                        dag_download_stats,
                        dag_submission_metrics.dag_submission_time_ms,
                        total_worker_startup_time_ms,
                        total_workers,
                        tasks,
                        resource_usage,
                        total_transferred_data_bytes,
                        total_inputs_downloaded,
                        total_outputs_uploaded
                    )
                )
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
        options=["All"] + sorted(list(workflow_types.keys())),
        index=0
    )
    
    # Filter workflow instances based on selected workflow type
    matching_workflow_instances: list[WorkflowInstanceInfo] = []
    if selected_workflow == "All":
        for workflow in workflow_types.values():
            matching_workflow_instances.extend(workflow.instances)
    else:
        matching_workflow_instances.extend(workflow_types[selected_workflow].instances)
    
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
            st.metric("Workflow Tasks", len(workflow_types[selected_workflow].representative_dag._all_nodes))
    
    # Show metrics based on selection
    if selected_workflow != 'All' and matching_workflow_instances:
            # Prepare data for the instance comparison table
            instance_data = []
            for idx, instance in enumerate(matching_workflow_instances):
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
                        print(f"Found a diff resource config. Prev: {common_resources} New: {task.metrics.worker_resource_configuration}")
                        common_resources = None
                
                sink_task_metrics = [t for t in instance.tasks if t.internal_task_id == instance.dag.sink_node.id.get_full_id()][0].metrics
                sink_task_ended_timestamp_ms = (sink_task_metrics.started_at_timestamp_s * 1000) + (sink_task_metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + (sink_task_metrics.tp_execution_time_ms or 0) + (sink_task_metrics.output_metrics.tp_time_ms or 0) + (sink_task_metrics.total_invocation_time_ms or 0)
                actual_makespan_s = (sink_task_ended_timestamp_ms - instance.start_time_ms) / 1000
                actual_data_transferred = instance.total_transferred_data_bytes
                actual_total_downloadable_input_size_bytes = instance.total_inputs_downloaded_bytes
                actual_total_uploadable_output_size_bytes = instance.total_outputs_uploaded_bytes

                # Get predicted metrics if available
                predicted_total_downloadable_input_size_bytes = predicted_total_download = predicted_execution = predicted_total_upload = predicted_makespan_s = 0
                predicted_total_uploadable_output_size_bytes = predicted_input_size = predicted_output_size = predicted_total_worker_startup_time_s = 0
                if instance.plan and instance.plan.nodes_info:
                    predicted_total_download = sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_execution = sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_total_upload = sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_input_size = sum(info.serialized_input_size for info in instance.plan.nodes_info.values())  # in bytes
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
                total_download_time = sum(stat.download_time_ms for stat in instance.dag_download_stats)
                dag_download_time = f"{total_download_time / 1000:.3f}s"

                unique_worker_ids = set()
                for task_metrics in instance.tasks:
                    unique_worker_ids.add(task_metrics.metrics.worker_resource_configuration.worker_id)
                
                # Store the instance data with metrics for SLA comparison
                instance_data.append({
                    'Workflow Type': selected_workflow,
                    'Planner': instance.plan.planner_name if instance.plan else 'N/A',
                    'Resources': f"{common_resources.cpus} CPUs {common_resources.memory_mb} MB" if common_resources else 'Non-Uniform',
                    'SLA': sla_value,
                    '_sla_percentile': sla_percentile,
                    'Master DAG ID': instance.master_dag_id,
                    'Makespan': format_metric(actual_makespan_s, predicted_makespan_s, 'makespan',
                                        sample_counts.for_execution_time if sample_counts else None),
                    'Total Execution Time': format_metric(actual_execution, predicted_execution, 'execution',
                                                sample_counts.for_execution_time if sample_counts else None),
                    'Total Downloaded Data': f"{format_bytes(predicted_total_downloadable_input_size_bytes)[2]} -> {format_bytes(actual_total_downloadable_input_size_bytes)[2]}",
                    'Total Download Time': format_metric(actual_total_download, predicted_total_download, 'download',
                                            sample_counts.for_download_speed if sample_counts else None),
                    'Total Uploaded Data': f"{format_bytes(predicted_total_uploadable_output_size_bytes)[2]} -> {format_bytes(actual_total_uploadable_output_size_bytes)[2]}",
                    'Total Upload Time': format_metric(actual_total_upload, predicted_total_upload, 'upload',
                                            sample_counts.for_upload_speed if sample_counts else None),
                    'Total Input Size': format_size_metric(actual_input_size, predicted_input_size, 'input_size',
                                            sample_counts.for_output_size if sample_counts else None),
                    'Total Output Size': format_size_metric(actual_output_size, predicted_output_size, 'output_size',
                                            sample_counts.for_output_size if sample_counts else None),
                    'Total Data Transferred': format_bytes(actual_data_transferred)[2],
                    'Total Task Invocation Time': f"{actual_invocation:.2f}s",
                    'Total Dependency Counter Update Time': f"{actual_dependency_update:.2f}s",
                    'Total DAG Download Time': dag_download_time,
                    'Total Worker Startup Time': format_metric(actual_total_worker_startup_time_s, predicted_total_worker_startup_time_s, 'worker_startup') + f" (Workers: {len(unique_worker_ids)})",
                    'Run Time': f"{instance.resource_usage.run_time_seconds:.2f}",
                    'CPU Time': f"{instance.resource_usage.cpu_seconds:.2f}",
                    'Memory Usage': f"{convert_bytes_to_GB(instance.resource_usage.memory_bytes):.2f} GB",
                    'Resources Cost': f"{instance.resource_usage.cost:.2f}",
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
                        'Total DAG Download Time': "Total DAG Download Time",
                        'Total Worker Startup Time': "Total Worker Startup Time (Predicted → Actual)",
                        'Run Time': "Run Time",
                        'CPU Time': "CPU Time",
                        'Memory Usage': "Memory Usage",
                        'Resources Cost': "Resources Cost",
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
                        'Total DAG Download Time',
                        'Total Worker Startup Time',
                        'Run Time',
                        'CPU Time',
                        'Memory Usage',
                        'Resources Cost',
                    ]
                )

                st.markdown("### View Task Metrics by DAG ID")
                
                # Create a text input for DAG ID
                dag_id = st.text_input("Enter DAG ID to view task metrics:", "")
                
                # Initialize variables
                selected_instance_info = None
                workflow_name = ""
                planner_name = ""
                
                # Process the entered DAG ID
                if dag_id and 'Master DAG ID' in df_instances.columns:
                    # Find rows with matching DAG ID
                    matching_rows = df_instances[df_instances['Master DAG ID'].astype(str) == dag_id]
                    
                    if len(matching_rows) > 0:
                        # Get the first matching row
                        row = matching_rows.iloc[0]  # type: ignore
                        workflow_name = str(row['Workflow Type']) if pd.notna(row['Workflow Type']) else ""
                        planner_name = str(row['Planner']) if pd.notna(row['Planner']) else ""
                        
                        # Show success message
                        st.success(f"Found matching workflow: {workflow_name} - {planner_name}")
                        
                        # Find the selected instance
                        if workflow_name and planner_name and workflow_name in workflow_types:
                            for instance in workflow_types[workflow_name].instances:
                                if (instance.plan and 
                                    instance.plan.planner_name == planner_name and
                                    instance.master_dag_id == dag_id):
                                    selected_instance_info = instance
                                    break
                    else:
                        st.warning(f"No workflow found with DAG ID: {dag_id}")
                    
                    if selected_instance_info and selected_instance_info.tasks:
                        st.markdown("---")
                        st.markdown(f"### Task Metrics for {workflow_name} - {planner_name}")
                        
                        # Prepare task metrics data
                        task_metrics_data = []
                        for task in selected_instance_info.tasks:
                            worker_startup_metrics_for_task = [n for n in st.session_state.worker_startup_metrics if task.internal_task_id in n.initial_task_ids]
                            worker_startup_metrics_for_task = worker_startup_metrics_for_task[0] if worker_startup_metrics_for_task else None
                            task_metrics = task.metrics
                            task_metrics_data.append({
                                'Task ID': task.global_task_id,
                                'Worker Config': str(task_metrics.worker_resource_configuration),
                                'Start Time (s)': task_metrics.started_at_timestamp_s,
                                'Input Size (bytes)': sum([input_metric.serialized_size_bytes for input_metric in task_metrics.input_metrics.input_download_metrics.values()]),
                                'Download Time (ms)': sum([input_metric.time_ms for input_metric in task_metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms]),
                                'Execution Time (ms)': task_metrics.tp_execution_time_ms,
                                'Output Size (bytes)': task_metrics.output_metrics.serialized_size_bytes if hasattr(task_metrics, 'output_metrics') else 0,
                                'Output Time (ms)': task_metrics.output_metrics.tp_time_ms if hasattr(task_metrics, 'output_metrics') else 0,
                                'Worker Startup Time (ms)': (worker_startup_metrics_for_task.end_time_ms - worker_startup_metrics_for_task.start_time_ms) if worker_startup_metrics_for_task and worker_startup_metrics_for_task.end_time_ms else 0,
                            })
                        
                        # Display task metrics table
                        if task_metrics_data:
                            df_task_metrics = pd.DataFrame(task_metrics_data)
                            st.dataframe(
                                df_task_metrics,
                                column_config={
                                    'Task ID': "Task ID",
                                    'Worker Config': "Worker Configuration",
                                    'Start Time (s)': st.column_config.NumberColumn("Start Time (s)", format="%.2f"),
                                    'Input Size (bytes)': st.column_config.NumberColumn("Input Size (bytes)", format="%d"),
                                    'Download Time (ms)': st.column_config.NumberColumn("Download Time (ms)", format="%.2f"),
                                    'Execution Time (ms)': st.column_config.NumberColumn("Execution Time (ms)", format="%.2f"),
                                    'Output Size (bytes)': st.column_config.NumberColumn("Output Size (bytes)", format="%d"),
                                    'Output Time (ms)': st.column_config.NumberColumn("Output Time (ms)", format="%.2f"),
                                    'Worker Startup Time (ms)': st.column_config.NumberColumn("Worker Startup Time (ms)", format="%.2f"),
                                },
                                use_container_width=True,
                                hide_index=True,
                            )

                st.markdown("---")
                # Add comparison bar chart for predicted vs actual metrics
                st.markdown("### Reality vs Predictions")
                
                # Calculate averages for the comparison
                metrics_data = []
                for instance in matching_workflow_instances:
                    if not instance.plan or not instance.tasks:
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
                    predicted_makespan_s = predicted_execution = predicted_total_download = predicted_total_upload = predicted_input_size = predicted_output_size = predicted_worker_startup_time_s = 0 # initialize them outside
                    if instance.plan and instance.plan.nodes_info:
                        predicted_makespan_s = instance.plan.nodes_info[instance.dag.sink_node.id.get_full_id()].task_completion_time_ms / 1000
                        predicted_total_download = sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values())
                        predicted_execution = sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values())
                        predicted_total_upload = sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values())
                        predicted_input_size = sum(info.serialized_input_size for info in instance.plan.nodes_info.values())
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
                        'input_size_predicted': predicted_input_size,
                        'output_size_actual': actual_output_size,
                        'output_size_predicted': predicted_output_size,
                        'worker_startup_time_actual': actual_worker_startup_time_s,
                        'worker_startup_time_predicted': predicted_worker_startup_time_s,
                    })
                
                if metrics_data:
                    # Group metrics by planner
                    planner_metrics = {}
                    for instance in workflow_types[selected_workflow].instances:
                        if not instance.plan or not instance.tasks:
                            continue
                            
                        planner_name = instance.plan.planner_name
                        if planner_name not in planner_metrics:
                            planner_metrics[planner_name] = []
                            
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
                        predicted_makespan_s = predicted_execution = predicted_total_download = predicted_total_upload = predicted_input_size = predicted_output_size = predicted_worker_startup_time_s = 0
                        if instance.plan and instance.plan.nodes_info:
                            predicted_makespan_s = instance.plan.nodes_info[instance.dag.sink_node.id.get_full_id()].task_completion_time_ms / 1000
                            predicted_total_download = sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values())
                            predicted_execution = sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values())
                            predicted_total_upload = sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values())
                            predicted_input_size = sum(info.serialized_input_size for info in instance.plan.nodes_info.values())
                            predicted_output_size = sum(info.serialized_output_size for info in instance.plan.nodes_info.values())
                            workers_accounted_for = set()
                            predicted_worker_startup_time_s = 0
                            for info in instance.plan.nodes_info.values():
                                if info.node_ref.worker_config.worker_id is None or info.node_ref.worker_config.worker_id not in workers_accounted_for:
                                    predicted_worker_startup_time_s += info.tp_worker_startup_time_ms / 1000
                                    workers_accounted_for.add(info.node_ref.worker_config.worker_id)
                            
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
                                'input_size_predicted': predicted_input_size,
                                'output_size_actual': actual_output_size,
                                'output_size_predicted': predicted_output_size,
                                'worker_startup_time_actual': actual_worker_startup_time_s,
                                'worker_startup_time_predicted': predicted_worker_startup_time_s,
                            })

                    # Calculate averages for each planner
                    planner_avg_metrics = {}
                    for planner_name, planner_data in planner_metrics.items():
                        if not planner_data:
                            continue
                            
                        planner_avg_metrics[planner_name] = {
                            'Makespan (s)': {
                                'actual': sum(m['makespan_actual'] for m in planner_data) / len(planner_data),
                                'predicted': sum(m['makespan_predicted'] for m in planner_data) / len(planner_data)
                            },
                            'Execution Time (s)': {
                                'actual': sum(m['execution_actual'] for m in planner_data) / len(planner_data),
                                'predicted': sum(m['execution_predicted'] for m in planner_data) / len(planner_data)
                            },
                            'Download Time (s)': {
                                'actual': sum(m['download_actual'] for m in planner_data) / len(planner_data),
                                'predicted': sum(m['download_predicted'] for m in planner_data) / len(planner_data)
                            },
                            'Upload Time (s)': {
                                'actual': sum(m['upload_actual'] for m in planner_data) / len(planner_data),
                                'predicted': sum(m['upload_predicted'] for m in planner_data) / len(planner_data)
                            },
                            'Input Size (bytes)': {
                                'actual': sum(m['input_size_actual'] for m in planner_data) / len(planner_data),
                                'predicted': sum(m['input_size_predicted'] for m in planner_data) / len(planner_data)
                            },
                            'Output Size (bytes)': {
                                'actual': sum(m['output_size_actual'] for m in planner_data) / len(planner_data),
                                'predicted': sum(m['output_size_predicted'] for m in planner_data) / len(planner_data)
                            },
                            'Worker Startup Time (s)': {
                                'actual': sum(m['worker_startup_time_actual'] for m in planner_data) / len(planner_data),
                                'predicted': sum(m['worker_startup_time_predicted'] for m in planner_data) / len(planner_data)
                            },
                        }
                    
                    # Create a dropdown to select planner
                    if planner_avg_metrics:
                        selected_planner = st.selectbox(
                            'Select Planner:',
                            options=list(planner_avg_metrics.keys()),
                            index=0,
                            key='planner_selector'
                        )
                        
                        # Prepare data for the selected planner
                        plot_data = []
                        for metric_name, values in planner_avg_metrics[selected_planner].items():
                            plot_data.append({
                                'Metric': metric_name,
                                'Value': values['actual'],
                                'Type': 'Actual'
                            })
                            plot_data.append({
                                'Metric': metric_name,
                                'Value': values['predicted'],
                                'Type': 'Predicted'
                            })
                        
                        df_plot = pd.DataFrame(plot_data)
                        
                        # Create bar chart for the selected planner
                        fig = px.bar(
                            df_plot, 
                            x='Metric', 
                            y='Value', 
                            color='Type',
                            barmode='group',
                            title=f'{selected_planner} (averages per planner per instance)',
                            labels={'Value': 'Value'},
                            color_discrete_map={'Actual': '#1f77b4', 'Predicted': '#ff7f0e'}
                        )
                        
                        # Update layout for better visualization
                        fig.update_layout(
                            xaxis_title='Metric',
                            yaxis_title='Value',
                            legend_title='',
                            plot_bgcolor='rgba(0,0,0,0)',
                            yaxis_type='log',  # Use log scale for better visualization of different magnitudes
                            height=500,
                            xaxis_tickangle=-45
                        )
                        
                        # Add value labels on top of bars
                        fig.update_traces(
                            texttemplate='%{y:.2f}',
                            textposition='outside',
                            textfont_size=10
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                
                # Add prediction accuracy evolution chart
                st.markdown("### Prediction Accuracy Evolution")
                
                # Collect data for accuracy evolution
                accuracy_data = []
                for instance in workflow_types[selected_workflow].instances:
                    if not instance.plan or not instance.tasks:
                        continue
                    
                    # Calculate total samples used for this prediction
                    samples = instance.plan.prediction_sample_counts
                    total_samples = sum([
                        samples.for_download_speed,
                        samples.for_upload_speed,
                        samples.for_execution_time,
                        samples.for_output_size
                    ])
                    
                    # Calculate all metrics for this instance
                    actual_metrics = {
                        'makespan_actual': (
                            max([
                                (task.metrics.started_at_timestamp_s * 1000) + 
                                (task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + 
                                (task.metrics.tp_execution_time_ms or 0) + 
                                (task.metrics.output_metrics.tp_time_ms or 0) + 
                                (task.metrics.total_invocation_time_ms or 0) 
                                for task in instance.tasks
                            ]) - instance.start_time_ms
                        ) / 1000,  # Convert to seconds
                        'execution_actual': sum(task.metrics.tp_execution_time_ms or 0 for task in instance.tasks) / 1000,
                        'download_actual': sum(task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0 for task in instance.tasks) / 1000,
                        'upload_actual': sum(task.metrics.output_metrics.tp_time_ms or 0 for task in instance.tasks) / 1000,
                        'input_size_actual': sum(sum(input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values()) + 
                                              (task.metrics.input_metrics.hardcoded_input_size_bytes or 0) for task in instance.tasks),
                        'output_size_actual': sum(task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks if hasattr(task.metrics, 'output_metrics')),
                        'worker_startup_time_actual': sum(
                            (metric.end_time_ms - metric.start_time_ms) / 1000 
                            for metric in st.session_state.worker_startup_metrics 
                            if metric.master_dag_id == instance.master_dag_id and metric.end_time_ms is not None
                        )
                    }
                    
                    # Get predicted metrics if available
                    predicted_metrics = {}
                    if instance.plan and instance.plan.nodes_info:
                        sink_node = instance.dag.sink_node.id.get_full_id()
                        workers_accounted_for = set()
                        predicted_worker_startup_time_s = 0
                        for info in instance.plan.nodes_info.values():
                            if info.node_ref.worker_config.worker_id is None or info.node_ref.worker_config.worker_id not in workers_accounted_for:
                                predicted_worker_startup_time_s += info.tp_worker_startup_time_ms / 1000
                                workers_accounted_for.add(info.node_ref.worker_config.worker_id)
                        predicted_metrics = {
                            'makespan_predicted': instance.plan.nodes_info[sink_node].task_completion_time_ms / 1000,
                            'execution_predicted': sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                            'download_predicted': sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                            'upload_predicted': sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                            'input_size_predicted': sum(info.serialized_input_size for info in instance.plan.nodes_info.values()),
                            'output_size_predicted': sum(info.serialized_output_size for info in instance.plan.nodes_info.values()),
                            'worker_startup_time_predicted': predicted_worker_startup_time_s
                        }
                    
                    # Add to accuracy data
                    accuracy_data.append({
                        'Planner': instance.plan.planner_name,
                        'Samples': total_samples,
                        'Previous Instances': instance.plan.prediction_sample_counts.previous_instances,
                        **{k: v for k, v in actual_metrics.items() if v is not None},
                        **{k: v for k, v in predicted_metrics.items() if v is not None}
                    })
                
                # Create the accuracy evolution chart if we have data
                if accuracy_data:
                    df_accuracy = pd.DataFrame(accuracy_data)
                    
                    # Sort by samples for each planner to get proper line connections
                    df_accuracy = df_accuracy.sort_values(['Planner', 'Samples'])
                    
                    # Create a formatted label for X-axis with just the instance counter
                    df_accuracy['X_Label'] = df_accuracy['Previous Instances'].astype(int).astype(str)
                    
                    # Define all possible metrics and their display names
                    all_metric_options = [
                        ('Makespan (s)', 'makespan_actual', 'makespan_predicted'),
                        ('Execution Time (s)', 'execution_actual', 'execution_predicted'),
                        ('Download Time (s)', 'download_actual', 'download_predicted'),
                        ('Upload Time (s)', 'upload_actual', 'upload_predicted'),
                        ('Input Size (bytes)', 'input_size_actual', 'input_size_predicted'),
                        ('Output Size (bytes)', 'output_size_actual', 'output_size_predicted'),
                        ('Worker Startup Time (s)', 'worker_startup_time_actual', 'worker_startup_time_predicted')
                    ]
                    
                    # Only include metrics that have data
                    available_columns = set(df_accuracy.columns)
                    metric_options = {
                        display_name: (actual_col, pred_col)
                        for display_name, actual_col, pred_col in all_metric_options
                        if actual_col in available_columns and pred_col in available_columns
                    }
                    
                    selected_metric = st.selectbox(
                        'Select Metric to Analyze',
                        options=list(metric_options.keys()),
                        index=0,  # Default to Makespan
                        key='accuracy_metric_selector'
                    )
                    
                    # Get the actual and predicted column names for the selected metric
                    actual_col, predicted_col = metric_options[selected_metric]
                    
                    # Calculate relative error for the selected metric, handling missing or zero values
                    df_accuracy['Relative Error'] = df_accuracy.apply(
                        lambda x: (
                            abs(x[actual_col] - x[predicted_col]) / x[actual_col] 
                            if x[actual_col] > 0 and not pd.isna(x[actual_col]) and not pd.isna(x[predicted_col])
                            else None
                        ),
                        axis=1
                    )
                    
                    # Remove rows where relative error couldn't be calculated
                    df_accuracy = df_accuracy.dropna(subset=['Relative Error'])
                    
                    # Create line chart for relative error with visible markers
                    fig_error = px.line(
                        df_accuracy,
                        x='X_Label',
                        y='Relative Error',
                        color='Planner',
                        title=f'Prediction Error vs Number of Samples - {selected_metric}',
                        labels={
                            'Relative Error': f'Relative Error (lower is better)', 
                            'X_Label': 'Number of Previous Instances'
                        },
                        hover_data={
                            'X_Label': False,  # Hide from hover
                            'Previous Instances': ':.0f',
                            actual_col: ':.2f',
                            predicted_col: ':.2f',
                            'Relative Error': ':.2f'
                        },
                        markers=True
                    )
                    
                    # Create a second chart for actual values evolution
                    st.markdown("### Actual Metric Values Evolution")
                    
                    # Create line chart for actual values
                    fig_actual = px.line(
                        df_accuracy,
                        x='X_Label',
                        y=actual_col,
                        color='Planner',
                        title=f'Actual {selected_metric} vs Number of Samples',
                        labels={
                            actual_col: selected_metric,
                            'X_Label': 'Number of Previous Instances'
                        },
                        hover_data={
                            'X_Label': False,  # Hide from hover
                            'Previous Instances': ':.0f',
                            actual_col: ':.2f'
                        },
                        markers=True
                    )
                    
                    # Update marker and line styles for better visibility
                    fig_actual.update_traces(
                        mode='lines+markers',
                        marker=dict(
                            size=8,
                            line=dict(width=1, color='DarkSlateGrey')
                        ),
                        line=dict(width=2)
                    )
                    
                    # Update layout for the actual values chart
                    fig_actual.update_layout(
                        xaxis={
                            'title': 'Number of Instances',
                            'tickangle': 45,
                            'tickmode': 'array',
                            'tickvals': df_accuracy['X_Label'].unique(),
                            'type': 'category'
                        },
                        yaxis_title=selected_metric,
                        legend_title='Planner',
                        plot_bgcolor='rgba(0,0,0,0)',
                        height=500,
                        hovermode='x unified',
                        margin=dict(b=100)
                    )
                    
                    # Update marker and line styles for better visibility
                    fig_error.update_traces(
                        mode='lines+markers',  # Show both lines and markers
                        marker=dict(
                            size=8,            # Larger markers
                            line=dict(
                                width=1,        # Border width
                                color='DarkSlateGrey'  # Border color
                            )
                        ),
                        line=dict(width=2)     # Thinner lines to emphasize markers
                    )
                    
                    # Update x-axis to show all tick labels and improve readability
                    fig_error.update_layout(
                        xaxis={
                            'title': 'Number of Instances',
                            'tickangle': 45,
                            'tickmode': 'array',
                            'tickvals': df_accuracy['X_Label'].unique(),

                            'type': 'category'  # Treat x-axis as categories to show all labels
                        },
                        yaxis_title='Relative Error (Actual vs Predicted)',
                        legend_title='Planner',
                        plot_bgcolor='rgba(0,0,0,0)',
                        height=500,
                        hovermode='x unified',
                        margin=dict(b=100)  # Add bottom margin for x-axis labels
                    )
                    
                    # Display both charts in tabs
                    tab1, tab2 = st.tabs(["Prediction Error", "Actual Values"])
                    
                    with tab1:
                        st.plotly_chart(fig_error, use_container_width=True)
                    
                    with tab2:
                        st.plotly_chart(fig_actual, use_container_width=True)
                    
                    st.markdown("### Prediction Error Distribution by Planner")
                        
                    # Prepare data for error distribution
                    error_data = []
                    for instance in workflow_types[selected_workflow].instances:
                        if not instance.plan or not instance.tasks:
                            continue
                        
                        # Calculate actual metrics
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
                            ) / 1000,  # Convert to seconds
                            'execution': sum(task.metrics.tp_execution_time_ms or 0 for task in instance.tasks) / 1000,
                            'download': sum(task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0 for task in instance.tasks) / 1000,
                            'upload': sum(task.metrics.output_metrics.tp_time_ms or 0 for task in instance.tasks) / 1000,
                            'input_size': sum(sum(input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values()) + 
                                        (task.metrics.input_metrics.hardcoded_input_size_bytes or 0) for task in instance.tasks),
                            'output_size': sum(task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks if hasattr(task.metrics, 'output_metrics')),
                            'worker_startup_time': instance.total_worker_startup_time_ms / 1000,
                        }
                        
                        # Get predicted metrics if available
                        predicted_metrics = {}
                        if instance.plan and instance.plan.nodes_info:
                            sink_node = instance.dag.sink_node.id.get_full_id()
                            predicted_metrics = {
                                'makespan': instance.plan.nodes_info[sink_node].task_completion_time_ms / 1000,
                                'execution': sum(info.tp_exec_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                                'download': sum(info.total_download_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                                'upload': sum(info.tp_upload_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                                'input_size': sum(info.serialized_input_size for info in instance.plan.nodes_info.values()),
                                'output_size': sum(info.serialized_output_size for info in instance.plan.nodes_info.values()),
                                'worker_startup_time': sum(info.tp_worker_startup_time_ms / 1000 for info in instance.plan.nodes_info.values()),
                            }
                        
                        # Calculate relative errors for each metric
                        for metric in ['makespan', 'execution', 'download', 'upload', 'input_size', 'output_size', 'worker_startup_time']:
                            actual = actual_metrics.get(metric, 0)
                            predicted = predicted_metrics.get(metric, 0)
                            
                            # Calculate relative error (absolute percentage error)
                            if actual != 0 and predicted != 0:  # Only include if both values are non-zero
                                rel_error = abs(actual - predicted) / actual * 100
                                error_data.append({
                                    'Metric': metric.replace('_', ' ').title(),
                                    'Relative Error (%)': rel_error,
                                    'Planner': instance.plan.planner_name if instance.plan else 'Unknown',
                                    'Instance ID': instance.master_dag_id
                                })
                    
                    if error_data:
                        df_errors = pd.DataFrame(error_data)
                        
                        # Create box plot
                        fig_errors = px.box(
                            df_errors,
                            x='Metric',
                            y='Relative Error (%)',
                            color='Planner',
                            title='Distribution of Prediction Errors by Metric',
                            points='all',  # Show all points
                            hover_data=['Instance ID'],
                            category_orders={"Metric": ["Makespan", "Execution", "Download", "Upload", "Input Size", "Output Size", "Worker Startup Time"]},
                            color_discrete_map={
                                planner: get_color_for_workflow(planner) 
                                for planner in df_errors['Planner'].unique()
                            }
                        )
                        
                        # Update layout
                        fig_errors.update_layout(
                            xaxis_title='Metric',
                            yaxis_title='Relative Error (%)',
                            legend_title='Planner',
                            plot_bgcolor='rgba(0,0,0,0.02)',
                            height=600,
                            xaxis_tickangle=-45,
                            boxmode='group',
                            margin=dict(t=60, b=120, l=60, r=60)
                        )
                        
                        # Add horizontal line at 0% error for reference
                        fig_errors.add_hline(y=0, line_dash='dash', line_color='gray', opacity=0.5)
                        
                        st.plotly_chart(fig_errors, use_container_width=True)

                        # Prepare data for box plots - calculate relative errors for all metrics
                        box_plot_data = []
                        for _, row in df_accuracy.iterrows():
                            for metric_display, (actual_col, pred_col) in metric_options.items():
                                if actual_col in row and pred_col in row and row[actual_col] > 0:
                                    relative_error = abs(row[actual_col] - row[pred_col]) / row[actual_col]
                                    box_plot_data.append({
                                        'Planner': row['Planner'],
                                        'Metric': metric_display,
                                        'Relative Error': relative_error * 100  # Convert to percentage
                                    })
                        
                        if box_plot_data:
                            df_box = pd.DataFrame(box_plot_data)
                            
                            # Create tabs for each metric
                            metric_tabs = st.tabs([f"{metric}" for metric in df_box['Metric'].unique()])
                            
                            for idx, metric in enumerate(df_box['Metric'].unique()):
                                with metric_tabs[idx]:
                                    metric_data = df_box[df_box['Metric'] == metric]
                                    
                                    # Create box plot
                                    fig_box = px.box(
                                        metric_data,
                                        x='Planner',
                                        y='Relative Error',
                                        color='Planner',
                                        title=f'Distribution of {metric} Prediction Errors',
                                        labels={
                                            'Relative Error': 'Prediction Error (%)',
                                            'Planner': ''
                                        },
                                        points="all",  # Show all points
                                        hover_data=['Relative Error'],
                                        color_discrete_sequence=px.colors.qualitative.Set1
                                    )
                                    
                                    # Customize layout
                                    fig_box.update_layout(
                                        showlegend=False,
                                        yaxis_title='Prediction Error (%)',
                                        xaxis_title='',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        height=500,
                                        margin=dict(t=40, b=100, l=50, r=50)
                                    )
                                    
                                    # Add horizontal line at 0% error for reference
                                    fig_box.add_hline(y=0, line_dash="dash", line_color="gray")
                                    
                                    # Add median and average markers
                                    for planner in metric_data['Planner'].unique():
                                        planner_data = metric_data[metric_data['Planner'] == planner]
                                        median_val = planner_data['Relative Error'].median()
                                        avg_val = planner_data['Relative Error'].mean()
                                        
                                        # Add average as a point
                                        fig_box.add_scatter(
                                            x=[planner],
                                            y=[avg_val],
                                            mode='markers',
                                            marker=dict(
                                                color='white',
                                                size=10,
                                                symbol='diamond'
                                            ),
                                            name="",
                                            showlegend=False
                                        )
                                        
                                        # Add average value label
                                        fig_box.add_annotation(
                                            x=planner,
                                            y=avg_val,
                                            text=f"Avg: {avg_val:.0f}%",
                                            showarrow=False,
                                            yshift=-15,
                                            font=dict(color='white', size=10)
                                        )
                                    
                                    st.plotly_chart(fig_box, use_container_width=True)
                        else:
                            st.warning("Not enough data to generate error distribution analysis.")
                
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
                    avg_memory_mb = total_memory_mb / len(instance.tasks) if instance.tasks else 0
                    
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
                        f'Input Size [{input_size_unit}]': input_size_value,  # Convert to MB
                        f'Output Size [{output_size_unit}]': output_size_value,  # Convert to MB
                        f'Total Data Transferred [{total_data_unit}]': total_data_value,  # Convert to MB
                        'Worker Startup Time [s]': instance.total_worker_startup_time_ms / 1000,
                        'Avg Memory Allocation [MB]': avg_memory_mb,
                        'Resource Usage': instance.resource_usage.cost
                    }
                    
                    # Add all metrics to the data list
                    for metric_name, value in instance_metrics.items():
                        metrics_data.append({
                            'Metric': metric_name,
                            'Value': value,
                            'Planner': instance.plan.planner_name if instance.plan else 'No Planner',
                            'Instance ID': instance.master_dag_id.split('-')[0]
                        })
                
                if metrics_data:
                    df_metrics = pd.DataFrame(metrics_data)
                    
                    # Get unique metrics for the dropdown
                    available_metrics = df_metrics['Metric'].unique().tolist()
                    
                    # Add metric selection dropdown
                    st.markdown("### Metrics Comparison (by Planner)")
                    selected_metric = st.selectbox(
                        'Select a metric to compare:',
                        available_metrics,
                        index=0,
                        key='metric_selector'
                    )
                    
                    # Filter data for the selected metric
                    df_metric = df_metrics[df_metrics['Metric'] == selected_metric]
                    
                    # Create box plot for the selected metric
                    fig = px.box(
                        df_metric,
                        x='Planner',
                        y='Value',
                        color='Planner',
                        title=f'{selected_metric} Distribution by Planner',
                        points="all",
                        hover_data=['Instance ID'],
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    
                    # Get the base metric name and unit for y-axis label
                    base_metric = selected_metric.split('[')[0].strip()
                    unit = ']' + selected_metric.split('[')[1] if '[' in selected_metric else ''
                    
                    # Add median and average markers for each planner
                    for planner in df_metric['Planner'].unique():
                        planner_data = df_metric[df_metric['Planner'] == planner]
                        median_val = planner_data['Value'].median()
                        avg_val = planner_data['Value'].mean()
                        
                        # Add average as a point
                        fig.add_scatter(
                            x=[planner],
                            y=[avg_val],
                            mode='markers',
                            marker=dict(
                                color='white',
                                size=10,
                                symbol='diamond'
                            ),
                            name='',
                            showlegend=False
                        )
                        
                        # Add average value label
                        fig.add_annotation(
                            x=planner,
                            y=avg_val,
                            text=f"Avg: {avg_val:.1f}",
                            showarrow=False,
                            yshift=-15,
                            font=dict(color='white', size=10)
                        )
                    
                    # Update layout with proper units
                    fig.update_layout(
                        xaxis_title='Planner',
                        yaxis_title=f"{base_metric} {unit}",
                        legend_title='Planner',
                        plot_bgcolor='rgba(0,0,0,0)',
                        boxmode='group',
                        height=500,
                        showlegend=False,  # Remove legend as it's redundant with x-axis
                        yaxis={
                            'title': f"{base_metric} {unit}",
                            'tickformat': '.2f' if 'Time' in base_metric or 'Makespan' in base_metric else None
                        }
                    )
                    
                    # Rotate x-axis labels if there are many planners
                    if len(df_metric['Planner'].unique()) > 3:
                        fig.update_xaxes(tickangle=45)
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Add pie charts for time distribution by activity for each planner
                    st.markdown("### Time Distribution by Activity (per Planner)")
                    
                    # Define the time metrics we want to include in the pie charts
                    time_metrics = ['Execution Time [s]', 'Total Time Waiting for Inputs [s]', 
                                  'Upload Time [s]', 'Worker Startup Time [s]']
                    
                    # Get unique planners and filter for time-related metrics
                    planner_names = df_metrics['Planner'].unique()
                    time_metrics_df = df_metrics[df_metrics['Metric'].isin(time_metrics)]
                    
                    # Create columns for the pie charts - one per planner
                    cols = st.columns(len(planner_names) if len(planner_names) > 0 else 1)
                    
                    for idx, planner_name in enumerate(planner_names):
                        with cols[idx % len(cols)]:
                            # Get metrics for this planner
                            planner_data = time_metrics_df[time_metrics_df['Planner'] == planner_name]
                            
                            # Create a dictionary with the time data
                            time_data = {}
                            for metric in time_metrics:
                                metric_data = planner_data[planner_data['Metric'] == metric]
                                time_data[metric] = metric_data['Value'].mean() if not metric_data.empty else 0
                            
                            # Create a DataFrame for the pie chart
                            df_pie = pd.DataFrame({
                                'Activity': list(time_data.keys()),
                                'Time (s)': list(time_data.values())
                            })
                            
                            # Create the pie chart with legend
                            fig_pie = px.pie(
                                df_pie, 
                                values='Time (s)', 
                                names='Activity',
                                title=f'{planner_name} Time Distribution',
                                color='Activity',
                                color_discrete_sequence=px.colors.qualitative.Pastel1
                            )
                            
                            # Update layout for better visualization
                            fig_pie.update_traces(
                                textposition='inside',
                                textinfo='percent+label',
                                hovertemplate='%{label}: %{value:.2f}s (%{percent})',
                                textfont_size=12,
                                # Enable click-to-toggle behavior
                                customdata=df_pie['Activity'],
                                selector=dict(type='pie')
                            )
                            
                            fig_pie.update_layout(
                                showlegend=True,
                                legend=dict(
                                    title='Legend',
                                    orientation='h',
                                    yanchor='bottom',
                                    y=-0.2,
                                    xanchor='center',
                                    x=0.5
                                ),
                                margin=dict(t=30, b=80, l=10, r=10),
                                height=450,
                                title_x=0.5,
                                title_font_size=14,
                                # Enable click-to-toggle functionality
                                clickmode='event+select'
                            )
                            
                            # Add click-to-toggle functionality
                            fig_pie.update_traces(
                                hovertemplate='%{label}: %{value:.2f}s (%{percent})<extra></extra>',
                                selector=dict(type='pie')
                            )
                            
                            st.plotly_chart(fig_pie, use_container_width=True)
                
                # Calculate metrics by planner type
                planner_metrics = {}
                
                for instance in workflow_types[selected_workflow].instances:
                    if not instance.plan or not instance.tasks:
                        continue
                        
                    planner = instance.plan.planner_name if instance.plan else 'Unknown'
                    if planner not in planner_metrics:
                        planner_metrics[planner] = {
                            'count': 0,
                            'makespan': 0,
                            'execution': 0,
                            'download': 0,
                            'upload': 0,
                            'input_size': 0,
                            'output_size': 0,
                            'data_transferred': 0,
                            'invocation': 0,
                            'dependency_update': 0,
                            'dag_download': 0,
                            'worker_startup': 0,
                            'resource_usage': 0
                        }
                    
                    # Calculate metrics for this instance
                    metrics = planner_metrics[planner]
                    metrics['count'] += 1
                    sink_task_metrics = [t for t in instance.tasks if t.internal_task_id == instance.dag.sink_node.id.get_full_id()][0].metrics
                    sink_task_ended_timestamp_ms = (sink_task_metrics.started_at_timestamp_s * 1000) + (sink_task_metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + (sink_task_metrics.tp_execution_time_ms or 0) + (sink_task_metrics.output_metrics.tp_time_ms or 0) + (sink_task_metrics.total_invocation_time_ms or 0)
                    actual_makespan_s = (sink_task_ended_timestamp_ms - instance.start_time_ms) / 1000
                    metrics['makespan'] += actual_makespan_s
                    metrics['execution'] += sum(task.metrics.tp_execution_time_ms / 1000 for task in instance.tasks)
                    metrics['download'] += sum(
                        sum(input_metric.time_ms / 1000 
                            for input_metric in task.metrics.input_metrics.input_download_metrics.values() 
                            if input_metric.time_ms is not None)
                        for task in instance.tasks
                    )
                    metrics['upload'] += sum(
                        task.metrics.output_metrics.tp_time_ms / 1000 
                        for task in instance.tasks 
                        if task.metrics.output_metrics.tp_time_ms is not None
                    )
                    metrics['input_size'] += sum(
                        sum(input_metric.serialized_size_bytes 
                            for input_metric in task.metrics.input_metrics.input_download_metrics.values())
                        for task in instance.tasks
                    )
                    metrics['output_size'] += sum(
                        task.metrics.output_metrics.serialized_size_bytes 
                        for task in instance.tasks
                    )
                    metrics['data_transferred'] += instance.total_transferred_data_bytes
                    metrics['invocation'] += sum(
                        task.metrics.total_invocation_time_ms / 1000 
                        for task in instance.tasks 
                        if task.metrics.total_invocation_time_ms is not None
                    )
                    metrics['dependency_update'] += sum(
                        task.metrics.update_dependency_counters_time_ms / 1000 
                        for task in instance.tasks 
                        if hasattr(task.metrics, 'update_dependency_counters_time_ms') and 
                           task.metrics.update_dependency_counters_time_ms is not None
                    )
                    metrics['dag_download'] += sum(
                        stat.download_time_ms / 1000 
                        for stat in instance.dag_download_stats
                    )
                    metrics['worker_startup'] += instance.total_worker_startup_time_ms / 1000
                    metrics['resource_usage'] += instance.resource_usage.cost
                
                if planner_metrics:
                    # Calculate averages
                    for planner_name, metrics in planner_metrics.items():
                        count = metrics['count']
                        if count > 0:
                            for key in metrics.keys():
                                if key != 'count':
                                    metrics[key] /= count
                    
                    # Prepare data for plotting
                    plot_data = []
                    for planner_name, metrics in planner_metrics.items():
                        plot_data.extend([
                            {'Planner': planner_name, 'Metric': 'Makespan (s)', 'Value': metrics['makespan']},
                            {'Planner': planner_name, 'Metric': 'Execution Time (s)', 'Value': metrics['execution']},
                            {'Planner': planner_name, 'Metric': 'Download Time (s)', 'Value': metrics['download']},
                            {'Planner': planner_name, 'Metric': 'Upload Time (s)', 'Value': metrics['upload']},
                            {'Planner': planner_name, 'Metric': 'Data Transferred (MB)', 'Value': metrics['data_transferred'] / 1024 / 1024},
                            {'Planner': planner_name, 'Metric': 'Task Invocation Time (s)', 'Value': metrics['invocation']},
                            {'Planner': planner_name, 'Metric': 'Dependency Counter Update Time (s)', 'Value': metrics['dependency_update']},
                            {'Planner': planner_name, 'Metric': 'DAG Download Time (s)', 'Value': metrics['dag_download']},
                            {'Planner': planner_name, 'Metric': 'Worker Startup Time (s)', 'Value': metrics['worker_startup']},
                            {'Planner': planner_name, 'Metric': 'Resource Usage', 'Value': metrics['resource_usage']},
                        ])
                    
                    df_planner_metrics = pd.DataFrame(plot_data)
                    
                    # Create bar chart
                    fig = px.bar(
                        df_planner_metrics,
                        x='Metric',
                        y='Value',
                        color='Planner',
                        barmode='group',
                        title='Average Actual Metrics by Planner Type',
                        labels={'Value': 'Value', 'Metric': 'Metric'}
                    )
                    
                    # Update layout for better visualization
                    fig.update_layout(
                        xaxis_title='Metric',
                        yaxis_title='Value',
                        legend_title='Planner',
                        plot_bgcolor='rgba(0,0,0,0)',
                        yaxis_type='log',
                        height=600,
                        xaxis={'categoryorder':'total descending'}
                    )
                    
                    # Add value labels on top of bars
                    fig.update_traces(
                        texttemplate='%{y:.2f}',
                        textposition='outside',
                        textfont_size=8
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No instance data available for the selected filters.")

if __name__ == "__main__":
    main()