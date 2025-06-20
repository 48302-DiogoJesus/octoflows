import seaborn as sns
import colorsys
from datetime import datetime
import hashlib
import os
import sys
from matplotlib import pyplot as plt
import numpy as np
from streamlit_agraph import agraph, Node, Edge, Config
import streamlit as st
import redis
import cloudpickle
import pandas as pd
import plotly.express as px
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage.prefixes import DAG_PREFIX
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.storage.metrics.metrics_types import FullDAGPrepareTime, TaskMetrics
from src.dag.dag import FullDAG
from src.dag_task_node import DAGTaskNode
from src.storage.metrics.metrics_storage import MetricsStorage

# Redis connection setup
def get_redis_connection(port: int = 6379):
    return redis.Redis(
        host='localhost',
        port=port,
        password='redisdevpwd123',
        decode_responses=False
    )

def format_bytes(size: float) -> str:
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"

def get_function_group(task_id: str, func_name: str) -> str:
    """Extract the function group from task ID and function name"""
    # For tasks like "grayscale_image_part-1e783776-abe4-4e1a-8ec2-81df12192b0b"
    # we want to group them as "grayscale_image_part"
    if '-' in task_id:
        base_part = task_id.split('-')[0]
        if base_part.startswith(func_name.split('_')[0]):
            return base_part
    return func_name

def main():
    # Configure page layout for wider usage
    st.set_page_config(layout="wide")
    st.title("Workflow Instance Analysis Dashboard")
    
    # Connect to Redis
    dag_redis = get_redis_connection(6379)
    metrics_redis = get_redis_connection(6380)
    
    # Get all DAG keys
    dag_keys = [key for key in dag_redis.keys() if key.decode('utf-8').startswith(DAG_PREFIX)]
    
    if not dag_keys:
        st.warning("No DAGs found in Redis")
        return
    
    # Sort DAG keys for consistent display
    dag_keys_sorted = sorted(dag_keys, key=lambda x: x.decode('utf-8'))
    
    # Select DAG - use session state to track changes
    if 'prev_dag_key' not in st.session_state:
        st.session_state.prev_dag_key = None
    
    selected_dag_key = st.selectbox(
        "Current DAG",
        options=dag_keys_sorted,
        format_func=lambda x: x.decode('utf-8')
    )
    
    # Reset task selection if DAG changed
    if selected_dag_key != st.session_state.prev_dag_key:
        if 'selected_task_id' in st.session_state:
            del st.session_state.selected_task_id
        st.session_state.prev_dag_key = selected_dag_key
    
    # Deserialize DAG
    try:
        dag_data = dag_redis.get(selected_dag_key)
        dag: FullDAG = cloudpickle.loads(dag_data)
    except Exception as e:
        st.error(f"Failed to deserialize DAG: {e}")
        return
    
    # Collect all metrics for this DAG
    dag_metrics: list[TaskMetrics] = []
    total_data_transferred = 0
    total_time_executing_tasks_ms = 0
    total_time_uploading_data_ms = 0
    total_time_downloading_data_ms = 0
    total_time_invoking_tasks_ms = 0
    total_time_updating_dependency_counters_ms = 0
    task_metrics_data = []
    function_groups = set()
    
    _task_with_earliest_start_time = None
    _sink_task_metrics = None
    for task_id in dag._all_nodes.keys():
        metrics_key = f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}{task_id}_{dag.master_dag_id}"
        metrics_data = metrics_redis.get(metrics_key)
        if not metrics_data: raise Exception(f"Could not find metrics for key: {metrics_key}")
        metrics: TaskMetrics = cloudpickle.loads(metrics_data) # type: ignore
        dag_metrics.append(metrics)
        
        func_name = dag._all_nodes[task_id].func_name
        function_groups.add(func_name)

        if task_id == dag.sink_node.id.get_full_id():
            _sink_task_metrics = metrics

        if _task_with_earliest_start_time is None or metrics.started_at_timestamp_s < _task_with_earliest_start_time.started_at_timestamp_s:
            _task_with_earliest_start_time = metrics

        total_time_invoking_tasks_ms += metrics.total_invocation_time_ms if metrics.total_invocation_time_ms is not None else 0
        total_time_updating_dependency_counters_ms += metrics.update_dependency_counters_time_ms if metrics.update_dependency_counters_time_ms is not None else 0

        # Calculate data transferred
        task_data = 0
        total_time_downloading_data_ms += metrics.input_metrics.tp_total_time_waiting_for_inputs_ms if metrics.input_metrics.tp_total_time_waiting_for_inputs_ms is not None else 0
        if metrics.output_metrics:
            task_data += metrics.output_metrics.deserialized_size_bytes
            total_time_uploading_data_ms += metrics.output_metrics.tp_time_ms if metrics.output_metrics.tp_time_ms is not None else 0
        
        total_data_transferred += task_data
        total_time_executing_tasks_ms += metrics.tp_execution_time_ms if metrics.tp_execution_time_ms is not None else 0

        downloadable_input_size_bytes = sum([input_metrics.deserialized_size_bytes for input_metrics in metrics.input_metrics.input_download_metrics.values()])
        # Prepare data for visualization
        task_metrics_data.append({
            'task_id': task_id,
            'task_started_at': datetime.fromtimestamp(metrics.started_at_timestamp_s).strftime("%Y-%m-%d %H:%M:%S:%f"),
            'function_name': func_name,
            'execution_time_ms': metrics.tp_execution_time_ms,
            'worker_id': metrics.worker_resource_configuration.worker_id,
            'worker_resource_configuration_cpus': metrics.worker_resource_configuration.cpus,
            'worker_resource_configuration_ram': metrics.worker_resource_configuration.memory_mb,
            'input_size': downloadable_input_size_bytes,
            'output_size': metrics.output_metrics.deserialized_size_bytes,
            'downstream_calls': metrics.total_invocations_count
        })
    
    assert _sink_task_metrics
    assert _task_with_earliest_start_time is not None

    sink_task_ended_timestamp_ms = (_sink_task_metrics.started_at_timestamp_s * 1000) + (_sink_task_metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + _sink_task_metrics.tp_execution_time_ms + (_sink_task_metrics.output_metrics.tp_time_ms or 0) + (_sink_task_metrics.total_invocation_time_ms or 0)
    makespan_ms = sink_task_ended_timestamp_ms - (_task_with_earliest_start_time.started_at_timestamp_s * 1000)

    keys = metrics_redis.keys(f'{MetricsStorage.DAG_METRICS_KEY_PREFIX}*')
    total_time_downloading_dag_ms = 0
    dag_prepare_metrics = []
    for key in keys:
        serialized_value = metrics_redis.get(key)
        deserialized: FullDAGPrepareTime = cloudpickle.loads(serialized_value) # type: ignore
        if not isinstance(deserialized, FullDAGPrepareTime): raise Exception(f"Deserialized value is not of type TaskMetrics: {type(deserialized)}")
        total_time_downloading_dag_ms += deserialized.download_time_ms
        dag_prepare_metrics.append({
            "dag_download_time": deserialized.download_time_ms,
            "create_subdag_time": deserialized.create_subdags_time_ms,
            "dag_size": deserialized.serialized_size_bytes
        })

    # Calculate task timing metrics (start times, end times)
    task_timings = {}
    
    # First pass: collect all task metrics and find the minimum start time
    min_start_time = None
    for task_id, metrics in zip(dag._all_nodes.keys(), dag_metrics):
        task_start_time = metrics.started_at_timestamp_s
        task_timings[task_id] = {
            'start_time': task_start_time,
            'end_time': None
        }
        
        if min_start_time is None or task_start_time < min_start_time:
            min_start_time = task_start_time
    
    assert min_start_time is not None

    # Second pass: calculate end times relative to min_start_time
    for task_id, metrics in zip(dag._all_nodes.keys(), dag_metrics):
        relative_start_time = (metrics.started_at_timestamp_s - min_start_time) * 1000  # Convert to ms
        end_time = relative_start_time + metrics.input_metrics.tp_total_time_waiting_for_inputs_ms if metrics.input_metrics.tp_total_time_waiting_for_inputs_ms else 0 + metrics.tp_execution_time_ms if metrics.tp_execution_time_ms else 0 + metrics.total_invocation_time_ms if metrics.total_invocation_time_ms else 0
        if metrics.output_metrics: end_time += metrics.output_metrics.tp_time_ms if metrics.output_metrics.tp_time_ms else 0
        task_timings[task_id]['end_time'] = end_time
    
    # Update task_metrics_data with the calculated timing information
    for i, task_data in enumerate(task_metrics_data):
        task_id = task_data['task_id']
        task_metrics_data[i].update({
            'relative_start_time_ms': (task_timings[task_id]['start_time'] - min_start_time) * 1000,
            'end_time_ms': task_timings[task_id]['end_time']
        })
    
    # Make timing information available for the rest of the code
    min_start_time_ms = min_start_time * 1000 if min_start_time else None  # Convert to ms for consistency
    task_end_times = {task_id: timing['end_time'] for task_id, timing in task_timings.items()}
    task_start_times = {task_id: timing['start_time'] for task_id, timing in task_timings.items()}

    # Create tabs for visualization and metrics
    tab_viz, tab_summary, tab_exec, tab_data, tab_workers = st.tabs([
        "Visualization", 
        "Summary", 
        "Execution Times", 
        "Data Transfer", 
        "Worker Distribution"
    ])
    
    # Visualization tab
    with tab_viz:
        # Create columns for graph and task details
        graph_col, details_col = st.columns([2, 1])
        
        def get_color_for_worker(worker_id):
            # Create a hash of the worker_id
            hash_obj = hashlib.md5(worker_id.encode())
            hash_int = int(hash_obj.hexdigest(), 16)
            
            # Generate a hue value that is more spaced out
            hue = (hash_int % 360)  # Full hue spectrum (0-359 degrees)
            saturation = 0.7  # Keep colors vibrant
            lightness = 0.5   # Ensure colors are not too dark or too bright

            # Convert HSL to RGB (values between 0-1)
            r, g, b = colorsys.hls_to_rgb(hue / 360, lightness, saturation)

            # Scale to 0-255 and format as RGB
            return f"rgb({int(r * 255)},{int(g * 255)},{int(b * 255)})"
            
        with graph_col:
            nodes = []
            edges = []
            node_levels = {}  # Tracks hierarchy levels
            visited = set()
            
            # Find critical path (longest path from source to sink)
            def find_critical_path():
                # Perform DFS to find the longest path from source to sink
                max_path = []
                max_length = -1
                
                def dfs(node, path, current_length):
                    nonlocal max_path, max_length
                    node_id = node.id.get_full_id()
                    
                    task_metrics = next((t for t in task_metrics_data if t['task_id'] == node_id), None)
                    if not task_metrics:
                        return
                        
                    task_time = task_metrics.get('end_time_ms', 0)
                    
                    # Update path and length
                    new_path = path + [node_id]
                    new_length = current_length + task_time
                    
                    # If this is the sink node, check if it's the longest path
                    if node_id == dag.sink_node.id.get_full_id():
                        if new_length > max_length:
                            max_length = new_length
                            max_path = new_path
                        return
                    
                    # Continue DFS to downstream nodes
                    for downstream in node.downstream_nodes:
                        dfs(downstream, new_path, new_length)
                
                # Start DFS from all root nodes
                for root in dag.root_nodes:
                    dfs(root, [], 0)
                
                # Convert the critical path to a set for O(1) lookups
                critical_nodes = set(max_path)
                critical_edges = set(zip(max_path[:-1], max_path[1:]))
                return critical_nodes, critical_edges
            
            # Get actual critical path
            critical_nodes, critical_edges = find_critical_path()
            
            # Get planned critical path if available
            planned_critical_edges = set()
            try:
                plan_key = f"{MetricsStorage.PLAN_KEY_PREFIX}{dag.master_dag_id}"
                plan_data = metrics_redis.get(plan_key)
                if plan_data:
                    # Ensure we have bytes before attempting to deserialize
                    if isinstance(plan_data, bytes):
                        plan: AbstractDAGPlanner.PlanOutput = cloudpickle.loads(plan_data)
                        if hasattr(plan, 'critical_path_node_ids') and plan.critical_path_node_ids:
                            planned_path = list(plan.critical_path_node_ids)
                            if len(planned_path) > 1:
                                planned_critical_edges = set(zip(planned_path[:-1], planned_path[1:]))
                    else:
                        st.warning(f"Expected bytes from Redis, got {type(plan_data).__name__}")
            except Exception as e:
                st.warning(f"Could not load planned critical path: {e}")
            
            def traverse_dag(node: DAGTaskNode, level=0):
                """ Recursively traverse DAG from root nodes """
                node_id = node.id.get_full_id()
                worker_id = [m for m in task_metrics_data if m["task_id"] == node_id][0]['worker_id']

                if node_id in visited:
                    return  # Prevents duplicate processing

                visited.add(node_id)
                node_levels[node_id] = level

                # Create node (no special styling for critical path nodes)
                node_color = get_color_for_worker(worker_id)
                
                nodes.append(Node(
                    id=node_id, 
                    label=node.func_name,
                    title=f"task_id: {node_id}\nworker_id: {worker_id}",
                    size=20,
                    color=node_color,
                    shape="dot",
                    font={"color": "white", "size": 10, "face": "Arial"},
                    level=level
                ))

                # Process downstream nodes - highlight actual and planned critical paths
                for downstream in node.downstream_nodes:
                    downstream_id = downstream.id.get_full_id()
                    edge = (node_id, downstream_id)
                    is_actual_critical = edge in critical_edges
                    is_planned_critical = edge in planned_critical_edges
                    
                    # Determine edge color and title
                    if is_actual_critical and is_planned_critical:
                        edge_color = "#FF0000"  # Red - both actual and planned critical
                        edge_title = "Critical Path (Actual & Planned)"
                    elif is_actual_critical:
                        edge_color = "#FF0000"  # Red - only actual critical
                        edge_title = "Critical Path (Actual)"
                    elif is_planned_critical:
                        edge_color = "#FFD700"  # Yellow - only planned critical
                        edge_title = "Critical Path (Planned)"
                    else:
                        edge_color = "#888888"  # Gray - not critical
                        edge_title = ""
                    
                    edges.append(Edge(
                        source=node_id, 
                        target=downstream_id, 
                        arrow="to",
                        color=edge_color,
                        width=2 if (is_actual_critical or is_planned_critical) else 1,
                        title=edge_title
                    ))
                    traverse_dag(downstream, level + 1)
                    
            # Start traversal from all root nodes
            assert dag.root_nodes
            for root in dag.root_nodes:
                traverse_dag(root, level=0)

            # Graph configuration
            config = Config(
                width="70%", # type: ignore
                height=600,
                directed=True,
                physics=False,
                hierarchical=True,
                hierarchical_sort_method="directed"
            )

            # Get selected node from graph interaction
            selected_node = agraph(nodes=nodes, edges=edges, config=config)
            
            # Update selected task if a node was clicked
            if selected_node and selected_node in dag._all_nodes:
                st.session_state.selected_task_id = selected_node
        
        with details_col:
            st.subheader("Selected Task Details")
            
            # Initialize selected_task_id if not set
            if 'selected_task_id' not in st.session_state:
                st.session_state.selected_task_id = list(dag._all_nodes.keys())[0] if dag._all_nodes else None
            
            if st.session_state.selected_task_id:
                # Get the task node
                task_node = dag._all_nodes[st.session_state.selected_task_id]
                
                # Try to find metrics for this task
                metrics_key = f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}{st.session_state.selected_task_id}_{dag.master_dag_id}"
                metrics_data = metrics_redis.get(metrics_key)
                if not metrics_data: raise Exception(f"Metrics not found for key {metrics_key}")
                
                metrics = cloudpickle.loads(metrics_data) # type: ignore
                
                # Basic task info
                st.metric("Function", task_node.func_name)
                st.metric("Worker", metrics.worker_resource_configuration.worker_id)
                st.metric("Worker Resources", f"{metrics.worker_resource_configuration.cpus} CPUs + {metrics.worker_resource_configuration.memory_mb} MB")
                col1, col2 = st.columns(2)
                output_data = metrics.output_metrics.deserialized_size_bytes
                with col1:
                    total_task_handling_time = (metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0) + (metrics.tp_execution_time_ms or 0) + (metrics.update_dependency_counters_time_ms or 0) + (metrics.output_metrics.tp_time_ms or 0) + (metrics.total_invocation_time_ms or 0)
                    st.metric("Total Task Handling Time", f"{total_task_handling_time:.2f} ms")
                    st.metric("Time Waiting for Dependencies", f"{(metrics.input_metrics.tp_total_time_waiting_for_inputs_ms or 0):.2f} ms")
                    st.metric("DC Updates Time", f"{(metrics.update_dependency_counters_time_ms or 0):.2f} ms")
                    st.metric("Output Upload Time", f"{(metrics.output_metrics.tp_time_ms or 0):.2f} ms")
                    st.metric("Input Size", format_bytes(sum([input_metric.deserialized_size_bytes for input_metric in metrics.input_metrics.input_download_metrics.values()]) + metrics.input_metrics.hardcoded_input_size_bytes))
                with col2:
                    st.metric("", "")
                    st.metric("", "")
                    st.metric("Total Time Downloading Dependencies", f"{sum([input_metric.time_ms for input_metric in metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms]):.2f} ms")
                    st.metric("Task Execution Time", f"{(metrics.tp_execution_time_ms or 0):.2f} ms")
                    st.metric("Downstream Invocations Time", f"{(metrics.total_invocation_time_ms or 0):.2f} ms")
                    st.metric("Output Size", format_bytes(output_data))
                
                # Add planned vs observed metrics if available
                st.subheader("Planned vs Observed Metrics")
                plan_key = f"{MetricsStorage.PLAN_KEY_PREFIX}{dag.master_dag_id}"
                plan_data = metrics_redis.get(plan_key)

                if plan_data:
                    try:
                        plan: AbstractDAGPlanner.PlanOutput = cloudpickle.loads(plan_data) # type: ignore
                        tp: AbstractDAGPlanner.PlanningTaskInfo | None = plan.nodes_info.get(st.session_state.selected_task_id)
                        
                        if tp:
                            # Get the current task's timing metrics
                            current_task_metrics = next(
                                (t for t in task_metrics_data if t['task_id'] == st.session_state.selected_task_id),
                                None
                            )
                            
                            if current_task_metrics:
                                # Calculate all the metrics we want to compare
                                output_size = metrics.output_metrics.deserialized_size_bytes if metrics.output_metrics else 0
                                actual_start_time = current_task_metrics['relative_start_time_ms']
                                end_time_ms = current_task_metrics['end_time_ms']
                                
                                # Create columns for comparison
                                col_metric, col_planned, col_observed, col_diff = st.columns([2, 1, 1, 1])

                                # Add header
                                with col_metric:
                                    st.markdown("**Metric**")
                                with col_planned:
                                    st.markdown("**Planned**")
                                with col_observed:
                                    st.markdown("**Observed**")
                                with col_diff:
                                    st.markdown("**Difference**")
                                
                                # Comparison fields
                                with col_metric:
                                    st.text('Input Size (bytes)')
                                    st.text('Output Size (bytes)')
                                    st.text('Downloading Deps. (ms)')
                                    st.text('Execution Time (ms)')
                                    st.text('Upload Time (ms)')
                                    st.text('Earliest Start (ms)')
                                    st.text('End Time (ms)')
                                with col_planned:
                                    st.text(format_bytes(tp.input_size))
                                    st.text(format_bytes(tp.output_size))
                                    st.text(f"{float(tp.total_download_time):.2f} ms")
                                    st.text(f"{float(tp.exec_time):.2f} ms")
                                    st.text(f"{float(tp.upload_time):.2f} ms")
                                    st.text(f"{float(tp.earliest_start):.2f} ms")
                                    st.text(f"{float(tp.path_completion_time):.2f} ms")
                                
                                with col_observed:
                                    st.text(format_bytes(sum([input_metric.deserialized_size_bytes for input_metric in metrics.input_metrics.input_download_metrics.values()]) + metrics.input_metrics.hardcoded_input_size_bytes))
                                    st.text(format_bytes(output_size))
                                    time_downloading_inputs = sum([input_metric.time_ms for input_metric in metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms])
                                    st.text(f"{float(time_downloading_inputs):.2f} ms")
                                    st.text(f"{float(metrics.tp_execution_time_ms):.2f} ms")
                                    st.text(f"{float(metrics.output_metrics.tp_time_ms or 0):.2f} ms")
                                    st.text(f"{float(actual_start_time):.2f} ms")
                                    st.text(f"{float(end_time_ms):.2f} ms")
                                
                                # Calculate and display difference
                                def get_diff_style(percentage):
                                    """Returns appropriate color style based on percentage difference"""
                                    if percentage is None:
                                        return ""
                                    if abs(percentage) > 70:
                                        return "color: red;"
                                    return "color: green;"

                                def format_percentage(diff, total):
                                    """Calculate and format percentage difference"""
                                    if total == 0:
                                        return None, "N/A"
                                    percentage = (diff / total) * 100
                                    return percentage, f"{percentage:+.2f}%"

                                with col_diff:
                                    # Input Size difference
                                    planned_input = tp.input_size
                                    observed_input = sum([input_metric.deserialized_size_bytes for input_metric in metrics.input_metrics.input_download_metrics.values()]) + metrics.input_metrics.hardcoded_input_size_bytes
                                    pct, pct_str = format_percentage(observed_input - planned_input, planned_input)
                                    st.markdown(f"<span style='{get_diff_style(pct)}'>{pct_str}</span>", unsafe_allow_html=True)
                                    
                                    # Output Size difference
                                    planned_output = tp.output_size
                                    pct, pct_str = format_percentage(output_size - planned_output, planned_output)
                                    st.markdown(f"<span style='{get_diff_style(pct)}'>{pct_str}</span>", unsafe_allow_html=True)

                                    # Time Downloading Dependencies difference
                                    planned_download = float(tp.total_download_time)
                                    observed_download = float(time_downloading_inputs)
                                    if planned_download is not None and observed_download is not None and planned_download != 0:
                                        pct = ((observed_download - planned_download) / planned_download) * 100
                                        st.markdown(f"<span style='{get_diff_style(pct)}'>{pct:+.2f}%</span>", unsafe_allow_html=True)
                                    else:
                                        st.text("N/A")
                                    
                                    # Execution Time difference
                                    planned_exec = float(tp.exec_time)
                                    observed_exec = float(metrics.tp_execution_time_ms)
                                    if planned_exec is not None and observed_exec is not None and planned_exec != 0:
                                        pct = ((observed_exec - planned_exec) / planned_exec) * 100
                                        st.markdown(f"<span style='{get_diff_style(pct)}'>{pct:+.2f}%</span>", unsafe_allow_html=True)
                                    else:
                                        st.text("N/A")

                                    # Upload Time difference
                                    planned_upload = float(tp.upload_time)
                                    observed_upload = float(metrics.output_metrics.tp_time_ms or 0)
                                    if planned_upload is not None and observed_upload is not None and planned_upload != 0:
                                        pct = ((observed_upload - planned_upload) / planned_upload) * 100
                                        st.markdown(f"<span style='{get_diff_style(pct)}'>{pct:+.2f}%</span>", unsafe_allow_html=True)
                                    else:
                                        st.text("N/A")
                                    
                                    # Earliest Start difference
                                    planned_start = float(tp.earliest_start)
                                    observed_start = float(actual_start_time)
                                    if planned_start is not None and observed_start is not None and planned_start != 0:
                                        pct = ((observed_start - planned_start) / planned_start) * 100
                                        st.markdown(f"<span style='{get_diff_style(pct)}'>{pct:+.2f}%</span>", unsafe_allow_html=True)
                                    else:
                                        st.text("N/A")
                                    
                                    # End Time difference
                                    planned_end = float(tp.path_completion_time)
                                    observed_end = float(end_time_ms)
                                    if planned_end is not None and observed_end is not None and planned_end != 0:
                                        pct = ((observed_end - planned_end) / planned_end) * 100
                                        st.markdown(f"<span style='{get_diff_style(pct)}'>{pct:+.2f}%</span>", unsafe_allow_html=True)
                                    else:
                                        st.text("N/A")
                        else:
                            st.warning("No planning data available for selected task")
                    except Exception as e:
                        st.error(f"Error loading plan data: {str(e)}")
                
    # Metrics tabs (unchanged from original)
    with tab_summary:
        # Create dataframe for visualizations
        metrics_df = pd.DataFrame(task_metrics_data)
        grouped_df = metrics_df.groupby('function_name').agg({
            'execution_time_ms': ['sum', 'mean', 'count']
        }).reset_index()
        
        # Flatten multi-index columns
        grouped_df.columns = ['_'.join(col).strip('_') for col in grouped_df.columns.values]
        
        # Worker distribution data
        worker_df = metrics_df.groupby(['function_name', 'worker_id']).agg({
            'execution_time_ms': 'sum',
            'task_id': 'count'
        }).reset_index()
        worker_df = worker_df.rename(columns={'task_id': 'task_count'})
        
        # DAG Summary Stats in columns
        # Calculate predicted makespan
        plan_key = f"{MetricsStorage.PLAN_KEY_PREFIX}{dag.master_dag_id}"
        plan_data = metrics_redis.get(plan_key)
        plan_output: AbstractDAGPlanner.PlanOutput | None = None
        if plan_data:
            plan_output = cloudpickle.loads(plan_data) # type: ignore

        predicted_makespan = 0.0
        if plan_output:
            earliest_finish = {node_id: 0.0 for node_id in plan_output.nodes_info}
            for node_id, info in plan_output.nodes_info.items():
                node_duration = (info.download_time + info.exec_time + info.upload_time) / 1000  # Convert to seconds
                max_upstream_finish = 0.0
                for upstream_node in info.node_ref.upstream_nodes:
                    upstream_id = upstream_node.id.get_full_id()
                    if upstream_id in earliest_finish:
                        max_upstream_finish = max(max_upstream_finish, earliest_finish[upstream_id])
                earliest_finish[node_id] = max_upstream_finish + node_duration
            predicted_makespan = max(earliest_finish.values()) * 1000 if earliest_finish else 0.0  # Convert back to ms

        col1, col2, col3, col4, col5 = st.columns(5)
        task_execution_time_avg = total_time_executing_tasks_ms / len(dag_metrics) if dag_metrics else 0
        avg_dag_download_time = sum(m['dag_download_time'] for m in dag_prepare_metrics) / len(dag_prepare_metrics)
        avg_subdag_create_time = sum(m['create_subdag_time'] for m in dag_prepare_metrics) / len(dag_prepare_metrics)
        avg_dag_size = sum(m['dag_size'] for m in dag_prepare_metrics) / len(dag_prepare_metrics)
        with col1:
            st.metric("Total Tasks", len(dag._all_nodes))
            st.metric(f"Total Time Executing Tasks (avg: {task_execution_time_avg:.2f} ms)", f"{total_time_executing_tasks_ms:.2f} ms")
            st.metric("Total Data Transferred", format_bytes(total_data_transferred))
            st.metric("Avg. DAG Download Time", f"{avg_dag_download_time:.2f} ms")
        with col2:
            if predicted_makespan > 0:
                percentage_diff = ((makespan_ms - predicted_makespan) / predicted_makespan) * 100
                st.metric(
                    "Makespan", 
                    f"{makespan_ms:.2f} ms",
                    delta=f"{percentage_diff:+.1f}% vs predicted",
                    help=f"Predicted: {predicted_makespan:.2f} ms"
                )
            else:
                st.metric("Makespan", f"{makespan_ms:.2f} ms")
            st.metric("Total Upload Time", f"{total_time_uploading_data_ms:.2f} ms")
            avg_data = total_data_transferred / len(dag_metrics) if dag_metrics else 0
            st.metric("Data Transferred per Task (avg)", format_bytes(avg_data))
            st.metric("DAG Size", format_bytes(avg_dag_size))
        with col3:
            st.metric("Unique Workers", int(metrics_df['worker_id'].nunique()))
            st.metric("Total Download Time", f"{total_time_downloading_data_ms:.2f} ms")
            st.metric("Total Worker Invocations (excludes initial)", f"{int(metrics_df['downstream_calls'].sum())}")
            st.metric("Total Time Downloading DAG", f"{total_time_downloading_dag_ms:.2f} ms")
        with col4:
            st.metric("Unique Tasks", len(function_groups))
            st.metric("Total Invocation Time", f"{total_time_invoking_tasks_ms:.2f} ms")
            st.metric(" ", " ", help="")
            st.metric(" ", " ", help="")
            st.metric("Avg. SubDAG Create Time", f"{avg_subdag_create_time:.2f} ms")
        with col5:
            st.metric("Total DC Update Time", f"{total_time_updating_dependency_counters_ms:.2f} ms")
            st.metric(" ", " ", help="")
            st.metric(" ", " ", help="")

        breakdown_data = {
            "Task Execution": total_time_executing_tasks_ms,
            "Data Download": total_time_downloading_data_ms,
            "Data Upload": total_time_uploading_data_ms,
            "Invocation Time": total_time_invoking_tasks_ms,
            "DC Updates": total_time_updating_dependency_counters_ms,
            "DAG Download Time": total_time_downloading_dag_ms
        }
        
        # Create pie chart
        breakdown_df = pd.DataFrame({
            "Component": breakdown_data.keys(),
            "Time (ms)": breakdown_data.values()
        })
       
        st.subheader("Times Breakdown")
        fig = px.pie(
            breakdown_df,
            names="Component",
            values="Time (ms)",
            title="",
            color="Component",
            color_discrete_map={
                "Task Execution": "#636EFA",
                "Data Download": "#EF553B",
                "Data Upload": "#00CC96",
                "Invocation Time": "#AB63FA",
                "DC Updates": "#DB61CE",
                "Unknown": "#333333"
            }
        )
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate="%{label}:<br>%{value:.2f} ms<br>%{percent}"
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Metrics by Function/Task type")
        st.dataframe(grouped_df, use_container_width=True)

        st.subheader("Raw Task Metrics")
        raw_task_df = metrics_df.sort_values('function_name').reset_index(drop=True)
        columns_to_exclude = { "function_name" }
        columns_to_show = [col for col in raw_task_df.columns if col not in columns_to_exclude]
        st.dataframe(
            raw_task_df[columns_to_show],
            use_container_width=True,
            column_config={
                "task_id": st.column_config.TextColumn("Task ID"),
                "task_started_at": st.column_config.TextColumn("Started At"),
                "execution_time_ms": st.column_config.NumberColumn("Exec Time (ms)", format="%.2f"),
                "worker_id": st.column_config.TextColumn("Worker"),
                "worker_resource_configuration_cpus": st.column_config.TextColumn("Worker Resources (CPUs)"),
                "worker_resource_configuration_ram": st.column_config.TextColumn("Worker Resources (RAM MBs)"),
                "input_size": st.column_config.NumberColumn("Input Size"),
                "output_size": st.column_config.NumberColumn("Output Size"),
                "downstream_calls": st.column_config.NumberColumn("Downstream Calls")
            }
        )
    
        with tab_exec:
            # Create two columns for the charts
            col1, col2 = st.columns(2)
            
            with col1:
                # Total Execution Time per Function Group
                total_time_df = metrics_df.groupby('function_name')['execution_time_ms'].sum().reset_index()
                fig = px.bar(
                    total_time_df,
                    x='function_name',
                    y='execution_time_ms',
                    labels={
                        'function_name': 'Function Group',
                        'execution_time_ms': 'Total Execution Time (ms)'
                    },
                    title="Total Execution Time by Function",
                    color='function_name',
                    text_auto=True
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Average Execution Time per Function Group
                avg_time_df = metrics_df.groupby('function_name')['execution_time_ms'].mean().reset_index()
                fig = px.bar(
                    avg_time_df,
                    x='function_name',
                    y='execution_time_ms',
                    labels={
                        'function_name': 'Function Group',
                        'execution_time_ms': 'Average Execution Time (ms)'
                    },
                    title="Average Execution Time by Function",
                    color='function_name',
                    text_auto=True
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
                
            # Add worker resource charts
            st.subheader("Worker Resource Utilization")
            
            # Create columns for resource charts
            res_col1, res_col2 = st.columns(2)
            
            with res_col1:
                # CPU distribution by worker
                if 'worker_resource_configuration_cpus' in metrics_df.columns:
                    cpu_df = metrics_df[metrics_df['worker_resource_configuration_cpus'] > 0]  # Filter out invalid entries
                    if not cpu_df.empty:
                        fig = px.box(
                            cpu_df,
                            x='worker_id',
                            y='worker_resource_configuration_cpus',
                            color='worker_id',
                            labels={
                                'worker_id': 'Worker ID',
                                'worker_resource_configuration_cpus': 'CPU Cores'
                            },
                            title="CPU Cores per Worker"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No valid CPU data available")
            
            with res_col2:
                # RAM distribution by worker
                if 'worker_resource_configuration_ram' in metrics_df.columns:
                    ram_df = metrics_df[metrics_df['worker_resource_configuration_ram'] > 0]  # Filter out invalid entries
                    if not ram_df.empty:
                        fig = px.box(
                            ram_df,
                            x='worker_id',
                            y='worker_resource_configuration_ram',
                            color='worker_id',
                            labels={
                                'worker_id': 'Worker ID',
                                'worker_resource_configuration_ram': 'RAM (MB)'
                            },
                            title="RAM Allocation per Worker (MB)"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No valid RAM data available")
            
            # Resource usage by function group
            st.subheader("Resource Usage by Function Group")
            
            # Create columns for function group charts
            func_col1, func_col2 = st.columns(2)
            
            with func_col1:
                # CPU usage by function group
                if 'worker_resource_configuration_cpus' in metrics_df.columns:
                    cpu_func_df = metrics_df[metrics_df['worker_resource_configuration_cpus'] > 0]
                    if not cpu_func_df.empty:
                        fig = px.box(
                            cpu_func_df,
                            x='function_name',
                            y='worker_resource_configuration_cpus',
                            color='function_name',
                            labels={
                                'function_name': 'Function Group',
                                'worker_resource_configuration_cpus': 'CPU Cores'
                            },
                            title="CPU Cores by Function Group"
                        )
                        st.plotly_chart(fig, use_container_width=True)
            
            with func_col2:
                # RAM usage by function group
                if 'worker_resource_configuration_ram' in metrics_df.columns:
                    ram_func_df = metrics_df[metrics_df['worker_resource_configuration_ram'] > 0]
                    if not ram_func_df.empty:
                        fig = px.box(
                            ram_func_df,
                            x='function_name',
                            y='worker_resource_configuration_ram',
                            color='function_name',
                            labels={
                                'function_name': 'Function Group',
                                'worker_resource_configuration_ram': 'RAM (MB)'
                            },
                            title="RAM Allocation by Function Group (MB)"
                        )
                        st.plotly_chart(fig, use_container_width=True)
            
            # Scatter plot of execution time vs resources
            st.subheader("Execution Time vs Resource Allocation")
            
            if 'worker_resource_configuration_cpus' in metrics_df.columns and 'worker_resource_configuration_ram' in metrics_df.columns:
                resource_df = metrics_df[
                    (metrics_df['worker_resource_configuration_cpus'] > 0) & 
                    (metrics_df['worker_resource_configuration_ram'] > 0)
                ]
                
                if not resource_df.empty:
                    # Create two columns for side-by-side plots
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Original CPU vs Execution Time plot
                        fig_cpu = px.scatter(
                            resource_df,
                            x='worker_resource_configuration_cpus',
                            y='execution_time_ms',
                            color='function_name',
                            hover_data=['task_id', 'worker_id'],
                            labels={
                                'worker_resource_configuration_cpus': 'CPU Cores',
                                'execution_time_ms': 'Execution Time (ms)',
                                'function_name': 'Function Group'
                            },
                            title="Execution Time vs CPU Cores"
                        )
                        st.plotly_chart(fig_cpu, use_container_width=True)
                    
                    with col2:
                        # New RAM vs Execution Time plot
                        fig_ram = px.scatter(
                            resource_df,
                            x='worker_resource_configuration_ram',
                            y='execution_time_ms',
                            color='function_name',
                            hover_data=['task_id', 'worker_id'],
                            labels={
                                'worker_resource_configuration_ram': 'RAM Allocation (MB)',
                                'execution_time_ms': 'Execution Time (ms)',
                                'function_name': 'Function Group'
                            },
                            title="Execution Time vs RAM Allocation"
                        )
                        st.plotly_chart(fig_ram, use_container_width=True)
                    
                    # Combined plot showing both CPU and RAM impact
                    st.subheader("Execution Time vs Resource Allocation (Combined View)")
                    fig_combined = px.scatter(
                        resource_df,
                        x='worker_resource_configuration_cpus',
                        y='execution_time_ms',
                        size='worker_resource_configuration_ram',
                        color='function_name',
                        hover_data=['task_id', 'worker_id'],
                        labels={
                            'worker_resource_configuration_cpus': 'CPU Cores',
                            'execution_time_ms': 'Execution Time (ms)',
                            'worker_resource_configuration_ram': 'RAM (MB)',
                            'function_name': 'Function Group'
                        },
                        title="Execution Time vs CPU Cores (Size=RAM Allocation)"
                    )
                    st.plotly_chart(fig_combined, use_container_width=True)
                
    with tab_data:
        if dag_metrics:
            # Collect all individual transfer metrics
            download_throughputs_mb_s = []
            upload_throughputs_mb_s = []
            all_transfer_speeds_b_ms = []  # In bytes/ms
            total_data_downloaded = 0
            total_data_uploaded = 0

            for task_metrics in dag_metrics:
                for input_metrics in task_metrics.input_metrics.input_download_metrics.values():
                    # Calculate download throughputs for each input
                    if input_metrics.time_ms is not None:
                        downloadable_input_size_bytes = sum([input_metric.deserialized_size_bytes for input_metric in task_metrics.input_metrics.input_download_metrics.values()])
                        throughput_mb = (downloadable_input_size_bytes / (input_metrics.time_ms / 1000)) / (1024 * 1024)  # MB/s
                        speed_bytes_ms = downloadable_input_size_bytes / input_metrics.time_ms  # bytes/ms
                        download_throughputs_mb_s.append(throughput_mb)
                        all_transfer_speeds_b_ms.append(speed_bytes_ms)
                        total_data_downloaded += downloadable_input_size_bytes

                    # Calculate upload throughput for output if available
                    if task_metrics.output_metrics.tp_time_ms is not None:
                        throughput_mb = (task_metrics.output_metrics.deserialized_size_bytes / (task_metrics.output_metrics.tp_time_ms / 1000)) / (1024 * 1024)  # MB/s
                        speed_bytes_ms = task_metrics.output_metrics.deserialized_size_bytes / task_metrics.output_metrics.tp_time_ms  # bytes/ms
                        upload_throughputs_mb_s.append(throughput_mb)
                        all_transfer_speeds_b_ms.append(speed_bytes_ms)
                        total_data_uploaded += task_metrics.output_metrics.deserialized_size_bytes

            # Calculate average throughputs
            avg_download_throughput = sum(download_throughputs_mb_s) / len(download_throughputs_mb_s) if download_throughputs_mb_s else 0
            avg_upload_throughput = sum(upload_throughputs_mb_s) / len(upload_throughputs_mb_s) if upload_throughputs_mb_s else 0

            # Display metrics in columns
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Data Downloaded", format_bytes(total_data_downloaded))
                st.metric("Total Data Uploaded", format_bytes(total_data_uploaded))
            with col2:
                st.metric("Download Throughput (avg)", f"{avg_download_throughput:.2f} MB/s")
                st.metric("Upload Throughput (avg)", f"{avg_upload_throughput:.2f} MB/s")
            with col3:
                st.metric("Number of Downloads", len(download_throughputs_mb_s))
                st.metric("Number of Uploads", len(upload_throughputs_mb_s))
            with col4:
                st.metric("Total Download Time", f"{total_time_downloading_data_ms:.2f} ms")
                st.metric("Total Upload Time", f"{total_time_uploading_data_ms:.2f} ms")

            # Add transfer speeds distribution visualization
            st.subheader("Transfer Speeds Distribution")
            
            if all_transfer_speeds_b_ms:
                # Calculate percentiles
                percentiles = [5, 25, 50, 75, 95]
                percentile_values = np.percentile(all_transfer_speeds_b_ms, percentiles)
                
                # Create figure with smaller size and custom background
                fig, ax = plt.subplots(figsize=(8, 4))  # Reduced from (10, 6)
                fig.patch.set_alpha(0)  # Light gray background
                # fig.patch.set_facecolor('#636EFA')  # Light gray background
                ax.set_facecolor('none')  # Same for axis background
                
                # Plot histogram with KDE
                sns.histplot(all_transfer_speeds_b_ms, bins=30, kde=True, ax=ax, color='#1f77b4')  # Added specific color
                
                # Add percentile lines
                colors = ['red', 'orange', 'green', 'blue', 'purple']
                for i, p in enumerate(percentiles):
                    ax.axvline(percentile_values[i], color=colors[i], linestyle='--', 
                            linewidth=2, label=f'{p}th: {percentile_values[i]:.2f} bytes/ms')
                
                # Customize the plot
                ax.set_title('Transfer Speeds Distribution with Percentile Markers', color="white")
                ax.set_xlabel('Transfer Speed (bytes/ms)', color="white")
                ax.set_ylabel('Frequency', color="white")
                ax.legend()
                ax.tick_params(axis='both', colors='white')  # Makes x & y axis numbers white
                ax.legend(facecolor='none', edgecolor='none', labelcolor='white')
                ax.grid(True, alpha=0.2, color='lightgray')
                for spine in ax.spines.values():
                    spine.set_color('white')
                
                st.pyplot(fig, use_container_width=False)
                
                # Add percentile predictions section
                st.subheader("Transfer Time Predictions")
                
                # Create input for data size
                data_size = st.number_input("Enter data size (bytes) for prediction:", min_value=1, value=1000000)
                
                # Calculate predictions for each percentile
                predictions = []
                for i, p in enumerate(percentiles):
                    speed = percentile_values[i]
                    if speed > 0:
                        time_ms = data_size / speed
                        predictions.append({
                            "Percentile": f"{p}th",
                            "Speed (bytes/ms)": f"{speed:.2f}",
                            "Predicted Time (ms)": f"{time_ms:.2f}",
                            "Description": [
                                "Very conservative estimate (95% confidence)",
                                "Conservative estimate (75% confidence)",
                                "Median speed (typical case)",
                                "Optimistic estimate (25% confidence)",
                                "Very optimistic estimate (5% confidence)"
                            ][i]
                        })
                
                # Display predictions as a table
                if predictions:
                    predictions_df = pd.DataFrame(predictions)
                    st.dataframe(
                        predictions_df,
                        use_container_width=True,
                        column_config={
                            "Percentile": st.column_config.TextColumn(width="small"),
                            "Speed (bytes/ms)": st.column_config.NumberColumn(width="medium"),
                            "Predicted Time (ms)": st.column_config.NumberColumn(width="medium"),
                            "Description": st.column_config.TextColumn(width="large")
                        },
                        hide_index=True
                    )
            else:
                st.warning("No transfer metrics available for visualization")

    with tab_workers:
        if dag_metrics:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(
                    worker_df,
                    x='function_name',
                    y='task_count',
                    color='worker_id',
                    title="Task Distribution by Worker and Function Group",
                    labels={
                        'function_name': 'Function Group',
                        'task_count': 'Number of Tasks',
                        'worker_id': 'Worker ID'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.sunburst(
                    worker_df,
                    path=['worker_id', 'function_name'],
                    values='task_count',
                    title="Worker-Function Group Task Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()