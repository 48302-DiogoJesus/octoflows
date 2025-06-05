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
    st.title("DAG Metrics Dashboard")
    
    # Connect to Redis
    dag_redis = get_redis_connection(6379)
    metrics_redis = get_redis_connection(6380)
    
    # Get all DAG keys
    dag_keys = [key for key in dag_redis.keys() if key.startswith(b'dag-')]
    
    if not dag_keys:
        st.warning("No DAGs found in Redis")
        return
    
    # Select DAG - use session state to track changes
    if 'prev_dag_key' not in st.session_state:
        st.session_state.prev_dag_key = None
    
    selected_dag_key = st.selectbox(
        "Current DAG",
        options=dag_keys,
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
        metrics = cloudpickle.loads(metrics_data) # type: ignore
        dag_metrics.append(metrics)
        
        func_name = dag._all_nodes[task_id].func_name
        function_groups.add(func_name)

        if task_id == dag.sink_node.id.get_full_id():
            _sink_task_metrics = metrics

        if _task_with_earliest_start_time is None or metrics.started_at_timestamp_s < _task_with_earliest_start_time.started_at_timestamp_s:
            _task_with_earliest_start_time = metrics

        total_time_invoking_tasks_ms += metrics.total_invocation_time_ms
        total_time_updating_dependency_counters_ms += metrics.update_dependency_counters_time_ms

        # Calculate data transferred
        task_data = 0
        for input_metric in metrics.input_metrics:
            task_data += input_metric.size_bytes
            total_time_downloading_data_ms += input_metric.time_ms
        if metrics.output_metrics:
            task_data += metrics.output_metrics.size_bytes
            total_time_uploading_data_ms += metrics.output_metrics.time_ms
        
        total_data_transferred += task_data
        total_time_executing_tasks_ms += metrics.execution_time_ms

        # Prepare data for visualization
        task_metrics_data.append({
            'task_id': task_id,
            'task_started_at': datetime.fromtimestamp(metrics.started_at_timestamp_s).strftime("%Y-%m-%d %H:%M:%S:%f"),
            'function_name': func_name,
            'execution_time_ms': metrics.execution_time_ms,
            'worker_id': metrics.worker_resource_configuration.worker_id,
            'worker_resource_configuration_cpus': metrics.worker_resource_configuration.cpus,
            'worker_resource_configuration_ram': metrics.worker_resource_configuration.memory_mb,
            'input_count': len(metrics.input_metrics),
            'input_size': sum([m.size_bytes for m in metrics.input_metrics]),
            'output_size': metrics.output_metrics.size_bytes if metrics.output_metrics else 0,
            'downstream_calls': metrics.total_invocations_count
        })
    
    assert _sink_task_metrics

    sink_task_ended_timestamp_ms = (_sink_task_metrics.started_at_timestamp_s * 1000) + _sink_task_metrics.total_input_download_time_ms + _sink_task_metrics.execution_time_ms + _sink_task_metrics.output_metrics.time_ms + _sink_task_metrics.total_invocation_time_ms # type: ignore
    makespan_ms = sink_task_ended_timestamp_ms - (_task_with_earliest_start_time.started_at_timestamp_s * 1000) # type: ignore

    keys = metrics_redis.keys('metrics-storage-dag-*')
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
            "dag_size": deserialized.size_bytes
        })

    # Calculate task timing metrics (start times, end times, path completion times)
    task_timings = {}
    
    # First pass: collect all task metrics and find the minimum start time
    min_start_time = None
    for task_id, metrics in zip(dag._all_nodes.keys(), dag_metrics):
        task_start_time = metrics.started_at_timestamp_s
        task_timings[task_id] = {
            'start_time': task_start_time,
            'end_time': None,
            'path_completion_time': None
        }
        
        if min_start_time is None or task_start_time < min_start_time:
            min_start_time = task_start_time
    
    # Second pass: calculate end times relative to min_start_time
    for task_id, metrics in zip(dag._all_nodes.keys(), dag_metrics):
        relative_start_time = (metrics.started_at_timestamp_s - min_start_time) * 1000  # Convert to ms
        end_time = relative_start_time + metrics.execution_time_ms
        if metrics.output_metrics:
            end_time += metrics.output_metrics.time_ms
        task_timings[task_id]['end_time'] = end_time
    
    # Third pass: calculate path completion times (max end time of all tasks in each path)
    for task_id in dag._all_nodes.keys():
        max_end_time = task_timings[task_id]['end_time']
        
        # Use DFS to find all paths from this task
        stack = [(task_id, max_end_time)]
        while stack:
            current_task_id, current_end_time = stack.pop()
            if current_end_time > max_end_time:
                max_end_time = current_end_time
                
            # Add downstream tasks to stack
            current_node = dag._all_nodes[current_task_id]
            for downstream in current_node.downstream_nodes:
                downstream_id = downstream.id.get_full_id()
                stack.append((downstream_id, task_timings[downstream_id]['end_time']))
        
        task_timings[task_id]['path_completion_time'] = max_end_time
    
    # Update task_metrics_data with the calculated timing information
    for i, task_data in enumerate(task_metrics_data):
        task_id = task_data['task_id']
        task_metrics_data[i].update({
            'relative_start_time_ms': (task_timings[task_id]['start_time'] - min_start_time) * 1000,
            'end_time_ms': task_timings[task_id]['end_time'],
            'path_completion_time_ms': task_timings[task_id]['path_completion_time']
        })
    
    # Make timing information available for the rest of the code
    min_start_time_ms = min_start_time * 1000  # Convert to ms for consistency
    task_end_times = {task_id: timing['end_time'] for task_id, timing in task_timings.items()}
    task_start_times = {task_id: timing['start_time'] for task_id, timing in task_timings.items()}
    path_completion_times = {task_id: timing['path_completion_time'] for task_id, timing in task_timings.items()}

    # Create tabs for visualization and metrics
    tab_viz, tab_summary, tab_exec, tab_data, tab_workers, tab_planning = st.tabs([
        "Visualization", 
        "Summary", 
        "Execution Times", 
        "Data Transfer", 
        "Worker Distribution",
        "Planning"
    ])
    
    def show_input_metrics(task_id):
        if not task_id:
            return
            
        metrics_key = f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}{task_id}_{dag.master_dag_id}"
        metrics_data = metrics_redis.get(metrics_key)
        
        if not metrics_data:
            st.info("No metrics data available for this task")
            return
            
        try:
            # Handle potential async response
            if hasattr(metrics_data, 'result') and callable(getattr(metrics_data, 'result', None)):
                metrics_data = metrics_data.result()  # type: ignore
                    
            # Ensure we have bytes data for cloudpickle
            if isinstance(metrics_data, str):
                metrics_data = metrics_data.encode('utf-8')
                
            if metrics_data:
                metrics: TaskMetrics = cloudpickle.loads(metrics_data)  # type: ignore
                
                if metrics and hasattr(metrics, 'input_metrics') and metrics.input_metrics:
                    # Display section header
                    st.subheader("Input Metrics")
                    
                    # Create a dataframe for the input metrics
                    input_data = []
                    for m in metrics.input_metrics:
                        if hasattr(m, 'task_id') and hasattr(m, 'size_bytes') and hasattr(m, 'time_ms'):
                            input_data.append({
                                'Source Task': m.task_id,
                                'Size': format_bytes(m.size_bytes),
                                'Download Time (ms)': m.time_ms
                            })
                    
                    if input_data:
                        input_df = pd.DataFrame(input_data)
                        # Display the input metrics table
                        st.dataframe(
                            input_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Source Task": st.column_config.TextColumn("Source Task"),
                                "Size": st.column_config.TextColumn("Size"),
                                "Download Time (ms)": st.column_config.NumberColumn("Download Time (ms)", format="%.2f")
                            }
                        )
                        
                        # Calculate and display total input data
                        total_input = sum(m.size_bytes for m in metrics.input_metrics if hasattr(m, 'size_bytes'))
                        st.write(f"**Total Input Data:** {format_bytes(total_input)}")
                    else:
                        st.info("No input metrics available for this task")
                else:
                    st.info("No input metrics available for this task")
        except Exception as e:
            st.error(f"Error processing task metrics: {str(e)}")
            import traceback
            st.text(traceback.format_exc())

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
                    
                    # Get the task's end time (or path completion time if available)
                    task_metrics = next((t for t in task_metrics_data if t['task_id'] == node_id), None)
                    if not task_metrics:
                        return
                        
                    # Use path completion time if available, otherwise use end time
                    task_time = task_metrics.get('path_completion_time_ms', task_metrics.get('end_time_ms', 0))
                    
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
                output_data = metrics.output_metrics.size_bytes
                with col1:
                    total_task_handling_time = metrics.total_input_download_time_ms + metrics.execution_time_ms + metrics.update_dependency_counters_time_ms + metrics.output_metrics.time_ms + metrics.total_invocation_time_ms
                    st.metric("Total Task Handling Time", f"{total_task_handling_time:.2f} ms")
                    st.metric("Dependencies Download Time", f"{metrics.total_input_download_time_ms:.2f} ms")
                    st.metric("DC Updates Time", f"{metrics.update_dependency_counters_time_ms:.2f} ms")
                    st.metric("Output Upload Time", f"{metrics.output_metrics.time_ms:.2f} ms")
                    st.metric("Tasks Upstream", len(task_node.upstream_nodes))
                with col2:
                    st.metric("", "")
                    st.metric("", "")
                    st.metric("Task Execution Time", f"{metrics.execution_time_ms:.2f} ms")
                    st.metric("Downstream Invocations Time", f"{metrics.total_invocation_time_ms:.2f} ms")
                    st.metric("Output Size", format_bytes(output_data))
                    st.metric("Tasks Downstream", len(task_node.downstream_nodes))
                
                # Add planned vs observed metrics if available
                st.subheader("Planned vs Observed Metrics")
                plan_key = f"{MetricsStorage.PLAN_KEY_PREFIX}{dag.master_dag_id}"
                plan_data = metrics_redis.get(plan_key)

                if plan_data:
                    try:
                        plan: AbstractDAGPlanner.PlanOutput = cloudpickle.loads(plan_data) # type: ignore
                        task_plan = plan.nodes_info.get(st.session_state.selected_task_id)
                        
                        if task_plan:
                            # Get the current task's timing metrics
                            current_task_metrics = next(
                                (t for t in task_metrics_data if t['task_id'] == st.session_state.selected_task_id),
                                None
                            )
                            
                            if current_task_metrics:
                                # Calculate all the metrics we want to compare
                                total_input_size = (
                                    sum(im.size_bytes for im in metrics.input_metrics) +
                                    sum(hm.size_bytes for hm in metrics.hardcoded_input_metrics)
                                )
                                
                                output_size = metrics.output_metrics.size_bytes if metrics.output_metrics else 0
                                actual_start_time = current_task_metrics['relative_start_time_ms']
                                actual_path_completion = current_task_metrics['path_completion_time_ms']
                                
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
                                numeric_fields = [
                                    ('Input Size (bytes)', 'input_size', total_input_size),
                                    ('Output Size (bytes)', 'output_size', output_size),
                                    ('Execution Time (ms)', 'exec_time', metrics.execution_time_ms),
                                    ('Earliest Start (ms)', 'earliest_start', actual_start_time),
                                    ('Path Completion Time (ms)', 'path_completion_time', actual_path_completion)
                                ]
                                
                                for label, field, observed_value in numeric_fields:
                                    with col_metric:
                                        st.text(label)
                                    
                                    # Get planned value if available
                                    planned_value = getattr(task_plan, field, None)
                                    
                                    # Format values for display
                                    if planned_value is not None:
                                        with col_planned:
                                            if 'Size' in label:
                                                st.text(format_bytes(planned_value))
                                            else:
                                                st.text(f"{float(planned_value):.2f} ms" if isinstance(planned_value, (int, float)) else str(planned_value))
                                        
                                        with col_observed:
                                            if 'Size' in label:
                                                st.text(format_bytes(observed_value))
                                            else:
                                                st.text(f"{float(observed_value):.2f} ms" if isinstance(observed_value, (int, float)) else str(observed_value))
                                        
                                        # Calculate and display difference
                                        with col_diff:
                                            if isinstance(planned_value, (int, float)) and isinstance(observed_value, (int, float)) and planned_value != 0:
                                                pct_diff = ((observed_value - planned_value) / planned_value) * 100
                                                # For time metrics, higher is worse; for size metrics, higher is also worse
                                                is_worse = (pct_diff > 0) if 'Time' in label or 'Size' in label else (pct_diff < 0)
                                                color = 'red' if is_worse else 'green'
                                                diff_text = f"<span style='color: {color}'>{pct_diff:+.1f}%</span>"
                                                st.markdown(diff_text, unsafe_allow_html=True)
                                            else:
                                                st.text("-")
                                    else:
                                        with col_planned:
                                            st.text("N/A")
                                        with col_observed:
                                            if 'Size' in label:
                                                st.text(format_bytes(observed_value))
                                            else:
                                                st.text(f"{float(observed_value):.2f} ms" if isinstance(observed_value, (int, float)) else str(observed_value))
                                        with col_diff:
                                            st.text("-")
                        else:
                            st.warning("No planning data available for selected task")
                    except Exception as e:
                        st.error(f"Error loading plan data: {str(e)}")
                
    # Planning tab
    with tab_planning:
        st.header("Planned vs Actual Execution Metrics")
        
        # Get plan data if available
        plan_key = f"{MetricsStorage.PLAN_KEY_PREFIX}{dag.master_dag_id}"
        plan_data = metrics_redis.get(plan_key)
        
        if not plan_data:
            st.warning("No planning data available for this DAG")
        else:
            try:
                plan: AbstractDAGPlanner.PlanOutput = cloudpickle.loads(plan_data)  # type: ignore
                
                # Calculate planned metrics from nodes_info
                nodes_info = plan.nodes_info
                # Makespan is the maximum path completion time across all nodes
                planned_makespan = max((node.path_completion_time for node in nodes_info.values()), default=0)
                planned_execution_time = sum(node.exec_time for node in nodes_info.values())
                
                # Calculate actual metrics
                actual_makespan = makespan_ms
                actual_execution_time = total_time_executing_tasks_ms
                
                # Create comparison data
                metrics_data = [
                    {
                        'Metric': 'Makespan',
                        'Planned (ms)': planned_makespan,
                        'Actual (ms)': actual_makespan,
                        'Difference (ms)': actual_makespan - planned_makespan,
                        'Difference (%)': ((actual_makespan - planned_makespan) / planned_makespan * 100) if planned_makespan > 0 else 0
                    },
                    {
                        'Metric': 'Total Execution Time',
                        'Planned (ms)': planned_execution_time,
                        'Actual (ms)': actual_execution_time,
                        'Difference (ms)': actual_execution_time - planned_execution_time,
                        'Difference (%)': ((actual_execution_time - planned_execution_time) / planned_execution_time * 100) if planned_execution_time > 0 else 0
                    }
                ]
                
                # Create a dataframe
                df = pd.DataFrame(metrics_data)
                
                # Display the comparison table
                st.subheader("Planned vs Actual Metrics")
                
                # Format the dataframe for display
                display_df = df.copy()
                for col in ['Planned (ms)', 'Actual (ms)']:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "N/A")
                
                for col in ['Difference (ms)']:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(lambda x: f"{x:+,.2f}" if pd.notna(x) else "N/A")
                
                for col in ['Difference (%)']:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A")
                
                # Display the table with custom styling
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'Metric': st.column_config.TextColumn("Metric"),
                        'Planned (ms)': st.column_config.TextColumn("Planned (ms)"),
                        'Actual (ms)': st.column_config.TextColumn("Actual (ms)"),
                        'Difference (ms)': st.column_config.TextColumn("Difference (ms)"),
                        'Difference (%)': st.column_config.TextColumn("Difference (%)")
                    }
                )
                
                # Create a bar chart for visual comparison
                st.subheader("Planned vs Actual Comparison")
                
                # Prepare data for the bar chart
                chart_data = pd.melt(
                    df, 
                    id_vars=['Metric'], 
                    value_vars=['Planned (ms)', 'Actual (ms)'],
                    var_name='Type',
                    value_name='Time (ms)'
                )
                
                # Create a grouped bar chart
                fig = px.bar(
                    chart_data,
                    x='Metric',
                    y='Time (ms)',
                    color='Type',
                    barmode='group',
                    title='Planned vs Actual Execution Metrics',
                    color_discrete_map={
                        'Planned (ms)': '#636EFA',
                        'Actual (ms)': '#EF553B'
                    }
                )
                
                # Add text to bars with 2 significant digits
                fig.update_traces(
                    texttemplate='%{y:.2f}',
                    textposition='outside'
                )
                
                # Update layout for better readability
                fig.update_layout(
                    xaxis_title='Metric',
                    yaxis_title='Time (ms)',
                    legend_title='',
                    hovermode='x unified',
                    showlegend=True,
                    height=500,
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )
                
                # Add difference annotations
                for _, row in df.iterrows():
                    try:
                        planned = float(row['Planned (ms)'])
                        actual = float(row['Actual (ms)'])
                        diff = float(row['Difference (ms)'])
                        diff_pct = float(row['Difference (%)'])
                        max_val = max(planned, actual)
                        if not pd.isna(max_val):
                            fig.add_annotation(
                                x=row['Metric'],
                                y=max_val * 1.1,
                                text=f"{diff:+,.2f} ms ({diff_pct:+.1f}%)",
                                showarrow=False,
                                font=dict(
                                    color='red' if diff > 0 else 'green',
                                    size=10
                                )
                            )
                    except (ValueError, TypeError):
                        continue
                
                st.plotly_chart(fig, use_container_width=True)
                
            except Exception as e:
                st.error(f"Error loading planning data: {str(e)}")
                import traceback
                st.text(traceback.format_exc())
    
    # Visualization tab
    with tab_viz:
        # Show input metrics for selected task in the Visualization tab
        if 'selected_task_id' in st.session_state and st.session_state.selected_task_id:
            show_input_metrics(st.session_state.selected_task_id)
    
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
        columns_to_exclude = { "input_count", "function_name" }
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
            download_throughputs = []
            upload_throughputs = []
            all_transfer_speeds = []  # In bytes/ms
            total_data_downloaded = 0
            total_data_uploaded = 0

            for task_metrics in dag_metrics:
                # Calculate download throughputs for each input
                for input_metric in task_metrics.input_metrics:
                    if input_metric.time_ms > 0:
                        throughput_mb = (input_metric.size_bytes / (input_metric.time_ms / 1000)) / (1024 * 1024)  # MB/s
                        speed_bytes_ms = input_metric.size_bytes / input_metric.time_ms  # bytes/ms
                        download_throughputs.append(throughput_mb)
                        all_transfer_speeds.append(speed_bytes_ms)
                    total_data_downloaded += input_metric.size_bytes

                # Calculate upload throughput for output if available
                if task_metrics.output_metrics and task_metrics.output_metrics.time_ms > 0:
                    throughput_mb = (task_metrics.output_metrics.size_bytes / (task_metrics.output_metrics.time_ms / 1000)) / (1024 * 1024)  # MB/s
                    speed_bytes_ms = task_metrics.output_metrics.size_bytes / task_metrics.output_metrics.time_ms  # bytes/ms
                    upload_throughputs.append(throughput_mb)
                    all_transfer_speeds.append(speed_bytes_ms)
                    total_data_uploaded += task_metrics.output_metrics.size_bytes

            # Calculate average throughputs
            avg_download_throughput = sum(download_throughputs) / len(download_throughputs) if download_throughputs else 0
            avg_upload_throughput = sum(upload_throughputs) / len(upload_throughputs) if upload_throughputs else 0

            # Display metrics in columns
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Data Downloaded", format_bytes(total_data_downloaded))
                st.metric("Total Data Uploaded", format_bytes(total_data_uploaded))
            with col2:
                st.metric("Download Throughput (avg)", f"{avg_download_throughput:.2f} MB/s")
                st.metric("Upload Throughput (avg)", f"{avg_upload_throughput:.2f} MB/s")
            with col3:
                st.metric("Number of Downloads", len(download_throughputs))
                st.metric("Number of Uploads", len(upload_throughputs))
            with col4:
                st.metric("Total Download Time", f"{total_time_downloading_data_ms:.2f} ms")
                st.metric("Total Upload Time", f"{total_time_uploading_data_ms:.2f} ms")

            # Add transfer speeds distribution visualization
            st.subheader("Transfer Speeds Distribution")
            
            if all_transfer_speeds:
                # Calculate percentiles
                percentiles = [5, 25, 50, 75, 95]
                percentile_values = np.percentile(all_transfer_speeds, percentiles)
                
                # Create figure with smaller size and custom background
                fig, ax = plt.subplots(figsize=(8, 4))  # Reduced from (10, 6)
                fig.patch.set_alpha(0)  # Light gray background
                # fig.patch.set_facecolor('#636EFA')  # Light gray background
                ax.set_facecolor('none')  # Same for axis background
                
                # Plot histogram with KDE
                sns.histplot(all_transfer_speeds, bins=30, kde=True, ax=ax, color='#1f77b4')  # Added specific color
                
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