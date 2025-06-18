import streamlit as st
import redis
import cloudpickle
from typing import Dict, List
import pandas as pd
import plotly.express as px
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
    master_dag_id: str
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
                workflow_types[dag.dag_name].instances.append(WorkflowInstanceInfo(dag.master_dag_id, plan_output, download_time, tasks))
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
                instance_execution_time = sum(task.metrics.tp_execution_time_ms for task in instance.tasks)
                instance_download_time = sum(task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms for task in instance.tasks if task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms is not None)
                instance_upload_time = sum(task.metrics.output_metrics.tp_time_ms for task in instance.tasks if task.metrics.output_metrics.tp_time_ms is not None)
                
                # Calculate makespan for this instance
                task_timings = []
                for task in instance.tasks:
                    task_start = task.metrics.started_at_timestamp_s * 1000  # Convert to ms
                    task_end = task_start
                    task_end += task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms if task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms is not None else 0
                    task_end += task.metrics.tp_execution_time_ms
                    task_end += task.metrics.total_invocation_time_ms if task.metrics.total_invocation_time_ms is not None else 0
                    task_end += task.metrics.output_metrics.tp_time_ms if task.metrics.output_metrics.tp_time_ms is not None else 0
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
            # Prepare data for the instance comparison table
            instance_data = []
            for idx, instance in enumerate(matching_workflow_instances):
                if not instance.plan or not instance.tasks:
                    continue
                    
                # Calculate actual metrics
                actual_total_download = sum([sum([input_metric.time_ms / 1000 for input_metric in task.metrics.input_metrics.input_download_metrics.values() if input_metric.time_ms is not None]) for task in instance.tasks]) 
                actual_execution = sum(task.metrics.tp_execution_time_ms / 1000 for task in instance.tasks)  # in seconds
                actual_upload = sum(task.metrics.output_metrics.tp_time_ms / 1000 for task in instance.tasks if task.metrics.output_metrics.tp_time_ms is not None)  # in seconds
                actual_invocation = sum(task.metrics.total_invocation_time_ms / 1000 for task in instance.tasks if task.metrics.total_invocation_time_ms is not None)  # in seconds
                actual_dependency_update = sum(task.metrics.update_dependency_counters_time_ms / 1000 for task in instance.tasks if task.metrics.update_dependency_counters_time_ms is not None)  # in seconds
                actual_input_size = sum([sum([input_metric.serialized_size_bytes for input_metric in task.metrics.input_metrics.input_download_metrics.values()]) for task in instance.tasks])  # in bytes
                actual_output_size = sum([task.metrics.output_metrics.serialized_size_bytes for task in instance.tasks])  # in bytes
                
                # Calculate actual makespan
                task_timings = []
                for task in instance.tasks:
                    task_start = task.metrics.started_at_timestamp_s * 1000  # Convert to ms
                    task_end = task_start
                    task_end += task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms if task.metrics.input_metrics.tp_total_time_waiting_for_inputs_ms is not None else 0
                    task_end += task.metrics.tp_execution_time_ms
                    task_end += task.metrics.total_invocation_time_ms if task.metrics.total_invocation_time_ms is not None else 0
                    task_end += task.metrics.output_metrics.tp_time_ms if task.metrics.output_metrics.tp_time_ms is not None else 0
                    task_timings.append((task_start, task_end))
                
                actual_makespan = 0
                if task_timings:
                    min_start = min(start for start, _ in task_timings)
                    max_end = max(end for _, end in task_timings)
                    actual_makespan = (max_end - min_start) / 1000  # Convert to seconds
                
                # Get predicted metrics if available
                predicted_total_download = predicted_execution = predicted_upload = predicted_makespan = 0
                predicted_input_size = predicted_output_size = 0
                if instance.plan and instance.plan.nodes_info:
                    predicted_total_download = sum(info.total_download_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_execution = sum(info.exec_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_upload = sum(info.upload_time / 1000 for info in instance.plan.nodes_info.values())  # in seconds
                    predicted_input_size = sum(info.input_size for info in instance.plan.nodes_info.values() if hasattr(info, 'input_size'))  # in bytes
                    predicted_output_size = sum(info.output_size for info in instance.plan.nodes_info.values() if hasattr(info, 'output_size'))  # in bytes
                    
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
                        base = "0.000s (0.000%)"
                    else:
                        diff = actual - predicted
                        pct_diff = (diff / predicted * 100) if predicted != 0 else float('inf')
                        sign = "+" if diff >= 0 else "-"
                        base = f"{predicted:.3f}s → {actual:.3f}s ({sign}{abs(diff):.3f}s, {sign}{abs(pct_diff):.1f}%)"
                    
                    if samples is not None:
                        return f"{base}\n(samples: {samples})"
                    return base
                
                # Get sample counts if available
                sample_counts = instance.plan.prediction_sample_counts if instance.plan and hasattr(instance.plan, 'prediction_sample_counts') else None
                
                def format_size_metric(actual, predicted, samples=None):
                    formatted_actual = format_bytes(actual)
                    formatted_predicted = format_bytes(predicted)
                    if predicted == 0 and actual == 0:
                        return f"{formatted_predicted} → {formatted_actual} (0.0%)"
                    diff = actual - predicted
                    pct_diff = (diff / predicted * 100) if predicted != 0 else float('inf')
                    sign = "+" if diff >= 0 else "-"
                    base = f"{formatted_predicted} → {formatted_actual} ({sign}{abs(pct_diff):.1f}%)"
                    if samples is not None:
                        return f"{base}\n(samples: {samples})"
                    return base
                    
                # Format SLA for display
                sla_value = 'N/A'
                if instance.plan:
                    if instance.plan.sla == 'avg':
                        sla_value = 'avg'
                    else:
                        sla_value = f'p{instance.plan.sla.value}'
                
                instance_data.append({
                    'Workflow Type': selected_workflow,
                    'Planner': instance.plan.planner_name if instance.plan else 'N/A',
                    'SLA': sla_value,
                    'Master DAG ID': instance.master_dag_id,
                    'Makespan': format_metric(actual_makespan, predicted_makespan, 
                                        sample_counts.for_execution_time if sample_counts else None),
                    'Total Execution Time': format_metric(actual_execution, predicted_execution, 
                                                sample_counts.for_execution_time if sample_counts else None),
                    'Total Download Time': format_metric(actual_total_download, predicted_total_download, 
                                            sample_counts.for_download_speed if sample_counts else None),
                    'Total Upload Time': format_metric(actual_upload, predicted_upload, 
                                            sample_counts.for_upload_speed if sample_counts else None),
                    'Total Input Size': format_size_metric(actual_input_size, predicted_input_size,
                                            sample_counts.for_download_speed if sample_counts else None),
                    'Total Output Size': format_size_metric(actual_output_size, predicted_output_size,
                                            sample_counts.for_upload_speed if sample_counts else None),
                    'Total Task Invocation Time': f"{actual_invocation:.3f}s",
                    'Total Dependency Counter Update Time': f"{actual_dependency_update:.3f}s",
                    '_actual_invocation': actual_invocation,
                    '_actual_dependency_update': actual_dependency_update,
                    '_sample_count': sample_counts.for_execution_time if sample_counts else 0,
                })

            if instance_data:
                # Create a DataFrame for the table
                df_instances = pd.DataFrame(instance_data)
                
                # Sort by sample count in descending order
                df_instances = df_instances.sort_values('_sample_count', ascending=False)
                
                # Remove the temporary columns before display
                df_instances = df_instances.drop(columns=['_sample_count', '_actual_invocation', '_actual_dependency_update'])
                
                # Reorder columns to put Master DAG ID first
                columns = ['Master DAG ID'] + [col for col in df_instances.columns if col != 'Master DAG ID']
                df_instances = df_instances[columns]
                
                # Display the instance comparison table
                st.markdown("### Instance Comparison")
                
                # Display the table
                st.dataframe(
                    df_instances,
                    column_config={
                        'Workflow Type': "Workflow Type",
                        'SLA': "SLA",
                        'Makespan': "Makespan (Predicted → Actual)",
                        'Total Execution Time': "Total Execution Time (Predicted → Actual)",
                        'Total Download Time': "Total Download Time (Predicted → Actual)",
                        'Total Upload Time': "Total Upload Time (Predicted → Actual)",
                        'Total Input Size': "Total Input Size (Predicted → Actual)",
                        'Total Output Size': "Total Output Size (Predicted → Actual)",
                        'Total Task Invocation Time': "Total Task Invocation Time (s)",
                        'Total Dependency Counter Update Time': "Total Dependency Counter Update Time (s)",
                    },
                    use_container_width=True,
                    height=min(400, 35 * (len(df_instances) + 1)),
                    hide_index=True,
                    column_order=[
                        'Workflow Type', 
                        'Planner',
                        'SLA',
                        'Master DAG ID',
                        'Makespan', 
                        'Total Execution Time', 
                        'Total Download Time',
                        'Total Upload Time',
                        'Total Input Size',
                        'Total Output Size',
                        'Total Task Invocation Time',
                        'Total Dependency Counter Update Time',
                    ]
                )
                
                st.markdown("""
                    **Legend:**
                    - Predicted → Actual: Shows predicted value followed by actual value
                    - Values in parentheses show the difference and percentage difference
                    - Sample counts show how many samples were used for predictions
                    """)
                
                st.markdown("---")

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
                            task_metrics = task.metrics
                            task_metrics_data.append({
                                'Task ID': task.task_id,
                                'Worker Config': str(task_metrics.worker_resource_configuration),
                                'Start Time (s)': task_metrics.started_at_timestamp_s,
                                'Input Size (bytes)': sum([input_metric.serialized_size_bytes for input_metric in task_metrics.input_metrics.input_download_metrics.values()]),
                                'Download Time (ms)': task_metrics.input_metrics.tp_total_time_waiting_for_inputs_ms,
                                'Execution Time (ms)': task_metrics.tp_execution_time_ms,
                                'Output Size (bytes)': task_metrics.output_metrics.serialized_size_bytes if hasattr(task_metrics, 'output_metrics') else 0,
                                'Output Time (ms)': task_metrics.output_metrics.tp_time_ms if hasattr(task_metrics, 'output_metrics') else 0,
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
                                },
                                use_container_width=True,
                                hide_index=True,
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