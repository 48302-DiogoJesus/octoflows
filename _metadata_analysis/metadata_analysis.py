import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import redis
import cloudpickle
import graphviz
from typing import Optional
from dataclasses import asdict
import pandas as pd
import plotly.express as px

from src.dag import DAG

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

# Main app
def main():
    # Configure page layout for wider usage
    st.set_page_config(layout="wide")
    st.title("DAG Visualization Dashboard")
    
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
    
    # Main layout columns
    col1, col2 = st.columns([1, 3])
    
    with col1:
        selected_dag_key = st.selectbox(
            "Select a DAG to visualize",
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
        dag: DAG = cloudpickle.loads(dag_data)
    except Exception as e:
        st.error(f"Failed to deserialize DAG: {e}")
        return
    
    # Create graph visualization in full width
    st.subheader("DAG Structure")
    dot = graphviz.Digraph(graph_attr={'size': '12,12'})  # Larger graph size
    
    # Add all nodes to the graph
    for node_id, node in dag._all_nodes.items():
        label = f"{node.func_name}-{node.id.task_id[:5]}"
        dot.node(node_id, label=label)
    
    # Add all edges
    for node_id, node in dag._all_nodes.items():
        for downstream_node in node.downstream_nodes:
            dot.edge(node_id, downstream_node.id.get_full_id())
    
    st.graphviz_chart(dot)
    
    # DAG-level metrics section - using tabs for better organization
    st.subheader("DAG Performance Metrics")
    tab1, tab2, tab3, tab4 = st.tabs(["Summary", "Execution Times", "Data Transfer", "Worker Distribution"])
    
    # Collect all metrics for this DAG
    dag_metrics = []
    total_data_transferred = 0
    total_time_executing_tasks_ms = 0
    task_metrics_data = []
    function_groups = set()
    
    for task_id in dag._all_nodes.keys():
        metrics_key = f"metrics-storage-{task_id}_{dag.master_dag_id}"
        metrics_data = metrics_redis.get(metrics_key)
        
        if metrics_data:
            try:
                metrics = cloudpickle.loads(metrics_data)
                dag_metrics.append(metrics)
                
                func_name = dag._all_nodes[task_id].func_name
                function_group = get_function_group(task_id, func_name)
                function_groups.add(function_group)
                
                # Calculate data transferred
                task_data = 0
                for input_metric in metrics.input_metrics:
                    task_data += input_metric.size
                if metrics.output_metrics:
                    task_data += metrics.output_metrics.size
                
                total_data_transferred += task_data
                total_time_executing_tasks_ms += metrics.execution_time_ms
                
                # Prepare data for visualization
                task_metrics_data.append({
                    'task_id': task_id,
                    'function_name': func_name,
                    'function_group': function_group,
                    'execution_time_ms': metrics.execution_time_ms,
                    'data_transferred': task_data,
                    'worker_id': metrics.worker_id,
                    'input_count': len(metrics.input_metrics),
                    'output_size': metrics.output_metrics.size if metrics.output_metrics else 0,
                    'downstream_calls': len(metrics.downstream_invocation_times) if metrics.downstream_invocation_times else 0
                })
                
            except Exception as e:
                st.warning(f"Failed to deserialize metrics for task {task_id}: {e}")
    
    if not dag_metrics:
        st.warning("No metrics found for any tasks in this DAG")
    else:
        # Create dataframe for visualizations
        metrics_df = pd.DataFrame(task_metrics_data)
        grouped_df = metrics_df.groupby('function_group').agg({
            'execution_time_ms': ['sum', 'mean', 'count'],
            'data_transferred': ['sum', 'mean'],
            'worker_id': pd.Series.mode
        }).reset_index()
        
        # Flatten multi-index columns
        grouped_df.columns = ['_'.join(col).strip('_') for col in grouped_df.columns.values]
        
        # Worker distribution data
        worker_df = metrics_df.groupby(['function_group', 'worker_id']).agg({
            'execution_time_ms': 'sum',
            'data_transferred': 'sum',
            'task_id': 'count'
        }).reset_index()
        worker_df = worker_df.rename(columns={'task_id': 'task_count'})
        
        with tab1:
            # DAG Summary Stats in columns
            st.subheader("Overall Metrics")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Tasks", len(dag._all_nodes))
                st.metric("Tasks with Metrics", len(dag_metrics))
            with col2:
                st.metric("Total Execution Time", f"{(total_time_executing_tasks_ms / 1000):.3f} s")
                avg_time = total_time_executing_tasks_ms / len(dag_metrics) if dag_metrics else 0
                st.metric("Avg Task Time", f"{(avg_time / 1000):.3f} s")
            with col3:
                st.metric("Total Data", format_bytes(total_data_transferred))
                avg_data = total_data_transferred / len(dag_metrics) if dag_metrics else 0
                st.metric("Avg Data per Task", format_bytes(avg_data))
            with col4:
                unique_workers = metrics_df['worker_id'].nunique()
                st.metric("Unique Workers", unique_workers)
                st.metric("Function Groups", len(function_groups))
            
            st.subheader("Function Group Metrics")
            st.dataframe(grouped_df, use_container_width=True)
        
        with tab2:
            st.subheader("Execution Time Analysis")
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(
                    grouped_df,
                    x='function_group',
                    y='execution_time_ms_sum',
                    color='worker_id_mode',
                    hover_data=['execution_time_ms_mean', 'execution_time_ms_count'],
                    labels={
                        'function_group': 'Function Group',
                        'execution_time_ms_sum': 'Total Execution Time (ms)',
                        'execution_time_ms_mean': 'Average Time (ms)',
                        'execution_time_ms_count': 'Task Count',
                        'worker_id_mode': 'Most Common Worker'
                    },
                    title="Total Execution Time by Function Group"
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.box(
                    metrics_df,
                    x='function_group',
                    y='execution_time_ms',
                    color='worker_id',
                    points="all",
                    hover_data=['task_id'],
                    title="Execution Time Distribution by Function Group"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            st.subheader("Data Transfer Analysis")
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(
                    grouped_df,
                    names='function_group',
                    values='data_transferred_sum',
                    hover_data=['execution_time_ms_count'],
                    title="Data Transferred by Function Group"
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.scatter(
                    metrics_df,
                    x='execution_time_ms',
                    y='data_transferred',
                    color='function_group',
                    size='output_size',
                    hover_name='task_id',
                    hover_data=['worker_id', 'input_count'],
                    log_y=True,
                    labels={
                        'execution_time_ms': 'Execution Time (ms)',
                        'data_transferred': 'Data Transferred (bytes)',
                        'output_size': 'Output Size'
                    },
                    title="Execution Time vs Data Transferred"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab4:
            st.subheader("Worker Distribution")
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(
                    worker_df,
                    x='function_group',
                    y='task_count',
                    color='worker_id',
                    title="Task Distribution by Worker and Function Group",
                    labels={
                        'function_group': 'Function Group',
                        'task_count': 'Number of Tasks',
                        'worker_id': 'Worker ID'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.sunburst(
                    worker_df,
                    path=['worker_id', 'function_group'],
                    values='task_count',
                    title="Worker-Function Group Task Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Task details section - using expanders for better organization
    st.subheader("Task Details")
    
    # Create two columns for task selection and metrics
    task_col1, task_col2 = st.columns([1, 3])
    
    with task_col1:
        # Initialize selected_task_id in session state if not present
        if 'selected_task_id' not in st.session_state:
            st.session_state.selected_task_id = list(dag._all_nodes.keys())[0] if dag._all_nodes else None
        
        # Create the selectbox and update session state on change
        task_ids = list(dag._all_nodes.keys())
        new_selection = st.selectbox(
            "Select a task to view details",
            options=task_ids,
            index=task_ids.index(st.session_state.selected_task_id) if st.session_state.selected_task_id in task_ids else 0,
            format_func=lambda x: x,
            key='task_selectbox'
        )
        
        # Update the selected task ID in session state
        st.session_state.selected_task_id = new_selection
    
    with task_col2:
        if st.session_state.selected_task_id:
            # Get the task node
            task_node = dag._all_nodes[st.session_state.selected_task_id]
            
            # Try to find metrics for this task
            metrics_key = f"metrics-storage-{st.session_state.selected_task_id}_{dag.master_dag_id}"
            metrics_data = metrics_redis.get(metrics_key)
            
            if metrics_data:
                metrics = cloudpickle.loads(metrics_data)
                
                # Display metrics in expandable sections
                with st.expander("Task Overview", expanded=True):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Function", task_node.func_name)
                        st.metric("Worker", metrics.worker_id)
                    with col2:
                        st.metric("Execution Time", f"{metrics.execution_time_ms:.2f} ms")
                        input_data = sum(m.size for m in metrics.input_metrics)
                        st.metric("Input Data", format_bytes(input_data))
                    with col3:
                        output_data = metrics.output_metrics.size if metrics.output_metrics else 0
                        st.metric("Output Data", format_bytes(output_data))
                        st.metric("Total Data", format_bytes(input_data + output_data))
                
                with st.expander("Detailed Metrics"):
                    tab1, tab2, tab3 = st.tabs(["Inputs", "Output", "Invocations"])
                    
                    with tab1:
                        st.write(f"**Number of Inputs:** {len(metrics.input_metrics)}")
                        input_df = pd.DataFrame([asdict(m) for m in metrics.input_metrics])
                        st.dataframe(input_df, use_container_width=True)
                    
                    with tab2:
                        if metrics.output_metrics:
                            output_df = pd.DataFrame([asdict(metrics.output_metrics)])
                            st.dataframe(output_df, use_container_width=True)
                        else:
                            st.write("No output metrics available")
                    
                    with tab3:
                        if metrics.downstream_invocation_times:
                            downstream_df = pd.DataFrame([asdict(m) for m in metrics.downstream_invocation_times])
                            st.dataframe(downstream_df, use_container_width=True)
                        else:
                            st.write("No downstream invocations recorded")
                
                with st.expander("Task Connections"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Upstream Tasks:**")
                        if task_node.upstream_nodes:
                            for node in task_node.upstream_nodes:
                                st.write(f"- {node.id.get_full_id()}")
                        else:
                            st.write("None")
                    with col2:
                        st.write("**Downstream Tasks:**")
                        if task_node.downstream_nodes:
                            for node in task_node.downstream_nodes:
                                st.write(f"- {node.id.get_full_id()}")
                        else:
                            st.write("None")
            else:
                st.warning("No metrics found for this task")
                
                with st.expander("Task Information"):
                    st.write(f"**Function Name:** {task_node.func_name}")
                    st.write(f"**Task ID:** {st.session_state.selected_task_id}")
                    st.write(f"**Upstream Tasks:** {len(task_node.upstream_nodes)}")
                    st.write(f"**Downstream Tasks:** {len(task_node.downstream_nodes)}")

if __name__ == "__main__":
    main()