import os
import sys
import streamlit as st
import redis
import cloudpickle
import graphviz
from typing import Optional
from dataclasses import asdict
import pandas as pd
import plotly.express as px

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
        dag = cloudpickle.loads(dag_data)
    except Exception as e:
        st.error(f"Failed to deserialize DAG: {e}")
        return
    
    # Create graph visualization
    st.subheader("DAG Structure")
    dot = graphviz.Digraph()
    
    # Add all nodes to the graph
    for node_id, node in dag._all_nodes.items():
        label = f"{node.func_name}\\n({node_id})"
        dot.node(node_id, label=label)
    
    # Add all edges
    for node_id, node in dag._all_nodes.items():
        for downstream_node in node.downstream_nodes:
            dot.edge(node_id, downstream_node.id.get_full_id())
    
    st.graphviz_chart(dot)
    
    # DAG-level metrics section
    st.subheader("DAG Performance Metrics")
    
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
                
                # Calculate data transferred (sum of all input sizes + output size)
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
        
        # DAG Summary Stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Tasks", len(dag._all_nodes))
        with col2:
            st.metric("Total Time Executing Tasks ", f"{(total_time_executing_tasks_ms / 1000):.3f} s")
            avg_time = total_time_executing_tasks_ms / len(dag_metrics) if dag_metrics else 0
            st.metric("Avg Task Execution Time", f"{(avg_time / 1000):.3f} s")
        with col3:
            st.metric("Total Data Transferred", format_bytes(total_data_transferred))
            avg_data = total_data_transferred / len(dag_metrics) if dag_metrics else 0
            st.metric("Avg Data per Task", format_bytes(avg_data))
        
        # Group metrics by function group
        grouped_df = metrics_df.groupby('function_group').agg({
            'execution_time_ms': ['sum', 'mean', 'count'],
            'data_transferred': ['sum', 'mean'],
            'worker_id': pd.Series.mode
        }).reset_index()
        
        # Flatten multi-index columns
        grouped_df.columns = ['_'.join(col).strip('_') for col in grouped_df.columns.values]
        
        # Execution Time Visualization by Function Group
        st.subheader("Task Execution Times by Function Group")
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
        
        # Data Transfer Visualization by Function Group
        st.subheader("Data Transfer by Function Group")
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
                grouped_df,
                x='execution_time_ms_sum',
                y='data_transferred_sum',
                color='function_group',
                size='execution_time_ms_count',
                hover_name='function_group',
                hover_data=['execution_time_ms_count'],
                labels={
                    'execution_time_ms_sum': 'Total Execution Time (ms)',
                    'data_transferred_sum': 'Total Data Transferred (bytes)',
                    'execution_time_ms_count': 'Number of Tasks'
                },
                title="Execution Time vs Data Transferred by Function Group"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Worker Distribution by Function Group
        st.subheader("Worker Distribution by Function Group")
        worker_df = metrics_df.groupby(['function_group', 'worker_id']).agg({
            'execution_time_ms': 'sum',
            'data_transferred': 'sum',
            'task_id': 'count'
        }).reset_index()
        worker_df = worker_df.rename(columns={'task_id': 'task_count'})
        
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
        
        # Show detailed metrics per function group
        st.subheader("Detailed Metrics per Function Group")
        with st.expander("View Function Group Metrics"):
            st.dataframe(grouped_df)
    
    # Task selection - use session state to track changes
    st.subheader("Task Details")
    task_ids = list(dag._all_nodes.keys())
    
    # Initialize selected_task_id in session state if not present
    if 'selected_task_id' not in st.session_state:
        st.session_state.selected_task_id = task_ids[0] if task_ids else None
    
    # Create the selectbox and update session state on change
    new_selection = st.selectbox(
        "Select a task to view details",
        options=task_ids,
        index=task_ids.index(st.session_state.selected_task_id) if st.session_state.selected_task_id in task_ids else 0,
        format_func=lambda x: x,
        key='task_selectbox'
    )
    
    # Update the selected task ID in session state
    st.session_state.selected_task_id = new_selection
    # Clear previous metrics to ensure fresh display
    metrics_container = st.container()
    
    # Show task details and metrics
    if st.session_state.selected_task_id:
        with metrics_container:
            # Get the task node
            task_node = dag._all_nodes[st.session_state.selected_task_id]
            
            # Display basic task info
            with st.expander("Task Information"):
                st.write(f"**Function Name:** {task_node.func_name}")
                st.write(f"**Task ID:** {st.session_state.selected_task_id}")
                st.write(f"**Upstream Tasks:** {len(task_node.upstream_nodes)}")
                st.write(f"**Downstream Tasks:** {len(task_node.downstream_nodes)}")
            
            # Try to find metrics for this task
            metrics_key = f"metrics-storage-{st.session_state.selected_task_id}_{dag.master_dag_id}"
            metrics_data = metrics_redis.get(metrics_key)
            
            # stored on session state to force re-render
            st.session_state.metrics = cloudpickle.loads(metrics_data)
            metrics = st.session_state.metrics
            
            # Display metrics
            with st.expander("Execution Metrics"):
                st.write(f"**Task ID:** {st.session_state.selected_task_id}")
                st.write(f"**Worker ID:** {metrics.worker_id}")
                st.write(f"**Total Execution Time:** {metrics.execution_time_ms:.2f} ms")
                
                # Calculate data transferred for this task
                input_data = sum(m.size for m in metrics.input_metrics)
                output_data = metrics.output_metrics.size if metrics.output_metrics else 0
                total_data = input_data + output_data
                
                st.write(f"**Total Data Transferred:** {format_bytes(total_data)}")
                st.write(f"**Breakdown:**")
                st.write(f"- Input: {format_bytes(input_data)} ({len(metrics.input_metrics)} inputs)")
                st.write(f"- Output: {format_bytes(output_data)}")
                
                # Input metrics table
                st.subheader("Input Metrics")
                input_df = pd.DataFrame([asdict(m) for m in metrics.input_metrics])
                st.table(input_df)
                
                # Output metrics
                st.subheader("Output Metrics")
                output_df = pd.DataFrame([asdict(metrics.output_metrics)])
                st.table(output_df)
                
                # Downstream invocation times
                if metrics.downstream_invocation_times:
                    st.subheader("Downstream Invocations")
                    downstream_df = pd.DataFrame([asdict(m) for m in metrics.downstream_invocation_times])
                    st.table(downstream_df)
                else:
                    st.write("No downstream invocations recorded")

if __name__ == "__main__":
    main()