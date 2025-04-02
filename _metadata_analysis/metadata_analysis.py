from datetime import datetime
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
from src.storage.metrics.metrics_storage import TaskMetrics

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
        dag: DAG = cloudpickle.loads(dag_data)
    except Exception as e:
        st.error(f"Failed to deserialize DAG: {e}")
        return
    
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
        
        with graph_col:
            from streamlit_agraph import agraph, Node, Edge, Config

            nodes = []
            edges = []
            node_levels = {}  # Tracks hierarchy levels
            visited = set()

            def traverse_dag(node, level=0):
                """ Recursively traverse DAG from root nodes """
                node_id = node.id.get_full_id()

                if node_id in visited:
                    return  # Prevents duplicate processing

                visited.add(node_id)
                node_levels[node_id] = level

                # Create node
                nodes.append(Node(
                    id=node_id, 
                    label=node.func_name,
                    size=20, 
                    color="green", 
                    shape="dot",  
                    font={"color": "white", "size": 10, "face": "Arial"},
                    level=level
                ))

                # Process downstream nodes
                for downstream in node.downstream_nodes:
                    edges.append(Edge(
                        source=node_id, 
                        target=downstream.id.get_full_id(), 
                        arrow="to",
                        color="#ffffff"
                    ))
                    traverse_dag(downstream, level + 1)  # Increase level

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
                hierarchical_sort_method="directed",
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
                metrics_key = f"metrics-storage-{st.session_state.selected_task_id}_{dag.master_dag_id}"
                metrics_data = metrics_redis.get(metrics_key)
                if not metrics_data: raise Exception(f"Metrics not found for key {metrics_key}")
                
                metrics = cloudpickle.loads(metrics_data) # type: ignore
                
                # Basic task info
                st.metric("Function", task_node.func_name)
                st.metric("Worker", metrics.worker_id)
                col1, col2 = st.columns(2)
                output_data = metrics.output_metrics.size
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
        
        # Input metrics section below the DAG visualization
        if 'selected_task_id' in st.session_state and st.session_state.selected_task_id:
            metrics_key = f"metrics-storage-{st.session_state.selected_task_id}_{dag.master_dag_id}"
            metrics_data = metrics_redis.get(metrics_key)
            
            if metrics_data:
                metrics: TaskMetrics = cloudpickle.loads(metrics_data)
                
                if metrics.input_metrics:
                    st.subheader("Input Metrics Details")
                    
                    # Create a dataframe for the input metrics
                    input_df = pd.DataFrame([{
                        'Source Task': m.task_id,
                        'Size': format_bytes(m.size),
                        'Download Time (ms)': m.time_ms
                    } for m in metrics.input_metrics])
                    
                    # Calculate and display total input data
                    total_input = sum(m.size for m in metrics.input_metrics)
                    st.write(f"**Total Input Data:** {format_bytes(total_input)}")
                    
                    # Display the input metrics table
                    st.dataframe(
                        input_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Source Task": st.column_config.TextColumn(width="medium"),
                            "Size": st.column_config.TextColumn(width="small"),
                            "Data Type": st.column_config.TextColumn(width="medium"),
                            "Transfer Time (ms)": st.column_config.NumberColumn(width="small")
                        }
                    )
                else:
                    st.write("**No input metrics available for this task**")
    
    # Metrics tabs (unchanged from original)
    with tab_summary:
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
        _task_with_latest_start_time = None
        for task_id in dag._all_nodes.keys():
            metrics_key = f"metrics-storage-{task_id}_{dag.master_dag_id}"
            metrics_data = metrics_redis.get(metrics_key)
            metrics = cloudpickle.loads(metrics_data) # type: ignore
            dag_metrics.append(metrics)
            
            func_name = dag._all_nodes[task_id].func_name
            function_group = get_function_group(task_id, func_name)
            function_groups.add(function_group)
            
            if _task_with_earliest_start_time is None or metrics.started_at_timestamp < _task_with_earliest_start_time.started_at_timestamp:
                _task_with_earliest_start_time = metrics

            if _task_with_latest_start_time is None or metrics.started_at_timestamp > _task_with_latest_start_time.started_at_timestamp:
                _task_with_latest_start_time = metrics

            total_time_invoking_tasks_ms += metrics.total_invocation_time_ms
            total_time_updating_dependency_counters_ms += metrics.update_dependency_counters_time_ms

            # Calculate data transferred
            task_data = 0
            for input_metric in metrics.input_metrics:
                task_data += input_metric.size
                total_time_downloading_data_ms += input_metric.time_ms
            if metrics.output_metrics:
                task_data += metrics.output_metrics.size
                total_time_uploading_data_ms += metrics.output_metrics.time_ms
            
            total_data_transferred += task_data
            total_time_executing_tasks_ms += metrics.execution_time_ms

            # Prepare data for visualization
            task_metrics_data.append({
                'task_id': task_id,
                'task_started_at': datetime.fromtimestamp(metrics.started_at_timestamp).strftime("%Y-%m-%d %H:%M:%S:%f"),
                'function_name': func_name,
                'function_group': function_group,
                'execution_time_ms': metrics.execution_time_ms,
                'data_transferred': task_data,
                'worker_id': metrics.worker_id,
                'input_count': len(metrics.input_metrics),
                'output_size': metrics.output_metrics.size if metrics.output_metrics else 0,
                'downstream_calls': len(metrics.downstream_invocation_times) if metrics.downstream_invocation_times else 0
            })

        last_task_total_time = (_task_with_latest_start_time.started_at_timestamp * 1000) + _task_with_latest_start_time.total_input_download_time_ms + _task_with_latest_start_time.execution_time_ms + _task_with_latest_start_time.output_metrics.time_ms + _task_with_latest_start_time.total_invocation_time_ms # type: ignore
        makespan_ms = last_task_total_time - (_task_with_earliest_start_time.started_at_timestamp * 1000) # type: ignore

        # Create dataframe for visualizations
        metrics_df = pd.DataFrame(task_metrics_data)
        grouped_df = metrics_df.groupby('function_group').agg({
            'execution_time_ms': ['sum', 'mean', 'count'],
            'data_transferred': ['sum', 'mean']
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
        
        # DAG Summary Stats in columns
        col1, col2, col3, col4, col5 = st.columns(5)
        task_execution_time_avg = total_time_executing_tasks_ms / len(dag_metrics) if dag_metrics else 0
        with col1:
            st.metric("Total Tasks", len(dag._all_nodes))
            st.metric(f"Total Time Executing Tasks (avg: {task_execution_time_avg:.2f} ms)", f"{total_time_executing_tasks_ms:.2f} ms")
            st.metric("Total Data Transferred", format_bytes(total_data_transferred))
        with col2:
            st.metric("Makespan", f"{makespan_ms:.2f} ms")
            st.metric("Total Upload Time", f"{total_time_uploading_data_ms:.2f} ms")
            avg_data = total_data_transferred / len(dag_metrics) if dag_metrics else 0
            st.metric("Data Transferred per Task (avg)", format_bytes(avg_data))
        with col3:
            st.metric("Unique Workers", metrics_df['worker_id'].nunique())
            st.metric("Total Download Time", f"{total_time_downloading_data_ms:.2f} ms")
        with col4:
            st.metric("Unique Tasks", len(function_groups))
            st.metric("Total Invocation Time", f"{total_time_invoking_tasks_ms:.2f} ms")
        with col5:
            st.metric("", "")
            st.metric("", "")
            st.metric("Total DC Update Time", f"{total_time_updating_dependency_counters_ms:.2f} ms")


        total_times = total_time_executing_tasks_ms + total_time_downloading_data_ms + total_time_uploading_data_ms + total_time_invoking_tasks_ms + total_time_updating_dependency_counters_ms
        breakdown_data = {
            "Task Execution": total_time_executing_tasks_ms,
            "Data Download": total_time_downloading_data_ms,
            "Data Upload": total_time_uploading_data_ms,
            "Invocation Time": total_time_invoking_tasks_ms,
            "DC Updates": total_time_updating_dependency_counters_ms
        }
        
        # Calculate accounted time
        accounted_time = sum(breakdown_data.values())
        unaccounted_time = max(0, total_times - accounted_time)
        
        # Add unaccounted time if needed
        if unaccounted_time > 0:
            breakdown_data["Other"] = unaccounted_time
        
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

        st.subheader("Function Group Metrics")
        st.dataframe(grouped_df, use_container_width=True)

        st.subheader("Raw Task Metrics")
        raw_task_df = metrics_df.sort_values('function_name').reset_index(drop=True)
        st.dataframe(
            raw_task_df,
            use_container_width=True,
            column_config={
                "task_id": st.column_config.TextColumn("Task ID"),
                "task_started_at": st.column_config.TextColumn("Started At"),
                "function_name": st.column_config.TextColumn("Function"),
                "function_group": st.column_config.TextColumn("Group"),
                "execution_time_ms": st.column_config.NumberColumn("Exec Time (ms)", format="%.2f"),
                "data_transferred": st.column_config.NumberColumn("Data Transferred"),
                "worker_id": st.column_config.TextColumn("Worker"),
                "input_count": st.column_config.NumberColumn("Inputs"),
                "output_size": st.column_config.NumberColumn("Output Size"),
                "downstream_calls": st.column_config.NumberColumn("Downstream Calls")
            }
        )
    
        with tab_exec:
            if dag_metrics:
                # Create two columns for the charts
                col1, col2 = st.columns(2)
                
                with col1:
                    # Total Execution Time per Function Group
                    total_time_df = metrics_df.groupby('function_group')['execution_time_ms'].sum().reset_index()
                    fig = px.bar(
                        total_time_df,
                        x='function_group',
                        y='execution_time_ms',
                        labels={
                            'function_group': 'Function Group',
                            'execution_time_ms': 'Total Execution Time (ms)'
                        },
                        title="Total Execution Time by Function",
                        color='function_group',
                        text_auto='.2s'
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Average Execution Time per Function Group
                    avg_time_df = metrics_df.groupby('function_group')['execution_time_ms'].mean().reset_index()
                    fig = px.bar(
                        avg_time_df,
                        x='function_group',
                        y='execution_time_ms',
                        labels={
                            'function_group': 'Function Group',
                            'execution_time_ms': 'Average Execution Time (ms)'
                        },
                        title="Average Execution Time by Function",
                        color='function_group',
                        text_auto='.2s'
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                
        with tab_data:
            if dag_metrics:
                # Collect all individual transfer metrics
                download_throughputs = []
                upload_throughputs = []
                total_data_downloaded = 0
                total_data_uploaded = 0

                for task_metrics in dag_metrics:
                    # Calculate download throughputs for each input
                    for input_metric in task_metrics.input_metrics:
                        if input_metric.time_ms > 0:
                            throughput = (input_metric.size / (input_metric.time_ms / 1000)) / (1024 * 1024)  # MB/s
                            download_throughputs.append(throughput)
                        total_data_downloaded += input_metric.size

                    # Calculate upload throughput for output if available
                    if task_metrics.output_metrics and task_metrics.output_metrics.time_ms > 0:
                        throughput = (task_metrics.output_metrics.size / (task_metrics.output_metrics.time_ms / 1000)) / (1024 * 1024)  # MB/s
                        upload_throughputs.append(throughput)
                        total_data_uploaded += task_metrics.output_metrics.size

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

    with tab_workers:
        if dag_metrics:
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

if __name__ == "__main__":
    main()