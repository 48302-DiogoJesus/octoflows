import os
import sys
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import cloudpickle
import redis
import graphviz
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.storage.metrics.metrics_storage import TaskMetrics

# Redis connection configuration
METRICS_KEY_PREFIX = "metrics-storage-"
DAG_KEY_PREFIX = "dag-"

def get_metrics_storage_redis_connection():
    """Create and return a Redis connection for metrics (port 6380)"""
    return redis.Redis(
        host="localhost",
        port=6380,
        password="redisdevpwd123",
        db=0,
        decode_responses=False
    )

def get_dag_storage_redis_connection():
    """Create and return a Redis connection for DAG information (port 6379)"""
    return redis.Redis(
        host="localhost",
        port=6379,
        password="redisdevpwd123",
        db=0,
        decode_responses=False
    )

def get_available_dags(r_dag) -> list[str]:
    """Get all available DAG IDs from the DAG Redis database"""
    dag_ids = []
    for key in r_dag.scan_iter(f"{DAG_KEY_PREFIX}*"):
        try:
            dag_id = key.decode().replace(DAG_KEY_PREFIX, "")
            dag_ids.append(dag_id)
        except Exception as e:
            st.warning(f"Failed to process DAG key {key}: {str(e)}")
            continue
    return sorted(dag_ids)

def load_dag(r_dag, dag_id: str):
    """Load a DAG from Redis"""
    key = f"{DAG_KEY_PREFIX}{dag_id}"
    raw_data = r_dag.get(key)
    if raw_data:
        return cloudpickle.loads(raw_data)
    return None

def load_metrics_for_dag(r_metrics, dag_id: str) -> list[TaskMetrics]:
    """Load metrics for a specific DAG from the metrics Redis database"""
    metrics = []
    pattern = f"{METRICS_KEY_PREFIX}*_{dag_id}"
    for key in r_metrics.scan_iter(pattern):
        try:
            raw_data = r_metrics.get(key)
            if raw_data:
                metric = cloudpickle.loads(raw_data)
                original_key = key.decode()
                metric.task_id = original_key.replace(METRICS_KEY_PREFIX, "").replace(f"_{dag_id}", "")
                metrics.append(metric)
        except Exception as e:
            st.warning(f"Failed to load metric from key {key}: {str(e)}")
            continue
    return metrics

def create_task_metrics_df(metrics_list: list[TaskMetrics]):
    """Create a pandas DataFrame from a list of TaskMetrics-like objects"""
    data = []
    
    for metric in metrics_list:
        try:
            input_size = sum([getattr(m, 'size', 0) for m in metric.input_metrics]) if metric.input_metrics else 0
            input_time = sum([getattr(m, 'time', 0) for m in metric.input_metrics]) if metric.input_metrics else 0
            downstream = metric.downstream_invocation_times
            
            data.append({
                "task_id": metric.task_id,
                "worker_id": metric.worker_id,
                "execution_time_ms": metric.execution_time_ms,
                "total_input_size": input_size,
                "total_input_time": input_time,
                "output_size": metric.output_metrics.size,
                "output_time_ms": metric.output_metrics.time_ms,
                "num_downstream_tasks": len(downstream) if downstream else 0
            })
        except Exception as e:
            st.warning(f"Failed to process metric: {str(e)}")
            continue
    
    return pd.DataFrame(data)

def visualize_dag(dag, selected_task_id: Optional[str] = None):
    """Create a Graphviz visualization of the DAG with optional highlighting"""
    dot = graphviz.Digraph()
    dot.attr(rankdir='LR')  # Left to right layout
    
    # Add all nodes
    for node_id, node in dag._all_nodes.items():
        # Simplify the label to show just the task name
        label = node.id.task_id
        
        # Highlight the selected node
        if selected_task_id and node.id.task_id == selected_task_id:
            dot.node(node_id, label, style='filled', fillcolor='lightblue', shape='box')
        else:
            dot.node(node_id, label, shape='box')
    
    # Add all edges
    for node in dag._all_nodes.values():
        for downstream_node in node.downstream_nodes:
            dot.edge(node.id.get_full_id(), downstream_node.id.get_full_id())
    
    return dot

def show_task_details(metrics: TaskMetrics, dag_node):
    """Display detailed information about a specific task"""
    st.write(f"**Task ID:** {dag_node.id.task_id}")
    st.write(f"**Function Name:** {dag_node.func_name}")
    st.write(f"**Worker ID:** {metrics.worker_id}")
    st.write(f"**Execution Time:** {metrics.execution_time_ms}ms")
    
    # Show upstream/downstream relationships
    st.write("**DAG Relationships:**")
    col1, col2 = st.columns(2)
    with col1:
        st.write("**Upstream Tasks:**")
        if dag_node.upstream_nodes:
            for upstream in dag_node.upstream_nodes:
                st.write(f"- {upstream.id.task_id}")
        else:
            st.write("None (Root task)")
    
    with col2:
        st.write("**Downstream Tasks:**")
        if dag_node.downstream_nodes:
            for downstream in dag_node.downstream_nodes:
                st.write(f"- {downstream.id.task_id}")
        else:
            st.write("None (Sink task)")
    
    # Input metrics
    st.write("**Input Metrics:**")
    input_metrics = metrics.input_metrics
    if input_metrics:
        input_data = []
        for m in input_metrics:
            input_data.append({
                "task_id": m.task_id,
                "size": m.size,
                "time_ms": m.time_ms
            })
        st.dataframe(pd.DataFrame(input_data))
    else:
        st.write("No input metrics available")
    
    # Output metrics
    output_metrics = metrics.output_metrics
    st.write("**Output Metrics:**")
    if output_metrics:
        st.write(f"Size: {output_metrics.size} bytes")
        st.write(f"Time: {output_metrics.time_ms}ms")
    else:
        st.write("No output metrics available")
    
    # Downstream tasks
    downstream = metrics.downstream_invocation_times
    st.write("**Downstream Invocation Times:**")
    if downstream:
        downstream_data = []
        for m in downstream:
            downstream_data.append({
                "task_id": m.task_id,
                "time_ms": m.time_ms
            })
        st.dataframe(pd.DataFrame(downstream_data))
    else:
        st.write("No downstream tasks")

def main():
    st.title("Task Metrics Dashboard")
    st.write("Visualizing task execution metrics and DAG structure from Redis")
    
    # Initialize Redis connections
    r_metrics = get_metrics_storage_redis_connection()
    r_dag = get_dag_storage_redis_connection()
    
    # Get available DAGs
    with st.spinner("Loading available DAGs..."):
        try:
            dag_ids = get_available_dags(r_dag)
            if not dag_ids:
                st.warning("No DAGs found in Redis.")
                return
        except Exception as e:
            st.error(f"Failed to load DAGs: {str(e)}")
            return
    
    # DAG selection
    selected_dag = st.selectbox("Select a DAG", dag_ids)
    
    # Load DAG structure
    with st.spinner(f"Loading DAG structure for {selected_dag}..."):
        try:
            dag = load_dag(r_dag, selected_dag)
            if not dag:
                st.warning(f"No DAG structure found for {selected_dag}")
                return
        except Exception as e:
            st.error(f"Failed to load DAG structure: {str(e)}")
            return
    
    # Load metrics for selected DAG
    with st.spinner(f"Loading metrics for DAG {selected_dag}..."):
        try:
            metrics_list = load_metrics_for_dag(r_metrics, selected_dag)
            if not metrics_list:
                st.warning(f"No metrics found for DAG {selected_dag}")
                return
            df = create_task_metrics_df(metrics_list)
        except Exception as e:
            st.error(f"Failed to load metrics: {str(e)}")
            return
    
    # Display DAG visualization
    st.subheader("DAG Visualization")
    
    # Create two columns - one for the graph, one for task selection
    col1, col2 = st.columns([3, 1])
    
    with col2:
        # Task selection dropdown
        task_options = [node.id.task_id for node in dag._all_nodes.values()]
        selected_task_id = st.selectbox(
            "Select a task to inspect", 
            options=task_options,
            index=0,
            key="task_select"
        )
    
    with col1:
        # Visualize DAG with selected task highlighted
        dot = visualize_dag(dag, selected_task_id)
        st.graphviz_chart(dot, use_container_width=True)
    
    # Find the selected task's metrics and DAG node
    selected_metric = next(
        (m for m in metrics_list if m.task_id == selected_task_id),
        None
    )
    
    selected_dag_node = dag._get_node_by_task_id(selected_task_id)
    
    if selected_metric and selected_dag_node:
        show_task_details(selected_metric, selected_dag_node)
    else:
        st.warning("No details available for selected task")
    
    # Metrics overview section
    st.subheader("Metrics Overview")
    
    if st.checkbox("Show raw metrics data"):
        st.dataframe(df)
    
    # Execution time chart
    st.subheader("Execution Time by Task")
    fig, ax = plt.subplots(figsize=(10, 4))
    df_sorted = df.sort_values("execution_time_ms", ascending=False)
    ax.bar(df_sorted["task_id"], df_sorted["execution_time_ms"])
    ax.set_ylabel("Execution Time (ms)")
    ax.set_xlabel("Task ID")
    plt.xticks(rotation=45, ha='right')
    st.pyplot(fig)

if __name__ == "__main__":
    main()