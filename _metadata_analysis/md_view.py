import os
import sys
import streamlit as st
import cloudpickle
import redis
from typing import List, Dict, Any
import pandas as pd
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.storage.metrics.metrics_storage import MetricsStorage

# Set page config to use wide layout (first thing in your script)
st.set_page_config(layout="wide")

# Redis connection setup
client = redis.Redis(
    host='localhost',
    port=6380,
    password='redisdevpwd123',
    decode_responses=False
)

def get_all_keys(pattern: str) -> List[bytes]:
    """Get all keys matching a pattern"""
    return client.keys(pattern)

def deserialize_data(serialized_value: bytes) -> Any:
    """Deserialize data using cloudpickle"""
    return cloudpickle.loads(serialized_value)

def stringify_complex_types(value: Any) -> str:
    """Convert complex objects to string representation"""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, indent=2, default=str)
    elif hasattr(value, '__dict__'):
        return json.dumps(vars(value), indent=2, default=str)
    return str(value)

def display_metrics_table(pattern: str, title: str) -> None:
    """Display metrics for a given key pattern in a table"""
    st.subheader(title)
    
    keys = get_all_keys(pattern)
    if not keys:
        st.warning(f"No keys found matching pattern: {pattern}")
        return
    
    data = []
    for key in keys:
        serialized_value = client.get(key)
        if serialized_value:
            try:
                deserialized = deserialize_data(serialized_value)
                
                # Handle the deserialized object
                if isinstance(deserialized, dict):
                    row = {k: stringify_complex_types(v) for k, v in deserialized.items()}
                elif hasattr(deserialized, '__dict__'):
                    row = {k: stringify_complex_types(v) for k, v in vars(deserialized).items()}
                else:
                    row = {'value': stringify_complex_types(deserialized)}
                
                row['key'] = key.decode('utf-8')  # Add the key as a column
                data.append(row)
            except Exception as e:
                st.error(f"Error deserializing {key}: {e}")
    
    if not data:
        st.warning("No valid data found")
        return
    
    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(data)
    
    # Reorder columns to have 'key' first if it exists
    if 'key' in df.columns:
        cols = ['key'] + [col for col in df.columns if col != 'key']
        df = df[cols]
    
    # Use st.dataframe with width configuration
    st.dataframe(df, use_container_width=True)

def main():
    st.title("Redis Metrics Viewer")
    
    tab1, tab2 = st.tabs(["Task Metrics", "DAG Prepare Metrics"])
    
    with tab1:
        display_metrics_table(
            pattern=f"{MetricsStorage.TASK_METRICS_KEY_PREFIX}*",
            title="Task Metrics"
        )
    
    with tab2:
        display_metrics_table(
            pattern=f"{MetricsStorage.DAG_METRICS_KEY_PREFIX}*",
            title="DAG Prepare Metrics"
        )

if __name__ == "__main__":
    main()