import os
import sys
import streamlit as st
import cloudpickle
import redis
from typing import List, Dict, Any
import pandas as pd

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

def flatten_object(obj: Any, prefix: str = '') -> Dict[str, Any]:
    """
    Recursively flatten an object into a dictionary.
    Handles nested objects, lists, and dictionaries.
    """
    flat_dict = {}
    
    if hasattr(obj, '__dict__'):
        items = vars(obj).items()
    elif isinstance(obj, dict):
        items = obj.items()
    else:
        return {prefix[:-1]: obj} if prefix else {str(obj): None}
    
    for k, v in items:
        full_key = f"{prefix}{k}"
        
        if hasattr(v, '__dict__') or isinstance(v, dict):
            flat_dict.update(flatten_object(v, f"{full_key}_"))
        elif isinstance(v, (list, tuple)) and v and (hasattr(v[0], '__dict__') or isinstance(v[0], dict)):
            for i, item in enumerate(v):
                flat_dict.update(flatten_object(item, f"{full_key}_{i}_"))
        else:
            flat_dict[full_key] = v
    
    return flat_dict

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
                flat_data = flatten_object(deserialized)
                flat_data['key'] = key.decode('utf-8')  # Add the key as a column
                data.append(flat_data)
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
