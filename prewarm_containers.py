import base64
import cloudpickle
import requests
from dataclasses import dataclass

# Define the TaskWorkerResourceConfiguration class to match the server's implementation
@dataclass
class TaskWorkerResourceConfiguration:
    cpus: float
    memory_mb: int
    worker_id: str | None = None

def send_prewarm_request():
    # Server URL - adjust this if your server is running on a different address
    server_url = "http://localhost:5000"
    endpoint = f"{server_url}/warmup"
    
    # Create 3 container configurations with 3 CPUs and 512MB RAM each
    resource_configs = [
        TaskWorkerResourceConfiguration(cpus=3, memory_mb=512, worker_id=f"worker-{i+1}")
        for i in range(3)
    ]
    
    # Serialize the configurations
    serialized_configs = base64.b64encode(cloudpickle.dumps(resource_configs)).decode('utf-8')
    
    # Prepare the request payload
    payload = {
        "resource_configurations": serialized_configs
    }
    
    try:
        # Send the POST request
        response = requests.post(endpoint, json=payload)
        
        # Check the response
        if response.status_code == 202:
            print("Prewarm request accepted. Containers are being started in the background.")
        else:
            print(f"Error: {response.status_code} - {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"Failed to send prewarm request: {e}")

if __name__ == "__main__":
    send_prewarm_request()
