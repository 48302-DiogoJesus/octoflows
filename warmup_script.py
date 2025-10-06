import asyncio
import aiohttp
import base64
import cloudpickle
from typing import List, Dict, Any
import uuid


from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration

# Configuration
BASE_URL = "http://localhost:5000"  # Update with your server URL
NUM_REQUESTS = 5

# Sample resource configuration - adjust based on your actual requirements
SAMPLE_RESOURCE_CONFIG = TaskWorkerResourceConfiguration(memory_mb=512)

# Convert resource config to the expected format
def get_resource_config() -> str:
    # Serialize the resource configuration
    serialized = cloudpickle.dumps([SAMPLE_RESOURCE_CONFIG])
    # Encode to base64 for transmission
    return base64.b64encode(serialized).decode('utf-8')

async def send_warmup_request(session: aiohttp.ClientSession, request_id: int) -> None:
    url = f"{BASE_URL}/warmup"
    payload = {
        "dag_id": f"warmup_request_{request_id}",
        "resource_configurations": get_resource_config()
    }
    
    try:
        async with session.post(url, json=payload) as response:
            if response.status == 202:
                print(f"Request {request_id}: Warmup request accepted")
            else:
                error = await response.text()
                print(f"Request {request_id}: Error - {response.status} - {error}")
    except Exception as e:
        print(f"Request {request_id}: Failed to send request - {str(e)}")

async def main():
    async with aiohttp.ClientSession() as session:
        # Create and run all warmup requests concurrently
        tasks = []
        for i in range(NUM_REQUESTS):
            task = asyncio.create_task(send_warmup_request(session, i))
            tasks.append(task)
        
        # Wait for all requests to complete
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    print(f"Sending {NUM_REQUESTS} warmup requests to {BASE_URL}/warmup")
    asyncio.run(main())
    print("All requests completed")
