import atexit
import os
import signal
import sys
import time
import uuid
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor

import src.docker_workers_gateway.container_pool_executor as container_pool_executor

DOCKER_WORKER_PYTHON_PATH = "/app/src/docker_worker/worker.py"
MAX_CONCURRENT_TASKS = 10

DOCKER_IMAGE = os.environ.get('DOCKER_IMAGE', None)
if DOCKER_IMAGE is None:
    print("Set the DOCKER_IMAGE environment variable to the name of the Docker image to use.")
    sys.exit(1)

DOCKER_IMAGE = DOCKER_IMAGE.strip()
print(f"Using Docker image: '{DOCKER_IMAGE}'")

app = Flask(__name__)
thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TASKS)
container_pool = container_pool_executor.ContainerPoolExecutor(docker_image=DOCKER_IMAGE, max_containers=MAX_CONCURRENT_TASKS)

def process_job_async(cpus, memory, base64_config, base64_dag):
    """
    Process a job asynchronously.
    This function will be run in a separate thread.
    """
    job_id = str(uuid.uuid4())[:4]

    def get_time_formatted():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    command = f"python {DOCKER_WORKER_PYTHON_PATH} {base64_config} {base64_dag}"

    with container_pool.wait_for_container(cpus=cpus, memory=memory) as container_id:
        try:
            print(f"[{get_time_formatted()}] {job_id}) EXECUTING IN CONTAINER: {container_id} | command length: {len(command)}") 
            exit_code = container_pool.execute_command_in_container(container_id, command)
            if exit_code == 0:
                # print(f"[{get_time_formatted()}] {job_id}) COMPLETED in container: {container_id}")
                return
            else:
                print(f"[{get_time_formatted()}] {job_id}) [BUG] Container {container_id} should be available but exit_code={exit_code}")
        except Exception as e:
            print(f"[{get_time_formatted()}] {job_id}) [BUG] Exception: {e}")


@app.route('/job', methods=['POST', 'GET'])
def handle_job():
    """
    Handles POST and GET requests to /job.
    - POST: Accepts the job and immediately returns 202, then processes the job asynchronously.
    - GET: Returns a list of container IDs grouped by resource configuration.
    """
    if request.method == 'POST':
        # Parse request data
        if not request.is_json: return jsonify({"error": "JSON data is required"}), 400
        data = request.get_json()

        resource_config = data.get('resource_configuration', {})
        cpus = resource_config.get('cpus', None)
        if cpus is None: return jsonify({"error": "cpus is required"}), 400
        cpus = float(cpus)
        memory = resource_config.get('memory', None)
        if memory is None: return jsonify({"error": "memory is required"}), 400
        memory = int(memory)
        b64subdag = data.get('subdag', None)
        if b64subdag is None: return jsonify({"error": "'subdag' field is required"}), 400
        b64config = data.get('config', None)
        if b64config is None: return jsonify({"error": "'config' field is required"}), 400

        thread_pool.submit(process_job_async, cpus, memory, b64config, b64subdag)
        
        return "", 202 # Immediately return 202 Accepted

    elif request.method == 'GET':
        # Get all resource configurations and their containers
        configurations = container_pool.get_all_resource_configurations()
        
        # Format the response
        result = {}
        for config_key, container_ids in configurations.items():
            cpus, memory = map(float, config_key.split('_'))
            result[f"{cpus};{memory}"] = container_ids
        
        return jsonify({"configurations": result}), 200

if __name__ == '__main__':
    def cleanup(signum, frame):
        print("Shutdown. Cleaning up...")
        container_pool.shutdown()
        thread_pool.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)  # Ctrl+C
    signal.signal(signal.SIGTERM, cleanup)  # Termination signal

    app.run(host='0.0.0.0', port=5000)
