import os
import subprocess
import sys
import base64
import threading
import time
import uuid
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor

import src.docker_workers_gateway.container_manager as container_manager

DOCKER_WORKER_PYTHON_PATH = "/app/src/docker_worker/worker.py"

DOCKER_IMAGE = os.environ.get('DOCKER_IMAGE', None)
if DOCKER_IMAGE is None:
    print("Set the DOCKER_IMAGE environment variable to the name of the Docker image to use.")
    sys.exit(1)

DOCKER_IMAGE = DOCKER_IMAGE.strip()
print(f"Using Docker image: '{DOCKER_IMAGE}'")

app = Flask(__name__)
thread_pool = ThreadPoolExecutor(max_workers=100)
container_pool = container_manager.ContainerPoolManager(docker_image=DOCKER_IMAGE)

def execute_command_in_container(container_id, command):
    """
    Executes a command in the specified container and returns the exit code.
    Prints the exit code, stdout, and stderr of the command execution.
    """
    result = subprocess.run(
        ["docker", "exec", container_id, "sh", "-c", command],
        capture_output=False,
        # text=True
    )
    # print(f"Exit Code: {result.returncode}")
    # print("STDOUT:")
    # print(result.stdout.strip() if result.stdout else "(No output)")
    # print("STDERR:")
    # print(result.stderr.strip() if result.stderr else "(No errors)")
    return result.returncode

busy_containers = set()
lock = threading.Lock()

def process_job_async(cpus, memory, base64_dag):
    """
    Process a job asynchronously.
    This function will be run in a separate thread.
    """
    job_id = str(uuid.uuid4())[:4]

    def get_time_formatted():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    print(f"[{get_time_formatted()}] {job_id}) ACCEPTED")
    command = f"python {DOCKER_WORKER_PYTHON_PATH} {base64_dag}"

    MAX_TRIES = 10
    currTries = 0
    while currTries <= MAX_TRIES:
        currTries += 1

        container_id = container_pool.get_container(cpus=cpus, memory=memory)
        if container_id:
            try:
                print(f"[{get_time_formatted()}] {job_id}) EXECUTING IN CONTAINER: {container_id}....") 
                exit_code = execute_command_in_container(container_id, command)
                if exit_code == 0:
                    print(f"[{get_time_formatted()}] {job_id}) COMPLETED in container: {container_id}")
                    return
                else:
                    print(f"[{get_time_formatted()}] {job_id}) CAN'T COMPLETE in container: {container_id}")
            finally:
                container_pool.release_container(container_id)

        # If no container succeeded, launch a new one
        print(f"[{get_time_formatted()}] {job_id}) LAUNCHING NEW CONTAINER: cpus={cpus}, memory={memory}")
        new_container_id = container_pool.launch_container(cpus=cpus, memory=memory)

        if new_container_id:
            print(f"[{get_time_formatted()}] {job_id}) NEW CONTAINER: {new_container_id}")
            # Add the new container to our pool (marked as busy by default)
            container_pool.add_new_container(new_container_id, cpus, memory)

            try:
                print(f"[{get_time_formatted()}] {job_id}) EXECUTING IN CONTAINER: {new_container_id}....")
                exit_code = execute_command_in_container(new_container_id, command)
                if exit_code == 0:
                    print(f"[{get_time_formatted()}] {job_id}) COMPLETED in NEW container: {new_container_id}")
                    return
                else:
                    print(f"[{get_time_formatted()}] {job_id}) CAN'T COMPLETE in NEW container: {new_container_id}")
                    continue
            finally:
                container_pool.mark_container_available(new_container_id)
        else:
            print(f"[{get_time_formatted()}] {job_id}) FAILED to launch container")
            sys.exit(0)

    print(f"[{get_time_formatted()}] {job_id}) JOB FAILED after {MAX_TRIES} tries")


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
        cpus = float(resource_config.get('cpus', 1))
        memory = int(resource_config.get('memory', 128))
        base64_dag = data.get('subdag', None)
        if base64_dag is None: return jsonify({"error": "subdag is required"}), 400

        # Submit the job to be processed asynchronously
        thread_pool.submit(process_job_async, cpus, memory, base64_dag)
        
        # Immediately return 202 Accepted
        return jsonify({
            "message": "Job accepted for processing",
            "resource_configuration": {
                "cpus": cpus,
                "memory": memory
            }
        }), 202

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
    # Run the Flask server on port 5000
    app.run(host='0.0.0.0', port=5000)