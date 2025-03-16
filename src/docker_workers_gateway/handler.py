import os
import subprocess
import sys
import base64
import threading
import time
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

# Create a thread pool executor
executor = ThreadPoolExecutor(max_workers=10)

DOCKER_WORKER_PYTHON_PATH = "/app/src/docker_worker/worker.py"

DOCKER_IMAGE = os.environ.get('DOCKER_IMAGE', None)
if DOCKER_IMAGE is None:
    print("Set the DOCKER_IMAGE environment variable to the name of the Docker image to use.")
    sys.exit(1)

DOCKER_IMAGE = DOCKER_IMAGE.strip()
print(f"Using Docker image: '{DOCKER_IMAGE}'")

def run_docker_container(cpus, memory):
    """
    Runs a Docker container with the specified resource limits.
    """
    print(f"Launching Docker container with {cpus} CPUs and {memory} MB of memory...")
    # Run the Docker container with resource limits
    container_id = subprocess.check_output(
        [
            "docker", "run", "-d",
            "--cpus", str(cpus),
            "--memory", f"{memory}m",
            "--network", "host",
            DOCKER_IMAGE
        ],
        text=True
    ).strip()
    return container_id

def get_running_containers(cpus, memory):
    """
    Returns a list of container IDs running the specified image with the given resource limits.
    """
    # Get all running containers
    containers = subprocess.check_output(
        ["docker", "ps", "--filter", f"ancestor={DOCKER_IMAGE}", "--format", "{{.ID}}"],
        text=True
    ).strip().splitlines()

    # Filter containers by resource limits
    filtered_containers = []
    for container_id in containers:
        inspect_output = subprocess.check_output(
            ["docker", "inspect", "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}", container_id],
            text=True
        ).strip().split()
        container_cpus = int(inspect_output[0]) / 1e9  # Convert nanoseconds to CPUs
        container_memory = int(inspect_output[1]) // (1024 * 1024)  # Convert bytes to MB

        if container_cpus == cpus and container_memory == memory:
            filtered_containers.append(container_id)

    return filtered_containers

def execute_command_in_container(container_id, command):
    """
    Executes a command in the specified container and returns the exit code.
    Prints the exit code, stdout, and stderr of the command execution.
    """
    result = subprocess.run(
        ["docker", "exec", container_id, "sh", "-c", command],
        capture_output=True,
        text=True
    )
    print(f"Exit Code: {result.returncode}")
    print("STDOUT:")
    print(result.stdout.strip() if result.stdout else "(No output)")
    print("STDERR:")
    print(result.stderr.strip() if result.stderr else "(No errors)")
    return result.returncode

def get_all_resource_configurations():
    """
    Returns a dictionary mapping resource configurations to container IDs.
    """
    # Get all running containers
    containers = subprocess.check_output(
        ["docker", "ps", "--filter", f"ancestor={DOCKER_IMAGE}", "--format", "{{.ID}}"],
        text=True
    ).strip().splitlines()

    # Group containers by resource configuration
    configurations = {}
    for container_id in containers:
        inspect_output = subprocess.check_output(
            ["docker", "inspect", "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}", container_id],
            text=True
        ).strip().split()
        container_cpus = int(inspect_output[0]) / 1e9  # Convert nanoseconds to CPUs
        container_memory = int(inspect_output[1]) // (1024 * 1024)  # Convert bytes to MB

        config_key = f"{container_cpus}_{container_memory}"
        if config_key not in configurations:
            configurations[config_key] = []
        configurations[config_key].append(container_id)

    return configurations

def process_job_async(resource_key, cpus, memory, base64_dag):
    """
    Process a job asynchronously.
    This function will be run in a separate thread.
    """
    req_id = time.time()
    print(f"{req_id}) Processing job asynchronously at {time.time()}")
    command = f"python {DOCKER_WORKER_PYTHON_PATH} {base64_dag}"
    
    MAX_TRIES = 10
    currTries = 0
    while currTries <= MAX_TRIES:
        currTries += 1
        # Get running containers with the specified configuration
        containers = get_running_containers(cpus=cpus, memory=memory)

        # Try executing the command in each container
        for container_id in containers:
            exit_code = execute_command_in_container(container_id, command)
            if exit_code == 0:
                print(f"{req_id}) Job completed successfully in container {container_id}")
                return

        # If no container succeeded, launch a new one
        new_container_id = run_docker_container(cpus=cpus, memory=memory)
        if new_container_id:
            exit_code = execute_command_in_container(new_container_id, command)
            if exit_code == 0:
                print(f"{req_id}) Job completed successfully in new container {new_container_id}")
                break
            else:
                print(f"{req_id}) Container picked was busy!")
        else:
            print(f"{req_id}) Failed to launch a new container")
            sys.exit(0)


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
        if base64_dag is None:
            return jsonify({"error": "subdag is required"}), 400

        # Create a unique key for this resource configuration
        resource_key = f"{cpus}_{memory}"
        
        req_id = time.time()
        print(f"{req_id}) Received POST request at {time.time()}")
        
        # Submit the job to be processed asynchronously
        executor.submit(process_job_async, resource_key, cpus, memory, base64_dag)
        
        # Immediately return 202 Accepted
        print(f"{req_id}) Responding with 202 Accepted at {time.time()}")
        return jsonify({
            "message": "Job accepted for processing",
            "resource_configuration": {
                "cpus": cpus,
                "memory": memory
            }
        }), 202

    elif request.method == 'GET':
        # Get all resource configurations and their containers
        configurations = get_all_resource_configurations()
        
        # Format the response
        result = {}
        for config_key, container_ids in configurations.items():
            cpus, memory = map(float, config_key.split('_'))
            result[f"{cpus};{memory}"] = container_ids
        
        return jsonify({"configurations": result}), 200

if __name__ == '__main__':
    # Run the Flask server on port 5000
    app.run(host='0.0.0.0', port=5000)