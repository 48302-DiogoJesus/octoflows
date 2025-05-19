import base64
import os
import signal
import sys
import threading
import time
import uuid
import cloudpickle
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor

from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.utils.logger import create_logger
import src.docker_workers_gateway.container_pool_executor as container_pool_executor

logger = create_logger(__name__)

DOCKER_WORKER_PYTHON_PATH = "/app/src/docker_worker_handler/worker.py"
MAX_CONCURRENT_TASKS = 20

DOCKER_IMAGE = os.environ.get('DOCKER_IMAGE', None)
if DOCKER_IMAGE is None:
    logger.warning("Set the DOCKER_IMAGE environment variable to the name of the Docker image to use.")
    sys.exit(1)

DOCKER_IMAGE = DOCKER_IMAGE.strip()
logger.info(f"Using Docker image: '{DOCKER_IMAGE}'")

app = Flask(__name__)
thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TASKS)
container_pool = container_pool_executor.ContainerPoolExecutor(docker_image=DOCKER_IMAGE, max_containers=MAX_CONCURRENT_TASKS)

def process_job_async(resource_configuration: TaskWorkerResourceConfiguration, base64_config: str, dag_id: str, base64_task_ids: list[str]):
    """
    Process a job asynchronously.
    This function will be run in a separate thread.
    """
    job_id = str(uuid.uuid4())

    def get_time_formatted():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    command = f"python {DOCKER_WORKER_PYTHON_PATH} {base64_config} {dag_id} {base64_task_ids}"

    container_id = container_pool.wait_for_container(cpus=resource_configuration.cpus, memory=resource_configuration.memory_mb)
    try:
        exit_code = container_pool.execute_command_in_container(container_id, command)
        if exit_code == 0:
            # print(f"[{get_time_formatted()}] {job_id}) COMPLETED in container: {container_id}")
            return
        else:
            logger.error(f"[{get_time_formatted()}] {job_id}) [ERROR] Container {container_id} should be available but exit_code={exit_code}")
    except Exception as e:
        logger.error(f"[{get_time_formatted()}] {job_id}) [ERROR] Exception: {e}")
    finally:
        container_pool.release_container(container_id)


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

        resource_config_key = data.get('resource_configuration', None)
        if resource_config_key is None: 
            logger.error("'resource_configuration' field is required")
            return jsonify({"error": "'resource_configuration' field is required"}), 400
        resource_configuration: TaskWorkerResourceConfiguration | None = cloudpickle.loads(base64.b64decode(resource_config_key))
        if resource_configuration is None: 
            logger.error("'resource_configuration' field is required")
            return jsonify({"error": "'resource_configuration' field is required"}), 400
        dag_id = data.get('dag_id', None)
        if dag_id is None: 
            logger.error("'dag_id' field is required")
            return jsonify({"error": "'dag_id' field is required"}), 400
        b64_task_ids = data.get('task_ids', None)
        if b64_task_ids is None: 
            logger.error("'task_id' field is required")
            return jsonify({"error": "'task_id' field is required"}), 400
        b64config = data.get('config', None)
        if b64config is None: 
            logger.error("'config' field is required")
            return jsonify({"error": "'config' field is required"}), 400

        thread_pool.submit(process_job_async, resource_configuration, b64config, dag_id, b64_task_ids)
        
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
    is_shutting_down_flag = threading.Event()

    def cleanup(signum, frame):
        if is_shutting_down_flag.is_set(): return # avoid executing shutdown more than once
        is_shutting_down_flag.set()
        logger.info("Shutdown. Cleaning up...")
        container_pool.shutdown()
        thread_pool.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)  # Ctrl+C
    signal.signal(signal.SIGTERM, cleanup)  # Termination signal

    app.run(host='0.0.0.0', port=5000)
