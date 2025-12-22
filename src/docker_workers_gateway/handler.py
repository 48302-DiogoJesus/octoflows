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

from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.utils.logger import create_logger
import src.docker_workers_gateway.container_pool_executor as container_pool_executor

logger = create_logger(__name__)

DOCKER_WORKER_PYTHON_PATH = "/app/src/docker_worker_handler/worker.py"

MAX_CONCURRENT_WORKERS = 32
DOCKER_IMAGE = os.environ.get('DOCKER_IMAGE', None)
if DOCKER_IMAGE is None:
    logger.warning("Set the DOCKER_IMAGE environment variable to the name of the Docker image to use.")
    sys.exit(3)

DOCKER_IMAGE = DOCKER_IMAGE.strip()
logger.info(f"Using Docker image: '{DOCKER_IMAGE}'")

app = Flask(__name__)
thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS)
container_pool = container_pool_executor.ContainerPoolExecutor(docker_image=DOCKER_IMAGE, max_containers=MAX_CONCURRENT_WORKERS)

def process_job_async(resource_configuration: TaskWorkerResourceConfiguration, base64_config: str, dag_id: str, base64_task_ids: list[str], base64_fulldag: str | None = None, base64_relevant_cached_results: str | None = None):
    """
    Process a job asynchronously in a separate thread.
    """
    job_id = str(uuid.uuid4())
    worker_id = resource_configuration.worker_id

    def get_time_formatted():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    if base64_fulldag is not None:
        command = f"python {DOCKER_WORKER_PYTHON_PATH} {base64_config} {dag_id} {base64_task_ids} {base64_relevant_cached_results} {base64_fulldag}"
    else:
        command = f"python {DOCKER_WORKER_PYTHON_PATH} {base64_config} {dag_id} {base64_task_ids} {base64_relevant_cached_results}"
        
    logger.info(f"[{get_time_formatted()}] {job_id}) [INFO] Waiting for container for W({worker_id})")

    container_id = container_pool.wait_for_container(cpus=resource_configuration.cpus, memory=resource_configuration.memory_mb, dag_id=dag_id)
    try:
        exit_code = container_pool.execute_command_in_container(container_id, command)
        if exit_code == 2:
            logger.error(f"[{get_time_formatted()}] {job_id}) W({worker_id}) [ERROR] Container {container_id} should be available but another task is using it (this should never happen!)")
        elif exit_code != 0:
            logger.error(f"[{get_time_formatted()}] {job_id}) W({worker_id}) [ERROR] Container {container_id} unexpected exit code={exit_code}")
    except Exception as e:
        logger.error(f"[{get_time_formatted()}] {job_id}) W({worker_id}) [ERROR] Exception: {e}")
    finally:
        container_pool.release_container(container_id)

@app.route('/warmup', methods=['POST'])
def handle_warmup():
    # 1. Get raw binary data (instead of JSON)
    raw_data = request.get_data()
    if not raw_data:
        logger.error("No binary data received in warmup request")
        return jsonify({"error": "Binary data is required"}), 400

    data = cloudpickle.loads(raw_data)
    dag_id = data.get('dag_id')
    resource_configurations = data.get('resource_configurations')

    if dag_id is None: 
        logger.error("'dag_id' field is required")
        return jsonify({"error": "'dag_id' field is required"}), 400
    if resource_configurations is None: 
        logger.error("'resource_configurations' field is required")
        return jsonify({"error": "'resource_configurations' field is required"}), 400

    for resource_configuration in resource_configurations:
        logger.info(f"Warming up resource configuration: {resource_configuration}")
        
        container_id = container_pool._launch_container(
            cpus=resource_configuration.cpus, 
            memory=resource_configuration.memory_mb, 
            dag_id=dag_id, 
            name_prefix="PRE-WARMED_", 
            is_prewarm=True
        )
        
        if container_id is None:
            # We log but continue for others, or return error depending on your policy
            logger.error(f"Max containers reached. Failed to warm up {resource_configuration}")
            return jsonify({"error": "Max containers reached"}), 400

        container_pool.release_container(container_id)

    return "", 202

@app.route('/wait-containers-shutdown', methods=['POST'])
def handle_containers_shutdown():
    # Parse request data
    logger.info("Waiting for all containers to shutdown")
    container_pool._wait_until_there_are_no_more_containers_active()
    logger.info("All containers have shutdown!")
    return "", 200

@app.route('/job', methods=['POST'])
def handle_job():
    raw_data = request.get_data()
    if not raw_data:
        logger.error("No data received in request")
        return jsonify({"error": "Binary data is required"}), 400

    data = cloudpickle.loads(raw_data)

    resource_configuration = data.get('resource_configuration')
    dag_id = data.get('dag_id')
    b64task_ids = data.get('task_ids')  # This is now the actual list of IDs
    b64config = data.get('config')
    b64fulldag = data.get('fulldag')
    b64relevant_cached_results = data.get('relevant_cached_results')

    required_fields = {
        'resource_configuration': resource_configuration,
        'dag_id': dag_id,
        'task_ids': b64task_ids,
        'config': b64config,
        'relevant_cached_results': b64relevant_cached_results
    }

    for field_name, value in required_fields.items():
        if value is None:
            logger.error(f"'{field_name}' field is required")
            return jsonify({"error": f"'{field_name}' field is required"}), 400

    thread_pool.submit(process_job_async, resource_configuration, b64config, dag_id, b64task_ids, b64fulldag, b64relevant_cached_results)
    
    return "", 202

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
