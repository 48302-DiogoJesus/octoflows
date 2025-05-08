import asyncio
import sys
import base64
import cloudpickle
import os
import platform

# Define a lock file path
LOCK_FILE = "/tmp/script.lock" if platform.system() != "Windows" else "C:\\Windows\\Temp\\script.lock"

# Be at the same level as the ./src directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.workers.docker_worker import DockerWorker
from src.storage.metrics.metrics_storage import FullDAGPrepareTime
from src.utils.timer import Timer
from src.dag_task_node import DAGTaskNodeId
from src.utils.logger import create_logger

logger = create_logger(__name__)

async def main():
    # Ensure only one instance of the script is running
    try:
        if platform.system() == "Windows":
            # Windows-specific file locking
            if os.path.exists(LOCK_FILE):
                logger.error("Error: Another instance of the script is already running. Exiting.")
                sys.exit(1)
            # Create the lock file
            with open(LOCK_FILE, "w") as lock_file:
                lock_file.write(str(os.getpid()))
        else:
            # Linux/Unix-specific file locking
            import fcntl
            lock_file = open(LOCK_FILE, "w")
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, BlockingIOError) as e:
        logger.error("Error: Another instance of the script is already running. Exiting.")
        raise e

    try:
        if len(sys.argv) != 4:
            raise Exception("Usage: python script.py <b64_config> <dag_id> <task_id>")
        
        # Get the serialized DAG from command-line argument
        config = cloudpickle.loads(base64.b64decode(sys.argv[1]))
        dag_id = str(sys.argv[2])
        b64_task_ids = str(sys.argv[3])
        
        if not isinstance(config, DockerWorker.Config):
            raise Exception("Error: config is not a DockerWorker.Config instance")
        
        wk = DockerWorker(config)

        dag_download_time_ms = Timer()
        dag_size_bytes, fulldag = await wk.get_full_dag(dag_id)
        dag_download_time_ms = dag_download_time_ms.stop()

        create_subdags_time_ms = Timer()
        task_ids: list[DAGTaskNodeId] = cloudpickle.loads(base64.b64decode(b64_task_ids))
        execution_coroutines = []
        for task_id in task_ids:
            subdag = fulldag.create_subdag(fulldag.get_node_by_id(task_id))
            # Create executor and start execution
            execution_coroutines.append(wk.start_executing(subdag))
        create_subdags_time_ms = create_subdags_time_ms.stop()

        logger.info("[DOCKER_WORKER] Started executing subdags...")
        await asyncio.gather(*execution_coroutines)

        if wk.metrics_storage:
            wk.metrics_storage.store_dag_download_time(
                task_ids[0].get_full_id_in_dag(fulldag),
                FullDAGPrepareTime(download_time_ms=dag_download_time_ms, size_bytes=dag_size_bytes, create_subdags_time_ms=create_subdags_time_ms)
            )
            await wk.metrics_storage.flush()

        logger.info("[DOCKER_WORKER] Execution completed successfully!")
    finally:
        # Release the lock and clean up
        if platform.system() == "Windows":
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        else:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
            os.remove(LOCK_FILE)

if __name__ == '__main__':
    # Run the main async function and wait until it completes
    asyncio.run(main())