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
import src.worker as worker
import src.dag as dag
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
        if len(sys.argv) != 3:
            raise Exception("Usage: python script.py <b64_config> <b64_subdag>")
        
        # Get the serialized DAG from command-line argument
        config = cloudpickle.loads(base64.b64decode(sys.argv[1]))
        subdag = cloudpickle.loads(base64.b64decode(sys.argv[2]))
        
        if not isinstance(config, worker.DockerWorker.Config):
            raise Exception("Error: config is not a DockerWorker.Config instance")
        if not isinstance(subdag, dag.DAG):
            raise Exception("Error: subdag is not a DAG instance")
        
        # Create executor and start execution
        ex = worker.DockerWorker(config)
        logger.info("Start executing subdag")
        await ex.start_executing(subdag)
        logger.info("Execution completed successfully")
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