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
import src.executor as executor
import src.dag as dag

async def main():
    # Ensure only one instance of the script is running
    try:
        if platform.system() == "Windows":
            # Windows-specific file locking
            if os.path.exists(LOCK_FILE):
                print("Error: Another instance of the script is already running. Exiting.")
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
        print("Error: Another instance of the script is already running. Exiting.")
        raise e

    try:
        if len(sys.argv) != 2:
            raise Exception("Usage: python script.py <base64_encoded_serialized_dag>")
        
        # Get the serialized DAG from command-line argument
        serialized_data = base64.b64decode(sys.argv[1])
        subdag = cloudpickle.loads(serialized_data)
        
        # Ensure the loaded object is a DAG
        if not isinstance(subdag, dag.DAG):
            raise Exception("Error: Loaded object is not a DAG instance")
        
        # Create executor and start execution
        ex = executor.DockerExecutor(subdag, "http://localhost:5000") # "redis" hostname because we assume its running on same docker network
        print("Start executing subdag")
        await ex.start_executing()
        print("Execution completed successfully")
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