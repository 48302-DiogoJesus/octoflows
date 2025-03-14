import random
import setuptools # docker uses distutils which was removed in Python 3.12
import docker
import base64
import asyncio
from typing import Any
import uuid
import cloudpickle
import aiohttp
from abc import ABC, abstractmethod

import src.dag as dag
import src.intermediate_storage as intermediate_storage
import src.dag_task_node as dag_task_node

class AbstractExecutor(ABC):
    shutdown_flag: asyncio.Event
    subdag: dag.DAG
    executor_id: str   # Use first 8 chars of a UUID for readable IDs

    def __init__(self, subdag: dag.DAG):
        self.shutdown_flag = asyncio.Event()
        self.subdag = subdag
        self.executor_id = str(uuid.uuid4())[:3]  # Use first 8 chars of a UUID for readable IDs
        if not subdag.root_node: raise Exception(f"AbstractExecutor expected a subdag with only 1 root node. Got {len(subdag.root_nodes)}")

    async def start_executing(self):
        task = self.subdag.root_node

        try:
            while not self.shutdown_flag.is_set():
                # 1. DOWNLOAD DEPENDENCIES
                self.log(task.id.get_full_id_in_dag(self.subdag), f"1) Grabbing Dependencies...")
                task_dependencies: dict[str, Any] = {}
                for dependency_task in task.upstream_nodes:
                    task_output = intermediate_storage.IntermediateStorage.get(self.subdag.get_dag_task_id(dependency_task))
                    if task_output is None: raise Exception(f"[BUG] Task {dependency_task.id.get_full_id_in_dag(self.subdag)}'s data is not available")
                    task_dependencies[dependency_task.id.get_full_id()] = cloudpickle.loads(task_output) # type: ignore
                
                # 2. EXECUTE TASK
                self.log(task.id.get_full_id_in_dag(self.subdag), f"2) Executing...")
                task_result = task.invoke(dependencies=task_dependencies)
                upload_result = intermediate_storage.IntermediateStorage.set(self.subdag.get_dag_task_id(task), cloudpickle.dumps(task_result))
                if not upload_result: raise Exception(f"[BUG] Task {task.id.get_full_id_in_dag(self.subdag)}'s data could not be uploaded to Redis (set() failed)")

                if self.shutdown_flag.is_set(): break

                if len(task.downstream_nodes) == 0: 
                    self.log(task.id.get_full_id_in_dag(self.subdag), f"Last Task finished. Shutting down executor...")
                    self.shutdown_flag.set()
                    break

                # 3. HANDLE FAN-OUT (1-1 or 1-N)
                self.log(task.id.get_full_id_in_dag(self.subdag), f"3) Handle Fan-Out {task.id.get_full_id_in_dag(self.subdag)} => [{[t.id.get_full_id_in_dag(self.subdag) for t in task.downstream_nodes]}]")
                ready_downstream: list[dag_task_node.DAGTaskNode] = []
                for downstream_task in task.downstream_nodes:
                    downstream_task_total_dependencies = len(self.subdag.get_node_by_id(downstream_task.id).upstream_nodes)
                    if downstream_task_total_dependencies == 1: # {task} was the only dependency
                        ready_downstream.append(downstream_task)
                    else:
                        dependencies_met = intermediate_storage.IntermediateStorage.increment_and_get(f"dependency-counter-{downstream_task.id.get_full_id_in_dag(self.subdag)}")
                        self.log(task.id.get_full_id_in_dag(self.subdag), f"Incremented DC of {downstream_task.id.get_full_id_in_dag(self.subdag)} ({dependencies_met}/{downstream_task_total_dependencies})")
                        if dependencies_met == downstream_task_total_dependencies:
                            ready_downstream.append(downstream_task)
                
                if self.shutdown_flag.is_set(): break

                # Delegate Downstream Tasks Execution
                ## 1 Task ?: the same executor continues with it
                if len(ready_downstream) == 1:
                    task = self.subdag.get_node_by_id(ready_downstream[0].id) # type: ignore downstream_task_id)
                    continue
                ## > 1 Task ?: Continue with 1 and spawn N-1 Workers for remaining tasks
                elif len(ready_downstream) > 1:
                    continuation_task = ready_downstream[0] # choose the first task
                    tasks_to_delegate = ready_downstream[1:]
                    
                    for task in tasks_to_delegate:
                        asyncio.create_task(self.delegate(self.subdag.create_subdag(task)))
                    
                    # Continue with one task in this executor
                    self.log(task.id.get_full_id_in_dag(self.subdag), f"Continuing with first of multiple downstream tasks: {continuation_task}")
                    task = self.subdag.get_node_by_id(ready_downstream[0].id) # type: ignore downstream_task_id)
                    continue
                else:
                    self.log(task.id.get_full_id_in_dag(self.subdag), f"No ready downstream tasks found. Shutting down executor...")
                    self.shutdown_flag.set()
                    break  # Give up
        except Exception as e:
            self.log(task.id.get_full_id_in_dag(self.subdag), f"Error: {str(e)}") # type: ignore
            raise e

        # Cleanup
        self.log(task.id.get_full_id_in_dag(self.subdag), f"Executor shut down!")

    @abstractmethod
    async def delegate(self, subsubdag: dag.DAG): pass

    def log(self, task_id: str, message: str):
        """Log a message with worker ID prefix."""
        print(f"Executor({self.executor_id}) Task({task_id}) | {message}")

class LocalExecutor(AbstractExecutor):
    """
    Processes DAG tasks
    continuing with single downstream tasks and spawning new workers (coroutines) for branches.
    """
    def __init__(self, subdag: dag.DAG):
       super().__init__(subdag)
    
    async def delegate(self, subsubdag: dag.DAG):
        asyncio.create_task(LocalExecutor(subsubdag).start_executing())

class FlaskExecutor(AbstractExecutor):
    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, subdag: dag.DAG, flask_server_address: str):
        self.flask_server_address = flask_server_address
        super().__init__(subdag)

    async def parallelize_self(self):
        ''' Delegate this subdag to a remote worker. This should be called by the Client to initiate a DAG execution '''
        await self.delegate(self.subdag)

    async def delegate(self, subsubdag: dag.DAG):
        '''
        Each invocation is done inside a new Coroutine without blocking the owner Thread
        '''
        self.log(subsubdag.root_node.id.get_full_id_in_dag(subsubdag), f"Invoking remote executor for subsubdag with {len(subsubdag.root_nodes)} root nodes | First Node: {subsubdag.root_node}")
        async with aiohttp.ClientSession() as session:
            async with await session.post(
                self.flask_server_address, data=cloudpickle.dumps(subsubdag), headers={'Content-Type': 'application/octet-stream'}
            ) as response:
                if response.status != 202:
                    text = await response.text()
                    raise Exception(f"Failed to invoke executor: {text}")
                
class DockerExecutor(AbstractExecutor):
    MAX_TRIES = 2
    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, subdag: dag.DAG, redis_hostname: str):
        super().__init__(subdag)
        intermediate_storage.IntermediateStorage.configure(host=redis_hostname)

    async def parallelize_self(self):
        ''' Delegate this subdag to a remote worker. This should be called by the Client to initiate a DAG execution '''
        await self.delegate(self.subdag)

    async def delegate(self, subsubdag: dag.DAG):
        '''
        Each invocation is done inside a new Coroutine without blocking the owner Thread
        '''
        self.log(subsubdag.root_node.id.get_full_id_in_dag(subsubdag), f"Invoking docker executor for subsubdag with {len(subsubdag.root_nodes)} root nodes | First Node: {subsubdag.root_node}")

        serialized_dag = cloudpickle.dumps(subsubdag)
        base64_encoded_dag = base64.b64encode(serialized_dag).decode('utf-8')
        # Prepare the command to run the script with the base64-encoded DAG
        command = f"python /app/src/standalone_process_worker/worker.py {base64_encoded_dag}"

        docker_service = "standalone-worker-small"
        docker_client = docker.from_env() # Initialize Docker client

        tries = 0
        try:
            while tries < self.MAX_TRIES:
                # Get the list of running containers for the service
                containers = docker_client.containers.list(filters={"status": "running", "label": f"com.docker.compose.service={docker_service}"})

                if not containers:
                    self.log(subsubdag.root_node.id.get_full_id_in_dag(subsubdag), "No running containers. Spawning new one and retrying...")
                    self._launch_new_container(docker_client, docker_service)
                    tries += 1
                    continue

                # Choose a random container from the list
                container = random.choice(containers)
                # Execute the command in the container
                exec_response = container.exec_run(command, detach=False) # TODO: detach true for non-blocking
                exec_status = exec_response[0]
                if exec_status != 0:
                    self.log(subsubdag.root_node.id.get_full_id_in_dag(subsubdag), "Container Busy. Spawning new one and retrying...")
                    tries += 1
                    self._launch_new_container(docker_client, docker_service)
                    continue

                # Log the execution response
                self.log(subsubdag.root_node.id.get_full_id_in_dag(subsubdag), f"Executed command in container {container.id} | Output: {exec_response}")
        except Exception as e:
            self.log(subsubdag.root_node.id.get_full_id_in_dag(subsubdag), f"Error invoking Docker executor: {e}")

    def _launch_new_container(self, docker_client: docker.DockerClient, docker_service: str):
        try:
            # Use Docker SDK to spin up a new container for the given service
            print(f"Launching a new container for service '{docker_service}'...")

            # Now create a container from that image and start it
            container = docker_client.containers.run(docker_service, detach=False)

            print(f"Successfully launched container")
            return container
        except Exception as e:
            print(f"Error: Unable to launch container for service '{docker_service}'.")
            print(str(e))
