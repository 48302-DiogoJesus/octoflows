import os
import sys
import threading
from dataclasses import dataclass
from typing import Dict, Set, Tuple
from collections import defaultdict
import subprocess
import time
import uuid

from src.utils.logger import create_logger

logger = create_logger(__name__)
ALLOW_CONTAINER_REUSAGE = True
TIME_UNTIL_WORKER_GOES_COLD_S = 5

@dataclass
class Container:
    id: str
    cpus: float
    memory: int
    is_busy: bool = True
    last_active_time: float = 0

class ContainerPoolExecutor:
    def __init__(self, docker_image: str, max_containers: int = 15):
        """
        {container_idle_timeout} is the time a container can stay idle (without executing tasks) before it is removed
        """
        self.docker_image = docker_image
        self.lock = threading.RLock()
        self.max_containers = max_containers
        self.container_by_resources: Dict[Tuple[float, int], Set[str]] = defaultdict(set)  # (cpus, memory) -> {container_ids}
        self.condition = threading.Condition(self.lock)
        self.containers: Dict[str, Container] = {}
        self.container_idle_timeout_s = TIME_UNTIL_WORKER_GOES_COLD_S
        self.container_cleanup_interval_s = TIME_UNTIL_WORKER_GOES_COLD_S / 3
        self.cleanup_thread = threading.Thread(target=self._cleanup_idle_containers, daemon=True)
        self.shutdown_flag = threading.Event()
        self.cleanup_thread.start()
        self._remove_all_containers()

    def _get_time_formatted(self):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    def execute_command_in_container(self, container_id, command):
        """
        Executes a command in the specified container and returns the exit code.
        Streams stdout and stderr in real-time during execution, then prints the final exit code.
        """
        from sys import platform
        
        logger.info(f"[{self._get_time_formatted()}] EXECUTING IN CONTAINER: {container_id} | command length: {len(command)}")

        # Use Popen instead of run to get real-time output
        process = subprocess.Popen(
            [
                "docker", "exec", "-i", 
                "-e", "TZ=UTC-1", 
                "-e", "LOGS=1",
                "-e", f"HOST_OS={platform}",
                container_id, "sh"
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # merge
            bufsize=1,
            universal_newlines=True
        )
        
        # Send the command to stdin
        if process.stdin:
            process.stdin.write(command)
            process.stdin.close()
        
        # Set up non-blocking reading for stdout and stderr
        if process.stdout: process.stdout.fileno()
        if process.stderr: process.stderr.fileno()
        
        # Make stdout and stderr non-blocking
        if process.stdout: os.set_blocking(process.stdout.fileno(), False)
        if process.stderr: os.set_blocking(process.stderr.fileno(), False)
        
        # Read and print output streams while process is running
        # logger.info("STDOUT (streaming):")
        
        exit_code = None
        while exit_code is None:
            # Read from stdout
            if process.stdout:
                stdout_data = process.stdout.read(4096)
                if stdout_data:
                    logger.info(stdout_data)
            
            # Read from stderr
            if process.stderr:
                stderr_data = process.stderr.read(4096)
                if stderr_data:
                    logger.error(stderr_data)
            
            # Check if process has finished
            exit_code = process.poll()
            if exit_code is None:
                time.sleep(0.4)
        
        # After process completion, read any remaining output
        if process.stdout:
            remaining_stdout = process.stdout.read()
            if remaining_stdout:
                logger.info(remaining_stdout)
        
        if process.stderr:
            remaining_stderr = process.stderr.read()
            if remaining_stderr:
                logger.error(remaining_stderr)
        
        logger.info(f"\nExit Code: {exit_code}")

        # Keep the original behavior of exiting on stderr
        if process.stderr and process.stderr.read():
            sys.exit(0) # ! for easier debugging
        
        with self.lock: 
            if not ALLOW_CONTAINER_REUSAGE:
                self._remove_container(container_id)
            else:
                # to avoid killing container after it exits (if it takes longer than the container idle timeout)
                self.containers[container_id].last_active_time = time.time()
            
        return exit_code

    def shutdown(self):
        logger.info("Shutting down container pool manager...")
        with self.lock: # Get the lock to avoid main thread from launching a container
            self.shutdown_flag.set()
            self.cleanup_thread.join(timeout=5)
            self._remove_all_containers()
        logger.info("Container pool manager shut down.")

    def _remove_all_containers(self):
        """Remove all containers running a specific Docker image."""
        try:
            list_command = ["docker", "ps", "-a", "--filter", f"ancestor={self.docker_image}", "-q"]
            result = subprocess.run(list_command, check=True, capture_output=True, text=True)
            container_ids = result.stdout.splitlines()
            if not container_ids: return

            logger.info(f"Removing {len(container_ids)} containers running image '{self.docker_image}'...")
            for container_id in container_ids:
                self._remove_container(container_id)
            
            logger.info(f"All containers running image '{self.docker_image}' have been removed.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error listing containers: {e}")

    def _cleanup_all_containers(self):
        containers_to_remove = []
        with self.lock:
            for container_id, container in list(self.containers.items()):
                container.is_busy = True
                containers_to_remove.append(container_id)
                
        for container_id in containers_to_remove:
            self._remove_container(container_id)

    def _cleanup_idle_containers(self):
        """Periodically check for and remove idle containers."""
        while not self.shutdown_flag.is_set():
            # Interruptible time.sleep() alternative
            self.shutdown_flag.wait(timeout=self.container_cleanup_interval_s)
            if self.shutdown_flag.is_set(): break

            current_time = time.time()
            containers_to_remove = []
            
            with self.lock:
                for container_id, container in list(self.containers.items()):
                    # Skip busy containers
                    if container.is_busy: continue
                    
                    # Check if the container has been idle for too long
                    if current_time - container.last_active_time > self.container_idle_timeout_s:
                        container.is_busy = True # Avoids the main thread from using this container
                        containers_to_remove.append(container_id)
            
            # Remove the idle containers (outside of the lock to minimize lock contention)
            for container_id in containers_to_remove:
                self._remove_container(container_id)
    
    def _remove_container(self, container_id: str):
        """Remove a container from Docker and from our tracking."""
        try:
            # Stop and remove the container
            logger.info(f"Removing idle container {container_id}")
            subprocess.run(["docker", "stop", "-t", "0", container_id], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["docker", "rm", "-f", container_id], check=True, stdout=subprocess.DEVNULL)
            
            with self.lock:
                if container_id in self.containers:
                    # Remove the container from our tracking structures
                    container = self.containers[container_id]
                    resource_key = (container.cpus, container.memory)
                    self.container_by_resources[resource_key].discard(container_id)
                    if not self.container_by_resources[resource_key]:
                        del self.container_by_resources[resource_key]
                    del self.containers[container_id]
        except subprocess.CalledProcessError as e:
            # container may have been removed already for being idle
            logger.error(f"Error removing container {container_id}: {e}")
        

    def release_container(self, container_id: str):
        with self.lock:
            self.containers[container_id].is_busy = False # ready to be used again
            self.condition.notify_all()

    def wait_for_container(self, cpus: float, memory: int, dag_id: str) -> str:
        """
        Wait for a container with the specified resources to become available,
        mark it as busy, and return its ID.
        """
        with self.lock:
            while True:
                resource_key = (cpus, memory)
                available_containers = [
                    cid for cid in self.container_by_resources[resource_key]
                    if not self.containers[cid].is_busy
                ]
                
                if available_containers and ALLOW_CONTAINER_REUSAGE:
                    container_id = available_containers[0]
                    self.containers[container_id].is_busy = True
                    self.containers[container_id].last_active_time = time.time()  # Update last active time
                    return container_id
                        
                if len(self.containers) >= self.max_containers:
                    # print("(wait_for_container) No container available. Waiting")
                    # Wait for a container to become available. Can't launch new ones
                    self.condition.wait(timeout=2)
                else:
                    # Launch a new container
                    logger.info(f"(wait_for_container) Launching new container for DAG: {dag_id}")
                    container_id = self._launch_container(cpus, memory, dag_id)
                    return container_id
    
    def _launch_container(self, cpus, memory, dag_id):
        # Generate a random 16-digit ID
        container_name = f"{cpus}x{memory}-DAG_{dag_id}-RAND_{uuid.uuid4()}"

        # Run the Docker container with resource limits and custom name
        container_id = subprocess.check_output(
            [
                "docker", "run", "-d",
                "--name", container_name,
                "--cpus", str(cpus),
                "--memory", f"{memory}m",
                "--network", "host",
                self.docker_image
            ],
            text=True
        ).strip()

        with self.lock:
            container = Container(
                id=container_id,
                cpus=cpus,
                memory=memory,
                is_busy=True,
                last_active_time=time.time()
            )
            self.containers[container_id] = container
            self.container_by_resources[(cpus, memory)].add(container_id)
            self.condition.notify_all()

        return container_id
    
    def get_all_resource_configurations(self):
        # Get all running containers
        containers = subprocess.check_output(
            ["docker", "ps", "--filter", f"ancestor={self.docker_image}", "--format", "{{.ID}}"],
            text=True
        ).strip().splitlines()

        with self.lock:
            # Group containers by resource configuration
            configurations: dict[str, list[str]] = {}
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