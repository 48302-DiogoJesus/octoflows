from contextlib import contextmanager
import sys
import threading
from dataclasses import dataclass
from typing import Dict, Set, Tuple
from collections import defaultdict
import subprocess
import time

@dataclass
class Container:
    id: str
    cpus: float
    memory: int
    is_busy: bool = False
    last_active_time: float = 0

class ContainerPoolManager:
    def __init__(self, docker_image: str, max_containers: int = 15, container_cleanup_interval: int = 5, container_idle_timeout: int = 10):
        self.docker_image = docker_image
        self.lock = threading.RLock()
        self.max_containers = max_containers
        self.container_by_resources: Dict[Tuple[float, int], Set[str]] = defaultdict(set)  # (cpus, memory) -> {container_ids}
        self.condition = threading.Condition(self.lock)  # Condition variable for waiting
        self.containers: Dict[str, Container] = {}
        
        # Cleanup configuration
        self.container_cleanup_interval = container_cleanup_interval 
        self.container_idle_timeout = container_idle_timeout
        
        self.cleanup_thread = threading.Thread(target=self._cleanup_idle_containers, daemon=True)
        self.shutdown_flag = threading.Event()
        self.cleanup_thread.start()
        
        self.get_initially_running_containers()
        print(f"Initial containers: {len(self.containers)}")

    @contextmanager
    def wait_for_container(self, cpus: float, memory: int):
        container_id = self._wait_for_container(cpus=cpus, memory=memory)
        try:
            yield container_id
        finally:
            self._release_container(container_id)

    def execute_command_in_container(self, container_id, command):
        """
        Executes a command in the specified container and returns the exit code.
        Prints the exit code, stdout, and stderr of the command execution.
        """
        result = subprocess.run(
            ["docker", "exec", "-i", container_id, "sh"],
            input=command.encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # print(f"Exit Code: {result.returncode}")
        # print("STDOUT:")
        # print(result.stdout.decode().strip() if result.stdout else "(No output)")
        if result.stderr:
            print("STDERR:")
            print(result.stderr.decode().strip())
            sys.exit(0)
        return result.returncode

    def shutdown(self):
        print("Shutting down container pool manager...")
        self.shutdown_flag.set()
        self.cleanup_thread.join(timeout=5)
        print("Container pool manager shut down.")

    def _cleanup_idle_containers(self):
        """Periodically check for and remove idle containers."""
        while not self.shutdown_flag.is_set():
            # Interruptible time.sleep() alternative
            self.shutdown_flag.wait(timeout=self.container_cleanup_interval)
            if self.shutdown_flag.is_set(): break

            current_time = time.time()
            containers_to_remove = []
            
            with self.lock:
                for container_id, container in list(self.containers.items()):
                    # Skip busy containers
                    if container.is_busy:
                        continue
                    
                    # Check if the container has been idle for too long
                    if current_time - container.last_active_time > self.container_idle_timeout:
                        container.is_busy = True # Avoids the main thread from using this container
                        containers_to_remove.append(container_id)
            
            # Remove the idle containers (outside of the lock to minimize lock contention)
            for container_id in containers_to_remove:
                self._remove_container(container_id)
    
    def _remove_container(self, container_id: str):
        """Remove a container from Docker and from our tracking."""
        try:
            # Stop and remove the container
            print(f"Removing idle container {container_id}")
            subprocess.run(["docker", "stop", container_id], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["docker", "rm", container_id], check=True, stdout=subprocess.DEVNULL)
            
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
            print(f"Error removing container {container_id}: {e}")
            
    def _wait_for_container(self, cpus: float, memory: int) -> str:
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
                
                if available_containers:
                    container_id = available_containers[0]
                    self.containers[container_id].is_busy = True
                    self.containers[container_id].last_active_time = time.time()  # Update last active time
                    return container_id
                        
                if len(self.containers) >= self.max_containers:
                    # print("(wait_for_container) No container available. Waiting")
                    # Wait for a container to become available. Can't launch new ones
                    self.condition.wait()
                else:
                    # Launch a new container
                    print("(wait_for_container) Launching new container")
                    container_id = self._launch_container(cpus, memory)
                    return container_id

    def _release_container(self, container_id: str) -> bool:
        """
        Mark a container as available and notify waiting threads.
        """
        with self.lock:
            if container_id in self.containers and self.containers[container_id].is_busy:
                self.containers[container_id].is_busy = False
                self.containers[container_id].last_active_time = time.time()  # Update last active time when released
                self.condition.notify_all()  # Notify all waiting threads
                return True
            return False

    def get_initially_running_containers(self):
        # Get all running containers of the specified image
        containers = subprocess.check_output(
            ["docker", "ps", "--filter", f"ancestor={self.docker_image}", "--format", "{{.ID}}"],
            text=True
        ).strip().splitlines()
        
        with self.lock:
            # Get resource information for each container
            for container_id in containers:
                inspect_output = subprocess.check_output(
                    ["docker", "inspect", "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}", container_id],
                    text=True
                ).strip().split()
                
                container_cpus = int(inspect_output[0]) / 1e9  # Convert nanoseconds to CPUs
                container_memory = int(inspect_output[1]) // (1024 * 1024)  # Convert bytes to MB
                
                self.containers[container_id] = Container(
                    id=container_id,
                    cpus=container_cpus,
                    memory=container_memory,
                    is_busy=False,
                    last_active_time=time.time()
                )
                self.container_by_resources[(container_cpus, container_memory)].add(container_id)
    
    def _launch_container(self, cpus, memory):
        # Run the Docker container with resource limits
        container_id = subprocess.check_output(
            [
                "docker", "run", "-d",
                "--cpus", str(cpus),
                "--memory", f"{memory}m",
                "--network", "host",
                self.docker_image
            ],
            text=True
        ).strip()

        with self.lock:
            container = Container(id=container_id, cpus=cpus, memory=memory, is_busy=True, last_active_time=time.time())
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