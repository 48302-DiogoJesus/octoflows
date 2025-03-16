from contextlib import contextmanager
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Set, Tuple, List
from collections import defaultdict
import subprocess

@dataclass
class Container:
    id: str
    cpus: float
    memory: int
    is_busy: bool = False

class ContainerPoolManager:
    def __init__(self, docker_image: str, max_containers: int = 15):
        self.docker_image = docker_image
        self.lock = threading.RLock()
        self.max_containers = max_containers
        self.container_by_resources: Dict[Tuple[float, int], Set[str]] = defaultdict(set)  # (cpus, memory) -> {container_ids}
        self.condition = threading.Condition(self.lock)  # Condition variable for waiting
        self.containers: Dict[str, Container] = {}
        self.get_initially_running_containers()
        print(f"Initial containers: {len(self.containers)}")
        self.last_update_time = 0

    @contextmanager
    def wait_for_container(self, cpus: float, memory: int):
        container_id = self._wait_for_container(cpus=cpus, memory=memory)
        try:
            yield container_id
        finally:
            self.release_container(container_id)
            
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
                    return container_id
                        
                if len(self.containers) >= self.max_containers:
                    # print("(wait_for_container) No container available. Waiting", flush=True)
                    # Wait for a container to become available. Can't launch new ones
                    self.condition.wait()
                else:
                    # Launch a new container
                    print("(wait_for_container) Launching new container", flush=True)
                    container_id = self._launch_container(cpus, memory)
                    return container_id

    def release_container(self, container_id: str) -> bool:
        """
        Mark a container as available and notify waiting threads.
        """
        with self.lock:
            if container_id in self.containers and self.containers[container_id].is_busy:
                self.containers[container_id].is_busy = False
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
                    is_busy=False
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
            container = Container(id=container_id, cpus=cpus, memory=memory, is_busy=True)
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