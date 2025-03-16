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
    def __init__(self, docker_image: str):
        self.docker_image = docker_image
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)  # Condition variable for waiting
        self.containers: Dict[str, Container] = self.get_running_containers()
        self.container_by_resources: Dict[Tuple[float, int], Set[str]] = defaultdict(set)  # (cpus, memory) -> {container_ids}
        self.last_update_time = 0

    def get_container(self, cpus: int, memory: int) -> Optional[str]:
        with self.lock:
            # Try to find a container with the exact resource requirements
            resource_key = (cpus, memory)
            available_containers = [
                cid for cid in self.container_by_resources[resource_key]
                if not self.containers[cid].is_busy
            ]
            
            if available_containers:
                container_id = available_containers[0]
                self.containers[container_id].is_busy = True
                return container_id
                
            # If no exact match, could implement a strategy to find containers
            # with more resources than required (not implemented here)
            return None

    def wait_for_container(self, cpus: int, memory: int) -> str:
        """
        Wait for a container with the specified resources to become available,
        mark it as busy, and return its ID.
        """
        with self.condition:
            while True:
                container_id = self.get_container(cpus, memory)
                if container_id:
                    return container_id
                # Wait for a container to become available
                self.condition.wait()

    def release_container(self, container_id: str) -> bool:
        """
        Mark a container as available and notify waiting threads.
        """
        with self.condition:
            if container_id in self.containers and self.containers[container_id].is_busy:
                self.containers[container_id].is_busy = False
                self.condition.notify_all()  # Notify all waiting threads
                return True
            return False

    def add_new_container(self, container_id: str, cpus: float, memory: int) -> None:
        """Add a newly launched container to the pool"""
        with self.lock:
            container = Container(id=container_id, cpus=cpus, memory=memory, is_busy=True)
            self.containers[container_id] = container
            self.container_by_resources[(cpus, memory)].add(container_id)
            self.condition.notify_all()  # Notify waiting threads

    def get_running_containers(self) -> Dict[str, Container]:
        # Get all running containers of the specified image
        containers = subprocess.check_output(
            ["docker", "ps", "--filter", f"ancestor={self.docker_image}", "--format", "{{.ID}}"],
            text=True
        ).strip().splitlines()
        
        container_dict = {}
        
        # Get resource information for each container
        for container_id in containers:
            inspect_output = subprocess.check_output(
                ["docker", "inspect", "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}", container_id],
                text=True
            ).strip().split()
            
            container_cpus = int(inspect_output[0]) / 1e9  # Convert nanoseconds to CPUs
            container_memory = int(inspect_output[1]) // (1024 * 1024)  # Convert bytes to MB
            
            container_dict[container_id] = Container(
                id=container_id,
                cpus=container_cpus,
                memory=container_memory,
                is_busy=False
            )
        
        return container_dict
    
    def launch_container(self, cpus, memory):
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
        return container_id
    
    def get_all_resource_configurations(self):
        # Get all running containers
        containers = subprocess.check_output(
            ["docker", "ps", "--filter", f"ancestor={self.docker_image}", "--format", "{{.ID}}"],
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