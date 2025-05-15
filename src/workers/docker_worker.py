import base64
from dataclasses import dataclass
from itertools import groupby
import json

import aiohttp
import cloudpickle
from src.dag import dag
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.utils.logger import create_logger
from src.workers.worker import Worker

logger = create_logger(__name__)

class DockerWorker(Worker):
    @dataclass
    class Config(Worker.Config):
        docker_gateway_address: str = "http://localhost:5000"
        def create_instance(self) -> "DockerWorker": return DockerWorker(self)

    docker_config: Config

    """
    Invokes workers by calling a Flask web server with the serialized subsubdag
    Waits for the completion of all workers
    """
    def __init__(self, config: Config):
        super().__init__(config)
        self.docker_config = config

    async def delegate(self, subdags: list[dag.SubDAG], called_by_worker: bool = True):
        '''
        Each invocation is done inside a new Coroutine without blocking the owner Thread
        '''
        if len(subdags) == 0: raise Exception("DockerWorker.delegate() received an empty list of subdags to delegate!")
        subdags.sort(key=lambda sd: sd.root_node.get_annotation(TaskWorkerResourceConfiguration).worker_id, reverse=True)
        tasks_grouped_by_id = {
            worker_id: list(tasks)
            for worker_id, tasks in groupby(subdags, key=lambda sd: sd.root_node.get_annotation(TaskWorkerResourceConfiguration).worker_id)
        }
        for worker_id in tasks_grouped_by_id.keys():
            worker_subdags = tasks_grouped_by_id[worker_id]
            # this function only delegates N tasks to 1 worker, so the config should be the same for all tasks
            targetWorkerResourcesConfig = worker_subdags[0].root_node.get_annotation(TaskWorkerResourceConfiguration)
            gateway_address = "http://host.docker.internal:5000" if called_by_worker else self.docker_config.docker_gateway_address
            logger.info(f"Invoking docker gateway ({gateway_address}) | Resource Configuration: {targetWorkerResourcesConfig}")
            async with aiohttp.ClientSession() as session:
                async with await session.post(
                    gateway_address + "/job",
                    data=json.dumps({
                        "resource_configuration": base64.b64encode(cloudpickle.dumps(targetWorkerResourcesConfig)).decode('utf-8'),
                        "dag_id": worker_subdags[0].master_dag_id,
                        "task_ids": base64.b64encode(cloudpickle.dumps([subdag.root_node.id for subdag in worker_subdags])).decode('utf-8'),
                        "config": base64.b64encode(cloudpickle.dumps(self.docker_config)).decode('utf-8'),
                    }),
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    if response.status != 202:
                        text = await response.text()
                        raise Exception(f"Failed to invoke worker: {text}")
