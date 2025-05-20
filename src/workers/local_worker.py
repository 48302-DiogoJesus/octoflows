import asyncio
from dataclasses import dataclass
from src.dag import dag
from src.utils.logger import create_logger
from src.workers.worker import Worker

logger = create_logger(__name__)

class LocalWorker(Worker):
    @dataclass
    class Config(Worker.Config):
        def create_instance(self) -> "LocalWorker": return LocalWorker(self)

    local_config: Config

    """
    Processes DAG tasks
    continuing with single downstream tasks and spawning new workers (coroutines) for branches.
    """
    def __init__(self, config: Config):
       if config.planner_config is not None: raise Exception("LocalWorker does not support planning")
       if config.metrics_storage_config is not None: raise Exception("LocalWorker does not support metrics storage")
       super().__init__(config)
       self.local_config = config
    
    async def delegate(self, subdags: list[dag.SubDAG], called_by_worker: bool = True):
        for subdag in subdags:
            await asyncio.create_task(self.start_executing(subdag), name=f"local_delegate_subdag(task={subdag.root_node.id.get_full_id()})")