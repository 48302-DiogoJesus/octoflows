import asyncio
import sys
import base64
import cloudpickle
import os
import platform

from src.dag_task_annotation import TaskAnnotation
from src.utils.utils import get_method_overridden
from src.workers.worker_execution_logic import WorkerExecutionLogic

# Define a lock file path
LOCK_FILE = "/tmp/script.lock" if platform.system() != "Windows" else "C:\\Windows\\Temp\\script.lock"

# Be at the same level as the ./src directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.events import TASK_READY_EVENT_PREFIX
from src.workers.docker_worker import DockerWorker
from src.storage.metrics.metrics_types import FullDAGPrepareTime
from src.utils.timer import Timer
from src.dag_task_node import DAGTaskNode, DAGTaskNodeId
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
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB) # type: ignore
    except (IOError, BlockingIOError) as e:
        logger.error("Error: Another instance of the script is already running. Exiting.")
        raise e

    try:
        logger.info("[DOCKER_WORKER] Started")

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

        immediate_task_ids: list[DAGTaskNodeId] = cloudpickle.loads(base64.b64decode(b64_task_ids))
        logger.info(f"I should do: {[id.get_full_id() for id in immediate_task_ids]}")
        
        all_tasks_for_this_worker: list[DAGTaskNode] = []
        this_worker_id = fulldag.get_node_by_id(immediate_task_ids[0]).get_annotation(TaskWorkerResourceConfiguration).worker_id
        _nodes_to_visit = fulldag.root_nodes
        visited_nodes = set()
        while _nodes_to_visit:
            current_node = _nodes_to_visit.pop(0)
            if current_node.id.get_full_id() in visited_nodes: continue
            visited_nodes.add(current_node.id.get_full_id())

            # Execute on_worker_ready for each annotation
            for annotation in current_node.annotations:
                on_worker_ready_overriden = get_method_overridden(annotation.__class__, WorkerExecutionLogic.override_on_worker_ready)
                if on_worker_ready_overriden: 
                    await on_worker_ready_overriden(wk.intermediate_storage, current_node, fulldag, this_worker_id)
                else: 
                    await WorkerExecutionLogic.override_on_worker_ready(wk.intermediate_storage, current_node, fulldag, this_worker_id)

            if current_node.get_annotation(TaskWorkerResourceConfiguration).worker_id == this_worker_id: all_tasks_for_this_worker.append(current_node)
            
            for downstream_node in current_node.downstream_nodes:
                if downstream_node.id.get_full_id() not in visited_nodes: _nodes_to_visit.append(downstream_node)
        
        def _on_task_ready_callback_builder(task_id: DAGTaskNodeId):
            def callback(_: dict):
                logger.info(f"Task {task_id.get_full_id()} is READY! Start executing...")
                subdag = fulldag.create_subdag(fulldag.get_node_by_id(task_id))
                asyncio.create_task(wk.start_executing(subdag))
            return callback
        
        for task in all_tasks_for_this_worker:
            if task.id in immediate_task_ids: continue # don't need to sub to these because we know they are READY
            if all(n.get_annotation(TaskWorkerResourceConfiguration).worker_id == this_worker_id for n in task.upstream_nodes):
                continue
            await wk.metadata_storage.subscribe(f"{TASK_READY_EVENT_PREFIX}{task.id.get_full_id_in_dag(fulldag)}", _on_task_ready_callback_builder(task.id))
            # all_dependent_tasks_for_this_worker.append(task)

        create_subdags_time_ms = Timer()
        direct_task_branches_coroutines = []
        # Launch direct invocation tasks concurrently
        for task_id in immediate_task_ids:
            node = fulldag.get_node_by_id(task_id)
            subdag = fulldag.create_subdag(node)
            direct_task_branches_coroutines.append(asyncio.create_task(wk.start_executing(subdag)))
        create_subdags_time_ms = create_subdags_time_ms.stop()

        logger.info(f"Waiting for {len(direct_task_branches_coroutines)} direct task branches to complete...")
        await asyncio.gather(*direct_task_branches_coroutines)

        # Wait for ALL tasks assigned to this worker to complete
        remaining_tasks_for_this_worker = [task for task in all_tasks_for_this_worker if not task.completed_event.is_set()]
        if len(remaining_tasks_for_this_worker) > 0:
            completion_events = [task.completed_event for task in remaining_tasks_for_this_worker]
            logger.info(f"Worker: {this_worker_id} | Waiting for {[task.id.get_full_id() for task in remaining_tasks_for_this_worker]} to complete locally...")
            await asyncio.wait([asyncio.create_task(event.wait()) for event in completion_events])

        # Wait for remaining coroutines to finish. Just because the final result is ready doesn't mean all work is done (emitting READY events, etc...)
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if pending:
            await asyncio.wait(pending, timeout=None)  # Wait indefinitely

        if wk.metrics_storage:
            wk.metrics_storage.store_dag_download_time(
                immediate_task_ids[0].get_full_id_in_dag(fulldag),
                FullDAGPrepareTime(download_time_ms=dag_download_time_ms, size_bytes=dag_size_bytes, create_subdags_time_ms=create_subdags_time_ms)
            )
            await wk.metrics_storage.flush()

        logger.info(f"Worker({this_worker_id}) [DOCKER_WORKER] Execution completed successfully!")
    finally:
        # Release the lock and clean up
        if platform.system() == "Windows":
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        else:
            fcntl.flock(lock_file, fcntl.LOCK_UN) # type: ignore
            lock_file.close()
            os.remove(LOCK_FILE)

if __name__ == '__main__':
    # Run the main async function and wait until it completes
    asyncio.run(main())