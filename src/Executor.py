import time
import asyncio

'''
Receives a DAG:
- starts by executing the root nodes
- waits for fan-ins
- invokes n - 1 executors on fan-outs
'''
class Executor:
    def __init__(self, dag):
        self.dag = dag
        self.ready_tasks = asyncio.Queue()
        for node in dag.root_nodes:
            print(f"Initially root {node.task_id} to ready queue...")
            start_execution_path(node)
        # self.task_queue = asyncio.Queue()

    def start_execution_path(self, subdag):
        while True:
            # TODO: Ability to execute more than 1 ready task at the same time
            task = self.ready_tasks.get()
            self._try_execute_task(task)

    def _try_execute_task(self, node):
        # WAIT FOR FAN-IN
        while not node.is_ready():
            print(f"{node.task_id} is NOT ready. Waiting for {node.upstream_nodes}...")
            time.sleep(1)

        node.execute()
        
        # FAN-OUT
        for dependent in node.downstream_nodes:
            print(f"Adding {dependent.task_id} to ready queue...")
            _ = self.ready_tasks.put(dependent)
