import asyncio
import tempfile
import base64
from collections import deque
import sys
import threading
import cloudpickle
import streamlit as st
import time
import subprocess
import os
import nest_asyncio
import signal

nest_asyncio.apply()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.logger import create_logger
import src.storage.storage as storage

logger = create_logger(__name__)

REFRESH_RATE_S = 3


class DAGVisualizationDashboard:
    def __init__(self, dag, worker_config):
        from src.workers import worker
        _worker_config: worker.Worker.Config = worker_config
        self.dag = dag
        self.intermediate_storage: storage.Storage = _worker_config.intermediate_storage_config.create_instance()
        self._lock = threading.RLock()
        self.completed_tasks = set()
        self.all_tasks_completed = False  # Add flag to track completion status

    @staticmethod
    def start(dag, worker_config):
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".dag") as dag_file, tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".config"
        ) as config_file:
            dag_file.write(base64.b64encode(cloudpickle.dumps(dag)).decode("utf-8"))
            config_file.write(base64.b64encode(cloudpickle.dumps(worker_config)).decode("utf-8"))

            dag_path = dag_file.name
            config_path = config_file.name

        current_script = os.path.abspath(__file__)
        # Start dashboard as a completely independent process
        dashboard_process = subprocess.Popen(
            ["streamlit", "run", current_script, config_path, dag_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            # Make it independent from parent process
            start_new_session=True,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,  # type: ignore
        )

        # Don't register cleanup - let the dashboard run independently
        return dashboard_process

    def _update_completed_tasks(self):
        """Check intermediate storage for completed tasks and update state."""
        if self.all_tasks_completed:
            return

        async def update():
            async with self.intermediate_storage.batch() as batch:
                for node_id, node in self.dag._all_nodes.items():
                    if node_id in self.completed_tasks:
                        continue
                    await batch.exists(node.id.get_full_id_in_dag(self.dag), result_key=node_id)

                await batch.execute()

                newly_completed = []
                for node_id in self.dag._all_nodes.keys():
                    if batch.get_result(node_id) and node_id not in self.completed_tasks:
                        newly_completed.append(node_id)
                        self.completed_tasks.add(node_id)

                if newly_completed:
                    self._propagate_completion_upstream(newly_completed)

                if len(self.completed_tasks) >= len(self.dag._all_nodes):
                    self.all_tasks_completed = True
                    logger.info("All DAG tasks completed. Stopping completion checks.")

        asyncio.run(update())

    def render_agraph(self):
        from streamlit_agraph import agraph, Node, Edge, Config
        self.dag: FullDAG = self.dag  # type hint without circular dep

        nodes = []
        edges = []
        node_levels = {}
        visited = set()

        def traverse_dag(node, level=0):
            node_id = node.id.get_full_id()

            if node_id in visited:
                return
            visited.add(node_id)
            node_levels[node_id] = level

            is_completed = node_id in self.completed_tasks
            node_color = "#4CAF50" if is_completed else "#9E9E9E"

            nodes.append(
                Node(
                    id=node_id,
                    label=node.func_name,
                    size=25,
                    color=node_color,
                    shape="box",
                    font={"color": "white", "size": 15, "face": "Arial" },
                    level=level,
                )
            )

            for downstream in node.downstream_nodes:
                downstream_id = downstream.id.get_full_id()
                edges.append(Edge(source=node_id, target=downstream_id, arrow="to", color="#888888", width=1))
                traverse_dag(downstream, level + 1)

        assert self.dag.root_nodes
        for root in self.dag.root_nodes:
            traverse_dag(root, level=0)

        config = Config(
            width="100%",  # type: ignore
            height=600,
            directed=True,
            physics=False,
            hierarchical=True,
            hierarchical_sort_method="directed",
        )

        return agraph(nodes=nodes, edges=edges, config=config)

    def run_dashboard(self):
        st.set_page_config(layout="wide", page_title="Real-time Workflow Visualization")

        # Add sidebar with controls
        if st.button("Close Dashboard", type="primary"):
            os.kill(os.getpid(), signal.SIGTERM)
            st.stop()

        self._check_completed_tasks()
        self._update_completed_tasks()

        completed = sum(
            1 for name, node in self.dag._all_nodes.items() if node.id.get_full_id() in self.completed_tasks
        )
        total = len(self.dag._all_nodes)
        st.write(f"{completed}/{total} tasks completed")

        selected_node = self.render_agraph()
        if selected_node:
            st.write(f"**Selected Node:** {selected_node}")

        if selected_node:
            st.session_state.last_selected = selected_node

        if not self.all_tasks_completed:
            time.sleep(REFRESH_RATE_S)
            st.rerun()

    def _propagate_completion_upstream(self, newly_completed):
        tasks_to_process = deque(newly_completed)

        while tasks_to_process:
            current_task_id = tasks_to_process.popleft()

            if current_task_id in self.dag._all_nodes:
                current_node = self.dag._all_nodes[current_task_id]
                upstream_nodes = getattr(current_node, "upstream_nodes", [])

                for upstream_node in upstream_nodes:
                    upstream_id = upstream_node.id.get_full_id()
                    if upstream_id not in self.completed_tasks:
                        self.completed_tasks.add(upstream_id)
                        tasks_to_process.append(upstream_id)

    def _check_completed_tasks(self):
        assert self.dag.root_nodes is not None
        visited = set()
        queue = deque(self.dag.root_nodes)

        while queue:
            node = queue.popleft()
            if node.id.get_full_id() in visited:
                continue

            if node.id.get_full_id() in self.completed_tasks:
                visited.add(node.id.get_full_id())
                for downstream in node.downstream_nodes:
                    queue.append(downstream)


if __name__ == "__main__":
    if "initialized" not in st.session_state:
        st.session_state.initialized = False

    if not st.session_state.initialized:
        st.session_state.initialized = True
        from src.workers.worker import Worker
        from src.dag.dag import FullDAG
        import base64
        import cloudpickle

        if len(sys.argv) != 3:
            raise Exception("Usage: python script.py <config_path> <dag_path>")

        try:
            with open(sys.argv[1], "r") as config_file:
                config = cloudpickle.loads(base64.b64decode(config_file.read()))
            with open(sys.argv[2], "r") as dag_file:
                dag = cloudpickle.loads(base64.b64decode(dag_file.read()))

            if not isinstance(config, Worker.Config):
                raise Exception("Error: config is not a Worker.Config instance")
            if not isinstance(dag, FullDAG):
                raise Exception("Error: dag is not a DAG instance")

            st.session_state.dashboard = DAGVisualizationDashboard(dag, config)

        except Exception as e:
            raise Exception(f"Error loading dashboard: {e}")

    st.session_state.dashboard.run_dashboard()
