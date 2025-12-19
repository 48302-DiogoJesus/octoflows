import time
import asyncio
from dataclasses import dataclass
from src.dag.dag import  SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import  DAGTaskNode
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger
from src.storage.metadata.metrics_types import TaskOptimizationMetrics

logger = create_logger(__name__)

@dataclass
class PreWarmOptimization(TaskOptimization, WorkerExecutionLogic):
    """ Indicates what resource configurations should be prewarmed by the worker annotated with this optimization upon task execution start """

    target_resource_configs: list[tuple[int, TaskWorkerResourceConfiguration]] # (delay in seconds, resource config)

    prewarm_delay_s: int = 0 # time to send request + be received + launch container
    ready_offset_s: float = 0.5

    @dataclass
    class OptimizationMetrics(TaskOptimizationMetrics):
        resource_config: TaskWorkerResourceConfiguration
        absolute_trigger_timestamp_s: float

    @property
    def name(self) -> str: return "PreWarm"

    def clone(self): return PreWarmOptimization([(relative_time, config.clone()) for relative_time, config in self.target_resource_configs])

    @staticmethod
    def configured(prewarm_delay_s: int, ready_offset_s: float) -> type["PreWarmOptimization"]: 
        PreWarmOptimization.prewarm_delay_s = prewarm_delay_s
        PreWarmOptimization.ready_offset_s = ready_offset_s
        return PreWarmOptimization
    
    @staticmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]):
        from src.planning.abstract_dag_planner import AbstractDAGPlanner
        from src.planning.predictions.predictions_provider import PredictionsProvider

        _planner: AbstractDAGPlanner = planner
        _predictions_provider: PredictionsProvider = predictions_provider
        _nodes_info: dict[str, AbstractDAGPlanner.PlanningTaskInfo] = nodes_info

        # Time allowed for the prewarm request to travel and process (e.g., 1.0s)
        PREWARM_DELAY_MS = PreWarmOptimization.prewarm_delay_s * 1000
        # Buffer: ensure worker is ready X seconds BEFORE the first task actually starts
        READY_OFFSET_MS = PreWarmOptimization.ready_offset_s * 1000

        # Max idle time allowed before a worker is considered "cold" again
        TIME_UNTIL_COLD_MS = _planner.TIME_UNTIL_WORKER_GOES_COLD_S * 1000

        # --- Step 1: Group tasks by worker ---
        workers: dict[str, list[AbstractDAGPlanner.PlanningTaskInfo]] = {}
        for node_info in _nodes_info.values():
            if node_info.node_ref.worker_config.worker_id is None: continue
            workers.setdefault(node_info.node_ref.worker_config.worker_id, []).append(node_info)

        # --- Step 2: Build worker summaries ---
        worker_summaries: dict[str, dict] = {}
        for worker_key, tasks in workers.items():
            # Sort tasks by start time for accurate timeline analysis
            sorted_tasks = sorted(tasks, key=lambda n: n.earliest_start_ms)
            
            start_ms = sorted_tasks[0].earliest_start_ms
            last_task = max(tasks, key=lambda n: n.earliest_start_ms + n.tp_exec_time_ms)
            end_ms = last_task.earliest_start_ms + last_task.tp_exec_time_ms
            
            startup_ms = _predictions_provider.predict_worker_startup_time("cold", _planner.config.sla)
            
            worker_summaries[worker_key] = {
                "tasks": sorted_tasks,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "startup_ms": startup_ms,
                "worker_config": sorted_tasks[0].node_ref.worker_config,
                "worker_startup_state": sorted_tasks[0].worker_startup_state,
                "first_node_ref": sorted_tasks[0].node_ref
            }

        # --- Step 3: Assign Prewarms ---
        for wid, target_worker in worker_summaries.items():
            # Filter: We only care about cold starts
            if target_worker["worker_startup_state"] != "cold":
                continue
            
            # Filter: Root nodes (no upstream) generally handled by external triggers, skip if needed
            if not target_worker["first_node_ref"].upstream_nodes:
                continue

            # Calculate the specific moment we need the trigger to fire
            # Formula: (Start of Task) - (Buffer) - (Startup Time) - (Network Latency)
            # The trigger must be SENT at this time for the worker to be ready exactly at (Start - Buffer)
            required_trigger_time_ms = (
                target_worker["start_ms"] 
                - READY_OFFSET_MS 
                - target_worker["startup_ms"] 
                - PREWARM_DELAY_MS
            )

            best_candidate = None
            best_candidate_score = -1 # Higher is better

            # Search for a valid triggerer
            for candidate_id, candidate_worker in worker_summaries.items():
                if candidate_id == wid:
                    continue

                # Basic Sanity Check: Is the candidate even running during the needed time?
                # We check the candidate's global start/end window first
                if not (candidate_worker["start_ms"] <= required_trigger_time_ms <= candidate_worker["end_ms"]):
                    continue

                # --- Reliability Scoring ---
                # Check if the trigger time falls specifically inside a task execution (High Reliability)
                # or just in the idle gap between tasks (Low Reliability)
                
                reliability_score = 1 # Default: Candidate is generally alive (in gap)
                
                # Check for precise task overlap
                for task in candidate_worker["tasks"]:
                    t_start = task.earliest_start_ms
                    t_end = t_start + task.tp_exec_time_ms
                    
                    if t_start <= required_trigger_time_ms <= t_end:
                        reliability_score = 2 # Excellent: Candidate is actively processing
                        break
                
                # Selection Logic: Maximize reliability, then pick closest start time as tie-breaker
                if reliability_score > best_candidate_score:
                    best_candidate = candidate_worker
                    best_candidate_score = reliability_score
                elif reliability_score == best_candidate_score:
                    # Tie-breaker: Pick the one that started earlier (arbitrary stable choice)
                    # or you could pick the one with higher load, etc.
                    current_best_start = best_candidate["start_ms"] if best_candidate else float('inf')
                    if candidate_worker["start_ms"] < current_best_start:
                        best_candidate = candidate_worker

            # --- Step 4: Apply Optimization ---
            if best_candidate:
                # Calculate delay relative to the candidate's start time
                # Delay = Trigger Time - Candidate Start Time
                delay_ms = required_trigger_time_ms - best_candidate["start_ms"]
                delay_s = max(0.0, delay_ms / 1000.0)
                
                logger.info(
                    f"[PREWARM-ASSIGNMENT] "
                    f"Target WID: {target_worker['worker_config'].worker_id} | "
                    f"Needed @ {target_worker['start_ms']/1000:.1f}s | "
                    f"Worker Launch Time: {target_worker['startup_ms']/1000:.1f}s | "
                    f"PreWarm @ {required_trigger_time_ms/1000:.1f}s | "
                    f"Config [ReadyOffset: {PreWarmOptimization.ready_offset_s}s, NetDelay: {PreWarmOptimization.prewarm_delay_s}s]"
                )

                target_node = best_candidate["first_node_ref"]
                annotation = target_node.try_get_optimization(PreWarmOptimization)
                if not annotation:
                    annotation = target_node.add_optimization(PreWarmOptimization([]))
                
                annotation.target_resource_configs.append(
                    (delay_s, target_worker["worker_config"])
                )

        return

    @staticmethod
    async def wel_on_worker_ready(worker, dag):
        from src.workers.worker import Worker
        _worker: Worker = worker
        _dag: SubDAG = dag

        async def delayed_warmup(delay_s: float, node: DAGTaskNode, worker: Worker, dag_id: str, resource_config):
            try:
                if delay_s > 0:
                    await asyncio.sleep(delay_s)  # non-blocking wait
                await worker.warmup(dag_id, [resource_config])
                node.metrics.optimization_metrics.append(
                    PreWarmOptimization.OptimizationMetrics(resource_config=resource_config, absolute_trigger_timestamp_s=time.time())
                )
            except Exception as e:
                # optional: log error
                print(f"Warmup failed after {delay_s}s delay: {e}")

        for node in _dag._all_nodes.values():
            if node.worker_config.worker_id != _worker.my_resource_configuration.worker_id: continue

            prewarm_optimization = node.try_get_optimization(PreWarmOptimization)
            if prewarm_optimization is None: continue

            for relative_time, resource_config in prewarm_optimization.target_resource_configs:
                logger.info(f"Scheduling prewarm in {relative_time} for {resource_config}")
                # schedule into the future without blocking caller
                # "background" in the name so that the worker doesn't wait for this coroutine if it wants to exit (not a priority)
                asyncio.create_task(delayed_warmup(relative_time, node, _worker, dag.master_dag_id, resource_config), name=f"background_PreWarm-{node.id.get_internal_id()}")
