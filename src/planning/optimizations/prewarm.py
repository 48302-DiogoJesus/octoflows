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

    @dataclass
    class OptimizationMetrics(TaskOptimizationMetrics):
        resource_config: TaskWorkerResourceConfiguration
        absolute_trigger_timestamp_s: float

    @property
    def name(self) -> str: return "PreWarm"

    def clone(self): return PreWarmOptimization([(relative_time, config.clone()) for relative_time, config in self.target_resource_configs])

    @staticmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]):
        from src.planning.abstract_dag_planner import AbstractDAGPlanner
        from src.planning.predictions.predictions_provider import PredictionsProvider

        _planner: AbstractDAGPlanner = planner
        _predictions_provider: PredictionsProvider = predictions_provider

        # --- Step 1: group tasks by worker config + id ---
        workers = {}  # key: (worker_id) -> list of tasks
        for node_info in nodes_info.values():
            workers.setdefault(node_info.node_ref.worker_config.worker_id, []).append(node_info)

        # --- Step 2: build worker timelines ---
        worker_timelines = {}
        for worker_key, tasks in workers.items():
            start = min(n.earliest_start_ms for n in tasks)
            end = max(n.earliest_start_ms + n.tp_exec_time_ms for n in tasks)
            startup = max(n.tp_worker_startup_time_ms for n in tasks)
            first_node = min(tasks, key=lambda n: n.earliest_start_ms).node_ref
            worker_timelines[worker_key] = {
                "tasks": tasks,
                "start": start,
                "end": end,
                "startup": startup,
                "worker_config": tasks[0].node_ref.worker_config,
                "worker_startup_state": tasks[0].worker_startup_state,
                "first_node": first_node
            }

        # --- Step 3: assign prewarms ---
        time_until_worker_goes_cold_ms = _planner.TIME_UNTIL_WORKER_GOES_COLD_S * 1000
        
        # DEBUG: Tuning parameter for prewarm timing preference
        # Range: 0.0 to 1.0
        # - 0.0: earliest possible prewarm (maximum time buffer, safest but worker might go cold)
        # - 0.5: balanced
        # - 1.0: latest possible prewarm (closest to target start, riskier but fresher warm state)
        PREWARM_TIMING_PREFERENCE = 0.7

        for my_key, my_info in worker_timelines.items():
            # Skip if not cold start or if it's a root node (no upstream)
            if my_info["worker_startup_state"] != "cold":
                continue
            if not my_info["tasks"][0].node_ref.upstream_nodes:
                continue

            # Check if startup time is significant enough to warrant prewarming
            total_other_exec = sum(
                o.tp_exec_time_ms
                for k, w in worker_timelines.items() if k != my_key
                for o in w["tasks"]
            )
            if not (my_info["startup"] > 0.10 * total_other_exec):
                continue

            # logger.info(f"[PREWARM-ASSIGNMENT] Will try to prewarm worker {my_key}")
            # Calculate the ideal prewarm trigger time:
            # We want the worker to be warm exactly when my_info["start"] happens
            # Prewarm takes my_info["startup"] time to complete
            # So prewarm should be triggered at: my_info["start"] - my_info["startup"]
            ideal_prewarm_trigger_time = my_info["start"] - my_info["startup"]

            best_worker = None
            best_delay_s = None
            best_prewarm_trigger_time = None

            # Collect all valid candidate workers with their trigger times
            candidates = []
            
            # Get the target worker's startup time for margin calculations
            target_startup_time_ms = _predictions_provider.predict_worker_startup_time(
                my_info["worker_config"], "cold", _planner.config.sla
            )
            
            for other_key, other_info in worker_timelines.items():
                if other_key == my_key:
                    continue

                # The prewarm can be triggered any time while this worker is active
                worker_active_start = other_info["start"]
                worker_active_end = other_info["end"]

                # We need to trigger the prewarm such that:
                # 1. It happens while the other worker is active: [worker_active_start, worker_active_end]
                # 2. The target worker becomes warm before it goes cold: within time_until_worker_goes_cold_ms of my_info["start"]
                
                # Calculate the valid window for triggering prewarm
                # Latest we can trigger: prewarm must complete before target worker starts
                # Prewarm completes at: trigger_time + target_startup_time_ms
                # So: trigger_time + target_startup_time_ms <= my_info["start"]
                # Therefore: trigger_time <= my_info["start"] - target_startup_time_ms
                latest_prewarm_trigger = my_info["start"] - target_startup_time_ms
                
                # Earliest we can trigger: worker should still be warm when needed
                # Worker becomes warm at (trigger_time + target_startup_time_ms), and stays warm for time_until_worker_goes_cold_ms
                # So: trigger_time + target_startup_time_ms + time_until_worker_goes_cold_ms >= my_info["start"]
                # Therefore: trigger_time >= my_info["start"] - target_startup_time_ms - time_until_worker_goes_cold_ms
                earliest_prewarm_trigger = my_info["start"] - target_startup_time_ms - time_until_worker_goes_cold_ms

                # Clamp to when the worker is actually active
                # Also ensure the prewarming worker has enough time to complete before target starts
                # A worker that starts at time X can't prewarm a worker that starts at X (needs margin)
                earliest_possible = max(earliest_prewarm_trigger, worker_active_start)
                latest_possible = min(latest_prewarm_trigger, worker_active_end)
                
                # Additional safety check: ensure there's enough time for the prewarm to complete
                # The prewarming worker must start early enough that: 
                # worker_active_start + delay + target_startup_time_ms <= my_info["start"]
                if worker_active_start + target_startup_time_ms > my_info["start"]:
                    # This worker starts too late to prewarm the target
                    continue

                # Check if this worker can trigger the prewarm in a valid time window
                if earliest_possible > latest_possible: 
                    continue

                # Choose the trigger time based on timing preference
                # This allows tuning between safety (early) and freshness (late)
                if PREWARM_TIMING_PREFERENCE <= 0.0:
                    # Prefer earliest possible
                    actual_prewarm_trigger_time = earliest_possible
                elif PREWARM_TIMING_PREFERENCE >= 1.0:
                    # Prefer latest possible (closest to target start)
                    actual_prewarm_trigger_time = latest_possible
                else:
                    # Interpolate between earliest and latest based on preference
                    # But also consider the ideal time
                    window_size = latest_possible - earliest_possible
                    
                    if ideal_prewarm_trigger_time < earliest_possible:
                        # Ideal is too early, use preference to choose in valid window
                        actual_prewarm_trigger_time = earliest_possible + (PREWARM_TIMING_PREFERENCE * window_size)
                    elif ideal_prewarm_trigger_time > latest_possible:
                        # Ideal is too late, use preference to choose in valid window
                        actual_prewarm_trigger_time = earliest_possible + (PREWARM_TIMING_PREFERENCE * window_size)
                    else:
                        # Ideal is within valid window
                        # Blend between ideal time and preference-based time
                        preference_based_time = earliest_possible + (PREWARM_TIMING_PREFERENCE * window_size)
                        # Weight: 70% ideal, 30% preference (adjust these weights as needed)
                        actual_prewarm_trigger_time = (0.7 * ideal_prewarm_trigger_time + 
                                                    0.3 * preference_based_time)

                # Calculate delay from the worker's start
                delay_from_start_ms = actual_prewarm_trigger_time - worker_active_start

                candidates.append({
                    "worker": other_info,
                    "delay_s": delay_from_start_ms / 1000.0,
                    "prewarm_trigger_time": actual_prewarm_trigger_time,
                    "worker_start": worker_active_start
                })

            # Choose the best worker: prefer one in the middle of the timeline for robustness
            if not candidates:
                # logger.warning(f"[PREWARM-ASSIGNMENT] No valid candidates for prewarm for worker starting at {my_info['start']}ms")
                pass
            else:
                if len(candidates) == 1:
                    best = candidates[0]
                elif len(candidates) == 2:
                    # With 2 candidates, pick the earlier one (more time buffer)
                    best = min(candidates, key=lambda c: c["worker_start"])
                else:
                    # With 3+ candidates, avoid earliest and latest, pick middle one(s)
                    # Sort by worker start time
                    candidates_sorted = sorted(candidates, key=lambda c: c["worker_start"])
                    
                    # Filter out the earliest and latest starting workers
                    middle_candidates = candidates_sorted[1:-1]
                    
                    # From middle candidates, choose the one closest to ideal prewarm time
                    best = min(middle_candidates, 
                            key=lambda c: abs(c["prewarm_trigger_time"] - ideal_prewarm_trigger_time))
                
                best_worker = best["worker"]
                best_delay_s = best["delay_s"]

            # --- Step 4: add annotation to first task of chosen worker ---
            if best_worker is not None:
                target_node = best_worker["first_node"]  # always attach to first task of the elected worker to perform the prewarming
                annotation = target_node.try_get_optimization(PreWarmOptimization)
                if not annotation: 
                    annotation = target_node.add_optimization(PreWarmOptimization([]))

                if best_delay_s is not None:
                    logger.info(f"[PREWARM-ASSIGNMENT] Prewarm successful for worker id: {my_info['worker_config'].worker_id} starting at {my_info['start']}ms: "
                        f"trigger from worker id {best_worker['worker_config'].worker_id} (trigger at {(best_worker['start'] + (best_delay_s * 1000)):.1f}ms)")

                annotation.target_resource_configs.append(
                    (best_delay_s, my_info["worker_config"])
                )

                # recompute timings after adding annotation
                nodes_info = _planner._calculate_workflow_timings(
                    dag, topo_sorted_nodes, _predictions_provider, _planner.config.sla
                )

        return

    @staticmethod
    async def wel_on_worker_ready(planner, intermediate_storage, metadata_storage, dag, this_worker_id: str | None, this_worker):
        from src.workers.worker import Worker
        _this_worker: Worker = this_worker
        _dag: SubDAG = dag

        async def delayed_warmup(delay_s: float, node: DAGTaskNode, worker: Worker, dag_id: str, resource_config):
            try:
                if delay_s > 0:
                    await asyncio.sleep(delay_s)  # non-blocking wait
                node.metrics.optimization_metrics.append(
                    PreWarmOptimization.OptimizationMetrics(resource_config=resource_config, absolute_trigger_timestamp_s=time.time())
                )
                await worker.warmup(dag_id, [resource_config])
            except Exception as e:
                # optional: log error
                print(f"Warmup failed after {delay_s}s delay: {e}")

        for node in _dag._all_nodes.values():
            if node.worker_config.worker_id != this_worker_id: continue

            prewarm_optimization = node.try_get_optimization(PreWarmOptimization)
            if prewarm_optimization is None: continue

            for relative_time, resource_config in prewarm_optimization.target_resource_configs:
                logger.info(f"Scheduling prewarm in {relative_time} for {resource_config}")
                # schedule into the future without blocking caller
                # "background" in the name so that the worker doesn't wait for this coroutine if it wants to exit (not a priority)
                asyncio.create_task(delayed_warmup(relative_time, node, _this_worker, dag.master_dag_id, resource_config), name=f"background_PreWarm-{node.id.get_full_id()}")
