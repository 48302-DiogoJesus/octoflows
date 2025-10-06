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
        _nodes_info: dict[str, AbstractDAGPlanner.PlanningTaskInfo] = nodes_info
        
        # --- Step 1: group tasks by worker config + id ---
        workers: dict[str, list[AbstractDAGPlanner.PlanningTaskInfo]] = {}
        for node_info in _nodes_info.values():
            if node_info.node_ref.worker_config.worker_id is None: continue
            workers.setdefault(node_info.node_ref.worker_config.worker_id, []).append(node_info)

        # --- Step 2: build worker timelines ---
        worker_timelines: dict[str, dict] = {}
        for worker_key, tasks in workers.items():
            start = min(n.earliest_start_ms for n in tasks)
            end = max(n.earliest_start_ms + n.tp_exec_time_ms for n in tasks)
            startup = _predictions_provider.predict_worker_startup_time(tasks[0].node_ref.worker_config, "cold", _planner.config.sla)
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
        # HTTP handler latency for prewarm requests (in milliseconds)
        PREWARM_LATENCY_MS = 1500  # time to send request + be received + launch container
        PREWARM_TIMING_PREFERENCE = 0.6

        for wid, my_info in worker_timelines.items():
            if my_info["worker_startup_state"] != "cold":
                continue
            if not my_info["tasks"][0].node_ref.upstream_nodes:
                continue

            tasks_exec_time = sum(
                o.tp_exec_time_ms
                for wid, w in worker_timelines.items() if w["worker_config"].memory_mb == my_info["worker_config"].memory_mb
                for o in w["tasks"] if o.earliest_start_ms >= my_info["start"]
            )
            if my_info["startup"] >= tasks_exec_time:
                logger.warning(f"Doesn't compensate to prewarm | tasks exec time: {tasks_exec_time / 1000:.2f}s | Worker Startup: {my_info['startup'] / 1000:.2f}s")
                continue

            # Total time from trigger to warm state includes HTTP latency + startup
            total_prewarm_time_ms = PREWARM_LATENCY_MS + my_info["startup"]
            
            # Ideal trigger time accounting for HTTP latency
            ideal_prewarm_trigger_time = my_info["start"] - total_prewarm_time_ms

            best_worker = None
            best_delay_s = None
            best_prewarm_trigger_time = None
            candidates = []
            
            target_startup_time_ms = my_info["startup"]
            
            for other_key, other_info in worker_timelines.items():
                if other_key == wid:
                    continue

                worker_active_start = other_info["start"]
                worker_active_end = other_info["end"]

                # Latest trigger: prewarm must complete (including HTTP latency) before target starts
                # trigger + HTTP_latency + startup <= my_info["start"]
                latest_prewarm_trigger = my_info["start"] - total_prewarm_time_ms
                
                # Earliest trigger: worker should still be warm when needed
                # trigger + HTTP_latency + startup + cold_time >= my_info["start"]
                earliest_prewarm_trigger = my_info["start"] - total_prewarm_time_ms - time_until_worker_goes_cold_ms

                earliest_possible = max(earliest_prewarm_trigger, worker_active_start)
                latest_possible = min(latest_prewarm_trigger, worker_active_end)
                
                # Safety check: prewarming worker must start early enough
                if worker_active_start + total_prewarm_time_ms > my_info["start"]:
                    continue

                if earliest_possible > latest_possible: 
                    continue

                # Choose trigger time based on timing preference
                if PREWARM_TIMING_PREFERENCE <= 0.0:
                    actual_prewarm_trigger_time = earliest_possible
                elif PREWARM_TIMING_PREFERENCE >= 1.0:
                    actual_prewarm_trigger_time = latest_possible
                else:
                    window_size = latest_possible - earliest_possible
                    
                    if ideal_prewarm_trigger_time < earliest_possible:
                        actual_prewarm_trigger_time = earliest_possible + (PREWARM_TIMING_PREFERENCE * window_size)
                    elif ideal_prewarm_trigger_time > latest_possible:
                        actual_prewarm_trigger_time = earliest_possible + (PREWARM_TIMING_PREFERENCE * window_size)
                    else:
                        preference_based_time = earliest_possible + (PREWARM_TIMING_PREFERENCE * window_size)
                        actual_prewarm_trigger_time = (0.7 * ideal_prewarm_trigger_time + 
                                                    0.3 * preference_based_time)

                delay_from_start_ms = actual_prewarm_trigger_time - worker_active_start

                candidates.append({
                    "worker": other_info,
                    "delay_s": delay_from_start_ms / 1000.0,
                    "prewarm_trigger_time": actual_prewarm_trigger_time,
                    "worker_start": worker_active_start
                })

            if not candidates:
                pass
            else:
                if len(candidates) == 1:
                    best = candidates[0]
                elif len(candidates) == 2:
                    best = min(candidates, key=lambda c: c["worker_start"])
                else:
                    candidates_sorted = sorted(candidates, key=lambda c: c["worker_start"])
                    middle_candidates = candidates_sorted[1:-1]
                    best = min(middle_candidates, 
                            key=lambda c: abs(c["prewarm_trigger_time"] - ideal_prewarm_trigger_time))
                
                best_worker = best["worker"]
                best_delay_s = best["delay_s"]

            # --- Step 4: add annotation to first task of chosen worker ---
            if best_worker is not None:
                target_node = best_worker["first_node"]
                annotation = target_node.try_get_optimization(PreWarmOptimization)
                if not annotation: 
                    annotation = target_node.add_optimization(PreWarmOptimization([]))

                if best_delay_s is not None:
                    logger.info(f"[PREWARM-ASSIGNMENT] WID: {my_info['worker_config'].worker_id} tasks starting at {(my_info['start'] / 1000):.1f}s | trigger from WID: {best_worker['worker_config'].worker_id} @{((best_worker['start'] / 1000) + best_delay_s):.1f}s | worker startup: {(best_worker['startup'] / 1000):.1f}s | HTTP latency: {PREWARM_LATENCY_MS / 1000}s | timing pref: {PREWARM_TIMING_PREFERENCE}")

                annotation.target_resource_configs.append(
                    (best_delay_s, my_info["worker_config"])
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
