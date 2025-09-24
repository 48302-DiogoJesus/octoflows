import asyncio
from dataclasses import dataclass
from src.dag.dag import  SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import  DAGTaskNode
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger

logger = create_logger(__name__)

@dataclass
class PreWarmOptimization(TaskOptimization, WorkerExecutionLogic):
    """ Indicates what resource configurations should be prewarmed by the worker annotated with this optimization upon task execution start """

    target_resource_configs: list[TaskWorkerResourceConfiguration]

    def clone(self): return PreWarmOptimization([config.clone() for config in self.target_resource_configs])

    @staticmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]):
        from src.planning.abstract_dag_planner import AbstractDAGPlanner
        from src.planning.predictions.predictions_provider import PredictionsProvider

        _planner: AbstractDAGPlanner = planner
        _predictions_provider: PredictionsProvider = predictions_provider
        
        # For each node that has a cold start
        for my_node_id, node_info in nodes_info.items():
            if node_info.worker_startup_state != "cold": continue
            if len(node_info.node_ref.upstream_nodes) == 0: continue # ignore root nodes

            # Calculate sum of execution times of tasks with same worker config that start after this node
            my_worker_config = node_info.node_ref.worker_config
            sum_exec_times = 0
            
            # Get tasks with same resources that start after this node (because they could also benefit from pre-warm)
            for other_node_id, other_node_info in nodes_info.items():
                if other_node_id == my_node_id: continue
                other_worker_config = other_node_info.node_ref.worker_config
                if other_worker_config.cpus != my_worker_config.cpus or other_worker_config.memory_mb != my_worker_config.memory_mb: continue
                if other_node_info.earliest_start_ms > node_info.earliest_start_ms:
                    sum_exec_times += other_node_info.tp_exec_time_ms
            

            if not (node_info.tp_worker_startup_time_ms > 0.15 * sum_exec_times):
                # don't apply pre-warm if the startup time is not significant when compared to the time that worker will be executing
                continue
            
            # Find the best node to add pre-warm annotation to
            best_node = None
            best_start_time = -1
            time_until_worker_goes_cold_ms = _planner.TIME_UNTIL_WORKER_GOES_COLD_S * 1000
            
            for other_node_id, other_node_info in nodes_info.items():
                if other_node_id == my_node_id: continue
                
                # time at which the worker config I need would be available if I were to add pre-warm annotation to this node
                my_worker_potential_ready_if_prewarmed = other_node_info.earliest_start_ms + node_info.tp_worker_startup_time_ms
                # avoid the worker being ready but cold by the time we need it
                min_prewarm_time = max(0, node_info.earliest_start_ms - time_until_worker_goes_cold_ms + time_until_worker_goes_cold_ms / 3)
                max_prewarm_time = max(0, node_info.earliest_start_ms)
                is_in_optimal_prewarm_window = min_prewarm_time < my_worker_potential_ready_if_prewarmed < max_prewarm_time
                
                if is_in_optimal_prewarm_window and (best_node is None or other_node_info.earliest_start_ms > best_start_time):
                    best_node = other_node_info.node_ref
                    best_start_time = other_node_info.earliest_start_ms
            
            # Add pre-warm annotation to the best node found
            if best_node is not None:
                annotation = best_node.try_get_optimization(PreWarmOptimization)
                if not annotation: annotation = best_node.add_optimization(PreWarmOptimization([]))
                # allow multiple pre-warms for the same worker config (only makes sense with local docker implementation. Lambda implementation)
                
                annotation.target_resource_configs.append(my_worker_config)
                # recomputing node timings is required because after adding `PreWarm` annotation, other tasks "cold" starts may become "warm"
                #  and the next iteration of this "pre-warm annotation assignment" algorithm needs to know the updated state ("cold" | "warm")
                nodes_info = _planner._calculate_workflow_timings(topo_sorted_nodes, _predictions_provider, _planner.config.sla)
        
        return nodes_info

    @staticmethod
    async def wel_before_task_handling(planner, this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode, is_dupping: bool):
        from src.workers.worker import Worker
        _this_worker: Worker = this_worker
        
        prewarm_optimization = current_task.try_get_optimization(PreWarmOptimization)
        if prewarm_optimization is None: return

        # "fire-and-forget" / non-blocking
        asyncio.create_task(_this_worker.warmup(subdag.master_dag_id, prewarm_optimization.target_resource_configs))
