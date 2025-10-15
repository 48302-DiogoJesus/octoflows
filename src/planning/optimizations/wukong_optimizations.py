from dataclasses import dataclass
import cloudpickle
import asyncio

from src.dag.dag import SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import DAGTaskNode
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger
from src.storage.prefixes import DEPENDENCY_COUNTER_PREFIX
from src.utils.utils import calculate_data_structure_size_bytes
from src.utils.timer import Timer

logger = create_logger(__name__)

@dataclass
class WukongOptimizations(TaskOptimization, WorkerExecutionLogic):
    """ 
    Implements the following optimizations:
    - Task Clustering on Fan-Outs
        - If a task has large output, execute all READY tasks locally 
    - Task Clustering on Fan-Ins
        - If a task has large output and wasn't able to 
    - Delayed I/O
        - If a task has large output, execute all READY tasks locally + re-check storage to see if other tasks became READY
    """

    task_clustering_fan_outs: bool
    task_clustering_fan_ins: bool
    delayed_io: bool
    large_output_b: int

    def __init__(self, task_clustering_fan_outs: bool | None = None, task_clustering_fan_ins: bool | None = None, delayed_io: bool | None = None, large_output_b: int | None = None):
        self.task_clustering_fan_outs = task_clustering_fan_outs if task_clustering_fan_outs is not None else WukongOptimizations.task_clustering_fan_outs
        self.task_clustering_fan_ins = task_clustering_fan_ins if task_clustering_fan_ins is not None else WukongOptimizations.task_clustering_fan_ins
        self.delayed_io = delayed_io if delayed_io is not None else WukongOptimizations.delayed_io
        self.large_output_b = large_output_b if large_output_b is not None else WukongOptimizations.large_output_b

    @property
    def name(self) -> str: return f"Wukong(tco={self.task_clustering_fan_outs}, tci={self.task_clustering_fan_ins}, io={self.delayed_io}, lo={self.large_output_b / 1024 / 1024}MB)"

    def clone(self): return WukongOptimizations()

    @staticmethod
    def configured(task_clustering_fan_outs: bool, task_clustering_fan_ins: bool, delayed_io: bool, large_output_b: int) -> type["WukongOptimizations"]: 
        WukongOptimizations.task_clustering_fan_outs = task_clustering_fan_outs
        WukongOptimizations.task_clustering_fan_ins = task_clustering_fan_ins
        WukongOptimizations.delayed_io = delayed_io
        WukongOptimizations.large_output_b = large_output_b
        return WukongOptimizations

    @staticmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]):
        for node in topo_sorted_nodes:
            is_fan_out_origin = len(node.downstream_nodes) > 1
            is_fan_in_upstream = len(node.downstream_nodes) > 0 and any([dnode for dnode in node.downstream_nodes if len(dnode.upstream_nodes) > 1])
            if is_fan_out_origin or is_fan_in_upstream: 
                node.add_optimization(WukongOptimizations(
                    task_clustering_fan_outs=is_fan_out_origin,
                    task_clustering_fan_ins=is_fan_in_upstream,
                    delayed_io=is_fan_out_origin and WukongOptimizations.delayed_io,
                ))

    @staticmethod
    async def wel_override_should_upload_output(worker, task: DAGTaskNode, subdag: SubDAG):
        from src.workers.worker import Worker
        optimization = task.try_get_optimization(WukongOptimizations)
        _worker: Worker = worker
        if optimization is None: return None
        if not optimization.task_clustering_fan_outs and not optimization.delayed_io and not optimization.task_clustering_fan_ins: return None
        assert task.cached_result
        
        if not optimization.task_clustering_fan_outs and not optimization.delayed_io and optimization.task_clustering_fan_ins:
            # When TCI, we always upload output, then check DCs again, and return all tasks with deps - 1 to execute locally
            return True

        task_output_size_b = calculate_data_structure_size_bytes(cloudpickle.dumps(task.cached_result.result))
        is_task_size_large = task_output_size_b > optimization.large_output_b
        logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) Task {task.id.get_full_id()} is large?: {is_task_size_large} | size: {task_output_size_b / 1024 / 1024}MB")
        return not is_task_size_large # only upload if not large

    @staticmethod
    async def wel_update_dependency_counters(worker, task: DAGTaskNode, subdag) -> list | None:
        from src.workers.worker import Worker
        _worker: Worker = worker
        optimization: WukongOptimizations | None = task.try_get_optimization(WukongOptimizations)
        if optimization is None: return None
        if not optimization.task_clustering_fan_outs and not optimization.delayed_io and not optimization.task_clustering_fan_ins: return None
        assert task.cached_result

        task_output_size_b = calculate_data_structure_size_bytes(cloudpickle.dumps(task.cached_result.result))
        is_task_size_large = task_output_size_b > optimization.large_output_b
        if not is_task_size_large: return None # let the default logic update DCs
        else: 
            # Same logic for TCI, TCO and DIO
            #   If no task has deps - 1, upload output, increment DC, and recheck
            #   Else return all tasks with deps - 1 to execute locally
            downstream_tasks_ready: list[DAGTaskNode] = []
            for downstream_task in task.downstream_nodes:
                dependencies_met = await _worker.metadata_storage.storage.get(f"{DEPENDENCY_COUNTER_PREFIX}{downstream_task.id.get_full_id_in_dag(subdag)}")
                dependencies_met = 0 if dependencies_met is None else int(dependencies_met)
                
                logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) Task {downstream_task.id.get_full_id()} Dependencies met: {dependencies_met}/{len(downstream_task.upstream_nodes)}")
                # dependencies_met could be none (not true when using increment_and_get)
                if (dependencies_met or 0) == len(downstream_task.upstream_nodes) - 1: # Would be READY if we incremented DC
                    downstream_tasks_ready.append(downstream_task)

            if len(downstream_tasks_ready) == 0:
                # upload output and return [] (worker exits)
                serialized_task_result = cloudpickle.dumps(task.cached_result.result)
                output_upload_timer = Timer()
                await _worker.intermediate_storage.set(task.id.get_full_id_in_dag(subdag), serialized_task_result)
                task.metrics.output_metrics.tp_time_ms = output_upload_timer.stop()
                task.upload_complete.set()
                _worker.log(task.id.get_full_id(), f"Big fan-out task had to upload output")
                # Increment DCs
                updating_dependency_counters_timer = Timer()
                # update DCs of ALL my downstream
                for downstream_task in task.downstream_nodes:
                    deps = await _worker.metadata_storage.storage.atomic_increment_and_get(f"{DEPENDENCY_COUNTER_PREFIX}{downstream_task.id.get_full_id_in_dag(subdag)}")
                    logger.info(f"[WUKONG_DBG] TCI UDC W({_worker.debug_worker_id}) Updated DC for {downstream_task.id.get_full_id()} ({deps}/{len(downstream_task.upstream_nodes)})")
                    # need to recheck, otherwise, another worker would see 8/10 and NOT execute. No one would execute the task
                    if deps == len(downstream_task.upstream_nodes):
                        downstream_tasks_ready.append(downstream_task)
                task.metrics.update_dependency_counters_time_ms = (task.metrics.update_dependency_counters_time_ms or 0) + updating_dependency_counters_timer.stop()
                return downstream_tasks_ready
            else:
                return downstream_tasks_ready

    @staticmethod
    async def wel_override_handle_downstream(worker, task: DAGTaskNode, fulldag, subdag, downstream_tasks_ready: list[DAGTaskNode]):
        from src.workers.worker import Worker
        _worker: Worker = worker

        optimization = task.try_get_optimization(WukongOptimizations)
        if optimization is None: return None
        if not downstream_tasks_ready: return []
        if not optimization.task_clustering_fan_outs and not optimization.delayed_io and not optimization.task_clustering_fan_ins: return None
        assert task.cached_result
        task_output_size_b = calculate_data_structure_size_bytes(cloudpickle.dumps(task.cached_result.result))
        is_task_size_large = task_output_size_b > optimization.large_output_b

        is_fan_in_upstream = len(task.downstream_nodes) > 0 and any([dnode for dnode in task.downstream_nodes if len(dnode.upstream_nodes) > 1])
        is_fan_out_origin = len(task.downstream_nodes) > 1
        tasks_completed: set[str] = set()

        if is_task_size_large:
            if is_fan_in_upstream:
                # upload large output
                serialized_task_result = cloudpickle.dumps(task.cached_result.result)
                output_upload_timer = Timer()
                await _worker.intermediate_storage.set(task.id.get_full_id_in_dag(subdag), serialized_task_result)
                task.metrics.output_metrics.tp_time_ms = output_upload_timer.stop()
                task.upload_complete.set()
                _worker.log(task.id.get_full_id(), f"Big fan-out task had to upload output")
                # Update DCs because output is already available
                updating_dependency_counters_timer = Timer()
                # update DCs of ALL my downstream
                for downstream_task in task.downstream_nodes:
                    deps = await _worker.metadata_storage.storage.atomic_increment_and_get(f"{DEPENDENCY_COUNTER_PREFIX}{downstream_task.id.get_full_id_in_dag(subdag)}")
                    logger.info(f"[WUKONG_DBG] TCI W({_worker.debug_worker_id}) Updated DC for {downstream_task.id.get_full_id()} ({deps}/{len(downstream_task.upstream_nodes)})")
                task.metrics.update_dependency_counters_time_ms = (task.metrics.update_dependency_counters_time_ms or 0) + updating_dependency_counters_timer.stop()

                # then re-check if in the meantime other tasks became READY, if so execute them
                for dtask in task.downstream_nodes:
                    if any([dnode.id.get_full_id() == dtask.id.get_full_id() for dnode in downstream_tasks_ready]):
                        # ignore if task was already READY
                        continue
                    if len(dtask.upstream_nodes) > 1:
                        dependencies_met = await _worker.metadata_storage.storage.get(f"{DEPENDENCY_COUNTER_PREFIX}{dtask.id.get_full_id_in_dag(subdag)}")
                        dependencies_met = 0 if dependencies_met is None else int(dependencies_met)
                        logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) TCI | Task {dtask.id.get_full_id()} dependencies met: {dependencies_met}/{len(dtask.upstream_nodes)}")
                        if dependencies_met == len(dtask.upstream_nodes):
                            assert _worker.debug_worker_id
                            asyncio.create_task(_worker.execute_branch(subdag.create_subdag(dtask), fulldag), name=f"TCI_{dtask.id.get_full_id()}")
                            logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) TCI | Executing task {dtask.id.get_full_id()}...")
                            await dtask.completed_event.wait()
                            logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) TCI | Task {dtask.id.get_full_id()} completed")
                            tasks_completed.add(dtask.id.get_full_id())
            
            # TASK-CLUSTERING ON FAN-OUTS + DELAYED I/O
            if is_fan_out_origin and (optimization.delayed_io or optimization.task_clustering_fan_outs):
                logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) Task {task.id.get_full_id()} is a fan-out origin. dio={optimization.delayed_io}, tco={optimization.task_clustering_fan_outs} | downstream_tasks_ready_count={len(downstream_tasks_ready)}")
                mutable_downstream_tasks_ready = downstream_tasks_ready.copy()
                while mutable_downstream_tasks_ready:
                    dtask_ready = mutable_downstream_tasks_ready.pop() # remove task
                    # can't await `execute_branch` as it will keep on going and we only want to wait for the first task to complete
                    asyncio.create_task(_worker.execute_branch(subdag.create_subdag(dtask_ready), fulldag), name=f"TCO_DIO_{dtask_ready.id.get_full_id()}")
                    logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) TCO | Executing task {dtask_ready.id.get_full_id()}...")
                    await dtask_ready.completed_event.wait()
                    logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) TCO | Task {dtask_ready.id.get_full_id()} completed")
                    tasks_completed.add(dtask_ready.id.get_full_id())

                    if not optimization.delayed_io: continue # only task clustering

                    # check if any of the other downstream tasks became ready, iof so add them to the list
                    if len(mutable_downstream_tasks_ready) != 0: continue # execute the next task

                    # there may be tasks that became READY. If at least one became ready, continue
                    for _dtask in task.downstream_nodes:
                        task_in_completed_list = _dtask.id.get_full_id() not in tasks_completed
                        task_in_ready_list = any([_dtask.id.get_full_id() == dtready.id.get_full_id() for dtready in downstream_tasks_ready])
                        # Check if a task that wasn't ready nor completed became ready while executing this task
                        if not task_in_completed_list and not task_in_ready_list:
                            dependencies_met = await _worker.metadata_storage.storage.get(f"{DEPENDENCY_COUNTER_PREFIX}{_dtask.id.get_full_id_in_dag(subdag)}")
                            dependencies_met = 0 if dependencies_met is None else int(dependencies_met)
                            if dependencies_met == len(_dtask.upstream_nodes):
                                logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) Delayed IO | Task {_dtask.id.get_full_id()} became ready while executing other task ({task.id.get_full_id()})")
                                mutable_downstream_tasks_ready.append(_dtask)
                            break # found 1 ready task that's enough for now, execute it
            
                # {not is_fan_in_upstream} because if it is, we already uploaded the output
                if not is_fan_in_upstream and len(tasks_completed) != len(task.downstream_nodes):
                    serialized_task_result = cloudpickle.dumps(task.cached_result.result)
                    output_upload_timer = Timer()
                    await _worker.intermediate_storage.set(task.id.get_full_id_in_dag(subdag), serialized_task_result)
                    task.metrics.output_metrics.tp_time_ms = output_upload_timer.stop()
                    task.upload_complete.set()
                    _worker.log(task.id.get_full_id(), f"Big fan-out task had to upload output")
                    # Update DCs because output is already available
                    updating_dependency_counters_timer = Timer()
                    # update DCs of ALL my downstream
                    for downstream_task in task.downstream_nodes:
                        deps = await _worker.metadata_storage.storage.atomic_increment_and_get(f"{DEPENDENCY_COUNTER_PREFIX}{downstream_task.id.get_full_id_in_dag(subdag)}")
                        logger.info(f"[WUKONG_DBG] DIO W({_worker.debug_worker_id}) Updated DC for {downstream_task.id.get_full_id()} ({deps}/{len(downstream_task.upstream_nodes)})")
                    task.metrics.update_dependency_counters_time_ms = (task.metrics.update_dependency_counters_time_ms or 0) + updating_dependency_counters_timer.stop()

        # Normal fan-out handling logic     
        task_for_me_to_execute: DAGTaskNode | None = None
        for dtask_ready in downstream_tasks_ready:
            if dtask_ready.id.get_full_id() in tasks_completed: continue
            if not task_for_me_to_execute:
                logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) I will execute: {dtask_ready.id.get_full_id()}")
                # 1 task for me to execute
                task_for_me_to_execute = dtask_ready
            else:
                logger.info(f"[WUKONG_DBG] W({_worker.debug_worker_id}) Delegate: {dtask_ready.id.get_full_id()}")
                # 1 worker for each of the N READY task
                await _worker.delegate([subdag.create_subdag(dtask_ready)], fulldag, called_by_worker=True)
        
        if not task_for_me_to_execute: return []
        else: return [task_for_me_to_execute]

        