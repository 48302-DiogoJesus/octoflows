from dataclasses import dataclass
from types import CoroutineType
from typing import Any
import uuid

from src.dag.dag import FullDAG, SubDAG
from src.planning.annotations.preload import PreLoadOptimization
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.annotations.taskdup import TaskDupOptimization, DUPPABLE_TASK_MAX_EXEC_TIME_MS, DUPPABLE_TASK_MAX_INPUT_SIZE
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.storage.storage import Storage
from src.utils.logger import create_logger

logger = create_logger(__name__, prefix="PLANNING")

class UniformPlanner(AbstractDAGPlanner):
    @dataclass
    class Config(AbstractDAGPlanner.BaseConfig):
        def create_instance(self) -> "UniformPlanner": 
            super().create_instance()
            return UniformPlanner(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def get_description(self) -> str: 
        return \
            """
            The first algorithm would target uniform Lambda
            workers. It would use the MetadataAccess API to predict the longest workflow path (critical path). Then it would
            simulate using the pre-load optimization on this path. If optimizing the critical path made it shorter than the
            second longest path, the algorithm would repeat the process for this new critical path. This would be repeated
            until we can't optimize the critical path any further. TaskDup optimization is also used.
            """

    def internal_plan(self, dag, predictions_provider: PredictionsProvider):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
        assert isinstance(self.config, UniformPlanner.Config)

        topo_sorted_nodes = self._topological_sort(dag)

        worker_resources = self.config.worker_resource_configurations[0]

        if not predictions_provider.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving uniform resources ({worker_resources}) to all nodes")
            # Assign worker resources and ids
            for node in topo_sorted_nodes: 
                unique_resources = worker_resources.clone()
                unique_resources.worker_id = None # note: ALL workers will be "flexible"
                node.worker_config = unique_resources
            # self._store_plan_image(dag)
            # self._store_plan_as_json(dag)
            return
        
        # Step 1: Assign uniform resources to all nodes
        # logger.info("=== Step 1: Initial assignment with best resources ===")
        self._basic_worker_id_assignment(dag, worker_resources, topo_sorted_nodes)

        nodes_info = self._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, self.config.sla)

        # OPTIMIZATIONS
        PreLoadOptimization.planning_assignment_logic(self, dag, predictions_provider, nodes_info, topo_sorted_nodes)
        TaskDupOptimization.planning_assignment_logic(self, dag, predictions_provider, nodes_info, topo_sorted_nodes)

        total_duppable_tasks, total_preload_optimizations = 0, 0
        for node_info in nodes_info.values():
            if node_info.node_ref.try_get_annotation(TaskDupOptimization): 
                total_duppable_tasks += 1
            if node_info.node_ref.try_get_annotation(PreLoadOptimization): 
                total_preload_optimizations += 1

        # Final statistics
        nodes_info = self._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, nodes_info)
        final_critical_path_node_ids = { node.id.get_full_id() for node in final_critical_path_nodes }
            
        unique_worker_ids: dict[str, int] = {}
        for my_node_id, node in _dag._all_nodes.items():
            resource_config = node.worker_config
            if resource_config.worker_id is None: continue
            if resource_config.worker_id not in unique_worker_ids: unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1

        prediction_samples_used = AbstractDAGPlanner.PlanPredictionSampleCounts(
            # note: data from ALL workflow instances
            previous_instances=predictions_provider.nr_of_previous_instances,
            for_download_speed=len(predictions_provider.cached_download_speeds),
            for_upload_speed=len(predictions_provider.cached_upload_speeds),
            # note: only related to instances from same workflow type
            for_execution_time=sum(map(len, predictions_provider.cached_execution_time_per_byte.values())),
            for_output_size=sum(map(len, predictions_provider.cached_deserialized_io_ratios.values()))
        )

        logger.info(f"=== FINAL RESULTS ===")
        logger.info(f"Critical Path Nodes Count: {len(final_critical_path_nodes)} | Predicted Completion Time: {final_critical_path_time / 1000:.2f}s | Unique workers: {len(unique_worker_ids)}")
        logger.info(f"Number of PreLoad optimizations: {total_preload_optimizations} | Number of duppable tasks: {total_duppable_tasks}/{len(nodes_info)}")
        logger.info(f"Worker Resource Configuration (same for all tasks): (cpus={worker_resources.cpus}, memory={worker_resources.memory_mb})")
        # logger.info(f"Prediction samples used: {prediction_samples_used}")

        return AbstractDAGPlanner.PlanOutput(
            self.planner_name, 
            self.config.sla,
            nodes_info,
            final_critical_path_node_ids,
            prediction_samples_used
        )

    @staticmethod
    async def wel_on_worker_ready(intermediate_storage: Storage, dag: FullDAG, this_worker_id: str | None):
        from src.planning.annotations.preload import PreLoadOptimization
        await PreLoadOptimization.wel_on_worker_ready(intermediate_storage, dag, this_worker_id)

    @staticmethod
    async def wel_before_task_handling(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task, is_dupping: bool = False):
        from src.planning.annotations.taskdup import TaskDupOptimization
        await TaskDupOptimization.wel_before_task_handling(this_worker, metadata_storage, subdag, current_task, is_dupping)

    @staticmethod
    async def wel_before_task_execution(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task, is_dupping: bool):
        from src.planning.annotations.taskdup import TaskDupOptimization
        await TaskDupOptimization.wel_before_task_execution(this_worker, metadata_storage, subdag, current_task, is_dupping)

    @staticmethod
    async def wel_override_handle_inputs(intermediate_storage: Storage, task, subdag: SubDAG, upstream_tasks_without_cached_results: list, worker_resource_config, task_dependencies: dict[str, Any]) -> tuple[list, list[str], CoroutineType | None]:
        """
        returns (
            tasks_to_fetch: list[task] (on default implementation, fetch ALL tasks that don't have cached results),
            wait_until_coroutine: list[TaskInputMetrics] (so that the caller can fetch the tasks in parallel)
        )
        """
        res = await PreLoadOptimization.wel_override_handle_inputs(intermediate_storage, task, subdag, upstream_tasks_without_cached_results, worker_resource_config, task_dependencies)
        return res

    @staticmethod
    async def wel_override_should_upload_output(current_task, subdag: SubDAG, this_worker, metadata_storage: Storage, is_dupping: bool) -> bool:
        from src.planning.annotations.taskdup import TaskDupOptimization
        res = await TaskDupOptimization.wel_override_should_upload_output(current_task, subdag, this_worker, metadata_storage, is_dupping)
        return res

    @staticmethod
    async def wel_override_handle_downstream(fulldag: FullDAG, current_task, this_worker, downstream_tasks_ready, subdag: SubDAG, is_dupping: bool) -> list:
        from src.planning.annotations.taskdup import TaskDupOptimization
        res = await TaskDupOptimization.wel_override_handle_downstream(fulldag, current_task, this_worker, downstream_tasks_ready, subdag, is_dupping)
        return res