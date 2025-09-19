from dataclasses import dataclass
import uuid

from src.planning.annotations.preload import PreLoadOptimization
from src.planning.annotations.prewarm import PreWarmOptimization
from src.planning.annotations.taskdup import TaskDupOptimization
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.planning.abstract_dag_planner import AbstractDAGPlanner
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.utils.logger import create_logger

logger = create_logger(__name__, prefix="PLANNING")

class NonUniformPlanner(AbstractDAGPlanner):
    @dataclass
    class Config(AbstractDAGPlanner.BaseConfig):
        def create_instance(self) -> "NonUniformPlanner": 
            super().create_instance()
            return NonUniformPlanner(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def get_description(self) -> str: 
        return \
            """
            Uses non-uniform workers. It would first find the critical path by predicting
            times of all workflow tasks running on the best worker configuration. Then, it would downgrade resources
            on the other paths as much as possible without introducing a new critical path. After attributing tasks to workers
            (at plan-time, not run-time), it simulates using preload optimization to reduce makespan by masking dependency download time.

            - Assign best resources to all tasks, find CP
            - Simulate downgrading resources on non-critical path tasks (w/o introducing a new CP)
            - Simulate using optimizations (preload)
            - Uses task dupping optimization
            """

    def internal_plan(self, dag, predictions_provider: PredictionsProvider):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        assert isinstance(self.config, NonUniformPlanner.Config)

        topo_sorted_nodes = self._topological_sort(dag)

        if len(self.config.worker_resource_configurations) == 1:
            # If only one resource config is available, use it for all nodes
            for node in topo_sorted_nodes:
                unique_resources = self.config.worker_resource_configurations[0].clone()
                node.worker_config = unique_resources
                if len(node.upstream_nodes) == 0:
                    unique_resources.worker_id = uuid.uuid4().hex
                else:
                    unique_resources.worker_id = node.upstream_nodes[0].worker_config.worker_id
            # self._store_plan_image(dag)
            # self._store_plan_as_json(dag)
            return

        middle_resource_config = self.config.worker_resource_configurations[len(self.config.worker_resource_configurations) // 2]
        
        if not predictions_provider.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving intermediate resources ({middle_resource_config}) to all nodes")
            for node in topo_sorted_nodes: 
                unique_resources = middle_resource_config.clone()
                node.worker_config = unique_resources
                unique_resources.worker_id = None # note: ALL workers will be "flexible"
            # self._store_plan_image(dag)
            # self._store_plan_as_json(dag)
            return
        
        best_resource_config = self.config.worker_resource_configurations[0]
        
        # Step 1: Assign worker ids and best resources to all nodes
        # logger.info("=== Step 1: Initial assignment with best resources ===")
        self._basic_worker_id_assignment(dag, best_resource_config, topo_sorted_nodes)

        # Calculate initial critical path with best resources
        nodes_info = self._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, self.config.sla)
        critical_path_nodes, critical_path_time = self._find_critical_path(dag, nodes_info)
        critical_path_node_ids = {node.id.get_full_id() for node in critical_path_nodes}
        
        # logger.info(f"Initial Critical Path | Nodes: {len(critical_path_nodes)} | Node IDs: {[node.id.get_full_id() for node in critical_path_nodes]} | Time: {critical_path_time} ms")

        # Step 2: Downgrade resources on non-critical paths without introducing new critical path
        # logger.info("=== Step 2: Downgrading resources on non-critical paths ===")
        worker_ids_outside_critical_path: set[str] = set()
        for node in topo_sorted_nodes:
            node_worker_id = node.worker_config.worker_id
            if not node_worker_id: continue
            if node.id.get_full_id() not in critical_path_node_ids and all(node_worker_id != cpnode.worker_config.worker_id for cpnode in critical_path_nodes):
                worker_ids_outside_critical_path.add(node_worker_id)

        nodes_outside_critical_path = [node for node in topo_sorted_nodes if node.id.get_full_id() not in critical_path_node_ids]
        successful_worker_resources_downgrades = 0

        # For each worker outside critical path, simulate downgrading resources without introducing a new critical path
        for worker_id in worker_ids_outside_critical_path:
            last_successful_configs: dict[str, TaskWorkerResourceConfiguration] = {}
            
            # Store initial configurations as the first "successful" state
            for node in nodes_outside_critical_path:
                if node.worker_config.worker_id == worker_id:
                    last_successful_configs[node.id.get_full_id()] = node.worker_config
            
            for simul_resource_config in self.config.worker_resource_configurations[1:]:
                new_res_config = simul_resource_config.clone()
                new_res_config.worker_id = worker_id # use same worker_id, just change the resource config
                
                for node in nodes_outside_critical_path:
                    if node.worker_config.worker_id != worker_id: continue
                    node.worker_config = new_res_config
                
                # Recalculate timings with this configuration
                nodes_info = self._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, self.config.sla)
                _, new_critical_path_time = self._find_critical_path(dag, nodes_info)
                
                # If downgrading doesn't change the critical path, allow it, else: reverse it
                if new_critical_path_time == critical_path_time:
                    successful_worker_resources_downgrades += 1
                    # Update last successful configs to current state
                    for node in nodes_outside_critical_path:
                        if node.worker_config.worker_id == worker_id:
                            last_successful_configs[node.id.get_full_id()] = node.worker_config
                else:
                    # This downgrade hurts performance, REVERT to last successful state and stop trying lower configs
                    for node in nodes_outside_critical_path:
                        if node.worker_config.worker_id != worker_id: continue
                        node.worker_config = last_successful_configs[node.id.get_full_id()]
                    break # try downgrading the next worker

        logger.info(f"Successfully downgraded {successful_worker_resources_downgrades} out of {len(nodes_outside_critical_path)} non-critical path nodes")

       # OPTIMIZATIONS
        for optimization in self.config.optimizations:
            optimization.planning_assignment_logic(self, dag, predictions_provider, nodes_info, topo_sorted_nodes)

        total_duppable_tasks, total_preload_optimizations, total_prewarm_optimizations = 0, 0, 0
        for node_info in nodes_info.values():
            if node_info.node_ref.try_get_annotation(TaskDupOptimization): 
                total_duppable_tasks += 1
            if node_info.node_ref.try_get_annotation(PreLoadOptimization): 
                total_preload_optimizations += 1
            if node_info.node_ref.try_get_annotation(PreWarmOptimization): 
                total_prewarm_optimizations += 1

        # Final statistics and logging
        nodes_info = self._calculate_workflow_timings(topo_sorted_nodes, predictions_provider, self.config.sla)
        final_critical_path_nodes, final_critical_path_time = self._find_critical_path(dag, nodes_info)
        final_critical_path_node_ids = {node.id.get_full_id() for node in final_critical_path_nodes}
        
        # Calculate resource distribution
        resource_distribution = {}
        unique_worker_ids: dict[str, int] = {}
        nodes_with_preload = 0
        
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.worker_config
            config_key = f"CPU:{resource_config.cpus},Memory:{resource_config.memory_mb}MB"
            if config_key not in resource_distribution:
                resource_distribution[config_key] = 0
            resource_distribution[config_key] += 1
            
            if resource_config.worker_id is None: continue
            if resource_config.worker_id not in unique_worker_ids:
                unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1
            
            if node.try_get_annotation(PreLoadOptimization):
                nodes_with_preload += 1

        prediction_samples_used = AbstractDAGPlanner.PlanPredictionSampleCounts(
            previous_instances=predictions_provider.nr_of_previous_instances,
            # note: data from ALL workflow instances
            for_download_speed=len(predictions_provider.cached_download_speeds),
            for_upload_speed=len(predictions_provider.cached_upload_speeds),
            # note: only related to instances from same workflow type
            for_execution_time=sum(map(len, predictions_provider.cached_execution_time_per_byte.values())),
            for_output_size=sum(map(len, predictions_provider.cached_deserialized_io_ratios.values()))
        )

        logger.info(f"=== FINAL RESULTS ===")
        logger.info(f"Critical Path | Nr. Nodes: {len(final_critical_path_nodes)}, Predicted Completion Time: {final_critical_path_time / 1000:.2f}s")
        logger.info(f"Number of PreLoad optimizations: {total_preload_optimizations} | Number of PreWarm optimizations: {total_prewarm_optimizations} | Number of duppable tasks: {total_duppable_tasks}/{len(nodes_info)}")
        logger.info(f"Number of unique workers: {len(unique_worker_ids)}")
        logger.info(f"Successfully downgraded resources for {successful_worker_resources_downgrades}/{len(_dag._all_nodes)} nodes")
        logger.info(f"Worker Resource Configuration Distribution: {resource_distribution}")
        # logger.info(f"Prediction samples used: {prediction_samples_used}")

        return AbstractDAGPlanner.PlanOutput(
            self.planner_name, 
            self.config.sla,
            nodes_info, 
            final_critical_path_node_ids, 
            prediction_samples_used
        )
