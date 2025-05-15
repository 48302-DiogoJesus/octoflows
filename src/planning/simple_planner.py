from dataclasses import dataclass
import uuid

from src.planning.dag_planner import DAGPlanner
from src.planning.dag_planner import DAGPlanner
from src.planning.metadata_access.metadata_access import MetadataAccess
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.workers.worker_execution_logic import WorkerExecutionLogic

logger = create_logger(__name__, prefix="PLANNING")

class SimpleDAGPlanner(DAGPlanner, WorkerExecutionLogic):
    @dataclass
    class Config(DAGPlanner.Config):
        available_worker_resource_configurations: list[DAGPlanner.TaskWorkerResourceConfiguration]

        def __post_init__(self):
            """
            Sort the available_resource_configurations by memory_mb
            Greatest {memory_mb} first
            """
            self.available_worker_resource_configurations.sort(key=lambda x: x.memory_mb, reverse=True)

        def create_instance(self) -> "SimpleDAGPlanner":
            return SimpleDAGPlanner(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def plan(self, dag, metadata_access: MetadataAccess):
        """
        dag: dag.DAG

        This planning algorithm:
        - Assigns the best resource config to each node in the DAG
        - Finds the critical path
        - Simulates downgrading resource configs of tasks outside the critical path without affecting the critical path significantly

        """
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        topo_sorted_nodes = self._topological_sort(dag)
        middle_resource_config = self.config.available_worker_resource_configurations[len(self.config.available_worker_resource_configurations) // 2]

        if len(self.config.available_worker_resource_configurations) == 1:
            # If only one resource config is available, use it for all nodes
            # Assign worker resources and ids
            for node in topo_sorted_nodes:
                unique_resources = self.config.available_worker_resource_configurations[0].clone()
                node.add_annotation(unique_resources)
                if len(node.upstream_nodes) == 0:
                    # Give each root node a unique worker id
                    unique_resources.worker_id = uuid.uuid4().hex
                else:
                    # Use same worker id as its first upstream node
                    unique_resources.worker_id = node.upstream_nodes[0].get_annotation(DAGPlanner.TaskWorkerResourceConfiguration).worker_id
            self._visualize_plan(dag)
            return

        if not metadata_access.has_required_predictions():
            logger.warning(f"No Metadata recorded for previous runs of the same DAG structure. Giving intermediate resources ({middle_resource_config}) to all nodes")
            # No Metadata recorded for previous runs of the same DAG structure => give intermediate resources to all nodes
            # Assign worker resources and ids
            for node in topo_sorted_nodes: 
                unique_resources = middle_resource_config.clone()
                node.add_annotation(unique_resources)
                if len(node.upstream_nodes) == 0:
                    # Give each root node a unique worker id
                    unique_resources.worker_id = uuid.uuid4().hex
                else:
                    # Use same worker id as its first upstream node
                    unique_resources.worker_id = node.upstream_nodes[0].get_annotation(DAGPlanner.TaskWorkerResourceConfiguration).worker_id
            self._visualize_plan(dag)
            return
        
        logger.info(f"Starting DAG Planning Algorithm")
        best_resource_config = self.config.available_worker_resource_configurations[0]
        
        algorithm_start_time = Timer()

        # Calculate critical path by analyzing execution times for each path
        # First, calculate execution times for each node with best resources
        
        # Give best resources to all nodes and reuse worker ids randomly
        for node in topo_sorted_nodes:
            resource_config = best_resource_config.clone()
            node.add_annotation(resource_config)
            if len(node.upstream_nodes) == 0:
                # Give each root node a unique worker id
                resource_config.worker_id = uuid.uuid4().hex
            else:
                # Use same worker id as its first upstream node
                resource_config.worker_id = node.upstream_nodes[0].get_annotation(DAGPlanner.TaskWorkerResourceConfiguration).worker_id

        # Initial planning with Best Resources for all nodes
        nodes_info = self._calculate_node_timings_with_common_resources(topo_sorted_nodes, metadata_access, best_resource_config, self.config.sla)
        critical_path_nodes, critical_path_time = self._find_critical_path(dag, nodes_info)
        critical_path_node_ids = { node.id.get_full_id() for node in critical_path_nodes }
        
        logger.info(f"CRITICAL PATH | Nodes: {len(critical_path_nodes)} | Predicted Completion Time: {critical_path_time} ms")

        nodes_outside_critical_path = [node for node in topo_sorted_nodes if node.id.get_full_id() not in critical_path_node_ids]
        lower_resources_simulation_timer = Timer()
        successful_downgrades = 0
        # Simulate downgrading resources for nodes NOT on the critical path without creating a new critical path
        for node in nodes_outside_critical_path:
            node_id = node.id.get_full_id()
            node_downgrade_successful = False
            # Try each resource config from highest to lowest on the same worker or new workers
            for resource_config in self.config.available_worker_resource_configurations:
                original_config = node.get_annotation(DAGPlanner.TaskWorkerResourceConfiguration)
                # if resource_config.cpus == original_config.cpus and resource_config.memory_mb == original_config.memory_mb: continue
                # Temporarily assign this resource config
                simulation_resource_config = resource_config.clone()
                simulation_resource_config.worker_id = ""

                for unode in node.upstream_nodes:
                    unode_resources = unode.get_annotation(DAGPlanner.TaskWorkerResourceConfiguration)
                    if unode_resources.cpus == simulation_resource_config.cpus and unode_resources.memory_mb == simulation_resource_config.memory_mb: 
                        simulation_resource_config.worker_id = unode.get_annotation(DAGPlanner.TaskWorkerResourceConfiguration).worker_id

                # couldn't find an upstream node with the same resources to reuse the worker, simulate a new worker with these resources
                if simulation_resource_config.worker_id == "": simulation_resource_config.worker_id = uuid.uuid4().hex
                node.add_annotation(simulation_resource_config) # replace the original one and simulate
                        
                # Recalculate timings with this resource configuration
                temp_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
                _, new_critical_path_time = self._find_critical_path(dag, temp_nodes_info)

                if new_critical_path_time != critical_path_time:
                    node.add_annotation(original_config) # REVERT: This config changes the critical path
                else:
                    # print(f"Node: {node_id[-6:]} | Downgraded Resources: {original_config.memory_mb} => {node_to_resource_config[node_id].memory_mb}")
                    if original_config.memory_mb != simulation_resource_config.memory_mb and original_config.cpus != simulation_resource_config.cpus:
                        node_downgrade_successful = True

            if node_downgrade_successful:
                successful_downgrades += 1

        logger.info(f"Downgraded resources for {successful_downgrades} nodes out of {len(nodes_outside_critical_path)} nodes outside the critical path in {lower_resources_simulation_timer.stop():.3f} ms")
        
        # Log Results
        resource_distribution = {}
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.get_annotation(DAGPlanner.TaskWorkerResourceConfiguration)
            config_key = f"CPU:{resource_config.cpus},Memory:{resource_config.memory_mb}MB"
            if config_key not in resource_distribution: resource_distribution[config_key] = 0
            resource_distribution[config_key] += 1
            
        unique_worker_ids = {}
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.get_annotation(DAGPlanner.TaskWorkerResourceConfiguration)
            if resource_config.worker_id not in unique_worker_ids: unique_worker_ids[resource_config.worker_id] = 0
            unique_worker_ids[resource_config.worker_id] += 1

        logger.info(f"CRITICAL PATH | Nodes: {len(critical_path_nodes)} | Predicted Completion Time: {critical_path_time} ms")
        logger.info(f"Resource distribution after optimization: {resource_distribution}")
        logger.info(f"Number of unique workers: {len(unique_worker_ids)}")
        logger.info(f"Completed in {algorithm_start_time.stop():.3f} ms")

        # DEBUG: Plan Visualization
        updated_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
        self._visualize_plan(dag, updated_nodes_info, critical_path_node_ids)
        self.validate_plan(_dag.root_nodes)
        # !!! FOR QUICK TESTING ONLY. REMOVE LATER !!!
        # exit()
