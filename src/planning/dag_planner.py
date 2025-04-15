from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.dag_task_node import DAGTaskNode
from src.planning.metadata_access.metadata_access import MetadataAccess
from src.planning.sla import SLA
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class DAGPlanner(ABC):
    @dataclass
    class PlanningTaskInfo:
        node_ref: DAGTaskNode
        input_size: int
        output_size: int
        download_time: float
        exec_time: float
        upload_time: float
        total_time: float
        earliest_start: float
        path_completion_time: float 

    @staticmethod
    @abstractmethod
    def plan(dag, metadata_access: MetadataAccess, sorted_available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA): 
        """
        dag: dag.DAG
        metadata_access: MetadataAccess

        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        pass

    
    @staticmethod
    def _topological_sort(dag) -> list[DAGTaskNode]:
        """
        Performs topological sort on DAG nodes
        """
        visited = set()
        topo_order = []
        
        def dfs(node):
            if node.id.get_full_id() in visited:
                return
            visited.add(node.id.get_full_id())
            
            for child in node.downstream_nodes:
                dfs(child)
            
            topo_order.insert(0, node)
        
        # Start DFS from all root nodes
        for root in dag.root_nodes:
            dfs(root)
        
        return topo_order
    
    @staticmethod
    def _calculate_input_size(node, nodes_info: dict[str, PlanningTaskInfo]):
        """
        Calculate the input size for a node based on its predecessors
        """
        if not node.upstream_nodes:
            # For root nodes, use the size from function args (estimate)
            return sum(len(str(arg)) for arg in node.func_args) + sum(len(str(k)) + len(str(v)) for k, v in node.func_kwargs.items())
        
        # For non-root nodes, sum the output sizes of all predecessors
        total_input_size = 0
        for pred in node.upstream_nodes:
            pred_id = pred.id.get_full_id()
            if pred_id in nodes_info:
                total_input_size += nodes_info[pred_id].output_size
        
        return total_input_size
    
    @staticmethod
    def _calculate_node_timings_with_common_resources(topo_sorted_nodes: list[DAGTaskNode], metadata_access: MetadataAccess, resource_config: TaskWorkerResourceConfiguration, sla: SLA):
        """
        Calculate timing information for all nodes using the same resource configuration
        """
        nodes_info: dict[str, DAGPlanner.PlanningTaskInfo] = {}
        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            input_size = SimpleDAGPlanner._calculate_input_size(node, nodes_info)
            download_time = metadata_access.predict_data_transfer_time('download', input_size, resource_config, sla) or 0.0
            exec_time = metadata_access.predict_execution_time(node.func_name, input_size, resource_config, sla) or 0.0
            output_size = metadata_access.predict_output_size(node.func_name, input_size, sla) or 0
            upload_time = metadata_access.predict_data_transfer_time('upload', output_size, resource_config, sla) or 0.0
            nodes_info[node_id] = DAGPlanner.PlanningTaskInfo(node, input_size, output_size, download_time, exec_time, upload_time, download_time + exec_time + upload_time, 0, 0)
        
        SimpleDAGPlanner._calculate_path_times(topo_sorted_nodes, nodes_info)
        
        return nodes_info
    
    @staticmethod
    def _calculate_node_timings_with_custom_resources(topo_sorted_nodes: list[DAGTaskNode], metadata_access: MetadataAccess, node_to_resource_config: dict[str,TaskWorkerResourceConfiguration], sla: SLA):
        """
        Calculate timing information for all nodes using custom resource configurations
        """
        nodes_info: dict[str, DAGPlanner.PlanningTaskInfo] = {}
        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            resource_config = node_to_resource_config[node_id]
            input_size = SimpleDAGPlanner._calculate_input_size(node, nodes_info)
            download_time = metadata_access.predict_data_transfer_time('download', input_size, resource_config, sla) or 0.0
            exec_time = metadata_access.predict_execution_time( node.func_name, input_size, resource_config, sla) or 0.0
            output_size = metadata_access.predict_output_size(node.func_name, input_size, sla) or 0
            upload_time = metadata_access.predict_data_transfer_time('upload', output_size, resource_config, sla) or 0.0
            nodes_info[node_id] = DAGPlanner.PlanningTaskInfo(node, input_size, output_size, download_time, exec_time, upload_time, download_time + exec_time + upload_time, 0, 0)
        
        SimpleDAGPlanner._calculate_path_times(topo_sorted_nodes, nodes_info)
        
        return nodes_info
    
    @staticmethod
    def _calculate_path_times(topo_sorted_nodes: list[DAGTaskNode], nodes_info: dict[str, PlanningTaskInfo]):
        """
        Calculate earliest possible start time and path completion time for each node
        """
        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            max_predecessor_completion = 0
            for pred_node in node.upstream_nodes:
                pred_id = pred_node.id.get_full_id()
                pred_completion_time = nodes_info[pred_id].path_completion_time
                max_predecessor_completion = max(max_predecessor_completion, pred_completion_time)
            
            nodes_info[node_id].earliest_start = max_predecessor_completion
            nodes_info[node_id].path_completion_time = max_predecessor_completion + nodes_info[node_id].total_time
    
    @staticmethod
    def _find_critical_path(dag, nodes_info: dict[str, PlanningTaskInfo]):
        """
        Find the critical path (longest path from start to finish)
        Returns the list of nodes on the critical path and the critical path time
        """
        critical_path = []
        current_node = dag.sink_node
        
        while current_node:
            # Add current node to critical path
            critical_path.insert(0, current_node)
            
            # Find predecessor with the latest completion time
            max_completion_time = -1
            critical_predecessor = None
            
            for pred in current_node.upstream_nodes:
                pred_id = pred.id.get_full_id()
                pred_completion = nodes_info[pred_id].path_completion_time
                
                if pred_completion > max_completion_time:
                    max_completion_time = pred_completion
                    critical_predecessor = pred
            
            # Move to predecessor on critical path
            current_node = critical_predecessor
        
        critical_path_time = nodes_info[dag.sink_node.id.get_full_id()].path_completion_time
        return critical_path, critical_path_time

class SimpleDAGPlanner(DAGPlanner):
    @staticmethod
    def plan(dag, metadata_access: MetadataAccess, sorted_available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA):
        """
        dag: dag.DAG

        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag

        if len(sorted_available_worker_resource_configurations) == 1:
            # If only one resource config is available, use it for all nodes
            for _, node in _dag._all_nodes.items():
                node.add_annotation(sorted_available_worker_resource_configurations[0])

        best_resource_config = sorted_available_worker_resource_configurations[-1]
        logger.info("Starting DAG Planning Algorithm")
        
        algorithm_start_time = Timer()

        # Calculate critical path by analyzing execution times for each path
        # First, calculate execution times for each node with best resources
        topo_sorted_nodes = SimpleDAGPlanner._topological_sort(dag)
        # Initial planning with Best Resources for all nodes
        nodes_info = SimpleDAGPlanner._calculate_node_timings_with_common_resources(topo_sorted_nodes, metadata_access, best_resource_config, sla)
        critical_path_nodes, critical_path_time = SimpleDAGPlanner._find_critical_path(dag, nodes_info)
        critical_path_node_ids = { node.id.get_full_id() for node in critical_path_nodes }
        
        logger.info(f"Initial critical path identified with {len(critical_path_nodes)} nodes")
        logger.info(f"Initial critical path completion time: {critical_path_time:.3f} ms")
        
        # Downgrade resources for nodes NOT on the critical path
        # Start with all nodes using best resources
        node_to_resource_config = { node.id.get_full_id(): best_resource_config for node in topo_sorted_nodes }
        
        lower_resources_simulation_timer = Timer()

        # For each NON-critical path node, try to use lower resources
        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            if node_id in critical_path_node_ids: continue
                
            # Try each resource config from lowest to highest
            for resource_config in sorted_available_worker_resource_configurations:
                if resource_config == node_to_resource_config[node_id]:
                    continue # Skip if it's the currently assigned config (no need to recalculate)
                    
                # Temporarily assign this resource config
                original_config = node_to_resource_config[node_id]
                node_to_resource_config[node_id] = resource_config
                
                # Recalculate timings with this resource configuration
                temp_nodes_info = SimpleDAGPlanner._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, node_to_resource_config, sla)
                new_critical_path_nodes, _ = SimpleDAGPlanner._find_critical_path(dag, temp_nodes_info)
                
                if new_critical_path_nodes != critical_path_nodes:
                    # This config changes the critical path, revert
                    node_to_resource_config[node_id] = original_config
        
        logger.info(f"Simulated downgrading for {len(topo_sorted_nodes) - len(critical_path_node_ids)} nodes in {lower_resources_simulation_timer.stop():.3f} ms")

        # Recalculate final timings with optimized resource configurations
        final_nodes_info = SimpleDAGPlanner._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, node_to_resource_config, sla)
        final_critical_path_nodes, final_critical_path_time = SimpleDAGPlanner._find_critical_path(dag, final_nodes_info)
        
        # Annotate nodes with resource configs
        for node in topo_sorted_nodes: node.add_annotation(node_to_resource_config[node.id.get_full_id()])
        
        # Log Results
        resource_distribution = {}
        for node_id, config in node_to_resource_config.items():
            config_key = f"CPU:{config.cpus},Memory:{config.memory_mb}MB"
            if config_key not in resource_distribution:
                resource_distribution[config_key] = 0
            resource_distribution[config_key] += 1
            
        logger.info(f"Final critical path identified with {len(final_critical_path_nodes)} nodes")
        logger.info(f"Final critical path completion time: {final_critical_path_time:.3f} ms")
        logger.info(f"Resource distribution after optimization: {resource_distribution}")
        logger.info(f"Planning completed in {algorithm_start_time.stop():.3f} ms")
        
        # !!! FOR QUICK TESTING ONLY. REMOVE LATER !!!
        exit()

class DummyDAGPlanner(DAGPlanner):
    @staticmethod
    def plan(dag, metadata_access: MetadataAccess, sorted_available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
        best_resource_config = sorted_available_worker_resource_configurations[-1]
        for _, node in _dag._all_nodes.items(): node.add_annotation(best_resource_config)