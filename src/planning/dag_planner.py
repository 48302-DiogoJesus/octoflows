
from abc import ABC, abstractmethod
from dataclasses import dataclass
import pickle
from typing import Literal, TypeAlias

from src.planning.metadata_access.metadata_access import MetadataAccess
from src.planning.sla import SLA, Percentile
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class DAGPlanner(ABC):
    @staticmethod
    @abstractmethod
    def plan(dag, metadata_access: MetadataAccess, sorted_available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA): 
        """
        dag: dag.DAG
        metadata_access: MetadataAccess

        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        pass

class DummyDAGPlanner(DAGPlanner):
    @staticmethod
    def plan(dag, metadata_access: MetadataAccess, sorted_available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA):
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
        best_resource_config = sorted_available_worker_resource_configurations[-1]
        for node_id, node in _dag._all_nodes.items():
            node.add_annotation(best_resource_config)
        return

# For A.I. prompting
# class DAGTaskNode:
#     def __init__(self, func: Callable[..., R], args: tuple, kwargs: dict, dynamic_fan_out_representative_id: DAGTaskNodeId | None = None, fan_out_idx: int = -1, fan_out_size: int = -1):
#         self.id: DAGTaskNodeId = DAGTaskNodeId(func.__name__)
#         self.func_name = func.__name__
#         self.func_code = func
#         self.func_args = args
#         self.func_kwargs = kwargs
#         self.downstream_nodes: list[DAGTaskNode] = []
#         self.upstream_nodes: list[DAGTaskNode] = []
# class DAG:
#     _all_nodes: dict[str, dag_task_node.DAGTaskNode]
#     root_nodes: list[dag_task_node.DAGTaskNode]
#     sink_node: dag_task_node.DAGTaskNode
#     master_dag_structure_hash: str
#     master_dag_id: str
#     def get_node_by_id(self, node_id: dag_task_node.DAGTaskNodeId) -> dag_task_node.DAGTaskNode: 
#         return self._all_nodes[node_id.get_full_id()]
# class MetadataAccess:
#     def predict_output_size(self, function_name: str, input_size: int , sla: SLA) -> float | None: pass
#     def predict_data_transfer_time(self, type: Literal['upload', 'download'], data_size_bytes: int, resource_config: TaskWorkerResourceConfiguration, sla: SLA) -> float | None: pass
#     def predict_execution_time(self, function_name: str, input_size: int,resource_config: TaskWorkerResourceConfiguration, sla: SLA) -> float | None: pass

class SimpleDAGPlanner(DAGPlanner):
    @staticmethod
    def plan(dag, metadata_access: MetadataAccess, sorted_available_worker_resource_configurations: list[TaskWorkerResourceConfiguration], sla: SLA):
        """
        dag: dag.DAG

        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
        best_resource_config = sorted_available_worker_resource_configurations[-1]
        logger.info("Starting DAG Planning Algorithm")
        
        algorithm_start_time = Timer()

        # Calculate critical path by analyzing execution times for each path
        # First, calculate execution times for each node with best resources
        topo_sorted_nodes = SimpleDAGPlanner._topological_sort(dag)
        # Initial planning with Best Resources for all nodes
        nodes_info = SimpleDAGPlanner._calculate_node_timings(topo_sorted_nodes, metadata_access, best_resource_config, sla)
        critical_path_nodes, critical_path_time = SimpleDAGPlanner._find_critical_path(dag, nodes_info)
        critical_path_node_ids = { node.id.get_full_id() for node in critical_path_nodes }
        
        logger.info(f"Initial critical path identified with {len(critical_path_nodes)} nodes")
        logger.info(f"Initial critical path completion time: {critical_path_time:.3f} ms")
        
        # Downgrade resources for nodes NOT on the critical path
        # Start with all nodes using best resources
        node_to_resource_config = { node.id.get_full_id(): best_resource_config for node in topo_sorted_nodes }
        
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
        
        # Recalculate final timings with optimized resource configurations
        final_nodes_info = SimpleDAGPlanner._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, node_to_resource_config, sla)
        final_critical_path_nodes, final_critical_path_time = SimpleDAGPlanner._find_critical_path(dag, final_nodes_info)
        
        # Annotate nodes with resource configs
        for node in topo_sorted_nodes:
            node['node_ref'].add_annotation(node_to_resource_config[node.id.get_full_id()])
        
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

    @staticmethod
    def _topological_sort(dag):
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
    def _calculate_input_size(node, nodes_info):
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
                total_input_size += nodes_info[pred_id]['output_size']
        
        return total_input_size
    
    @staticmethod
    def _calculate_node_timings(topo_sorted_nodes, metadata_access, resource_config, sla):
        """
        Calculate timing information for all nodes using the same resource configuration
        """
        nodes_info = {}
        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            
            input_size = SimpleDAGPlanner._calculate_input_size(node, nodes_info)
            download_time = metadata_access.predict_data_transfer_time('download', input_size, resource_config, sla) or 0.0
            exec_time = metadata_access.predict_execution_time(node.func_name, input_size, resource_config, sla) or 0.0
            output_size = metadata_access.predict_output_size(node.func_name, input_size, sla) or 0.0
            upload_time = metadata_access.predict_data_transfer_time('upload', output_size, resource_config, sla) or 0.0
            
            nodes_info[node_id] = {
                'node_ref': node,
                'input_size': input_size,
                'output_size': output_size,
                'upload_time': upload_time,
                'exec_time': exec_time,
                'download_time': download_time,
                'total_time': download_time + exec_time + upload_time,
                'earliest_start': 0, # Will be updated below
                'path_completion_time': 0 # Will be updated below
            }
        
        SimpleDAGPlanner._calculate_path_times(topo_sorted_nodes, nodes_info)
        
        return nodes_info
    
    @staticmethod
    def _calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, node_to_resource_config, sla):
        """
        Calculate timing information for all nodes using custom resource configurations
        """
        nodes_info = {}
        
        # First pass: calculate basic timing information for each node
        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            resource_config = node_to_resource_config[node_id]
            input_size = SimpleDAGPlanner._calculate_input_size(node, nodes_info)
            download_time = metadata_access.predict_data_transfer_time('download', input_size, resource_config, sla) or 0.0
            exec_time = metadata_access.predict_execution_time( node.func_name, input_size, resource_config, sla) or 0.0
            output_size = metadata_access.predict_output_size(node.func_name, input_size, sla) or 0.0
            upload_time = metadata_access.predict_data_transfer_time('upload', output_size, resource_config, sla) or 0.0
            
            nodes_info[node_id] = {
                'node_ref': node,
                'input_size': input_size,
                'output_size': output_size,
                'upload_time': upload_time,
                'exec_time': exec_time,
                'download_time': download_time,
                'total_time': download_time + exec_time + upload_time,
                'earliest_start': 0,  # Will be updated below
                'path_completion_time': 0  # Will be updated below
            }
        
        SimpleDAGPlanner._calculate_path_times(topo_sorted_nodes, nodes_info)
        
        return nodes_info
    
    @staticmethod
    def _calculate_path_times(topo_sorted_nodes, nodes_info):
        """
        Calculate earliest start time and path completion time for each node
        """
        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            
            # Get the earliest time this node can start (max completion time of all predecessors)
            max_predecessor_completion = 0
            for pred_node in node.upstream_nodes:
                pred_id = pred_node.id.get_full_id()
                pred_completion_time = nodes_info[pred_id]['path_completion_time']
                max_predecessor_completion = max(max_predecessor_completion, pred_completion_time)
            
            # Set earliest start time
            nodes_info[node_id]['earliest_start'] = max_predecessor_completion
            
            # Calculate path completion time (earliest start + total time for this node)
            nodes_info[node_id]['path_completion_time'] = (
                max_predecessor_completion + nodes_info[node_id]['total_time']
            )
    
    @staticmethod
    def _find_critical_path(dag, nodes_info):
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
                pred_completion = nodes_info[pred_id]['path_completion_time']
                
                if pred_completion > max_completion_time:
                    max_completion_time = pred_completion
                    critical_predecessor = pred
            
            # Move to predecessor on critical path
            current_node = critical_predecessor
        
        critical_path_time = nodes_info[dag.sink_node.id.get_full_id()]['path_completion_time']
        return critical_path, critical_path_time