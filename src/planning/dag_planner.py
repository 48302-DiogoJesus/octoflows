from typing import Any
import uuid
import cloudpickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from graphviz import Digraph

from src import dag_task_node
from src.dag.dag import SubDAG
from src.dag_task_node import DAGTaskNode
from src.planning.metadata_access.metadata_access import MetadataAccess
from src.planning.sla import SLA
from src.storage.metrics.metrics_storage import BASELINE_MEMORY_MB, TaskInputMetrics
from src.storage.storage import Storage
from src.utils.logger import create_logger
from src.utils.timer import Timer
from src.utils.utils import calculate_data_structure_size
from src.worker_execution_logic import WorkerExecutionLogic
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

logger = create_logger(__name__)

class DAGPlanner(ABC):
    @dataclass
    class Config(ABC):
        sla: SLA

        @abstractmethod
        def create_instance(self) -> "DAGPlanner": 
            pass

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

    @abstractmethod
    def plan(self, dag, metadata_access: MetadataAccess):
        """
        dag: dag.DAG
        metadata_access: MetadataAccess
        
        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        pass

    def _topological_sort(self, dag) -> list[DAGTaskNode]:
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
    
    def _calculate_input_sizes_per_worker_id(self, node, nodes_info: dict[str, PlanningTaskInfo]) -> tuple[dict[str, int], int]:
        """
        Returns input sizes of upstream_nodes grouped by worker_id
        Returns total input size
        """

        grouped_input_sizes: dict[str, int] = {} # <worker_id, input_size>
        total_input_size = 0
        # For root nodes, use the size from function args (estimate)
        for func_arg in node.func_args:
            if isinstance(func_arg, dag_task_node.DAGTaskNodeId): 
                upstream_node_id = func_arg.get_full_id()
                unode_worker_id = nodes_info[upstream_node_id].node_ref.get_annotation(TaskWorkerResourceConfiguration).worker_id
                if upstream_node_id in nodes_info: 
                    if unode_worker_id not in grouped_input_sizes: grouped_input_sizes[unode_worker_id] = 0
                    output_size = nodes_info[upstream_node_id].output_size
                    grouped_input_sizes[unode_worker_id] += output_size
                    total_input_size += output_size
            else:
                total_input_size += calculate_data_structure_size(func_arg)
        for func_kwarg_val in node.func_kwargs.values():
            if isinstance(func_kwarg_val, dag_task_node.DAGTaskNodeId): 
                upstream_node_id = func_kwarg_val.get_full_id()
                unode_worker_id = nodes_info[upstream_node_id].node_ref.get_annotation(TaskWorkerResourceConfiguration).worker_id
                if upstream_node_id in nodes_info: 
                    if unode_worker_id not in grouped_input_sizes: grouped_input_sizes[unode_worker_id] = 0
                    output_size = nodes_info[upstream_node_id].output_size
                    grouped_input_sizes[unode_worker_id] += output_size
                    total_input_size += output_size
            else:
                total_input_size += calculate_data_structure_size(func_kwarg_val)

        return (grouped_input_sizes, total_input_size)
    
    def _calculate_node_timings_with_common_resources(self, topo_sorted_nodes: list[DAGTaskNode], metadata_access: MetadataAccess, resource_config: TaskWorkerResourceConfiguration, sla: SLA):
        """
        Calculate timing information for all nodes using the same resource configuration
        """
        nodes_info: dict[str, DAGPlanner.PlanningTaskInfo] = {}
        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            worker_id = node.get_annotation(TaskWorkerResourceConfiguration).worker_id
            (input_size_per_worker, total_input_size) = self._calculate_input_sizes_per_worker_id(node, nodes_info)
            downloadable_input_size = 0
            for upstream_worker_id, upstream_node_input_size in input_size_per_worker.items():
                if upstream_worker_id != worker_id: downloadable_input_size += upstream_node_input_size
            download_time = metadata_access.predict_data_transfer_time('download', downloadable_input_size, resource_config, sla, allow_cached=True)
            assert download_time is not None
            exec_time = metadata_access.predict_execution_time(node.func_name, total_input_size, resource_config, sla, allow_cached=True)
            assert exec_time is not None
            output_size = metadata_access.predict_output_size(node.func_name, total_input_size, sla, allow_cached=True)
            assert output_size is not None
            # Won't need to upload since it will be executed on the same worker
            if len(node.downstream_nodes) > 0 and all(dt.get_annotation(TaskWorkerResourceConfiguration).worker_id == worker_id for dt in node.downstream_nodes):
                upload_time = 0
            else:
                upload_time = metadata_access.predict_data_transfer_time('upload', output_size, resource_config, sla, allow_cached=True)
            assert upload_time is not None
            nodes_info[node_id] = DAGPlanner.PlanningTaskInfo(node, total_input_size, output_size, download_time, exec_time, upload_time, download_time + exec_time + upload_time, 0, 0)
        self._calculate_path_times(topo_sorted_nodes, nodes_info)
        
        return nodes_info
    
    def _calculate_node_timings_with_custom_resources(self, topo_sorted_nodes: list[DAGTaskNode], metadata_access: MetadataAccess, sla: SLA):
        """
        Calculate timing information for all nodes using custom resource configurations
        """
        nodes_info: dict[str, DAGPlanner.PlanningTaskInfo] = {}

        for node in topo_sorted_nodes:
            node_id = node.id.get_full_id()
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            worker_id = resource_config.worker_id
            (input_size_per_worker, total_input_size) = self._calculate_input_sizes_per_worker_id(node, nodes_info)
            downloadable_input_size = 0
            for upstream_worker_id, upstream_node_input_size in input_size_per_worker.items():
                if upstream_worker_id != worker_id: downloadable_input_size += upstream_node_input_size
            download_time = metadata_access.predict_data_transfer_time('download', downloadable_input_size, resource_config, sla, allow_cached=True)
            assert download_time is not None
            exec_time = metadata_access.predict_execution_time(node.func_name, total_input_size, resource_config, sla, allow_cached=True)
            assert exec_time is not None
            output_size = metadata_access.predict_output_size(node.func_name, total_input_size, sla, allow_cached=True)
            assert output_size is not None
            if len(node.downstream_nodes) > 0 and all(dt.get_annotation(TaskWorkerResourceConfiguration).worker_id == worker_id for dt in node.downstream_nodes):
                upload_time = 0
            else:
                upload_time = metadata_access.predict_data_transfer_time('upload', output_size, resource_config, sla, allow_cached=True)
            assert upload_time is not None
            nodes_info[node_id] = DAGPlanner.PlanningTaskInfo(node, total_input_size, output_size, download_time, exec_time, upload_time, download_time + exec_time + upload_time, 0, 0)

        self._calculate_path_times(topo_sorted_nodes, nodes_info)

        return nodes_info
    
    def _calculate_path_times(self, topo_sorted_nodes: list[DAGTaskNode], nodes_info: dict[str, PlanningTaskInfo]):
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
    
    def _find_critical_path(self, dag, nodes_info: dict[str, PlanningTaskInfo]):
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
    
    def validate_plan(self, nodes_info: dict[str, PlanningTaskInfo]):
        """ 
        - Ensure that all tasks have TaskWorkerResourceConfiguration annotation with non-empty worker_id
        - Ensure that equal worker_ids are assigned to tasks with the same resource config
        """
        worker_id_to_resources_map: dict[str, tuple[float, int]] = {}
        for node_id, info in nodes_info.items():
            resource_config = info.node_ref.get_annotation(TaskWorkerResourceConfiguration)
            worker_id = resource_config.worker_id
            if worker_id == "":
                raise Exception(f"Task {node_id} has no 'worker_id' assigned")
            if worker_id in worker_id_to_resources_map and worker_id_to_resources_map[worker_id] != (resource_config.cpus, resource_config.memory_mb):
                raise Exception(f"Worker {worker_id} has different resource configurations on different tasks")
            worker_id_to_resources_map[worker_id] = (resource_config.cpus, resource_config.memory_mb)

    def _visualize_plan(self, dag, nodes_info: dict[str, PlanningTaskInfo], critical_path_node_ids):
        """
        Visualize the DAG with task information using Graphviz.
        
        Args:
            dag: The DAG object
            nodes_info: Dictionary mapping node IDs to PlanningTaskInfo objects
            node_to_resource_config: Dictionary mapping node IDs to resource configurations
            critical_path_node_ids: Set of node IDs in the critical path
            output_file: Base filename to save the visualization (without extension)
        """
        from src.dag.dag import GenericDAG
        _dag: GenericDAG = dag
        # Create a new directed graph
        dot = Digraph(comment='DAG Visualization')
        dot.attr(rankdir='LR')  # Left to right layout
        # Set node attributes with increased height/width margins to prevent text cutoff
        dot.attr('node', shape='box', fontname='Arial', fontsize='11', margin='0.3,0.2', height='0.8')
        
        # Collect unique resource configurations and sort them
        resource_configs = {}
        for node in _dag._all_nodes.values():
            config = node.get_annotation(TaskWorkerResourceConfiguration)
            config_key = f"CPU:{config.cpus},Mem:{config.memory_mb}MB"
            resource_configs[config_key] = (config.cpus, config.memory_mb)
        
        # Sort resource configurations by CPU and memory (highest to lowest)
        sorted_configs = sorted(resource_configs.items(), 
                            key=lambda x: (x[1][0], x[1][1]))
        
        # Generate color map using shades of blue (from dark to light)
        color_map = {}
        base_color = (173, 216, 230)
        num_configs = len(sorted_configs)
        
        for i, (config_key, _) in enumerate(sorted_configs):
            # Calculate shade - more resources = darker shade but not too dark
            # Increased minimum intensity from 0.3 to 0.5 to make the darkest tone lighter
            intensity = 0.5 + 0.5 * (1 - i / max(1, num_configs - 1))  
            
            # Apply intensity to each RGB component
            r = int(base_color[0] * intensity)
            g = int(base_color[1] * intensity)
            b = int(base_color[2] * intensity)
            
            hex_color = '#{:02x}{:02x}{:02x}'.format(r, g, b)
            color_map[config_key] = hex_color
        
        # Add nodes
        for node_id, info in nodes_info.items():
            node = info.node_ref
            resource_config = info.node_ref.get_annotation(TaskWorkerResourceConfiguration)
            config_key = f"CPU:{resource_config.cpus},Mem:{resource_config.memory_mb}MB"
            
            # Create node label with task name in bold and larger font
            # Use HTML formatting to better control spacing and prevent text cutoff
            # Added extra <BR/> spacing between lines and smaller font for details
            label = f"<<TABLE BORDER='0' CELLBORDER='0' CELLSPACING='0' CELLPADDING='0'>" \
                    f"<TR><TD><B><FONT POINT-SIZE='13'>{node.func_name}</FONT></B></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>I/O: {info.input_size} - {info.output_size} bytes</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>Time: {info.earliest_start:.2f} - {info.path_completion_time:.2f}ms</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>{config_key}</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>Worker: {node.get_annotation(TaskWorkerResourceConfiguration).worker_id[:6]}...</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>TID: {node.id.get_full_id()[-6:]}...</FONT></TD></TR>" \
                    f"</TABLE>>"
            
            # Set node properties
            fillcolor = color_map[config_key]
            
            # Set node style
            if node_id in critical_path_node_ids:
                # Critical path nodes get bold outline
                dot.node(node_id, label=label, style='filled,bold', penwidth='3', fillcolor=fillcolor)
            else:
                # Regular nodes
                dot.node(node_id, label=label, style='filled', fillcolor=fillcolor)
        
        # Add edges
        for node_id, info in nodes_info.items():
            node = info.node_ref
            for upstream in node.upstream_nodes:
                upstream_id = upstream.id.get_full_id()
                dot.edge(upstream_id, node_id)
        
        # Add resource configuration legend
        with dot.subgraph(name='cluster_legend_resources') as legend:
            if legend is not None:  # Check if legend was created properly
                legend.attr(label='Resource Configurations', style='filled', fillcolor='white')
                
                # Add a node for each resource configuration in sorted order
                for i, (config_key, _) in enumerate(sorted_configs):
                    hex_color = color_map[config_key]
                    legend.node(f'resource_{i}', label=config_key, style='filled', fillcolor=hex_color)
                
                # Arrange legend nodes horizontally
                legend.attr(rank='sink')
                legend.edges([])
        
        # Add critical path legend with white background
        with dot.subgraph(name='cluster_legend_critical') as legend:
            if legend is not None:  # Check if legend was created properly
                legend.attr(label='Path Type', style='filled', fillcolor='white')
                
                # Create legend for critical vs normal path (white background)
                legend.node('critical', label='Critical Path', style='filled,bold', penwidth='3', fillcolor='white')
                legend.node('normal', label='Normal Path', style='filled', fillcolor='white')
                
                # Arrange legend nodes horizontally
                legend.attr(rank='sink')
                legend.edges([])
        
        # Save to file
        _dag: GenericDAG = dag
        output_file_name = f"planned_{_dag.sink_node.func_name}"
        dot.render(output_file_name, format='png', cleanup=True)
        # dot.render(output_file_name, format='png', cleanup=True, view=True)
        print(f"DAG visualization saved to {output_file_name}.png")
        
        return dot

class SimpleDAGPlanner(DAGPlanner, WorkerExecutionLogic):
    @dataclass
    class Config(DAGPlanner.Config):
        available_worker_resource_configurations: list[TaskWorkerResourceConfiguration]

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
                    unique_resources.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id
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
                    unique_resources.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id
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
                resource_config.worker_id = node.upstream_nodes[0].get_annotation(TaskWorkerResourceConfiguration).worker_id

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
                original_config = node.get_annotation(TaskWorkerResourceConfiguration)
                # if resource_config.cpus == original_config.cpus and resource_config.memory_mb == original_config.memory_mb: continue
                # Temporarily assign this resource config
                simulation_resource_config = resource_config.clone()
                simulation_resource_config.worker_id = ""

                for unode in node.upstream_nodes:
                    unode_resources = unode.get_annotation(TaskWorkerResourceConfiguration)
                    if unode_resources.cpus == simulation_resource_config.cpus and unode_resources.memory_mb == simulation_resource_config.memory_mb: 
                        simulation_resource_config.worker_id = unode.get_annotation(TaskWorkerResourceConfiguration).worker_id

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
                    node_downgrade_successful = True

            if node_downgrade_successful:
                successful_downgrades += 1

        logger.info(f"Downgraded resources for {successful_downgrades} nodes out of {len(nodes_outside_critical_path)} nodes outside the critical path in {lower_resources_simulation_timer.stop():.3f} ms")
        
        # Log Results
        resource_distribution = {}
        for node_id, node in _dag._all_nodes.items():
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            config_key = f"CPU:{resource_config.cpus},Memory:{resource_config.memory_mb}MB"
            if config_key not in resource_distribution: resource_distribution[config_key] = 0
            resource_distribution[config_key] += 1
            
        logger.info(f"CRITICAL PATH | Nodes: {len(critical_path_nodes)} | Predicted Completion Time: {critical_path_time} ms")
        logger.info(f"Resource distribution after optimization: {resource_distribution}")
        logger.info(f"Planning completed in {algorithm_start_time.stop():.3f} ms")

        # DEBUG: Plan Visualization
        updated_nodes_info = self._calculate_node_timings_with_custom_resources(topo_sorted_nodes, metadata_access, self.config.sla)
        self._visualize_plan(dag, updated_nodes_info, critical_path_node_ids)
        # !!! FOR QUICK TESTING ONLY. REMOVE LATER !!!
        self.validate_plan(updated_nodes_info)
        # exit()
