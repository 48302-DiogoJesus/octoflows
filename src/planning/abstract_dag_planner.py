from abc import ABC, abstractmethod
from dataclasses import dataclass
from graphviz import Digraph

from src import dag_task_node
from src.dag_task_node import DAGTaskNode
from src.planning.annotations.preload import PreLoadOptimization
from src.planning.metadata_access.metadata_access import MetadataAccess
from src.planning.sla import SLA
from src.utils.logger import create_logger
from src.utils.utils import calculate_data_structure_size
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.workers.worker_execution_logic import WorkerExecutionLogic

logger = create_logger(__name__, prefix="PLANNING")

class AbstractDAGPlanner(ABC, WorkerExecutionLogic):
    """
    A planner should override WorkerExecutionLogic methods if it uses annotations that may conflict with each other.
    This way, the planner can specify the desired behavior.
    """
    @dataclass
    class Config(ABC):
        sla: SLA

        @abstractmethod
        def create_instance(self) -> "AbstractDAGPlanner": pass

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

    @dataclass
    class PlanOutput:
        nodes_info: dict[str, "AbstractDAGPlanner.PlanningTaskInfo"]
        critical_path_node_ids: set[str]

    def plan(self, dag, metadata_access: MetadataAccess):
        """
        dag: dag.DAG
        metadata_access: MetadataAccess
        
        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
        
        logger.info(f"Planner: {self.__class__.__name__}")
        logger.info(f"Planner Algorithm Description:\n{self.get_description()}")
        plan_result = self.internal_plan(_dag, metadata_access)
        if not plan_result: return # no plan was made
        self._visualize_plan(_dag, plan_result.nodes_info, plan_result.critical_path_node_ids)
        self.validate_plan(_dag.root_nodes)
        # exit() # !!! FOR QUICK TESTING ONLY. REMOVE LATER !!

    @abstractmethod
    def internal_plan(self, dag, metadata_access: MetadataAccess) -> PlanOutput | None:
        """
        dag: dag.DAG
        metadata_access: MetadataAccess
        
        To be implemented by the Planners
        """
        pass

    @abstractmethod
    def get_description(self) -> str: pass

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
    
    def _calculate_total_input_size(self, node, nodes_info: dict[str, PlanningTaskInfo]) -> int:
        """
        Returns input sizes of upstream_nodes grouped by worker_id
        Returns total input size
        """

        total_input_size = 0
        # For root nodes, use the size from function args (estimate)
        for func_arg in node.func_args:
            if isinstance(func_arg, dag_task_node.DAGTaskNodeId): 
                upstream_node_id = func_arg.get_full_id()
                if upstream_node_id in nodes_info: 
                    output_size = nodes_info[upstream_node_id].output_size
                    total_input_size += output_size
            else:
                total_input_size += calculate_data_structure_size(func_arg)
        for func_kwarg_val in node.func_kwargs.values():
            if isinstance(func_kwarg_val, dag_task_node.DAGTaskNodeId): 
                upstream_node_id = func_kwarg_val.get_full_id()
                if upstream_node_id in nodes_info: 
                    output_size = nodes_info[upstream_node_id].output_size
                    total_input_size += output_size
            else:
                total_input_size += calculate_data_structure_size(func_kwarg_val)

        return total_input_size
    
    def __calculate_node_timings_for_node(self, nodes_info: dict[str, PlanningTaskInfo], node: DAGTaskNode, resource_config: TaskWorkerResourceConfiguration, metadata_access: MetadataAccess, sla: SLA):
        node_id = node.id.get_full_id()
        worker_id = node.get_annotation(TaskWorkerResourceConfiguration).worker_id
        total_input_size = self._calculate_total_input_size(node, nodes_info)
        
        # Calculate earliest start time and latest upstream completion time
        earliest_start = 0
        latest_upstream_node_completion_time = 0
        
        for unode in node.upstream_nodes:
            unode_info = nodes_info[unode.id.get_full_id()]
            # Update earliest start based on all upstream nodes
            earliest_start = max(earliest_start, unode_info.path_completion_time)
            
            # Track latest completion time for nodes on different workers (for preload calculations)
            if unode.get_annotation(TaskWorkerResourceConfiguration).worker_id != worker_id:
                latest_upstream_node_completion_time = max(latest_upstream_node_completion_time, unode_info.path_completion_time)

        download_time = 0
        for unode in node.upstream_nodes:
            if unode.get_annotation(TaskWorkerResourceConfiguration).worker_id == worker_id: 
                continue
            unode_info = nodes_info[unode.id.get_full_id()]
            if not node.try_get_annotation(PreLoadOptimization):
                predicted_download_time = metadata_access.predict_data_transfer_time('download', unode_info.output_size, resource_config, sla, allow_cached=True)
                assert predicted_download_time
                download_time += predicted_download_time
                continue
            
            earliest_possible_download_start_offset = latest_upstream_node_completion_time - unode_info.path_completion_time
            predicted_download_time = metadata_access.predict_data_transfer_time('download', unode_info.output_size, resource_config, sla, allow_cached=True)
            assert predicted_download_time
            download_time += max(predicted_download_time - earliest_possible_download_start_offset, 0)
            
        exec_time = metadata_access.predict_execution_time(node.func_name, total_input_size, resource_config, sla, allow_cached=True)
        assert exec_time is not None
        output_size = metadata_access.predict_output_size(node.func_name, total_input_size, sla, allow_cached=True)
        assert output_size is not None
        
        # Won't need to upload since it will be executed on the same worker
        if len(node.downstream_nodes) > 0 and worker_id is not None and all(dt.get_annotation(TaskWorkerResourceConfiguration).worker_id == worker_id for dt in node.downstream_nodes):
            upload_time = 0
        else:
            upload_time = metadata_access.predict_data_transfer_time('upload', output_size, resource_config, sla, allow_cached=True)
        assert upload_time is not None
        
        total_time = download_time + exec_time + upload_time
        path_completion_time = earliest_start + total_time
        
        nodes_info[node_id] = AbstractDAGPlanner.PlanningTaskInfo(
            node, 
            total_input_size, 
            output_size, 
            download_time, 
            exec_time, 
            upload_time, 
            total_time, 
            earliest_start,
            path_completion_time
        )

    def _calculate_node_timings_with_common_resources(self, topo_sorted_nodes: list[DAGTaskNode], metadata_access: MetadataAccess, resource_config: TaskWorkerResourceConfiguration, sla: SLA):
        """
        Calculate timing information for all nodes using the same resource configuration
        """
        nodes_info: dict[str, AbstractDAGPlanner.PlanningTaskInfo] = {}
        for node in topo_sorted_nodes:
            # note: modifies `nodes_info`
            self.__calculate_node_timings_for_node(nodes_info, node, resource_config, metadata_access, sla)
            
        return nodes_info
    
    def _calculate_node_timings_with_custom_resources(self, topo_sorted_nodes: list[DAGTaskNode], metadata_access: MetadataAccess, sla: SLA):
        """
        Calculate timing information for all nodes using custom resource configurations
        """
        nodes_info: dict[str, AbstractDAGPlanner.PlanningTaskInfo] = {}

        for node in topo_sorted_nodes:
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            # note: modifies `nodes_info`
            self.__calculate_node_timings_for_node(nodes_info, node, resource_config, metadata_access, sla)

        return nodes_info
    
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
    
    def validate_plan(self, root_nodes: list[DAGTaskNode]):
        """
        - Ensure that all tasks have TaskWorkerResourceConfiguration annotation with non-empty worker_id
        - Ensure that equal worker_ids are assigned to tasks with the same resource config
        - Ensure that there is at least 1 uninterrupted branch of tasks assigned to the same worker id
        """
        worker_id_to_resources_map: dict[str, tuple[float, int]] = {}
        used_worker_ids: set[str] = set()
        visited_nodes = set()
        # tasks that were already verified
        worker_id_branches_verification_set: set[str] = set()

        # Initialize queue with root nodes
        queue: list[DAGTaskNode] = []
        for node in root_nodes:
            queue.append(node)

        # BFS traversal
        while queue:
            node = queue.pop(0)
            if node in visited_nodes: continue
            visited_nodes.add(node)
            
            # Add all downstream nodes to the queue
            for ds_node in node.downstream_nodes:
                if ds_node not in visited_nodes:
                    queue.append(ds_node)
            
            # Get resource configuration
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            worker_id = resource_config.worker_id
            
            # Validation #1 => Similar Worker IDs have same resources
            if worker_id == None:
                raise Exception(f"Task {node.id.get_full_id()} has no 'worker_id' assigned")
            
            if worker_id in worker_id_to_resources_map and worker_id_to_resources_map[worker_id] != (resource_config.cpus, resource_config.memory_mb):
                raise Exception(f"Worker {worker_id} has different resource configurations on different tasks")
            
            worker_id_to_resources_map[worker_id] = (resource_config.cpus, resource_config.memory_mb)
            
            # Validation #2 => Ensure that there are NO interrupted branches of tasks assigned to the same worker id
            if node in root_nodes:
                worker_config = node.get_annotation(TaskWorkerResourceConfiguration)
                if worker_config.worker_id is not None: used_worker_ids.add(worker_config.worker_id)
            else:
                dnodes_grouped_by_worker_id: dict[str, list[DAGTaskNode]] = {}
                for n in node.downstream_nodes:
                    wid = n.get_annotation(TaskWorkerResourceConfiguration).worker_id
                    if wid is None: continue
                    if wid not in dnodes_grouped_by_worker_id: dnodes_grouped_by_worker_id[wid] = []
                    dnodes_grouped_by_worker_id[wid].append(n)

                for dwid, tasks in dnodes_grouped_by_worker_id.items():
                    for task in tasks:
                        if task.id.get_full_id() in worker_id_branches_verification_set: continue
                        upstream_nodes_w_same_wid = [n for n in task.upstream_nodes if n.get_annotation(TaskWorkerResourceConfiguration).worker_id == dwid]
                        if dwid in used_worker_ids and len(upstream_nodes_w_same_wid) == 0:
                            raise Exception(f"Worker {dwid} has no uninterrupted branch of tasks. Detected at task: {task.id.get_full_id()}")
                        worker_id_branches_verification_set.add(task.id.get_full_id())
                    used_worker_ids.add(dwid)
                
        logger.info("Validation Succeeded!")

    def _visualize_plan(self, dag, nodes_planning_info: dict[str, PlanningTaskInfo] = dict(), critical_path_node_ids: set[str] = set()):
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
        for node in _dag._all_nodes.values():
            node_id = node.id.get_full_id()
            node_info = nodes_planning_info[node_id] if node_id in nodes_planning_info else None
            resource_config = node.get_annotation(TaskWorkerResourceConfiguration)
            config_key = f"CPU:{resource_config.cpus},Mem:{resource_config.memory_mb}MB"
            
            # Create node label with task name in bold and larger font
            # Use HTML formatting to better control spacing and prevent text cutoff
            # Added extra <BR/> spacing between lines and smaller font for details
            label = f"<<TABLE BORDER='0' CELLBORDER='0' CELLSPACING='0' CELLPADDING='0'>" \
                    f"<TR><TD><B><FONT POINT-SIZE='13'>{node.func_name}</FONT></B></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>I/O: {node_info.input_size if node_info else 0} - {node_info.output_size if node_info else 0} bytes</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>Time: {node_info.earliest_start if node_info else 0:.2f} - {node_info.path_completion_time if node_info else 0:.2f}ms</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>{config_key}</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>PreLoad: {node.try_get_annotation(PreLoadOptimization) is not None}</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>Worker: ...{resource_config.worker_id[-6:] if resource_config.worker_id else 'Flexbile'}</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>TID: ...{node.id.get_full_id()[-6:]}</FONT></TD></TR>" \
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
        for node_id, node in _dag._all_nodes.items():
            for upstream in node.upstream_nodes:
                upstream_id = upstream.id.get_full_id()
                dot.edge(upstream_id, node_id)
        
        # Add resource configuration legend
        with dot.subgraph(name='cluster_legend_resources') as legend: # type: ignore
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
        with dot.subgraph(name='cluster_legend_critical') as legend: # type: ignore
            if legend is not None:  # Check if legend was created properly
                legend.attr(label='Path Type', style='filled', fillcolor='white')
                
                # Create legend for critical vs normal path (white background)
                legend.node('critical', label='Critical Path', style='filled,bold', penwidth='3', fillcolor='white')
                legend.node('normal', label='Normal Path', style='filled', fillcolor='white')
                
                # Arrange legend nodes horizontally
                legend.attr(rank='sink')
                legend.edges([])
        
        # Save to file
        output_file_name = f"planned_{_dag.sink_node.func_name}"
        dot.render(output_file_name, format='png', cleanup=True)
        # dot.render(output_file_name, format='png', cleanup=True, view=True)
        print(f"DAG visualization saved to {output_file_name}.png")
        
        return dot
