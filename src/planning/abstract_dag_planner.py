from abc import ABC, abstractmethod
from dataclasses import dataclass
import cloudpickle
from graphviz import Digraph
from typing import Literal
from collections import defaultdict

from src import dag_task_node
from src.dag_task_node import DAGTaskNode
from src.planning.annotations.preload import PreLoadOptimization
from src.planning.annotations.prewarm import PreWarmOptimization
from src.planning.annotations.taskdup import TaskDupOptimization
from src.planning.predictions.predictions_provider import PredictionsProvider
from src.planning.sla import SLA
from src.utils.logger import create_logger
from src.utils.utils import calculate_data_structure_size
from src.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.workers.worker_execution_logic import WorkerExecutionLogic

logger = create_logger(__name__, prefix="PLANNING")


class AbstractDAGPlanner(ABC, WorkerExecutionLogic):
    """
    A planner should override WorkerExecutionLogic methods if it uses annotations that may conflict with each other.
    This way, the planner can specify the desired behavior.
    """

    MAX_FAN_OUT_SIZE_W_SAME_WORKER = 4
    TIME_UNTIL_WORKER_GOES_COLD_S = 5

    @property
    def planner_name(self) -> str:
        return self.__class__.__name__

    @dataclass
    class BaseConfig(ABC):
        sla: SLA

        @abstractmethod
        def create_instance(self) -> "AbstractDAGPlanner": pass

    config: BaseConfig

    @dataclass
    class DuppableTaskPrediction:
        original_exec_time_ms: float # time to execute the duppable task on it's planned worker
        original_upload_time_ms: float # time to upload the duppable task output to storage
        original_download_time_ms: float # time for ME to download the duppable task OUTPUT from storage

        inputs_download_time_ms: float # time to download inputs of the duppable task
        exec_time_ms: float # time to execute the duppable task

    @dataclass
    class PlanningTaskInfo:
        node_ref: DAGTaskNode
        
        deserialized_input_size: int
        deserialized_output_size: int
        
        serialized_input_size: int
        serialized_output_size: int

        worker_startup_state: Literal["cold", "warm"] | None # None if no need to invoke new worker
        tp_worker_startup_time_ms: float # 0 if no need to invoke new worker
        tp_download_time_ms: float  # task-path download time (can be 0 if data was "preloaded")
        tp_exec_time_ms: float
        tp_upload_time_ms: float
        
        total_download_time_ms: float # time spent downloading data for the task (can't be 0 if data was "preloaded")
        
        earliest_start_ms: float # includes the time waiting for worker startup
        task_completion_time_ms: float

    @dataclass
    class PlanPredictionSampleCounts:
        previous_instances: int
        for_download_speed: int
        for_upload_speed: int
        for_execution_time: int
        for_output_size: int

    @dataclass
    class PlanOutput:
        planner_name: str
        sla: SLA
        nodes_info: dict[str, "AbstractDAGPlanner.PlanningTaskInfo"]
        critical_path_node_ids: set[str]
        prediction_sample_counts: "AbstractDAGPlanner.PlanPredictionSampleCounts"
        
        total_time_waiting_for_worker_startup_ms: float = -1

        def __post_init__(self):
            self.total_time_waiting_for_worker_startup_ms = sum(map(lambda node_info: node_info.tp_worker_startup_time_ms, self.nodes_info.values()))

    def plan(self, dag, predictions_provider: PredictionsProvider) -> PlanOutput | None:
        """
        dag: dag.DAG
        predictions_provider: PredictionsProvider
        
        Adds annotations to the given DAG tasks (mutates the tasks)
        """
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
        
        logger.info(f"Planner: {self.__class__.__name__} | SLA: {self.config.sla}")
        logger.info(f"Planner Algorithm Description:\n{self.get_description()}")
        plan_result = self.internal_plan(_dag, predictions_provider)
        if not plan_result: 
            self.validate_plan(_dag.root_nodes)
            return None # no plan was made
        else:
            # self._store_plan_image(_dag, plan_result.nodes_info, plan_result.critical_path_node_ids)
            # self._store_plan_as_json(_dag, plan_result.nodes_info)
            self.validate_plan(_dag.root_nodes)
        # exit() # !!! FOR QUICK TESTING ONLY. REMOVE LATER !!
        return plan_result

    @abstractmethod
    def internal_plan(self, dag, predictions_provider: PredictionsProvider) -> PlanOutput | None:
        """
        dag: dag.DAG
        predictions_provider: PredictionsProvider
        
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
    
    def _calculate_total_input_size(self, node, nodes_info: dict[str, PlanningTaskInfo], deserialized: bool) -> int:
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
                    output_size = nodes_info[upstream_node_id].deserialized_output_size if deserialized else nodes_info[upstream_node_id].serialized_output_size
                    total_input_size += output_size
            elif isinstance(func_arg, list) and all(isinstance(item, dag_task_node.DAGTaskNodeId) for item in func_arg):
                for item in func_arg:
                    upstream_node_id = item.get_full_id()
                    if upstream_node_id in nodes_info:
                        output_size = nodes_info[upstream_node_id].deserialized_output_size if deserialized else nodes_info[upstream_node_id].serialized_output_size
                        total_input_size += output_size
            else:
                total_input_size += calculate_data_structure_size(func_arg) if deserialized else calculate_data_structure_size(cloudpickle.dumps(func_arg))
        for func_kwarg_val in node.func_kwargs.values():
            if isinstance(func_kwarg_val, dag_task_node.DAGTaskNodeId): 
                upstream_node_id = func_kwarg_val.get_full_id()
                if upstream_node_id in nodes_info: 
                    output_size = nodes_info[upstream_node_id].deserialized_output_size if deserialized else nodes_info[upstream_node_id].serialized_output_size
                    total_input_size += output_size
            elif isinstance(func_kwarg_val, list) and all(isinstance(item, dag_task_node.DAGTaskNodeId) for item in func_kwarg_val):
                for item in func_kwarg_val:
                    upstream_node_id = item.get_full_id()
                    if upstream_node_id in nodes_info:
                        output_size = nodes_info[upstream_node_id].deserialized_output_size if deserialized else nodes_info[upstream_node_id].serialized_output_size
                        total_input_size += output_size
            else:
                total_input_size += calculate_data_structure_size(func_kwarg_val) if deserialized else calculate_data_structure_size(cloudpickle.dumps(func_kwarg_val))

        return total_input_size
    
    def __calculate_node_timings(self, nodes_info: dict[str, PlanningTaskInfo], node: DAGTaskNode, resource_config: TaskWorkerResourceConfiguration, predictions_provider: PredictionsProvider, sla: SLA):
        node_id = node.id.get_full_id()
        worker_id = node.worker_config.worker_id
        deserialized_input_size = self._calculate_total_input_size(node, nodes_info, deserialized=True)
        serialized_input_size = self._calculate_total_input_size(node, nodes_info, deserialized=False)

        downloadable_input_size = 0
        
        # 1. Calculate earliest start time (max of upstream completions)
        earliest_start = 0.0
        for unode in node.upstream_nodes:
            earliest_start = max(earliest_start, nodes_info[unode.id.get_full_id()].task_completion_time_ms)
        
        # 2. Calculate download finish time (considering parallel downloads)
        download_finish_time = 0.0
        for unode in node.upstream_nodes:
            if unode.worker_config.worker_id == worker_id: 
                continue # same worker => no need to download from storage
            
            unode_info = nodes_info[unode.id.get_full_id()]
            predicted_download_time = predictions_provider.predict_data_transfer_time('download', unode_info.serialized_output_size, resource_config, sla)
            downloadable_input_size += unode_info.serialized_output_size
            
            if node.try_get_annotation(PreLoadOptimization):
                # preload: start downloading as soon as data is available
                download_start = unode_info.task_completion_time_ms # Data available time
            else:
                # Non-preload: Downloads start AFTER all upstreams complete
                download_start = earliest_start 
            
            download_finish_time = max(download_finish_time, download_start + predicted_download_time)
        
        # 3. Compute effective download delay
        tp_download_time = max(download_finish_time - earliest_start, 0)
        total_download_time = predictions_provider.predict_data_transfer_time('download', downloadable_input_size, resource_config, sla)
        
        # 4. Proceed with execution and upload calculations...
        exec_time = predictions_provider.predict_execution_time(node.func_name, deserialized_input_size, resource_config, sla)
        deserialized_output_size = predictions_provider.predict_output_size(node.func_name, deserialized_input_size, sla, deserialized=True)
        serialized_output_size = predictions_provider.predict_output_size(node.func_name, deserialized_input_size, sla, deserialized=False)

        # 5. Calculate upload_time (existing logic is correct)
        if len(node.downstream_nodes) > 0 and worker_id is not None and \
            all(dt.worker_config.worker_id == worker_id for dt in node.downstream_nodes):
            upload_time = 0.0
        else:
            upload_time = predictions_provider.predict_data_transfer_time('upload', deserialized_output_size, resource_config, sla)

        # 6. Total timing
        task_completion_time = earliest_start + tp_download_time + exec_time + upload_time

        for u_task in node.upstream_nodes:
            if not u_task.try_get_annotation(TaskDupOptimization): continue
            node.duppable_tasks_predictions[u_task.id.get_full_id()] = AbstractDAGPlanner.DuppableTaskPrediction(
                original_exec_time_ms=predictions_provider.predict_execution_time(u_task.func_name, nodes_info[u_task.id.get_full_id()].deserialized_input_size, u_task.worker_config, sla),
                original_upload_time_ms=predictions_provider.predict_data_transfer_time('upload', nodes_info[u_task.id.get_full_id()].serialized_output_size, u_task.worker_config, sla),
                original_download_time_ms=predictions_provider.predict_data_transfer_time('download', nodes_info[u_task.id.get_full_id()].serialized_output_size, resource_config, sla),

                exec_time_ms=predictions_provider.predict_execution_time(u_task.func_name, nodes_info[u_task.id.get_full_id()].deserialized_input_size, resource_config, sla),
                inputs_download_time_ms=predictions_provider.predict_data_transfer_time('download', nodes_info[u_task.id.get_full_id()].serialized_input_size, resource_config, sla)
            )
            
        nodes_info[node_id] = AbstractDAGPlanner.PlanningTaskInfo(
            node, 
            deserialized_input_size, 
            deserialized_output_size,
            serialized_input_size,
            serialized_output_size,
            None,
            0,
            tp_download_time,
            exec_time, 
            upload_time,
            total_download_time,
            earliest_start,
            task_completion_time
        )

    def __update_node_timings_with_worker_startup(self, topo_sorted_nodes: list[DAGTaskNode], nodes_info: dict[str, PlanningTaskInfo], predictions_provider: PredictionsProvider, sla: SLA):
        """
        Note: Needs to run after {earliest_start} and {path_completion_time} are calculated so that it can predict if the startup will be WARM or COLD
        """
        
        # create a new sorted list from topo_sorted_nodes where nodes where earliest start appear first
        # note: there can be overlapping time periods within the same resource_configuration
        worker_active_periods: dict[tuple[float, int], list[tuple[str | None, float, float]]] = defaultdict(list)  # (cpus, memory_mb) -> List[Tuple[worker_id, start_ms, end_ms]]

        # Collect expected worker activity periods
        for node in topo_sorted_nodes:
            my_resource_config = node.worker_config
            my_node_info = nodes_info[node.id.get_full_id()]
            # register when MY worker config should be active
            if my_resource_config.worker_id:
                worker_active_periods[(my_resource_config.cpus, my_resource_config.memory_mb)].append((
                    my_resource_config.worker_id,
                    my_node_info.earliest_start_ms, 
                    my_node_info.task_completion_time_ms + AbstractDAGPlanner.TIME_UNTIL_WORKER_GOES_COLD_S * 1_000
                ))
            # register when the worker config I PRE-WARM should be active
            prewarm_optimization = node.try_get_annotation(PreWarmOptimization)
            if prewarm_optimization:
                time_at_which_worker_will_be_ready_ms = my_node_info.earliest_start_ms + predictions_provider.predict_worker_startup_time(my_resource_config, 'cold', sla)
                for target_resource_config in prewarm_optimization.target_resource_configs:
                    worker_active_periods[(target_resource_config.cpus, target_resource_config.memory_mb)].append((
                        None,
                        time_at_which_worker_will_be_ready_ms,
                        time_at_which_worker_will_be_ready_ms + AbstractDAGPlanner.TIME_UNTIL_WORKER_GOES_COLD_S * 1_000
                    ))

        def _is_worker_warm_at_time(my_worker_id: str | None, worker_config: tuple[float, int], target_time_ms: float) -> bool:
            return any(
                worker_id != my_worker_id and start_ms < target_time_ms < end_ms 
                for worker_id, start_ms, end_ms in worker_active_periods[(worker_config[0], worker_config[1])]
            )

        # Second pass: apply the scheduling logic with simplified condition
        for node in topo_sorted_nodes:
            my_resource_config = node.worker_config
            my_node_info = nodes_info[node.id.get_full_id()]

            if any(
                n.worker_config.cpus == my_resource_config.cpus and \
                n.worker_config.memory_mb == my_resource_config.memory_mb and \
                (
                    n.worker_config.worker_id == my_resource_config.worker_id or \
                    n.worker_config.worker_id is None or \
                    my_resource_config.worker_id is None
                )
                for n in node.upstream_nodes
            ):
                # won't cause a worker launch, it will execute on already running worker
                my_node_info.tp_worker_startup_time_ms = 0
                my_node_info.worker_startup_state = None
                my_node_info.earliest_start_ms += 0
                my_node_info.task_completion_time_ms += 0
                continue

            if _is_worker_warm_at_time(my_resource_config.worker_id, (my_resource_config.cpus, my_resource_config.memory_mb), my_node_info.earliest_start_ms):
                # WARM START
                worker_startup_prediction = predictions_provider.predict_worker_startup_time(my_resource_config, "warm", sla)
                my_node_info.worker_startup_state = "warm" 
                my_node_info.tp_worker_startup_time_ms = worker_startup_prediction
                my_node_info.earliest_start_ms += worker_startup_prediction
                my_node_info.task_completion_time_ms += worker_startup_prediction
            else:
                # COLD START
                worker_startup_prediction = predictions_provider.predict_worker_startup_time(my_resource_config, "cold", sla)
                my_node_info.worker_startup_state = "cold"
                my_node_info.tp_worker_startup_time_ms = worker_startup_prediction
                my_node_info.earliest_start_ms += worker_startup_prediction
                my_node_info.task_completion_time_ms += worker_startup_prediction

        # Recalculate {earliest_start_ms} and {path_completion_times} after changing some {earliest_start_ms} to include {tp_worker_startup_time_ms}
        for node in topo_sorted_nodes:
            node_info = nodes_info[node.id.get_full_id()]
            for unode in node.upstream_nodes:
                node_info.earliest_start_ms = max(node_info.earliest_start_ms, nodes_info[unode.id.get_full_id()].task_completion_time_ms)
            node_info.task_completion_time_ms = node_info.earliest_start_ms + node_info.tp_download_time_ms + node_info.tp_exec_time_ms + node_info.tp_upload_time_ms

    def _calculate_node_timings_with_common_resources(self, topo_sorted_nodes: list[DAGTaskNode], predictions_provider: PredictionsProvider, resource_config: TaskWorkerResourceConfiguration, sla: SLA):
        """
        Calculate timing information for all nodes using the same resource configuration
        """
        nodes_info: dict[str, AbstractDAGPlanner.PlanningTaskInfo] = {}
        for node in topo_sorted_nodes:
            # note: modifies `nodes_info`
            self.__calculate_node_timings(nodes_info, node, resource_config, predictions_provider, sla)
            
        # Note: Needs to run after earliest_start and path_completion_time are calculated (self.__calculate_node_timings)
        self.__update_node_timings_with_worker_startup(topo_sorted_nodes, nodes_info, predictions_provider, sla)

        return nodes_info
    
    def _calculate_node_timings_with_custom_resources(self, topo_sorted_nodes: list[DAGTaskNode], predictions_provider: PredictionsProvider, sla: SLA):
        """
        Calculate timing information for all nodes using custom resource configurations
        """
        nodes_info: dict[str, AbstractDAGPlanner.PlanningTaskInfo] = {}

        for node in topo_sorted_nodes:
            resource_config = node.worker_config
            # note: modifies `nodes_info`
            self.__calculate_node_timings(nodes_info, node, resource_config, predictions_provider, sla)

        # Note: Needs to run after earliest_start and path_completion_time are calculated (self.__calculate_node_timings)
        self.__update_node_timings_with_worker_startup(topo_sorted_nodes, nodes_info, predictions_provider, sla)

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
                pred_completion = nodes_info[pred_id].task_completion_time_ms
                
                if pred_completion > max_completion_time:
                    max_completion_time = pred_completion
                    critical_predecessor = pred
            
            # Move to predecessor on critical path
            current_node = critical_predecessor
        
        critical_path_time = nodes_info[dag.sink_node.id.get_full_id()].task_completion_time_ms
        return critical_path, critical_path_time
    
    def validate_plan(self, root_nodes: list[DAGTaskNode]):
        """
        - Ensure that all tasks have TaskWorkerResourceConfiguration annotation with non-empty worker_id
        - Ensure that equal worker_ids are assigned to tasks with the same resource config
        - Ensure that there is at least 1 uninterrupted branch of tasks assigned to the same worker id
        """
        worker_id_to_resources_map: dict[str, tuple[float, int]] = {}
        # (worker_id, first_seen_task_id)
        seen_worker_ids: dict[str, str] = {}
        visited_nodes = set()

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
            resource_config = node.worker_config
            worker_id = resource_config.worker_id

            if worker_id is None and node.try_get_annotation(PreLoadOptimization):
                node.remove_annotation(PreLoadOptimization)
                logger.warning(f"Task {node.id.get_full_id()} has a 'PreLoadOptimization' optimization but since it's assigned to a flexible worker (worker_id=None), this optimization will be ignored in this run")
            
            # Validation #1 => Similar Worker IDs have same resources
            if worker_id is not None and worker_id in worker_id_to_resources_map and worker_id_to_resources_map[worker_id] != (resource_config.cpus, resource_config.memory_mb):
                raise Exception(f"Worker {worker_id} has different resource configurations on different tasks")
            
            if worker_id is not None:
                worker_id_to_resources_map[worker_id] = (resource_config.cpus, resource_config.memory_mb)
            
            # Validation #2 => Ensure that there are NO interrupted branches of tasks assigned to the same worker id
            if worker_id is not None:
                upstream_nodes_w_same_wid = [n for n in node.upstream_nodes if n.worker_config.worker_id == worker_id]
                if worker_id in seen_worker_ids and len(upstream_nodes_w_same_wid) == 0 and node.id.get_full_id() not in [rn.id.get_full_id() for rn in root_nodes]:
                    # could still be valid if AT LEAST 1 of its upstream tasks downstream tasks has the same worker id (meaning it was launched at the "same time")
                    other_udtasks_w_same_wid: set[str] = set()
                    for unode in node.upstream_nodes:
                        for udnode in unode.downstream_nodes:
                            if udnode.id.get_full_id() != node.id.get_full_id() and worker_id == udnode.worker_config.worker_id:
                                other_udtasks_w_same_wid.add(udnode.id.get_full_id())
                        
                    if len(other_udtasks_w_same_wid) > 0 and not any([other_udtask_id == seen_worker_ids[worker_id] for other_udtask_id in other_udtasks_w_same_wid]):
                        raise Exception(f"Worker {worker_id} has no uninterrupted branch of tasks. Detected at task: {node.id.get_full_id()} |task name: {node.func_name}")
                
                seen_worker_ids[worker_id] = node.id.get_full_id()
                
        # logger.info("Validation Succeeded!")

    def _store_plan_image(self, dag, nodes_planning_info: dict[str, PlanningTaskInfo] = dict(), critical_path_node_ids: set[str] = set()):
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
            config = node.worker_config
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
            resource_config = node.worker_config
            config_key = f"CPU:{resource_config.cpus},Mem:{resource_config.memory_mb}MB"
            
            # Create node label with task name in bold and larger font
            # Use HTML formatting to better control spacing and prevent text cutoff
            # Added extra <BR/> spacing between lines and smaller font for details
            has_optimization_preload = node.try_get_annotation(PreLoadOptimization) is not None
            has_optimization_prewarm = node.try_get_annotation(PreWarmOptimization) is not None
            has_optimization_taskdup = node.try_get_annotation(TaskDupOptimization) is not None
            label = f"<<TABLE BORDER='0' CELLBORDER='0' CELLSPACING='0' CELLPADDING='0'>" \
                    f"<TR><TD><B><FONT POINT-SIZE='13'>{node.func_name}</FONT></B></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>I/O: {node_info.deserialized_input_size if node_info else 0} - {node_info.deserialized_output_size if node_info else 0} bytes</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>Time: {node_info.earliest_start_ms if node_info else 0:.2f} - {node_info.task_completion_time_ms if node_info else 0:.2f}ms</FONT></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>{config_key}</FONT></TD></TR>" \
                    f"<TR><TD><B><FONT POINT-SIZE='11'>{"PreLoad" if has_optimization_preload else ""} {"PreWarm" if has_optimization_prewarm else ""} {"TaskDup" if has_optimization_taskdup else ""} </FONT></B></TD></TR>" \
                    f"<TR><TD><FONT POINT-SIZE='11'>Worker: ...{resource_config.worker_id[-6:] if resource_config.worker_id else 'Flexbile'} | State: {node_info.worker_startup_state if node_info and node_info.worker_startup_state else '-'}</FONT></TD></TR>" \
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
        output_file_name = f"./_dag_plans/{_dag.dag_name}_{self.planner_name}"
        dot.render(output_file_name, format='png', cleanup=True)
        # dot.render(output_file_name, format='png', cleanup=True, view=True)
        print(f"DAG visualization saved to {output_file_name}")
        
        return dot

    def _store_plan_as_json(self, dag, nodes_planning_info: dict[str, PlanningTaskInfo] = dict()):
        """
        Store DAG task information as hierarchical JSON starting from root nodes.
        
        Args:
            dag: The DAG object
            nodes_planning_info: Dictionary mapping node IDs to PlanningTaskInfo objects
        """
        import json
        from src.dag.dag import GenericDAG
        
        _dag: GenericDAG = dag
        
        def build_node_tree(node, visited=None):
            """Recursively build hierarchical tree structure for a node and its children."""
            if visited is None:
                visited = set()
            
            node_id = node.id.get_full_id()
            
            # Avoid infinite loops in case of cycles
            if node_id in visited:
                return {
                    "id": node_id,
                    "function_name": node.func_name,
                    "assigned_worker_id": node.worker_config.worker_id if node.worker_config.worker_id else "Flexible",
                    "note": "Already processed (cycle detected)"
                }
            
            visited.add(node_id)
            
            # Build node info
            node_info = {
                "id": node_id,
                "function_name": node.func_name,
                "assigned_worker_id": node.worker_config.worker_id if node.worker_config.worker_id else "Flexible",
                "children": []
            }
            
            # Recursively add downstream nodes as children
            for downstream_node in node.downstream_nodes:
                child_info = build_node_tree(downstream_node, visited.copy())
                node_info["children"].append(child_info)
            
            return node_info
        
        # Find root nodes (nodes with no upstream dependencies)
        root_nodes = [node for node in _dag._all_nodes.values() if len(node.upstream_nodes) == 0]
        
        # Build hierarchical structure starting from root nodes
        hierarchical_tasks = []
        for root_node in root_nodes:
            root_tree = build_node_tree(root_node)
            hierarchical_tasks.append(root_tree)
        
        # Create output structure
        output_data = {
            "dag_name": _dag.sink_node.func_name,
            "total_tasks": len(_dag._all_nodes),
            "root_count": len(root_nodes),
            "hierarchical_tasks": hierarchical_tasks
        }
        
        # Save to JSON file
        output_file_name = f"./_dag_plans/{_dag.dag_name}.json"
        with open(output_file_name, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"DAG task information saved to {output_file_name}")
        
        return output_data