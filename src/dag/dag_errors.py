class DAGError(Exception):
    """Base class for all DAG-related exceptions"""
    pass

class MultipleSinkNodesError(DAGError):
    """Raised when more than one sink node is detected in the DAG"""
    def __init__(self, func_name: str):
        message = f"[ClientError] Invalid DAG! There can only be one sink node. Found more than 1 node with 0 downstream tasks (function_name={func_name})!"
        super().__init__(message)
        
class NoRootNodesError(DAGError):
    """Raised when there are no root nodes in the DAG"""
    def __init__(self, sink_node_id: str):
        message = f"[ClientError] No root nodes found for DAG with sink node: {sink_node_id}"
        super().__init__(message)