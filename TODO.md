- Redis communications using the "dag.master_id"
    master_id is used start from at the executor level?
- Implement RemoteWorker (similar to VirtualWorker) but instead of invoking new asyncio task, invokes new remote worker (how to handle finishing?)