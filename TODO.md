- BUG: using the threading event doesnt work because it blocks the Executor thread and doesn't allow coroutines to execute the tasks
    using asycnio event doesn work because it cant be set from another thread
- Redis communications using the "dag.master_id"
    master_id is used start from at the executor level?
- InternalWorker signal master executor shutdown if its the last task and finished
- Improve user interface to `DAGTaskNode.compute() -> R`