- BUG: Calling visualize before compute causes errors
    ```
    Traceback (most recent call last):
        File "C:\Users\dijes\Desktop\Current\PG\Tecnico\_A2\_PIC\code\Dask\kong\src\executor.py", line 98, in _start_executing
            raise e
        File "C:\Users\dijes\Desktop\Current\PG\Tecnico\_A2\_PIC\code\Dask\kong\src\executor.py", line 51, in _start_executing
            task_result = task.invoke(dependencies=task_dependencies)
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        File "C:\Users\dijes\Desktop\Current\PG\Tecnico\_A2\_PIC\code\Dask\kong\src\dag_task_node.py", line 52, in invoke
            if arg.value not in dependencies: raise Exception(f"[BUG] Output of {arg.value} not in dependencies")
                                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        Exception: [BUG] Output of a-939_36d7 not in dependencies
    ```
    - cause: calling dag_representation = dag.DAG(sink_node=self) more than once on client
        both DAG constructors see that their id is not there so they append it to the original task!
            should we clone `DAGNodeTask` instances??
            format checks?
- At least 1 VirtualWorker should be killing itself on fan-ins. Is it already happening??
- Find common stuff between `LocalCoroutineWorker` and `FlaskExecutor`

- Implement RemoteWorker (similar to VirtualWorker) but instead of invoking new asyncio task, invokes new remote worker (how to handle finishing?)

- How to handle user-code dependencies
    How does pywren do it?
    Any way to identify dependencies at runtime + pip install dynamically?

- Collect metrics (make them persistent in Redis)
    Execution time
    Input data size
    Output data size
    Data download time
    Data upload time
- Create another storage class (MetadataStorage) but use same DB in development

- More WUKONG-specific stuff
    - Task Clustering (fan-ins + fan-outs)
    - Delayed I/O

- Implement similar WUKONG algorithms
    can I wrap `Dask` functions?

- Make the executor run on OpenFaaS