[OPTIMIZATION:WORKER_STARTUP_SPEED]
- Storing the full dag on redis is costly (DAG retrieval time adds up)
    - If below a certain bytes threshold, pass the subDAG in the invocation itself
    - Worker first checks if the invocation contains the subDAG and if so, doesn't download it from storage
    - Functions with the same name have same code => use a dict[function_name, self.func_code] to save space
    - Also, DAG size is too big for small code and tasks (35kb for image_transform)
    [EXTRA_SAME_SPIRIT] DAGTaskNode.clone() should clone cached_result if below a threshold => then worker checks the DAG received for cached results before downloading from storage

[NEW_OPTIMIZATION?:OUTPUT_STREAMING]
- As 1 worker starts uploading task output, another worker it immediatelly downloading it, masking download time, since it doesn't have to wait for the upload to complete
- Possible to do using Redis?
- BENEFITS
    - Using pubsub to avoid storing intermediate outputs (when applicable) permanently and save download time (would be same as upload time because as 1 byte gets uploaded, it gets downloaded immediatelly)
- DRAWBACKS
    - Would have to know if the receiver was already active (SYNC required?)
        how:
            - Check if the result of the first task of the receiver worker already **exists** in storage

[PREDICTIONS_IMPROVEMENT] Adaptative `exponent` for **data transfer** relationships

[WORKER_EFFICIENCY] Async output upload => to allow the worker to execute other tasks without waiting for upload to finish
- useful on fan-outs where this worker will execute some of the downstream tasks

[SIMULATIONS_MORE_GENERIC] Change the way simulations are made, to be more realistic and follow the scheduling logic defined by the planner to be used
    - Run the actual `Worker.execute_branch` or a mock `Worker.mock_execute_branch` but with dummy stuff and don't actually execute the tasks nor upload metrics
    - As it "executes" the simulation, keep track of metrics and then the planner can use them to make decisions
    - REPLACES CURRENT FUNCTION: `__calculate_node_timings`
    - Think if all predictions are possible (can we fill all fields of `PlanningTaskInfo`??)

[VISUALIZATION] A dashboard that simulates diff. planning algorithms for workflow types that were already seen?
[VISUALIZATION] Update the realtime dashboard to use the same "agrapgh" configuration as the metrics Dashboard

[LANGUAGE] Add decorator for user to provide information about specific functions
    - I/O Relationship (linear or exponential (what exponent) or None)

[LANGUAGE]
- Allow user to specify requirements.txt dependencies so that they don't need to be downloaded at runtime
- At the "workflow language", support more levels: e.g., list[list[DAGTaskNode]]
    Find examples where this makes sense (arg: dict[str, DAGTaskNode])
    Find a better way to iterate through them to avoid repetition
