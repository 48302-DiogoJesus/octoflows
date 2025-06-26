- WARM/COLD starts ASSUMPTION:
    - using resource_config (memory + cpus) instead of worker_id

- BUG: When planning is done, the input_metrics time_ms is always 0

- `global_planning` dashboard
    - add makespan to the 2 visualizations

- See notion todo for "visualization todo"

---

- [!!] Add support for final result to be None, store a wrapper in storage instead

[DIFF_IDEA]
- Change the way simulations are made, to be more realistic and follow the scheduling logic defined by the planner to be used
    - Run the actual `Worker.execute_branch` or a mock `Worker.mock_execute_branch` but with dummy stuff and don't actually execute the tasks nor upload metrics
    - As it "executes" the simulation, keep track of metrics and then the planner can use them to make decisions
    - REPLACES CURRENT FUNCTION: `__calculate_node_timings`
    - Think if all predictions are possible (can we fill all fields of `PlanningTaskInfo`??)

- [DIFF_IDEA] A dashboard that simulates diff. planning algorithms for workflow types that were already seen?

[EVALUATION:PREPARE]
- Implement **WUKONG** planner
    + optimizations
        - Task Clustering (fan-ins + fan-outs)
        - Delayed I/O

[THINK:PLANNER_OPTIMIZATIONS]
- `pre-warm` (Just make an "empty" invocation (special message that the `Worker Handler` must be ready for?) to a container with a given **resource config**)
    Possible benefits: faster startup times for some tasks on "new" workers
        can't measure it at the planner level since the predictions aren't considering worker startup times (warm/cold)
- `task-dup`
    If a Worker A is waiting for the data of an upstream Task 1 (executing or to be executed on Worker 2 ) to be available, 
    it can execute that task itself. By executing Task 1 locally, Worker 2 won’t need to wait for the data to be available 
    and then download it from external storage. The results produced by Worker 2 will be ignored by Worker 1. 
    Possible benefits: - makespan ; - data download time.
- Create a planner that uses them + make the prediction calculations take it into account

[PREDICTIONS:IMPROVEMENTS]
- Give more importance to most recent predictions
- Trying to progressively understand the I/O relationship of the functions instead of using `exponential` function

[EVALUATION:PREPARE]
?? Implement **Dask** planner ?? 
    - just to produce a "Plan" and compare the expected impact of the diff. scheduling decisions for diff. types of workflows

[NEW_OPTIMIZATION?:OUTPUT_STREAMING]
    - As 1 worker starts uploading task output, another worker it immediatelly downloading it, masking download time, since it doesn't have to wait for the upload to complete
    - Possible to do using Redis?
    - BENEFITS
        - Using pubsub to avoid storing intermediate outputs (when applicable) permanently and save download time (would be same as upload time because as 1 byte gets uploaded, it gets downloaded immediatelly)
    - DRAWBACKS
        - Would have to know if the receiver was already active (SYNC required?)
            how:
                - Check if the result of the first task of the receiver worker already **exists** in storage

[OPTIMIZATION:WORKER_STARTUP_SPEED]
- Storing the full dag on redis is costly (DAG retrieval time adds up)
    - If below a certain bytes threshold, pass the subDAG in the invocation itself
    - Worker first checks if the invocation contains the subDAG and if so, doesn't download it from storage
    - Functions with the same name have same code => use a dict[function_name, self.func_code] to save space
    - Also, DAG size is too big for small code and tasks (35kb for image_transform)
    [EXTRA_SAME_SPIRIT] DAGTaskNode.clone() should clone cached_result if below a threshold => then worker checks the DAG received for cached results before downloading from storage

[EVALUATION:AWS_LAMBDA]
Implement Lambda worker and create an automated deployment process

---

- Add decorator for user to provide information about specific functions
    - I/O Relationship (linear or exponential (what exponent) or None)

[VISUALIZATION]
- Update the realtime dashboard to use the same "agrapgh" configuration as the metrics Dashboard
    then, allow clicking on individual tasks to see details (task_id, worker_id, resource config, individual logs)??
    [BUG] (could use the individual task logs to debug this)
    - Sometimes, on some workflows, ALL workers exit and the client doesn't receive the `sink_task_finished_completed` notification
        check if the final result is even produced or if the worker is exiting too early

[VERSATILITY]
- Allow user to specify requirements.txt dependencies so that they don't need to be downloaded at runtime
- At the "workflow language", support more levels: e.g., list[list[DAGTaskNode]]
    Find examples where this makes sense (arg: dict[str, DAGTaskNode])
    Find a better way to iterate through them to avoid repetition