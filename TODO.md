- Measure cold-starts and warm-starts
    - cold-start: time diff. between invoking EXISTING worker and worker starting it's execution
    - warm-start: time diff. between invoking UNEXISTING worker and worker starting it's execution
    - predictions
        - predict_worker_invocation_time(resource_config, "cold" | "warm")
    - metrics required (invoke_time, start_time, state: cold | warm)
        [DONE]
        on lambda
        - static variable initialized at "cold" and set to "warm" when the first worker is invoked

- Add worker_startup_time to pie chart on `workflow_analysis_dashboard`

- Update metadata_access to make predictions about cold and warm starts
- Update abstract dag planner to understand when to add cold/warm starts

- Update worker_startup `workflow_analysis_dashboard`
- Update global_predictions dashboard to show `cold-starts` and `warm-starts` too

- [DIFF_IDEA] A dashboard that simulates diff. planning algorithms for workflow types that were already seen?

---

- [!!] Add support for final result to be None, store a wrapper in storage instead

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
- Give more importance to the most recent predictions
- Discard outliers (is this on the users' side? selecting `percentile` instead of `avg`?)
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