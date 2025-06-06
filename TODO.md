[OPTIMIZATION:STORAGE_USAGE] Task output doesn't always need to go to intermediate storage
    
[OPTIMIZATION:WORKER_STARTUP_SPEED]
- Storing the full dag on redis is costly (DAG retrieval time adds up)
    - If below a certain bytes threshold, pass the subDAG in the invocation itself
    - Worker first checks if the invocation contains the subDAG and if so, doesn't download it from storage
    - Functions with the same name have same code => use a dict[function_name, self.func_code] to save space
    - Also, DAG size is too big for small code and tasks (35kb for image_transform)
    [EXTRA_SAME_SPIRIT] DAGTaskNode.clone() should clone cached_result if below a threshold => then worker checks the DAG received for cached results before downloading from storage

[EVALUATION:PREPARE]
- Implement **WUKONG** planner
    + optimizations
        - Task Clustering (fan-ins + fan-outs)
        - Delayed I/O

---

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

[REFACTOR]
- Should the faas worker handler logic (`docker_worker_handler/worker.py`) be encapsulated in a class (e.g., on existing `Worker` class ?)
    ! only if it's generic enough that it would also work on `AWS Lambda` workers for example. Otherwise keep it separate.
    - new function would be called `Worker.start_worker_lifecycle`

[EVALUATION:PREPARE]
?? Implement **Dask** planner ?? 
    - just to produce a "Plan" and compare the expected impact of the diff. scheduling decisions for diff. types of workflows

[NEW_OPTIMIZATION?:OUTPUT_STREAMING]
    - As 1 worker starts uploading task output, another worker it immediatelly downloading it, masking download time, since it doesn't have to wait for the upload to complete
    - Possible to do using Redis?
    - BENEFITS
        - Using pubsub to avoid storing intermediate outputs (when applicable) permanently and save download time (would be same as upload time because as 1 byte gets uploaded, it gets downloaded immediatelly)
    - DRAWBACKS
        - Would have to know if the receiver was already active (SYNC point?)
            how:
                - Check if the result of the first task of the receiver worker already **exists** in storage

[VISUALIZATION]
- Update the realtime dashboard to use the same "agrapgh" configuration as the metrics Dashboard
    then, allow clicking on individual tasks to see details (task_id, worker_id, resource config, individual logs)??
    [BUG] (could use the individual task logs to debug this)
    - Sometimes, on some workflows, ALL workers exit and the client doesn't receive the `sink_task_finished_completed` notification
        check if the final result is even produced or if the worker is exiting too early

[OPTIMIZATION:PLANNING]
- When predicting normalized stuff, and we don't have enough samples yet, try to use the samples of the closest resource config  (up or odnw)

---

[VERSATILITY]
- Allow user to specify requirements.txt dependencies so that they don't need to be downloaded at runtime
- At the "workflow language", support more levels: e.g., list[list[DAGTaskNode]]
    Find examples where this makes sense (arg: dict[str, DAGTaskNode])
    Find a better way to iterate through them to avoid repetition
- Support for dynamic fan-outs w/ special nodes (re-think implementation to allow chains of dynamic fan-out nodes)

[ROBUSTNESS] Create more tests for more complex and edge case DAG structures + DAG compute tests
