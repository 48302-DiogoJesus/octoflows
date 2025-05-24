[GENERIC] Make `worker_id` optional to support "uniform workers" planner?
    On the "generic code" level (support worker id being `None` (STRONGLY TYPED, not -1))
        The default WEL can use similar logic to WUKONG to delegate tasks
        Would this support preload?
    re-test

[ISSUE]
- With uniform planner => prediction is a lot more accurate, but above
- With simple planner => prediction is too low
    [=>] Implement 2nd planner from report => 3rd (keep the simple planner)
        re-simulate w/ **downgrading resources**

[PLANNER] Implement 2nd report algoritm

[PLANNER_OPTIMIZATION] Explore Output Streaming
    - BENEFITS
        - Using pubsub to avoid storing intermediate outputs (when applicable) permanently

[PLANNING_ALGORITHMS] [!!] Store the plan on storage so that the "Metadata Analysis" dashboard can compare the real results to the plan and draw conclusions

[OPTIMIZATION:DATA_ACCESS]
TRANSACTION/PIPE REDIS OPERATIONS DONE TO THE SAME STORAGE
- Publishing TASK_READY events
- Downloading input from storage
- Uploading final metrics
- Realtime dashboard
[OPTIMIZATION:WORKER_STARTUP_SPEED]
- Storing the full dag on redis is costly (DAG retrieval time adds up)
    - Don't store the whole DAG (figure out how to partition DAG in a way that is correct)
    - If below a certain bytes threshold, pass the subDAG in the invocation itself
        DAGTaskNode.clone() should clone cached_result if below a threshold => then worker checks the DAG received for cached results before downloading from storage
    - Also, DAG size is too big for small code and tasks (35kb for image_transform)
    - Functions with the same name have same code => use a dict[function_name, self.func_code] to save space
[OPTIMIZATION:STORAGE_USAGE] Task output doesn't always need to go to intermediate storage
[OPTIMIZATION:STORAGE_CLEANUP] Remove intermediate results of a dag after complete (sink task is responsible for this)
    impl => remove storage keys that contain <master_dag_id>

[PLANNING_ALGORITHMS] Dashboard makespan (5 sec) VS client console (9 sec) time big diff.

[EVALUATION:PREPARE]
- Implement **WUKONG** planner
    + optimizations
        - Task Clustering (fan-ins + fan-outs)
        - Delayed I/O
- Implement **Dask** planner
    - just to produce a "Plan" and compare the expected impact of the diff. scheduling decisions for diff. types of workflows

[VISUALIZATION]
- Update the realtime dashboard to use the same "agrapgh" configuration as the metrics Dashboard
    then, allow clicking on individual tasks to see details (task_id, worker_id, resource config)??

[ERROR_HANDLING]
- Add retry mechanisms
    - On a task level (logic inside a worker)
    - On a worker level (how to report this error if we don't have a centralized Job queue?)
- Redis key to store errors (JSON that stores stderr for each DAG task). Use redis transactions
    Client then should also poll this key periodically

[VERSATILITY]
- Allow user to specify requirements.txt dependencies so that they don't need to be downloaded at runtime
- At the "workflow language", support more levels: e.g., list[list[DAGTaskNode]]
    Find examples where this makes sense (arg: dict[str, DAGTaskNode])
    Find a better way to iterate through them to avoid repetition
- Support for dynamic fan-outs w/ special nodes (re-think implementation to allow chains of dynamic fan-out nodes)

[ROBUSTNESS] Create more tests for more complex and edge case DAG structures + DAG compute tests
