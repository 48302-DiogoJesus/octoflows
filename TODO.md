- What is the problem?
    Allowing arbitrary worker_ids to tasks can lead to workers having to wait for the completion of tasks instead of being invoked

- Stricter requirement for now: worker_ids must be in UNBROKEN chains + have a single origin point (node that fans-out to them)

- I think the validator allows branching later on while the other branches haven't been killed 
    (Issue for knowing if a worker is already active or not?)
- The validator ensures that the worker_id branches start at the same time. Use algorithm to know if the worker with id = `worker_id` is already active

- When worker spawns, it should check storage (`storage.exists`) because it may spawn late and NOT receive the READY events

- Run on docker multiple times
    docker execution is failing
        what ID is in the container name? worker_id?? can't find it
        N? workers don't finish, doesn't upload metadata (only happens sometimes)
        produce planning image for all SIMPLE PLANS
        understand if they are subbing to the right task readyness
        understand where it's stuck
- Issue:
    `worker_id` => when we are delegating, how to know if the `worker_id` is already active?

- Think how to implement the `pre-load` optimization
    - What is `pre-load` ?: worker which is already active can start downloading ready dependencies it will need in the future
    - When ?: Annotation `pre-load` (means that worker should TRY (listen for pubsub IF NOT ALREADY) to `pre-load` dependencies for ITS future tasks)
        If pre-loading is already happening, start_executing() function should wait for the download to complete (use coroutine events)
    - How ?:
        Before starting a task, check if the `pre-load` annotation exists. If so, look at the `downstream_tasks.upstream_tasks` and listen for pubsub events
    - [Optimization] to avoid sending pubsub msgs for every task completion
        When a task completes, go to the `upstream_tasks` of the `downstream_tasks` and only if at least one of those has the `pre-load` annotation, send pubsub event

`pre-load` => Implement report 1st algorithm as a NEW algorithm (keep the first one that just does 1 pass and uses no optimizations)
- Explore Output Streaming
    - BENEFITS
        - Using pubsub to avoid storing intermediate outputs (when applicable) permanently

- Make `worker_id` optional

[OPTIMIZATION]
TRANSACTION/PIPE REDIS OPERATIONS DONE TO THE SAME STORAGE
- Publishing TASK_READY events
- Downloading input from storage
        
# PLANNING ALGORITHMS
- Dashboard makespan (9 sec) VS client console (5 sec) completion time big diff.
- 1 second diff. between `planned time` and `real time`
- Planning times don't consider cold starts meaning that changing workers is not penalized

- [REFACTOR]
    - If serialized DAG size is below a threshold (passed on WorkerConfig, pass it on the invocation)
        store_full_dag DOESN'T change
        NEW => workers MAY not need to download dag from storage
            serialized_dag: SubDAG | None

- Remove intermediate results of a dag after complete (sink task is responsible for this)
    redis remove keys that contain <master_dag_id>
- Further separate Worker configs (it's a mess to know which props are required for each worker)

- [PERFORMANCE] 
    - Storing the full dag on redis is costly (DAG retrieval time adds up)
        - Don't store the whole DAG (figure out how to partition DAG in a way that is correct)
        - If below a certain bytes threshold, pass the subDAG in the invocation itself
            DAGTaskNode.clone() should clone cached_result if below a threshold => then worker checks the DAG received for cached results before downloading from storage
        - Also, DAG size is too big for small code and tasks (35kb for image_transform)
        - Functions with the same name have same code => use a dict[function_name, self.func_code] to save space
    - Task output doesn't always need to go to intermediate storage

- Metrics upload strategy configurable in the `MetricsStorage` class:
    before worker shutdown (all do these)
    after each task
    periodic (X seconds)
    after queue fills up (X queue size)

# Evaluation
- [NNP] Implement WUKONG-specific optimizations
    - Task Clustering (fan-ins + fan-outs)
    - Delayed I/O

# Visualization
- [NNP] Update the realtime graph visualization using the same "agrapgh" configuration as the metrics Dashboard

# Error handling
- [NP] Add retry mechanisms
    - On a task level (logic inside a worker)
    - On a worker level (how to report this error if we don't have a centralized Job queue?)
- [NNP] Redis key to store errors (JSON that stores stderr for each DAG task). Use redis transactions
    Client then should also poll this key periodically

# Efficiency/Usability
- [NNP] Support for dynamic fan-outs w/ special nodes (re-think implementation to allow chains of dynamic fan-out nodes)
- [NNP] Allow user to specify requirements.txt dependencies so that they don't need to be downloaded at runtime
- [NP] Support more levels: e.g., list[list[DAGTaskNode]]
    Find examples where this makes sense (arg: dict[str, DAGTaskNode])
    Find a better way to iterate through them to avoid repetition

# Testing
- Create more tests for more complex and edge case DAG structures + DAG compute tests