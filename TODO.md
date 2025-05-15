- Think how to implement the `pre-load` optimization
    IMPLEMENTATION: When worker spawns:
        - Look for tasks **assigned to it** + that have `PreLoad` annotation
        for each task:
            - `pubsub.subscribe` to the `READY` events for all the `upstream_tasks` from **DIFFERENT** workers
            - on `READY` => download the data from `intermediate_storage` => store it on `.cached_result` + `.completed_event.set()`
            **EXPECTED EFFECT**: worker logic will see the data is cached and won't go to external storage
    NOTES: If pre-loading is already happening, start_executing() function should wait for the download to complete (use coroutine events)
    EDGE CASES:
        - [POSSIBLE] Preload is happening, but `wel.override_handle_inputs` starts executing
            WHEN IT HAPPENS => 
                - Last fan-in task COMPLETES and emits the READY event/invokes worker with `task_id`
                    pre-load and start_executing will be called at almost the same time
            SOLUTION => Download the inputs that are not being `preloaded` + wait for the `preloading` to finish
    Annotation is added on a task of the worker_id that has upstream tasks to be executed on other workers

`pre-load` => Implement report 1st algorithm as a NEW algorithm (keep the first one that just does 1 pass and uses no optimizations)

- Make `worker_id` optional
    Not at the planner level, just on the "generic code" level

- Think how to isolate annotations
    planners just use whatever annotatations they want?

- Explore Output Streaming
    - BENEFITS
        - Using pubsub to avoid storing intermediate outputs (when applicable) permanently

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