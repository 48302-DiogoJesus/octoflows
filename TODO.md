- PreLoad Annotation
    [DONE] overrides `WEL.on_worker_ready(...)` (called for all fulldag annotations at the end of worker.__init__())
        - `pubsub.subscribe` to the `READY` events for all the `upstream_tasks` from **DIFFERENT** workers
        - on `READY` => download the data from `intermediate_storage` => store it on `.cached_result` + `.completed_event.set()`
        **EXPECTED EFFECT**: worker logic will see the data is cached and won't go to external storage
    [DONE] overrides `WEL.override_handle_inputs(...)`:
        - if `pre-loading` is already happening for an input task (asyncioevent on the annotation itself):
            Download the inputs that are not being `preloaded` + wait for the `preloading` to finish
    
    [DONE] How will the `worker` call the `on_worker_ready()` overrides of the annotations?
        go through all the tasks annotations and execute their `on_worker_ready()`
        check if overriden as before

    [DONE] How will the `worker` call the overriden stuff of the annotations?
        task can have N annotations, each implementing `overrides`
        find the first task that `OVERRIDES` the given stage
        check if overriden as before

- [TODO] RUN auto tests + manual tests on DOCKER WORKERS
- [TEST]
    - Make a planning and check if it is adding preload to any tasks
        (print it on the image)
    - Add logs to see preloading in action
        seeing if preloading was already ongoing, etc... (`simpleplanner.override_handle_inputs()`)

- [TEST] On docker worker logic, don't need to:
    - Sub to READY events (because of the constraints that the fan-out must start the same time + be unbroken)
    - 

- RUN auto tests
- RUN manual tests

- Make `worker_id` optional
    Not at the planner level, just on the "generic code" level

- Refactor/Rethink annotations? Ugly now
    planners just use whatever annotatations they want?

`pre-load` => Implement report 1st algorithm as a NEW algorithm (keep the first one that just does 1 pass and uses no optimizations)

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