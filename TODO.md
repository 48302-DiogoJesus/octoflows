- [REQUIRED_FOR_PRELOAD] Planner should also define the worker_id, not just worker resources? without it we don't guarantee locality + we don't know locality
    [DONE] New optional parameter on worker config. annotation `worker_id`
    BUG: I/O of some tasks is showing 0-0 on the planning image
    Fix the simulation
        Change `_calculate_node_timings_with_custom_resources` to ignore times:
            [DONE] upload time if   => all downstream tasks have the same `worker_id` as `this task`
            [DONE] download time if => all upstream tasks have the same `worker_id` as `this task`
                not correct, because can have N inputs, some from the same worker, some from other workers
                    FIX: change return of `_calculate_input_size` to not merge the input size, but instead group `input_size` per `worker_id`
        Worker Config Upgrade Changes
            [DONE] Simulate for NEW workers of ALL configs
            [DONE] Simulate for SAME workers of the upstream tasks
        How to do the initial/reference simulation (`_calculate_node_timings_with_common_resources`)??
            baseline: give best resources to all tasks, then downgrade
            new/required: give best resources to all tasks

    How to USE annotation `worker_id` (worker pov)
        Delegating
            Before executing its first task, it finds its own `worker_id`
            When delegating
                group the tasks by `worker_id`
                foreach task in `my_worker_id_group` asyncio.create_task(task)
                Delegate the rest accordingly
                    Add support for delegating a **list of subdags**
                    Add support for workers to **receive and execute a list of subdags** (separate coroutines)
                    Test it (artificially influence planning)
    Add a validation step after planning to ensure that equal `worker_ids` have the same resource configuration

- Think how to implement the `pre-load` optimization
    - What is `pre-load` ?: worker which is already active can start downloading ready dependencies it will need in the future
    - When ?: Annotation `pre-load` (means that worker should TRY (listen for pubsub IF NOT ALREADY) to `pre-load` dependencies for ITS future tasks)
        If pre-loading is already happening, start_executing() function should wait for the download to complete (use coroutine events)
    - How ?:
        Before starting a task, check if the `pre-load` annotation exists. If so, look at the `upstream_tasks` of the `downstream_tasks` and listen for pubsub events
    - [Optimization] to avoid sending pubsub msgs for every task completion
        When a task completes, go to the `upstream_tasks` of the `downstream_tasks` and only if at least one of those has the `pre-load` annotation, send pubsub event
    
- Implement `pre-load` optimization
    => Implement report 1st algorithm as a NEW algorithm (keep the first one that just does 1 pass and uses no optimizations)
- Experiment with Output Streaming
    - BENEFITS
        - Using pubsub to avoid storing intermediate outputs (when applicable) permanently
        
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