- [DONE] `MetadataAccess` grab the cached metrics on the ctor
- [DONE] Store metrics by namespace (Redis key format) (change dag id format <time>_<sink_name>_<uuid>_<dag_signature>)
- [DONE] Normalize TIME of metrics collection by memory using a baseline memory (512mb)
    - kept previous times (real times)
    - added normalized fields for "task execution times" and "data transfer times"

- Implement the first **Planning** algorithm
    - ISSUE: It takes too long to simulate for ALL nodes
        Make predictions faster
            Predictions take a lot longer with more data (e.g., T.R. 512 vs 256)
            - Threadpool
            - Incremental Critical Path Calculation => Instead of recalculating the entire DAG for each change, develop an algorithm that incrementally updates affected paths
        Use thread pool
        Don't simulate for ALL nodes. establish a max tries and select specific paths
    - Grabbing redis metrics takes too long (see what's possible)
    - BUG: In tree reduction, all workers are being assigned the same worker configuration
    - BUG?: Critical path completion time is too low! Doesn't seem correct
    - BUG?: If prediction returns None I shouldn't default to 0! It will influence algorithm wrongly!
        solution: discard? how?
    - Finish new algorithm and describe it as a comment
        best resources to all
        try downgrading resources on all tasks as much as possible without introducing a new critical path
    - Convert json structure to a dataclass for typesafety and performance
    - How to test the algorithm?
        data ?
        correctness ?
    - Make the SLA configurable by the user (currently it's hardcoded on `dag.py` as "avg")
    - Pre-defined data structures/functions to facilitate creating algorithms?
    - Report 1st algorithm (requires: pre-load optimization implemented)
    - IMPROVEMENTS
        - use `from pympler import asizeof` to calculate data structure sizes (pickle returns the size of them when serialized, not the real in-memory size)
        - not correct => `return sum(len(pickle.dumps(arg)) for arg in node.func_args) + sum(len(str(k)) + len(str(v)) for k, v in node.func_kwargs.items())`
            doesn't account for Pointers to other `dagtasknodes` (negligible?)

- [REFACTOR]
    - If serialized DAG size is below a threshold (passed on WorkerConfig, pass it on the invocation)
        store_full_dag DOESN'T change
        NEW => workers MAY not need to download dag from storage
            serialized_dag: SubDAG | None

- Think about "namespaces for Task Annotations per algorithm": <ALGORITHM_NAME>_PRELOAD ??
    - how to make workers follow annotations in a scalable manner?
        Annotations become classes that override parts of the worker code?
        Split the worker logic into sections that are implemented by the planner (planner becomes mandatory)
            The planner I have now would be WukongPlanner?

- Remove intermediate results of a dag after complete (sink task is responsible for this)
    redis remove keys that contain <master_dag_id>
- Parallelize **dependency grabbing** and **dependency counter updates** with Threads, for now
- Further separate Worker configs (it's a mess to know which props are required for each worker)

- [PERFORMANCE] Storing the full dag on redis is costly
    - Don't store the whole DAG (figure out how to partition DAG in a way that is correct)
    - If below a certain bytes threshold, pass the subDAG in the invocation itself
    - Also, DAG size is too big for small code and tasks (35kb for image_transform)

- [NNP] [PERFORMANCE] Make the parallelized **dependency grabbing** and **dependency counter updates** use coroutines + async redis instead of Threads
    NOTE: I tried it, but redis server was crashing when i used asyncredis library
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
