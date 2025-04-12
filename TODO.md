- Normalize TIME of metrics collection by memory using a baseline memory (512mb for example)
    - e.g., task.normalized_execution_time = exec_time * (memory_used/512)
        - do same for data transfer times
    !! keep the existing field !! add a new one prefixed "normalized_"
    only use the "normalized_" for predictions and the other for statistics (makespan, time spent doing X)
- Namespaces for Task Annotations per algorithm: ALGORITHM_NAME_PRELOAD
    - how to make workers follow annotations in a scalable manner?
        Annotations become classes that override parts of the worker code?

- [DONE] `MetadataAccess` grab the cached metrics on the ctor
- [DONE] Store metrics by namespace (Redis key format) (change dag id format <time>_<sink_name>_<uuid>_<dag_signature>)

- [REFACTOR] 
    - Clearer separation between a fulldag and a subdag
    - Create more custom exceptions for DAG structure

- Implement basic **Planning** algorithm
    See my report for the algorithm insight
        simulate best resources on all tasks
        find critical path
        alleviate resources on tasks outside the critical path
            re-simulate to ensure the critical path is still the same
            do this N amount of times

- Remove intermediate results of a dag after complete (sink task is responsible for this)
- Parallelize **dependency grabbing** and **dependency counter updates** with Threads, for now

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
