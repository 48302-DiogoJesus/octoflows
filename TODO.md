- Make Storage require the pubsub operations + implement them on the inmemory implementation
    or separate into: Storage, StorageWithPubSub

- Use pubsub for notifying that the final result is ready
    Change code on the override_handle_output()
        ```python
            # Worker
            upload to a normal redis key
            if final_result: upload to pubsub (key: `master_dag_id`)
        ```
        ```python
            # Client
            sub to pubsub `master_dag_id`
            when callback is called => get the sinknode key
        ```
    (tuple: final result and timestamp of finish to allow calculating more accurate makespan from client POV?)

- Think how to implement the `pre-load` optimisation
- Implement `pre-load` optimization
    => Implement report 1st algorithm as a NEW algorithm (keep the first one that just does 1 pass and uses no optimizations)

# PLANNING ALGORITHMS
- Make the SLA configurable by the user (currently it's hardcoded on `dag.py` as "avg")
- Dashboard makespan (9 sec) VS client console (5 sec) completion time big diff.
- 1 second diff. between `planned time` and `real time`

- [REFACTOR]
    - If serialized DAG size is below a threshold (passed on WorkerConfig, pass it on the invocation)
        store_full_dag DOESN'T change
        NEW => workers MAY not need to download dag from storage
            serialized_dag: SubDAG | None

- Remove intermediate results of a dag after complete (sink task is responsible for this)
    redis remove keys that contain <master_dag_id>
- Parallelize **dependency grabbing** and **dependency counter updates** with Threads, for now
- Further separate Worker configs (it's a mess to know which props are required for each worker)

- [PERFORMANCE] 
    - Storing the full dag on redis is costly (DAG retrieval time adds up)
        - Don't store the whole DAG (figure out how to partition DAG in a way that is correct)
        - If below a certain bytes threshold, pass the subDAG in the invocation itself
        - Also, DAG size is too big for small code and tasks (35kb for image_transform)
        - Functions with the same name have same code => use a dict[function_name, self.func_code] to save space
    - Parallelize **dependency grabbing** and **dependency counter updates** (easy, since storage is async now)
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