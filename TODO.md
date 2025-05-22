- resposta à pergunta do prof:
    Q: "As runs que usam otimizações produzem metadados diferentes das runs que não as usam"
    R: acho que não, porque ...

- Refactor the realtime dashboard to use COMPLETE events instead of polling (faster and more efficient)

[ISSUE]
- With uniform planner => prediction is a lot more accurate, but above
- With simple planner => prediction is too low
    [=>] Implement 2nd planner from report => 3rd (keep the simple planner)
        re-simulate w/ **downgrading resources**

- Make `worker_id` optional to support "uniform workers" planner?
    Not at the planner level, just on the "generic code" level

- Explore Output Streaming
    - BENEFITS
        - Using pubsub to avoid storing intermediate outputs (when applicable) permanently

- [ALGORITHM_OPTIMIZATION] Change the criteria to applying the `preload` optimization?
    from: all tasks that have > 1 upstream task from another worker
    to: All tasks that have > 1 upstream task from another worker **AND** only upstream tasks that are *expected* to finish at least `X ms earlier` than the others
        [REQUIRES] Update the semantics of `PreLoad` to also allow specifying which upstream tasks to preload
            e.g., node.add_annotation(PreLoad(preload=[t1, t2, t3]))

[OPTIMIZATION]
TRANSACTION/PIPE REDIS OPERATIONS DONE TO THE SAME STORAGE
- Publishing TASK_READY events
- Downloading input from storage

# PLANNING ALGORITHMS
- Dashboard makespan (9 sec) VS client console (5 sec) time big diff.
- 1 second diff. between `planned time` and `real time`
- Planning times don't consider cold starts meaning that changing workers is not penalized

- [NP_FIX] realtime dashboard

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