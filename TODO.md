# PLANNING ALGORITHMS
- 1. Experiment planning with diverse resources for `dagforpredictions_expensive_computations.py`
    Compare `planned time` VS `real time`
- Problem ?: Dashboard makespan VS client console completion time big diff.
- Make the SLA configurable by the user (currently it's hardcoded on `dag.py` as "avg")
- 1 second diff. between planned time and real time

# WORKER
- 2. Plan the worker logic refactor
Pieces: Worker, SchedulingLogic tied to a Planner (1 => N, there can be 1 Scheduling Logic for N Planner algorithms that use the same optimizations. The SchL determines the optimizations available)
    class SchedulingLogic
        - virtual methods: handle_inputs, handle_execution, handle_output, handle_downstream
            their default implementations are the current implementation on worker.start_executing()
    class Planner
        - Internally points to a specialized SchedulingLogic.Annotations/Optimizations
    class Worker
        - 
Further abstract the worker
- grab_dependencies (allow algorithm to use threads or coroutines to download input, only download some input and get other input locally or from another source)
- execute (allow algorithm to do other things before, after and while task is executing or completely skip execution)
- handle_downstream (allow algorithm to not upload output, upload lazilly, etc..)

- Implement pre-load optimization
    - Implement report 1st algorithm as a NEW algorithm (keep the first one that just does 1 pass and uses no optimizations)

- [REFACTOR]
    - If serialized DAG size is below a threshold (passed on WorkerConfig, pass it on the invocation)
        store_full_dag DOESN'T change
        NEW => workers MAY not need to download dag from storage
            serialized_dag: SubDAG | None

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