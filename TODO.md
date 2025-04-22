# PLANNING ALGORITHMS
- BUG in predictions (storing normalized time doesn't take input size into account)
    2025-04-22 20:18:15,590 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 500.2587362663083
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 500.2587362663083
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 7754.010412127778
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 500.2587362663083
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 7754.010412127778
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 15007.76208798925
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 500.2587362663083
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 7754.010412127778
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 15007.76208798925
    2025-04-22 20:18:15,591 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 22261.51376385072
    2025-04-22 20:18:15,593 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 500.2587362663083
    2025-04-22 20:18:15,593 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 7754.010412127778
    2025-04-22 20:18:15,593 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 15007.76208798925
    2025-04-22 20:18:15,593 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 22261.51376385072
    2025-04-22 20:18:15,593 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 29515.265439712188
    2025-04-22 20:18:15,593 - src.planning.dag_planner - INFO - Predicted Exec. Time (expect:500): 22258.375255982173
    POSSIBLE PROBLEM SOURCE: OUTPUT SIZE CALCULATION FUNCTION ON THE PLANNER SCRIPT
- BUG in normalization ?
    im only running using the same config (medium (NOT BASELINE))
    test with FIXED TIME DAG
- Problem: Test Planning accuracy (is the predicted time close to the real time?)
    1. Create a DAG that actually does expensive computation that runs faster on better memory
        Adjust `dagforpredictions_expensive_computations.py`
    2. [DONE] Change planner to just use middle configuration so that predictions algorithm doesn't influence too much

- Test the SimplePlanner algorithm: make a run without the planning algorithm and make another with the planning algorithm, then compare
- Make the SLA configurable by the user (currently it's hardcoded on `dag.py` as "avg")

- IMPROVEMENTS
    - Make predictions faster (not too important for now)
        - Threadpool
        - Incremental Critical Path Calculation => Instead of recalculating the entire DAG for each change, develop an algorithm that incrementally updates affected paths
        - ? Don't simulate for ALL nodes. establish a max tries and select specific paths

- Report 1st algorithm (requires: pre-load optimization implemented)

# WORKER
Further abstract the worker
- grab_dependencies (allow algorithm to use threads or coroutines to download input, only download some input and get other input locally or from another source)
- execute (allow algorithm to do other things before, after and while task is executing or completely skip execution)
- handle_output (allow algorithm to not upload output, upload lazilly, etc..)

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