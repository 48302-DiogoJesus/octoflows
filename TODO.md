- Use Windsurf to refactor dashboard code

- Parallelize **dependency grabbing** and **dependency counter updates** with Threads, for now

- Metrics Digestion
    - what types of things to store? (percentiles, averages, median)
    - research incremental updates to them (percentiles, averages, median)

- Metrics upload strategy configurable in the `MetricsStorage` class:
    before worker shutdown (all do these)
    after each task
    periodic (X seconds)
    after queue fills up (X queue size)

- Make all examples use MetricsStorage
- Remove intermediate results of a dag after complete (sink task is responsible for this)

- [NP] [PERFORMANCE] Make the parallelized **dependency grabbing** and **dependency counter updates** use coroutines + async redis instead of Threads
    NOTE: I tried it, but redis server was crashing when i used asyncredis library

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
