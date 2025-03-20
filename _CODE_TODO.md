- Executors should act on **DAG clones** (If executor spawns multiple coroutines/threads to execute multiple subgraphs in parallel its needed)
    downsides of cloning: can't share cached results directly

- Create another field (metadata_storage) but pass it the same DB while in development
    - Collect metrics (make them persistent in Redis)
        Execution time
        Input data size
        Output data size
        Data download time
        Data upload time
        Visualization capabilities (separate program that creates graphics)

- Implement similar WUKONG algorithms
    Can I wrap `Dask` functions?
    - GEMM
    - Tree Reduction
    - Singular Value Decomposition

- Support more levels: e.g., list[list[DAGTaskNode]]
    Find a better way to iterate through them to avoid repetition
 -Pytest? Test the DAG operations in multiple types of DAGs + DAG partitions actually work

- [NP] Avoid Intermediate storage - sometimes writes can be avoided (leave it up to the worker logic)

- [NP] Find a fix for cyclic dependencies

- [NNP] More WUKONG-specific stuff
    - Task Clustering (fan-ins + fan-outs)
    - Delayed I/O

- [NNP] Redis key to store errors (JSON that stores stderr for each DAG task). Use redis transactions
    Client then should also poll this key periodically
- [NNP] Allow user to specify requirements.txt dependencies so that they don't need to be downloaded at runtime

- [NNP] Web dashboard (Svelte or React) to visualize DAG execution in real-time
    Cisualize Nodes, which worker executed each task
    Complete tasks
    Data transfers (upload download)
    allow replays with a timeline of events