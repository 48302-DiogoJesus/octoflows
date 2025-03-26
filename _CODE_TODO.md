- Tree reduction 512 can't be sent to container Docker. Argument too long
    Send the entire dag to Storage and the invocation only contains the task id where the worker should start from

- Add support for a DAGTaskNode to be a DAG itself (handle DAG completion differently?)
    - Is this necessary to allow Dynamic multi-level fan-outs? 1 - N (each of {N} fans-out to {X} or even to {1}) [currently_not_supported]

- BUG: After removing a fake sink node, go back to its upstream nodes and check them after this removal
    could be a chain of tasks that don't endup in the sink
    maybe change algorithm to check paths instead of individual nodes (if a path doesn't end up at the sink node, delete it)
    INSTEAD OF REMOVING THEM. THROW ERROR AT CLIENT!

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
- Create more tests for more complex and edge case DAG structures + DAG compute tests

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