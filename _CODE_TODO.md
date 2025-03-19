- Delegate function should receive argument "resource configuration"!!!!

- [BUG] Cutting behind roots is not enough. Since the other nodes have pointers to "upstream_nodes" (think of a DAG with a fan-out 3-1, the 1 will have pointers to the other 2)
    fix ?: make the upstream_nodes array an array of task_ids
        I need the FULL DATA in order to backtrack in DAG ctor (goes from sink to roots using "upstream_nodes" (confirm this))
            after building the DAG im working with clones, can I convert the upstream stuff then??
                don't use "_find_all_nodes_from_sink", create "_find_all_nodes_from_roots", first node without "downstream_tasks" is sink node?
- [CHECK] Print the complete subDAG (starting from the sink until no more UPSTREAM_NODES) each worker receives to ensure the roots are well cut
- [CHECK] Ensure that separate workers use cached results from other workers ()

- Support more levels: e.g., list[list[DAGTaskNode]]

- Create another storage class (MetadataStorage) but use same DB in development
    - Collect metrics (make them persistent in Redis)
        Execution time
        Input data size
        Output data size
        Data download time
        Data upload time

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