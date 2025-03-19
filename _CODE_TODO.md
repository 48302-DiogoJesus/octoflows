- Delegate function should receive argument "resource configuration"!!!!

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