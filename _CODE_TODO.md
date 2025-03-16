- Currently Not calling multiple containers (only 1)!
    Create a workflow with bigger fan-outs
- Kill containers without requests for 5 seconds
- PERFORMANCE: Client code too slow to invoke remote
    building the dag is slow?

- Rename `FlaskExecutor` to `WebServerExecutor`
- Improve user interface (user should instantiate a `LocalExecutor.Configure() -> LocalExecutorConfiguration` and when calls compute, pass the LocalExecutorConfiguration which is used to instantiate then)
- `.Configure(intermediate_storage_config: redis_hostname, etc.. for now)`

- Avoid Redis where unneded
- Avoid Redis on LocalExecutor (base abstract `ExternalStorage` Class (`InMemoryStorage`, `RedisStorage`))

- Support more levels: e.g., list[list[DAGTaskNode]]

- Create another storage class (MetadataStorage) but use same DB in development
    - Collect metrics (make them persistent in Redis)
        Execution time
        Input data size
        Output data size
        Data download time
        Data upload time

- [NNP] Web dashboard (Svelte or React) to visualize DAG execution in real-time
    Cisualize Nodes, which worker executed each task
    Complete tasks
    Data transfers (upload download)
    allow replays with a timeline of events

- [NNP] More WUKONG-specific stuff
    - Task Clustering (fan-ins + fan-outs)
    - Delayed I/O

- [NNP] Allow user to specify requirements.txt dependencies so that they don't need to be downloaded at runtime