- Implement SVD1 and SVD2?

- Make compute() return a Future? so that users can execute multiple independent workflows in parallel and grab their results
    can test on wordcount, image_transform, gemmm

- Create more tests for more complex and edge case DAG structures + DAG compute tests
- Add tests for asserting node count and final result of new workflows: tree reduction, gemm, wordcount, image_transform, svd

- Change the worker_config.metadata_storage to `MetadataStorage(Storage)`
    - Collect metrics as Events (make them PERSISTENT in Redis):
        TaskExecutionMetrics(worker_id, execution_time, input_data_size, output_data_size, input_download_time, output_upload_size)
        Visualization capabilities (separate program that creates graphics)
        Metrics upload strategy configurable in the `MetadataStorage` class:
            before worker shutdown (all do these)
            after each task
            periodic (X seconds)
            after queue fills up (X queue size)
- A way to analyse important metrics from stored data. Separate python program that reads redis and creates dashboard

## Performance Optimizations (after MetadataStorage metrics can be analyzed)
- See where it's suitable to use storage in an async way (make the storage implementations offer "sync" and "async" functions)
    - Parallelize
- Local implementation is slower than Dasks (e.g., Tree Reduction): could hint at some internal structure inneficiencies
- Local implementation could use threads instead of coroutines? (test to check concurrency errors)

- Support more levels: e.g., list[list[DAGTaskNode]]
    Find a better way to iterate through them to avoid repetition

- [NNP] Implement WUKONG-specific optimizations
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