- Change the worker_config.metadata_storage to `MetricsStorage(Storage)`
        TaskExecutionMetrics(task_id, worker_id, execution_time, input_data_size, output_data_size, input_download_time, output_upload_size)
        [TODO] Store DAGTaskNodeId instance (to preserve func name etc..) instead of String
        [TODO] Metrics upload strategy configurable in the `MetricsStorage` class:
            before worker shutdown (all do these)
            after each task
            periodic (X seconds)
            after queue fills up (X queue size)
        [TODO] A way to analyse important metrics from stored data. Separate python program that reads redis and creates dashboard

- Make all examples use MetricsStorage
- Remove intermediate results of a dag after complete (last task is responsible for this)
- Create more tests for more complex and edge case DAG structures + DAG compute tests
- Add tests for asserting node count and final result of new workflows: tree reduction, gemm, wordcount, image_transform, svd

## Performance Optimizations (after MetadataStorage metrics can be analyzed)
- See where it's suitable to use storage in an async way (make the storage implementations offer "sync" and "async" functions)
    - Parallelize
- Local implementation is slower than Dasks (e.g., Tree Reduction): could hint at some internal structure inneficiencies
- Local implementation could use threads instead of coroutines? (test to check concurrency errors)
- Parallelize {fanout invocations} (don't wait for previous invocation to succeed)
- Parallelize dependency grabbing

- [NP] Support more levels: e.g., list[list[DAGTaskNode]]
    Find examples where this makes sense (arg: dict[str, DAGTaskNode])
    Find a better way to iterate through them to avoid repetition

- [NP] Support for dynamic fan-outs?

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