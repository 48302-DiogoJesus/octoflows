- Run all tests on Docker
- Implement SVD1 and SVD2
- Make compute() return a Future? so that users can execute multiple independent workflows in parallel and grab their results
    can test on wordcount, image_transform, gemmm

- Can I currently support a workflow where image is divided into fixed chunks, MULTIPLE transformations (separate "DAGTasks") are applied to each branch and then fan-in to build final image

- Create more tests for more complex and edge case DAG structures + DAG compute tests
- Add tests for asserting node count and final result of new workflows: tree reduction, gemm, wordcount, image_transform, svd

- Create another field (metadata_storage) but pass it the same DB while in development
    - Collect metrics (make them persistent in Redis)
        Execution time
        Input data size
        Output data size
        Data download time
        Data upload time
        Which worker id executed it
        Visualization capabilities (separate program that creates graphics)
        Upload them efficiently

## Performance Optimizations
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