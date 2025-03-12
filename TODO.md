- Support more levels: e.g., list[list[DAGTaskNode]]

- Collect metrics (make them persistent in Redis)
    Execution time
    Input data size
    Output data size
    Data download time
    Data upload time
- Create another storage class (MetadataStorage) but use same DB in development

- More WUKONG-specific stuff
    - Task Clustering (fan-ins + fan-outs)
    - Delayed I/O

- Implement similar WUKONG algorithms
    Can I wrap `Dask` functions?
    - GEMM
    - Tree Reduction
    - Singular Value Decomposition

- Make the executor run on OpenFaaS