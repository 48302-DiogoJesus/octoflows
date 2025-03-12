- Implement RemoteWorker (similar to VirtualWorker) but instead of invoking new asyncio task, invokes new remote worker (how to handle finishing?)

- More WUKONG-specific stuff
    - delayed I/O

- Collect metrics (make them persistent in Redis)
    Execution time
    Input data size
    Output data size
    Data download time
    Data upload time
- Create another storage class (MetadataStorage) but use same DB in development

- Implement similar WUKONG algorithms
    can I wrap `Dask` functions?

- Make the executor run on OpenFaaS