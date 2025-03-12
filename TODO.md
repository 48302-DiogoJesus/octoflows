- How to handle user-code dependencies
    How does pywren do it?
    Any way to identify dependencies at runtime + pip install dynamically?

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
    can I wrap `Dask` functions?

- Make the executor run on OpenFaaS