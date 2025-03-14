- [P] Deploy on Docker locally (Flask Web Server constrained to executing 1 Executor at a time, throw Exception "Server Busy" if receives reqs while executing)
    - 2 Diff. Configurations
    - When the client scheduler receives an exception ("Server not available" OR "Server busy"), it uses docker api (python lib?) to create another container with the same resources 
- Research cgroup (avoids Docker, but requires Linux)
- Deploy on OpenWhisk ? (less generic than Docker)

- Implement similar WUKONG algorithms
    Can I wrap `Dask` functions?
    - GEMM
    - Tree Reduction
    - Singular Value Decomposition
