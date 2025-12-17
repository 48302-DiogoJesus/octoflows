Test Setup
    - Vitamina02 (10.15.0.22) 
        Client
        DBs
        Gateway (Inside Vagrant)
        Tunnel Docker API from 10.15.0.14
    - Proteina04 (10.15.0.14) 
        Gateway (Inside Vagrant)

- IMPROVEMENT: Check if need to use calculate_data_structure_size_bytes vs len since I only use it on serialized stuff
- How to make Prewarm useful? 
    (avoid prewarming a container on a gateway that won't be the one used)
    (if a worker prewarms a container on gateway X, the delegate() call should also go to gateway X)
    solution
    - Use the hashcode of the worker_id. Delegate and PreWarm of the worker_id should result in the same gateway being chosen
- Run experiments script 2 of each planner and workflow
- Check if viz make sense 
parameters:
    resource configs
    cold start time
    fanout max clustering

- Split experiments into 3 executions, backup the rdb files after each, in case we need to go back

- Update `Deployment.md` to also include instructions/mention for Vagrant
- Update paper with graphics
- Update paper with experiment parameters, configuration, and deployment

[KNOWN_ISSUES]
- In simulation, {worker_active_periods} (`abstract_dag_planner.py`) are not being calculated accuratelly
