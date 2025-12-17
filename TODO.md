Test Setup
    - Vitamina02 (10.15.0.22) 
        Client
        DBs
        Gateway (Inside Vagrant)
        Tunnel Docker API from 10.15.0.14
    - Proteina04 (10.15.0.14) 
        Gateway (Inside Vagrant)

- Implement:
    - Client does balanced delegation OR random if easier to implement (!!use the hashcode of the task_id to choose the worker!!)
    - Workers always talk to their own gateway
- How to make Prewarm useful? (avoid prewarming a container on a gateway that won't be the one used)
    - Use the hashcode of the task_id to choose the worker
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
