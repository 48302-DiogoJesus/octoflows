Test Setup
    - M1
        Client
        DBs
        Gateway (Inside Vagrant)
        Tunnel Docker API from M2
    - M2
        Gateway (Inside Vagrant)

- NEW RESOURCE USAGE CALCULATION MIGRATION
    - Update dashboard to measure prewarms with ne metrics

- Run experiments script 2 of each planner and workflow

- Split experiments into 3 executions, backup the rdb files after each, in case we need to go back

- Update `Deployment.md` to also include instructions/mention for Vagrant + remove mention of Docker API
- Update paper with graphics
- Update paper with experiment parameters, configuration, and deployment

[KNOWN_ISSUES]
- In simulation, {worker_active_periods} (`abstract_dag_planner.py`) are not being calculated accuratelly
