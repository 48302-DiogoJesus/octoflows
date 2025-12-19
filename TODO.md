Test Setup
    - M1
        Client
        DBs
        Gateway (Inside Vagrant)
        Tunnel Docker API from M2
    - M2
        Gateway (Inside Vagrant)

- NEW RESOURCE USAGE CALCULATION MIGRATION
    - run experiments with old system but getting new metrics
    - check on dashboard if the new metrics collected are equivallent/proportional
    - if so, remove the old system + update dashboards

- Run experiments script 2 of each planner and workflow

- Split experiments into 3 executions, backup the rdb files after each, in case we need to go back

- Update `Deployment.md` to also include instructions/mention for Vagrant
- Update paper with graphics
- Update paper with experiment parameters, configuration, and deployment

[KNOWN_ISSUES]
- In simulation, {worker_active_periods} (`abstract_dag_planner.py`) are not being calculated accuratelly
