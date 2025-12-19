Test Setup
    - M1
        Client
        DBs
        Gateway (Inside Vagrant)
        Tunnel Docker API from M2
    - M2
        Gateway (Inside Vagrant)

- Issue: too many failed prewarms
- Check new resource calculation formula
- Prewarm not effective? adjust parameters and try again?

- Run experiments script 2 of each planner and workflow
- 1 downgrade simulation takes around 5 seconds for 60 nodes DAGs

- Split experiments into 3 executions, backup the rdb files after each, in case we need to go back

- Update `Deployment.md` to also include instructions/mention for Vagrant + remove mention of Docker API
- Update paper with graphics
- Update paper with experiment parameters, configuration, and deployment

[KNOWN_ISSUES]
- In simulation, {worker_active_periods} (`abstract_dag_planner.py`) are not being calculated accuratelly
