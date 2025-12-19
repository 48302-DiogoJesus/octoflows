Test Setup
    - M1
        Client
        DBs
        Gateway (Inside Vagrant)
        Tunnel Docker API from M2
    - M2
        Gateway (Inside Vagrant)

- Issue: too many failed prewarms. experiments:
    `uniform`
        ready_offset_s=1 => 8/13
        ready_offset_s=2 => 8/18 (30%)
        ready_offset_s=3 => 8/18 | TODO
        ready_offset_s=4 => 11/15
        ready_offset_s=5 => 0
    `non-uniform`
        ready_offset_s=1 => 13/8
        ready_offset_s=1.5 => TODO
        ready_offset_s=2 => 12/15 (45%)
        ready_offset_s=3 => 9/18
        ready_offset_s=4 => 8/13
        ready_offset_s=5 => 0

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
