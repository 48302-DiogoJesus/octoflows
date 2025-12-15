 Notes:
    - ISSUE: Warmup requests could warm a container on a diff. gateway than the task will run
        - TODO

- Try Vagrant up on local machine
- Remove .bat scripts from repo

- cluster node vagrant (gateway) + exmachina (gateway + DBs)
    - Test each workflow individually
    - Run experiments script 1 of each planner and workflow

- Split experiments into 3 executions, backup the rdb files after each, in case we need to go back

- Update `Deployment.md` to also include instructions/mention for Vagrant

[KNOWN_ISSUES]
- In simulation, {worker_active_periods} (`abstract_dag_planner.py`) are not being calculated accuratelly
