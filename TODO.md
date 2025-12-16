Vitamina02 (10.15.0.22) (db + vagrant gateway) + Proteina04 (vagrant gateway)

 Notes:
    - ISSUE: Warmup requests could warm a container on a diff. gateway than the task will run
        - TODO

- Run experiments script 2 of each planner and workflow
- Check if viz make sense 

- Split experiments into 3 executions, backup the rdb files after each, in case we need to go back

- Update `Deployment.md` to also include instructions/mention for Vagrant
- Update paper with graphics
- Update paper with experiment parameters, configuration, and deployment

[KNOWN_ISSUES]
- In simulation, {worker_active_periods} (`abstract_dag_planner.py`) are not being calculated accuratelly
