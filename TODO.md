Test Setup
    - M1
        Client
        DBs
        Gateway (Inside Vagrant)
        Tunnel Docker API from M2
    - M2
        Gateway (Inside Vagrant)

- NEW RESOURCE USAGE CALCULATION MIGRATION
    - resource_usage = sum(worker_script_execution_time) + count(total_prewarms_made) * 50ms (time estimate to execute dummy function)
    - Update dashboard to measure prewarms succeeded using new metrics:
        - prewarms done: {node.metrics.optimization_metrics}
        - prewarms successful: count({workerstartupmetrics.was_prewarmed})

- Prewarm not effective? adjust parameters and try again?


- Run experiments script 2 of each planner and workflow

- Split experiments into 3 executions, backup the rdb files after each, in case we need to go back

- Update `Deployment.md` to also include instructions/mention for Vagrant + remove mention of Docker API
- Update paper with graphics
- Update paper with experiment parameters, configuration, and deployment

[KNOWN_ISSUES]
- In simulation, {worker_active_periods} (`abstract_dag_planner.py`) are not being calculated accuratelly
