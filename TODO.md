Test Setup
    - M1
        Client
        DBs
        Gateway (Inside Vagrant)
        Tunnel Docker API from M2
    - M2
        Gateway (Inside Vagrant)

- Yry 4, 6, 8
- Analyze workflow by workflow
ISSUES:
- text-analysis
    - NonUniform-opt is a lot worse than NonUniform
- gemm
    - nonuniform-opt is a little worse than NonUniform
- image-transformer
    - nonuniform is worse than uniform

- Check if pre-warm co-location optimization had better effect now
- Convert to octet-stream to avoid JSON serialization and smaller HTTP payloads
    warmup + delegate
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
