Test Setup
- M1
    DBs (Bare Metal)
    Client (Inside Vagrant)
    Gateway (Inside Vagrant)
- M2
    Gateway (Inside Vagrant)


- Check how much RAM does redis occupy after 200 workflows and maybe increase Vagrantfile memory to 50GB

- Try importing snapshot locally and check if lost data is there

- Rerun experiemnts
- Prewarm not good
- Run experiments script 2 of every planner and workflow
- 1 downgrade simulation takes around 5 seconds for 60 nodes DAGs

- Split experiments into 3 executions, backup the rdb files after each, in case we need to go back

- Update `Deployment.md` to also include instructions/mention for Vagrant + remove mention of Docker API
- Update paper with graphics
- Update paper with experiment parameters, configuration, and deployment

- !! NOTE: Intermediate result is being removed from storage, ONLY to make experiments easier and require less redis memory

[KNOWN_ISSUES]
- In simulation, {worker_active_periods} (`abstract_dag_planner.py`) are not being calculated accuratelly
