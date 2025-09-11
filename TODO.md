- Fix GEMM
    multiplication result not correct

- Test montage converted alone with hardcoded inputs
    Try montage with multiple small files instead of just one .fits file

- Run all 4 workflows (10 iterations each) to see which have better results

- Run the best 2 workflows (20 iterations each)
    - then run more if necessary


- [TODO] Update dashboards:
    Compare SLAs ("global"):
    - success rate (percentage of workflows that finished below the SLA)
    - overall prediction success rate (compared to the real values)
    PreWarm ("individual"):
    - how many happened, which tasks had them
    Dup ("individual"):
    - how many happened, which tasks were dupped?
    
[NEW_ISSUES_FOUND]
- In the start, planners assign worker ids randomly/first upstream worker id
    this is not optimal: for example, if task is on critical path it should have priority to use the same worker id as the CP upstream task
    requires: rethinking order of actions by the planner algorithm
- [SEMI_BAD] worker_active_periods are not being calculated correctly (circular issue where I need to these times to know warm and cold starts but I only know them if I calculate worker times). Result: worker_active_periods assumes NO worker startup time

[EVALUATION]
- Implement **WUKONG** planner or make it the default (flex. workers)
    + optimizations
        - Task Clustering (fan-ins + fan-outs)
        - Delayed I/O
