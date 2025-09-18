- implement wukong planner
    - don't use simulation
    - just assign flex. workers to everyone
        to make wukong optimizations possible I can't assign fixed workers
    - make the flex workers logic behave like wukong basic scheduling (on fan-out, 1 worker per task)
- simplify simple planner (should be worse than WUKONG's approach)
    - basic assignment
- rename planners to uniform and non-uniform

- Algorithmos NÃO devem ter otimizações implicitamente, devia ser lógica comum correr a lógica de aplicação de otimizações??
    abstract the optimization application algorithm from the planners and then the planners just call them
        then user could be able to tell the planner which optimizations they want (inclusive)

- try find fix for worker active periods predictions

- make the dashboards load data faster
    log times spent fetching stuff to understand what's the heaviest op.

# not so important

- Providing the DAG representation in worker invocations instead of having to download from storage
    - if below a certain threshold, because worker invocation data has size limits

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

- supporting dynamic fan-outs
        so that its possible to, for example, dynamically partition an output and process it in parallel
