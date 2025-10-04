- Dashboard
    [IMPLEMENTATION]
    - Dashboard
        - [DONE] Check preload metrics
        - Check prewarm metrics
        - Check taskdup metrics
            taskdup isn't being applying in reality
            need a workflow with short task + small input + downstream task that has other upstream tasks that finish first
    - debug: check if preload is happening and results are being used (gemm is a good test)
    - Run experiments on VM

    [EVAL_+_IMPLEMENTATION] Measuring Optimizations:
        - [DONE] Add a field to TaskMetrics of type list[TaskOptimizationMetrics]
        - TaskDup
            MEASURE 
            HOW
            - [DONE] track when dupping happened
                `TaskOptimizationMetrics.dupped`: list[DAGTaskNodeId] # indicating the tasks that the current_task dupped
        - PreWarm (note: (only makes sense for non uniform))
            HOW
            - count the prewarms by counting the optimizations (sum of all len(opt.target_resource_configs))
            - look at annotation/optimization and check cold starts versus the same workflow on other planners??
            MEASURE
            - do nothing, use the existing cold start vs warm start comparison (expect the planner that uses it to have more warm starts)
        - PreLoad
            HOW
            - [DONE] count the preloads: `TaskOptimizationMetrics.preloaded`: list[DAGTaskNodeId] # indicating the tasks that the current_task preloaded
            MEASURE
            - compare the start times of all tasks with preload optimization that have non-empty `TaskOptimizationMetrics.preloaded` VERSUS planners that don't use this optimization (start times should be lower/earlier)

    - Have to RERUN ALL!!
    - !! optimizations seem to be increasing times (makespan and resource usage for uniform vs uniform w/ opts)
        - fix: make preloading happen on separate Thread?
        - Force prewarm to be used! (test on non-uniform)
    
    - More variation in workflows
        - Non uniform without optimizations + with optimizations
        - More SLAs (90, 95, 99)

[NEW_ISSUES_FOUND]
- In the start, planners assign worker ids randomly/first upstream worker id
    this is not optimal: for example, if task is on critical path it should have priority to use the same worker id as the CP upstream task
    requires: rethinking order of actions by the planner algorithm
- [SEMI_BAD] worker_active_periods are not being calculated correctly (circular issue where I need to these times to know warm and cold starts but I only know them if I calculate worker times). Result: worker_active_periods assumes NO worker startup time

# Where we can be better than WUKONG
    - On 1-1, it may compensate to swap workers if the second task benefits from running on a stronger worker and it compensates sending the data over the network
    - On fan-out, clustering tasks that are expected to produce large outputs on the same worker will make fan-ins cheaper, because then, the worker that accumulated the most amount of data can run the fan-in task and avoid sending the data over the network

# Possible extensions/improvements
    - improvements to prediction logic
    - make the metrics collection more scalable while not sacrificing predictions
    - supporting dynamic fan-outs
        so that its possible to, for example, dynamically partition an output and process it in parallel
        compromise predictions but give more expressiveness to the user 
            OR
        just force them to partition into 2 workflows (this way predictions are more accurate, despite having to split up the workflow)
    - Optimize DAG structure to make it smaller and scale better as number of tasks increase
    - ? supporting execution of generic executables as tasks, and not just python functions
        CLI program (input (stdin or cli args) => output (stdout))
    - Handle conflicting optimizations that override the same problematic (those that have return values) WEL methods AND have side effects (like delegating stuff inside them)
    - Workflow error handling and presentation to the user