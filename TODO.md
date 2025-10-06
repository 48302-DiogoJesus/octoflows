- Dashboard
    [IMPLEMENTATION]
        - BUG: not all workflows are storing their metrics

    - Cold starts were not too effective locally, re-check if remote is the same and try adjusing parameters
        compare metrics prewarm timestamp vs target tast actual start time to measure effectiveness
    
    - Run all experiments on VM
        Only run 4 instances each to begin with

    - Showing Optimizations Impact:
        - TaskDup
            MEASURE (time waiting for inputs should be lower => add this metric to the Actual Metrics bar chart)
        - PreLoad
            MEASURE
            - compare the start times of all tasks with preload optimization that have non-empty `TaskOptimizationMetrics.preloaded` VERSUS planners that don't use this optimization (start times should be lower/earlier)
        - PreWarm (note: (only makes sense for non uniform))
            MEASURE
            - compare metrics prewarm timestamp vs target tast actual start time to measure effectiveness
            - + use the existing cold start vs warm start comparison (expect the planner that uses it to have more warm starts)
        - Plot showing the median of counts for each optimization per instance, only for the "opt" planners

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

# Possible future directions, extensions, and improvements
    - Don't need to compare cpus, as they are proportional to memory now
    - Make dynamic library fetching work and efficient
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
    - AWS Lambda worker implementation an tests