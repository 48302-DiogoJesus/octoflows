- Dashboard
    [IMPLEMENTATION]
    - Analyse remote data
    - DASHBOARD: Show "non-uniform" vs "uniform" regarding makespan, execution time and resource usage in a single chart
        to see for example, we have 8% less makespan, while spending 10% more resources
    - Showing Optimizations Impact:
        - [continue creating preload viz after running more instances] (https://chatgpt.com/c/68e3ff78-525c-832e-aa15-85c4ade67a26)
            PreLoad
            MEASURE
            - compare the start times of all tasks with preload optimization that have non-empty `TaskOptimizationMetrics.preloaded` VERSUS planners that don't use this optimization (start times should be lower/earlier)
        - Measure prediction accuracy over time (line chart, need to sort instances by time: use dag_submission_time_ms)
        - Measure the impact of the SLAs in the actual metrics (see if more conservative yeilds better results than optimistic SLAs)

[KNOWN_ISSUES]
- worker_active_periods are not being calculated correctly (circular issue where I need to these times to know warm and cold starts but I only know them if I calculate worker times). Result: worker_active_periods assumes NO worker startup time

- move taskdup logic from the docker worker.py to `taskup.on_worker_ready()`

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