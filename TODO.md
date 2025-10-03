- Dashboard
    - Slack metrics
        DONE Percentagem de SLA fulfillment para cada nível de SLA
    - See WUKONG evaluation chapter (https://chatgpt.com/c/68dd8d50-2c90-832c-9083-3bd76a766232)
        - Improve "Metrics Comparison (by Planner)" by showing all metrics in a single graph
        - Time breakdown: instead of having a pie chart, add a bar chart with multiple layers (one for each planner)
        - Network I/O (data downloaded (I), data uploaded (O))
        - CPU time + ?Memory time? (I haven't yet added a graph comparing resource utilization (cpu time + memory time or costs?))
    
    - Understand individual optimizations impact:
        - Easiest way is to simply run the same workflow multiple times with different individual optimizations and then all of them
            Choose one planner to do this with and add to `config.py`
        - PreLoad
            measure
            HOW:
            - 
        - TaskDup
            measure 
            HOW:
            - track when dupping happened (is the flag in metrics_storage? dont think so)
        - PreWarm
            measure (only makes sense for non uniform)
            HOW:
            - track when prewarming happened (look at annotation/optimization and check cold starts versus the same workflow on other planners??)
    
    - More variation in workflows
        - Force prewarm to be used! (test on non-uniform)
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