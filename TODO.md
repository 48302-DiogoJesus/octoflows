- PRELOAD: Update "paper" with planning assignment logic

- Confirm wukong optimizations are being applied
    - Check workflow dashboard to see if workers were reused on fan-outs with BIG OUTPUT
    - Find a way to know where/how many task clusterings and delayed I/Os happened
        possible just from looking at worker ids at the end?

- Dashboard
    - [!] Show not only input and output sizes but also ALL the data that travelled the network
            how?: total data uploaded + total data downloaded
            PROMPT: ON @global_planning_analysis_dashboard.py , can you add a new metric to the dashboard: Total Transferred Data which is the sum of the data uploaded and the data downloaded. This should help understand which planners transfer more data over the network
            see deepseek: https://chat.deepseek.com/a/chat/s/955c80aa-1505-4ca5-849c-231266a413ff
    - Calculate a resource usage metric per workflow: sum(CPUs*memória*tempo for each task)
    - Compare SLAs ("global"):
        - success rate (percentage of workflows that finished below the SLA)
        - overall prediction success rate (compared to the real values)
        PreWarm ("individual"):
        - how many happened, which tasks had them
        Dup ("individual"):
    - how many happened, which tasks were dupped?

- try find fix for worker active periods predictions
    
[NEW_ISSUES_FOUND]
- In the start, planners assign worker ids randomly/first upstream worker id
    this is not optimal: for example, if task is on critical path it should have priority to use the same worker id as the CP upstream task
    requires: rethinking order of actions by the planner algorithm
- [SEMI_BAD] worker_active_periods are not being calculated correctly (circular issue where I need to these times to know warm and cold starts but I only know them if I calculate worker times). Result: worker_active_periods assumes NO worker startup time

- supporting dynamic fan-outs
        so that its possible to, for example, dynamically partition an output and process it in parallel

# Possible extensions/improvements
    - improvements to prediction logic
    - make the metrics collection more scalable while not sacrificing predictions
    - supporting dynamic fan-outs
        so that its possible to, for example, dynamically partition an output and process it in parallel
    - Optimize DAG structure to make it smaller and scale better as number of tasks increase
    - ? supporting execution of generic executables as tasks, and not just python functions
        CLI program (input (stdin or cli args) => output (stdout))
    - Handle conflicting optimizations that override the same problematic (those that have return values) WEL methods AND have side effects (like delegating stuff inside them)