- Dashboard
    [IMPLEMENTATION]
    - try with taskdup again + compare
        - decide on what to do (keep taskdup or not? ask prof) and run 5 instances of each
    
    - some charts are showing ALL information instead of the selected planner only
    - show all slas show all workflows in the time breakdown chart
    - Check new results to see the impact of NOT using taskdup
        => if it's not that, should I retry lowering baseline resources to 1GB + just ignore the killed instances?

    - Showing Optimizations Impact:
        - Measure prediction accuracy over time (line chart, need to sort instances by time: use dag_submission_time_ms)
        - Measure the impact of the SLAs in the actual metrics (see if more conservative yeilds better results than optimistic SLAs)
    - Analyze remote data

[KNOWN_ISSUES]
- worker_active_periods are not being calculated correctly (circular issue where I need to these times to know warm and cold starts but I only know them if I calculate worker times). Result: worker_active_periods assumes NO worker startup time

- move taskdup + preload logic from the docker worker.py to `taskup.on_worker_ready()`

# Possible future directions, extensions, and improvements
- Solution Improvements
    - prediction samples summarization (to avoid infinite scaling of samples stored)
    - improve optimizations abstraction to handle conflicting optimizations that override the same worker execution logic stages
    - Implement error handling, using exponential backoff for example, to retry failed task executions and error presentation to the client
- Usability improvements
    - adding supporting dynamic fan-outs
    - allow users to also specify a minimum resource configuration for specific tasks
    - presentation: making the real-time dashboard more informative and maybe even allowing users to manage, launch and manually simulate workflows from there
- Experimentation
    - experiment with other prediction strategies acessing their performance and accuracy
    - implement worker logic for commercial FaaS platforms to see how it performs under higher instability, in a more realistic environment