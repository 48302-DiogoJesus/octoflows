- Ensure that GEMM is actually GEMM

[NEW_ISSUES_FOUND]
- [SEMI_BAD] worker_active_periods are not being calculated correctly (circular issue where I need to these times to know warm and cold starts but I only know them if I calculate worker times). Result: worker_active_periods assumes NO worker startup time
- In the start, planners assign worker ids randomly/first upstream worker id
    this is not optimal: for example, if task is on critical path it should have priority to use the same worker id as the CP upstream task
    requires: rethinking order of actions by the planner algorithm

- [TODO] Update dashboards:
    Compare SLAs ("global"):
    - success rate (percentage of workflows that finished below the SLA)
    - overall prediction success rate (compared to the real values)
    PreWarm ("individual"):
    - how many happened, which tasks had them
    Dup ("individual"):
    - how many happened, which tasks were dupped?

---

**1** | [DONE] `pre-warm` (an "empty" invocation (special message) to a **target resource config**)
    [A] a task that has this annotation, should prewarm `prewarmoptimization.targetResourceConfig` before it starts it's own input handling => execution => etc...
    when it receives this invocation, the FaaS engine will startup a container, run the "empty invocation" code path (could exit immediatelly), and then leave the environment running for a few more seconds (not controllable by us) in hope of another invocation (would be "warm")
    [A] when is it added by the planners?
        - for each node that is expected to be a "cold start", if a its worker configuration startup time represents more than 15% of the sum of execution times of tasks with same worker config that start AFTER this node, then that worker config should be prewarmed
            - Look at nodes before this node and find a node that could prewarm the worker config needed for this node without it becoming cold when I start
    possible benefits: faster startup times for some tasks on workers with a resource config for which there are not yet enough worker instances
    possible issues: to simulate this, I have to set "ALLOW_CONTAINER_REUSAGE=True", but this will make the experiments unfair because some startups will be warm

**2** | `task-dup` (to represent tasks that can be duplicated)
    - Use-case:
        if a W1 is waiting for the data of an upstream TA (executing or to be executed on worker 2) to be available, 
        it can execute that task itself (if certain conditions are met). . the results produced by W2 will be ignored by W1. 
    - Possible benefits: - makespan ; - data download time.
        W1 won’t need to wait for the data to be available and then download it from external storage.
        W2 may not need to upload the output to storage.
    - [A] Drawbacks:
        - double execution time (task MAY be executed twice)
        - double input download (target task inputs MAY be downloaded twice)
        - extra storage accesses to:
            - more completion events subscriptions (publishing already happened for all tasks, now we just have more consumers)
            - check duppable tasks startup times
            - check cancelation flag before **input grabbing**, **execution** and **output upload** (cost: resources, not latency)
            - check cancelation flag before **dupping** (to TRY avoid 2 workers dupping the same task)
            - the duppable.upstream tasks all need to send their outputs to storage because the "dupper" may need it
        - dupping may be CANCELLED because the dependencies for running the duppable task may not be available
    - [A] Implementation:
        - Before they start executing, duppable tasks will store a timestamp
        - For each task that has at least 1 upstream task that can be dupped (has annotation), subscribe to COMPLETION events of ALL upstream tasks (not just the duppable ones)
            - every time this event is fired:
                for each unfinished duppable task:
                    formula: 
                        expected_ready = start_time + pred_execution_time + pred_upload_time + pred_download_time
                        potential_ready = now + pred_download_time_locally + pred_execution_time_locally
                        if potential_ready + THRESHOLD < expected_ready:
                            is duppable
                            time_saved = expected_ready - potential_ready
                DUP the duppable task with the greatest `time_saved`
                happens while using a lock meaning that 1 worker can only DUP 1 task at a time
        - [A] Cancelation flag:
            - Set for a duppable task when it's being dupped by another worker
            - also used to TRY to avoid 2 workers from dupping the same task
    [TODO:IMPROVEMENTS]
    - Check if ALL duppable task dependencies are ready before dupping
        replace the current logic that aborts execution if not READY (lazy)
        use `storage.exists(keys[])`
    - BATCH storage operations
    - Use atomic cancelation flag to avoid 2 workers dupping the same task (redis atomic_increment?)
    - On the dashboard, show how many dups happened and where visually

---

- [-] Find/Create more workflows

[EVALUATION:PREPARE]
- Implement **WUKONG** planner
    + optimizations
        - Task Clustering (fan-ins + fan-outs)
        - Delayed I/O
