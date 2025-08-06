[PLANNER_OPTIMIZATIONS]
- `task-dup` (to represent tasks that can be duplicated)
    - Use-case:
        if a W1 is waiting for the data of an upstream TA (executing or to be executed on worker 2) to be available, 
        it can execute that task itself (if certain conditions are met). . the results produced by W2 will be ignored by W1. 
    - Possible benefits: - makespan ; - data download time.
        W1 won’t need to wait for the data to be available and then download it from external storage.
        W2 may not need to upload the output to storage.
    - Drawbacks:
        - double execution time (task MAY be executed twice)
        - double input download (target task inputs MAY be downloaded twice)
        - extra storage accesses to:
            - more completion events subscriptions (publishing already happened for all tasks, now we just have more consumers)
            - check duppable tasks startup times
            - check cancelation flag before **input grabbing**, **execution** and **output upload** (cost: resources, not latency)
            - check cancelation flag before **dupping** (avoid 2 workers dupping the same task)
    - Implementation:
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
        - Cancelation flag:
            - Set for a duppable task when it's being dupped by another worker
            - also used to try to avoid 2 workers from dupping the same task
        - CRITERION: which tasks should be duppable?:
            - fast tasks + small input size (because will need to fetch it again (besides the original assigned worker))
            - big output size (because would avoid waiting for storage upload)
        - on the dashboard, show how many dups happened and where visually
    [TODO]
    - make the planners (1 and 2) call the overrides of the `taskdup` annotation
    - make the planners assign this annotation (see criterion above)
    - batch storage operations
- `output-streaming`
    (think/research) (see slack)

- [DONE] `pre-warm` (an "empty" invocation (special message) to a **target resource config**)
    a task that has this annotation, should prewarm `prewarmoptimization.targetResourceConfig` before it starts it's own input handling => execution => etc...
    when it receives this invocation, the FaaS engine will startup a container, run the "empty invocation" code path (could exit immediatelly), and then leave the environment running for a few more seconds (not controllable by us) in hope of another invocation (would be "warm")
    possible benefits: faster startup times for some tasks on workers with a resource config for which there are not yet enough worker instances
    possible issues: to simulate this, I have to set "ALLOW_CONTAINER_REUSAGE=True", but this will make the experiments unfair because some startups will be warm
    [TODO] Manipulate workflow to force this annotation to be used, then see it in action

- [TODO] Create python script to run experiments to test diff. SLAs:
    - this script should by run as: `python script.py`
    - run each planner X times with Y different SLAs for each

- [TODO] Update "global" dashboard to allow comparing data by SLA:
    - success rate (percentage of workflows that finished below the SLA)
    - overall prediction success rate (compared to the real values)

--- 

- [-] Find/Create more workflows

[EVALUATION:PREPARE]
- Implement **WUKONG** planner
    + optimizations
        - Task Clustering (fan-ins + fan-outs)
        - Delayed I/O

---

[EVALUATION:AWS_LAMBDA]
Implement Lambda worker (similar to Docker `worker.py`) and create an automated deployment process