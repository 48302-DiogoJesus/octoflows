- [+] Usar user SLA para calcular SLA de uma workflow
    + testar com percentis e ver se promessa se cumpre:
        quando selectiona p90, os resultados estão abaixo de 90% das execuções
        adicionar ao dashboard (para cada metrica da tabela, a verde se a promessa foi cumprida, vermelho se não)
- [+] Otimizações
    pre-warm
    task-duplication
    output-streaming (think/research) (see slack)

- [-] Mais workflows

[THINK:PLANNER_OPTIMIZATIONS]
- `pre-warm` (an "empty" invocation (special message) to a **target resource config**)
    when it receives this invocation, the FaaS engine will startup a container, run the "empty invocation" code path (could exit immediatelly), and then leave the environment running for a few more seconds (not controllable by us) in hope of another invocation (would be "warm")
    possible benefits: faster startup times for some tasks on workers with a resource config for which there are not yet enough worker instances
    possible issues: to simulate this, I have to set "ALLOW_CONTAINER_REUSAGE=True", but this will make the experiments unfair because some startups will be warm
- `task-dup`
    if a worker A is waiting for the data of an upstream task 1 (executing or to be executed on worker 2) to be available, 
    it can execute that task itself. by executing task 1 locally, worker 2 won’t need to wait for the data to be available 
    and then download it from external storage. the results produced by worker 2 will be ignored by worker 1. 
    possible benefits: - makespan ; - data download time.
    implementation:
        - what if has N **not-ready** dependencies (how to choose/which to `dup`?)
        - how to predict how late a dependency is
            - need to know the REAL startup time of DEPENDENCY tasks of eligible tasks?
            - to predict, embed the produced plan data in the DAG representation
                for dependency tasks:
                    `expected_ready_to_exec_ts` = `REAL_task_startup_time` + `pred_execution_time` + `pred_upload_time` + `pred_download_time` (by eligible task)
                    `simulated_ready_to_exec_ts` = `NOW_TS` + `pred_download_time` (all deps. of the dep. task) + `pred_execution_time`
                        (live predictions because my **resource config** may be different)
                    if `simulated_ready_to_exec_ts` + `TIME_TOLERANCE` < `expected_ready_to_exec_ts`, then **dup**
        - which tasks are good targets for considering duplication (planning phase should mark the eligible tasks)
            - fast tasks + small input size (because will need to fetch it again (besides the original assigned worker))
            - big output size (because would avoid waiting for storage upload)
        - cancelation signal: so that the assigned worker can avoid **executing** or **uploading** output to storage if not needed (check signal at checkpoint)
        - on the dashboard, show how many dups happened and where visually
    drawbacks:
        - double execution time (task may be executed twice)
        - double input download (task inputs may be downloaded twice)
        - double output upload (if not able to send cancelation signal)
        - signal checks consume storage and add small latency
        - DAG representation will be bigger because of embedded plan
- `output-streaming`
- Create a planner that uses them + make the prediction calculations take it into account


[EVALUATION:PREPARE]
- Implement **WUKONG** planner
    + optimizations
        - Task Clustering (fan-ins + fan-outs)
        - Delayed I/O

---

[EVALUATION:AWS_LAMBDA]
Implement Lambda worker (similar to Docker `worker.py`) and create an automated deployment process