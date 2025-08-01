[EVALUATION:PREPARE]
- Implement **WUKONG** planner
    + optimizations
        - Task Clustering (fan-ins + fan-outs)
        - Delayed I/O

---

[THINK:PLANNER_OPTIMIZATIONS]
- `pre-warm` (Just make an "empty" invocation (special message that the `Worker Handler` must be ready for?) to a container with a given **resource config**)
    Possible benefits: faster startup times for some tasks on "new" workers
        can't measure it at the planner level since the predictions aren't considering worker startup times (warm/cold)
- `task-dup`
    If a Worker A is waiting for the data of an upstream Task 1 (executing or to be executed on Worker 2) to be available, 
    it can execute that task itself. By executing Task 1 locally, Worker 2 won’t need to wait for the data to be available 
    and then download it from external storage. The results produced by Worker 2 will be ignored by Worker 1. 
    Possible benefits: - makespan ; - data download time.
- Create a planner that uses them + make the prediction calculations take it into account

---

[EVALUATION:AWS_LAMBDA]
Implement Lambda worker (similar to Docker `worker.py`) and create an automated deployment process