[KNOWN_ISSUES]
- worker_active_periods are not being calculated correctly (circular issue where I need to these times to know warm and cold starts but I only know them if I calculate worker times). Result: worker_active_periods assumes NO worker startup time

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