# Requirements
- Docker
- Python 3.12

# Steps
- Create **python venv**
    - `python3.12 -m venv venv`
    - use `. activate_venv.sh` to activate it
    - install requirements (first **activate venv**): `pip install -r src/requirements.txt`
- Install `graphviz` executable
```bash
sudo apt-get update
sudo apt-get install graphviz
```
- Install `redis-cli`
```bash
sudo apt-get install redis-tools
```
- Run `create_redis_docker.sh`
    - creates 2 redis containers, password protected and with persistence enabled

- Start the docker gateway (FaaS "simulator"): `bash build_docker_worker_image.sh && bash start_gateway_docker.sh`


# See metrics dashboards
```bash
. activate_venv.sh && cd _metadata_analysis && streamlit run global_planning_analysis_dashboard.py
```

# Experiment
```bash
. activate_venv.sh && cd_examples/original && python matrix_multiplications.py simple
```