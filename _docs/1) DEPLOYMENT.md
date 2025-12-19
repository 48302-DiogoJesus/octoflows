# DEPLOYMENT

## Requirements
- Docker
- Python 3.12

## Steps
1) Install Python dependencies: `pip install -r src/requirements.txt`
2) Install `graphviz`
```bash
sudo apt-get update
sudo apt-get install graphviz
```
3) Install `redis-cli`
```bash
sudo apt-get install redis-tools
```
4) Run `sc_create_redis_docker.sh`. This script creates two password-protected Redis containers.
5) Start the Docker gateway (FaaS emulator): `bash sc_build_worker.sh && bash sc_start_gateway.sh`