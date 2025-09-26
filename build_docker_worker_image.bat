@echo off
echo Building Docker image...
docker build --name docker_worker -f Dockerfile .
echo Docker image built successfully.