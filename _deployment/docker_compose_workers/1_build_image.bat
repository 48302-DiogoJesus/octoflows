@echo off
echo Building Docker image...
docker build -t flask-worker-server -f Dockerfile ../../
echo Docker image built successfully.
pause