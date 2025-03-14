@echo off
echo Building Docker image...
docker build -t standalone-worker -f Dockerfile ../..
echo Docker image built successfully.