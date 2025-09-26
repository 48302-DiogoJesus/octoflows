#!/bin/bash

echo "Building Docker image..."
docker build -t docker_worker -f Dockerfile .
echo "Docker image built successfully."
