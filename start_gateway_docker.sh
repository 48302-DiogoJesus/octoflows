#!/bin/bash

# expose the Docker REST API
sudo dockerd -H unix:///var/run/docker.sock -H tcp://127.0.0.1:2375 &

export DOCKER_IMAGE=docker_worker
export TZ=UTC
waitress-serve --port=5000 src.docker_workers_gateway.handler:app
