#!/bin/bash

source venv/bin/activate
export DOCKER_IMAGE=docker_worker
export TZ=UTC
waitress-serve --port=5000 src.docker_workers_gateway.handler:app
