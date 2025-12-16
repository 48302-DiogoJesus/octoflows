#!/bin/bash

export DOCKER_IMAGE=docker_worker
export TZ=UTC
waitress-serve --listen=0.0.0.0:5000 src.docker_workers_gateway.handler:app
