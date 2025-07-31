#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Run pytest with verbose output and stop on first failure
pytest -x -v tests/
