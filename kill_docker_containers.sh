#!/bin/bash

# Find all container IDs with "DAG" in their name
containers=$(docker ps -a --filter "name=DAG" -q)

# Check if any containers were found
if [ -z "$containers" ]; then
    echo "No containers with 'DAG' in their name found."
    exit 0
fi

# Force remove all matching containers
docker rm -f $containers

echo "Removed containers:"
echo "$containers"
