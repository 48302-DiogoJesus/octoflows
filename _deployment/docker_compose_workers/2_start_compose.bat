@echo off
echo Starting Docker Compose services...
docker-compose up -d --remove-orphans
echo Docker Compose services started.