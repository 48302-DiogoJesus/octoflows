@echo off
set /p small_replicas="Target number of SMALL replicas: "
set /p large_replicas="Target number of LARGE replicas: "

echo Scaling services...
docker-compose up -d --scale standalone-worker-small=%small_replicas% --scale standalone-worker-large=%large_replicas%

if %errorlevel% equ 0 (
    echo Services scaled successfully.
) else (
    echo Failed to scale services. Check Docker logs for details.
)