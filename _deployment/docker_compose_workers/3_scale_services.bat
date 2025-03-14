@echo off
set /p small_replicas="Target number of flask-small replicas: "
set /p large_replicas="Target number of flask-large replicas: "

echo Scaling services...
docker-compose up -d --scale flask-small=%small_replicas% --scale flask-large=%large_replicas%

if %errorlevel% equ 0 (
    echo Services scaled successfully.
) else (
    echo Failed to scale services. Check Docker logs for details.
)

pause