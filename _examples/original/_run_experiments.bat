@echo off
setlocal
set ITERATIONS=7

REM Check if Python script name is provided
if "%~1"=="" (
    echo Error: Please provide the Python script name as the first argument.
    echo Example: %~nx0 my_script.py
    exit /b 1
)

set PYTHON_SCRIPT=%~1
set TZ=UTC-1

REM Check if the Python script exists
if not exist "%PYTHON_SCRIPT%" (
    echo Error: The file "%PYTHON_SCRIPT%" does not exist.
    exit /b 1
)

REM first run just to get some history
python "%PYTHON_SCRIPT%" simple
REM give some time for worker metrics to be flushed, etc...
timeout /t 2 /nobreak

REM Actually run the loops
for /L %%i in (1,1,%ITERATIONS%) do (
    echo Running SIMPLE algorithm %%i
    python "%PYTHON_SCRIPT%" simple
    timeout /t 2 /nobreak
)

for /L %%j in (1,1,%ITERATIONS%) do (
    echo Running FIRST algorithm %%j
    python "%PYTHON_SCRIPT%" first
    timeout /t 2 /nobreak
)

for /L %%j in (1,1,%ITERATIONS%) do (
    echo Running SECOND algorithm %%j
    python "%PYTHON_SCRIPT%" second
    timeout /t 2 /nobreak
)

echo All runs completed.
exit /b
