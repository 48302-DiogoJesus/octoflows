@echo off
setlocal
set ITERATIONS=20

REM first run just to get some history
python dagforpredictions_expensive_computations.py simple
REM give some time for worker metrics to be flushed, etc...
timeout /t 2 /nobreak

REM Actually run the loops
for /L %%i in (1,1,%ITERATIONS%) do (
    echo Running first iteration %%i
    python dagforpredictions_expensive_computations.py simple
    timeout /t 2 /nobreak
)

for /L %%j in (1,1,%ITERATIONS%) do (
    echo Running second iteration %%j
    python dagforpredictions_expensive_computations.py first
    timeout /t 2 /nobreak
)

for /L %%j in (1,1,%ITERATIONS%) do (
    echo Running second iteration %%j
    python dagforpredictions_expensive_computations.py second
    timeout /t 2 /nobreak
)

echo All runs completed.
pause
exit /b
