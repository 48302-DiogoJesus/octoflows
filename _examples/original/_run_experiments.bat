@echo off
setlocal
set ITERATIONS=5

REM first run just to get some history
@REM python dagforpredictions_expensive_computations.py simple

REM Actually run the loops
for /L %%i in (1,1,%ITERATIONS%) do (
    echo Running first iteration %%i
    python dagforpredictions_expensive_computations.py simple
)

for /L %%j in (1,1,%ITERATIONS%) do (
    echo Running second iteration %%j
    python dagforpredictions_expensive_computations.py first
)

echo All runs completed.
pause
exit /b
