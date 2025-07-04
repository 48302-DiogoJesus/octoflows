@echo off
setlocal
set ITERATIONS=20

REM first run just to get some history
@REM python dagforpredictions_expensive_computations.py simple
REM give some time for worker metrics to be flushed, etc...
timeout /t 2 /nobreak

REM Actually run the loops
for /L %%i in (1,1,%ITERATIONS%) do (
    echo Running first iteration %%i
    python dagforpredictions_expensive_computations.py second
    timeout /t 2 /nobreak
)

@REM for /L %%j in (1,1,%ITERATIONS%) do (
@REM     echo Running second iteration %%j
@REM     python dagforpredictions_expensive_computations.py first
@REM     timeout /t 1 /nobreak
@REM )

echo All runs completed.
pause
exit /b
