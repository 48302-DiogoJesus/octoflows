import os
import sys
import time
import subprocess
from typing import List, Optional

WORKFLOWS_PATHS = [
        'matrix_multiplications.py',
        # 'gemm.py',
        'fixed_times.py',
        # 'wordcount.py',
        'image_transform.py',
    ]
    
ITERATIONS = 3
ALGORITHMS = ['simple', 'first', 'second']

TIME_UNTIL_WORKER_GOES_COLD_S = 1

def run_experiment(script_path: str, algorithm: str, iteration: str) -> None:
    """Run the specified Python script with the given algorithm parameter."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_script_path = os.path.join(script_dir, script_path)
    
    cmd = [sys.executable, full_script_path, algorithm]
    
    print(f" > Running {os.path.basename(script_path)} with {algorithm.upper()} algorithm (iteration: {iteration})")
    
    try:
        subprocess.run(cmd, check=True, cwd=script_dir)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path} with {algorithm}: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Small delay between runs to give time for the workers to go cold between diff. workflow runs
    print(f"Sleeping for {TIME_UNTIL_WORKER_GOES_COLD_S * 2} seconds...")
    time.sleep(TIME_UNTIL_WORKER_GOES_COLD_S * 2)

def main():
    os.environ['TZ'] = 'UTC-1' # Set timezone for log timestamps consistency
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    for script_name in WORKFLOWS_PATHS:
        script_path = os.path.join(script_dir, script_name)
        
        # Check if the script exists
        if not os.path.isfile(script_path):
            print(f"Warning: The file \"{script_path}\" does not exist. Skipping...", file=sys.stderr)
            continue
        
        print(f"\n{'='*50}")
        print(f"Running experiments for {script_name}")
        print(f"{'='*50}")
        
        # First run to get some history
        run_experiment(script_name, 'simple', iteration="initial")
        
        # Run experiments for each algorithm
        for algo in ALGORITHMS:
            for i in range(ITERATIONS):
                run_experiment(script_name, algo, iteration=str(i))
    
    print("All runs completed.")

if __name__ == "__main__":
    main()
