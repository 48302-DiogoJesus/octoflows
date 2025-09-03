import os
import sys
import time
import subprocess

WORKFLOWS_PATHS = [
        # 'matrix_multiplications.py',
        # 'gemm.py',
        # 'fixed_times.py',
        # 'wordcount.py',
        # 'image_transform.py',
        'tree_reduction.py'
    ]
    
ITERATIONS_PER_ALGORITHM = 3
ALGORITHMS = ['simple', 'first', 'second']

TIME_UNTIL_WORKER_GOES_COLD_S = 5

def run_experiment(script_path: str, algorithm: str, iteration: str, current: int, total: int) -> None:
    """Run the specified Python script with the given algorithm parameter."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_script_path = os.path.join(script_dir, script_path)
    
    cmd = [sys.executable, full_script_path, algorithm]
    
    percentage = (current / total) * 100 if total > 0 else 0
    print(f" > [{percentage:5.1f}%] Running {os.path.basename(script_path)} with {algorithm.upper()} algorithm (iteration: {iteration}) [{current}/{total}]")
    
    try:
        subprocess.run(cmd, check=True, cwd=script_dir)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path} with {algorithm}: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Small delay between runs to give time for the workers to go cold between diff. workflow runs
    print(f"Sleeping for {TIME_UNTIL_WORKER_GOES_COLD_S * 1.8} seconds...")
    time.sleep(TIME_UNTIL_WORKER_GOES_COLD_S * 1.8)

def main():
    os.environ['TZ'] = 'UTC-1' # Set timezone for log timestamps consistency
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Calculate total number of runs
    total_runs = 0
    for script_name in WORKFLOWS_PATHS:
        script_path = os.path.join(script_dir, script_name)
        if os.path.isfile(script_path):
            total_runs += len(ALGORITHMS) * ITERATIONS_PER_ALGORITHM
    
    current_run = 0
    
    for script_name in WORKFLOWS_PATHS:
        script_path = os.path.join(script_dir, script_name)
        
        # Check if the script exists
        if not os.path.isfile(script_path):
            print(f"Warning: The file \"{script_path}\" does not exist. Skipping...", file=sys.stderr)
            continue
        
        print(f"\n{'='*50}")
        print(f"Running experiments for {script_name}")
        print(f"{'='*50}")
        
        # First run to get some history (not counted in progress)
        print(" > [Initial run] Warming up with 'simple' algorithm...")
        run_experiment(script_name, 'simple', iteration="initial", current=0, total=0)
        
        # Run experiments for each algorithm
        for algorithm in ALGORITHMS:
            for i in range(1, ITERATIONS_PER_ALGORITHM + 1):
                current_run += 1
                run_experiment(script_name, algorithm, str(i), current_run, total_runs)
    
    print("All runs completed.")

if __name__ == "__main__":
    main()
