import os
import sys
import time
import subprocess

WORKFLOWS_PATHS = [
    # 'tree_reduction.py',
    'text_analysis.py',
    # 'wordcount.py',
    # 'image_transformer.py',
    # 'gemm.py',
    # 'montage.py',
]

ITERATIONS_PER_ALGORITHM = 2
ALGORITHMS = ['wukong', 'simple', 'first', 'second']
SLAS = ['50']
# SLAS = ['50', '75', '90', '95', '99']

TIME_UNTIL_WORKER_GOES_COLD_S = 5

def run_experiment(script_path: str, algorithm: str, sla: str, iteration: str, current: int, total: int) -> None:
    """Run the specified Python script with the given algorithm and SLA parameters."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_script_path = os.path.join(script_dir, script_path)
    
    cmd = [sys.executable, full_script_path, algorithm, sla]
    
    percentage = (current / total) * 100 if total > 0 else 0
    print(f" > [{percentage:5.1f}%] Running {os.path.basename(script_path)} with {algorithm.upper()} algorithm, SLA {sla} (iteration: {iteration}) [{current}/{total}]")
    
    try:
        subprocess.run(cmd, check=True, cwd=script_dir)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path} with {algorithm} and SLA {sla}: {e}", file=sys.stderr)
        sys.exit(-1)
    
    # Small delay between runs to give time for the workers to go cold between diff. workflow runs
    print(f"Sleeping for {TIME_UNTIL_WORKER_GOES_COLD_S * 2.7} seconds...")
    time.sleep(TIME_UNTIL_WORKER_GOES_COLD_S * 2.7)

def main():
    os.environ['TZ'] = 'UTC-1'  # Set timezone for log timestamps consistency
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Calculate total number of runs
    total_runs = 0
    for script_name in WORKFLOWS_PATHS:
        script_path = os.path.join(script_dir, script_name)
        if os.path.isfile(script_path):
            total_runs += len(ALGORITHMS) * len(SLAS) * ITERATIONS_PER_ALGORITHM
    
    current_run = 0
    
    for script_name in WORKFLOWS_PATHS:
        script_path = os.path.join(script_dir, script_name)
        
        # Check if the script exists
        if not os.path.isfile(script_path):
            print(f"Warning: The file \"{script_path}\" does not exist. Skipping...", file=sys.stderr)
            continue
        
        print(f"\n{'='*60}")
        print(f"Running experiments for {script_name}")
        print(f"{'='*60}")
        
        # First run to get some history (not counted in progress)
        print(" > [Initial run] Warming up with 'simple' algorithm and 'average' SLA...")
        run_experiment(script_name, 'simple', 'average', iteration="initial", current=0, total=0)
        
        # Run experiments for each algorithm and SLA combination
        for algorithm in ALGORITHMS:
            for sla in SLAS:
                for i in range(1, ITERATIONS_PER_ALGORITHM + 1):
                    current_run += 1
                    run_experiment(script_name, algorithm, sla, str(i), current_run, total_runs)
    
    print("All runs completed.")

if __name__ == "__main__":
    main()