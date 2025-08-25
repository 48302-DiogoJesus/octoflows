import os
import sys
import time
import subprocess
from typing import List, Optional

def run_experiment(script_path: str, algorithm: str, iteration: Optional[int] = None) -> None:
    """Run the specified Python script with the given algorithm parameter."""
    cmd = [sys.executable, script_path, algorithm]
    if iteration is not None:
        print(f"Running {algorithm.upper()} algorithm {iteration}")
    else:
        print(f"Initial run with {algorithm.upper()} algorithm")
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path} with {algorithm}: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Add a small delay between runs
    print("Sleeping for 2 seconds...")
    time.sleep(2)

def main():
    # Set timezone for consistency
    os.environ['TZ'] = 'UTC-1'
    
    # Check if script name is provided
    if len(sys.argv) < 2:
        print(f"Error: Please provide the Python script name as the first argument.")
        print(f"Example: {sys.argv[0]} my_script.py")
        sys.exit(1)
    
    script_name = sys.argv[1]
    iterations = 2
    algorithms = ['simple', 'first', 'second']
    
    # Check if the script exists
    if not os.path.isfile(script_name):
        print(f"Error: The file \"{script_name}\" does not exist.", file=sys.stderr)
        sys.exit(1)
    
    # First run to get some history
    run_experiment(script_name, 'simple')
    
    # Run experiments for each algorithm
    for algo in algorithms:
        for i in range(1, iterations + 1):
            run_experiment(script_name, algo, i)
    
    print("All runs completed.")

if __name__ == "__main__":
    main()
