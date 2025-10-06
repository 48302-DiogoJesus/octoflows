import os
import sys
import time
import subprocess
import requests

WORKFLOWS_PATHS = [
    # 'gemm.py',
    # 'tree_reduction.py',
    'image_transformer.py',
    'text_analysis.py',
]

ITERATIONS_PER_ALGORITHM = 6
ALGORITHMS = ['uniform-opt', 'non-uniform-opt'] # testing prewarm
# ALGORITHMS = ['wukong', 'wukong-opt', 'uniform', 'uniform-opt', 'non-uniform', 'non-uniform-opt']
# ALGORITHMS = ['uniform']
# SLAS = ['50', '75', '90', '95', '99']
SLAS = ['50']

DOCKER_FAAS_GATEWAY_IP = "localhost"

montage_workload = "heavy"
if len(sys.argv) > 1:
    montage_workload = sys.argv[1]
print(f"Montage workload: {montage_workload}")

failed_instances = 0

def wait_containers_shutdown():
    url = f"http://{DOCKER_FAAS_GATEWAY_IP}:5000/wait-containers-shutdown"
    print("Waiting for all containers to shutdown...")
    try:
        response = requests.post(url)
        if response.status_code == 200:
            print("All containers have shutdown!")
        else:
            print(f"Unexpected response: {response.status_code}, {response.text}")
    except requests.RequestException as e:
        print(f"Error making request: {e}")

def run_experiment(script_path: str, algorithm: str, sla: str, iteration: str, current: int, total: int) -> None:
    """Run the specified Python script with the given algorithm and SLA parameters, enforcing a timeout."""
    global failed_instances
    wait_containers_shutdown()  # sync
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_script_path = os.path.join(script_dir, script_path)
    
    if os.path.basename(script_path) == 'montage.py':
        print(f"Montage Workflow will run with workload: {montage_workload}")
        cmd = [sys.executable, full_script_path, algorithm, sla, montage_workload]
    else:
        cmd = [sys.executable, full_script_path, algorithm, sla]
    
    percentage = (current / total) * 100 if total > 0 else 0
    print(f" > [{percentage:5.1f}%] Workflow: {os.path.basename(script_path)} | Planner: {algorithm.upper()} algorithm | SLA: {sla} (iteration: {iteration}) [{current}/{total}]")
    
    try:
        subprocess.run(cmd, check=True, cwd=script_dir, timeout=120)  # if workflow doesn't finish after 2 minutes, assume it failed and move on
    except subprocess.TimeoutExpired:
        print(f"Timeout: {script_path} with {algorithm} and SLA {sla} exceeded 2 minutes. Skipping...", file=sys.stderr)
        failed_instances += 1
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path} with {algorithm} and SLA {sla}: {e}", file=sys.stderr)


def main():
    global failed_instances
    start_time = time.time()
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
        # print(" > [Initial run] Algorithm: uniform | SLA: average | Iteration: -1")
        # run_experiment(script_name, 'uniform', 'average', iteration="-1", current=0, total=0)
        
        # Run experiments for each algorithm and SLA combination
        for algorithm in ALGORITHMS:
            for sla in SLAS:
                for i in range(1, ITERATIONS_PER_ALGORITHM + 1):
                    current_run += 1
                    run_experiment(script_name, algorithm, sla, str(i), current_run, total_runs)

    total_time = time.time() - start_time    
    print(f"All runs completed in {total_time/60:.2f}mins | Failed instances: {failed_instances}")

if __name__ == "__main__":
    main()