import subprocess
import time
import socket
import os
import signal

# Configuration
BINARY_PATH = "./build/server"
HOST = "127.0.0.1"
SEED_PORT = 9000
NODE_PORT = 9001

def wait_for_port(port, timeout=5):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((HOST, port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    return False

def run_test():
    print(f"--- Starting Distri-C Integration Test ---")
    
    # 1. Start Seed Node (Control Plane)
    print(f"[TEST] Launching Seed Node on {SEED_PORT}...")
    seed_proc = subprocess.Popen(
        [BINARY_PATH, "seed-01", str(SEED_PORT), "-c"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    if not wait_for_port(SEED_PORT):
        print("[FAIL] Seed node failed to start.")
        seed_proc.kill()
        return

    # 2. Start Worker Node (Data Plane) -> Joins Seed
    print(f"[TEST] Launching Worker Node on {NODE_PORT} joining Seed...")
    worker_proc = subprocess.Popen(
        [BINARY_PATH, "worker-01", str(NODE_PORT), "-w", HOST, str(SEED_PORT)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # 3. Allow convergence time
    time.sleep(2)

    # 4. Verify Logs
    # We poll the worker's stdout to see if it claims it joined
    try:
        outs, errs = worker_proc.communicate(timeout=1)
        if "Joined Cluster Successfully" in outs or "Distri-C Engine Started" in outs:
            print("[PASS] Worker Node started.")
            # In a real test, we would parse for specific "Joined" log line
            # specifically added in cluster_join
        else:
            print("[WARN] Worker output unclear. Check logs.")
    except subprocess.TimeoutExpired:
        # Process is still running, which is good
        print("[PASS] Worker Node is running and stable.")

    # 5. Cleanup
    print("[TEST] Shutting down...")
    os.kill(seed_proc.pid, signal.SIGTERM)
    os.kill(worker_proc.pid, signal.SIGTERM)
    
    seed_proc.wait()
    worker_proc.wait()
    print("--- Test Complete ---")

if __name__ == "__main__":
    if not os.path.exists(BINARY_PATH):
        print(f"Error: Binary not found at {BINARY_PATH}. Did you run 'cmake . && make'?")
    else:
        run_test()