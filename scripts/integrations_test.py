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
SEED_LOG = "seed.log"
WORKER_LOG = "worker.log"

def wait_for_port(port, timeout=5):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((HOST, port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    return False

def check_log_for_entry(logfile, target_string):
    """Reads a log file and returns True if target_string is found."""
    if not os.path.exists(logfile):
        return False
    with open(logfile, 'r') as f:
        content = f.read()
        return target_string in content

def run_test():
    print(f"--- Starting Distri-C Integration Test ---")
    
    # Clean previous logs
    for f in [SEED_LOG, WORKER_LOG]:
        if os.path.exists(f): os.remove(f)

    # Open file handles for logging
    seed_log_fd = open(SEED_LOG, "w")
    worker_log_fd = open(WORKER_LOG, "w")

    try:
        # 1. Start Seed Node
        print(f"[TEST] Launching Seed Node on {SEED_PORT}...")
        seed_proc = subprocess.Popen(
            [BINARY_PATH, "seed-01", str(SEED_PORT), "-c"],
            stdout=seed_log_fd,
            stderr=subprocess.STDOUT, # Merge stderr into stdout
            text=True
        )
        
        if not wait_for_port(SEED_PORT):
            print("[FAIL] Seed node failed to start.")
            return

        # 2. Start Worker Node
        print(f"[TEST] Launching Worker Node on {NODE_PORT}...")
        worker_proc = subprocess.Popen(
            [BINARY_PATH, "worker-01", str(NODE_PORT), "-w", HOST, str(SEED_PORT)],
            stdout=worker_log_fd,
            stderr=subprocess.STDOUT,
            text=True
        )

        # 3. Allow convergence time
        time.sleep(2)

        # 4. Verify JOIN Success
        # Check Seed logs for acceptance
        if check_log_for_entry(SEED_LOG, f"Accepted connection"):
            print("[PASS] Seed accepted a connection.")
        else:
            print("[FAIL] Seed did not log a connection accept.")

        # Check Seed logs for JOIN_REQ processing (Added in previous turn changes)
        if check_log_for_entry(SEED_LOG, "JOIN_REQ from node worker-01"):
            print("[PASS] Seed received JOIN_REQ from worker-01.")
        else:
            print("[FAIL] Seed did not receive/parse JOIN_REQ.")

        # 5. Test LEAVE Protocol (The "Phantom Node" Fix)
        print("[TEST] Sending SIGTERM to Worker to test Graceful Leave...")
        os.kill(worker_proc.pid, signal.SIGTERM)
        worker_proc.wait() # Wait for worker to finish cleanup
        
        time.sleep(1) # Give seed a moment to process the LEAVE packet

        # Verify Seed received the LEAVE RPC
        if check_log_for_entry(SEED_LOG, "LEAVE request from worker-01"):
            print("[PASS] Seed received graceful LEAVE request.")
        else:
            print("[FAIL] Seed did not receive LEAVE request (Phantom Node issue).")

    finally:
        # Cleanup
        print("[TEST] Cleaning up...")
        if 'seed_proc' in locals() and seed_proc.poll() is None:
            os.kill(seed_proc.pid, signal.SIGTERM)
            seed_proc.wait()
        
        if 'worker_proc' in locals() and worker_proc.poll() is None:
            os.kill(worker_proc.pid, signal.SIGTERM)
            worker_proc.wait()
            
        seed_log_fd.close()
        worker_log_fd.close()
        
        # Optional: Print logs on failure
        # print(open(SEED_LOG).read())

    print("--- Test Complete ---")

if __name__ == "__main__":
    if not os.path.exists(BINARY_PATH):
        print(f"Error: Binary not found at {BINARY_PATH}.")
    else:
        run_test()