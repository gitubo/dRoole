import subprocess
import time
import os
import signal
import sys
import re

# Configuration
BINARY_PATH = "./build/server"
BASE_PORT = 9000
NUM_NODES = 3
LOG_DIR = "logs_raft"
STARTUP_WAIT = 2
ELECTION_TIMEOUT = 10  # Seconds to wait for a leader to emerge

def ensure_build():
    if not os.path.exists(BINARY_PATH):
        print(f"Error: Binary not found at {BINARY_PATH}")
        print("Please run: cmake . && make")
        sys.exit(1)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def start_node(node_id, port, seed_ip=None, seed_port=None):
    cmd = [
        BINARY_PATH,
        "--node-id", f"raft-{node_id}",
        "--port", str(port),
        "--role", "CONTROL" # Raft only runs on CONTROL nodes
    ]
    
    if seed_ip and seed_port:
        cmd.extend(["--join", f"{seed_ip}:{seed_port}"])

    log_file = open(f"{LOG_DIR}/node_{node_id}.log", "w")
    print(f"[TEST] Starting raft-{node_id} on port {port}...")
    
    # Launch process
    proc = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid # Create new session group for clean kill
    )
    return proc, log_file

def check_logs_for_leader(node_ids):
    leaders = []
    terms = {}

    print(f"[TEST] Scanning logs for Leader Election results (Timeout: {ELECTION_TIMEOUT}s)...")
    
    start_time = time.time()
    while time.time() - start_time < ELECTION_TIMEOUT:
        current_leaders = []
        
        for nid in node_ids:
            with open(f"{LOG_DIR}/node_{nid}.log", "r") as f:
                content = f.read()
                # Look for state transition to LEADER
                # Adjust regex based on your exact logging in raft_core.c
                if "State transition: CANDIDATE -> LEADER" in content or "State changed to: LEADER" in content:
                    current_leaders.append(nid)
                
                # Try to grab the term
                term_match = re.findall(r"Current Term is now (\d+)", content)
                if term_match:
                    terms[nid] = int(term_match[-1])

        if len(current_leaders) > 0:
            leaders = current_leaders
            # If we found a leader, wait a tiny bit to ensure stability then break
            if len(leaders) == 1:
                time.sleep(1) 
                break
        
        time.sleep(0.5)

    return leaders, terms

def run_test():
    ensure_build()
    procs = []
    files = []
    
    try:
        # 1. Start Seed Node (Node 0)
        p0, f0 = start_node(0, BASE_PORT)
        procs.append(p0); files.append(f0)
        time.sleep(1)

        # 2. Start Node 1 (Joins Node 0)
        p1, f1 = start_node(1, BASE_PORT + 1, "127.0.0.1", BASE_PORT)
        procs.append(p1); files.append(f1)
        
        # 3. Start Node 2 (Joins Node 0)
        p2, f2 = start_node(2, BASE_PORT + 2, "127.0.0.1", BASE_PORT)
        procs.append(p2); files.append(f2)

        print("[TEST] Cluster started. Waiting for convergence...")
        time.sleep(3)

        # 4. Verification
        leaders, terms = check_logs_for_leader([0, 1, 2])
        
        print("-" * 40)
        print(f"Results:")
        print(f"Leaders found: {leaders}")
        print(f"Terms observed: {terms}")
        print("-" * 40)

        if len(leaders) == 0:
            print("FAILURE: No leader elected.")
            sys.exit(1)
        elif len(leaders) > 1:
            print(f"FAILURE: Split brain detected! Multiple leaders: {leaders}")
            sys.exit(1)
        else:
            print(f"SUCCESS: Exactly one leader elected (Node {leaders[0]}).")
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("[TEST] Shutting down nodes...")
        for p in procs:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
        for f in files:
            f.close()

if __name__ == "__main__":
    run_test()