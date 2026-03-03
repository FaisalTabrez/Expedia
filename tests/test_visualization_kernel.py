import subprocess
import sys
import os
import json
import time
import threading
import random

def run_test():
    script_path = os.path.join("src", "core", "science_kernel.py")
    if not os.path.exists(script_path):
        print(f"Error: Script not found at {script_path}")
        return

    # Start the kernel process
    print("Starting Science Kernel subprocess...")
    process = subprocess.Popen(
        [sys.executable, script_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=0
    )

    # Thread to capture stderr
    def log_monitor(proc):
        for line in proc.stderr:
            print(f"[KERNEL LOG] {line.strip()}")
    
    log_thread = threading.Thread(target=log_monitor, args=(process,))
    log_thread.daemon = True
    log_thread.start()

    try:
        # Wait for readiness
        print("Waiting for kernel readiness...")
        start_wait = time.time()
        ready = False
        while not ready and (time.time() - start_wait < 30):
            line = process.stdout.readline()
            if not line: continue
            
            try:
                msg = json.loads(line)
                print(f"[KERNEL IPC] {msg}")
                if msg.get("status") == "ready":
                    ready = True
            except json.JSONDecodeError:
                print(f"[KERNEL RAW] {line.strip()}")

        if not ready:
            print("Timeout waiting for kernel ready signal.")
            process.kill()
            return

        # Prepare Topology Request
        # 768-dim random vector
        vector = [random.random() for _ in range(768)]
        
        command = {
            "command": "get_localized_topology",
            "vector": vector,
            "id": "TEST-TOPOLOGY-REQUEST",
            "k": 500
        }
        
        cmd_str = json.dumps(command) + "\n"
        print(f"Sending Topology Request...")
        process.stdin.write(cmd_str)
        process.stdin.flush()

        # Listen for results
        print("Listening for topology results...")
        start_process = time.time()
        while (time.time() - start_process < 60):
            line = process.stdout.readline()
            if not line: break
            
            try:
                msg = json.loads(line)
                msg_type = msg.get("type")
                print(f"[KERNEL IPC] {msg_type}: {str(msg)[:200]}...")
                
                if msg_type == "localized_manifold_ready":
                    file_path = msg.get("file_path")
                    print(f"SUCCESS: Handshake received pointing to {file_path}")
                    
                    if os.path.exists(file_path):
                        print("Verifying file content...")
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                        print(f"File contains valid JSON with {len(data.get('neighbors', []))} neighbors.")
                        print(f"Query Coords: {data.get('query', {}).get('coords')}")
                    else:
                        print("ERROR: Handshake file not found!")
                    
                    # We can stop now
                    break

                if msg_type == "error":
                    print("ERROR Received!")
                    break

            except json.JSONDecodeError:
                print(f"[KERNEL RAW] {line.strip()}")

        # Clean exit
        stop_cmd = json.dumps({"command": "shutdown"}) + "\n"
        process.stdin.write(stop_cmd)
        process.stdin.flush()
                
    except Exception as e:
        print(f"Test Exception: {e}")
    finally:
        print("Stopping Kernel...")
        process.kill()
        process.wait()

if __name__ == "__main__":
    run_test()
