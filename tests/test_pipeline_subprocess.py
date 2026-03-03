import subprocess
import sys
import os
import json
import time
import threading

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
        text=True,  # Handle text streams
        bufsize=0   # Unbuffered for real-time output
    )

    # Thread to capture stderr (logs) separately
    def log_monitor(proc):
        for line in proc.stderr:
            print(f"[KERNEL LOG] {line.strip()}")
    
    log_thread = threading.Thread(target=log_monitor, args=(process,))
    log_thread.daemon = True
    log_thread.start()

    try:
        # Wait specifically for the "ready" signal from the kernel
        print("Waiting for kernel readiness...")
        start_wait = time.time()
        ready = False
        while not ready and (time.time() - start_wait < 30): # 30s timeout for model loading
            line = process.stdout.readline()
            if not line:
                continue
            
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

        # Send Process Command
        test_file = r"C:\Volume D\DeepBio_Edgev4\data\raw\discovery_dark_taxa.fasta"
        command = {
            "command": "process_fasta",
            "file_path": test_file
        }
        
        cmd_str = json.dumps(command) + "\n"
        print(f"Sending Command: {cmd_str.strip()}")
        process.stdin.write(cmd_str)
        process.stdin.flush()

        # Listen for results
        print("Listening for processing results...")
        start_process = time.time()
        while (time.time() - start_process < 120): # 2 min timeout
            line = process.stdout.readline()
            if not line:
                break
            
            try:
                msg = json.loads(line)
                msg_type = msg.get("type")
                print(f"[KERNEL IPC] {msg_type}: {str(msg)[:200]}...") # Truncate large payloads
                
                if msg_type == "batch_complete" or msg_type == "error":
                    # We might get multiple batch_completes or partial updates
                    pass
                
                if msg_type == "discovery_summary" or msg.get("status") == "idle":
                    print("Process cycle seems complete.")
                    break

            except json.JSONDecodeError:
                print(f"[KERNEL RAW] {line.strip()}")
                
    except Exception as e:
        print(f"Test Exception: {e}")
    finally:
        print("Stopping Kernel...")
        process.kill()
        process.wait()

if __name__ == "__main__":
    run_test()
