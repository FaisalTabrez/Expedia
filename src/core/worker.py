import logging
import json
import os
import sys
import subprocess
from pathlib import Path
from PySide6.QtCore import QObject, Signal

# @BioArch-Pro: Subprocess Orchestration
# We no longer import the scientific stack directly to avoid DLL Hell.
# from .embedder import NucleotideEmbedder  <-- REMOVED
# from .database import AtlasManager        <-- REMOVED
# from .taxonomy import TaxonomyEngine      <-- REMOVED
from ..config import app_config

logger = logging.getLogger("EXPEDIA.Worker")

class DiscoveryWorker(QObject):
    """
    @BioArch-Pro: Asynchronous Worker for eDNA Pipeline (Subprocess Mode).
    Launches 'src/core/science_kernel.py' as a separate process to isolate
    the Python 3.13 Scientific Stack from the PySide6 UI thread.
    
    Communication: JSON-RPC over Stdin/Stdout.
    """

    # Signals
    started = Signal()
    sequence_processed = Signal(dict)       # Emits individual result
    batch_complete = Signal(list, list)     # Emits (all_results, ntu_clusters)
    error = Signal(str)
    progress = Signal(int)
    kernel_log = Signal(str)                # New signal for raw kernel logs
    localized_topology_ready = Signal(dict) # Emits Localized Topology JSON
    finished = Signal()
    kernel_ready = Signal()                 # Emits when science kernel is fully loaded
    status_update = Signal(str)             # Emits boot status messages

    def __init__(self):
        super().__init__()
        self._is_running = False
        self._process = None

    def startup_kernel(self):
        """
        Public slot to boot the kernel on app launch.
        Captures the 53s 'Importing Torch' phase and reports status.
        """
        try:
            self._ensure_kernel_started(emit_status=True)
        except Exception as e:
            self.error.emit(f"Kernel Boot Failed: {e}")

    def _ensure_kernel_started(self, emit_status=False):
        """
        Idempotent function to ensure the Science Kernel process is running.
        """
        if self._process is not None:
             if self._process.poll() is None:
                 if emit_status: self.kernel_ready.emit()
                 return # Already running
             else:
                 logger.warning(f"Kernel process died with code {self._process.returncode}. Restarting...")

        # Launch Science Kernel
        # Determine strict path to kernel (relative to this file)
        kernel_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "science_kernel.py"))
        cmd = [sys.executable, kernel_path]
        
        # Ensure PYTHONPATH includes project root AND Sync Hardware Anchor
        env = os.environ.copy()
        
        # @Data-Ops: Sync Root Path to Child Process
        env["EXPEDIA_ROOT_PATH"] = str(app_config.DATA_ROOT)
        
        # Fix PYTHONPATH to include project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        if "PYTHONPATH" in env:
             env["PYTHONPATH"] = project_root + os.pathsep + env["PYTHONPATH"]
        else:
             env["PYTHONPATH"] = project_root
             
        # Add DLL paths on Windows to find OpenMP/BLAS
        if sys.platform == "win32":
            library_paths = [
                 os.path.join(sys.prefix, 'Library', 'bin'),
                 os.path.join(sys.prefix, 'bin')
            ]
            env["PATH"] = os.pathsep.join(library_paths) + os.pathsep + env.get("PATH", "")

        logger.info(f"Launching Science Kernel: {kernel_path}")
        if emit_status: self.status_update.emit("Booting Science Kernel...")
        
        try:
            # Popen with piping
            self._process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=sys.stderr, # Forward fatal errors to console
                env=env,
                text=True,
                bufsize=1, # Line buffered
                encoding='utf-8' # Force UTF-8 for JSON
            )
            
            # Handshake Loop (~50s of Imports)
            while True:
                ready_line = self._process.stdout.readline()
                if not ready_line:
                    if self._process.poll() is not None:
                         raise RuntimeError(f"Science Kernel process exited with code {self._process.returncode}")
                    continue # Spin wait? No, readline blocks. 
                
                try:
                    # Parse Log vs Status
                    # The kernel emits {"type": "log", "message": "Importing Torch..."}
                    msg = json.loads(ready_line)
                    
                    if msg.get("type") == "log":
                         text = msg.get('message', '')
                         logger.info(f"KERNEL LOG: {text}")
                         if emit_status: self.status_update.emit(text)
                         
                    elif msg.get("type") == "status" and msg.get("status") == "ready":
                         logger.info("Kernel reported READY status.")
                         if emit_status: 
                             self.status_update.emit("Science Kernel Ready.")
                             self.kernel_ready.emit()
                         break
                         
                    elif msg.get("type") == "error":
                         err_msg = msg.get("message")
                         if emit_status: self.status_update.emit(f"Kernel Error: {err_msg}")
                         logger.error(f"KERNEL ERROR: {err_msg}")

                except json.JSONDecodeError:
                     # Raw print debugging
                     if ready_line.strip():
                        logger.warning(f"Raw Kernel Output: {ready_line.strip()}")
                     
        except Exception as e:
            logger.error(f"Failed to start Science Kernel: {e}")
            if emit_status: self.status_update.emit(f"Kernel Integrity Failure: {e}")
            self._process = None
            raise

    def run_inference(self, file_path: str):
        """
        Main execution loop.
        Launches the Science Kernel and pipes data.
        """
        self._is_running = True
        self.started.emit()
        
        results_buffer = []
        ntu_clusters = []
        
        # 1. Pre-Flight: Count records for progress bar
        total_records = 0
        try:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
                
            with open(path, "r") as f:
                for line in f:
                    if line.startswith(">"):
                        total_records += 1
            
            if total_records == 0:
                raise ValueError("No sequences found in FASTA file.")
                
            # If kernel crashed or wasn't started, boot it now (blocking UI for imports if not ready)
            self._ensure_kernel_started()
                
        except Exception as e:
            self.error.emit(str(e))
            self.finished.emit()
            return

        if self._process is None or self._process.stdin is None:
            self.error.emit("Kernel initialization failed.")
            self.finished.emit()
            return
            
        logger.info("Sending process command...")
        
        # Send Command
        command = json.dumps({
            "command": "process_fasta",
            "file_path": str(path)
        })
        self._process.stdin.write(command + "\n")
        self._process.stdin.flush()
        
        # 4. Event Loop (Reading Output)
        processed_count = 0
        
        while self._is_running:
            if self._process is None or self._process.stdout is None:
                break

            # Blocking read (line by line)
            line = self._process.stdout.readline()
            
            if not line:
                if self._process.poll() is not None:
                    break # Process ended unexpectedly
                continue

            try:
                message = json.loads(line)
                msg_type = message.get("type")
                
                if msg_type == "result":
                    # Standard Result
                    data = message.get("data")
                    results_buffer.append(data)
                    self.sequence_processed.emit(data)
                    
                    processed_count += 1
                    pct = int((processed_count / total_records) * 100)
                    self.progress.emit(pct)
                    
                elif msg_type == "discovery_results":
                    # HDBSCAN Output
                    # Legacy format support
                    ntu_clusters.extend(message.get("data", []))
                    logger.info(f"Received {len(ntu_clusters)} Discovery Clusters from Kernel (Legacy).")

                elif msg_type == "batch_discovery_summary":
                    # Full Satellite Cluster Aggregation
                    ntus = message.get("ntus", [])
                    isolated_count = message.get("isolated_count", 0)
                    
                    logger.info(f"Received Discovery Summary: {len(ntus)} NTUs, {isolated_count} Isolated.")
                    
                    # Store for batch_complete emit
                    ntu_clusters = ntus 

                elif msg_type == "status" and message.get("status") == "idle":
                    logger.info("Kernel reported IDLE. Batch complete.")
                    break

                elif msg_type == "finished": # Legacy fallback
                    logger.info("Kernel finished processing (Legacy Signal).")
                    break
                    
                elif msg_type == "error":
                    err_msg = message.get("message", "Unknown Kernel Error")
                    trace = message.get("traceback", "")
                    logger.error(f"Kernel Error: {err_msg}\n{trace}")
                    self.error.emit(f"Science Kernel: {err_msg}")
                    break
                    
            except json.JSONDecodeError:
                # Robust parsing: Treat non-JSON lines as log messages
                raw_msg = line.strip()
                if raw_msg:
                    logger.info(f"KERNEL RAW: {raw_msg}")
                    self.kernel_log.emit(raw_msg)
        
        # 5. Cleanup
        # DO NOT STOP KERNEL HERE. IT MUST REMAIN ALIVE FOR TOPOLOGY.
        self.batch_complete.emit(results_buffer, ntu_clusters)
        self.finished.emit()
        self._is_running = False

    def request_localized_topology(self, payload: dict):
        """
        Runs the 'get_localized_topology' command cleanly.
        Now reuses the persistent Kernel process.
        """
        if not payload: return

        vector = payload.get("vector")
        record_id = payload.get("id", "Unknown")
        
        try:
            self._ensure_kernel_started()
            
            if self._process is None or self._process.stdin is None:
                self.error.emit("Kernel unavailable for topology request.")
                return

            # Send Command
            cmd = json.dumps({
                "command": "get_localized_topology",
                "vector": vector,
                "id": record_id,
                "k": 500
            })
            self._process.stdin.write(cmd + "\n")
            self._process.stdin.flush()
            
            logger.info("[WORKER] Command sent to Kernel. Awaiting Manifold JSON...")
            
            # Read Response
            while True:
                if self._process is None or self._process.stdout is None:
                     self.error.emit("Kernel disconnected during topology request.")
                     break

                line = self._process.stdout.readline()
                if not line: break
                
                # Check for empty lines to avoid spamming
                if not line.strip(): continue 

                logger.info(f"[WORKER] Received packet: {len(line)} bytes.")
                try:
                     # Robust JSON decoding
                    msg = json.loads(line)
                    
                    # Check for IDLE status which means done or aborted
                    if msg.get("type") == "status" and msg.get("status") == "idle":
                         # Should have received topology before this
                         break

                    if msg.get("type") == "localized_manifold":
                        # Standard Payload
                        self.localized_topology_ready.emit(msg)
                        # Don't break yet, wait for IDLE signal to keep sync or break if we only expect one response?
                        # The Kernel emits IDLE *after* the command.
                        # So we can break here if we trust one response per command. 
                        # But better to consume until idle to clear pipe?
                        # Actually standard practice: break on result, but what about the "idle" message following it?
                        # If we leave "idle" in the pipe, next read will see it.
                        pass
                        
                    elif msg.get("type") == "localized_manifold_ready":
                        # Disk Handshake (Large Payload)
                        logger.info("[WORKER] Large payload detected. Successfully offloaded to disk handshake.")
                        file_path = msg.get("file_path")
                        
                        try:
                            # Asynchronous File Read
                            if os.path.exists(file_path):
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    data = json.load(f)
                                
                                self.localized_topology_ready.emit(data)
                                
                                # Cleanup to keep drive clean
                                try:
                                    os.remove(file_path)
                                except Exception as clean_err:
                                    logger.warning(f"Failed to delete temp handshake file: {clean_err}")
                            else:
                                logger.error(f"Handshake file missing: {file_path}")
                                self.error.emit("Protocol Error: Handshake file missing")
                                
                        except Exception as file_err:
                            logger.error(f"Handshake Read Failed: {file_err}")
                            self.error.emit(f"Handshake Failed: {file_err}")
                            
                        # Same logic about IDLE signal...

                    elif msg.get("type") == "error":
                        self.error.emit(msg.get("message"))
                        # Consuming until idle is safer
                        
                except json.JSONDecodeError:
                    # Might be a log message from kernel
                    logger.info(f"[KERNEL LOG] {line.strip()}")
                except Exception as e:
                    logger.warning(f"Worker Parse Error: {e}")
            
            # Since we are reusing the process, we CANNOT do:
            # while True: readline()
            # Because it will block forever waiting for next command output.
            # So we MUST break when we get the response OR the "idle" signal.
            
            # Rethink loop: Loop until IDLE.
            pass

        except Exception as e:
            logger.error(f"Topology Request Failed: {e}")
            self.error.emit(str(e))


    def stop_kernel(self):
        """Terminates the subprocess safely."""
        if hasattr(self, '_process') and self._process is not None:
            logger.info("Terminating Science Kernel...")
            
            # Try graceful shutdown
            if self._process.poll() is None:
                try:
                    if self._process.stdin:
                        self._process.stdin.write(json.dumps({"command": "shutdown"}) + "\n")
                        self._process.stdin.flush()
                        
                        # Close pipes explicitly
                        self._process.stdin.close()
                        if self._process.stdout: self._process.stdout.close()
                        if self._process.stderr: self._process.stderr.close()
                        
                        try:
                            self._process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            pass
                except (IOError, BrokenPipeError, OSError, ValueError):
                    pass
            
            # Force kill if still running
            try:
                if self._process.poll() is None:
                    self._process.kill()
                    self._process.wait()
            except (OSError, ProcessLookupError, ValueError):
                    pass
            
            self._process = None
            logger.info("[WORKER] Science Kernel process cleaned up safely.")

    def stop(self):
        """Request worker to stop."""
        self._is_running = False
        self.stop_kernel()
