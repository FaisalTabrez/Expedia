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
    manifold_ready = Signal(dict)           # Emits Localized Topology JSON
    finished = Signal()

    def __init__(self):
        super().__init__()
        self._is_running = False
        self._process = None

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
                
        except Exception as e:
            self.error.emit(str(e))
            self.finished.emit()
            return

        # 2. Launch Science Kernel
        kernel_path = os.path.join(os.path.dirname(__file__), "science_kernel.py")
        cmd = [sys.executable, kernel_path]
        
        # Ensure PYTHONPATH includes project root AND Sync Hardware Anchor
        env = os.environ.copy()
        
        # @Data-Ops: Sync Root Path to Child Process
        env["EXPEDIA_ROOT_PATH"] = str(app_config.DATA_ROOT)
        
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = project_root + os.pathsep + env["PYTHONPATH"]
        else:
            env["PYTHONPATH"] = project_root

        logger.info(f"Launching Science Kernel: {kernel_path} [Root: {app_config.DATA_ROOT}]")
        
        try:
            # Popen with piping
            self._process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=sys.stderr, # Forward kernel logs to main console
                env=env,
                text=True,
                bufsize=1, # Line buffered
                encoding='utf-8' # Force UTF-8 for JSON
            )
            
            # Assert pipes for type safety
            if not self._process.stdout or not self._process.stdin:
                raise RuntimeError("Failed to open pipes to Science Kernel")

            # 3. Handshake & Command
            # Read stdout until {"type": "status", "status": "ready"}
            while True:
                ready_line = self._process.stdout.readline()
                if not ready_line:
                    # Check if process exited with error
                    if self._process.poll() is not None:
                         raise RuntimeError(f"Science Kernel process exited with code {self._process.returncode}")
                    raise RuntimeError("Science Kernel failed to start (EOF).")
                
                try:
                    msg = json.loads(ready_line)
                    if msg.get("type") == "log":
                         logger.info(f"KERNEL LOG: {msg.get('message')}")
                         # self.progress.emit(0) # Keep signals valid
                    elif msg.get("type") == "status" and msg.get("status") == "ready":
                         logger.info("Kernel reported READY status.")
                         break
                    else:
                         logger.warning(f"Unexpected startup JSON: {msg}")
                except json.JSONDecodeError:
                     logger.warning(f"Kernel sent non-JSON on startup: {ready_line.strip()}")

            logger.info("Kernel Ready. Sending process command...")
            
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
                # Blocking read (line by line)
                line = self._process.stdout.readline()
                
                if not line and self._process.poll() is not None:
                    break # Process ended
                
                if not line:
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
                        # We could also expose isolated taxa if needed, but UI primarily wants clusters

                    elif msg_type == "manifold_data":
                        # Manifold Output
                        logger.info("Localized Manifold Calculated.")
                        self.manifold_ready.emit(message)

                    elif msg_type == "finished":
                        logger.info("Kernel finished processing.")
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
            self.batch_complete.emit(results_buffer, ntu_clusters)
            self.finished.emit()

        except Exception as e:
            logger.error(f"Worker Orchestration Error: {e}")
            self.error.emit(str(e))
            self.finished.emit()
        finally:
            self.stop_kernel()
            self._is_running = False

    def request_localized_topology(self, payload: dict):
        """
        Runs the 'get_localized_topology' command cleanly.
        Spawns a transient Kernel process to avoid blocking the main ingestion pipeline.
        """
        if not payload: return

        vector = payload.get("vector")
        record_id = payload.get("id", "Unknown")
        
        # 1. Start Transient Subprocess
        python_exe = sys.executable 
        kernel_path = Path(__file__).parent / "science_kernel.py"
        
        # Env setup (same as main worker)
        env = os.environ.copy()
        env["EXPEDIA_ROOT_PATH"] = str(app_config.DATA_ROOT)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = project_root + os.pathsep + env["PYTHONPATH"]
        else:
            env["PYTHONPATH"] = project_root

        logger.info(f"Worker: Launching Topology Kernel for {record_id}...")
        
        try:
            # We use a separate process variable to not clobber the main self._process if running
            topo_process = subprocess.Popen(
                [python_exe, str(kernel_path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=sys.stderr, # Forward stderr to console for debugging
                env=env,
                text=True,
                bufsize=1,
                encoding='utf-8'
            )
            
            if not topo_process.stdout or not topo_process.stdin:
                logger.error("Failed to open pipes for topology kernel.")
                return

            # Handshake
            while True:
                line = topo_process.stdout.readline()
                if not line: break
                try:
                    msg = json.loads(line)
                    if msg.get("status") == "ready": break
                except: pass

            # Send Command
            cmd = json.dumps({
                "command": "get_localized_topology",
                "vector": vector,
                "id": record_id,
                "k": 500
            })
            topo_process.stdin.write(cmd + "\n")
            topo_process.stdin.flush()
            
            # Read Response
            while True:
                line = topo_process.stdout.readline()
                if not line: break
                
                logger.info(f"[WORKER] Received manifold data packet: {len(line)} bytes.")
                try:
                    msg = json.loads(line)
                    if msg.get("type") == "localized_manifold":
                        # Success
                        self.manifold_ready.emit(msg)
                        break
                    elif msg.get("type") == "error":
                        self.error.emit(msg.get("message"))
                        break
                except: pass
            
            # Cleanup
            topo_process.terminate()

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
