import sys
import os
import json
import time
import logging
import traceback
import numpy as np
import pandas as pd

# Configure logging to stderr to keep stdout clean for JSON IPC
# 1. Clear any existing handlers (e.g. from imports)
root_logger = logging.getLogger()
if root_logger.handlers:
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

# 2. Configure global logging to stderr
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr, # Semantic logs go to stderr (console)
    format='%(asctime)s | [KERNEL] | %(levelname)s | %(message)s',
    force=True
)

# 3. Explicitly mute noisy libraries
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logger = logging.getLogger("ScienceKernel")

# Add project root to path
# Assuming this file is in src/core/science_kernel.py
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

def safe_ipc_write(data_str: str):
    """Writes to the real stdout if available."""
    if sys.__stdout__:
        sys.__stdout__.write(data_str)
        sys.__stdout__.flush()

# Import Scientific Stack (Wrapped in try-except for robust fail reporting)
try:
    safe_ipc_write(json.dumps({"type": "log", "message": "Starting Imports..."}) + "\n")

    # WORKAROUND: Python 3.13 importlib regression with transformer imports
    # Pre-loading modules that might trigger metadata scans
    import importlib.metadata
    
    # PATCH: The `transformers` library calls `importlib.metadata.packages_distributions()`
    # on import, which scans every installed package. On Windows/Python 3.13, this can hang
    # or be extremely slow in a subprocess. We monkey-patch it to return a minimal map.
    # This bypasses the disk scan.
    def _dummy_packages_distributions():
        return {"torch": ["torch"], "scipy": ["scipy"], "transformers": ["transformers"]}
    
    importlib.metadata.packages_distributions = _dummy_packages_distributions
    
    safe_ipc_write(json.dumps({"type": "log", "message": "Importing Torch..."}) + "\n")
    import torch

    safe_ipc_write(json.dumps({"type": "log", "message": "Importing Scipy..."}) + "\n")
    import scipy

    safe_ipc_write(json.dumps({"type": "log", "message": "Importing Bio..."}) + "\n")
    from Bio import SeqIO
    # Discovery Engines
    safe_ipc_write(json.dumps({"type": "log", "message": "Importing Config..."}) + "\n")
    from src.config import app_config

    safe_ipc_write(json.dumps({"type": "log", "message": "Importing Embedder..."}) + "\n")
    from src.core.embedder import NucleotideEmbedder

    safe_ipc_write(json.dumps({"type": "log", "message": "Importing Atlas..."}) + "\n")
    from src.core.database import AtlasManager

    safe_ipc_write(json.dumps({"type": "log", "message": "Importing Taxonomy..."}) + "\n")
    from src.core.taxonomy import TaxonomyEngine

    safe_ipc_write(json.dumps({"type": "log", "message": "Importing Discovery..."}) + "\n")
    from src.core.discovery import DiscoveryEngine

    safe_ipc_write(json.dumps({"type": "log", "message": "Imports Complete."}) + "\n")

    # Re-force logging config in case imports messed it up
    root_logger = logging.getLogger()
    for h in root_logger.handlers[:]:
        if isinstance(h, logging.StreamHandler) and (h.stream == sys.stdout or h.stream == sys.__stdout__):
            h.stream = sys.stderr

except ImportError as e:
    logger.critical(f"Failed to import scientific stack: {e}")
    if sys.__stdout__:
        sys.__stdout__.write(json.dumps({
            "type": "error",
            "message": f"Kernel Import Error: {str(e)}",
            "traceback": traceback.format_exc()
        }) + "\n")
        sys.__stdout__.flush()
    sys.exit(1)

class ScienceKernel:
    """
    @Neural-Core: Standalone Scientific Process.
    Wrapper for Nucleotide Transformer & LanceDB to run isolated from PySide6.
    """
    def __init__(self):
        self.embedder = None
        self.db = None
        self.taxonomy = None
        self.discovery = None

    def initialize(self):
        """
        Lazy load heavy models.
        """
        logger.info("Initializing Neural-Core & Vector-Ops engines...")
        self.embedder = NucleotideEmbedder()
        self.db = AtlasManager()
        self.taxonomy = TaxonomyEngine()
        # Initialize discovery engine if needed
        try:
             self.discovery = DiscoveryEngine()
        except Exception as e:
             logger.warning(f"Discovery Engine (HDBSCAN) failed to init: {e}")
             self.discovery = None
             
        logger.info("Science Kernel Ready.")

    def run(self):
        """
        Main IPC Loop. Reads JSON commands from stdin.
        """
        # Signal readiness (handled in __main__ now to ensure fully loaded)
        # print(json.dumps({"type": "status", "status": "ready"}))
        # sys.stdout.flush()

        for line in sys.stdin:
            try:
                line = line.strip()
                if not line:
                    continue
                
                command = json.loads(line)
                cmd_type = command.get("command")

                if cmd_type == "process_fasta":
                    self.process_fasta(command.get("file_path"))
                elif cmd_type == "shutdown":
                    logger.info("Shutdown command received.")
                    break
                else:
                    logger.warning(f"Unknown command: {cmd_type}")

            except json.JSONDecodeError:
                logger.error("Invalid JSON received.")
            except Exception as e:
                logger.error(f"Kernel Loop Error: {e}")
                if sys.__stdout__:
                    sys.__stdout__.write(json.dumps({
                        "type": "error",
                        "message": str(e),
                        "traceback": traceback.format_exc()
                    }) + "\n")
                    sys.__stdout__.flush()

    def process_fasta(self, file_path):
        """
        Re-implementation of DiscoveryWorker logic.
        """
        if not self.embedder:
            self.initialize()

        logger.info(f"Processing FASTA: {file_path}")
        
        if not os.path.exists(file_path):
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({"type": "error", "message": f"File not found: {file_path}"}) + "\n")
                sys.__stdout__.flush()
            return

        batch_size = 32
        batch_seqs = []
        batch_ids = []
        nrt_vectors = []
        nrt_ids = []
        nrt_meta = [] # Store classification/lineage metadata for NRTs

        try:
            total_records = 0
            # Just count for progress estimation (optional)
            
            # Stream Processing
            for record in SeqIO.parse(file_path, "fasta"):
                seq_str = str(record.seq).upper()
                seq_id = record.id
                
                batch_seqs.append(seq_str)
                batch_ids.append(seq_id)

                if len(batch_seqs) >= batch_size:
                    self._process_batch(batch_seqs, batch_ids, nrt_vectors, nrt_ids, nrt_meta)
                    # Clear batch
                    batch_seqs = []
                    batch_ids = []

            # Process remaining
            if batch_seqs:
                self._process_batch(batch_seqs, batch_ids, nrt_vectors, nrt_ids, nrt_meta)

            # Final Step: Discovery on NRTs
            if self.discovery and nrt_vectors:
                # Pass accumulated vectors + ids + metadata
                self._run_discovery(nrt_vectors, nrt_ids, nrt_meta)

            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({"type": "finished", "file_path": file_path}) + "\n")
                sys.__stdout__.flush()

        except Exception as e:
            logger.error(f"Processing Error: {e}")
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({
                    "type": "error",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }) + "\n")
                sys.__stdout__.flush()

    def _process_batch(self, batch_seqs, batch_ids, nrt_container, nrt_id_container, nrt_meta_container=None):
        """
        Embeds -> Searches -> Classifies a batch.
        """
        if not self.embedder:
            logger.error("Embedder not initialized.")
            return

        for i, seq in enumerate(batch_seqs):
            seq_id = batch_ids[i]
            
            # 1. Embed
            try:
                embedding = self.embedder.generate_embedding(seq) # type: ignore
            except Exception as e:
                logger.error(f"Embedding failed for {seq_id}: {e}")
                continue
            
            # 2. Vector Search (LanceDB)
            neighbors = pd.DataFrame()
            if self.db:
                try:
                    neighbors = self.db.vector_search(embedding, top_k=50)
                except Exception as db_err:
                     logger.warning(f"DB Search failed for {seq_id}: {db_err}")

            # 3. Taxonomy Classification
            try:
                analysis = self.taxonomy.analyze_sample(neighbors, seq) # type: ignore
            except Exception as tax_err:
                logger.error(f"Taxonomy analysis failed for {seq_id}: {tax_err}")
                continue

            # Check for NRT
            if analysis.get("status") == "Novel":
                nrt_container.append(embedding)
                nrt_id_container.append(seq_id)
                if nrt_meta_container is not None:
                    nrt_meta_container.append({
                        "id": seq_id,
                        "classification": analysis.get("classification"),
                        "lineage": analysis.get("lineage")
                    })

            # Construct Result
            result = {
                "id": seq_id,
                "sequence_length": len(seq),
                "status": analysis.get("status", "Unknown"),
                "classification": analysis.get("classification", "Unknown"),
                "confidence": float(analysis.get("confidence", 0.0)),
                "lineage": analysis.get("lineage", "Unknown Lineage"),
                "workflow": analysis.get("workflow", "Tier 0")
            }
            
            # Emit result
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({"type": "result", "data": result}) + "\n")
                sys.__stdout__.flush()
            
    def _run_discovery(self, nrt_vectors, nrt_ids, nrt_meta=None):
        """
        Runs HDBSCAN on accumulated NRTs
        """
        if not self.discovery:
            logger.warning("Discovery Engine not initialized. Skipping HDBSCAN.")
            return

        logger.info(f"Running HDBSCAN on {len(nrt_vectors)} NRTs...")
        try:
            # Stack vectors
            vectors_array = np.vstack(nrt_vectors)
            
            # Cluster (pass metadata if available)
            clusters_df = self.discovery.cluster_nrt_batch(vectors_array, nrt_ids, nrt_meta) # type: ignore
            
            clean_data = [] # Declare outside invalid block
            
            if not clusters_df.empty:
                # Convert to dict for JSON
                clusters_data = clusters_df.to_dict(orient="records")
                
                # ---------------------------------------------------------------------
                # IPC HYGIENE: Prepare Data for JSON Serialization
                # ---------------------------------------------------------------------
                for record in clusters_data:
                    # Convert NumPy arrays to lists
                    if 'centroid' in record:
                         if isinstance(record['centroid'], np.ndarray):
                             record['centroid'] = record['centroid'].tolist()
                    
                    # Convert NumPy scalars (int64/float32) to Python natives
                    if 'size' in record:
                        record['size'] = int(record['size'])
                        
                    # Ensure IDs are strings
                    if 'ntu_id' in record:
                        record['ntu_id'] = str(record['ntu_id'])
                        
                    # Convert lists in members if needed (usually fine)
                    
                    clean_data.append(record)

            # ALWAYS Emit Result (Even if empty, to unblock UI)
            json_str = json.dumps({
                "type": "discovery_results",
                "data": clean_data
            })
            
            if sys.__stdout__:
                sys.__stdout__.write(json_str + "\n")
                sys.__stdout__.flush()
                
        except Exception as e:
            logger.error(f"Discovery phase failed: {e}")
            logger.error(traceback.format_exc())
            # Send empty result to unblock UI
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({
                    "type": "discovery_results",
                    "data": []
                }) + "\n")
                sys.__stdout__.flush()

if __name__ == "__main__":
    
    # -------------------------------------------------------------------------
    # STDIO HYGIENE: Redirect 'print' to stderr by default to catch rogue libs
    # ---------------------------------------------------------------------
    # Save original stdout for IPC. Assert it exists (we are a CLI tool).
    ipc_stdout = sys.__stdout__
    if ipc_stdout is None:
        # Fallback to current stdout if __stdout__ is somehow missing, though rare
        ipc_stdout = sys.stdout

    # Redirect global stdout to stderr so ANY unhandled print() goes to logs, not IPC
    sys.stdout = sys.stderr
    
    # Force minimal logging format to stderr for clarity
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, force=True)

    def send_ipc(data: dict):
        """Helper to send valid JSON to the actual stdout pipe."""
        if ipc_stdout is None:
            return
            
        try:
            json_str = json.dumps(data)
            ipc_stdout.write(json_str + "\n")
            ipc_stdout.flush()
        except Exception as e:
            sys.stderr.write(f"IPC Error: {e}\n")

    try:
        kernel = ScienceKernel()
        logger.info("Science Kernel Online. Waiting for commands.")
        
        # Announce readiness
        send_ipc({"type": "status", "status": "ready"})
        
        kernel.run()
    except Exception as critical_error:
        sys.stderr.write(f"CRITICAL KERNEL CRASH: {critical_error}\n")
        sys.stderr.write(traceback.format_exc())
        sys.exit(1)
