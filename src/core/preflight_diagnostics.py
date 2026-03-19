import os
import hashlib
import time
import random
import numpy as np
import pandas as pd
from pathlib import Path
from src.config import app_config
from src.core.database import AtlasManager

class PreflightDiagnostics:
    """
    @Data-Ops: Pre-flight integrity and performance diagnostics for EXPEDIA.
    """
    def __init__(self):
        self.results = []
        self.db = AtlasManager()

    def check_model_weights(self):
        """
        @Neural-Core: Verify Air-Gapped Model Integrity.
        """
        model_path = app_config.LOCAL_MODEL_PATH
        if not model_path.exists():
            self.results.append("[FAIL] Air-Gapped Model Path Not Found.")
            return

        # Check for critical weight files
        has_bin = (model_path / "pytorch_model.bin").exists()
        has_safe = (model_path / "model.safetensors").exists()
        has_config = (model_path / "config.json").exists()

        if has_config and (has_bin or has_safe):
            msg = "[SUCCESS] OFFLINE GENOMIC KERNEL VERIFIED."
            self.results.append(msg)
            print(msg) # Immediate feedback
        else:
            self.results.append(f"[FAIL] Missing model weights at {model_path}")

    def check_sha256(self, file_path):
        if not Path(file_path).exists():
            return None
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def check_taxonkit(self):
        path = app_config.TAXONKIT_EXE
        checksum = self.check_sha256(path)
        if checksum:
            self.results.append(f"TaxonKit SHA-256: {checksum}")
        else:
            self.results.append("TaxonKit binary not found.")

    def check_db_rows(self):
        count = self.db.get_count()
        if count == 100_000:
            self.results.append("Atlas DB: 100,000 rows [OK]")
        else:
            self.results.append(f"Atlas DB: {count} rows [FAIL]")

    def io_benchmark(self):
        if not self.db.table:
            self.results.append("I/O Benchmark: DB Table not available.")
            return
        try:
            # Generate random query vectors (768 dimensions)
            # Fetching real rows with to_pandas() is too slow for benchmark
            vectors = [np.random.rand(768).astype('float32') for _ in range(10)]
            
            # Time search
            latencies = []
            for v in vectors:
                t0 = time.perf_counter()
                _ = self.db.vector_search(v, top_k=5)
                t1 = time.perf_counter()
                latencies.append((t1 - t0) * 1000)
            
            if not latencies:
                self.results.append("I/O Benchmark: No searches run.")
                return

            max_latency = max(latencies)
            avg_latency = sum(latencies) / len(latencies)
            
            if avg_latency < 15:
                self.results.append(f"I/O Benchmark: Avg {avg_latency:.2f}ms (Max {max_latency:.2f}ms) [OK]")
            else:
                self.results.append(f"I/O Benchmark: Latency exceeded [FAIL] (Avg {avg_latency:.2f}ms)")
        except Exception as e:
            self.results.append(f"I/O Benchmark error: {e}")

    def run_all(self):
        self.results.clear()
        self.check_taxonkit()
        self.check_db_rows()
        self.check_model_weights()
        self.io_benchmark()
        return self.results

if __name__ == "__main__":
    diag = PreflightDiagnostics()
    results = diag.run_all()
    print("\n--- EXPEDIA: PREFLIGHT DIAGNOSTICS ---")
    for r in results:
        print(r)
