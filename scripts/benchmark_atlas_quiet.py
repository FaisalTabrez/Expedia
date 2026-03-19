import os
# Suppress LanceDB / Rust logs
os.environ["LANCE_LOG"] = "error"
os.environ["RUST_LOG"] = "error"

import time
import lancedb
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("Bench")

DB_PATH = r"E:/EXPEDIA_Data/data/db"
TABLE_NAME = "EXPEDIA_ATLAS_500K_UNIQUE"

def main():
    try:
        db = lancedb.connect(DB_PATH)
        if TABLE_NAME not in db.table_names():
            logger.error(f"Table {TABLE_NAME} not found.")
            return
            
        tbl = db.open_table(TABLE_NAME)
        count = len(tbl)
        logger.info(f"Connected to {TABLE_NAME} ({count} unique vectors).")
        
        # Benchmark
        np.random.seed(42)
        queries = np.random.randn(100, 4096).astype(np.float32)
        queries = queries / np.linalg.norm(queries, axis=1, keepdims=True)
        
        latencies = []
        
        # Warmup
        tbl.search(queries[0]).limit(10).to_arrow()
        
        for i in range(100):
            t0 = time.time()
            tbl.search(queries[i]).limit(10).to_arrow()
            t1 = time.time()
            latencies.append((t1 - t0) * 1000.0)
            
        mean_lat = np.mean(latencies)
        p95 = np.percentile(latencies, 95)
        
        logger.info(f"Mean Latency: {mean_lat:.2f}ms")
        logger.info(f"P95 Latency:  {p95:.2f}ms")
        
        if mean_lat < 10.0:
            logger.info("STATUS: PASS (< 10ms)")
        else:
            logger.warning("STATUS: FAIL (> 10ms)")
            
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    main()
