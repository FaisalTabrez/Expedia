import os
import gc
import sys
import time
import glob
import logging
import random
import numpy as np
import lancedb
import pandas as pd
import pyarrow as pa

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger("EXPEDIA.Ingest")

# Constants
RAW_DATA_PATH = r"E:/EXPEDIA_Data/data/raw/"
DB_PATH = r"E:/EXPEDIA_Data/data/db"
TABLE_NAME = "EXPEDIA_ATLAS_500K"

def main():
    logger.info("Initializing EXPEDIA 500k Atlas Ingestor...")

    # 1. Connect to DB
    try:
        db = lancedb.connect(DB_PATH)
    except Exception as e:
        logger.error(f"Failed to connect to DB at {DB_PATH}: {e}")
        return

    # 2. Locate Shards
    shard_pattern = os.path.join(RAW_DATA_PATH, "vector_shard_*.parquet")
    shards = sorted(glob.glob(shard_pattern))
    
    if not shards:
        logger.error(f"No shards found matching {shard_pattern}")
        return

    logger.info(f"Found {len(shards)} shards to ingest.")

    # 3. Clean Slate
    if TABLE_NAME in db.table_names():
        logger.warning(f"Dropping existing table: {TABLE_NAME}")
        db.drop_table(TABLE_NAME)

    # 4. Ingestion Loop
    tbl = None
    total_records = 0
    
    for i, shard_path in enumerate(shards):
        logger.info(f"Processing Shard {i+1}/{len(shards)}: {os.path.basename(shard_path)}")
        
        try:
            df = pd.read_parquet(shard_path)
            
            # Normalize Columns (Safety)
            df.columns = [c.lower() for c in df.columns]
            
            # Ensure Vector is list-of-floats for LanceDB
            # (Parquet often stores lists as np.array or similar, LanceDB python expects list or np.ndarray)
            # Just to be safe, no op needed if already correct, but good to check.
            
            record_count = len(df)
            total_records += record_count
            
            if tbl is None:
                # First Shard: Create Table
                tbl = db.create_table(TABLE_NAME, data=df)
            else:
                # Subsequent: Add
                tbl.add(df)

            # MEMORY GUARD: Explicit GC
            del df
            gc.collect()
            
        except Exception as e:
            logger.error(f"Failed to process shard {shard_path}: {e}")
            return

    logger.info(f"Ingestion Complete. Total Records: {total_records}")

    # 5. Indexing
    logger.info("Building IVF-PQ Index (Partitions=512, SubVectors=96)...")
    try:
        tbl.create_index(
            metric="cosine", 
            vector_column_name="vector",
            index_type="IVF_PQ", 
            num_partitions=512, 
            num_sub_vectors=96
        )
    except Exception as e:
        logger.error(f"Indexing Failed: {e}")
        return

    # 6. Auditing
    final_count = len(tbl)
    if final_count != 500000:
        logger.warning(f"Audit Mismatch: Expected 500,000, Found {final_count}")
    else:
        logger.info("Audit Passed: 500,000 Records Confirmed.")

    # 7. Benchmark
    logger.info("Running Performance Benchmark (100 Random Queries)...")
    latencies = []
    
    # Generate 100 random vectors for testing (768-dim)
    # Using numpy for speed
    queries = np.random.rand(100, 768).astype(np.float32)
    # L2 normalize them to match query behavior
    queries /= np.linalg.norm(queries, axis=1)[:, np.newaxis]

    for q in queries:
        t0 = time.perf_counter()
        _ = tbl.search(q).limit(10).to_list() # Execute search
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000.0)

    mean_latency = sum(latencies) / len(latencies)
    logger.info(f"[DB] 500k Indexing Complete. Mean search latency: {mean_latency:.2f}ms.")

if __name__ == "__main__":
    main()
