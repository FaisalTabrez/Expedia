import polars as pl
import lancedb
import pyarrow as pa
import os
import glob
import logging
import sys

# Setup Logging for @Data-Ops Persona
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | [DATA-OPS] | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("EXPEDIA.DataOps")

# Paths
RAW_DATA_PATH = r"E:/EXPEDIA_Data/data/raw/"
DB_PATH = r"E:/EXPEDIA_Data/data/db"
TABLE_NAME = "EXPEDIA_ATLAS_500K_UNIQUE"

def main():
    logger.info("Initializing 500k Atlas Rebuild Protocol...")
    logger.info("Target: Global Genomic De-Duplication")

    # 1. Locate Shards
    shard_pattern = os.path.join(RAW_DATA_PATH, "vector_shard_*.parquet")
    shards = sorted(glob.glob(shard_pattern))
    
    if not shards:
        logger.error(f"No shards found at {shard_pattern}")
        return
        
    logger.info(f"Detected {len(shards)} shards. Engaging Polars Engine...")

    # 2. Ingestion & Normalization (Polars)
    try:
        # Load all shards into a single LazyFrame for global cross-shard deduplication
        # Using scan_parquet is memory efficient, but we need to collect for de-duplication
        lf = pl.scan_parquet(shards)
        
        # Collect into memory (500k vectors * 4KB is ~2GB RAM, feasible)
        df = lf.collect()
        
        # Normalize column names to lowercase
        df.columns = [c.lower() for c in df.columns]
        
        initial_count = len(df)
        logger.info(f"Ingested Buffer: {initial_count} records")

    except Exception as e:
        logger.critical(f"Polars Ingestion Failure: {e}")
        return

    # 3. Deduplication Layer
    logger.info("Executing Purge Logic (Vector + Sequence)...")
    
    # Identify key columns
    cols = df.columns
    vec_col = next((c for c in cols if 'vector' in c), None)
    seq_col = next((c for c in cols if 'sequence' in c or 'seq' in c), None)
    id_col = next((c for c in cols if 'id' in c or 'accession' in c), None)

    if not vec_col:
        logger.error("CRITICAL: Vector column not found.")
        return
        
    # Check for list type and cast to string for robust hashing if needed
    # Polars typically handles list comparison well, but string casting ensures exact byte matching
    # and handles any potential floating point oddities by treating the representation as the hash
    
    # --- PHASE 1: VECTOR DEDUPLICATION ---
    # We maintain order to keep the "first" instance (e.g., PQ734708 vs PQ734630)
    
    # Note: If maintain_order=True is slower, we accept it for determinism.
    
    try:
        # Attempt native unique
        df_unique_vec = df.unique(subset=[vec_col], maintain_order=True)
    except Exception:
        logger.warning("Native List Unique failed. Fallback to String Hashing...")
        # Hash Strategy: Cast vector to string (e.g. "[0.1, 0.2...]")
        df = df.with_columns(pl.col(vec_col).cast(pl.Utf8).alias("_vec_hash"))
        df_unique_vec = df.unique(subset=["_vec_hash"], maintain_order=True).drop("_vec_hash")

    count_after_vec_purge = len(df_unique_vec)
    vec_duplicates = initial_count - count_after_vec_purge
    
    logger.info(f"Vector Purge: Dropped {vec_duplicates} redundant signatures.")
    
    # --- PHASE 2: SEQUENCE DEDUPLICATION (Secondary Check) ---
    if seq_col:
        df_final = df_unique_vec.unique(subset=[seq_col], maintain_order=True)
        count_final = len(df_final)
        seq_duplicates = count_after_vec_purge - count_final
        logger.info(f"Syntax Purge: Dropped {seq_duplicates} sequence collisions.")
    else:
        logger.warning("Sequence column missing. Skipping syntactic check.")
        df_final = df_unique_vec
        count_final = count_after_vec_purge

    # 4. Audit Report
    logger.info("=" * 40)
    logger.info(f"PURGE STATISTICS")
    logger.info(f"Final Count: {count_final} (Unique)")
    logger.info(f"Total Purged: {initial_count - count_final}")
    logger.info(f"Efficiency: {100 * (1 - count_final/initial_count):.2f}% Redundancy Removed")
    logger.info("=" * 40)

    # 5. Connect & Create Table
    try:
        db = lancedb.connect(DB_PATH)
        
        # Clean Slate
        if TABLE_NAME in db.table_names():
            logger.info(f"Dropping existing table: {TABLE_NAME}")
            db.drop_table(TABLE_NAME)
            
        logger.info(f"Creating Table: {TABLE_NAME}")
        # Convert to PyArrow for LanceDB ingestion
        arrow_table = df_final.to_arrow()
        tbl = db.create_table(TABLE_NAME, data=arrow_table)
        
        logger.info("Table Created. Commencing Indexing...")
        
    except Exception as e:
        logger.critical(f"Database Operation Failed: {e}")
        return

    # 6. Indexing (IVF-PQ)
    try:
        # Partitions = 512 (Standard for 100k-1M range)
        # Subvectors = 96 (4096 / 96 ~ 42 dims per subvector, decent compression)
        tbl.create_index(
            metric="cosine", 
            vector_column_name=vec_col,
            index_type="IVF_PQ", 
            num_partitions=512, 
            num_sub_vectors=96
        )
        logger.info("IVF-PQ Index Built Successfully.")
    except Exception as e:
        logger.error(f"Indexing Error: {e}")

    logger.info("ATLAS REBUILD COMPLETE.")

if __name__ == "__main__":
    main()
