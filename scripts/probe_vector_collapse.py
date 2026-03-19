import logging
import lancedb
import numpy as np
import pandas as pd
import sys

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | [DIAGNOSTIC] | %(message)s')
logger = logging.getLogger("EXPEDIA.CollapseProbe")

# Constants
DB_PATH = r"E:/EXPEDIA_Data/data/db"
TABLE_NAME = "EXPEDIA_ATLAS_500K"

def main():
    logger.info("Initializing Collapse Probe (Data-Ops & Neural-Core)...")
    
    # 1. Connect
    try:
        db = lancedb.connect(DB_PATH)
        # Handle deprecation
        tables = db.table_names()
        if TABLE_NAME not in tables:
            logger.critical(f"Table {TABLE_NAME} not found. Available: {tables}")
            return
        tbl = db.open_table(TABLE_NAME)
    except Exception as e:
        logger.error(f"Connection Failed: {e}")
        return

    # 2. Extract Sample (10k)
    logger.info("Fetching strict random sample (N=10,000)...")
    # LanceDB iterators are clean; taking head is fine for collapse check if it's systematic
    df = tbl.search().limit(10000).to_pandas()
    
    if df.empty:
        logger.error("Database is empty.")
        return

    logger.info(f"Columns Found: {df.columns.tolist()}")

    # 3. Vector Extraction
    try:
        # Vectors are list-of-floats in pandas
        vectors = np.stack(df['vector'].values)
        
        # ID Column Detection
        id_col = 'id'
        if 'id' not in df.columns:
            if 'accessionid' in df.columns: id_col = 'accessionid'
            elif 'seq_id' in df.columns: id_col = 'seq_id'
        
        ids = df[id_col].values
        
        # Classification Column Detection
        class_col = 'classification'
        if 'classification' not in df.columns:
            if 'scientificname' in df.columns: class_col = 'scientificname'
            
        classifications = df[class_col].values
    except Exception as e:
        logger.error(f"Vector extraction failed: {e}")
        return

    logger.info(f"Analyzed Shape: {vectors.shape}")

    # 4. Uniqueness Check
    # Convert to bytes for fast hashing/unique check or just use unique rows
    unique_vectors = np.unique(vectors, axis=0)
    unique_count = unique_vectors.shape[0]
    total_count = vectors.shape[0]
    
    logger.info("-" * 40)
    logger.info(f"TOTAL VECTORS : {total_count}")
    logger.info(f"UNIQUE VECTORS: {unique_count}")
    logger.info("-" * 40)

    if unique_count < 9000:
        logger.critical("ALERT: MASSIVE VECTOR COLLAPSE DETECTED.")
        logger.critical(f"Redundancy Rate: {100 * (1 - unique_count/total_count):.2f}%")
    else:
        logger.info("Uniqueness Check: PASS")

    # 5. Variance Analysis (Neural-Core)
    # Check if vectors are just zero or uniform noise
    std_dev = np.std(vectors)
    mean_val = np.mean(vectors)
    
    logger.info(f"Global Mean: {mean_val:.6f}")
    logger.info(f"Global Std:  {std_dev:.6f}")

    if std_dev < 0.0001:
        logger.critical("ALERT: VARIANCE COLLAPSE (Model Outputting Constants?)")
    elif std_dev < 0.01:
        logger.warning("WARNING: Low Variance (Potential Normalization Issue)")
    else:
        logger.info("Variance Check: PASS")

    # 6. Linkage Audit
    logger.info("Linkage Audit (First 5 Rows):")
    for i in range(5):
        # Slicing first 10 dims
        vec_slice = vectors[i][:10]
        vec_str = ", ".join([f"{x:.4f}" for x in vec_slice])
        logger.info(f"ID: {ids[i]} | Taxon: {classifications[i]}")
        logger.info(f" -> Vec[0:10]: [{vec_str}]")

    # Check for identical vectors with different IDs
    # Find duplicates
    u, indices, counts = np.unique(vectors, axis=0, return_index=True, return_counts=True)
    duplicates = u[counts > 1]
    
    if len(duplicates) > 0:
        logger.warning(f"Found {len(duplicates)} vector signatures shared by multiple IDs.")
        # Print one example
        # Find rows equal to the first duplicate
        dup_vec = duplicates[0]
        # This is slow but fine for diagnostic
        matches = []
        for i in range(len(vectors)):
            if np.array_equal(vectors[i], dup_vec):
                matches.append(ids[i])
                if len(matches) >= 3: break
        
        logger.warning(f"Example Collision: Vector X shared by {matches}")
        
        logger.info("\nRECOMMENDATION (@Data-Ops):")
        logger.info("1. Purge duplicates retaining only the first accession ID.")
        logger.info("2. If variance is near zero, the Colab Inference Loop likely failed to update the tensor in the batch.")
        logger.info("3. Re-run Ingestion with a 'drop_duplicates' filter on the vector column.")

if __name__ == "__main__":
    main()
