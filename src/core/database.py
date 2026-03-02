import lancedb
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from ..config import app_config

logger = logging.getLogger("EXPEDIA.Database")

class AtlasManager:
    """
    @Vector-Ops: Manages the LanceDB connection on Volume E:
    - Handles vector search (Cosine Similarity).
    - Manages the reference_atlas_v100k table.
    - Uses disk-native memory mapping.
    """
    def __init__(self):
        self.db_path = app_config.VECTOR_DB_PATH
        self.table_name = app_config.ATLAS_TABLE_NAME
        self.connect()
        self.check_index_health()

    def connect(self):
        try:
            logger.info(f"Connecting to Vector DB at {self.db_path}...")
            # Connect to existing DB
            self.db = lancedb.connect(str(self.db_path))
            
            # STRICTLY OPEN EXISTING TABLE - No Creation
            if self.table_name in self.db.table_names():
                self.table = self.db.open_table(self.table_name)
                logger.info(f"Connected to table: {self.table_name}")
            else:
                logger.critical(f"CRITICAL: Table '{self.table_name}' not found on Volume E:.")
                self.table = None
                # In production this might raise, but for now we log critical
                
        except Exception as e:
            logger.critical(f"Database connection failed: {e}")
            raise

    def check_index_health(self):
        """
        Verifies the table size and index status.
        """
        if self.table is None:
            return

        try:
            # count_rows() is efficient in LanceDB
            # If using older SDK, len(self.table) might work or self.table.to_pandas().shape[0] (too slow)
            # Assuming count_rows() exists or we use to_arrow().shape[0] for quick check on metadata if needed
            # For now len() usually works on LanceTable in newer versions
            count = len(self.table)
            
            # Log to console strictly as requested
            msg = f"[DB] Lancedb linked. {count} signatures ready for sub-10ms search."
            # print(msg) - REMOVED: Pollution of stdout breaks IPC
            logger.info(msg)
            return count
            
        except Exception as e:
            logger.error(f"Index Health Check Failed: {e}")
            return 0

    def get_count(self) -> int:
        """
        Returns the total number of vectors in the table.
        """
        if self.table:
            return len(self.table)
        return 0


    def vector_search(self, query_vector: np.ndarray, top_k: int = 50) -> pd.DataFrame:
        """
        Performs an approximate nearest neighbor search.
        Returns Top-50 neighbors.
        """
        if self.table is None:
            logger.error("DB Table not initialized.")
            return pd.DataFrame()

        try:
            # LanceDB Search
            # metric="cosine" is default or explicit depending on version, 
            # usually L2 is default but for normalized vectors inner product ~ cosine.
            # Explicitly requesting metric='cosine' if supported by the pyarrow bindings or API.
            results = (
                self.table.search(query_vector)
                .metric("cosine") # type: ignore
                .limit(top_k)
                .to_pandas()
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return pd.DataFrame()
