import logging
import numpy as np
import pandas as pd
import hdbscan
from sklearn.preprocessing import Normalizer
try:
    import umap
except ImportError:
    # Fallback or try different import structure if needed, but standard is 'import umap'
    import umap.umap_ as umap

from collections import Counter
from ..config import app_config

logger = logging.getLogger("EXPEDIA.Discovery")

class DiscoveryEngine:
    """
    @BioArch-Pro: Avalanche eDNA Standard Implementation.
    Pipeline: L2 Norm -> UMAP (10D) -> HDBSCAN -> UMAP (3D).
    """

    def __init__(self):
        # 1. Feature Pre-processing
        self.normalizer = Normalizer(norm='l2')
        
        # 2. Discovery Manifold (10D)
        # Optimized for density-based clustering separation
        self.reducer_cluster = umap.UMAP(
            n_neighbors=15,
            min_dist=0.0, # Tight clusters
            n_components=10,
            metric='cosine',
            random_state=42 # Reproducibility
        )
        
        # 3. Visual Manifold (3D)
        # Optimized for human inspection
        self.reducer_vis = umap.UMAP(
            n_neighbors=15,
            min_dist=0.1, # Spread out for visibility
            n_components=3,
            metric='cosine',
            random_state=42
        )
        
        # 4. Density Clustering
        # Operates on the 10D manifold
        self.clusterer = hdbscan.HDBSCAN(
            min_cluster_size=2, # Ultra-sensitivity
            min_samples=1,
            cluster_selection_method='leaf',
            prediction_data=True
        )
        
        logger.info("Discovery Engine Initialized (Avalanche Standard).")

    def cluster_nrt_batch(self, nrt_vectors: np.ndarray, nrt_ids: list, nrt_meta: list | None = None) -> dict:
        """
        Takes a batch of vectors flagged as Non-Reference Taxa and clusters them.
        Returns: { 'labels': array, 'embeddings_3d': array, 'success': bool }
        """
        N = nrt_vectors.shape[0]
        if N < 2:
            return {"labels": np.full(N, -1), "visuals": np.zeros((N, 3)), "success": False}

        logger.info(f"Clustering {N} NRT sequences...")

        # A. Normalization
        # eDNA vectors must be L2 normalized so cosine distance works best
        try:
            X_norm = self.normalizer.transform(nrt_vectors)
        except Exception as e:
            logger.error(f"Normalization failed: {e}")
            return {"labels": np.full(N, -1), "visuals": np.zeros((N, 3)), "success": False}

        # B. Fallback Strategy (Low Sample Size)
        # UMAP needs n_neighbors < N generally. Default n_neighbors=15
        if N < 5:
            logger.info(f"Sample size {N} < 5. Using linear layout fallback.")
            # For tiny batches, just return random labels or treat as noise
            # And random 3D points
            rng = np.random.RandomState(42)
            
            # Use as continuous array for typing
            visuals = np.zeros((N, 3), dtype=np.float64)
            visuals[:, :] = rng.rand(N, 3)
            
            return {
                "labels": np.full(N, -1, dtype=np.int32),
                "visuals": visuals,
                "norm_vectors": X_norm,
                "success": True 
            }

        # C. Topology Generation
        try:
            # 1. Clustering Embedding (10D)
            # Ensure X_norm is numeric matrix
            result_10d = self.reducer_cluster.fit_transform(X_norm)
            embedding_10d = result_10d[0] if isinstance(result_10d, tuple) else result_10d
            embedding_10d = np.asarray(embedding_10d)
            
            # 2. Density Clustering (on 10D)
            labels = self.clusterer.fit_predict(embedding_10d)
            
            # 3. Visual Embedding (3D)
            result_3d = self.reducer_vis.fit_transform(X_norm)
            embedding_3d = result_3d[0] if isinstance(result_3d, tuple) else result_3d

            # 4. Outlier Resilience
            visuals = np.asarray(embedding_3d, dtype=np.float64)
            if np.isnan(visuals).any():
                visuals = np.nan_to_num(visuals)

            return {
                "labels": np.asarray(labels, dtype=np.int32),
                "visuals": visuals,
                "norm_vectors": X_norm,
                "success": True
            }

        except Exception as e:
            logger.error(f"Manifold Projection Failed: {e}")
            return {
                "labels": np.full(N, -1, dtype=np.int32), 
                "visuals": np.zeros((N, 3), dtype=np.float64), 
                "success": False
            }
