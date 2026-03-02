import logging
import numpy as np
import pandas as pd
import hdbscan
from collections import Counter
from ..config import app_config

logger = logging.getLogger("EXPEDIA.Discovery")

class DiscoveryEngine:
    """
    Handles Novelty Discovery for sequences < 85% similarity.
    Groups NRTs (Non-Reference Taxa) into NTUs (Novel Taxonomic Units).
    """

    def __init__(self):
        self.min_cluster_size = 2
        self.min_samples = 1
        self.clusterer = hdbscan.HDBSCAN(
            min_cluster_size=self.min_cluster_size,
            min_samples=self.min_samples,
            metric='euclidean' # Embeddings are normalized? If not, maybe cosine needed or pre-calc.
            # HDBSCAN on high dim data can be slow, but for batches it is okay.
        )
        logger.info("Discovery Engine Initialized (HDBSCAN).")

    def cluster_nrt_batch(self, nrt_vectors: np.ndarray, nrt_ids: list, nrt_meta: list | None = None) -> pd.DataFrame:
        """
        Takes a batch of vectors flagged as Non-Reference Taxa and clusters them.
        """
        if len(nrt_ids) < self.min_cluster_size:
            logger.info("Not enough NRTs to form a cluster.")
            return pd.DataFrame()

        logger.info(f"Clustering {len(nrt_vectors)} NRT sequences...")
        
        # Fit HDBSCAN
        labels = self.clusterer.fit_predict(nrt_vectors)
        
        results = []
        unique_labels = set(labels)
        
        for label in unique_labels:
            if label == -1:
                # Noise
                continue
            
            # Get indices for this cluster
            indices = np.where(labels == label)[0]
            cluster_ids = [nrt_ids[i] for i in indices]
            cluster_vectors = nrt_vectors[indices]
            
            # Resolve Metadata (Anchor & Lineage)
            anchor = "Unresolved"
            lineage = ""
            
            if nrt_meta and len(nrt_meta) == len(nrt_ids):
                # Safe access
                cluster_meta = [nrt_meta[i] for i in indices]
                
                # 1. Consensus Classification (Anchor)
                cl_candidates = [m.get('classification') for m in cluster_meta if m.get('classification')]
                if cl_candidates:
                    anchor = Counter(cl_candidates).most_common(1)[0][0]
                
                # 2. Consensus Lineage
                ln_candidates = [m.get('lineage') for m in cluster_meta if m.get('lineage')]
                if ln_candidates:
                    lineage = Counter(ln_candidates).most_common(1)[0][0]
            
            # Calculate Centroid
            centroid = np.mean(cluster_vectors, axis=0)
            
            results.append({
                "ntu_id": f"EXPEDIA-NTU-{label}",
                "size": len(cluster_ids),
                "member_ids": cluster_ids,
                "centroid": centroid,
                "anchor_taxon": anchor,
                "lineage": lineage
            })

        df_results = pd.DataFrame(results)
        return df_results
