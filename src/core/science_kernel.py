import sys
import os
import json
import time
import logging
import traceback
import numpy as np
import pandas as pd
from collections import Counter

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
    
    # PCA Removed (Avalanche Standard uses UMAP)
    # safe_ipc_write(json.dumps({"type": "log", "message": "Importing Sklearn..."}) + "\n")
    # from sklearn.decomposition import PCA

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

SCHEMA_MAP = {
    'AccessionID': 'id',
    'ScientificName': 'classification', 
    'Lineage': 'lineage',
    'Vector': 'vector',
    'vector': 'vector', # Handle pre-normalized
    'id': 'id',
    'classification': 'classification',
    'lineage': 'lineage'
}

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
        self._background_cache = None

    @staticmethod
    def _extract_top_rank(lineage: str, classification: str) -> str:
        """Extract a stable taxonomic bucket for stratified sampling."""
        if lineage:
            if 'p__' in lineage:
                for token in lineage.split(';'):
                    token = token.strip()
                    if token.startswith('p__'):
                        parts = token.split('__', 1)
                        if len(parts) > 1 and parts[1]:
                            return parts[1]
            # Generic lineage formats: Kingdom;Phylum;Class... or K > P > C...
            if ';' in lineage:
                parts = [p.strip() for p in lineage.split(';') if p.strip()]
                if len(parts) > 1:
                    return parts[1]
                if parts:
                    return parts[0]
            if '>' in lineage:
                parts = [p.strip() for p in lineage.split('>') if p.strip()]
                if len(parts) > 1:
                    return parts[1]
                if parts:
                    return parts[0]

        if classification and classification not in ("Unknown", "Unknown Organism"):
            return classification.split(' ')[0]
        return "Unclassified"

    def _build_stratified_background_sample(self, sample_size: int = 5000) -> list:
        """
        @Vector-Ops: Builds a 5,000-point background sample using stratified pulls
        over broad atlas coverage via random-probe ANN queries.
        """
        if not self.db:
            return []

        if self._background_cache is not None and len(self._background_cache) >= sample_size:
            return self._background_cache[:sample_size]

        candidate_rows = []
        seen_ids = set()
        dims = int(app_config.EMBEDDING_DIMENSION_PADDED)
        probe_count = 64
        probe_k = 200

        try:
            for _ in range(probe_count):
                probe = np.random.randn(dims).astype(np.float32)
                norm = np.linalg.norm(probe)
                if norm > 0:
                    probe = probe / norm

                df = self.db.vector_search(probe, top_k=probe_k)
                if df.empty:
                    continue

                cols = {c.lower(): c for c in df.columns}
                id_col = cols.get('id') or cols.get('accessionid') or cols.get('seq_id')
                class_col = cols.get('classification') or cols.get('scientificname')
                lineage_col = cols.get('lineage') or cols.get('taxonomy') or cols.get('phylum')

                if not id_col or 'vector' not in cols:
                    continue

                for _, row in df.iterrows():
                    rid = str(row.get(id_col, ''))
                    if not rid or rid in seen_ids:
                        continue

                    vec = row.get(cols['vector'])
                    if vec is None:
                        continue

                    seen_ids.add(rid)
                    cls = str(row.get(class_col, 'Unknown Organism')) if class_col else 'Unknown Organism'
                    lin = str(row.get(lineage_col, '')) if lineage_col else ''
                    candidate_rows.append({
                        "id": rid,
                        "classification": cls,
                        "lineage": lin,
                        "vector": np.array(vec, dtype=np.float32)
                    })

            if not candidate_rows:
                return []

            strata = {}
            for item in candidate_rows:
                bucket = self._extract_top_rank(item.get("lineage", ""), item.get("classification", ""))
                strata.setdefault(bucket, []).append(item)

            per_bucket = max(1, sample_size // max(1, len(strata)))
            selected = []
            for bucket_items in strata.values():
                if len(bucket_items) <= per_bucket:
                    selected.extend(bucket_items)
                else:
                    idxs = np.random.choice(len(bucket_items), size=per_bucket, replace=False)
                    selected.extend([bucket_items[i] for i in idxs])

            if len(selected) < sample_size:
                selected_ids = {item["id"] for item in selected}
                remaining = [r for r in candidate_rows if r["id"] not in selected_ids]
                if remaining:
                    need = min(sample_size - len(selected), len(remaining))
                    idxs = np.random.choice(len(remaining), size=need, replace=False)
                    selected.extend([remaining[i] for i in idxs])

            selected = selected[:sample_size]
            background = []
            for item in selected:
                vec = item["vector"]
                # A stable pseudo-layout for background cloud projection.
                background.append({
                    "id": item["id"],
                    "classification": item.get("classification", "Unknown Organism"),
                    "lineage": item.get("lineage", ""),
                    "coords": [float(vec[0]), float(vec[1]), float(vec[2])]
                })

            self._background_cache = background
            return background
        except Exception as sample_err:
            logger.warning(f"Background stratified sampling failed: {sample_err}")
            return []

    def initialize(self):
        """
        Lazy load heavy models.
        """
        logger.info("Initializing Neural-Core & Vector-Ops engines...")
        try:
            self.embedder = NucleotideEmbedder()
            self.db = AtlasManager()
            self.taxonomy = TaxonomyEngine()
        except Exception as e:
            logger.critical(f"Model Load Failed: {e}")
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({
                    "type": "error",
                    "message": "FATAL: MODEL LOAD FAILED",
                    "details": str(e)
                }) + "\n")
                sys.__stdout__.flush()
            # Stop execution or re-raise
            raise e

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

        # Persistent Service Loop
        while True:
            # Blocking read line by line
            line = sys.stdin.readline()
            if not line:
                break # EOF from parent

            try:
                line = line.strip()
                if not line:
                    continue
                
                command = json.loads(line)
                cmd_type = command.get("command")

                if cmd_type == "process_fasta":
                    self.process_fasta(command.get("file_path"))
                elif cmd_type == "get_localized_topology":
                    vector = command.get("vector")
                    k = command.get("k", 1000)
                    record_id = command.get("id", "Unknown")
                    self.get_localized_topology(vector, record_id, k)
                elif cmd_type == "shutdown":
                    logger.info("Shutdown command received.")
                    break
                else:
                    logger.warning(f"Unknown command: {cmd_type}")

                # Idle Signal - Ready for next command
                # Wrapped in try-except for pipe safety
                try:
                    if sys.__stdout__:
                        sys.__stdout__.write(json.dumps({"type": "status", "status": "idle"}) + "\n")
                        sys.__stdout__.flush()
                except OSError:
                    pass

            except json.JSONDecodeError:
                logger.error("Invalid JSON received.")
            except Exception as e:
                logger.error(f"Kernel Loop Error: {e}")
                try:
                    if sys.__stdout__:
                        sys.__stdout__.write(json.dumps({
                            "type": "error",
                            "message": str(e),
                            "traceback": traceback.format_exc()
                        }) + "\n")
                        sys.__stdout__.flush()
                except OSError:
                    pass

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
                self._process_batch(batch_seqs, batch_ids, nrt_vectors, nrt_ids, nrt_meta) # type: ignore

            # Final Step: Discovery on NRTs
            # Run "Satellite Cluster Aggregation"
            if nrt_vectors:
                 logger.info(f"Aggregating {len(nrt_vectors)} NRTs for satellite clustering...")
                 self._aggregate_ntus(nrt_vectors, nrt_ids, nrt_meta)
            else:
                 logger.info("No NRTs found in batch. Skipping aggregation.")
                 if sys.__stdout__:
                    sys.__stdout__.write(json.dumps({
                        "type": "batch_discovery_summary",
                        "ntus": [],
                        "isolated": 0
                    }) + "\n")
                    sys.__stdout__.flush()

        except Exception as e:
            logger.error(f"Batch Processing Error: {e}")
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({
                    "type": "error",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }) + "\n")
                sys.__stdout__.flush()
            
    def _aggregate_ntus(self, nrt_vectors, nrt_ids, nrt_meta):
        """
        @BioArch-Pro: Avalanche eDNA Standard (UMAP -> HDBSCAN).
        """
        if not self.discovery:
            logger.warning("Discovery Engine missing.")
            return

        try:
            # 1. Coordinate Extraction
            X = np.vstack(nrt_vectors).astype(np.float32)
            
            # 2. Avalanche Pipeline Execution
            # L2 Norm -> UMAP (10D) -> HDBSCAN -> Labels -> UMAP (3D)
            pipeline_result = self.discovery.cluster_nrt_batch(X, nrt_ids, nrt_meta)
            
            if not pipeline_result.get("success"):
                logger.info("Pipeline returned isolated state.")
                self._emit_discovery_result([], nrt_meta)
                return

            labels = pipeline_result['labels']
            visuals = pipeline_result['visuals'] # 3D Coords for future visualization (not used in aggregation payload yet but available)
            
            # 3. Aggregation
            unique_labels = set(labels)
            ntus = []
            isolated = []

            for label in unique_labels:
                if label == -1:
                    # Noise / Isolated
                    noise_indices = np.where(labels == -1)[0]
                    for idx in noise_indices:
                        isolated.append(nrt_meta[idx])
                    continue

                # Cluster Members
                indices = np.where(labels == label)[0]
                cluster_vectors = X[indices] # ORIGINAL 768D VECTORS for Centroid
                cluster_ids = [nrt_ids[i] for i in indices]
                cluster_meta = [nrt_meta[i] for i in indices]
                
                # A. Centroid (768D) & Holotype
                centroid = np.mean(cluster_vectors, axis=0)
                
                # Find closest to centroid (Euclidean in 768D space)
                distances = np.linalg.norm(cluster_vectors - centroid, axis=1)
                holotype_idx = np.argmin(distances)
                holotype_id = cluster_ids[holotype_idx]
                holotype_vector = cluster_vectors[holotype_idx]
                
                # B. Variance (Mean distance from centroid)
                variance = np.mean(distances)
                
                # C. Consensus Anchor
                anchors = [m.get('classification', 'Unknown') for m in cluster_meta]
                common_anchor = Counter(anchors).most_common(1)[0][0]
                
                # Lineage Consensus
                lineages = [m.get('lineage', '') for m in cluster_meta]
                common_lineage = Counter(lineages).most_common(1)[0][0]

                # D. ID Generation
                ntu_id = f"EXPEDIA-NRGS-{int(time.time())}-{label}"
                
                # E. Confidence Metrics
                if cluster_meta:
                    confidences = [float(m.get('confidence', 0.0)) for m in cluster_meta]
                    mean_confidence = float(np.mean(confidences))
                    holotype_confidence = float(cluster_meta[holotype_idx].get('confidence', 0.0))
                else:
                    mean_confidence = 0.0
                    holotype_confidence = 0.0

                ntus.append({
                    "ntu_id": ntu_id,
                    "anchor_taxon": common_anchor,
                    "lineage": common_lineage,
                    "size": len(indices),
                    "divergence": float(variance),
                    "mean_confidence": mean_confidence,
                    "holotype_confidence": holotype_confidence,
                    "centroid_id": holotype_id,
                    "centroid_vector": holotype_vector.tolist(), # Serialize
                    "members": cluster_ids,
                    "cluster_label": int(label) # Avalanche Standard: Explicit Label
                })

            # 4. Emit
            self._emit_discovery_result(ntus, isolated)

        except Exception as e:
            logger.error(f"Aggregation Failed: {e}")
            self._emit_discovery_result([], [])

    def _emit_discovery_result(self, ntus, isolated):
        """Helper to emit JSON safely."""
        try:
             # Basic serialization helper
            def _make_json_serializable(obj):
                if isinstance(obj, np.ndarray): return obj.tolist()
                if isinstance(obj, np.generic): return obj.item()
                if isinstance(obj, dict): return {k: _make_json_serializable(v) for k, v in obj.items()}
                if isinstance(obj, list): return [_make_json_serializable(i) for i in obj]
                return obj

            payload = {
                "type": "batch_discovery_summary",
                "ntus": _make_json_serializable(ntus),
                "isolated_count": len(isolated),
                "isolated_taxa": _make_json_serializable(isolated) 
            }

            if sys.__stdout__:
                sys.__stdout__.write(json.dumps(payload) + "\n")
                sys.__stdout__.flush()
                
        except Exception as e:
            logger.error(f"Emit Error: {e}")

    def get_localized_topology(self, vector, record_id="Unknown", k=1000):
        """
        @Data-Ops: Micro-Topology Engine.
        Fetches {k} neighbors, runs HDBSCAN on (Query + k), and calculates PCA.
        """
        if not self.db:
            self.initialize()

        if self.db is None:
            error_msg = "Database Engine failed to initialize. Cannot run topology."
            logger.error(error_msg)
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({
                    "type": "error", 
                    "message": error_msg
                }) + "\n")
                sys.__stdout__.flush()
            return
            
        logger.info(f"[KERNEL] Computing localized topology for ID: {record_id} with {k} neighbors")
        
        try:
            if vector is None:
                raise ValueError("Vector key missing from input payload.")
                
            query_vector = np.array(vector, dtype=np.float32)
            
            # 1. Fetch Neighbors
            # vector_search returns a DataFrame with columns that might vary by DB version
            df_neighbors = self.db.vector_search(query_vector, top_k=k)
            
            # Global Column Normalization
            if not df_neighbors.empty:
                # Force lowercase
                df_neighbors.columns = [c.lower() for c in df_neighbors.columns]
                logger.info(f"[KERNEL] Available normalized columns: {df_neighbors.columns.tolist()}")

                # Smart Lineage Mapping
                if 'lineage' not in df_neighbors.columns:
                     alternatives = ['taxonomy', 'phylum', 'gbseq_taxonomy', 'tax_string']
                     found = False
                     for alt in alternatives:
                         if alt in df_neighbors.columns:
                             df_neighbors = df_neighbors.rename(columns={alt: 'lineage'})
                             found = True
                             break
                     
                     if not found:
                         logger.warning("Lineage column missing. Using 'Unclassified' placeholder.")
                         df_neighbors['lineage'] = 'Unclassified'
                
                # ID Mapping
                if 'id' not in df_neighbors.columns:
                     if 'accessionid' in df_neighbors.columns:
                         df_neighbors = df_neighbors.rename(columns={'accessionid': 'id'})
                     elif 'seq_id' in df_neighbors.columns:
                          df_neighbors = df_neighbors.rename(columns={'seq_id': 'id'})
                
                # Classification Mapping
                if 'classification' not in df_neighbors.columns:
                     if 'scientificname' in df_neighbors.columns:
                         df_neighbors = df_neighbors.rename(columns={'scientificname': 'classification'})
                     elif 'tax_name' in df_neighbors.columns:
                         df_neighbors = df_neighbors.rename(columns={'tax_name': 'classification'})
                
                # Final Safety Fill
                if 'classification' not in df_neighbors.columns:
                     df_neighbors['classification'] = 'Unknown Organism'
                     
                if 'id' not in df_neighbors.columns:
                     # This is critical but we can try to gen consistent IDs if really needed
                     df_neighbors['id'] = [f"REF-{i}" for i in range(len(df_neighbors))]


            if df_neighbors.empty:
                logger.warning("No neighbors found.")
                if sys.__stdout__:
                    try:
                        sys.__stdout__.write(json.dumps({"type": "manifold_data", "data": [], "status": "empty"}) + "\n")
                        sys.__stdout__.flush()
                    except OSError:
                        pass
                return

            # Validate Required Columns
            if 'vector' not in df_neighbors.columns:
                 # Check for 'embedding' or 'vec'
                 if 'embedding' in df_neighbors.columns:
                      df_neighbors = df_neighbors.rename(columns={'embedding': 'vector'})
                 elif 'vec' in df_neighbors.columns:
                      df_neighbors = df_neighbors.rename(columns={'vec': 'vector'})
            
            required = ['vector', 'id']
            missing = [r for r in required if r not in df_neighbors.columns]
            if missing:
                raise KeyError(f"Missing required columns from database: {missing}. Found: {df_neighbors.columns.tolist()}")

            # 2. Prepare Data Matrix (501 points)
            # Row 0 is ALWAYS the Query
            try:
                 # Check strict filtering by ID
                 # Filter out the query itself if it appears in neighbors (to avoid duplicate at 0 dist)
                 if record_id != "Unknown":
                      df_neighbors = df_neighbors[df_neighbors['id'] != record_id]
                      
                 neighbor_vectors = np.stack(df_neighbors['vector'].tolist())
            except KeyError as ke:
                 logger.error(f"Vector Column Missing: {ke}")
                 raise
                 
            all_vectors = np.vstack([query_vector, neighbor_vectors])
            
            # Metadata sync
            neighbor_meta = df_neighbors[['id', 'classification', 'lineage']].to_dict(orient='records')
            
            # 3. Localized Discovery (Avalanche Standard: UMAP -> HDBSCAN)
            # Using existing discovery engine instance
            ids_for_clustering = ["QUERY"] + [m['id'] for m in neighbor_meta]
            
            # Default fallbacks
            labels = np.full(len(all_vectors), -1)
            coords_3d = np.zeros((len(all_vectors), 3))
            query_label = -1
            consensus_summary = "Outlier"

            if self.discovery:
                 # RUN PIPELINE (L2 -> UMAP 10D -> HDBSCAN -> UMAP 3D)
                 # Treat the query + 500 neighbors as a "batch" for manifold learning
                 analysis = self.discovery.cluster_nrt_batch(all_vectors, ids_for_clustering)
                 
                 if analysis.get('success'):
                     labels = analysis['labels']
                     coords_3d = analysis['visuals'] # 3D UMAP Coords
                     
                     # Analyze Cluster containing Query (Index 0)
                     query_label = labels[0]
                     
                     if query_label != -1:
                         # Filter points in same cluster
                         cluster_indices = np.where(labels == query_label)[0]
                         # Get their metadata (offset by -1 for neighbors)
                         cluster_meta = []
                         for idx in cluster_indices:
                             if idx == 0: continue
                             cluster_meta.append(neighbor_meta[idx-1])
                             
                         # Calculate Consensus
                         if cluster_meta:
                             taxa = [m.get('classification', 'Unknown') for m in cluster_meta]
                             common = Counter(taxa).most_common(1)
                             if common:
                                 consensus_name, count = common[0]
                                 pct = (count / len(cluster_meta)) * 100
                                 consensus_summary = f"{consensus_name} ({pct:.1f}%)"
            else:
                 consensus_summary = "Discovery Engine Offline"

            # 4. Serialize Coordinates (Already 3D from UMAP)
            query_pc = coords_3d[0].tolist()
            neighbors_pc = coords_3d[1:].tolist()
            
            # Run Lineage Prediction on the Neighborhood
            # @Neural-Core: Leveraging the neighborhood to predict lineage
            predicted_lineage_obj = None
            if self.taxonomy and not df_neighbors.empty:
                 try:
                     # Filter out the query from the prediction set if it exists to avoid self-bias?
                     # predict_lineage expects a dataframe of neighbors.
                     clean_neighbors = df_neighbors[df_neighbors['id'] != record_id]
                     if not clean_neighbors.empty:
                         # Use top 50 for consensus even if we fetched 500 for topology
                         top_50 = clean_neighbors.head(50)
                         # Returns the dict we fashioned in taxonomy.py
                         full_prediction = self.taxonomy.predict_lineage(top_50)
                         
                         predicted_lineage_obj = {
                            "status": full_prediction.get("lineage_status", "UNKNOWN"),
                            "lineage_string": full_prediction.get("lineage_string", "Unknown"),
                            "anchor_rank": full_prediction.get("anchor_rank", "None")
                         }
                 except Exception as ex:
                     logger.warning(f"Failed to generate lineage prediction in topology: {ex}")

            # 5. Serialize
            response = {
                "type": "localized_manifold",
                "status": "success",
                "consensus": consensus_summary,
                "predicted_lineage": predicted_lineage_obj,
                "background": self._build_stratified_background_sample(sample_size=5000),
                "query": {
                    "coords": query_pc,
                    "label": int(query_label) if 'labels' in locals() else -1,
                    "isolated_id": f"iso_query" if query_label == -1 else None
                },
                "neighbors": []
            }
            
            for i, pc in enumerate(neighbors_pc):
                meta = neighbor_meta[i]
                lbl = int(labels[i+1]) if 'labels' in locals() else -1
                response["neighbors"].append({
                    "coords": pc,
                    "id": meta.get('id'),
                    "classification": meta.get('classification'),
                    "lineage": meta.get('lineage'),
                    "label": lbl,
                    "isolated_id": f"iso_{i}" if lbl == -1 else None
                })
            
            # Safe Serialization Helper
            def _make_json_serializable(obj):
                if isinstance(obj, np.ndarray): return obj.tolist()
                if isinstance(obj, np.generic): return obj.item()
                if isinstance(obj, dict): return {k: _make_json_serializable(v) for k, v in obj.items()}
                if isinstance(obj, list): return [_make_json_serializable(i) for i in obj]
                return obj

            clean_response = _make_json_serializable(response)

            # Disk-Based Data Handshake
            # Fallback path if E: doesn't exist
            base_dir = r"E:\EXPEDIA_Data\data\db"
            if not os.path.exists(base_dir):
                 # Try to create it, or fallback to temp relative to CWD
                 try:
                     os.makedirs(base_dir, exist_ok=True)
                 except:
                     base_dir = os.path.join(os.getcwd(), 'temp_db')
                     os.makedirs(base_dir, exist_ok=True)

            temp_path = os.path.join(base_dir, "temp_manifold.json")

            logger.info(f"KERNEL: Offloading data handshake to: {temp_path}")
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(clean_response, f)

            logger.info("KERNEL: Sending handshake notification.")
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({
                    "type": "localized_manifold_ready",
                    "file_path": temp_path
                }) + "\n")
                sys.__stdout__.flush()

        except Exception as e:
            logger.error(f"Topology Error: {e}")
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
                        "lineage": analysis.get("lineage"),
                        "confidence": float(analysis.get("confidence", 0.0)),
                        "predicted_lineage": analysis.get("predicted_lineage") # Pass through
                    })

            # Construct Result
            result = {
                "id": seq_id,
                "sequence_length": len(seq),
                "status": analysis.get("status", "Unknown"),
                "classification": analysis.get("classification", "Unknown"),
                "confidence": float(analysis.get("confidence", 0.0)),
                "lineage": analysis.get("lineage", "Unknown Lineage"),
                "predicted_lineage": analysis.get("predicted_lineage"),
                "workflow": analysis.get("workflow", "Tier 0"),
                "vector": embedding.tolist() # Enable localized topology query
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
        
        def _make_json_serializable(obj):
            """Recursively converts numpy types to python natives."""
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, np.generic):
                return obj.item()
            if isinstance(obj, dict):
                return {k: _make_json_serializable(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_make_json_serializable(i) for i in obj]
            return obj

        try:
            # Stack vectors
            if len(nrt_vectors) == 0:
                 # Should fail fast but handled just in case
                 if sys.__stdout__:
                    response_payload = {
                        "type": "discovery_results",
                        "data": [],
                        "status": "no_clusters_found"
                    }
                    sys.__stdout__.write(json.dumps(response_payload) + "\n")
                    sys.__stdout__.flush()
                 return
            
            vectors_array = np.vstack(nrt_vectors)
            
            # Cluster
            # The new Discovery API returns a dict, not a DataFrame
            # {'labels': ..., 'visuals': ..., 'norm_vectors': ..., 'success': bool}
            analysis = self.discovery.cluster_nrt_batch(vectors_array, nrt_ids, nrt_meta) # type: ignore
            
            success = analysis.get("success", False)
            if not success:
                if sys.__stdout__:
                    sys.__stdout__.write(json.dumps({
                        "type": "discovery_results",
                        "data": [],
                        "status": "clustering_failed"
                    }) + "\n")
                    sys.__stdout__.flush()
                return

            labels = analysis['labels']
            visuals = analysis['visuals']
            
            # Convert to results list for JSON dump
            # Group by label
            unique_labels = set(labels)
            results = []
            
            for label in unique_labels:
                if label == -1: continue
                indices = np.where(labels == label)[0]
                
                # Metadata
                cluster_members = [nrt_ids[i] for i in indices]
                cluster_meta = [nrt_meta[i] for i in indices] if nrt_meta else []
                cluster_vectors = vectors_array[indices]

                # Consensus
                anchor = "Unresolved"
                lineage = ""
                if cluster_meta:
                   anchors = [m.get('classification', 'Unknown') for m in cluster_meta]
                   if anchors: anchor = Counter(anchors).most_common(1)[0][0]
                   
                   lineages = [m.get('lineage', '') for m in cluster_meta]
                   if lineages: lineage = Counter(lineages).most_common(1)[0][0]
                
                # Calculate Centroid
                centroid_vector = np.mean(cluster_vectors, axis=0) # 768D
                # Find member closest to centroid
                dists = np.linalg.norm(cluster_vectors - centroid_vector, axis=1)
                centroid_idx_local = np.argmin(dists)
                centroid_id = cluster_members[centroid_idx_local]
                
                # Divergence (mean distance to centroid)
                divergence = np.mean(dists)

                results.append({
                    "ntu_id": f"EXPEDIA-NRGS-{int(time.time())}-{label}",
                    "size": len(cluster_members),
                    "anchor_taxon": anchor,
                    "lineage": lineage,
                    "member_ids": cluster_members,
                    "centroid_id": centroid_id,
                    "centroid_vector": centroid_vector, # Now included for UI Jump
                    "divergence": float(divergence)
                })
            
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({
                    "type": "discovery_results",
                    "data": _make_json_serializable(results),
                    "status": "success"
                }) + "\n")
                sys.__stdout__.flush()
                
            # Clean up old redundant code block
            # (The block that ran cluster_nrt_batch again is removed by this overwrite)

        except Exception as e:
            logger.error(f"Discovery phase failed: {e}")
            logger.error(traceback.format_exc())
            # Send empty result to unblock UI
            if sys.__stdout__:
                sys.__stdout__.write(json.dumps({
                    "type": "discovery_results",
                    "data": [],
                    "status": "error", 
                    "error": str(e)
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
