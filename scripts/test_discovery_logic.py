
import sys
import os
import json
import numpy as np
import logging

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mocking modules that might not be fully available or too heavy for a unit test
# But we try to import the actual classes
try:
    from src.core.science_kernel import ScienceKernel
    from src.core.taxonomy import TaxonomyEngine
except ImportError as e:
    print(f"Import Failed: {e}")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestDiscovery")

def test_taxonomy_consensus():
    print("\n[TEST] Taxonomy Consensus Logic")
    engine = TaxonomyEngine()
    
    # Mock DataFrame
    import pandas as pd
    data = {
        'genus': ['Vibrio', 'Vibrio', 'Vibrio', 'Pseudomonas', 'Vibrio'],
        'confidence': [0.9, 0.8, 0.9, 0.7, 0.6]
    }
    df = pd.DataFrame(data)
    
    # Test _get_consensus_at_rank
    result = engine._get_consensus_at_rank(df, 'genus')
    print(f"Consensus Result: {result}")
    
    if result['taxon'] == 'Vibrio' and result['confidence'] > 0.6:
        print("PASS: Taxonomy Consensus Logic Verified.")
    else:
        print("FAIL: Taxonomy Consensus Logic Incorrect.")

def test_aggregation_logic():
    print("\n[TEST] Satellite Cluster Aggregation")
    
    # Mock ScienceKernel to avoid full initialization
    kernel = ScienceKernel()
    
    # Mock DiscoveryEngine (Avalanche Standard)
    class MockDiscovery:
        def cluster_nrt_batch(self, vectors, ids, meta=None):
            # Return valid Avalanche structure
            N = len(vectors)
            return {
                "success": True,
                "labels": np.array([0]*5 + [1]*5 + [-1]*2),
                "visuals": np.random.rand(12, 3), # 3D UMAP simulation
                "norm_vectors": vectors
            }
            
    kernel.discovery = MockDiscovery()
    
    # Create Dummy NRT Vectors
    # 12 vectors of dimension 10 (simulating 768)
    nrt_vectors = [np.random.rand(10).astype(np.float32) for _ in range(12)]
    nrt_ids = [f"seq_{i}" for i in range(12)]
    nrt_meta = [
        {"id": f"seq_{i}", "classification": "Novel", "lineage": "Bacteria"} 
        for i in range(12)
    ]
    
    # Capture output
    from io import StringIO
    captured_output = StringIO()
    original_stdout = sys.__stdout__
    sys.__stdout__ = captured_output
    
    try:
        kernel._aggregate_ntus(nrt_vectors, nrt_ids, nrt_meta)
    except Exception as e:
        sys.__stdout__ = original_stdout
        print(f"FAIL: Aggregation crashed: {e}")
        import traceback
        traceback.print_exc()
        return

    sys.__stdout__ = original_stdout
    output = captured_output.getvalue()
    
    # Parse Output
    for line in output.strip().split('\n'):
        try:
            msg = json.loads(line)
            if msg.get("type") == "batch_discovery_summary":
                ntus = msg.get("ntus", [])
                isolated = msg.get("isolated_count", 0)
                print(f"Result: {len(ntus)} NTUs, {isolated} Isolated.")
                
                # We expect 2 clusters (label 0, 1) and 2 isolated (label -1)
                # But check NTU content for explicit label
                if len(ntus) > 0 and 'cluster_label' in ntus[0]:
                     print("PASS: Avalanche Schema Verified (cluster_label present).")
                else:
                     print("FAIL: Schema missing cluster_label.")

                if len(ntus) == 2 and isolated == 2:
                    print("PASS: Aggregation Logic Verified.")
                else:
                    print(f"FAIL: Unexpected counts (Expected 2 NTUs, 2 Isolated). Got {len(ntus)}, {isolated}")
                return
        except json.JSONDecodeError:
            pass
            
    print("FAIL: No valid output received.")

def test_vector_payload():
    print("\n[TEST] Vector Payload in Result")
    kernel = ScienceKernel()
    
    # Mock Embedder
    class MockEmbedder:
        def generate_embedding(self, seq):
            return np.random.rand(10).astype(np.float32)
    kernel.embedder = MockEmbedder()
    
    # Mock DB
    kernel.db = None # Skip DB search
    
    # Mock Taxonomy
    class MockTaxonomy:
        def analyze_sample(self, neighbors, seq):
            return {"status": "Novel", "classification": "Unknown", "confidence": 0.5}
    kernel.taxonomy = MockTaxonomy()
    
    # Capture output
    from io import StringIO
    captured_output = StringIO()
    original_stdout = sys.__stdout__
    sys.__stdout__ = captured_output
    
    try:
        kernel._process_batch(["ATCG"], ["test_seq"], [], [], [])
    except Exception as e:
        sys.__stdout__ = original_stdout
        print(f"FAIL: Batch Processing Error: {e}")
        return

    sys.__stdout__ = original_stdout
    output = captured_output.getvalue()
    
    for line in output.strip().split('\n'):
        try:
            msg = json.loads(line)
            if msg.get("type") == "result":
                data = msg.get("data", {})
                if "vector" in data:
                    vec = data["vector"]
                    if isinstance(vec, list) and len(vec) == 10:
                         print("PASS: Vector Payload Valid.")
                    else:
                         print(f"FAIL: Vector format invalid. Got {type(vec)}")
                else:
                    print("FAIL: 'vector' key missing in result.")
                return
        except json.JSONDecodeError:
            pass
            
    print("FAIL: No valid result output.")

if __name__ == "__main__":
    print("Running Tests on New Implementations...")
    test_taxonomy_consensus()
    test_aggregation_logic()
    test_vector_payload()
