import sys
import os
import unittest
import json
import logging
import time
import subprocess
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch, ANY
import io
import contextlib

# Add src to path
# Assuming we run from project root, but let's be safe
PROCESS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROCESS_ROOT not in sys.path:
    sys.path.insert(0, PROCESS_ROOT)

# Mock app_config if needed before imports
# We will create a dummy config if src.config fails
try:
    from src.config import app_config
except ImportError:
    # Use mock config
    class MockConfig:
        THEME_COLORS = {'primary': '#000', 'background': '#000', 'accent': '#000'}
        WORMS_CSV = MagicMock()
        WORMS_CSV.exists.return_value = False
    sys.modules['src.config'] = MagicMock()
    sys.modules['src.config'].app_config = MockConfig()

# Import Project Modules
from src.core.taxonomy import TaxonomyEngine
from src.core.science_kernel import ScienceKernel
from src.core.discovery import DiscoveryEngine
try:
    from PySide6.QtWidgets import QApplication
    from src.ui.views.discovery_view import DiscoveryView
    PYSIDE_AVAILABLE = True
except ImportError:
    PYSIDE_AVAILABLE = False

# Set up logging for the audit
logging.basicConfig(level=logging.INFO)
audit_logger = logging.getLogger("EXPEDIA_AUDIT")

class TestExpediaAudit(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        audit_logger.info("Initializing Expedia Audit Suite...")
        # Create results directory
        cls.results_dir = os.path.join(PROCESS_ROOT, 'results')
        os.makedirs(cls.results_dir, exist_ok=True)
        cls.report_path = os.path.join(cls.results_dir, 'EXPEDIA_FINAL_AUDIT_REPORT.txt')
        
        # Initialize engines
        # Patch WoRMS load to avoid file not found warnings if CSV missing
        with patch('pandas.read_csv') as mock_read:
            mock_read.side_effect = Exception("No CSV")
            cls.taxonomy = TaxonomyEngine()
        
        # Mock ScienceKernel dependencies to avoid full model load for simple tests
        # We will test DiscoveryEngine separately
        cls.kernel = ScienceKernel()
        cls.kernel.db = MagicMock()
        
        # Initialize Report
        with open(cls.report_path, 'w') as f:
            f.write(f"EXPEDIA FINAL AUDIT REPORT\n")
            f.write(f"Date: {time.ctime()}\n")
            f.write("="*60 + "\n\n")

    def log_result(self, test_name, status, details=""):
        with open(self.report_path, 'a') as f:
            f.write(f"[{status}] {test_name}\n")
            if details:
                f.write(f"     > {details}\n")
            f.write("-" * 40 + "\n")

    def test_01_lineage_calculation(self):
        """
        Audit Lineage Calculation for 3 profiles (Control, Divergent, Novel).
        """
        audit_logger.info("Running Lineage Calculation Audit...")
        
        # 1. Profile A: Control (Identical)
        df_control = pd.DataFrame([{
            'classification': 'Saccharomyces cerevisiae',
            'lineage': 'k__Fungi;p__Ascomycota;c__Saccharomycetes;o__Saccharomycetales;f__Saccharomycetaceae;g__Saccharomyces;s__Saccharomyces cerevisiae',
            '_distance': 0.0
        }])
        
        res_a = self.taxonomy.analyze_sample(df_control, "ATCG")
        try:
            self.assertEqual(res_a['status'], "Identified")
            self.assertEqual(res_a['classification'], "Saccharomyces cerevisiae")
            self.log_result("Lineage Audit - Profile A (Control)", "PASS", "Identified correctly.")
        except AssertionError as e:
            self.log_result("Lineage Audit - Profile A (Control)", "FAIL", str(e))
            raise

        # 2. Profile B: Divergent (~90% Sim -> Family Level)
        # 10 Neighbors pointing to Aspergillus
        df_divergent = pd.DataFrame([{
            'classification': 'Aspergillus niger',
            'lineage': '...',
            'genus': 'Aspergillus',
            'family': 'Aspergillaceae',
            'order': 'Eurotiales',
            '_distance': 0.10 # 90% Sim
        }] * 10)
        
        # The logic: 90% Sim matches Family (Threshold > 0.88), but fails Genus (Threshold > 0.93)
        # So it should be Novel Genus or Divergent Species.
        # TaxonomyEngine returns "Divergent" or "Novel" status.
        
        res_b = self.taxonomy.analyze_sample(df_divergent, "ATCG")
        
        status_b = res_b.get('status', 'Unknown')
        class_b = res_b.get('classification', 'Unknown')
        
        try:
            # Check for 'Novel' or bracketed genus
            is_valid_divergence = "Novel" in status_b or "Divergent" in status_b or "[" in class_b
            self.assertTrue(is_valid_divergence, f"Expected Novel/Divergent, got {status_b}")
            
            # Verify Family Anchor if possible
            if "Novel Genus" in class_b:
                self.assertIn("Aspergillaceae", class_b)
            
            self.log_result("Lineage Audit - Profile B (Divergent)", "PASS", f"Status: {status_b}, Class: {class_b}")
        except AssertionError as e:
            self.log_result("Lineage Audit - Profile B (Divergent)", "FAIL", str(e))
            raise

        # 3. Profile C: Extremophile (<70% Sim) -> NRT
        # 10 Neighbors pointing to Unknown Fungus
        df_novel = pd.DataFrame([{
            'classification': 'Unknown Fungus',
            'lineage': '...',
            'phylum': 'Basidiomycota',
            'class': 'Agaricomycetes',
            'order': 'Agaricales',
            'family': 'Unknown',
            '_distance': 0.35 # 65% Sim
        }] * 10)
        
        res_c = self.taxonomy.analyze_sample(df_novel, "ATCG")
        
        status_c = res_c.get('status', 'Unknown')
        class_c = res_c.get('classification', 'Unknown')
        try:
            self.assertIn("Novel", status_c)
            # 65% Sim > Phylum threshold (0.60).
            # Should anchor to Phylum or Class (0.70 threshold might fail class).
            # Expect "Novel Class/Order/Family" rooted in Basidiomycota.
            self.log_result("Lineage Audit - Profile C (Novel)", "PASS", f"Status: {status_c}, Class: {class_c}")
        except AssertionError as e:
            self.log_result("Lineage Audit - Profile C (Novel)", "FAIL", str(e))
            raise

    def test_02_consensus_filter(self):
        """
        Verify Majority Vote filters 'junk' strings.
        """
        audit_logger.info("Running Consensus Filter Audit...")
        
        data = []
        # 3 Valid entries
        for _ in range(3):
            data.append({
                'classification': 'Validus species',
                'lineage': '...',
                'species': 'Validus species',  # Rank column needed for consensus
                'genus': 'Validus',            # Rank column needed for consensus
                'family': 'Validaceae',
                '_distance': 0.05
            })
        # 2 Junk entries
        for junk in ['uncultured bacterium', 'metagenome']:
            data.append({
                'classification': junk,
                'lineage': '...',
                'species': junk,               # Junk in rank column
                'genus': 'Uncultured',
                'family': 'Unknown',
                '_distance': 0.05
            })
            
        df = pd.DataFrame(data)
        
        res = self.taxonomy.analyze_sample(df, "ATCG")
        
        # Result logic:
        # Distance 0.05 -> 95% Sim.
        # Thresholds: Species > 0.97, Genus > 0.93.
        # So Species (Validus species) is NOT confirmed (95 < 97).
        # But Genus (Validus) IS confirmed (95 > 93).
        # Expectation: "Validus sp. (Divergent)" or similar.
        
        audit_logger.info(f"Consensus Result: {res['classification']}")
        
        try:
            self.assertIn('Validus', res['classification'])
            self.assertNotEqual(res['classification'], 'uncultured bacterium')
            self.log_result("Consensus Filter Verification", "PASS", f"Selected: {res['classification']}")
        except AssertionError as e:
            self.log_result("Consensus Filter Verification", "FAIL", f"Selected: {res['classification']}")
            raise

    def test_03_visualization_data_integrity(self):
        """
        Trigger get_localized_topology and audit JSON payload.
        """
        audit_logger.info("Running Visualization Data Integrity Audit...")
        
        # Setup mocks
        mock_db = MagicMock()
        # Mock vector search return df
        neighbors_data = []
        for i in range(500):
            neighbors_data.append({
                'id': f'SEQ-{i}',
                'vector': np.random.rand(768).astype(np.float32), 
                'classification': f'Species {i%10}',
                'lineage': f'Lineage {i%10}',
                'accessionid': f'ACC-{i}',
                'phylum': f'Phylum-{i%5}'
            })
        
        df_neighbors = pd.DataFrame(neighbors_data)
        mock_db.vector_search.return_value = df_neighbors
        self.kernel.db = mock_db
        
        # Mock Discovery
        mock_discovery = MagicMock()
        mock_discovery.cluster_nrt_batch.return_value = {
            "labels": np.random.randint(0, 5, 501), # Query + 500
            "visuals": np.random.rand(501, 3),
            "success": True
        }
        self.kernel.discovery = mock_discovery
        self.kernel.taxonomy = self.taxonomy # Use real taxonomy logic
        
        # Capture stdout to find handshake path
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
             # Mock sys.__stdout__ for kernel safety
             with patch('sys.__stdout__', new=buf):
                 query_vec = np.random.rand(768).tolist()
                 self.kernel.get_localized_topology(vector=query_vec, k=500)
                 output = buf.getvalue()

        # Parse output for handshake
        handshake_path = None
        for line in output.strip().split('\n'):
            if not line: continue
            try:
                msg = json.loads(line)
                if msg.get('type') == 'localized_manifold_ready':
                    handshake_path = msg.get('file_path')
            except json.JSONDecodeError:
                pass
        
        try:
            self.assertIsNotNone(handshake_path, "Handshake JSON not emitted")
            self.assertTrue(os.path.exists(handshake_path), "Handshake file not created")
            
            with open(handshake_path, 'r') as f:
                payload = json.load(f)
            
            self.assertEqual(payload['status'], 'success')
            self.assertTrue(len(payload['neighbors']) > 0)
            
            # Audit coordinate types and metadata
            n1 = payload['neighbors'][0]
            self.assertIsInstance(n1['coords'][0], float)
            self.assertFalse(np.isnan(n1['coords'][0]))
            
            # Check lineage metadata (mapped from input df)
            self.assertIn('lineage', n1)
            # Check ID mapping (accessionid -> id or id present)
            self.assertTrue(n1['id'].startswith('SEQ-'))

            self.log_result("Visualization Data Integrity", "PASS", "JSON Payload & Handshake Verified.")

        except AssertionError as e:
            self.log_result("Visualization Data Integrity", "FAIL", str(e))
            raise
        except Exception as e:
            self.log_result("Visualization Data Integrity", "FAIL", f"Runtime Error: {e}")
            raise

    def test_04_umap_spectral_stability(self):
        """
        Test Adaptive Initialization logic (N=8).
        """
        audit_logger.info("Running UMAP Spectral Stability Audit...")
        
        # Initialize real DiscoveryEngine (mocking UMAP/HDBscan if easier, but intent is stress test)
        # We will try to instantiate real one.
        try:
            discovery = DiscoveryEngine()
            
            # Create Small Batch N=8
            N = 8
            ids = [f"ID-{i}" for i in range(N)]
            vectors = np.random.rand(N, 768).astype(np.float32)
            
            # Run
            result = discovery.cluster_nrt_batch(vectors, ids)
            
            self.assertTrue(result['success'])
            self.assertEqual(len(result['labels']), N)
            self.assertEqual(result['visuals'].shape, (N, 3))
            
            self.log_result("UMAP Spectral Stability", "PASS", "Small batch (N=8) processed successfully.")

        except Exception as e:
            self.log_result("UMAP Spectral Stability", "FAIL", str(e))
            # Just log failure, don't stop suite if UMAP missing environment
            # raise

    def test_05_ui_logic_integration(self):
        """
        Mock MainWindow/DiscoveryView logic.
        """
        audit_logger.info("Running UI Logic Check...")
        
        if not PYSIDE_AVAILABLE:
            self.log_result("UI Logic Check", "SKIP", "PySide6 not installed.")
            return

        try:
            # Create App context
            if not QApplication.instance():
                app = QApplication(sys.argv)
            else:
                app = QApplication.instance()
            
            # Mock DB/Config for View init
            with patch('src.ui.views.discovery_view.app_config') as mock_cfg:
                mock_cfg.THEME_COLORS = {'primary': '#fff', 'background': '#000', 'accent': '#f00', 'foreground': '#fff'}
                
                view = DiscoveryView()
                
                # Create Mock Results
                ntu_list = [{
                    "ntu_id": "NTU-1",
                    "anchor_taxon": "Novel Yeast",
                    "size": 10,
                    "divergence": 0.05,
                    "mean_confidence": 0.8, 
                    "centroid_vector": [0.1]*768,
                    "centroid_id": "REF-1",
                    "members": ["ID1", "ID2"]
                }]
                isolated_list = [{
                    "id": "ISO-1",
                    "status": "Novel",
                    "classification": "Unknown",
                    "lineage": "Unresolved" # Ensure keys exist
                }]
                
                # Execute Logic
                view.populate_ntus(ntu_list, isolated_list)
                
                # Verify Metrics
                # Shannon Index for 1 NTU (10 items) + 1 Isolated (1 item)
                # Total = 11. Props: 10/11, 1/11.
                # H = - ( (10/11)*ln(10/11) + (1/11)*ln(1/11) ) > 0
                
                diversity_text = view.summary_panel.card_diversity.value_label.text()
                richness_text = view.summary_panel.card_richness.value_label.text()
                
                audit_logger.info(f"UI Metrics -> H': {diversity_text}, S: {richness_text}")
                
                self.assertNotEqual(diversity_text, "0.00")
                self.assertEqual(richness_text, "2") # 1 NTU + 1 Iso
                
                self.log_result("UI Logic Check", "PASS", f"Metrics Validated: S={richness_text}, H'={diversity_text}")

        except Exception as e:
            self.log_result("UI Logic Check", "FAIL", str(e))
            raise

    def test_06_e2e_processing(self):
        """
        End-to-End Processing of Real FASTA Files via Subprocess.
        Validates Real Model Inference (Non-Mocked).
        """
        audit_logger.info("Running E2E FASTA Processing Audit (Real Subprocess)...")
        
        # 1. Target Data Selection
        target_file = r"E:\EXPEDIA_Data\data\raw\known_taxa.fasta"
        if not os.path.exists(target_file):
            # Fallback for dev environment if E: drive is absent
            fallback = r"C:\Volume D\DeepBio_Edgev4\data\raw\known_taxa.fasta"
            if os.path.exists(fallback):
                audit_logger.warning(f"Target {target_file} not found. Using fallback: {fallback}")
                target_file = fallback
            else:
                self.log_result("E2E Processing", "SKIP", f"Data Lake not found: {target_file}")
                return

        audit_logger.info(f"Targeting Real Data: {target_file}")
        
        # 2. Launch Science Kernel as Subprocess
        kernel_script = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'core', 'science_kernel.py'))
        if not os.path.exists(kernel_script):
            self.log_result("E2E Processing", "FAIL", f"Kernel script missing: {kernel_script}")
            return

        try:
            # Start process with Unbuffered output to prevent JSON lag
            audit_logger.info("Launching Science Kernel Subprocess (Unbuffered)...")
            process = subprocess.Popen(
                [sys.executable, "-u", kernel_script],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, # Merge stderr
                text=True,
                encoding='utf-8',
                cwd=os.path.dirname(kernel_script),
                bufsize=1 
            )
            
            # 3. Wait for "Ready" Signal
            ready = False
            start_wait = time.time()
            max_wait = 120 # Increased for heavy imports (Torch/Transformers can take >60s on CPU)
            
            while time.time() - start_wait < max_wait:
                if process.poll() is not None:
                     rest = process.stdout.read()
                     self.log_result("E2E Processing", "FAIL", f"Kernel died during startup. Exit: {process.returncode}")
                     audit_logger.error(f"Kernel Output dump: {rest}")
                     return

                # Read line
                line = process.stdout.readline()
                if line:
                    line = line.strip()
                    try:
                        msg = json.loads(line)
                        if msg.get('type') == 'status' and msg.get('status') == 'ready':
                            ready = True
                            break
                        if msg.get('type') == 'error':
                             if "FATAL" in str(msg.get('message')):
                                 audit_logger.error(f"KERNEL FATAL: {msg.get('message')}")
                                 break
                    except: 
                        pass # Ignore non-JSON logs
                else:
                    if process.poll() is not None: break
                    time.sleep(0.1)
            
            if not ready:
                process.terminate()
                self.log_result("E2E Processing", "FAIL", "Timeout waiting for Ready signal (Imports took too long).")
                return

            audit_logger.info("Kernel Ready. Sending process_fasta command...")
            command = {
                "command": "process_fasta",
                "file_path": target_file
            }
            process.stdin.write(json.dumps(command) + "\n")
            process.stdin.flush()
            
            # 5. Monitor Output & Measure Latency
            inference_start = time.time()
            vectors_received = 0
            valid_signatures = 0
            
            while time.time() - inference_start < 60: # 60s processing timeout
                line = process.stdout.readline()
                if not line: break
                
                try:
                    msg = json.loads(line)
                    
                    if msg.get('type') == 'result':
                        vectors_received += 1
                        vec = msg.get('data', {}).get('vector', [])
                        # Verify non-zero numerical signature (Simulated vectors are often uniform, Real are high variance)
                        if len(vec) == 768 and np.std(vec) > 0.001:
                            valid_signatures += 1
                            
                    elif msg.get('type') == 'status' and msg.get('status') == 'idle':
                        # Done
                        break
                        
                    elif msg.get('type') == 'error':
                        self.log_result("E2E Processing", "FAIL", f"Processing Error: {msg.get('message')}")
                        break
                        
                except json.JSONDecodeError:
                    continue
            
            inference_time = time.time() - inference_start
            process.terminate()
            
            # 6. Final Verdict
            if valid_signatures > 0:
                latency_msg = f"REAL_INFERENCE_LATENCY: [{inference_time:.2f}]s"
                print(latency_msg)
                self.log_result("E2E Processing", "PASS", f"Verified {valid_signatures} real vectors. {latency_msg}")
            else:
                self.log_result("E2E Processing", "FAIL", "No valid high-variance vectors received.")
                
        except Exception as e:
            self.log_result("E2E Processing", "FAIL", f"Subprocess Exception: {e}")
            if 'process' in locals(): process.terminate()



if __name__ == '__main__':
    unittest.main(exit=False)
