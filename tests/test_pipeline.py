import sys
import os
import json
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure Logging to file
logging.basicConfig(level=logging.INFO, filename='test_pipeline.log', filemode='w')

from src.core.science_kernel import ScienceKernel

def mock_stdout(msg):
    # Capture json messages for inspection
    try:
        data = json.loads(msg)
        print(f"KERNEL MSG: {data.get('type')} - {str(data)}")
    except:
        print(f"RAW MSG: {msg}")

# Overwrite sys.stdout.write to capture test results
original_stdout = sys.stdout
sys.stdout.write = mock_stdout

if __name__ == "__main__":
    print("Initializing Science Kernel Test...")
    kernel = ScienceKernel()
    
    try:
        kernel.initialize()
        print("Initialization Complete.")
        
        test_file = r"C:\Volume D\DeepBio_Edgev4\data\raw\discovery_dark_taxa.fasta"
        if not os.path.exists(test_file):
            print(f"ERROR: Test file not found: {test_file}")
            sys.exit(1)
            
        print(f"Processing FASTA: {test_file}")
        kernel.process_fasta(test_file)
        print("Test Complete.")
        
    except Exception as e:
        print(f"TEST FAILED: {e}")
        import traceback
        traceback.print_exc()