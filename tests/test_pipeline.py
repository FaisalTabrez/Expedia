import sys
import os
import json
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure Logging to file
logging.basicConfig(level=logging.INFO, filename='test_pipeline.log', filemode='w')

from src.core.science_kernel import ScienceKernel

class StdoutWrapper:
    def __init__(self, original_stdout):
        self.original_stdout = original_stdout

    def write(self, msg):
        # Capture json messages for inspection
        try:
            data = json.loads(msg)
            self.original_stdout.write(f"KERNEL MSG: {data.get('type')} - {str(data)}\n")
        except:
             self.original_stdout.write(f"RAW MSG: {msg}\n")
    
    def flush(self):
        self.original_stdout.flush()

# Install the wrapper
sys.stdout = StdoutWrapper(sys.stdout)

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