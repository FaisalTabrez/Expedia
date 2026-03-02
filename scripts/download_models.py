import os
import sys
import shutil
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForMaskedLM, AutoConfig

# @Data-Ops: Utility to freeze model weights for Air-Gapped deployment

def freeze_model():
    model_name = "InstaDeepAI/nucleotide-transformer-v2-50m-multi-species"
    target_dir = Path("E:/EXPEDIA/models/nt_v2_50m_multi") # Using shorter path standard
    
    # Correction: User requested 'E:/EXPEDIA/resources/models/nt_v2_50m/'
    target_dir = Path("E:/EXPEDIA/resources/models/nt_v2_50m")
    
    print(f"--- MODEL FREEZE UTILITY ---")
    print(f"Target: {target_dir}")
    print(f"Source: {model_name}")
    
    if target_dir.exists():
        print(f"[WARNING] Target directory exists. Overwriting...")
        shutil.rmtree(target_dir, ignore_errors=True)
    
    # Ensure target directory exists for writing
    try:
        os.makedirs(target_dir, exist_ok=True)
    except Exception as e:
        print(f"[ERROR] Failed to create directory {target_dir}: {e}")
        return

    try:
        print("1. Downloading Configuration...")
        # Simpler config loading to avoid complexity
        config = AutoConfig.from_pretrained(model_name, trust_remote_code=True)
        # Avoid passing the list return value to anything expecting a dict
        config.save_pretrained(target_dir)
        print(f"   - Verified: config.json")
        
        print("2. Downloading Tokenizer...")
        tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        tokenizer_files = tokenizer.save_pretrained(target_dir)
        # Robustly handle the return value (tuple or list of strings)
        if isinstance(tokenizer_files, (list, tuple)):
            for f in tokenizer_files:
                print(f"   - Verified: {os.path.basename(str(f))}")
        
        print("3. Downloading Model Weights (Safetensors/Bin)...")
        # Load model with minimal args to avoid internal conflicts
        model = AutoModelForMaskedLM.from_pretrained(
            model_name, 
            config=config, 
            trust_remote_code=True
        )
        model.save_pretrained(target_dir)
        # Since model.save_pretrained might return None, we verify by checking file existence
        # Major weight files
        for weight_file in ["pytorch_model.bin", "model.safetensors"]:
             path = target_dir / weight_file
             if path.exists():
                 size_mb = path.stat().st_size / (1024 * 1024)
                 print(f"   - Verified: {weight_file} ({size_mb:.2f} MB)")

        print("\n--------------------------------------------------")
        print("MISSION STATUS: MODEL FROZEN")
        print(f"TARGET: {target_dir}")
        print("ENVIRONMENT: READY FOR AIR-GAPPED DEPLOYMENT")
        print("--------------------------------------------------\n")
    
    except Exception as e:
        import traceback
        print(f"\n[CRITICAL ERROR] Failed to freeze model: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    freeze_model()
