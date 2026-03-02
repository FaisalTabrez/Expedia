import sys
import logging
from pathlib import Path
import os

# -----------------------------------------------------------------------------
# HARDWARE ANCHOR: VOLUME E:
# -----------------------------------------------------------------------------

# Primary Data Volume (NTFS required for atomic renames)
BASE_DRIVE = Path("E:/EXPEDIA")
_fallback_drive = Path("C:/EXPEDIA_Data") # Dev fallback only

if BASE_DRIVE.exists():
    DATA_ROOT = BASE_DRIVE
else:
    DATA_ROOT = _fallback_drive

# Auxiliary Maps
VECTOR_DB_PATH = DATA_ROOT / "data/db"
TAXONKIT_EXE = DATA_ROOT / "taxonkit.exe"
TAXDATA_DIR = DATA_ROOT / "data/taxonomy_db"
WORMS_CSV = TAXDATA_DIR / "worms_deepsea_ref.csv"

# AI Model Anchor
LOCAL_MODEL_PATH = DATA_ROOT / "resources/models/nt_v2_50m"
MODEL_INTERMEDIATE_SIZE = 4096 # Configuration Patch for v2-50m

# -----------------------------------------------------------------------------
# DATABASE CONSTANTS
# -----------------------------------------------------------------------------

ATLAS_TABLE_NAME = "reference_atlas_v100k"
REFERENCE_SIGNATURES_COUNT = 100_000

# LanceDB Index Configuration (IVF-PQ)
LANCEDB_PARTITIONS = 128
LANCEDB_SUB_VECTORS = 96
EMBEDDING_DIMENSION_MODEL = 512
EMBEDDING_DIMENSION_PADDED = 768

# -----------------------------------------------------------------------------
# HARDWARE VERIFICATION
# -----------------------------------------------------------------------------

def verify_auxiliaries():
    """
    Checks for critical hardware auxiliaries on Volume E:.
    Returns: (bool, str) -> (Success, Error Message)
    """
    missing = []
    
    # Check 1: LanceDB Folder
    if not VECTOR_DB_PATH.exists():
        missing.append(f"Vector Database not found at: {VECTOR_DB_PATH}")
        
    # Check 2: TaxonKit Binary
    if not TAXONKIT_EXE.exists():
        missing.append(f"TaxonKit Binary not found at: {TAXONKIT_EXE}")

    # Check 3: WoRMS Reference
    if not WORMS_CSV.exists():
        missing.append(f"WoRMS Reference CSV not found at: {WORMS_CSV}")

    # Check 4: AI Model Weights (Air-Gapped)
    if not LOCAL_MODEL_PATH.exists():
        missing.append(f"Nucleotide Transformer weights not found at: {LOCAL_MODEL_PATH}")
        
    if missing:
        error_msg = "\n".join(missing)
        return False, f"HARDWARE DISCONNECT: PLEASE INSERT VOLUME E:\n\nMISSING RESOURCES:\n{error_msg}"
    
    return True, "Volume E: Auxiliaries Verified."

# Re-export old name for compatibility if needed, but we rely on VECTOR_DB_PATH now
# VECTOR_DB_PATH is already defined above

# -----------------------------------------------------------------------------
# AI & MODEL CONFIGURATION
# -----------------------------------------------------------------------------

MODEL_NAME = "Nucleotide Transformer v2-50M"
# Patching config to prevent SwiGLU tensor errors
# Halving the intermediate size because SwiGLU doubles it in the model implementation
MODEL_INTERMEDIATE_SIZE = 2048

# -----------------------------------------------------------------------------
# TAXONOMY THRESHOLDS
# -----------------------------------------------------------------------------

# Similarity Thresholds
THRESHOLD_CONFIRMED = 0.95  # >95% for species confirmation
THRESHOLD_NOVEL = 0.85      # <85% for Novel Taxonomic Units (NTUs)

# Inference Logic
CONSENSUS_K_NEIGHBORS = 50   # Number of neighbors for majority vote

# -----------------------------------------------------------------------------
# UI/THEME CONFIGURATION
# -----------------------------------------------------------------------------

THEME_COLORS = {
    "background": "#0A0F1E",  # Abyss Dark
    "primary": "#00E5FF",     # Cyan
    "accent": "#FF007A",      # Neon Pink
}

NAVIGATION_ITEMS = [
    "MONITOR",
    "MANIFOLD",
    "INFERENCE",
    "DISCOVERY",
    "DOCUMENTATION"
]

# -----------------------------------------------------------------------------
# LOGGING CONFIGURATION
# -----------------------------------------------------------------------------

# Logs directory (Keep logs local to app, or on E: if preferred. Defaulting to local relative to executable for safety)
# If app is installed in Program Files, this might need to change to AppData. 
# For portable USB app, local is fine.
BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
SESSION_LOG_PATH = LOGS_DIR / "session.log"

def setup_logging():
    """
    Configures the logging system to write to file and console.
    """
    logger = logging.getLogger("EXPEDIA")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File Handler
    file_handler = logging.FileHandler(SESSION_LOG_PATH, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console Handler (for dev/debugging)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

# Initialize logger
app_logger = setup_logging()
