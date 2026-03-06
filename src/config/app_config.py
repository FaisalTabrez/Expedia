import sys
import logging
from pathlib import Path
import os
import string
import ctypes

# -----------------------------------------------------------------------------
# HARDWARE ANCHOR: INTELLIGENT VOLUME DISCOVERY
# -----------------------------------------------------------------------------

def detect_expedia_root():
    """
    @Data-Ops: Scans available drives for 'EXPEDIA_Data' anchor.
    Priority: Enviroment Var -> External (D-Z) -> C: Fallback.
    """
    # 1. Environment Variable Override (Science Kernel Sync)
    if "EXPEDIA_ROOT_PATH" in os.environ:
        override = Path(os.environ["EXPEDIA_ROOT_PATH"])
        if override.exists():
            return override

    scanned_locations = []
    
    # Get Logical Drives
    drives = []
    bitmask = ctypes.windll.kernel32.GetLogicalDrives()
    for letter in string.ascii_uppercase:
        if bitmask & 1:
            drives.append(letter)
        bitmask >>= 1
        
    # 2. Scan External Drives (D: to Z:)
    # We prioritize E: if available by checking it explicitly or just strictly alpha?
    # Prompt says: "D: to Z:, then E:, then C:"
    # This implies D, E, F... Z. So just alphabetical is fine as E is included.
    # But if we want to ensure E is checked "specifically" or maybe checking D-Z implies E is checked.
    
    candidates = [d for d in drives if d >= 'D']
    for drive in candidates:
        # Check for 'EXPEDIA_Data'
        candidate_path = Path(f"{drive}:/EXPEDIA_Data")
        scanned_locations.append(str(candidate_path))
        if candidate_path.exists():
            return candidate_path

    # 3. Fallback: C:/EXPEDIA_Data
    c_path = Path("C:/EXPEDIA_Data")
    scanned_locations.append(str(c_path))
    if c_path.exists():
        return c_path

    # 4. Critical Failure
    drive_list_str = "\n".join(scanned_locations)
    msg = (
        f"CRITICAL HARDWARE DISCONNECT\n\n"
        f"The EXPEDIA_Data anchor volume could not be located.\n"
        f"Please insert the EXPEDIA Array Drive (USB).\n\n"
        f"Scanned Locations:\n{drive_list_str}"
    )
    # 0x10 = Stop Icon, 0x0 = OK Button
    ctypes.windll.user32.MessageBoxW(0, msg, "EXPEDIA: HARDWARE FAILURE", 0x10)
    sys.exit(1)

# Execute Discovery
DATA_ROOT = detect_expedia_root()
BASE_DRIVE = DATA_ROOT # Alias check

# Auxiliary Maps (Relative to Discovered Root)
VECTOR_DB_PATH = DATA_ROOT / "data/db"
TAXONKIT_EXE = DATA_ROOT / "taxonkit.exe"
TAXDATA_DIR = DATA_ROOT / "data/taxonomy_db"
WORMS_CSV = TAXDATA_DIR / "worms_deepsea_ref.csv"

# AI Model Anchor
LOCAL_MODEL_PATH = DATA_ROOT / "resources/models/nt_v2_50m"

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
THRESHOLD_NOVEL = 0.70      # <70% for Novel Taxonomic Units (NTUs)

# Inference Logic
CONSENSUS_K_NEIGHBORS = 50   # Number of neighbors for majority vote

# -----------------------------------------------------------------------------
# UI/THEME CONFIGURATION
# -----------------------------------------------------------------------------

THEME_COLORS = {
    "background": "#0F0F0F",  # True Black/Onyx
    "primary": "#0078D4",     # Windows Professional Blue
    "accent": "#0078D4",      # Windows Professional Blue (Unified)
    "sidebar": "#1A1A1A",     # Sidebar Dark Grey
    "border": "#333333",      # Subtle Border
    "text_primary": "#FFFFFF", # White
    "foreground": "#FFFFFF"   # Added for backward compatibility/Dashboard
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
