
import sys
import unittest
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout
from PySide6.QtCore import Qt, Signal

# Mock dependencies
class MockAppConfig:
    THEME_COLORS = {'accent': '#00ADB5', 'background': '#222831'}
sys.modules['src.config'] = type('config_module', (), {})()
sys.modules['src.config'].app_config = MockAppConfig()
sys.modules['...config'] = sys.modules['src.config']

# Stub qfluentwidgets
class MockCardWidget(QWidget):
    def __init__(self, parent=None): super().__init__(parent)
class MockLabel(QWidget):
    def __init__(self, text, parent=None): super().__init__(parent)
    def setText(self, t): pass
    def setStyleSheet(self, s): pass
    def setAlignment(self, a): pass
class MockButton(QWidget):
    def __init__(self, icon, text, parent=None): 
        super().__init__(parent)
        self.clicked = type('signal', (), {'connect': lambda x: None})()
    def setText(self, t): pass
    def setEnabled(self, e): pass

import qfluentwidgets
qfluentwidgets.CardWidget = MockCardWidget
qfluentwidgets.TitleLabel = MockLabel
qfluentwidgets.SubtitleLabel = MockLabel
qfluentwidgets.CaptionLabel = MockLabel
qfluentwidgets.PrimaryPushButton = MockButton
qfluentwidgets.FluentIcon = type('FIF', (), {'GLOBE': 'globe', 'INFO': 'info'})
qfluentwidgets.ProgressBar = QWidget
qfluentwidgets.InfoBar = type('InfoBar', (), {'success': lambda *a, **k: None, 'warning': lambda *a, **k: None})
qfluentwidgets.InfoBarPosition = type('Pos', (), {'TOP_RIGHT': 1})

# Now load the actual view file
# We need to hack the import path
import os
sys.path.append(os.path.join(os.getcwd(), 'src'))
# This might fail if relative imports are strictly enforced.
# Instead, let's just copy the relevant class definitions or try to import if path is set.

# Since we can't easily import the complex module structure with relative imports in this environment,
# I will try to verify the logic by creating a standalone test that mimics the known structure.

# But actually, the best way to debug "not proceeding properly" is to verify the data integrity.

def test_payload_integrity():
    # Mock data from ScienceKernel
    ntu_payload = {
        "ntu_id": "EXPEDIA-NTU-123456-1",
        "anchor_taxon": "Pseudomonas",
        "lineage": "Bacteria;...",
        "size": 15,
        "divergence": 0.05,
        "mean_confidence": 0.98,
        "holotype_confidence": 0.99,
        "centroid_id": "SEQ_001",
        "centroid_vector": [0.1] * 768,
        "members": ["SEQ_001", "SEQ_002"],
        "cluster_label": 1
    }
    
    # Simulate NTUCard logic
    # 1. ID Check
    ntu_id = ntu_payload.get("ntu_id", "UNKNOWN-NTU")
    if "EXPEDIA-NTU" not in ntu_id:
        ntu_id = f"EXPEDIA-NTU-{ntu_id}"
    print(f"ID: {ntu_id}")
    
    # 2. Vector Check
    has_vector = ntu_payload.get("centroid_vector") is not None or ntu_payload.get("centroid") is not None
    print(f"Has Vector: {has_vector}")
    
    # 3. Divergence Type Check
    div_raw = ntu_payload.get("divergence")
    print(f"Div Raw: {div_raw} ({type(div_raw)})")
    div_float = float(div_raw)
    print(f"Div Float: {div_float}")

if __name__ == "__main__":
    test_payload_integrity()
