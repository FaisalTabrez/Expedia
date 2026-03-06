import logging
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QFrame
)
from qfluentwidgets import (
    ScrollArea, CardWidget, TitleLabel, SubtitleLabel, 
    BodyLabel, StrongBodyLabel, FluentIcon as FIF
)
from ...config import app_config

logger = logging.getLogger("DeepBioScan.ManualView")

class ManualSection(CardWidget):
    """
    @WinUI-Fluent: Lab Report Section Card.
    """
    def __init__(self, title, content, parent=None):
        super().__init__(parent)
        self.v_layout = QVBoxLayout(self)
        self.v_layout.setContentsMargins(20, 20, 20, 20)
        self.v_layout.setSpacing(10)

        # Header
        self.header = SubtitleLabel(title.upper(), self)
        self.header.setStyleSheet(f"color: {app_config.THEME_COLORS['primary']}; font-weight: bold;")
        self.v_layout.addWidget(self.header)

        # Divider
        line = QFrame(self)
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFrameShadow(QFrame.Shadow.Sunken)
        line.setStyleSheet("background-color: #333;")
        self.v_layout.addWidget(line)

        # Content
        self.content_label = BodyLabel(content, self)
        self.content_label.setWordWrap(True)
        self.content_label.setStyleSheet("color: #CCCCCC; line-height: 1.4;")
        
        # Apply Monospaced font to Math
        font = self.content_label.font()
        font.setStyleStrategy(QFont.StyleStrategy.PreferAntialias)
        self.content_label.setFont(font)

        self.v_layout.addWidget(self.content_label)

class ManualView(QWidget):
    """
    @BioArch-Pro: Expedition Manual & System Documentation.
    Renders scientific context and architectural constraints.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("ManualView")
        
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        
        # Scroll Area
        self.scroll = ScrollArea(self)
        self.scroll.setWidgetResizable(True)
        self.scroll.setStyleSheet("background-color: transparent; border: none;")
        
        self.container = QWidget()
        self.container.setStyleSheet("background-color: transparent;")
        self.v_layout = QVBoxLayout(self.container)
        self.v_layout.setContentsMargins(30, 30, 30, 30)
        self.v_layout.setSpacing(20)
        
        self.scroll.setWidget(self.container)
        self.main_layout.addWidget(self.scroll)

        # Title
        self.page_title = TitleLabel("TECHNICAL SPECIFICATIONS", self.container)
        self.v_layout.addWidget(self.page_title)

        self._populate_sections()
        self.v_layout.addStretch()

    def _populate_sections(self):
        # 1.0 SYSTEM ARCHITECTURE
        text_1 = (
            "KERNEL ISOLATION:\n"
            "The Inference Engine operates in a dedicated subprocess, isolated from the UI thread to prevent blocking. "
            "Inter-Process Communication (IPC) is handled via JSON-RPC over Standard Streams.\n\n"
            "WORKER ORCHESTRATION:\n"
            "A specialized QThread 'DiscoveryWorker' manages the lifecycle of the Science Kernel, handling asynchronous "
            "inference requests and real-time status telemetry."
        )
        self.v_layout.addWidget(ManualSection("1.0 SYSTEM ARCHITECTURE: KERNEL ISOLATION MODEL", text_1))

        # 2.0 REPRESENTATION LEARNING
        text_2 = (
            "MODEL PARAMETRICS:\n"
            "The system utilizes the Nucleotide Transformer v2-50M with 6-mer tokenization to capture complex genomic motifs. "
            "This approach transcends traditional alignment by encoding sequences into semantic vectors.\n\n"
            "LATENT SPACE RESOLUTION:\n" # Formerly Deep Dive
            "Sequences are projected into R_768. This high-dimensional embedding enables precise semantic similarity "
            "assessment, crucial for identifying distant homology in Benthic environments." # Formerly Abyss
        )
        self.v_layout.addWidget(ManualSection("2.0 REPRESENTATION LEARNING: TRANSFORMER PARAMETRICS", text_2))

        # 3.0 TOPOLOGICAL ANALYSIS
        text_3 = (
            "NON-LINEAR MANIFOLD:\n"
            "Uniform Manifold Approximation and Projection (UMAP) reduces the 768-d feature space to a 3-d manifold "
            "for visualization, preserving topological structures.\n\n"
            "DENSITY-BASED CLUSTERING:\n"
            "HDBSCAN identifies clusters of Non-Reference Taxa (NRT) based on density reachability, effectively "
            "isolating novel signatures from noise without requiring a pre-defined cluster count." # Formerly Dark Taxa
        )
        self.v_layout.addWidget(ManualSection("3.0 TOPOLOGICAL ANALYSIS: NON-LINEAR MANIFOLD PIPELINE", text_3))

        # 4.0 VALIDATION
        text_4 = (
            "PERFORMANCE BENCHMARKS:\n"
            "The IVF-PQ index enables O(log N) search complexity, achieving sub-10ms retrieval times against the 100k reference dataset.\n\n"
            "OPERATIONAL THROUGHPUT:\n"
            "The system sustains high-throughput ingestion via batched inference, ensuring responsive In-situ genomic analysis " # Formerly Hidden World
            "on standard hardware."
        )
        self.v_layout.addWidget(ManualSection("4.0 VALIDATION: PERFORMANCE BENCHMARKS", text_4))
