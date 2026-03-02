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
        self.page_title = TitleLabel("SYSTEM ARCHITECTURE & MANUAL", self.container)
        self.v_layout.addWidget(self.page_title)

        self._populate_sections()
        self.v_layout.addStretch()

    def _populate_sections(self):
        # 1. Systemic Constraints
        text_1 = (
            "THE REFERENCE GAP:\n"
            "Traditional BLAST algorithms fail on novel taxa due to reliance on exact string matching. "
            "Deep-sea biodiversity assessment faces a 'Reference Gap' where >90% of eDNA sequences "
            "lack direct matches in public databases (NCBI/GenBank).\n\n"
            "COMPUTATIONAL WALL:\n"
            "Pairwise alignment scales at O(N) complexity per query against N references. "
            "For large environmental batches, this becomes computationally intractable on standard hardware. "
            "DeepBio-Scan Pro utilizes approximate nearest neighbor search to achieve O(log N) complexity."
        )
        self.v_layout.addWidget(ManualSection("1. SYSTEMIC CONSTRAINTS", text_1))

        # 2. Genomic Representation Learning
        text_2 = (
            "MODEL FOUNDATION:\n"
            "The system utilizes the Nucleotide Transformer v2-50M, a foundational model pretrained on "
            "diverse genomic datasets. It employs 6-mer tokenization to capture local sequence motifs.\n\n"
            "LATENT SPACE MANIFOLD:\n"
            "Sequences are projected into a high-dimensional feature space R_768. Unlike scalar metrics, "
            "this embedding captures semantic evolutionary relationships, allowing for the detection of "
            "homology even in the absence of significant sequence identity."
        )
        self.v_layout.addWidget(ManualSection("2. GENOMIC REPRESENTATION LEARNING", text_2))

        # 3. Vector Engineering
        text_3 = (
            "INDEXING STRATEGY:\n"
            "To manage the 100,000+ reference signatures, the system employs an Inverted File Index "
            "with Product Quantization (IVF-PQ) via LanceDB. This technique partitions the vector space "
            "using Voronoi cells, reducing the search scope.\n\n"
            "PERFORMANCE:\n"
            "This architecture enables sub-10ms query latency on Volume E: (NTFS) via disk-native memory "
            "mapping, maintaining low RAM footprint during high-throughput inference."
        )
        self.v_layout.addWidget(ManualSection("3. VECTOR ENGINEERING", text_3))

        # 4. Hybrid Inference
        text_4 = (
            "TRIPLE-TIER RESOLUTION:\n"
            "Tier 1 (Consensus): Determines classification via majority vote of the k=50 nearest neighbors "
            "in the latent space.\n"
            "Tier 2 (Validation): Cross-references candidates against the World Register of Marine Species "
            "(WoRMS) for taxonomic authority.\n"
            "Tier 3 (Reconstruction): Utilizes TaxonKit to expand lineage metadata for resolved taxa."
        )
        self.v_layout.addWidget(ManualSection("4. HYBRID INFERENCE ENGINE", text_4))

        # 5. NRT Discovery
        text_5 = (
            "NOVEL TAXONOMIC UNITS (NTU):\n"
            "Sequences with a semantic similarity score < 85% relative to the best reference match are "
            "flagged as Non-Reference Taxa (NRT). These entities represent potential novel biodiversity.\n\n"
            "UNSUPERVISED CLUSTERING:\n"
            "The Discovery Engine aggregates NRTs and utilizes HDBSCAN to identify density-based clusters. "
            "Valid clusters are designated as NTUs, and their centroids are calculated to represent the "
            "hypothetical consensus sequence of the new taxon."
        )
        self.v_layout.addWidget(ManualSection("5. NOVELTY DISCOVERY FRAMEWORK", text_5))
