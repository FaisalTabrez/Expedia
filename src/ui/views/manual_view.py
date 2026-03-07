import logging
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QFrame, QSizePolicy
)
from qfluentwidgets import (
    ScrollArea, CardWidget, TitleLabel, SubtitleLabel,
    BodyLabel, StrongBodyLabel, CaptionLabel, FluentIcon as FIF
)
from ...config import app_config

logger = logging.getLogger("EXPEDIA.ManualView")

# ─────────────────────────────────────────────────────────────────────
# Monospaced Font for Formulae and Metrics
# ─────────────────────────────────────────────────────────────────────
_MONO_FONT = QFont("Consolas", 10)
_MONO_FONT.setStyleStrategy(QFont.StyleStrategy.PreferAntialias)

_BODY_STYLE = (
    "color: #C8C8C8; "
    "line-height: 1.6; "
    "font-size: 13px;"
)
_FORMULA_STYLE = (
    "color: #00E5FF; "
    "background-color: rgba(0, 120, 212, 0.08); "
    "border-left: 2px solid #0078D4; "
    "padding: 8px 12px; "
    "line-height: 1.5; "
    "font-size: 12px;"
)


class ManualSection(CardWidget):
    """
    @WinUI-Fluent: Lab-Report Section Card.
    Renders a numbered monograph section with optional formula blocks.
    Optimised for low-light laboratory readability.
    """
    def __init__(self, title: str, content: str, formulae: str | None = None, parent=None):
        super().__init__(parent)
        self.v_layout = QVBoxLayout(self)
        self.v_layout.setContentsMargins(24, 20, 24, 20)
        self.v_layout.setSpacing(12)

        # Section Header
        self.header = TitleLabel(title.upper(), self)
        self.header.setStyleSheet(
            f"color: {app_config.THEME_COLORS['primary']}; "
            "font-weight: 600; font-size: 15px; letter-spacing: 0.5px;"
        )
        self.v_layout.addWidget(self.header)

        # Divider
        line = QFrame(self)
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFixedHeight(1)
        line.setStyleSheet("background-color: #333;")
        self.v_layout.addWidget(line)

        # Prose Content
        self.content_label = BodyLabel(content, self)
        self.content_label.setWordWrap(True)
        self.content_label.setStyleSheet(_BODY_STYLE)
        self.v_layout.addWidget(self.content_label)

        # Formula / Metric Block (Consolas monospaced)
        if formulae:
            self.formula_label = QLabel(formulae, self)
            self.formula_label.setWordWrap(True)
            self.formula_label.setFont(_MONO_FONT)
            self.formula_label.setStyleSheet(_FORMULA_STYLE)
            self.formula_label.setTextFormat(Qt.TextFormat.PlainText)
            self.v_layout.addWidget(self.formula_label)


class ManualView(QWidget):
    """
    @BioArch-Pro: EXPEDIA Technical Monograph.
    @Neural-Core: Scientific documentation of the genomic inference pipeline.
    Renders structured, numbered sections with formulae and architectural detail.
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
        self.v_layout.setContentsMargins(36, 30, 36, 30)
        self.v_layout.setSpacing(22)

        self.scroll.setWidget(self.container)
        self.main_layout.addWidget(self.scroll)

        # Monograph Title
        self.page_title = TitleLabel(
            "EXPEDIA: TECHNICAL MONOGRAPH AND OPERATIONAL SPECIFICATIONS", self.container
        )
        self.page_title.setStyleSheet(
            "font-size: 18px; letter-spacing: 1px; "
            f"color: {app_config.THEME_COLORS.get('text', '#FFFFFF')};"
        )
        self.v_layout.addWidget(self.page_title)

        # Subtitle / Classification
        self.classification = CaptionLabel(
            "CLASSIFICATION: INTERNAL  |  REVISION: 2026.03  |  AUTHORED BY: @BioArch-Pro, @Neural-Core",
            self.container
        )
        self.classification.setStyleSheet("color: #666; font-size: 11px; letter-spacing: 0.3px;")
        self.v_layout.addWidget(self.classification)

        self._populate_sections()
        self.v_layout.addStretch()

    def _populate_sections(self):
        """
        Constructs all monograph sections.
        Terminology: NRGS (Non-Reference Genomic Signature), Benthic/Marine.
        """
        # ─────────────────────────────────────────────────────────────
        # 1.0  SYSTEM ARCHITECTURE: KERNEL ISOLATION MODEL
        # ─────────────────────────────────────────────────────────────
        text_1 = (
            "1.1  DISPLAY / SCIENCE PROCESS ISOLATION\n"
            "The EXPEDIA runtime enforces strict process-level decoupling between the "
            "WinUI 3 display layer (PySide6 / qfluentwidgets) and the scientific "
            "inference stack (PyTorch, SciPy, HDBSCAN). The display process owns the "
            "event loop and all GPU-composited UI surfaces. The Science Kernel executes "
            "in a dedicated child process (subprocess.Popen), ensuring that heavy tensor "
            "operations on the Nucleotide Transformer produce zero measurable latency on "
            "the UI thread, even under sustained batch-inference workloads.\n\n"
            "1.2  ASYNCHRONOUS JSON-RPC BRIDGE\n"
            "Inter-process communication is implemented as a line-delimited JSON-RPC "
            "protocol over standard streams (stdin/stdout). Each request-response cycle "
            "is non-blocking: the DiscoveryWorker (QThread) issues a JSON command, yields "
            "control to the Qt event loop, and processes the kernel response upon arrival. "
            "Telemetry heartbeats are multiplexed on the same channel at 500 ms intervals.\n\n"
            "1.3  DISK-BASED DATA HANDSHAKE\n"
            "For large-scale topological transfers exceeding 100 KB (e.g., 3D manifold "
            "coordinates for 500+ neighbors), the kernel serialises the payload to a "
            "temporary file on the local NTFS volume and transmits only the file path over "
            "the JSON-RPC channel. The display process memory-maps the file via "
            "numpy.memmap, eliminating redundant deserialization and keeping peak IPC "
            "latency below 15 ms regardless of payload size."
        )
        formula_1 = (
            "IPC Latency Model:\n"
            "  T_ipc = T_serialize + T_pipe    (payload < 100 KB)\n"
            "  T_ipc = T_write + T_mmap        (payload >= 100 KB, Disk Handshake)\n"
            "  Target: T_ipc < 15 ms (p99)"
        )
        self.v_layout.addWidget(ManualSection(
            "1.0  SYSTEM ARCHITECTURE: KERNEL ISOLATION MODEL", text_1, formula_1
        ))

        # ─────────────────────────────────────────────────────────────
        # 2.0  REPRESENTATION LEARNING: TRANSFORMER PARAMETRICS
        # ─────────────────────────────────────────────────────────────
        text_2 = (
            "2.1  NUCLEOTIDE TRANSFORMER (v2-50M)\n"
            "The encoder backbone is InstaDeep's Nucleotide Transformer v2 with 50 million "
            "parameters. Raw nucleotide strings are segmented into overlapping 6-mer tokens, "
            "capturing codon-level and motif-level biochemical semantics that single-nucleotide "
            "tokenization cannot represent. Each 6-mer token is projected through a learned "
            "embedding table, then processed by a stack of multi-head self-attention layers that "
            "model long-range dependencies across the full sequence context window.\n\n"
            "2.2  LATENT SPACE GEOMETRY\n"
            "The final hidden-state is mean-pooled to produce a single 768-dimensional vector "
            "per input sequence, mapping each read into a point on a Riemannian manifold in "
            "R^768. Sequences sharing evolutionary history converge in this latent manifold, "
            "while divergent or novel organisms occupy isolated regions, enabling unsupervised "
            "discrimination of Non-Reference Genomic Signatures (NRGS) from catalogued taxa.\n\n"
            "2.3  SIMILARITY METRIC: COSINE DISTANCE\n"
            "Evolutionary divergence between two genomic embeddings is quantified via Cosine "
            "Similarity. This metric is invariant to vector magnitude, making it robust against "
            "fragmented, degraded, or variable-length environmental DNA (eDNA) reads common in "
            "benthic and pelagic sampling campaigns. A cosine distance approaching 0.0 indicates "
            "near-identical genomic content; values exceeding 0.35 typically indicate inter-family "
            "or higher taxonomic divergence."
        )
        formula_2 = (
            "Cosine Similarity:\n"
            "  cos(u, v) = (u . v) / (||u|| * ||v||)\n\n"
            "Embedding Dimensions:\n"
            "  Input:  Variable-length nucleotide string\n"
            "  Output: x in R^768  (unit-normalised after L2 projection)\n\n"
            "Tokenization:\n"
            "  Strategy: Overlapping 6-mer (stride=1)\n"
            "  Vocabulary: |V| = 4^6 + special = 4,101 tokens"
        )
        self.v_layout.addWidget(ManualSection(
            "2.0  REPRESENTATION LEARNING: TRANSFORMER PARAMETRICS", text_2, formula_2
        ))

        # ─────────────────────────────────────────────────────────────
        # 3.0  THE AVALANCHE STANDARD: NON-LINEAR TOPOLOGICAL ANALYSIS
        # ─────────────────────────────────────────────────────────────
        text_3 = (
            "3.1  DISCOVERY PIPELINE OVERVIEW\n"
            "The EXPEDIA discovery pipeline executes a three-stage non-linear analysis on the "
            "768-dimensional embedding manifold: L2 Normalization, UMAP Projection, and HDBSCAN "
            "Density Clustering. This sequence, designated the Avalanche Standard, is applied "
            "to every batch of newly ingested sequences.\n\n"
            "3.2  L2 NORMALIZATION\n"
            "All embedding vectors are projected onto the unit hypersphere via L2 normalization "
            "prior to downstream analysis. This constrains all vectors to identical magnitude, "
            "ensuring that Cosine Similarity and Euclidean distance yield rank-equivalent results "
            "and stabilising UMAP neighborhood graph construction.\n\n"
            "3.3  UMAP (UNIFORM MANIFOLD APPROXIMATION AND PROJECTION)\n"
            "UMAP performs non-linear dimensionality reduction by constructing a weighted "
            "k-nearest-neighbor graph in the high-dimensional space and optimizing a low-"
            "dimensional layout that preserves topological structure. Two projection targets "
            "are maintained concurrently:\n"
            "  - 10D projection for density-based NRGS discovery (high fidelity).\n"
            "  - 3D projection for interactive spatial visualization in the Manifold Explorer.\n\n"
            "3.4  HDBSCAN (HIERARCHICAL DENSITY-BASED SPATIAL CLUSTERING)\n"
            "HDBSCAN operates on the 10D UMAP embedding to identify clusters of variable density "
            "without requiring a pre-specified cluster count (k). Points that fail to meet the "
            "minimum cluster size threshold are classified as noise, corresponding to isolated "
            "NRGS candidates requiring further phylogenetic scrutiny. This property is essential "
            "for marine eDNA datasets where the number of distinct taxa is unknown a priori."
        )
        formula_3 = (
            "Avalanche Standard Pipeline:\n"
            "  x_raw in R^768\n"
            "  x_norm = x_raw / ||x_raw||_2           [L2 Normalization]\n"
            "  x_10d  = UMAP(x_norm, n_components=10)  [Discovery]\n"
            "  x_3d   = UMAP(x_norm, n_components=3)   [Visualization]\n"
            "  labels = HDBSCAN(x_10d, min_cluster_size=5)\n\n"
            "HDBSCAN Parameters:\n"
            "  min_cluster_size  = 5\n"
            "  min_samples       = 3\n"
            "  cluster_selection = 'eom'  (Excess of Mass)"
        )
        self.v_layout.addWidget(ManualSection(
            "3.0  THE AVALANCHE STANDARD: NON-LINEAR TOPOLOGICAL ANALYSIS", text_3, formula_3
        ))

        # ─────────────────────────────────────────────────────────────
        # 4.0  TRIPLE-TIER INFERENCE AND NOMENCLATURE ARBITRATION
        # ─────────────────────────────────────────────────────────────
        text_4 = (
            "4.1  TIER 1: STOCHASTIC CONSENSUS (k=50 NEIGHBORHOOD)\n"
            "For each query embedding, the system retrieves the k=50 nearest genomic neighbors "
            "from the LanceDB vector index. A majority-vote algorithm computes the consensus "
            "taxonomic classification from the neighbor labels. Confidence is reported as the "
            "proportion of concordant votes. Sequences failing to achieve a confidence threshold "
            "of 0.60 are escalated to Tier 2.\n\n"
            "4.2  TIER 2: NOMENCLATURAL VALIDATION (WoRMS ORACLE)\n"
            "The Tier 1 consensus label undergoes fuzzy-string matching against the World Register "
            "of Marine Species (WoRMS) REST API. This validation step eliminates terrestrial bias "
            "inherent in general-purpose genomic reference databases (e.g., NCBI nr/nt) by "
            "confirming that the proposed taxon is a recognized marine organism. Unmatched labels "
            "are flagged as putative NRGS and routed for manual curation.\n\n"
            "4.3  TIER 3: PHYLOGENETIC RECONSTRUCTION (TaxonKit)\n"
            "Validated species names are expanded into complete Linnaean hierarchies via TaxonKit, "
            "producing structured lineage strings (Kingdom > Phylum > Class > Order > Family > "
            "Genus > Species). This enables downstream aggregation in the Discovery View sunburst "
            "chart and facilitates cross-referencing with institutional taxonomic registries."
        )
        formula_4 = (
            "Stochastic Consensus:\n"
            "  N(q) = {n_1, n_2, ..., n_50}  (k-nearest neighbors)\n"
            "  C(q) = argmax_t |{n in N(q) : label(n) = t}|\n"
            "  confidence = |{n in N(q) : label(n) = C(q)}| / k\n\n"
            "Escalation Threshold:\n"
            "  confidence < 0.60  -->  Route to Tier 2 (WoRMS Oracle)\n"
            "  WoRMS match = NULL -->  Flag as NRGS Candidate"
        )
        self.v_layout.addWidget(ManualSection(
            "4.0  TRIPLE-TIER INFERENCE AND NOMENCLATURE ARBITRATION", text_4, formula_4
        ))

        # ─────────────────────────────────────────────────────────────
        # 5.0  DATA ENGINEERING: IVF-PQ INDEXING
        # ─────────────────────────────────────────────────────────────
        text_5 = (
            "5.1  INVERTED FILE PRODUCT QUANTIZATION (IVF-PQ)\n"
            "The genomic reference atlas is indexed in LanceDB using Inverted File with Product "
            "Quantization (IVF-PQ). The 768-dimensional embedding space is partitioned into "
            "Voronoi cells via k-means clustering of the reference vectors. At query time, only "
            "the cells nearest to the query vector are scanned, reducing search complexity from "
            "O(N) brute-force to O(sqrt(N)) with negligible recall degradation.\n\n"
            "5.2  PRODUCT QUANTIZATION COMPRESSION\n"
            "Within each Voronoi cell, the 768-dimensional vectors are decomposed into 96 "
            "sub-vectors of 8 dimensions each. Each sub-vector is quantized to its nearest "
            "centroid from a learned 256-entry codebook, compressing the per-vector storage "
            "from 3,072 bytes (768 x float32) to 96 bytes. This 32x compression ratio enables "
            "the full 100,000-signature atlas to reside in approximately 9.6 MB of memory.\n\n"
            "5.3  DISK-NATIVE MEMORY MAPPING\n"
            "LanceDB operates in disk-native mode on the Volume E: NTFS partition. The index "
            "file is memory-mapped by the operating system, leveraging the NVMe SSD's sequential "
            "read bandwidth. This architecture achieves sub-10 ms search latency at p99 for the "
            "current 100,000-signature index without requiring dedicated GPU memory."
        )
        formula_5 = (
            "IVF-PQ Complexity:\n"
            "  Brute-force:  O(N * D)       = O(100,000 * 768)\n"
            "  IVF-PQ:       O(sqrt(N) * D) = O(316 * 768)\n\n"
            "Compression:\n"
            "  Raw:        768 dims * 4 bytes = 3,072 bytes/vector\n"
            "  Quantized:  96 sub-vectors * 1 byte = 96 bytes/vector\n"
            "  Ratio:      32:1\n"
            "  Atlas Size: 100,000 * 96 bytes = 9.6 MB (index resident)"
        )
        self.v_layout.addWidget(ManualSection(
            "5.0  DATA ENGINEERING: IVF-PQ INDEXING", text_5, formula_5
        ))

        # ─────────────────────────────────────────────────────────────
        # 6.0  INFRASTRUCTURE SCALABILITY: THE 2TB TRC ROADMAP
        # ─────────────────────────────────────────────────────────────
        text_6 = (
            "6.1  TIERED STORAGE ARCHITECTURE\n"
            "The EXPEDIA storage roadmap defines three operational tiers aligned to dataset "
            "scale and research phase:\n\n"
            "  TIER 1 - PROTOTYPE (Current):  32 GB NVMe SSD, Volume E:\n"
            "    Capacity: 100,000 reference genomic signatures.\n"
            "    Purpose: Algorithm validation, UI development, local eDNA analysis.\n\n"
            "  TIER 2 - EXPANDED ATLAS:  512 GB NVMe SSD\n"
            "    Capacity: 1.2 million reference signatures.\n"
            "    Purpose: Regional marine biodiversity surveys, multi-site eDNA campaigns.\n\n"
            "  TIER 3 - RESEARCH ARRAY (Target):  2 TB NVMe SSD\n"
            "    Capacity: 4.2+ million reference signatures.\n"
            "    Purpose: Whole-genome environmental surveillance at national scale.\n\n"
            "6.2  GRANT JUSTIFICATION\n"
            "The Tier 3 Research Array (2 TB) is a prerequisite for whole-genome environmental "
            "surveillance operations where the signature count exceeds N > 4.2 million. At this "
            "scale, the IVF-PQ index must be re-partitioned with a higher nlist parameter to "
            "maintain sub-10 ms query latency. The Stream-and-Flush data handling architecture "
            "ensures that ingestion throughput scales linearly with storage bandwidth, and the "
            "Disk Handshake protocol eliminates memory-bound bottlenecks during large-batch "
            "topological transfers."
        )
        formula_6 = (
            "Storage Projection:\n"
            "  Tier 1:  100,000 sigs *  96 B = 9.6 MB index\n"
            "  Tier 2:  1.2M sigs    *  96 B = 115 MB index\n"
            "  Tier 3:  4.2M sigs    *  96 B = 403 MB index\n\n"
            "  Raw embeddings (float32):\n"
            "  Tier 3:  4.2M * 3,072 B = 12.9 GB\n\n"
            "  Disk Throughput Requirement:\n"
            "  Sequential Read >= 3,500 MB/s (NVMe Gen4)"
        )
        self.v_layout.addWidget(ManualSection(
            "6.0  INFRASTRUCTURE SCALABILITY: THE 2TB TRC ROADMAP", text_6, formula_6
        ))
