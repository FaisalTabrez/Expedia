import logging
import numpy as np
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, 
    QLabel, QScrollArea, QFrame, QSizePolicy
)
from qfluentwidgets import (
    TitleLabel, SubtitleLabel, CaptionLabel,
    CardWidget, PrimaryPushButton, FluentIcon as FIF,
    ProgressBar, InfoBar, InfoBarPosition
)
from ...config import app_config

logger = logging.getLogger("DeepBioScan.DiscoveryView")

class SessionSummaryPanel(QFrame):
    """
    @WinUI-Fluent: Session Summary Panel.
    Displays aggregate novelty statistics.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(120)  # slightly taller for info
        self.setStyleSheet(f"background-color: {app_config.THEME_COLORS['background']}; border-bottom: 1px solid #333;")
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(20, 10, 20, 10)
        
        # Title
        title_layout = QVBoxLayout()
        title_layout.addWidget(TitleLabel("NOVELTY DISCOVERY", self))
        title_layout.addWidget(CaptionLabel("EXPEDITION ANALYSIS", self))
        layout.addLayout(title_layout)
        
        layout.addStretch()
        
        # Stats
        self.total_processed_label = self._create_stat("PROCESSED", "0")
        layout.addLayout(self.total_processed_label[0])
        
        self.ntus_found_label = self._create_stat("NTUS IDENTIFIED", "0", color=app_config.THEME_COLORS['accent'])
        layout.addLayout(self.ntus_found_label[0])
        
        self.novelty_rate_label = self._create_stat("NOVELTY RATE", "0.0%", color=app_config.THEME_COLORS['accent'])
        layout.addLayout(self.novelty_rate_label[0])

    def _create_stat(self, title, value, color="#FFFFFF"):
        container = QVBoxLayout()
        container.setSpacing(2)
        t_lbl = CaptionLabel(title, self)
        t_lbl.setStyleSheet("color: #888; font-family: 'Consolas'; font-size: 10px;")
        v_lbl = TitleLabel(value, self)
        v_lbl.setStyleSheet(f"color: {color}; font-family: 'Consolas'; font-size: 24px;")
        container.addWidget(t_lbl)
        container.addWidget(v_lbl)
        return container, v_lbl

    def update_stats(self, processed, ntus, rate):
        self.total_processed_label[1].setText(str(processed))
        self.ntus_found_label[1].setText(str(ntus))
        self.novelty_rate_label[1].setText(f"{rate:.1f}%")

class NTUCard(CardWidget):
    """
    @Bio-Taxon: NTU Satellite Cluster Card.
    Represents an aggregated group of Non-Reference Taxa.
    """
    view_manifold_signal = Signal(dict)

    def __init__(self, ntu_data, parent=None):
        super().__init__(parent)
        self.ntu_data = ntu_data
        self.setFixedSize(360, 240) # Standard Card Size
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(6)
        
        # 1. Header: EXPEDIA-NTU ID
        ntu_id = ntu_data.get("ntu_id", "UNKNOWN-NTU")
        # Ensure format consistency
        if "EXPEDIA-NTU" not in ntu_id:
             ntu_id = f"EXPEDIA-NTU-{ntu_id}"
             
        self.id_label = SubtitleLabel(ntu_id, self)
        self.id_label.setStyleSheet(f"color: {app_config.THEME_COLORS['accent']}; font-family: 'Consolas'; font-weight: bold; font-size: 13px;")
        layout.addWidget(self.id_label)
        
        # 2. Anchor Taxon
        anchor = ntu_data.get("anchor_taxon", "Unresolved").upper()
        # "PHYLOGENETIC ANCHOR: [FAMILY]"
        self.anchor_label = CaptionLabel(f"PHYLOGENETIC ANCHOR: {anchor}", self)
        self.anchor_label.setStyleSheet("color: #CCCCCC; font-family: 'Segoe UI'; font-weight: 600; font-size: 10px; letter-spacing: 0.5px;")
        layout.addWidget(self.anchor_label)
        
        layout.addSpacing(4)
        
        # 3. Metrics Grid
        metrics_container = QWidget()
        metrics_layout = QGridLayout(metrics_container)
        metrics_layout.setContentsMargins(0,0,0,0)
        metrics_layout.setVerticalSpacing(4)
        
        # POPULATION
        pop_size = ntu_data.get("size", 0)
        metrics_layout.addWidget(CaptionLabel("POPULATION", self), 0, 0)
        pop_val = TitleLabel(f"{pop_size}", self)
        pop_val.setStyleSheet("font-size: 18px;")
        metrics_layout.addWidget(pop_val, 1, 0)
        
        # GENOMIC DIVERGENCE
        divergence = float(ntu_data.get("divergence", 0.0))
        metrics_layout.addWidget(CaptionLabel("DIVERGENCE", self), 0, 1)
        # Convert variance to Percentage Divergence (0.05 -> 5.0%)
        div_val = TitleLabel(f"{divergence*100:.1f}%", self) 
        # Color code variance
        var_color = "#00FF00" # Green (Tight)
        if divergence > 0.1: var_color = app_config.THEME_COLORS['accent'] # Yellow/Cyan
        if divergence > 0.25: var_color = "#FF4444" # Red (Loose)
        
        div_val.setStyleSheet(f"font-size: 18px; color: {var_color};")
        metrics_layout.addWidget(div_val, 1, 1)
        
        layout.addWidget(metrics_container)
        
        layout.addStretch()
        
        # 4. Action Button
        self.btn_explore = PrimaryPushButton(FIF.GLOBE, "VIEW TOPOLOGY", self)
        
        # Check for vector availability
        self.centroid_vector = ntu_data.get("centroid_vector")
        
        if self.centroid_vector is None:
             self.btn_explore.setText("VECTOR UNAVAILABLE")
             self.btn_explore.setEnabled(False)
             
        self.btn_explore.clicked.connect(self._on_explore)
        layout.addWidget(self.btn_explore)

    def _on_explore(self):
        """Prepare data payload for Manifold View."""
        # Payload for Localized Manifold View
        payload = {
            "id": self.ntu_data.get("centroid_id") or self.ntu_data.get("ntu_id"),
            "vector": self.ntu_data.get("centroid_vector"), 
            "context": "ntu_exploration"
        }
        self.view_manifold_signal.emit(payload)

class DiscoveryView(QWidget):
    """
    @WinUI-Fluent: Novelty Discovery Dashboard.
    Manages the grid of identified NTUs.
    """
    request_cluster_view = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DiscoveryView")
        
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        
        # 1. Session Summary Panel
        self.summary_panel = SessionSummaryPanel(self)
        self.main_layout.addWidget(self.summary_panel)
        
        # 2. Scroll Area for Grid
        self.scroll_area = QScrollArea(self)
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("background-color: transparent; border: none;")
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        
        self.grid_container = QWidget()
        self.grid_layout = QGridLayout(self.grid_container)
        self.grid_layout.setContentsMargins(30, 30, 30, 30)
        self.grid_layout.setSpacing(20)
        self.grid_layout.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        
        self.scroll_area.setWidget(self.grid_container)
        self.main_layout.addWidget(self.scroll_area)
        
        # Empty State
        self.empty_label = SubtitleLabel("NO NOVEL TAXONOMIC UNITS (NTUS) IDENTIFIED YET.", self.grid_container)
        self.empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.grid_layout.addWidget(self.empty_label, 0, 0)

        # State Tracking
        self.ntu_cards = []

    def populate_ntus(self, ntu_list: list, isolated_taxa: list = None):
        """
        Clears grid and repopulates with NTU cards.
        """
        isolated_taxa = isolated_taxa or []

        # Clear existing
        for i in reversed(range(self.grid_layout.count())): 
            item = self.grid_layout.itemAt(i)
            if item and item.widget():
                widget = item.widget()
                widget.setParent(None)
        
        self.ntu_cards = []

        is_isolated_mode = False
        display_list = []
        
        if ntu_list:
            display_list = ntu_list
        elif isolated_taxa:
            is_isolated_mode = True
            # Convert Isolated Taxa to Pseudo-Clusters
            for taxon in isolated_taxa:
                # Robust Safe-Guarding for undefined keys
                display_list.append({
                    "ntu_id": taxon.get("id", "UNKNOWN-ISOLATE"),
                    "anchor_taxon": taxon.get("classification") or "Incertae sedis",
                    "lineage": taxon.get("lineage", "Lineage Unresolved"),
                    "size": 1,
                    "divergence": 1.0, 
                    # Ensure compatible vector access
                    "centroid_vector": taxon.get('vector', taxon.get('embedding', None)),
                    "centroid_id": taxon.get("id", "UNKNOWN")
                })
            
            # Show InfoBar Warning
            InfoBar.warning(
                title="NO STABLE BIOLOGICAL CLUSTERS FOUND",
                content=f"NO DENSE CLUSTERS DETECTED. ANALYZING {len(isolated_taxa)} ISOLATED NON-REFERENCE TAXA",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP_RIGHT,
                duration=8000,
                parent=self
            )
        else:
            # Empty
            self.grid_layout.addWidget(self.empty_label, 0, 0)
            self.empty_label.show()
            return

        self.empty_label.hide()
        
        # Grid Logic (Responsive-ish: 3 columns)
        cols = 3
        for idx, ntu_data in enumerate(display_list):
            card = NTUCard(ntu_data, self.grid_container)
            card.view_manifold_signal.connect(self.request_cluster_view.emit)
            
            # Disable Manifold Jump if no vector available
            # Check for centroid_vector or centroid
            has_vector = ntu_data.get("centroid_vector") is not None or ntu_data.get("centroid") is not None
            
            if not has_vector:
                if hasattr(card, 'btn_explore'):
                     card.btn_explore.setText("VECTOR UNAVAILABLE")
                     card.btn_explore.setEnabled(False)
                elif hasattr(card, 'btn_view'):
                     card.btn_view.setText("VECTOR UNAVAILABLE")
                     card.btn_view.setEnabled(False)

            row = idx // cols
            col = idx % cols
            self.grid_layout.addWidget(card, row, col)
            self.ntu_cards.append(card)

    def update_session_stats(self, processed_count, ntu_count):
        rate = (ntu_count / processed_count * 100) if processed_count > 0 else 0.0
        self.summary_panel.update_stats(processed_count, ntu_count, rate)

    def export_data(self, file_path: str):
        """Exports the current discovery set to CSV."""
        if not self.ntu_cards:
            return False
            
        import csv
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Header
                writer.writerow(["NTU_ID", "ANCHOR_TAXON", "LINEAGE", "SIZE", "DIVERGENCE", "CENTROID_ID"])
                
                for card in self.ntu_cards:
                    data = card.ntu_data
                    writer.writerow([
                        data.get("ntu_id", ""),
                        data.get("anchor_taxon", ""),
                        data.get("lineage", ""),
                        data.get("size", 0),
                        data.get("divergence", 0.0),
                        # Handles both real clusters and pseudo-clusters
                        data.get("centroid_id", data.get("ntu_id", ""))
                    ])
            return True
        except Exception as e:
            logger.error(f"Failed to export: {e}")
            return False
