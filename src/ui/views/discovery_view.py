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
        self.setFixedHeight(80)
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
        t_lbl = CaptionLabel(title, self)
        t_lbl.setStyleSheet("color: #888; font-family: 'Consolas';")
        v_lbl = TitleLabel(value, self)
        v_lbl.setStyleSheet(f"color: {color}; font-family: 'Consolas';")
        container.addWidget(t_lbl)
        container.addWidget(v_lbl)
        return container, v_lbl

    def update_stats(self, processed, ntus, rate):
        self.total_processed_label[1].setText(str(processed))
        self.ntus_found_label[1].setText(str(ntus))
        self.novelty_rate_label[1].setText(f"{rate:.1f}%")

class NTUCard(CardWidget):
    """
    @Bio-Taxon: NTU Discovery Card.
    Visualizes a Novel Taxonomic Unit cluster found by HDBSCAN.
    Includes Divergence Gauge and Manifold Link.
    """
    view_manifold_signal = Signal(dict) # Emits cluster data

    def __init__(self, ntu_data, parent=None):
        super().__init__(parent)
        self.ntu_data = ntu_data
        self.setFixedSize(320, 220)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Header: ID and Anchor
        header = QHBoxLayout()
        id_label = SubtitleLabel(ntu_data.get("ntu_id", "Unknown NTU").upper(), self)
        id_label.setStyleSheet(f"color: {app_config.THEME_COLORS['accent']}; font-weight: bold; font-family: 'Consolas';")
        header.addWidget(id_label)
        header.addStretch()
        layout.addLayout(header)
        
        # Anchor Taxon (Classification)
        anchor = ntu_data.get("anchor_taxon", "Unresolved").upper()
        anchor_lbl = CaptionLabel(f"ANCHOR: {anchor}", self)
        anchor_lbl.setStyleSheet("color: #AAA; font-family: 'Consolas'; font-weight: bold;")
        layout.addWidget(anchor_lbl)
        
        # Lineage Breadcrumb
        lineage = ntu_data.get("lineage", "").replace(" > ", " › ")
        if lineage:
            # Elide if too long
            fm = self.fontMetrics()
            elided = fm.elidedText(lineage, Qt.TextElideMode.ElideRight, 290)
            
            lineage_lbl = CaptionLabel(elided, self)
            lineage_lbl.setToolTip(lineage)
            lineage_lbl.setStyleSheet("color: #888888; font-style: italic;")
            layout.addWidget(lineage_lbl)
        
        # Population Size
        pop_size = ntu_data.get("size", 0)
        pop_lbl = CaptionLabel(f"CLUSTER POPULATION: {pop_size} SEQUENCES", self)
        pop_lbl.setStyleSheet("font-family: 'Consolas';")
        layout.addWidget(pop_lbl)
        
        # Divergence Gauge
        div_layout = QVBoxLayout()
        div_lbl = CaptionLabel("MEAN SEMANTIC DIVERGENCE", self)
        
        self.div_bar = ProgressBar(self)
        self.div_bar.setRange(0, 100)
        
        # Divergence is usually distance. 
        # Assuming ntu_data has 'divergence' (0.0 to 1.0, where 1.0 is far)
        divergence = ntu_data.get("divergence", 0.15) # Default low divergence
        pct = int(divergence * 100)
        self.div_bar.setValue(pct)
        # Custom style for bar color based on divergence severity
        if pct > 20:
             self.div_bar.setCustomBarColor(QColor(app_config.THEME_COLORS['accent']), QColor(app_config.THEME_COLORS['accent']))
        
        div_layout.addWidget(div_lbl)
        div_layout.addWidget(self.div_bar)
        layout.addLayout(div_layout)
        
        layout.addStretch()
        
        # Action Button
        self.btn_view = PrimaryPushButton(FIF.GLOBE, "VIEW CLUSTER MANIFOLD", self)
        self.btn_view.clicked.connect(self._on_view_clicked)
        layout.addWidget(self.btn_view)

    def _on_view_clicked(self):
        self.view_manifold_signal.emit(self.ntu_data)

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

    def populate_ntus(self, ntu_list: list):
        """
        Clears grid and repopulates with NTU cards.
        """
        # Clear existing
        for i in reversed(range(self.grid_layout.count())): 
            item = self.grid_layout.itemAt(i)
            if item and item.widget():
                widget = item.widget()
                widget.setParent(None)
        
        self.ntu_cards = []

        if not ntu_list:
            self.grid_layout.addWidget(self.empty_label, 0, 0)
            self.empty_label.show()
            return

        self.empty_label.hide()
        
        # Grid Logic (Responsive-ish: 3 columns)
        cols = 3
        for idx, ntu in enumerate(ntu_list):
            card = NTUCard(ntu, self.grid_container)
            card.view_manifold_signal.connect(self.request_cluster_view.emit)
            
            row = idx // cols
            col = idx % cols
            self.grid_layout.addWidget(card, row, col)
            self.ntu_cards.append(card)

    def update_session_stats(self, processed_count, ntu_count):
        rate = (ntu_count / processed_count * 100) if processed_count > 0 else 0.0
        self.summary_panel.update_stats(processed_count, ntu_count, rate)
