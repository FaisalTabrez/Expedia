import logging
import numpy as np
import pandas as pd
import plotly.express as px
import tempfile
import math
from PySide6.QtCore import Qt, QUrl, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, 
    QLabel, QScrollArea, QFrame, QSizePolicy
)
# WebEngine Import with Fallback
try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    WEB_ENGINE_AVAILABLE = True
except ImportError:
    WEB_ENGINE_AVAILABLE = False
    class QWebEngineView(QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)
            QVBoxLayout(self).addWidget(QLabel("Visualization Unavailable (Missing QtWebEngine)", self))
        def load(self, url): pass
        def setHtml(self, html, baseUrl=None): pass

from qfluentwidgets import (
    TitleLabel, SubtitleLabel, CaptionLabel,
    CardWidget, PrimaryPushButton, FluentIcon as FIF,
    ProgressBar, InfoBar, InfoBarPosition
)
from ...config import app_config

logger = logging.getLogger("DeepBioScan.DiscoveryView")

class SessionSummaryPanel(QWidget):
    """
    @Bio-Taxon: Comprehensive Community Analysis.
    Integrates Sunburst visualization and Ecological KPIs.
    Replaces legacy summary panel.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(450) # Compact height
        
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 15)
        main_layout.setSpacing(20)
        
        # LEFT COLUMN: KPI Cards & Metrics
        left_col = QVBoxLayout()
        left_col.setContentsMargins(0,0,0,0)
        left_col.setSpacing(10)
        
        # Header
        left_col.addWidget(SubtitleLabel("ECOLOGICAL METRICS", self))
        
        # 1. Species Richness (S)
        self.card_richness = self._create_kpi_card("SPECIES RICHNESS (S)", "0", FIF.PEOPLE)
        left_col.addWidget(self.card_richness)
        
        # 2. Novelty Ratio
        self.card_novelty = self._create_kpi_card("NOVELTY RATIO", "0.0%", FIF.QUESTION)
        left_col.addWidget(self.card_novelty)
        
        # 3. Shannon Index
        self.card_diversity = self._create_kpi_card("SHANNON INDEX (H')", "0.00", FIF.IOT)
        left_col.addWidget(self.card_diversity)
        
        left_col.addStretch()
        
        container_left = QWidget()
        container_left.setLayout(left_col)
        container_left.setFixedWidth(280) 
        main_layout.addWidget(container_left)
        
        # RIGHT COLUMN: Sunburst Chart
        self.chart_container = QFrame()
        self.chart_container.setStyleSheet(f"background-color: {app_config.THEME_COLORS['background']}; border: 1px solid #333; border-radius: 8px;")
        chart_layout = QVBoxLayout(self.chart_container)
        chart_layout.setContentsMargins(0,0,0,0)
        
        if WEB_ENGINE_AVAILABLE:
            self.web_view = QWebEngineView(self)
            self.web_view.setStyleSheet("background-color: transparent;")
            chart_layout.addWidget(self.web_view)
        else:
            lbl = SubtitleLabel("Visualization Unavailable (QtWebEngine Missing)", self)
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            chart_layout.addWidget(lbl)
            
        main_layout.addWidget(self.chart_container)

    def _create_kpi_card(self, title, value, icon):
        card = CardWidget(self)
        card.setFixedHeight(90)
        
        layout = QHBoxLayout(card)
        layout.setContentsMargins(20, 10, 20, 10)
        
        # Icon
        icon_widget = getattr(icon, 'icon', lambda c: icon)(color=QColor(app_config.THEME_COLORS['primary']))
        if hasattr(icon, 'icon'): 
             icon_widget = icon.icon(color=QColor(app_config.THEME_COLORS['primary']))
        
        icon_lbl = QLabel()
        if hasattr(icon_widget, 'pixmap'):
             icon_lbl.setPixmap(icon_widget.pixmap(32, 32))
        else:
             icon_lbl.setText("M")
             
        layout.addWidget(icon_lbl)
        layout.addSpacing(15)
        
        # Text
        text_layout = QVBoxLayout()
        text_layout.setSpacing(2)
        
        title_lbl = CaptionLabel(title, card)
        title_lbl.setStyleSheet("color: #888;")
        
        val_lbl = TitleLabel(value, card)
        val_lbl.setStyleSheet(f"font-size: 22px; color: {app_config.THEME_COLORS['foreground']}; font-weight: bold;")
        
        text_layout.addWidget(title_lbl)
        text_layout.addWidget(val_lbl)
        layout.addLayout(text_layout)
        
        # Store ref
        card.value_label = val_lbl
        return card

    def update_stats(self, processed, ntus, rate):
        # Legacy adapter
        pass

    def update_dashboard(self, results):
        """
        Calculates ecological metrics and renders Sunburst.
        """
        if not results: return
        
        try:
            # 1. Convert to DataFrame
            df = pd.DataFrame(results)
            
            # --- METRICS ---
            total = len(df)
            
            # S: Unique Classifications
            if 'classification' in df.columns:
                valid_taxa = df[df['classification'] != 'Unknown']['classification']
                S = valid_taxa.nunique()
                
                # H': Shannon Entropy
                counts = valid_taxa.value_counts()
                props = counts / total
                H = -np.sum(props * np.log(props + 1e-9))
            else:
                S = 0
                H = 0.0

            # Novelty Ratio
            if 'status' in df.columns:
                novel = len(df[df['status'] == 'Novel'])
                ratio = (novel / total * 100) if total > 0 else 0.0
            else:
                novel = 0
                ratio = 0.0
            
            # Update UI
            self.card_richness.value_label.setText(str(S))
            self.card_novelty.value_label.setText(f"{ratio:.1f}%")
            self.card_diversity.value_label.setText(f"{H:.2f}")

            # --- VISUALIZATION (Sunburst) ---
            if WEB_ENGINE_AVAILABLE:
                self._render_sunburst(df)

        except Exception as e:
            logger.error(f"Dashboard Update Failed: {e}")

    def _render_sunburst(self, df):
        # Build Hierarchy
        def parse_lineage(row):
            lin = str(row.get('lineage', ''))
            parts = lin.split(';')
            
            # Defaults
            phylum = "Unclassified-Phylum"
            cls = "Unclassified-Class"
            order = "Unclassified-Order"
            identity = row.get('classification', 'Unknown')
            
            # Parse standard greengenes/silva format
            for p in parts:
                p = p.strip()
                if p.startswith('p__'): phylum = p[3:] or phylum
                elif p.startswith('c__'): cls = p[3:] or cls
                elif p.startswith('o__'): order = p[3:] or order
            
            # Override identity for Novel
            if row.get('status') == 'Novel':
                identity = f"NOVEL-{str(row.get('id',''))[:8]}"

            return pd.Series([phylum, cls, order, identity])

        hierarchy = df.apply(parse_lineage, axis=1)
        hierarchy.columns = ['Phylum', 'Class', 'Order', 'Identity']
        hierarchy['Count'] = 1
        
        # "Bioluminescent Abyss" -> Deep Blues to Cyan
        fig = px.sunburst(
            hierarchy,
            path=['Phylum', 'Class', 'Order', 'Identity'],
            values='Count',
            color='Count', 
            color_continuous_scale='Teal',
            title='<b>COMMUNITY COMPOSITION</b>'
        )
        
        fig.update_layout(
             paper_bgcolor=app_config.THEME_COLORS['background'],
             plot_bgcolor=app_config.THEME_COLORS['background'],
             font=dict(color='#A0A0A0', family="Segoe UI"),
             margin=dict(t=30, l=0, r=0, b=0),
             coloraxis_showscale=False
        )
        
        # Render Offline
        try:
             with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                 html = fig.to_html(include_plotlyjs=True, full_html=True)
                 f.write(html)
                 temp_path = f.name
             
             self.web_view.load(QUrl.fromLocalFile(temp_path))
        except Exception as e:
            logger.error(f"Sunburst Render Failed: {e}")

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
        try:
             divergence = float(ntu_data.get("divergence", 0.0))
        except (ValueError, TypeError):
             divergence = 0.0

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
        
        # Update Community Dashboard (SessionSummaryPanel)
        try:
             # isolated_taxa contains the full 'results' list from ScienceKernel
             if isolated_taxa:
                 self.summary_panel.update_dashboard(isolated_taxa)
        except AttributeError:
             pass # In case panel is missing or methods differ

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
            
            # Limit isolated display to prevent UI freeze
            # If we have 10,000 results, we don't want 10,000 cards.
            # Show "All Novel" + "Sample of Known" or just "All Novel"?
            # The input 'isolated_taxa' is 'results' (Mixed).
            # Let's filter for Novel first.
            
            novel_subset = [t for t in isolated_taxa if t.get("status") == "Novel"]
            
            # If no novel items either, take top 50 of whatever is there (Known) 
            target_list = novel_subset if novel_subset else isolated_taxa[:50]
            
            if len(target_list) > 100:
                 # Hard cap for performance
                 target_list = target_list[:100]
            
            # Convert Isolated Taxa to Pseudo-Clusters
            for taxon in target_list:
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
