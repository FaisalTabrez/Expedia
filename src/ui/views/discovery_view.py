import logging
import numpy as np
import pandas as pd
import plotly.express as px
import tempfile
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
        self.card_novelty = self._create_kpi_card("NRGS RATIO (%)", "0.0%", FIF.QUESTION)
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
        # Recalculate based on current NTU list + Isolated items
        # Usually 'results' here is the list of isolated items? Or full list? 
        # The user said: "S should be the sum of len(ntu_clusters) + len(isolated_taxa)"
        # But this function only receives 'results'.
        # We need to know about NTU clusters separately?
        # Let's assume 'results' contains everything flattened or we fix calling side.
        # Ideally, passed in populate_ntus. But let's look at populate_ntus logic.
        
        # Actually populate_ntus calls update_dashboard with `isolated_taxa`.
        # This is wrong if we want full community stats.
        # But wait, populate_ntus gets `ntu_list` AND `isolated_taxa`.
        pass

    def update_metrics(self, ntu_list, isolated_list):
        try:
            # 1. Species Richness (S)
            S = len(ntu_list) + len(isolated_list)
            
            # 2. Shannon Index (H')
            # N = Total Individuals
            # For NTU: size = ntu['size']
            # For Isolated: size = 1
            
            total_individuals = 0
            counts = []
            
            for ntu in ntu_list:
                s = ntu.get('size', 1)
                counts.append(s)
                total_individuals += s
                
            for iso in isolated_list:
                counts.append(1)
                total_individuals += 1
                
            if total_individuals > 0:
                props = np.array(counts) / total_individuals
                # Filter zeros
                props = props[props > 0]
                H = -np.sum(props * np.log(props))
            else:
                H = 0.0

            # 3. Novelty Ratio
            # We assume ALL in this view are Novel? No.
            # ntu_list are Novel/Divergent clusters.
            # isolated_list might be mixed.
            # Let's count novel items.
            novel_cnt = len(ntu_list) # Clusters are inherently novel-ish in this view?
            # Check isolated status?
            # Assuming typical workflow where Discovery View shows Novel things.
            # But let's be safe.
            for iso in isolated_list:
                if 'Novel' in iso.get('status', 'Unknown'):
                    novel_cnt += 1
                    
            ratio = (novel_cnt / (len(ntu_list) + len(isolated_list)) * 100) if (ntu_list or isolated_list) else 0.0

            # UI Update
            self.card_richness.value_label.setText(str(S))
            self.card_novelty.value_label.setText(f"{ratio:.1f}%")
            self.card_diversity.value_label.setText(f"{H:.2f}")
            
            # Sunburst Data
            # Combine for chart
            combined_data = []
            for ntu in ntu_list:
                combined_data.append({
                    'lineage': ntu.get('lineage', ''),
                    'classification': ntu.get('anchor_taxon', 'Unknown Cluster'),
                    'count': ntu.get('size', 1)
                })
            for iso in isolated_list:
                combined_data.append({
                    'lineage': iso.get('lineage', ''),
                    'classification': iso.get('classification', 'Isolated'),
                    'count': 1
                })
            
            if WEB_ENGINE_AVAILABLE:
                self._render_sunburst_from_list(combined_data)

        except Exception as e:
            logger.error(f"Metrics Update Failed: {e}", exc_info=True)

    def _render_sunburst_from_list(self, data_list):
        if not data_list: return
        df = pd.DataFrame(data_list)
        # Reuse existing logic but adapted
        # ... (Implementation of aggregation similar to existing but using 'count' column)
        
        # Quick aggregation:
        rows = []
        for _, row in df.iterrows():
            lineage = str(row.get('lineage', ''))
            parts = [p.strip() for p in lineage.split('>')] # Changed from ; to >
            # Default ranks
            phylum = "Unclassified"
            cls = "Unclassified"
            order = "Unclassified"
            
            # Very naive mapping, robust parsing needed?
            # Assuming standard lineage string order: K > P > C > O ...
            if len(parts) > 1: phylum = parts[1]
            if len(parts) > 2: cls = parts[2]
            if len(parts) > 3: order = parts[3]
            
            rows.append({
                'Phylum': phylum, 'Class': cls, 'Order': order, 
                'Identity': row['classification'], 'Count': row['count']
            })
            
        hdf = pd.DataFrame(rows)
        # Use existing render logic
        self._render_sunburst_figure(hdf)

    def _render_sunburst_figure(self, hierarchy_df):
        try:
            fig = px.sunburst(
                hierarchy_df,
                path=['Phylum', 'Class', 'Order', 'Identity'],
                values='Count',
                color='Count', 
                color_continuous_scale='Viridis',
                title='<b>COMMUNITY COMPOSITION</b>'
            )
            
            bg_color = app_config.THEME_COLORS['background']
            text_color = "#E0E0E0"
            
            fig.update_layout(
                 paper_bgcolor=bg_color,
                 plot_bgcolor=bg_color,
                 font=dict(color=text_color, family="Segoe UI"),
                 margin=dict(t=40, l=10, r=10, b=10),
                 coloraxis_showscale=False
            )
            
            # JS Retry Wrapper
            plot_html = fig.to_html(include_plotlyjs='cdn', full_html=False)
            
            # We construct a full HTML page with a retry mechanism
            full_html = f"""
            <html>
            <head>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                <style>body {{ background-color: {bg_color}; margin: 0; overflow: hidden; }}</style>
            </head>
            <body>
                <div id="chart" style="width:100%; height:100%;"></div>
                <script>
                    var plotData = {fig.to_json()};
                    
                    function drawChart() {{
                        if (typeof Plotly === 'undefined') {{
                            console.log("Waiting for Plotly...");
                            setTimeout(drawChart, 100);
                            return;
                        }}
                        Plotly.newPlot('chart', plotData.data, plotData.layout, {{responsive: true}});
                    }}
                    
                    drawChart();
                </script>
            </body>
            </html>
            """
            
            # Write temp
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                 f.write(full_html)
                 temp_path = f.name
             
            self.web_view.load(QUrl.fromLocalFile(temp_path))

        except Exception as e:
            logger.error(f"Sunburst Error: {e}")

    # Legacy method redirect
    def update_dashboard(self, results):
        self.update_metrics([], results)

    def _aggregate_community_data(self, df):
        """
        Groups session results by Phylum > Class > Order.
        """
        rows = []
        
        for _, row in df.iterrows():
            # Parse lineage string: e.g., "p__Proteobacteria;c__Gammaproteobacteria;..."
            lineage_str = str(row.get('lineage', ''))
            parts = [p.strip() for p in lineage_str.split(';')]
            
            phylum = "Unclassified"
            cls = "Unclassified"
            order = "Unclassified"
            identity = row.get('classification', 'Unknown Organism')
            
            # Simple heuristic parsing
            for p in parts:
                if p.startswith('p__'): phylum = p[3:] or "Unclassified"
                elif p.startswith('c__'): cls = p[3:] or "Unclassified"
                elif p.startswith('o__'): order = p[3:] or "Unclassified"
            
            # Override for Novel/Unknown
            status = row.get('status', 'Known')
            if 'Novel' in status:
                phylum = "Novel Biological Entities"
                # Use NTU ID if available, else generic
                # If grouped by NTU, we might want that here.
                # For now, keep it simple.
                
            rows.append({
                'Phylum': phylum,
                'Class': cls,
                'Order': order,
                'Identity': identity,
                'Count': 1
            })
            
        return pd.DataFrame(rows)


class NTUCard(CardWidget):
    """
    @Bio-Taxon: NRGS Taxonomic Cluster Card.
    Represents an aggregated group of Non-Reference Genomic Signatures.
    """
    view_manifold_signal = Signal(dict)

    def __init__(self, ntu_data, parent=None):
        super().__init__(parent)
        # Apply Sharp/Solid Style
        self.setStyleSheet(f"""
            NTUCard {{
                background-color: {app_config.THEME_COLORS['sidebar']};
                border: 1px solid {app_config.THEME_COLORS['border']};
                border-radius: 0px;
            }}
        """)
        
        self.ntu_data = ntu_data
        self.setFixedSize(360, 240) # Standard Card Size
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(6)
        
        # 1. Header: EXPEDIA-NRGS ID
        ntu_id = ntu_data.get("ntu_id", "UNKNOWN-NRGS")
        if "EXPEDIA-NRGS" not in ntu_id:
             ntu_id = f"EXPEDIA-NRGS-{ntu_id}"
             
        self.id_label = SubtitleLabel(ntu_id, self)
        self.id_label.setStyleSheet(f"color: {app_config.THEME_COLORS['accent']}; font-family: 'Consolas'; font-weight: bold; font-size: 13px;")
        layout.addWidget(self.id_label)
        
        # 2. Anchor Taxon & Contamination Check
        anchor = ntu_data.get("anchor_taxon", "Unresolved")
        is_contaminated = ntu_data.get("contamination_warning", False)
        
        if is_contaminated:
            anchor_text = f"⚠ CONTAMINATION RISK: {anchor.upper()}"
            anchor_color = "#FF4444"
        else:
            anchor_text = f"PHYLOGENETIC ANCHOR: {anchor.upper()}"
            anchor_color = "#CCCCCC"
            
        self.anchor_label = CaptionLabel(anchor_text, self)
        self.anchor_label.setStyleSheet(f"color: {anchor_color}; font-family: 'Segoe UI'; font-weight: 600; font-size: 10px; letter-spacing: 0.5px;")
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

        # LINEAGE RELIABILITY GAUGE
        # Row 2: Header
        metrics_layout.addWidget(CaptionLabel("LINEAGE CONSENSUS", self), 2, 0, 1, 2)
        
        # Row 3: Progress Bar
        mean_conf = ntu_data.get("mean_confidence", 0.0) * 100
        self.reliability_bar = ProgressBar(self)
        self.reliability_bar.setRange(0, 100)
        self.reliability_bar.setValue(int(mean_conf))
        self.reliability_bar.setFixedHeight(6)
        
        # Dynamic color based on reliability
        color = "#00FF00" 
        if mean_conf < 70: color = "#FFAA00"
        if mean_conf < 40: color = "#FF4444"
        
        # Custom style for progress bar
        self.reliability_bar.setStyleSheet(f"""
            QProgressBar::chunk {{
                background-color: {color};
                border-radius: 3px;
            }}
            QProgressBar {{
                background-color: #222;
                border: none;
                border-radius: 3px;
            }}
        """)
        
        metrics_layout.addWidget(self.reliability_bar, 3, 0, 1, 2)
        
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
             # Calculate metrics for both Clusters and Isolated items
             self.summary_panel.update_metrics(ntu_list, isolated_taxa)
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
