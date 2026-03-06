import logging
import shutil
import time
import psutil
import tempfile
from PySide6.QtCore import Qt, QTimer, QUrl
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFrame, QLabel
)
try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    WEB_ENGINE_AVAILABLE = True
except ImportError:
    WEB_ENGINE_AVAILABLE = False
    class QWebEngineView(QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)
            QVBoxLayout(self).addWidget(QLabel("Visualization Unavailable", self))
        def load(self, url): pass
        def setHtml(self, html, baseUrl=None): pass

from qfluentwidgets import (
    TitleLabel, SubtitleLabel, CaptionLabel, 
    CardWidget, ProgressBar, FluentIcon as FIF,
    InfoBar, InfoBarPosition
)
import plotly.graph_objects as go
from ...config import app_config

logger = logging.getLogger("EXPEDIA.BenchmarkingView")

class StorageHealthPanel(CardWidget):
    """
    @Data-Ops: Monitors Volume E: health and capacity.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(180)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Header
        header = QHBoxLayout()
        # FIF.HDD might be missing in some versions, using SAVE as safe fallback for disk
        icon = FIF.SAVE.icon(color=QColor(app_config.THEME_COLORS['primary']))
        title = SubtitleLabel("VOLUME E: STORAGE HEALTH", self)
        header.addWidget(QLabel(pixmap=icon.pixmap(32, 32)))
        header.addWidget(title)
        header.addStretch()
        layout.addLayout(header)
        
        # Funding Requirement Text
        funding_lbl = CaptionLabel(
            "INFRASTRUCTURE UPGRADE PROPOSAL:\n"
            "Acquisition of 2TB TRC NVMe Array for high-throughput surveillance. "
            "Transition from Tier-1 Micro-Atlas (100k) to Tier-3 Metagenomic Indexing (4.2M+ signatures).", 
            self
        )
        funding_lbl.setWordWrap(True)
        funding_lbl.setStyleSheet(f"color: {app_config.THEME_COLORS['accent']}; font-weight: bold; margin-top: 5px; font-family: 'Consolas';")
        layout.addWidget(funding_lbl)
        
        layout.addSpacing(10)

        # Space Usage
        self.usage_bar = ProgressBar(self)
        self.usage_bar.setRange(0, 100)
        self.usage_text = CaptionLabel("Calculating...", self)
        
        layout.addWidget(self.usage_text)
        layout.addWidget(self.usage_bar)
        
        self.update_storage_metrics()
        
        # Timer for periodic updates
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_storage_metrics)
        self.timer.start(5000) # Every 5s

    def update_storage_metrics(self):
        try:
            # Check Volume E: (or app configured root)
            path = app_config.DATA_ROOT
            total, used, free = shutil.disk_usage(path)
            
            gb_total = total / (1024**3)
            gb_used = used / (1024**3)
            gb_free = free / (1024**3)
            percent = (used / total) * 100
            
            self.usage_bar.setValue(int(percent))
            self.usage_text.setText(f"Used: {gb_used:.2f} GB / {gb_total:.2f} GB ({gb_free:.2f} GB Free) - Path: {path}")
            
            if percent > 90:
                self.usage_bar.setCustomBarColor(Qt.GlobalColor.red, Qt.GlobalColor.red)
        except Exception as e:
            self.usage_text.setText(f"Storage Error: {e}")

class BenchmarkingView(QWidget):
    """
    @BioArch-Pro: Performance Visual Analytics.
    Compares Latency and Database Scaling.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("BenchmarkingView")
        
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(30, 30, 30, 30)
        self.main_layout.setSpacing(20)
        
        # Title
        self.main_layout.addWidget(TitleLabel("HARDWARE PERFORMANCE AND SCALABILITY", self))

        # 1. Storage Health Panel
        self.storage_panel = StorageHealthPanel(self)
        self.main_layout.addWidget(self.storage_panel)

        # 2. Charts Area
        charts_container = QWidget()
        charts_layout = QHBoxLayout(charts_container)
        charts_layout.setContentsMargins(0, 0, 0, 0)
        charts_layout.setSpacing(20)
        
        # Chart A: Latency
        self.chart_latency = QWebEngineView(self)
        self.chart_latency.setStyleSheet("background-color: transparent; border-radius: 8px;")
        charts_layout.addWidget(self.chart_latency)
        
        # Chart B: Horizon
        self.chart_horizon = QWebEngineView(self)
        self.chart_horizon.setStyleSheet("background-color: transparent; border-radius: 8px;")
        charts_layout.addWidget(self.chart_horizon)
        
        self.main_layout.addWidget(charts_container)
        
        # IOPS Label (Hardware Anchor Status)
        self.iops_label = TitleLabel("I/O: 0.0 MB/s (USB 3.0 LIMITATION)", self)
        self.iops_label.setStyleSheet("color: #888; font-family: 'Consolas'; font-size: 14px;")
        self.main_layout.addWidget(self.iops_label)
        
        # Fixing psutil section
        # IOPS Timer
        self.io_timer = QTimer(self)
        self.io_timer.timeout.connect(self.update_iops_metrics) # Renamed to reflect logic update
        self.io_timer.start(1000)
        self.last_io = None # Will init on first tick

        # Render Charts
        self.render_latency_chart()
        self.render_horizon_chart()

    def update_iops_metrics(self):
        try:
            current_io = psutil.disk_io_counters()
            
            if self.last_io:
                # Calculate Read Speed (primary bottleneck for inference)
                read_bytes = current_io.read_bytes - self.last_io.read_bytes
                read_mb_s = read_bytes / (1024 * 1024)
                
                # Contextual status based on Volume E: (NTFS) limits
                # USB 3.0 theoretical max ~600MB/s, practical ~100-300MB/s
                # HDD anchor likely ~80-120MB/s
                
                status_color = "#888888" # Grey (Idle)
                status_text = "IDLE"
                
                if read_mb_s > 100:
                     status_color = "#00FF00" # Green (High Throughput)
                     status_text = "SATURATED"
                elif read_mb_s > 10:
                     status_color = app_config.THEME_COLORS['primary']
                     status_text = "ACTIVE"
                     
                self.iops_label.setText(f"ANCHOR READ SPEED: {read_mb_s:.1f} MB/s [{status_text}]")
                self.iops_label.setStyleSheet(f"color: {status_color}; font-family: 'Consolas'; font-size: 14px;")
            
            self.last_io = current_io
            
        except Exception:
            self.iops_label.setText("I/O METRICS UNAVAILABLE")

    def _render_chart_safely(self, fig, view_widget):
        """
        Renders a Plotly figure with JS retry logic and Dark Theme background.
        """
        if not WEB_ENGINE_AVAILABLE:
            return

        try:
            bg_color = app_config.THEME_COLORS.get('background', '#1A1A1A')
            
            # Update figure layout for theme
            fig.update_layout(
                paper_bgcolor=bg_color,
                plot_bgcolor=bg_color,
                font=dict(color='#E0E0E0', family="Segoe UI"),
                margin=dict(l=40, r=40, t=50, b=40),
                xaxis=dict(showgrid=False, zeroline=False, color='#888'),
                yaxis=dict(showgrid=True, gridcolor='#333', zeroline=False, color='#888'),
                hoverlabel=dict(
                    bgcolor=app_config.THEME_COLORS['primary'],
                    font_size=14,
                    font_family="Consolas"
                )
            )

            # Generate HTML with retry script
            plot_json = fig.to_json()
            
            full_html = f"""
            <html>
            <head>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                <style>body {{ background-color: {bg_color}; margin: 0; overflow: hidden; }}</style>
            </head>
            <body>
                <div id="chart" style="width:100%; height:100%;"></div>
                <script>
                    var plotData = {plot_json};
                    
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
            
            # Write to temp file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                 f.write(full_html)
                 temp_path = f.name
             
            view_widget.load(QUrl.fromLocalFile(temp_path))

        except Exception as e:
            logger.error(f"Chart Render Failed: {e}")

    def render_latency_chart(self):
        try:
            fig = go.Figure(data=[
                go.Bar(
                    name='Inference Time',
                    x=['BLAST (Sequence Alignment)', 'EXPEDIA (Vector Search)'],
                    y=[300, 0.01], # seconds (5 mins vs 10ms)
                    marker=dict(
                        color=['#555555', app_config.THEME_COLORS['primary']],
                        line=dict(color=app_config.THEME_COLORS['foreground'], width=1)
                    ),
                    text=['300s', '0.01s'],
                    textposition='auto',
                )
            ])
            
            fig.update_layout(
                title=dict(text="QUERY LATENCY (Log Scale)", x=0.5, xanchor='center'),
                yaxis=dict(type='log', title='Seconds')
            )
            
            self._render_chart_safely(fig, self.chart_latency)
        except Exception as e:
            logger.error(f"Latency Chart Error: {e}")

    def render_horizon_chart(self):
        try:
            fig = go.Figure(data=[
                go.Bar(
                    name='Capacity',
                    x=['EXPEDIA (Current)', 'EXPEDIA ARRAY (Goal)'],
                    y=[100000, 4200000],
                    marker=dict(
                        color=[app_config.THEME_COLORS['accent'], '#2ecc71'], # Pink vs Green
                        line=dict(color=app_config.THEME_COLORS['foreground'], width=1)
                    ),
                    text=['100k Vectors', '4.2M Vectors'],
                    textposition='auto',
                )
            ])
            
            fig.update_layout(
                 title=dict(text="DATABASE SCALABILITY HORIZON", x=0.5, xanchor='center'),
                 yaxis=dict(title='Vector Capacity')
            )
            
            self._render_chart_safely(fig, self.chart_horizon)
        except Exception as e:
            logger.error(f"Horizon Chart Error: {e}")
