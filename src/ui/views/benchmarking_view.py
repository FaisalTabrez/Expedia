import logging
import shutil
import time
import psutil
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFrame, QLabel
)
from PySide6.QtWebEngineWidgets import QWebEngineView
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
            "TRC FUNDING REQUIREMENT: 2TB NVMe SSD for Whole-Genome Metagenomic Surveillance.", 
            self
        )
        funding_lbl.setStyleSheet(f"color: {app_config.THEME_COLORS['accent']}; font-weight: bold; margin-top: 5px;")
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
        self.main_layout.addWidget(TitleLabel("SYSTEM BENCHMARKING & SCALABILITY", self))

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
        
        # IOPS Label
        self.iops_label = TitleLabel("0.0 MB/s", self)
        self.main_layout.addWidget(self.iops_label)
        
        # Fixing psutil section
        # IOPS Timer
        self.io_timer = QTimer(self)
        self.io_timer.timeout.connect(self.update_iops)
        self.io_timer.start(1000)
        self.last_io = psutil.disk_io_counters()

        # Render Charts
        self.render_latency_chart()
        self.render_horizon_chart()

    def update_iops(self):
        try:
            current_io = psutil.disk_io_counters()
            # Calculate diff
            if current_io and self.last_io:
                read_bytes = current_io.read_bytes - self.last_io.read_bytes
                mb_s = read_bytes / (1024 * 1024)
                
                self.iops_label.setText(f"{mb_s:.1f} MB/s")
                self.last_io = current_io
            else:
                 self.iops_label.setText("N/A")
        except Exception:
            self.iops_label.setText("N/A")

    def _get_common_layout(self, title):
        return dict(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#A0A0A0'),
            title=dict(text=title, x=0.5, xanchor='center'),
            margin=dict(l=40, r=40, t=50, b=40),
            xaxis=dict(showgrid=False, zeroline=False),
            yaxis=dict(showgrid=True, gridcolor='#333', zeroline=False),
        )

    def render_latency_chart(self):
        fig = go.Figure(data=[
            go.Bar(
                name='Inference Time',
                x=['BLAST (Sequence Alignment)', 'EXPEDIA (Vector Search)'],
                y=[300, 0.01], # seconds (5 mins vs 10ms)
                marker_color=['#555555', app_config.THEME_COLORS['primary']],
                text=['300s', '0.01s'],
                textposition='auto',
            )
        ])
        
        layout = self._get_common_layout("QUERY LATENCY (Log Scale)")
        layout['yaxis']['type'] = 'log'
        layout['yaxis']['title'] = 'Seconds'
        fig.update_layout(layout)
        
        html = fig.to_html(include_plotlyjs='cdn', full_html=True)
        self.chart_latency.setHtml(html)

    def render_horizon_chart(self):
        fig = go.Figure(data=[
            go.Bar(
                name='Capacity',
                x=['EXPEDIA (Current)', 'EXPEDIA ARRAY (Goal)'],
                y=[100000, 4200000],
                marker_color=[app_config.THEME_COLORS['accent'], '#2ecc71'], # Pink vs Green
                text=['100k Vectors', '4.2M Vectors'],
                textposition='auto',
            )
        ])
        
        layout = self._get_common_layout("DATABASE SCALABILITY HORIZON")
        layout['yaxis']['title'] = 'Vector Capacity'
        fig.update_layout(layout)
        
        html = fig.to_html(include_plotlyjs='cdn', full_html=True)
        self.chart_horizon.setHtml(html)
