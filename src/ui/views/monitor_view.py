from PySide6.QtCore import Qt, QSize, Signal
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
    QScrollArea, QFrame, QTextEdit, QSizePolicy
)
from qfluentwidgets import (
    TitleLabel, SubtitleLabel, CaptionLabel,
    PrimaryPushButton, ProgressBar, CardWidget, TransparentPushButton, 
    FluentIcon as FIF, SmoothScrollArea
)
from ...config import app_config

class DropZone(QFrame):
    """
    @WinUI-Fluent: Ingestion Zone.
    Supports file browsing and visual drop indication.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DropZone")
        self.setFixedHeight(120)
        self.setStyleSheet(f"""
            QFrame#DropZone {{
                border: 2px dashed {app_config.THEME_COLORS["primary"]};
                border-radius: 10px;
                background-color: rgba(0, 229, 255, 0.05);
            }}
            QFrame#DropZone:hover {{
                background-color: rgba(0, 229, 255, 0.1);
            }}
        """)
        
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.label = SubtitleLabel("DRAG & DROP FASTA FILES OR BROWSE EDGE FILESYSTEM", self)
        self.label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.browse_btn = PrimaryPushButton(FIF.FOLDER, "SELECT SEQUENCE FILE", self)
        self.browse_btn.setFixedWidth(200)
        
        layout.addWidget(self.label)
        layout.addSpacing(10)
        layout.addWidget(self.browse_btn)

class DiscoveryCard(CardWidget):
    """
    @WinUI-Fluent: Result Card.
    Visualizes genomic signal classification.
    """
    view_topology = Signal(dict)

    def __init__(self, result_data, parent=None):
        super().__init__(parent)
        self.result_data = result_data
        self.setFixedSize(340, 130)  # Compact design
        
        # Colors
        is_novel = result_data.get("status") == "Novel"
        accent_color = app_config.THEME_COLORS["accent"] if is_novel else app_config.THEME_COLORS["primary"]
        
        # Layout
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 10, 0)
        layout.setSpacing(0)
        
        # Accent Bar
        self.accent_bar = QFrame(self)
        self.accent_bar.setFixedWidth(4)
        self.accent_bar.setStyleSheet(f"background-color: {accent_color}; border-top-left-radius: 4px; border-bottom-left-radius: 4px;")
        
        # Content
        content_layout = QVBoxLayout()
        content_layout.setContentsMargins(12, 10, 5, 10)
        content_layout.setSpacing(4)
        
        # Header: ID + Confidence
        header = QHBoxLayout()
        header.setSpacing(10)
        
        id_text = result_data.get("id", "Unknown").upper()
        self.id_label = CaptionLabel(id_text, self)
        self.id_label.setStyleSheet("color: #888; font-weight: bold; font-family: 'Consolas';")
        
        conf_val = result_data.get("confidence", 0.0) * 100
        self.conf_label = CaptionLabel(f"{conf_val:.1f}% CONF", self)
        self.conf_label.setStyleSheet(f"color: {accent_color}; font-family: 'Consolas'; font-weight: bold;")
        
        header.addWidget(self.id_label)
        header.addStretch()
        header.addWidget(self.conf_label)
        
        # Classification
        class_text = result_data.get("classification", "Unclassified").upper()
        self.class_label = SubtitleLabel(class_text, self)
        if is_novel:
            self.class_label.setStyleSheet(f"color: {accent_color}; font-weight: bold;")
            
        # Lineage (Taxonomic Context)
        lineage_raw = result_data.get("lineage", "Unknown Lineage")
        if isinstance(lineage_raw, list):
            lineage_raw = " › ".join(lineage_raw)
        lineage_text = str(lineage_raw).replace(" > ", " › ")
        
        self.lineage_label = CaptionLabel(lineage_text, self)
        self.lineage_label.setStyleSheet("color: #AAA; font-style: italic;")
        self.lineage_label.setWordWrap(False)
        
        # Action Button
        action_layout = QHBoxLayout()
        action_layout.addStretch()
        
        self.topology_btn = TransparentPushButton(FIF.SEARCH, "VIEW TOPOLOGY", self)
        self.topology_btn.setFixedHeight(24)
        self.topology_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.topology_btn.clicked.connect(lambda: self.view_topology.emit(self.result_data))
        
        action_layout.addWidget(self.topology_btn)
        
        content_layout.addLayout(header)
        content_layout.addWidget(self.class_label)
        content_layout.addWidget(self.lineage_label)
        content_layout.addStretch()
        content_layout.addLayout(action_layout)
        
        layout.addWidget(self.accent_bar)
        layout.addLayout(content_layout)

class BatchSummary(QFrame):
    """
    @UX-Visionary: Expedition Metrics.
    Displays high-level batch statistics and reporting actions.
    """
    request_report = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.total_count = 0
        self.novel_count = 0
        
        self.setFixedHeight(50)
        self.setStyleSheet(f"""
            QFrame {{
                background-color: rgba(255, 255, 255, 0.03);
                border: 1px solid #333;
                border-radius: 6px;
            }}
        """)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(15, 0, 15, 0)
        layout.setAlignment(Qt.AlignmentFlag.AlignVCenter)
        
        # Metrics
        self.total_label = CaptionLabel("TOTAL SEQUENCES: 0", self)
        self.total_label.setStyleSheet("color: #888; font-weight: bold; font-family: 'Consolas';")
        
        self.separator = QFrame(self)
        self.separator.setFrameShape(QFrame.Shape.VLine)
        self.separator.setFixedHeight(16)
        self.separator.setStyleSheet("color: #444;")
        
        self.novel_label = CaptionLabel("NOVEL ENTITIES: 0", self)
        self.novel_label.setStyleSheet(f"color: {app_config.THEME_COLORS['accent']}; font-weight: bold; font-family: 'Consolas';")
        
        # Action
        self.report_btn = PrimaryPushButton(FIF.DOCUMENT, "GENERATE EXPEDITION REPORT", self)
        self.report_btn.setFixedSize(240, 32)
        self.report_btn.clicked.connect(self.request_report.emit)
        
        layout.addWidget(self.total_label)
        layout.addSpacing(15)
        layout.addWidget(self.separator)
        layout.addSpacing(15)
        layout.addWidget(self.novel_label)
        layout.addStretch()
        layout.addWidget(self.report_btn)
        
    def update_stats(self, total, novel):
        self.total_count = total
        self.novel_count = novel
        self.total_label.setText(f"TOTAL SEQUENCES: {self.total_count}")
        self.novel_label.setText(f"NOVEL ENTITIES: {self.novel_count}")

class MonitorView(QWidget):
    """
    @WinUI-Fluent: Monitor View.
    Main dashboard for ingestion and real-time results.
    """
    view_topology_requested = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("MonitorView")

        self.total_sequences = 0
        self.novel_entities = 0
        
        # Main Layout
        self.v_layout = QVBoxLayout(self)
        self.v_layout.setContentsMargins(30, 30, 30, 30)
        self.v_layout.setSpacing(20)
        
        # 1. Header
        self.header_layout = QVBoxLayout()
        self.title = TitleLabel("EXPEDIA: GENOMIC SIGNAL MONITOR", self)
        self.subtitle = SubtitleLabel("CURRENT EXPEDITION: ACTIVE BATCH", self)
        self.subtitle.setStyleSheet("color: #888;")
        
        self.header_layout.addWidget(self.title)
        self.header_layout.addWidget(self.subtitle)
        
        # 2. Ingestion Zone
        self.drop_zone = DropZone(self)
        self.progress_bar = ProgressBar(self)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.hide()  # Hidden until start
        
        # 2.5 Batch Summary
        self.batch_summary = BatchSummary(self)
        
        # 3. Results Feed (Scroll Area)
        self.feed_label = CaptionLabel("LIVE CLASSIFICATION FEED", self)
        self.feed_label.setStyleSheet(f"color: {app_config.THEME_COLORS['primary']}; font-weight: bold; margin-top: 10px;")
        
        self.scroll_area = SmoothScrollArea(self)
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("background-color: transparent; border: none;")
        
        self.feed_container = QWidget()
        self.feed_layout = QVBoxLayout(self.feed_container)
        self.feed_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.feed_layout.setSpacing(10)
        
        self.scroll_area.setWidget(self.feed_container)
        
        # 4. Terminal Box
        self.terminal_label = CaptionLabel("SYSTEM LOGS", self)
        self.terminal_output = QTextEdit(self)
        self.terminal_output.setReadOnly(True)
        self.terminal_output.setFixedHeight(150)
        self.terminal_output.setStyleSheet(f"""
            QTextEdit {{
                background-color: #05080F;
                color: #00FF00;
                font-family: 'Consolas', 'Courier New', monospace;
                border: 1px solid #333;
                border-radius: 5px;
                font-size: 11px;
            }}
        """)
        
        # Assemble
        self.v_layout.addLayout(self.header_layout)
        self.v_layout.addWidget(self.batch_summary)
        self.v_layout.addWidget(self.drop_zone)
        self.v_layout.addWidget(self.progress_bar)
        
        self.v_layout.addWidget(self.feed_label)
        self.v_layout.addWidget(self.scroll_area, stretch=1) # Takes available space
        
        self.v_layout.addWidget(self.terminal_label)
        self.v_layout.addWidget(self.terminal_output)

    def add_result_card(self, result_data: dict):
        """Adds a new card to the feed."""
        card = DiscoveryCard(result_data, self.feed_container)
        
        # Connect Topology Signal
        card.view_topology.connect(self.view_topology_requested.emit)
        
        # Update Stats
        self.total_sequences += 1
        if result_data.get("status") == "Novel":
            self.novel_entities += 1
        self.batch_summary.update_stats(self.total_sequences, self.novel_entities)
        
        # Insert at top for reverse chronological order
        self.feed_layout.insertWidget(0, card)

    def log_message(self, message: str):
        """Appends a message to the terminal."""
        self.terminal_output.append(message)
        # Scroll to bottom
        sb = self.terminal_output.verticalScrollBar()
        sb.setValue(sb.maximum())
