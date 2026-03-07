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
    file_selected = Signal(str)

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
        
        self.layout = QVBoxLayout(self)
        self.layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Normal State Widgets
        self.label = SubtitleLabel("DRAG & DROP FASTA FILES OR BROWSE EDGE FILESYSTEM", self)
        self.label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.browse_btn = PrimaryPushButton(FIF.FOLDER, "SELECT SEQUENCE FILE", self)
        self.browse_btn.setFixedWidth(200)
        self.browse_btn.clicked.connect(self.select_file)
        
        # Loading State Widgets (Hidden by default)
        self.loading_label = CaptionLabel("INITIALISING NEURAL-CORE (30s EST)", self)
        self.loading_label.setStyleSheet(f"color: {app_config.THEME_COLORS['accent']}; font-weight: bold;")
        self.loading_label.hide()
        
        self.progress_bar = ProgressBar(self)
        self.progress_bar.setFixedWidth(200)
        # Indeterminate pulsing animation
        self.progress_bar.setRange(0, 0) 
        self.progress_bar.hide()
        
        self.layout.addWidget(self.label)
        self.layout.addSpacing(10)
        self.layout.addWidget(self.browse_btn)
        self.layout.addWidget(self.loading_label)
        self.layout.addWidget(self.progress_bar)
        
        # Start in Boot Mode by default
        self.set_kernel_loading(True, "INITIALISING NEURAL-CORE (50s EST)")

    def set_kernel_loading(self, is_loading, message=None):
        """
        Toggles between File Selection and Kernel Boot visualization.
        """
        if is_loading:
            self.browse_btn.setEnabled(False)
            self.browse_btn.setText("SYSTEM BOOTING...")
            self.label.hide()
            self.loading_label.show()
            self.progress_bar.show()
            if message:
                self.loading_label.setText(message.upper())
        else:
            self.browse_btn.setEnabled(True)
            self.browse_btn.setText("SELECT SEQUENCE FILE")
            self.label.show()
            self.loading_label.hide()
            self.progress_bar.hide()

    def select_file(self):
        from PySide6.QtWidgets import QFileDialog
        from ...config import app_config
        
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Sequence File",
            str(app_config.DATA_ROOT), # Start in project root
            "Genomic Sequences (*.fasta *.fastq *.txt)"
        )
        
        if file_path:
            self.file_selected.emit(file_path)

class RankBreadcrumbBar(QWidget):
    """
    @UX-Visionary: Visual Rank Indicators.
    Displays confidence levels for Linnaean ranks (P, C, O, F, G, S).
    """
    def __init__(self, confidence_per_rank, lineage_parts, parent=None):
        super().__init__(parent)
        self.setFixedHeight(18)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
        
        # Standard Ranks
        RANKS = [
            ('P', 'Phylum'), 
            ('C', 'Class'), 
            ('O', 'Order'), 
            ('F', 'Family'), 
            ('G', 'Genus'), 
            ('S', 'Species')
        ]
        
        # Parse available data into a map: RankName -> (Confidence, IsBracketed)
        data_map = {}
        
        # Zip logic: lineage_parts corresponds to confidence_per_rank by index
        # BUT confidence_per_rank has full names like "Phylum", "Class"
        # We need to trust the order from taxonomy.py is consistently descending?
        # taxonomy.py: for rank in ['Kingdom', 'Phylum', ...] -> append
        # So yes, they are ordered.
        
        for idx, (rank_name, conf) in enumerate(confidence_per_rank):
            name_in_string = lineage_parts[idx] if idx < len(lineage_parts) else ""
            is_bracketed = "[" in name_in_string
            clean_name = name_in_string.replace("[", "").replace("]", "")
            data_map[rank_name] = {
                "conf": conf,
                "is_bracketed": is_bracketed,
                "name": clean_name
            }

        # Build Indicators
        for code, full_rank in RANKS:
            lbl = QLabel(code, self)
            lbl.setFixedSize(16, 16)
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            
            # Determine Color
            # Default: Red/Grey (Missing/Low)
            bg_color = "#331111" # Dark Red background for missing
            border_color = "#552222"
            text_color = "#885555"
            tooltip = f"{full_rank}: Insufficient Data"

            if full_rank in data_map:
                info = data_map[full_rank]
                name = info['name']
                is_bracketed = info['is_bracketed']
                
                tooltip = f"{full_rank}: {name}"
                if is_bracketed:
                    # Inferred (Yellow)
                    bg_color = "#333300"
                    border_color = "#666600"
                    text_color = "#FFFF00"
                    tooltip += " (Inferred/Divergent)"
                else:
                    # Confirmed (Green)
                    bg_color = "#003300"
                    border_color = "#006600"
                    text_color = "#00FF00"
                    tooltip += " (Confirmed)"
            
            lbl.setStyleSheet(f"""
                QLabel {{
                    background-color: {bg_color};
                    border: 1px solid {border_color};
                    border-radius: 2px;
                    color: {text_color};
                    font-size: 9px;
                    font-weight: bold;
                    font-family: 'Consolas';
                }}
            """)
            lbl.setToolTip(tooltip)
            layout.addWidget(lbl)
            
        layout.addStretch()

class DiscoveryCard(CardWidget):
    """
    @WinUI-Fluent: Result Card.
    Visualizes genomic signal classification.
    """
    view_topology = Signal(dict)

    def __init__(self, result_data, parent=None):
        super().__init__(parent)
        # Apply Sharp/Solid Style
        self.setStyleSheet(f"""
            DiscoveryCard {{
                background-color: {app_config.THEME_COLORS['sidebar']};
                border: 1px solid {app_config.THEME_COLORS['border']};
                border-radius: 0px;
            }}
        """)
        
        self.result_data = result_data
        self.setFixedSize(340, 140)  # Slightly taller for breadcrumbs
        
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
        content_layout.setContentsMargins(12, 8, 5, 8)
        content_layout.setSpacing(2)
        
        # Header: ID + Rank Indicators
        header = QHBoxLayout()
        header.setSpacing(10)
        
        id_text = result_data.get("id", "Unknown").upper()
        # Truncate long IDs
        if len(id_text) > 12: id_text = id_text[:12] + "..."
        
        self.id_label = CaptionLabel(id_text, self)
        self.id_label.setStyleSheet("color: #888; font-weight: bold; font-family: 'Consolas';")
        self.id_label.setToolTip(f"Sequence ID: {result_data.get('id')}")
        
        header.addWidget(self.id_label)
        header.addStretch()
        
        # RANK INDICATORS (Breadcrumb Bar)
        pred_lineage = result_data.get("predicted_lineage", {})
        conf_per_rank = pred_lineage.get("confidence_per_rank", [])
        lineage_str = pred_lineage.get("lineage_string", "")
        
        # We need to split the lineage string to match ranks
        # Logic: "Kingdom > Phylum > ..."
        # Note: If lineage is "Unknown", parts is ["Unknown"]
        lineage_parts = [p.strip() for p in lineage_str.split(">")]
        
        # If confidence_per_rank is missing (legacy data), show single conf
        if not conf_per_rank:
            conf_val = result_data.get("confidence", 0.0) * 100
            self.conf_label = CaptionLabel(f"{conf_val:.1f}% CONF", self)
            self.conf_label.setStyleSheet(f"color: {accent_color}; font-family: 'Consolas'; font-weight: bold;")
            header.addWidget(self.conf_label)
        else:
            # Show Visual Breadcrumbs
            self.breadcrumbs = RankBreadcrumbBar(conf_per_rank, lineage_parts, self)
            self.breadcrumbs.setToolTip("Identity thresholds calibrated based on 18S rRNA evolutionary decay constants (97% Species / 88% Family).")
            header.addWidget(self.breadcrumbs)
        
        # Classification
        class_text = result_data.get("classification", "Unclassified").upper()
        # Clean Terminology
        if "(DARK TAXA)" in class_text:
            class_text = class_text.replace("(DARK TAXA)", "(NRGS)")
            
        # Advanced NRGS Naming
        lineage_str = result_data.get("predicted_lineage", {}).get("lineage_string", "")
        if "NON-REFERENCE" in class_text or "NRGS" in class_text:
             # Try to extract Phylum
             parts = [p.strip() for p in lineage_str.split('>')]
             # 0:k, 1:p
             if len(parts) > 1 and parts[1] not in ["Unclassified", "Unknown"]:
                 class_text = f"NON-REFERENCE {parts[1].upper()}"

        self.class_label = SubtitleLabel(class_text, self)
        if is_novel:
            self.class_label.setStyleSheet(f"color: {accent_color}; font-weight: bold;")
        
        # Lineage Label with Smart Redaction
        # Convert [Name] to Name (?) in italics
        smart_lineage_text = lineage_str
        if smart_lineage_text:
             # HTML Replacement
             # Escape existing HTML chars first? Assumed safe.
             import re
             # Replace [Name] with <i>Name (?)</i>
             smart_lineage_text = re.sub(r"\[(.*?)\]", r"<i style='color:#FFFF00'>\1 (?)</i>", smart_lineage_text)
             # Replace separators with clean arrows
             smart_lineage_text = smart_lineage_text.replace(">", "<span style='color:#444'> › </span>")
        else:
            smart_lineage_text = "Metazoa › Unresolved Lineage"
            
        self.lineage_label = QLabel(self)
        self.lineage_label.setText(smart_lineage_text)
        self.lineage_label.setStyleSheet("color: #CCC; font-family: 'Consolas', monospace; font-size: 11px;")
        self.lineage_label.setWordWrap(False)
        self.lineage_label.setTextFormat(Qt.TextFormat.RichText)
        
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
        
        self.novel_label = CaptionLabel("NRGS (NON-REF): 0", self)
        self.novel_label.setStyleSheet(f"color: {app_config.THEME_COLORS['accent']}; font-weight: bold; font-family: 'Consolas';")
        
        # Action
        self.report_btn = PrimaryPushButton(FIF.SHARE, "EXPORT GENOMIC DATASET", self)
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
        self.novel_label.setText(f"NRGS (NON-REF): {self.novel_count}")

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
        self.subtitle = SubtitleLabel("CURRENT GENOMIC DATASET: ACTIVE BATCH", self)
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
        self.terminal_label = CaptionLabel("SYSTEM OPERATIONS LOG", self)
        self.terminal_output = QTextEdit(self)
        self.terminal_output.setReadOnly(True)
        self.terminal_output.setFixedHeight(150)
        self.terminal_output.setStyleSheet(f"""
            QTextEdit {{
                background-color: #0D0D0D;
                color: #00FF00;
                font-family: 'Consolas', 'Courier New', monospace;
                border: 1px solid {app_config.THEME_COLORS['border']};
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

    def update_progress(self, value: int):
        """Updates the progress bar value."""
        self.progress_bar.setValue(value)
