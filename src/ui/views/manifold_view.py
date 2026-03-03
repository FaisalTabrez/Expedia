import json
import logging
import numpy as np
import pandas as pd
from PySide6.QtCore import Qt, QUrl
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFrame, QLabel
)

logger = logging.getLogger("EXPEDIA.ManifoldView")

try:
    from PySide6.QtWebEngineWidgets import QWebEngineView # type: ignore
    WEB_ENGINE_AVAILABLE = True
except ImportError as e:
    logger.error(f"Failed to import PySide6.QtWebEngine: {e}. Falling back to capture.")
    WEB_ENGINE_AVAILABLE = False
    
    # Simple container for static image
    class QWebEngineView(QWidget): # type: ignore
        def __init__(self, parent=None):
            super().__init__(parent)
            self.v_layout = QVBoxLayout(self)
            self.v_layout.setContentsMargins(0, 0, 0, 0)
            self.label = QLabel("Initializing Static Capture...", self)
            self.label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.label.setStyleSheet("color: #666; font-size: 14px;")
            self.v_layout.addWidget(self.label)
            
        def setHtml(self, html, baseUrl=None):
            # No-op for HTML, we need setPixmap or similar for static fallback
            pass
            
        def setPixmap(self, pixmap):
            self.label.setPixmap(pixmap.scaled(
                self.size(), 
                Qt.AspectRatioMode.KeepAspectRatio, 
                Qt.TransformationMode.SmoothTransformation
            ))

from qfluentwidgets import (
    SubtitleLabel, ToolButton, FluentIcon as FIF, InfoBar, InfoBarPosition
)
import plotly.graph_objects as go
# PCA Removed (Avalanche Standard UMAP handled in Kernel)
# from sklearn.decomposition import PCA

from ...config import app_config
from ...core.database import AtlasManager

# logger = logging.getLogger("EXPEDIA.ManifoldView") # Moved up due to usage in try-except

class ManifoldView(QWidget):
    """
    @WinUI-Fluent: Genomic Manifold View.
    @Neural-Core: Visualization of the 768-dim latent space via 3D PCA.
    Uses QWebEngineView to render interactive Plotly charts.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("ManifoldView")
        
        # Layouts
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # Toolbar
        self.toolbar = QFrame(self)
        self.toolbar.setFixedHeight(50)
        self.toolbar.setStyleSheet(f"background-color: {app_config.THEME_COLORS['background']}; border-bottom: 1px solid #333;")
        self.toolbar_layout = QHBoxLayout(self.toolbar)
        self.toolbar_layout.setContentsMargins(10, 0, 10, 0)
        
        self.title_label = SubtitleLabel("GENOMIC MANIFOLD EXPLORER", self.toolbar)
        self.toolbar_layout.addWidget(self.title_label)
        self.toolbar_layout.addStretch()

        # Tools
        self.btn_recenter = ToolButton(FIF.ZOOM_IN, self.toolbar)
        self.btn_recenter.setToolTip("Recenter View")
        
        self.btn_export = ToolButton(FIF.SAVE, self.toolbar)
        self.btn_export.setToolTip("Export Manifold Snapshot")
        
        self.btn_labels = ToolButton(FIF.TAG, self.toolbar)
        self.btn_labels.setToolTip("Toggle Labels")
        
        self.toolbar_layout.addWidget(self.btn_recenter)
        self.toolbar_layout.addWidget(self.btn_export)
        self.toolbar_layout.addWidget(self.btn_labels)

        self.main_layout.addWidget(self.toolbar)

        # Web Engine for Plotly
        self.web_view = QWebEngineView(self)
        self.web_view.setStyleSheet("background-color: transparent;")
        self.main_layout.addWidget(self.web_view)

        # Database Link (Initialized on demand or passed in)
        self.db = AtlasManager() 
        
        # UI Components for Loading (needed for handle_error)
        self.loading_overlay = QLabel(self)
        self.loading_overlay.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.loading_overlay.setStyleSheet("background-color: rgba(0, 0, 0, 0.8); color: #00E5FF; font-size: 16px;")
        self.loading_overlay.setText("Creating Manifold...")
        self.loading_overlay.hide()

        # State
        self.current_query_vector = None
        self.has_content = False
        
        # Initial Empty State
        self.show_empty_state()

        if not WEB_ENGINE_AVAILABLE:
            from qfluentwidgets import InfoBar
            InfoBar.warning(
                title='Rendering Mode: Compatibility (Static)',
                content='Interactive 3D Manifold unavailable due to missing WebEngine components. Using static capture mode.',
                orient=Qt.Orientation.Horizontal,
                position=InfoBarPosition.BOTTOM_RIGHT,
                duration=10000,
                parent=self
            )

    def show_empty_state(self):
        """Displays instruction when no sequence is selected."""
        if WEB_ENGINE_AVAILABLE:
            # Using a blank HTML with dark theme to keep aesthetic
            html = f"""
            <html>
            <head>
            <style>
                body {{ background-color: {app_config.THEME_COLORS['background']}; color: #555; font-family: 'Segoe UI', sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
                h1 {{ font-weight: normal; font-size: 18px; }}
            </style>
            </head>
            <body>
                <h1>SELECT A SEQUENCE FROM THE MONITOR TO EXPLORE GENOMIC TOPOLOGY</h1>
            </body>
            </html>
            """
            self.web_view.setHtml(html)
        else:
             if hasattr(self.web_view, 'label'):
                self.web_view.label.setText("SELECT A SEQUENCE FROM THE MONITOR TO EXPLORE GENOMIC TOPOLOGY (STATIC MODE)")

    def show_loading(self):
        """Displays loading state."""
        logger.info("Showing loading state for manifold view")
        if WEB_ENGINE_AVAILABLE:
            html = f"""
            <html>
            <head>
            <style>
                body {{ background-color: {app_config.THEME_COLORS['background']}; color: #00E5FF; font-family: 'Consolas', monospace; display: flex; flexDirection: column; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
                h2 {{ font-weight: normal; font-size: 20px; letter-spacing: 2px; }}
            </style>
            </head>
            <body>
                <h2>CALCULATING MICRO-TOPOLOGY...</h2>
                <p style="color: #666; font-size: 14px;">(HDBSCAN Clustering + Local PCA)</p>
            </body>
            </html>
            """
            self.web_view.setHtml(html)
        else:
            if hasattr(self.web_view, 'label'):
                self.web_view.label.setText("CALCULATING MICRO-TOPOLOGY... (STATIC MODE)")

    def generate_neighborhood_view(self, query_id: str, query_vector: np.ndarray, is_novel: bool = False):
        """
        Legacy local calculation. Delegating to render_manifold via warning to update calling code.
        """
        logger.warning(f"Deprecated 'generate_neighborhood_view' called for {query_id}. Update caller to use async 'request_localized_topology'.")
        # Can't do much here because we don't have neighbors.
        # Just show loading until the async worker comes back (if it was triggered separately).
        self.show_loading()

    def handle_error(self, message: str):
        """
        Handles topology generation errors gracefully.
        """
        logger.error(f"Manifold Error: {message}")
        if hasattr(self, 'loading_overlay'):
            self.loading_overlay.hide()
            
        from qfluentwidgets import InfoBar, InfoBarPosition
        InfoBar.error(
            title='Topology Reconstruction Failed',
            content=f'{message}',
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP_RIGHT,
            duration=8000,
            parent=self
        )
        self.show_empty_state()

    def render_manifold(self, data: dict):
        """
        Renders the 3D plot from Kernel JSON data.
        """
        # State Reset
        if hasattr(self, 'loading_overlay'):
            self.loading_overlay.hide()

        logger.info("Rendering Manifold from Kernel Data")
        
        # Debug Logging (JavaScript Handshake)
        neighborhood_size = len(data.get("neighbors", []))
        print(f"UI: Received Manifold Data. Points: {neighborhood_size + 1}")

        self.has_content = True
        
        # Immediate UI Clean
        if WEB_ENGINE_AVAILABLE:
            # Clear loading message to avoid flicker or stale state
            pass # setHtml will overwrite
            
        try:
            # 1. Parse Data
            status = data.get("status")
            if status == "empty" or status == "error":
                logger.warning(f"Manifold Generation Failed: {data.get('message', 'Unknown')}")
                from qfluentwidgets import InfoBar, InfoBarPosition
                InfoBar.warning(
                    title='Micro-Topology Error',
                    content='FAILED TO RECONSTRUCT GENOMIC TOPOLOGY',
                    orient=Qt.Orientation.Horizontal,
                    isClosable=True,
                    position=InfoBarPosition.TOP_RIGHT,
                    duration=5000,
                    parent=self
                )
                self.show_empty_state()
                return

            query_point = data.get("query")
            neighbors = data.get("neighbors", [])
            consensus = data.get("consensus", "Analyzing...")
            
            if not query_point or not neighbors:
                logger.warning("Manifold data missing query point or neighbors.")
                self.show_empty_state()
                return

            # 2. Create Plotly Figure
            fig = go.Figure()

            # ... (Existing Trace Logic) ...
            # A. Neighbors
            if neighbors:
                x_vals = [n['coords'][0] for n in neighbors if n.get('coords')]
                y_vals = [n['coords'][1] for n in neighbors if n.get('coords')]
                z_vals = [n['coords'][2] for n in neighbors if n.get('coords')]
                
                text_vals = [f"{n.get('classification', 'Unknown')}<br>{n.get('lineage','')}" for n in neighbors]
                
                # Dynamic Coloring based on classification if available, else depth/index
                # Using 'Viridis' color scale as requested for better contrast
                colors = np.linspace(0, 1, len(neighbors))
                
                fig.add_trace(go.Scatter3d(
                    x=x_vals, y=y_vals, z=z_vals,
                    mode='markers',
                    marker=dict(
                        size=4,
                        color=colors,
                        colorscale='Viridis',
                        opacity=0.8,
                        line=dict(width=0)
                    ),
                    text=text_vals,
                    name='Neighborhood'
                ))

            # B. Query Point / Holotype Reference
            q_coords = query_point.get("coords")
            if not q_coords: return

            q_label = query_point.get("label", -1)
            # Neon Pink Pulsing Star for Holotype
            query_color = '#FF007A' 
            symbol = 'cross' # Star-like
            
            fig.add_trace(go.Scatter3d(
                x=[q_coords[0]], y=[q_coords[1]], z=[q_coords[2]],
                mode='markers',
                marker=dict(
                    size=12,
                    color=query_color,
                    symbol='diamond-open', # Distinctive Holotype Marker
                    opacity=1.0,
                    line=dict(color='#FFFFFF', width=2)
                ),
                text=["HOLOTYPE REFERENCE (Analysed Seq)"],
                name='Holotype Reference'
            ))
            
            # C. Dashed Line (Evolutionary Distance)
            if neighbors:
                nn = neighbors[0] 
                nn_coords = nn.get('coords')
                if nn_coords:
                    fig.add_trace(go.Scatter3d(
                        x=[q_coords[0], nn_coords[0]],
                        y=[q_coords[1], nn_coords[1]],
                        z=[q_coords[2], nn_coords[2]],
                        mode='lines',
                        line=dict(color='#AAAAAA', width=3, dash='dash'),
                        name='Min. Distance Vector'
                    ))

            # D. Cluster Hull (Bioluminescent Aura)
            if q_label != -1:
                cluster_points = [n['coords'] for n in neighbors if n.get('label') == q_label and n.get('coords')]
                if len(cluster_points) > 4:
                     pts = np.array(cluster_points)
                     # Include the query point in hull
                     pts = np.vstack([pts, q_coords])
                     try:
                         # AlphaHull=7 for smoother, organic shape
                         fig.add_trace(go.Mesh3d(
                            x=pts[:,0], y=pts[:,1], z=pts[:,2],
                            opacity=0.2,
                            color='#FF007A',
                            alphahull=7,
                            name='Bioluminescent Aura'
                         ))
                     except Exception as e: 
                        logger.warning(f"Hull generation failed: {e}")
            
            # E. Consensus Annotation
            fig.add_annotation(
                text=f"LOCAL CONSENSUS:<br>{consensus}",
                xref="paper", yref="paper",
                x=0.02, y=0.98,
                showarrow=False,
                font=dict(family="Consolas", size=14, color="#00E5FF"),
                align="left",
                bgcolor=app_config.THEME_COLORS['background'],
                bordercolor="#333",
                borderwidth=1,
                borderpad=10
            )

            # Styling & Injection
            self.update_plot(fig)

        except Exception as e:
            logger.error(f"Manifold Render Error: {e}")
            self.show_empty_state()
    
    def update_plot(self, fig: go.Figure):
        """
        Serializes Plotly figure and updates the WebEngineView.
        """
        # Layout customization for 'Bioluminescent Abyss'
        fig.update_layout(
            paper_bgcolor=app_config.THEME_COLORS['background'],
            plot_bgcolor=app_config.THEME_COLORS['background'],
            font=dict(color='#A0A0A0'),
            scene=dict(
                xaxis=dict(showgrid=False, zeroline=False, showbackground=False, visible=False),
                yaxis=dict(showgrid=False, zeroline=False, showbackground=False, visible=False),
                zaxis=dict(showgrid=False, zeroline=False, showbackground=False, visible=False),
                bgcolor=app_config.THEME_COLORS['background'],
                aspectmode='data' # Scale axes to match data range (prevents squashing)
            ),
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=True,
            legend=dict(
                x=0, y=1,
                bgcolor='rgba(0,0,0,0)'
            )
        )

        if WEB_ENGINE_AVAILABLE:
            import os # Ensure os is available for CWD
            html = fig.to_html(include_plotlyjs='cdn', full_html=True)
            self.web_view.setHtml(html, baseUrl=QUrl.fromLocalFile(os.getcwd()))
        else:
            # STATIC CAPTURE FALLBACK
            try:
                # Requires kaleido or similar
                # Let's try Plotly static image generation (needs kaleidoscope)
                # If that fails, text.
                
                img_bytes = fig.to_image(format="png", width=800, height=600)
                
                # Convert bytes to QPixmap
                from PySide6.QtGui import QImage, QPixmap
                img = QImage.fromData(img_bytes)
                pixmap = QPixmap.fromImage(img)
                self.web_view.setPixmap(pixmap)
                
            except Exception as img_err:
                 logger.warning(f"Static capture failed (missing kaleido?): {img_err}")
                 if hasattr(self.web_view, 'label'):
                    self.web_view.label.setText("Interactive Plot Unavailable.\n(Install 'kaleido' for static previews)")
