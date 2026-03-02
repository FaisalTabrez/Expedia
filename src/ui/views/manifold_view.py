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
            
        def setHtml(self, html, base_url=None):
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
from sklearn.decomposition import PCA

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


    def generate_neighborhood_view(self, query_id: str, query_vector: np.ndarray, is_novel: bool = False):
        """
        Calculates local PCA and renders the 3D plot.
        """
        logger.info(f"Generating manifold for {query_id}")
        self.has_content = True
        self.current_query_vector = query_vector

        try:
            # 1. Fetch Neighbors (500 for context)
            df_neighbors = self.db.vector_search(query_vector, top_k=500)
            
            if df_neighbors.empty:
                logger.warning("No neighbors found for manifold view.")
                return

            # 2. Prepare Data for PCA
            # Query vector needs to be part of the set to be projected
            # Assuming 'vector' column exists in dataframe and is a list/array
            # If LanceDB returns 'vector' as a column.
            
            # Extract vectors from dataframe. 
            # Note: Depending on LanceDB version/config, vector might not be returned by default unless requested
            # For this 'mock' implementation, we assume column 'vector' exists or we can't project.
            # If not available, we can't do PCA.
            
            if 'vector' not in df_neighbors.columns:
                 # If vectors aren't returned, we might need to re-query or handle error
                 # For now, let's assume we have them or mock them for the UI demo if DB is empty
                 logger.warning("Vector column missing from search results. Cannot project.")
                 return

            neighbor_vectors = np.stack(df_neighbors['vector'].tolist())
            all_vectors = np.vstack([query_vector, neighbor_vectors])
            
            # 3. Apply Localized PCA (768 -> 3)
            pca = PCA(n_components=3)
            principal_components = pca.fit_transform(all_vectors)
            
            # Split back
            query_pc = principal_components[0]
            neighbors_pc = principal_components[1:]
            
            # 4. Create Plotly Figure
            fig = go.Figure()

            # A. Neighbors (Small Bioluminescent Spheres)
            fig.add_trace(go.Scatter3d(
                x=neighbors_pc[:, 0],
                y=neighbors_pc[:, 1],
                z=neighbors_pc[:, 2],
                mode='markers',
                marker=dict(
                    size=4,
                    color='#00E5FF', # Cyan
                    opacity=0.4,
                    line=dict(width=0)
                ),
                text=df_neighbors.get('species', df_neighbors.get('id', 'Unknown')),
                name='Reference Neighbors'
            ))

            # B. Query (Large Glowing Neon Star)
            query_color = app_config.THEME_COLORS['accent'] if is_novel else app_config.THEME_COLORS['primary']
            symbol = 'diamond' if is_novel else 'cross'
            
            fig.add_trace(go.Scatter3d(
                x=[query_pc[0]],
                y=[query_pc[1]],
                z=[query_pc[2]],
                mode='markers',
                marker=dict(
                    size=12,
                    color=query_color,
                    symbol=symbol,
                    opacity=1.0,
                    line=dict(
                        color='#FFFFFF',
                        width=2
                    )
                ),
                text=[f"QUERY: {query_id}"],
                name='Active Sequence'
            ))

            # Styling
            self.update_plot(fig)

        except Exception as e:
            logger.error(f"Manifold Generation Error: {e}")
            InfoBar.error(
                title='Rendering Error',
                content=str(e),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                parent=self
            )

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
                bgcolor=app_config.THEME_COLORS['background']
            ),
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=True,
            legend=dict(
                x=0, y=1,
                bgcolor='rgba(0,0,0,0)'
            )
        )

        if WEB_ENGINE_AVAILABLE:
            html = fig.to_html(include_plotlyjs='cdn', full_html=True)
            self.web_view.setHtml(html)
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
