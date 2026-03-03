
import sys
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout
try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
except ImportError:
    print("QWebEngineView not available")
    sys.exit(0)

import plotly.graph_objects as go
import os
from PySide6.QtCore import QUrl

def test_manifold_view():
    app = QApplication(sys.argv)
    window = QWidget()
    layout = QVBoxLayout(window)
    
    web_view = QWebEngineView()
    layout.addWidget(web_view)
    
    fig = go.Figure(data=go.Scatter3d(
        x=[1, 2, 3], y=[2, 1, 3], z=[1, 2, 3],
        mode='markers',
        marker=dict(size=12, color='red', opacity=0.8)
    ))

    print("Generating HTML...")
    try:
        html = fig.to_html(include_plotlyjs='cdn', full_html=True)
        print("Set HTML...")
        local_url = QUrl.fromLocalFile(os.getcwd())
        web_view.setHtml(html, baseUrl=local_url)
        print("HTML Set.")
    except Exception as e:
        print(f"Error: {e}")

    window.show()
    # Auto-close for headless test purposes
    # sys.exit(app.exec()) 
    print("Test Complete (Visual confirmation required in GUI)")

if __name__ == "__main__":
    test_manifold_view()
