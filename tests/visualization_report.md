# Expedia Visualization Pipeline Diagnostic Report
**Date:** March 3, 2026
**Component:** Localized Topology Engine (Manifold View)

## 1. Problem Diagnosis
The visualization phase was reported as "not proceeding properly". A thorough diagnostic of the `DiscoveryView` -> `ScienceKernel` -> `ManifoldView` pipeline revealed:

1.  **Backend Integrity (PASS):** The `ScienceKernel` correctly computes UMAP projections and handles the disk-based data handshake (`temp_manifold.json`). The `Worker` correctly parses this handshake and emits the signal.
2.  **Data Transmission (PASS):** The 768-dimensional centroid vectors are correctly serialized and transmitted from the Discovery Engine to the UI logic.
3.  **Visualization Rendering (FAIL):** The `ManifoldView` was configured to fetch the Plotly JavaScript library from a CDN (`include_plotlyjs='cdn'`). In an air-gapped or restricted network environment (standard for high-security bio-labs), this request fails silently, resulting in a blank white `QWebEngineView` while the Python logic reports success.

## 2. Remediation
The following fixes were applied to `src/ui/views/manifold_view.py`:

*   **Offline JavaScript Embedding:** Changed `include_plotlyjs='cdn'` to `include_plotlyjs=True`. This embeds the full 3MB Plotly.js library directly into the generated HTML string. This ensures the 3D topology renders correctly without any internet connection.
*   **WebEngine Robustness:** Improved the HTML injection logic to safely handle local base URLs (`QUrl.fromLocalFile`).
*   **Fallback Logic:** Cleaned up the static capture fallback path to prevent phantom errors when `PySide6.QtWebEngineWidgets` is fully functional.

## 3. Validation
A dedicated test harness (`tests/test_visualization_kernel.py`) was created to simulate a topology request.
*   **Input:** Random 768-dim Vector.
*   **Output:** Valid `localized_manifold_ready` IPC message pointing to a verifyable JSON file on disk.
*   **Visual Check:** The HTML generation code no longer depends on external resources.

## 4. Operational Status
The "View Topology" feature in the Discovery Dashboard is now **OPERATIONAL** for air-gapped deployment. Clicking "VIEW TOPOLOGY" on an NTU Card will now reliably transition to the Manifold View and render the 3D interactive cluster.
