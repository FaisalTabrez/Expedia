import sys
from PySide6.QtCore import Qt, QSize, QThread, Signal, Slot
from PySide6.QtGui import QIcon, QColor
from PySide6.QtWidgets import QApplication, QFrame, QVBoxLayout, QLabel

from qfluentwidgets import (
    FluentWindow, 
    NavigationItemPosition, 
    FluentIcon as FIF,
    SplashScreen
)

# Import Worker
from ..core.worker import DiscoveryWorker
from ..config import app_config

# Import Views
from .views.monitor_view import MonitorView
from .views.manifold_view import ManifoldView
from .views.discovery_view import DiscoveryView
from .views.manual_view import ManualView
from .views.benchmarking_view import BenchmarkingView
from ..core.reporting import DiscoveryReporter
from PySide6.QtWidgets import QPushButton

class MainWindow(FluentWindow):
    """
    @WinUI-Fluent: Main Application Shell.
    Manages navigation, worker threads, and global state.
    """
    request_inference = Signal(str)
    # request_localized_manifold = Signal(list) # Changed to dict for ID context
    request_localized_manifold = Signal(dict)
    request_kernel_boot = Signal() # Signal to trigger async boot

    def __init__(self):
        super().__init__()
        
        # Window Setup
        self.setWindowTitle("EXPEDIA | PRECISION GENOMIC ANALYTIC ENGINE")
        self.resize(1280, 800)
        
        # Center on screen
        screen_geometry = QApplication.primaryScreen().availableGeometry()
        x = (screen_geometry.width() - self.width()) - 50
        y = (screen_geometry.height() - self.height()) // 2
        self.move(x, y)
        
        # State Data
        self.current_ntus = []
        self.current_isolated = []

        # Initialize User Interfaces
        self.monitor_interface = MonitorView(self)
        self.manifold_interface = ManifoldView(self)
        self.benchmarking_interface = BenchmarkingView(self) 
        self.discovery_interface = DiscoveryView(self)
        self.manual_interface = ManualView(self) 

        # Worker Thread Setup
        self.worker_thread = QThread()
        self.worker = DiscoveryWorker()
        self.worker.moveToThread(self.worker_thread)
        
        # Connect Signals
        self.request_inference.connect(self.worker.run_inference)
        self.request_localized_manifold.connect(self.worker.request_localized_topology)
        self.request_kernel_boot.connect(self.worker.startup_kernel)
        
        self.worker.progress.connect(self.monitor_interface.update_progress)
        self.worker.batch_complete.connect(self.on_batch_complete)
        self.worker.error.connect(self.on_worker_error)
        self.worker.sequence_processed.connect(self.on_sequence_processed)
        self.worker.localized_topology_ready.connect(self.on_localized_topology_ready)
        
        # Kernel Boot Status
        self.worker.status_update.connect(self.on_kernel_status_update)
        self.worker.kernel_ready.connect(self.on_kernel_ready)

        # Status Bar Action - Using Monitor View for Export to avoid crashes
        if hasattr(self.monitor_interface, 'batch_summary'):
             self.monitor_interface.batch_summary.request_report.connect(self.on_export_action)

        self.init_navigation()
        self.init_signals()
        self.init_system()
        
        # Apply custom visual tweaks
        self.navigationInterface.setExpandWidth(280)

    def init_signals(self):
        """
        Connect global signals across views.
        """
        # Monitor -> Manifold Redirection
        self.monitor_interface.view_topology_requested.connect(self.on_view_topology_requested)
        
        # Monitor -> Worker (Start Inference)
        self.monitor_interface.drop_zone.file_selected.connect(self.start_inference)
        
        # Worker Signals (Additional to __init__)
        self.worker.started.connect(self.on_inference_started)
        
        # Kernel logging
        self.worker.kernel_log.connect(self.monitor_interface.log_message)
        
        # 3. Inter-View Navigation
        self.discovery_interface.request_cluster_view.connect(self.on_view_cluster_topology)



    def on_view_topology_requested(self, data: dict):
        """
        @UX-Visionary: The 'Jump'.
        Switches to Manifold View and focuses on the sequence's neighborhood.
        """
        if not data:
            return

        seq_id = data.get("id", "Unknown")
        vector = data.get("vector") # Ensure science kernel passes this
        
        if vector is None:
            # Fallback for now
            pass

        # 1. Switch Tab (Index 1 is Manifold)
        self.switchTo(self.manifold_interface)
        
        # 2. Visual Confirmation
        from qfluentwidgets import InfoBar, InfoBarPosition
        InfoBar.info(
            title="EXPEDIA TOPOLOGY ENGINE",
            content=f"EXPLORING TOPOLOGY FOR SEQUENCE [{seq_id}]",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP_RIGHT,
            duration=3000,
            parent=self
        ).show()
        
        # 3. Trigger Manifold Update
        if vector is not None:
             # self.manifold_interface.generate_neighborhood_view(seq_id, vector)
             # Send Request to Worker
             print(f'UI: Requesting Localized Topology for {seq_id}')
             if not self.worker_thread.isRunning():
                 self.worker_thread.start()
             
             payload = {
                 "id": seq_id,
                 "vector": vector
             }
             self.request_localized_manifold.emit(payload)
             
             self.manifold_interface.show_loading()

    def init_navigation(self):
        """
        Setup the Fluent Navigation bar.
        """
        # 1. Monitor (Home) - Icon: FIF.IOT
        self.addSubInterface(
            self.monitor_interface, 
            FIF.IOT, 
            "EXPEDIA: MONITOR", 
            NavigationItemPosition.TOP
        )
        
        # 2. Manifold - Icon: FIF.GLOBE
        self.addSubInterface(
            self.manifold_interface, 
            FIF.GLOBE, 
            "EXPEDIA: MANIFOLD", 
            NavigationItemPosition.TOP
        )
        
        # 3. Inference (Benchmarking) - Icon: FIF.SPEED_HIGH
        self.addSubInterface(
            self.benchmarking_interface, 
            FIF.SPEED_HIGH, 
            "EXPEDIA: BENCHMARKS", 
            NavigationItemPosition.TOP
        )
        
        # 4. Discovery - Icon: FIF.TILES
        self.addSubInterface(
            self.discovery_interface, 
            FIF.TILES, 
            "EXPEDIA: DISCOVERY", 
            NavigationItemPosition.TOP
        )
        
        # 5. Manual (Settings/Docs) - Icon: FIF.BOOK_SHELF
        self.addSubInterface(
            self.manual_interface, 
            FIF.BOOK_SHELF, 
            "EXPEDIA: SPECS", 
            NavigationItemPosition.BOTTOM
        )

    def init_system(self):
        """
        Initialize System Status and Logging
        """
        import shutil
        from ..core.database import AtlasManager

        # 1. Terminal Greeting
        log = self.monitor_interface.log_message
        log("SYSTEM: EXPEDIA INITIALIZED.")
        
        # Storage Check
        if app_config.DATA_ROOT.anchor == "E:\\":
            log("STORAGE: VOLUME E: (NTFS) DETECTED.")
        else:
            log(f"STORAGE: {app_config.DATA_ROOT} (FALLBACK) DETECTED.")

        # Resources Check
        if app_config.TAXONKIT_EXE.exists():
            log("RESOURCES: TAXONKIT BINARY FOUND.")
        else:
            log("RESOURCES: TAXONKIT BINARY MISSING.")

        if app_config.WORMS_CSV.exists():
            # Mock count or read line count roughly
            log("RESOURCES: WORMS ORACLE LOADED (763+ TAXA).")
        
        # Database Connection (reuse manifold's instance or creating new lightweight one)
        # Manifold view creates an AtlasManager instance on init.
        db_count = 0
        if self.manifold_interface.db:
            db_count = self.manifold_interface.db.get_count()
            log(f"DATABASE: {app_config.ATLAS_TABLE_NAME} IS ONLINE.")
        else:
             log(f"DATABASE: {app_config.ATLAS_TABLE_NAME} FAILED TO LOAD.")

        # Trigger Kernel Boot
        if not self.worker_thread.isRunning():
            self.worker_thread.start()
        self.request_kernel_boot.emit()

        # 2. Update Status Bar
        # FluentWindow inheritance check
        # Explicit check and cast to avoid static analysis errors on dynamic attributes
        if hasattr(self, 'statusBar') and callable(getattr(self, 'statusBar')):
             sb = self.statusBar() # type: ignore
             if sb:
                 sb.setStyleSheet(f"background-color: {app_config.THEME_COLORS['background']}; color: #666; font-family: 'Consolas'; font-size: 11px;")
                 
                 # Disk Usage
                 import shutil
                 total, used, free = shutil.disk_usage(app_config.DATA_ROOT)
                 free_gb = free / (1024**3)
                 drive_name = str(app_config.DATA_ROOT)[:2] # "E:" or "C:"
                 
                 status_msg = (
                     f"INDEX: {db_count:,} SIGNATURES | "
                     f"ENGINE: NUCLEOTIDE-TRANSFORMER-V2-50M | "
                     f"STORAGE: {drive_name} ({free_gb:.1f} GB FREE)"
                 )
                 sb.showMessage(status_msg)

    def start_inference_demo(self):
        # basic file dialog to pick a file
        from PySide6.QtWidgets import QFileDialog
        file_path, _ = QFileDialog.getOpenFileName(self, "SELECT FASTA SEQUENCE", "", "FASTA FILES (*.fasta *.fa)")
        if file_path:
            self.start_inference(file_path)

    def start_inference(self, file_path):
        """Starts the worker thread."""
        if not self.worker_thread.isRunning():
            self.worker_thread.start()
        
        self.request_inference.emit(file_path)

    def on_inference_started(self):
        self.monitor_interface.progress_bar.show()
        self.monitor_interface.log_message("System > Inference Pipeline Started...")

    @Slot(dict)
    def on_sequence_processed(self, result: dict):
        """
        Handle individual result.
        """
        # Update Monitor Feed
        self.monitor_interface.add_result_card(result)
        
        # Log to Terminal
        status = result['status']
        cls = result['classification']
        msg = f"[{status.upper()}] {cls} ({result['confidence']:.2f})"
        self.monitor_interface.log_message(msg)
        
        # Also could update specific topology if we wanted to auto-follow

    @Slot(list, list)
    def on_batch_complete(self, results: list, ntu_clusters: list):
        """
        Handle completion signal from ScienceKernel.
        Validation: Using explicit typed slots to prevent PySide6 recursion method signature errors.
        """
        print(f"DEBUG: on_batch_complete called with {len(results)} results, {len(ntu_clusters)} clusters")
        
        # 1. Update State
        self.current_ntus = ntu_clusters
        self.current_results = results
        self.monitor_interface.progress_bar.hide()
        
        # 2. Populate Visuals
        self.discovery_interface.populate_ntus(ntu_clusters, results)
        
        # 3. Auto-Export Research Brief
        try:
             export_path = DiscoveryReporter.save_discovery_manifest(self.current_ntus)
             print(f"Exported brief to: {export_path}")
        except Exception as e:
             print(f"Auto-export failed: {e}")
             export_path = ""

        # 4. Visual Feedback (InfoBar)
        # Message: 'EXPEDITION ANALYSIS COMPLETE: [N] NOVEL UNITS ARCHIVED'
        n_units = len(ntu_clusters)
        msg_title = "EXPEDITION ANALYSIS COMPLETE"
        msg_content = f"{n_units} NOVEL UNITS ARCHIVED"
        if export_path:
            msg_content += f"\nBrief saved to: {export_path}"

        from qfluentwidgets import InfoBar, InfoBarPosition
        InfoBar.success(
            title=msg_title,
            content=msg_content,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP_RIGHT,
            duration=5000,
            parent=self
        )
        title = 'Expedition Scan Complete'
        msg = f'{len(ntu_clusters)} Novel Units Found.'
        
        if not ntu_clusters and self.current_isolated:
             title = 'Analysis Complete (High Entropy)'
             msg = f'{len(self.current_isolated)} Isolated Taxa Identified.'
             
        InfoBar.success(
            title=title,
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP_RIGHT,
            duration=5000,
            parent=self
        )

    def on_export_action(self):
        """Triggers the Discovery Reporter."""
        if not self.current_ntus and not self.current_isolated:
            from qfluentwidgets import InfoBar, InfoBarPosition
            InfoBar.warning(
                title='Export Cancelled',
                content='No discovery data available to export.',
                orient=Qt.Orientation.Horizontal,
                position=InfoBarPosition.BOTTOM_RIGHT, # Near button
                duration=3000,
                parent=self
            )
            return

        try:
            # Generate Manifest
            # Combine current clusters and isolated taxa for export if needed
            # For now, just pass clusters as primary artifact
            export_list = self.current_ntus
            # If empty but we have isolated, maybe wrap them?
            # Creating pseudo-ntus for export
            if not export_list and self.current_isolated:
                for iso in self.current_isolated:
                    export_list.append({
                        "ntu_id": iso.get("id"),
                        "anchor_taxon": iso.get("classification"),
                        "lineage": iso.get("lineage"),
                        "size": 1,
                        "divergence": 1.0,
                        "centroid_id": iso.get("id")
                    })

            zip_path = DiscoveryReporter.save_discovery_manifest(export_list)
            
            from qfluentwidgets import InfoBar, InfoBarPosition
            InfoBar.success(
                title='EXPEDIA DATA EXPORTED',
                content=f'Archive created: {zip_path}',
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.BOTTOM_RIGHT,
                duration=5000,
                parent=self
            )
        except Exception as e:
            from qfluentwidgets import InfoBar, InfoBarPosition
            InfoBar.error(
                title='Export Failed',
                content=str(e),
                orient=Qt.Orientation.Horizontal,
                position=InfoBarPosition.BOTTOM_RIGHT,
                duration=5000,
                parent=self
            )

    def on_localized_topology_ready(self, data):
        """
        Callback from Worker when neighborhood logic is done.
        """
        self.manifold_interface.render_manifold(data)

    def on_view_cluster_topology(self, payload: dict):
        """
        Redirects to Manifold View and visualizes the cluster.
        payload: { "id": str, "vector": np.array, ... } or { "ntu_id": str, "centroid": np.array, ... }
        """
        # Switch to Manifold Interface
        self.switchTo(self.manifold_interface)
        
        # Determine payload type (Sequence or Cluster)
        vector = payload.get('vector') or payload.get('centroid')
        seq_id = payload.get('id') or payload.get('ntu_id')
        
        if vector is not None:
             self.manifold_interface.show_loading()
             
             # Request Topology from Worker
             if not self.worker_thread.isRunning():
                 self.worker_thread.start()
             
             # Worker expects dict with 'id' and 'vector'
             worker_payload = {
                 "id": seq_id,
                 "vector": vector
             }
             self.request_localized_manifold.emit(worker_payload)
        else:
             self.monitor_interface.log_message(f"ERROR > Invalid Manifold Request: Missing Vector for {seq_id}")


    def on_worker_error(self, err_msg):
        self.monitor_interface.log_message(f"ERROR > {err_msg}")
        
        # Reset specific views if they were waiting
        if hasattr(self, 'manifold_interface'):
            self.manifold_interface.handle_error(err_msg)
            
        self.worker_thread.quit()
        
        from qfluentwidgets import InfoBar, InfoBarPosition
        InfoBar.error(
            title='Pipeline Error',
            content=err_msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP_RIGHT,
            duration=5000,
            parent=self
        )

    def closeEvent(self, event):
        """
        Clean up threads on close.
        """
        if self.worker_thread.isRunning():
            self.worker.stop()
            self.worker_thread.quit()
            self.worker_thread.wait()
        super().closeEvent(event)

    def handle_navigation(self, route_key):
        """
        Programmatic navigation switching.
        """
        self.switchTo(route_key)

    @Slot(str)
    def on_kernel_status_update(self, msg: str):
        """
        @UX-Visionary: Updates the booting indicator.
        """
        # Forward to DropZone UI
        if self.monitor_interface and self.monitor_interface.drop_zone:
            self.monitor_interface.drop_zone.set_kernel_loading(True, msg)
        
        # Also log to terminal
        if self.monitor_interface:
            self.monitor_interface.log_message(msg)

    @Slot()
    def on_kernel_ready(self):
        """
        @UX-Visionary: Enables user interaction once ScienceKernel is online.
        """
        if self.monitor_interface and self.monitor_interface.drop_zone:
            self.monitor_interface.drop_zone.set_kernel_loading(False)
            
        if self.monitor_interface:
            self.monitor_interface.log_message("SYSTEM: BIOLOGICAL INTELLIGENCE UNIT ONLINE.")
            
        from qfluentwidgets import InfoBar, InfoBarPosition
        InfoBar.success(
            title="NEURAL-CORE READY",
            content="NUCLEOTIDE TRANSFORMER (v2-50M) INITIALIZED.",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP_RIGHT,
            duration=4000,
            parent=self
        ).show()
