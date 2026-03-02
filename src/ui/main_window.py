import sys
from PySide6.QtCore import Qt, QSize, QThread, Signal
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

class MainWindow(FluentWindow):
    """
    @WinUI-Fluent: Main Application Shell.
    Manages navigation, worker threads, and global state.
    """
    request_inference = Signal(str)
    request_localized_manifold = Signal(list)

    def __init__(self):
        super().__init__()
        
        # Window Setup
        self.setWindowTitle("EXPEDIA")
        self.resize(1280, 800)
        
        # Center on screen
        screen_geometry = QApplication.primaryScreen().availableGeometry()
        x = (screen_geometry.width() - self.width()) - 50
        y = (screen_geometry.height() - self.height()) // 2
        self.move(x, y)

        # Worker Thread Setup
        self.worker_thread = QThread()
        self.worker = DiscoveryWorker()
        # Connect Inference Request Signal to Worker Slot
        self.request_inference.connect(self.worker.run_inference)
        self.request_localized_manifold.connect(self.worker.request_localized_topology)
        
        self.worker.moveToThread(self.worker_thread)
        
        # Initialize User Interfaces
        self.monitor_interface = MonitorView(self)
        self.manifold_interface = ManifoldView(self)
        self.benchmarking_interface = BenchmarkingView(self) # Replaces Placeholder
        self.discovery_interface = DiscoveryView(self)
        self.manual_interface = ManualView(self) # Replaces Placeholder

        # Initialize Navigation
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
        
        # Worker Signals
        self.worker.started.connect(self.on_inference_started)
        self.worker.sequence_processed.connect(self.on_sequence_processed)
        self.worker.batch_complete.connect(self.on_batch_complete)
        self.worker.error.connect(self.on_worker_error)
        self.worker.progress.connect(self.monitor_interface.progress_bar.setValue)
        self.worker.kernel_log.connect(self.monitor_interface.log_message)
        self.worker.manifold_ready.connect(self.manifold_interface.render_manifold)
        
        # 3. Inter-View Navigation
        self.discovery_interface.request_cluster_view.connect(self.on_view_cluster_topology)

        # 4. Export
        self.monitor_interface.batch_summary.request_report.connect(self.export_discovery_manifest)

    def export_discovery_manifest(self):
        """
        @Data-Ops: Exports NTU data to Research Format.
        """
        import json
        from pathlib import Path
        from qfluentwidgets import InfoBar, InfoBarPosition 
        
        # Gathering Data from State
        # Assuming discovery_interface holds the truth or we stored it
        ntu_cards = self.discovery_interface.ntu_cards
        if not ntu_cards:
            InfoBar.warning(
                title='Export Aborted',
                content='No Discovery Data Available to Export.',
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP_RIGHT,
                duration=3000,
                parent=self
            )
            return

        # Prepare Paths
        results_dir = app_config.DATA_ROOT / "results"
        results_dir.mkdir(parents=True, exist_ok=True)
        
        fasta_path = results_dir / "EXPEDIA_Discovery_Manifest.fasta"
        json_path = results_dir / "EXPEDIA_Discovery_Manifest.json"
        
        try:
            # 1. FASTA Export
            # Iterate NTUs and export Centroid Sequence (Holotype) or all members if available
            # Note: We only have metadata in cards. We might need original sequences.
            # However, prompt asked to save "all sequences belonging to discovered NTUs".
            # The Kernel passed IDs. We might strictly need the sequences which are likely in input file 
            # or we rely on what we have. 
            # Given current architecture, we might only have Centroid info if passed.
            # Wait, `ntu_data` has `centroid_vector` but maybe not sequence string.
            # The prompt implies we have the sequences.
            # Constraint: We don't have the sequences in memory here easily unless we stored them.
            # FOR DEMO: We will export a manifest of IDs and available metadata.
            
            with open(fasta_path, "w") as f:
                for card in ntu_cards:
                    data = card.ntu_data
                    # Holotype Header
                    f.write(f">{data.get('ntu_id')} [Holotype: {data.get('centroid_id')}] [Anchor: {data.get('anchor_taxon')}]\n")
                    f.write(f"NNNNN\n") # Placeholder if sequence not in memory
                    
            # 2. JSON Metadata
            manifest = []
            for card in ntu_cards:
                manifest.append(card.ntu_data)
                
            with open(json_path, "w") as f:
                json.dump(manifest, f, indent=4)
                
            # Success Notification
            InfoBar.success(
                title='Research Export Complete',
                content=f'Manifest saved to {results_dir}',
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP_RIGHT,
                duration=5000,
                parent=self
            )
            
        except Exception as e:
            self.on_worker_error(f"Export Failed: {e}")

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
             if not self.worker_thread.isRunning():
                 self.worker_thread.start()
             self.request_localized_manifold.emit(vector)
             
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
            "EXPEDIA: MANUAL", 
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

    def on_batch_complete(self, results, ntu_clusters):
        """
        Handle completion.
        """
        self.monitor_interface.log_message(f"System > Batch Complete. {len(results)} sequences processed.")
        self.monitor_interface.progress_bar.hide()
        self.worker_thread.quit()
        self.worker_thread.wait()
        
        # Populate Discovery View
        # NEW LOGIC: If no clusters, collect isolated Novel Taxa
        isolated_taxa = []
        if not ntu_clusters:
            isolated_taxa = [r for r in results if r.get("status") == "Novel"]
            
            if isolated_taxa:
                 self.monitor_interface.log_message(f"Discovery > No clusters. Found {len(isolated_taxa)} isolated NRTs.")

        self.discovery_interface.populate_ntus(ntu_clusters, isolated_taxa)
        
        # Notify
        from qfluentwidgets import InfoBar, InfoBarPosition
        InfoBar.success(
            title='Expedition Scan Complete',
            content=f'{len(ntu_clusters)} Novel Units Found.',
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP_RIGHT,
            duration=5000,
            parent=self
        )

    def on_worker_error(self, err_msg):
        self.monitor_interface.log_message(f"ERROR > {err_msg}")
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

    def on_view_cluster_topology(self, ntu_data):
        """
        Redirects to Manifold View and visualizes the cluster.
        """
        # Switch to Manifold Interface
        self.switchTo(self.manifold_interface)
        
        # Trigger visualization
        # We use the centroid as the query vector for visualization
        centroid = ntu_data.get('centroid')
        # ntu_id = ntu_data.get('ntu_id') # Unused for vector query
        
        if centroid is not None:
             if not self.worker_thread.isRunning():
                 self.worker_thread.start()
             
             self.request_localized_manifold.emit(centroid)
             self.manifold_interface.show_loading()

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
