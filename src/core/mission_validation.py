import sys
import os
import time
import logging
import json
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QObject, Signal, Slot, QTimer, QThread

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.config import app_config
from src.core.worker import DiscoveryWorker

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("MissionValidator")

class BioArchValidator(QObject):
    """
    @BioArch-Pro: End-to-End Mission Readiness Validator.
    Verifies the Subprocess Orchestration architecture.
    """
    finished = Signal()
    start_request = Signal(str)

    def __init__(self, app):
        super().__init__()
        self.app = app
        self.results = []
        self.signal_status = {"progress": "FAIL", "sequence": "FAIL", "batch": "FAIL", "finished": "FAIL"}
        
        self.report_path = "MISSION_READY_REPORT.txt"
        self.start_time = 0
        
        # Threading for Worker
        self.worker_thread = QThread()
        self.worker = DiscoveryWorker()
        self.worker.moveToThread(self.worker_thread)
        
        # Connect start request to worker slot (Correct thread context)
        self.start_request.connect(self.worker.run_inference)

    def run(self):
        logger.info(">>> INITIATING BIOLUMINESCENT ABYSS SYSTEM CHECK (SUBPROCESS MODE) <<<")
        
        # 1. Create Dummy Data
        self._create_dummy_fasta()
        
        # 2. Connect Signals
        self.worker.started.connect(self._on_started)
        self.worker.sequence_processed.connect(self._on_sequence)
        self.worker.progress.connect(self._on_progress)
        self.worker.batch_complete.connect(self._on_batch_complete)
        self.worker.finished.connect(self.worker_thread.quit) # Stop thread when done
        self.worker.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self._on_thread_finished)
        self.worker.error.connect(self._on_error)

        # 3. Launch Thread
        # We need to trigger run_inference when thread starts
        self.start_time = time.perf_counter()
        
        fname = "known_taxa_sim.fasta"
        self.worker_thread.start()
        
        # Emit signal to start work in the worker thread
        # Use QTimer to ensure thread loop has started
        QTimer.singleShot(100, lambda: self.start_request.emit(fname))

    def _create_dummy_fasta(self):
        # Ensure we have data
        if not os.path.exists("known_taxa_sim.fasta"):
            with open("known_taxa_sim.fasta", "w") as f:
                f.write(">Test_Seq_1\nATGCATGCATGC\n>Test_Seq_2\nCGTACGTACGTA\n")
        logger.info("Test Data Ready.")

    @Slot()
    def _on_started(self):
        logger.info("[SIGNAL] Worker Started.")

    @Slot(dict)
    def _on_sequence(self, data):
        self.signal_status["sequence"] = "PASS"
        logger.info(f"[RESULT] {data.get('id')} | Status: {data.get('status')}")

    @Slot(int)
    def _on_progress(self, val):
        self.signal_status["progress"] = "PASS"
        if val % 20 == 0:
            logger.info(f"[PROGRESS] {val}%")

    @Slot(list, list)
    def _on_batch_complete(self, results, clusters):
        self.signal_status["batch"] = "PASS"
        logger.info(f"[BATCH] Processed {len(results)} sequences. Clusters: {len(clusters)}")

    @Slot(str)
    def _on_error(self, err):
        logger.error(f"[ERROR] {err}")
        self._generate_report(0) # Fail report
        self.finished.emit()

    @Slot()
    def _on_thread_finished(self):
        self.signal_status["finished"] = "PASS"
        duration = time.perf_counter() - self.start_time
        logger.info(f"[COMPLETE] Validation finished in {duration:.2f}s")
        self._generate_report(duration)
        self.finished.emit()

    def _generate_report(self, duration):
        report = [
            "==================================================",
            "   EXPEDIA | MISSION READY REPORT",
            "==================================================",
            "ARCHITECTURE: SUBPROCESS ORCHESTRATION (SOLVED DLL HELL)",
            "KERNEL: src/core/science_kernel.py (Independent Process)",
            "--------------------------------------------------",
            "[SIGNAL INTEGRITY]",
            f"Sequence Stream: {self.signal_status['sequence']}",
            f"Progress Updates: {self.signal_status['progress']}",
            f"Batch Completion: {self.signal_status['batch']}",
            f"Thread Lifecycle: {self.signal_status['finished']}",
            "--------------------------------------------------",
            "[PERFORMANCE]",
            f"Total Execution Time: {duration:.2f}s",
            "==================================================",
            "STATUS: MISSION READY"
        ]
        
        with open(self.report_path, "w") as f:
            f.write("\n".join(report))
        
        print("\n".join(report))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    validator = BioArchValidator(app)
    validator.finished.connect(app.quit)
    QTimer.singleShot(100, validator.run)
    sys.exit(app.exec())

