import sys
import os
import time
from PySide6.QtWidgets import QApplication, QSplashScreen
from PySide6.QtGui import QColor, QFont, QPixmap, QPainter, QPen
from PySide6.QtCore import Qt

from qfluentwidgets import setTheme, Theme, setThemeColor, qconfig

# Add src to python path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.ui.main_window import MainWindow
from src.config import app_config

def apply_abyss_theme(app):
    """
    @WinUI-Fluent: Injects the Bioluminescent Abyss Theme.
    """
    # 1. Set Global Dark Theme
    setTheme(Theme.DARK)
    
    # 2. Set Primary Accent Color (Cyan #00E5FF)
    # qfluentwidgets helper to set accent for components
    setThemeColor(QColor(app_config.THEME_COLORS["primary"]))

    # 3. Custom QSS for Background and Specific Overrides
    # Force the main window background to Abyss Dark #0A0F1E
    # And ensure text contrast
    
    abyss_qss = f"""
    * {{
        font-family: "Segoe UI";
        font-size: 12px;
    }}
    Window {{
        background-color: {app_config.THEME_COLORS["background"]};
        color: white;
    }}
    FluentWindow {{
        background-color: {app_config.THEME_COLORS["background"]};
    }}
    stackedWidget {{
        background-color: {app_config.THEME_COLORS["background"]};
        border: none;
    }}
    /* Navigation Panel Override */
    NavigationInterface {{
        background-color: {app_config.THEME_COLORS["sidebar"]};
        border-right: 1px solid {app_config.THEME_COLORS["border"]};
    }}
    """
    
    app.setStyleSheet(app.styleSheet() + abyss_qss)
    
    # Set default font
    font = QFont("Segoe UI", 10)
    app.setFont(font)

def main():
    # Enable High DPI scaling (Handled by Default in Qt6, but setting env vars for safety)
    os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "0"
    os.environ["QT_SCALE_FACTOR"] = "1.25"
    
    app = QApplication(sys.argv)
    # Qt.AA_UseHighDpiPixmaps is removed in Qt6 (Always Enabled)
    
    # -------------------------------------------------------------------------
    # 0. SPLASH SCREEN (Visual Feedback)
    # -------------------------------------------------------------------------
    # Create a programmatic splash screen since assets might be missing
    splash_pix = QPixmap(600, 400)
    splash_pix.fill(QColor(app_config.THEME_COLORS["background"])) # True Black

    painter = QPainter(splash_pix)
    painter.setPen(QPen(QColor(app_config.THEME_COLORS["primary"]), 2)) # Windows Blue
    painter.setFont(QFont("Segoe UI", 24, QFont.Weight.Bold))
    
    # Draw Title
    painter.drawText(splash_pix.rect(), Qt.AlignmentFlag.AlignCenter, "EXPEDIA\nPRECISION GENOMIC ANALYTIC ENGINE")
    
    # Draw Loading Text
    painter.setFont(QFont("Consolas", 10))
    painter.setPen(QPen(QColor("#888888")))
    painter.drawText(50, 350, "INITIALIZING NEURAL CORE...")
    
    painter.end()

    splash = QSplashScreen(splash_pix, Qt.WindowType.WindowStaysOnTopHint)
    splash.show()
    app.processEvents() # Ensure splash shows immediately
    
    # Apply Theme
    splash.showMessage("APPLYING FLUENT THEME...", Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignLeft, QColor(app_config.THEME_COLORS["primary"]))
    apply_abyss_theme(app)
    time.sleep(0.5) # Simulate load for visual feedback
    
    # -------------------------------------------------------------------------
    # HARDWARE ANCHOR VERIFICATION
    # -------------------------------------------------------------------------
    splash.showMessage("VERIFYING HARDWARE ANCHORS...", Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignLeft, QColor("#00E5FF"))
    app.processEvents()
    
    is_ready, msg = app_config.verify_auxiliaries()
    
    if not is_ready:
        splash.hide() # Hide splash to show error clearly
        
        # Using standard QMessageBox for critical boot error
        from PySide6.QtWidgets import QMessageBox
        
        error_box = QMessageBox()
        error_box.setIcon(QMessageBox.Icon.Critical)
        error_box.setWindowTitle("HARDWARE ERROR")
        error_box.setText("CRITICAL HARDWARE DISCONNECT")
        error_box.setInformativeText(msg)
        error_box.setStandardButtons(QMessageBox.StandardButton.Ok)
        error_box.exec()
        
        sys.exit(1)
    
    # Init Window
    splash.showMessage("LAUNCHING ABYSS INTERFACE...", Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignLeft, QColor("#00E5FF"))
    app.processEvents()
    
    window = MainWindow()
    window.show()
    
    # Close Splash
    splash.finish(window)
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
