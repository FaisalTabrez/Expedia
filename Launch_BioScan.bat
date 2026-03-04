@echo off
cd /d "%~dp0"
TITLE EXPEDIA v2.0 | RECOVERING THE UNKNOWN BIOSPHERE | SOURCE MODE
COLOR 0B

:: -----------------------------------------------------------------------------
:: HIGH-TECH ASCII HEADER & BOOTLOADER
:: @BioArch-Pro
:: -----------------------------------------------------------------------------
cls
echo.
echo  ================================================================================
echo.
echo    [ D E E P B I O - S C A N   P R O   v 2 . 0 ]
echo    RECOVERING THE UNKNOWN BIOSPHERE
echo.
echo    SYSTEM ARCHITECT: @BIOARCH-PRO
echo    UI ENGINE: FLUENT DESIGN SYSTEM
echo    INFERENCE CORE: NUCLEOTIDE TRANSFORMER (CPU-OPTIMIZED)
echo.
echo  ================================================================================
echo.

:: -----------------------------------------------------------------------------
:: ENVIRONMENT CONFIGURATION
:: -----------------------------------------------------------------------------

echo [SYSTEM] Configuring Runtime Environment...

:: Python Environment Check
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [CRIT] Python not found in PATH.
    echo Please install Python 3.10+ and add to PATH.
    pause
    exit /b 1
)

:: Set Source Python Path logic (Crucial for module resolution)
set PYTHONPATH=%cd%
set PYTHONIOENCODING=utf-8
set QT_API=pyside6

:: UI Scaling & Theme
set QT_FONT_DPI=96
set QT_SCALE_FACTOR=1.25
set QT_ENABLE_HIGHDPI_SCALING=0
set QT_QPA_PLATFORM=windows:darkmode=2

:: -----------------------------------------------------------------------------
:: HARDWARE CHECK
:: -----------------------------------------------------------------------------

echo [SYSTEM] Verifying Hardware Anchor (Volume E:)...
if exist "E:\" (
    echo [OK] VOLUME E: MOUNTED.
) else (
    echo [WARN] VOLUME E: NOT DETECTED.
    echo [INFO] System will attempt to use C:/EXPEDIA_Data fallback or local resources.
)

:: -----------------------------------------------------------------------------
:: EXECUTION STRATEGY
:: -----------------------------------------------------------------------------

:: 1. Check for Compiled Distribution (If built)
if exist "dist\DeepBioScan\DeepBioScan.exe" (
    echo [SYSTEM] Found Compiled Distribution. Launching binary...
    start "" "dist\DeepBioScan\DeepBioScan.exe"
    exit
)

:: 2. Fallback to Source Execution
echo [SYSTEM] Binary not found. Initializing Source Mode...

:: Run Pre-Flight Diagnostics
:: (Assuming this module exists based on previous file analysis)
if exist src\core\preflight_diagnostics.py (
    echo [SYSTEM] Running Pre-flight Diagnostics...
    python -m src.core.preflight_diagnostics
    if %ERRORLEVEL% NEQ 0 (
        echo [WARN] Diagnostics reported issues. Review logs before proceeding.
    )
)

echo [SYSTEM] Initializing Bioluminescent Abyss Interface...
python main.py %*

if %ERRORLEVEL% NEQ 0 (
    echo [CRIT] Application crashed with code %ERRORLEVEL%.
    pause
)

pause
