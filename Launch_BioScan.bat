@echo off
TITLE EXPEDIA v2.0 | RECOVERING THE UNKNOWN BIOSPHERE
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

:: Ensure High-DPI Scaling for Lab Monitors
set QT_FONT_DPI=96
set QT_SCALE_FACTOR=1.25
set QT_ENABLE_HIGHDPI_SCALING=0

:: Force Dark Mode preferences for Qt
set QT_QPA_PLATFORM=windows:darkmode=2

:: -----------------------------------------------------------------------------
:: EXECUTION LAUNCHER
:: -----------------------------------------------------------------------------

echo  [SYSTEM] VERIFYING VOLUME E: CONNECTION...
if exist "E:\" (
    echo  [OK] VOLUME E: MOUNTED.
) else (
    echo  [WARNING] VOLUME E: NOT FOUND. USING FALLBACK STORAGE C:.
)

echo  [SYSTEM] INITIALIZING UI THREADS...
echo.

:: Check for Dist bundle first, else run python source
if exist "dist\DeepBioScan\DeepBioScan.exe" (
    start "" "dist\DeepBioScan\DeepBioScan.exe"
) else (
    echo  [DEV_MODE] RUNNING FROM SOURCE...
    python main.py
)

exit
