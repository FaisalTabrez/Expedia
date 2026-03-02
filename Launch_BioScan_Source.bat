@echo off
cd /d "%~dp0"
TITLE EXPEDIA v2.0 | SOURCE LAUNCHER
COLOR 0B
@echo off
cls
echo.
echo  ================================================================================
echo.
echo    [ D E E P B I O - S C A N   P R O   v 2 . 0 ]
echo    RECOVERING THE UNKNOWN BIOSPHERE | SOURCE MODE
echo.
echo    SYSTEM ARCHITECT: @BIOARCH-PRO
echo    UI ENGINE: FLUENT DESIGN SYSTEM
echo    INFERENCE CORE: NUCLEOTIDE TRANSFORMER (CPU-OPTIMIZED)
echo.
echo  ================================================================================
echo.

:: 1. Verify Environment
echo [SYSTEM] Checking Python Environment...
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [CRIT] Python not found in PATH.
    pause
    exit /b 1
)

:: 1.5. Configure Runtime Environment
set PYTHONPATH=%cd%
set PYTHONIOENCODING=utf-8
set QT_API=pyside6


:: 2. Verify Hardware Anchor (Volume E:)
if not exist "E:\DeepBio_Scan\data\db" (
    echo [WARN] Volume E: Database not detected. Running in DEMO MODE.
) else (
    echo [OK] VOLUME E: MOUNTED.
)

:: 3. Run Pre-flight Diagnostics
echo [SYSTEM] Running Pre-flight Diagnostics...
python -m src.core.preflight_diagnostics
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Diagnostics failed or incomplete. Proceeding with caution...
)

:: 4. Launch Application
echo [SYSTEM] Initializing Bioluminescent Abyss Inteface...
python main.py %*

if %ERRORLEVEL% NEQ 0 (
    echo [CRIT] Application crashed with code %ERRORLEVEL%.
    pause
)
pause
