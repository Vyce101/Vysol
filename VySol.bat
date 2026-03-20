@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

set "MIN_PYTHON_MINOR=10"
set "MIN_NODE_MAJOR=18"
set "PYTHON_INSTALL_ID=Python.Python.3.11"
set "NODE_INSTALL_ID=OpenJS.NodeJS.LTS"
set "BACKEND_VENV=backend\venv"
set "BACKEND_REQ=backend\requirements.txt"
set "BACKEND_REQ_STAMP=%BACKEND_VENV%\.requirements.sha256"
set "FRONTEND_DEPS_SOURCE=frontend\package-lock.json"
if not exist "%FRONTEND_DEPS_SOURCE%" set "FRONTEND_DEPS_SOURCE=frontend\package.json"
set "FRONTEND_DEPS_STAMP=frontend\node_modules\.deps.sha256"

echo ============================================
echo    VySol - Setup ^& Launch
echo ============================================
echo.

echo [1/6] Checking Python...
call :ensure_python
if errorlevel 1 goto :fail
echo       Using Python: %PYTHON_EXE%

echo [2/6] Checking Node.js and npm...
call :ensure_node
if errorlevel 1 goto :fail
echo       Using Node.js: %NODE_EXE%
echo       Using npm: %NPM_EXE%

echo [3/6] Preparing backend virtual environment...
call :ensure_backend_venv
if errorlevel 1 goto :fail

echo [4/6] Installing backend dependencies if needed...
call :ensure_backend_deps
if errorlevel 1 goto :fail

echo [5/6] Installing frontend dependencies if needed...
call :ensure_frontend_deps
if errorlevel 1 goto :fail

echo [6/6] Launching VySol...
echo.
echo       Backend:  http://localhost:8000
echo       Frontend: http://localhost:3000
echo.
echo       Press Ctrl+C to stop both servers.
echo       If ports 8000 or 3000 are already in use, stop those processes first.
echo ============================================
echo.

start "VySol Backend" cmd /k "cd backend && venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload"

timeout /t 3 /nobreak >nul
start "" "http://localhost:3000"

cd frontend
call "%NPM_EXE%" run dev
pause
exit /b 0

:ensure_python
set "PYTHON_EXE="
py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3, %MIN_PYTHON_MINOR%) else 1)" >nul 2>nul
if not errorlevel 1 (
    for /f "usebackq delims=" %%I in (`py -3 -c "import sys; print(sys.executable)"`) do set "PYTHON_EXE=%%I"
)
if not defined PYTHON_EXE (
    python -c "import sys; sys.exit(0 if sys.version_info >= (3, %MIN_PYTHON_MINOR%) else 1)" >nul 2>nul
    if not errorlevel 1 (
        for /f "usebackq delims=" %%I in (`python -c "import sys; print(sys.executable)"`) do set "PYTHON_EXE=%%I"
    )
)
if defined PYTHON_EXE exit /b 0

call :require_winget || exit /b 1
echo       No supported Python found. Attempting install via winget...
winget install --id %PYTHON_INSTALL_ID% -e --accept-package-agreements --accept-source-agreements
if errorlevel 1 (
    echo ERROR: Failed to install Python automatically. Install Python 3.%MIN_PYTHON_MINOR% or newer and re-run this script.
    exit /b 1
)
py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3, %MIN_PYTHON_MINOR%) else 1)" >nul 2>nul
if not errorlevel 1 (
    for /f "usebackq delims=" %%I in (`py -3 -c "import sys; print(sys.executable)"`) do set "PYTHON_EXE=%%I"
)
if not defined PYTHON_EXE (
    python -c "import sys; sys.exit(0 if sys.version_info >= (3, %MIN_PYTHON_MINOR%) else 1)" >nul 2>nul
    if not errorlevel 1 (
        for /f "usebackq delims=" %%I in (`python -c "import sys; print(sys.executable)"`) do set "PYTHON_EXE=%%I"
    )
)
if defined PYTHON_EXE exit /b 0
echo ERROR: Python installation finished, but no supported Python was detected in this session.
exit /b 1

:ensure_node
call :detect_node_tools
if not errorlevel 1 exit /b 0

call :require_winget || exit /b 1
echo       No supported Node.js found. Attempting install via winget...
winget install --id %NODE_INSTALL_ID% -e --accept-package-agreements --accept-source-agreements
if errorlevel 1 (
    echo ERROR: Failed to install Node.js automatically. Install Node.js %MIN_NODE_MAJOR% or newer and re-run this script.
    exit /b 1
)
call :detect_node_tools
if not errorlevel 1 exit /b 0

echo       Refreshing environment and retrying Node.js detection...
call :refresh_path_for_current_session
call :detect_node_tools
if not errorlevel 1 exit /b 0

echo ERROR: Node.js installation finished, but no supported Node.js/npm was detected in this session.
exit /b 1

:detect_node_tools
set "NODE_EXE="
set "NPM_EXE="
node -e "const major = parseInt(process.versions.node.split('.')[0], 10); process.exit(major >= %MIN_NODE_MAJOR% ? 0 : 1)" >nul 2>nul
if not errorlevel 1 (
    for /f "usebackq delims=" %%I in (`where node 2^>nul`) do (
        set "NODE_EXE=%%I"
        goto :node_path_found
    )
)
:node_path_found
for /f "usebackq delims=" %%I in (`where npm.cmd 2^>nul`) do (
    set "NPM_EXE=%%I"
    goto :npm_path_found
)
:npm_path_found
if defined NODE_EXE if defined NPM_EXE exit /b 0
exit /b 1

:refresh_path_for_current_session
set "MACHINE_PATH="
set "USER_PATH="
where refreshenv >nul 2>nul
if not errorlevel 1 (
    call refreshenv >nul 2>nul
)
for /f "tokens=2,*" %%A in ('reg query "HKLM\System\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul ^| findstr /R /I "Path"') do set "MACHINE_PATH=%%B"
for /f "tokens=2,*" %%A in ('reg query "HKCU\Environment" /v Path 2^>nul ^| findstr /R /I "Path"') do set "USER_PATH=%%B"
if defined USER_PATH if defined MACHINE_PATH (
    set "PATH=!USER_PATH!;!MACHINE_PATH!;%PATH%"
    exit /b 0
)
if defined USER_PATH (
    set "PATH=!USER_PATH!;%PATH%"
    exit /b 0
)
if defined MACHINE_PATH (
    set "PATH=!MACHINE_PATH!;%PATH%"
)
exit /b 0

:ensure_backend_venv
set "TARGET_VENV_MM="
set "CURRENT_VENV_MM="
for /f "usebackq delims=" %%I in (`"%PYTHON_EXE%" -c "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')"` ) do set "TARGET_VENV_MM=%%I"
if exist "%BACKEND_VENV%\pyvenv.cfg" (
    for /f "tokens=1,* delims==" %%A in ('findstr /b /c:"version = " "%BACKEND_VENV%\pyvenv.cfg"') do set "CURRENT_VENV_VERSION=%%B"
    set "CURRENT_VENV_VERSION=!CURRENT_VENV_VERSION: =!"
    for /f "tokens=1,2 delims=." %%A in ("!CURRENT_VENV_VERSION!") do set "CURRENT_VENV_MM=%%A.%%B"
    if /I not "!CURRENT_VENV_MM!"=="!TARGET_VENV_MM!" (
        echo       Existing venv uses !CURRENT_VENV_MM!, rebuilding for !TARGET_VENV_MM!...
        rmdir /s /q "%BACKEND_VENV%"
    )
)
if not exist "%BACKEND_VENV%\Scripts\python.exe" (
    echo       Creating backend virtual environment...
    "%PYTHON_EXE%" -m venv "%BACKEND_VENV%"
    if errorlevel 1 (
        echo ERROR: Failed to create backend virtual environment.
        exit /b 1
    )
    exit /b 0
)
echo       Existing backend virtual environment found.
exit /b 0

:ensure_backend_deps
set "BACKEND_HASH="
set "BACKEND_STORED_HASH="
for /f "skip=1 tokens=1" %%H in ('certutil -hashfile "%BACKEND_REQ%" SHA256 ^| findstr /R /I "^[0-9A-F][0-9A-F]"') do (
    set "BACKEND_HASH=%%H"
    goto :backend_hash_done
)
:backend_hash_done
if exist "%BACKEND_REQ_STAMP%" set /p BACKEND_STORED_HASH=<"%BACKEND_REQ_STAMP%"
if /I "%BACKEND_HASH%"=="%BACKEND_STORED_HASH%" (
    echo       Backend requirements unchanged, skipping pip install.
    exit /b 0
)
"%BACKEND_VENV%\Scripts\python.exe" -m pip install -r "%BACKEND_REQ%" --disable-pip-version-check --quiet
if errorlevel 1 (
    echo ERROR: pip install failed.
    exit /b 1
)
>"%BACKEND_REQ_STAMP%" echo %BACKEND_HASH%
echo       Backend dependencies ready.
exit /b 0

:ensure_frontend_deps
set "FRONTEND_HASH="
set "FRONTEND_STORED_HASH="
for /f "skip=1 tokens=1" %%H in ('certutil -hashfile "%FRONTEND_DEPS_SOURCE%" SHA256 ^| findstr /R /I "^[0-9A-F][0-9A-F]"') do (
    set "FRONTEND_HASH=%%H"
    goto :frontend_hash_done
)
:frontend_hash_done
if exist "%FRONTEND_DEPS_STAMP%" set /p FRONTEND_STORED_HASH=<"%FRONTEND_DEPS_STAMP%"
if exist "frontend\node_modules" if /I "%FRONTEND_HASH%"=="%FRONTEND_STORED_HASH%" (
    echo       Frontend dependencies unchanged, skipping npm install.
    exit /b 0
)
pushd frontend
call "%NPM_EXE%" install
if errorlevel 1 (
    popd
    echo ERROR: npm install failed.
    exit /b 1
)
popd
if not exist "frontend\node_modules" (
    echo ERROR: npm install finished but node_modules was not created.
    exit /b 1
)
>"%FRONTEND_DEPS_STAMP%" echo %FRONTEND_HASH%
echo       Frontend dependencies ready.
exit /b 0

:require_winget
where winget >nul 2>nul
if errorlevel 1 (
    echo ERROR: winget was not found. Install the missing prerequisite manually and re-run VySol.bat.
    exit /b 1
)
exit /b 0

:fail
echo.
echo Setup failed. Fix the error above and run VySol.bat again.
pause
exit /b 1
