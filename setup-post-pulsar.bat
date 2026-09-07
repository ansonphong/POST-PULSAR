@echo off
rem Copyright (C) 2024 Anson Phong
rem SPDX-License-Identifier: GPL-3.0-only
setlocal EnableExtensions DisableDelayedExpansion
cd /d "%~dp0" || exit /b 1
set "SCRIPT_DIR=%~dp0"
set "VENV_DIR=%SCRIPT_DIR%.venv"
set "PYTHON=%SCRIPT_DIR%.venv\Scripts\python.exe"
set "UV_VERSION=0.12.10"

if /i "%~1"=="--hardened-guide" goto hardened_guide
if not "%~1"=="" goto usage

if exist "%VENV_DIR%\bin" (
    1>&2 echo POST PULSAR setup: foreign POSIX layout found at "%VENV_DIR%".
    1>&2 echo It is never removed automatically; review it, then remove it manually and rerun setup.
    exit /b 2
)
if exist "%VENV_DIR%" if not exist "%PYTHON%" (
    1>&2 echo POST PULSAR setup: incomplete Windows environment at "%VENV_DIR%".
    1>&2 echo It is never removed automatically; review it, then remove it manually and rerun setup.
    exit /b 2
)

python -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)" >nul 2>&1
if errorlevel 1 (
    1>&2 echo POST PULSAR setup: Python 3.12 or newer is required on PATH.
    exit /b 1
)

if not exist "%PYTHON%" (
    python -m venv "%VENV_DIR%"
    if errorlevel 1 exit /b 1
)

"%PYTHON%" -m pip install --disable-pip-version-check "uv==%UV_VERSION%"
if errorlevel 1 exit /b 1
set "VIRTUAL_ENV=%VENV_DIR%"
"%PYTHON%" -m uv sync --frozen --active
if errorlevel 1 exit /b 1

echo POST PULSAR is installed in "%VENV_DIR%".
echo Simple same-user setup is a convenience mode; it is not an agent sandbox.
echo Run: "%SCRIPT_DIR%run-post-pulsar.bat" --help
echo Install user startup explicitly: "%SCRIPT_DIR%task-setup-post-pulsar.bat" install
echo Recommended separate-identity instructions: "%~f0" --hardened-guide
exit /b 0

:hardened_guide
if not "%~2"=="" goto usage
echo Recommended hardened separate-identity installation ^(validate and report only^)
echo.
echo This script does not create users, change ACLs, move content, or install/start a service.
echo Use distinct daemon and agent SIDs. Keep config, state, operator-verifier,
echo publishable QUEUE/RANDOM/REELS buckets, and every .ready marker daemon-only.
echo Grant the agent write access only to DRAFTS and read-only access to the
echo bootstrap, endpoint, and agent-capability records. Protected inheritance
echo must preserve read-only access when those records are atomically replaced.
echo Validate with icacls and a real agent logon/token before enabling a dedicated
echo Windows service. Set deployment_mode to hardened and bootstrap service mode
echo to manual: hardened mode disables MCP auto-start and daemon absence must
echo return operator setup guidance instead of crossing the identity boundary.
echo Do not use task-setup-post-pulsar.bat for hardened mode.
exit /b 0

:usage
1>&2 echo Usage: "%~f0" [--hardened-guide]
exit /b 2
