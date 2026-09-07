@echo off
rem Copyright (C) 2024 Anson Phong
rem SPDX-License-Identifier: GPL-3.0-only
setlocal EnableExtensions DisableDelayedExpansion
cd /d "%~dp0" || exit /b 1
set "PYTHON=%~dp0.venv\Scripts\python.exe"

if exist "%~dp0.venv\bin" (
    1>&2 echo POST PULSAR: foreign POSIX .venv layout; review and remove it manually, then run setup-post-pulsar.bat.
    exit /b 2
)
if not exist "%PYTHON%" (
    1>&2 echo POST PULSAR is not set up. Run "%~dp0setup-post-pulsar.bat".
    exit /b 2
)

if /i "%~1"=="daemon" if "%~2"=="" (
    "%PYTHON%" -m post_pulsar daemon foreground
) else (
    "%PYTHON%" -m post_pulsar %*
)
set "POST_PULSAR_EXIT=%ERRORLEVEL%"
endlocal & exit /b %POST_PULSAR_EXIT%
