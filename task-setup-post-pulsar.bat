@echo off
rem Copyright (C) 2024 Anson Phong
rem SPDX-License-Identifier: GPL-3.0-only
setlocal EnableExtensions DisableDelayedExpansion
cd /d "%~dp0" || exit /b 1
set "TASK_NAME=PostPulsar"
set "LAUNCHER=%~dp0run-post-pulsar.bat"
set "TASK_ACTION=\"%LAUNCHER%\" daemon"
set "MODE=%~1"
set "ACTION=%~2"

if not "%~3"=="" goto usage
if not "%~4"=="" goto usage
if not "%~5"=="" goto usage
if not "%~6"=="" goto usage
if not "%~7"=="" goto usage
if not "%~8"=="" goto usage
if not "%~9"=="" goto usage
if /i "%MODE%"=="dry-run" goto dry-run
if not "%ACTION%"=="" goto usage
if /i "%MODE%"=="install" goto install
if /i "%MODE%"=="remove" goto remove
goto usage

:dry-run
if /i "%ACTION%"=="install" goto dry-run-install
if /i "%ACTION%"=="remove" goto dry-run-remove
goto usage

:dry-run-install
echo DRY RUN: schtasks /create /tn "%TASK_NAME%" /tr "%TASK_ACTION%" /sc ONLOGON /rl LIMITED /it /f
echo DRY RUN: existing task ownership would be checked before update.
exit /b 0

:dry-run-remove
echo DRY RUN: verify ownership, then schtasks /delete /tn "%TASK_NAME%" /f
exit /b 0

:install
if not exist "%LAUNCHER%" (
    1>&2 echo POST PULSAR launcher not found: "%LAUNCHER%"
    exit /b 2
)
set "TASK_XML=%TEMP%\PostPulsar-task-%RANDOM%-%RANDOM%.xml"
set "CREATE_FORCE="
call schtasks /query /tn "%TASK_NAME%" /xml >"%TASK_XML%" 2>nul
if errorlevel 1 goto install-create
findstr /i /l /c:"%LAUNCHER%" "%TASK_XML%" >nul
if errorlevel 1 goto install-unrelated
findstr /i /l /c:"daemon" "%TASK_XML%" >nul
if errorlevel 1 goto install-unrelated
set "CREATE_FORCE=/f"

:install-create
del /q "%TASK_XML%" >nul 2>&1
call schtasks /create /tn "%TASK_NAME%" /tr "%TASK_ACTION%" /sc ONLOGON /rl LIMITED /it %CREATE_FORCE%
if errorlevel 1 exit /b 1
echo Installed or updated user task "%TASK_NAME%". It was not started.
echo Bootstrap service selection: --service-mode windows-task --service-identifier %TASK_NAME%
exit /b 0

:install-unrelated
del /q "%TASK_XML%" >nul 2>&1
1>&2 echo Refusing to replace an unrelated task named "%TASK_NAME%".
exit /b 3

:remove
set "TASK_XML=%TEMP%\PostPulsar-task-%RANDOM%-%RANDOM%.xml"
call schtasks /query /tn "%TASK_NAME%" /xml >"%TASK_XML%" 2>nul
if errorlevel 1 (
    del /q "%TASK_XML%" >nul 2>&1
    echo POST PULSAR task is not installed.
    exit /b 0
)
findstr /i /l /c:"%LAUNCHER%" "%TASK_XML%" >nul
if errorlevel 1 (
    del /q "%TASK_XML%" >nul 2>&1
    1>&2 echo Refusing to remove an unrelated task named "%TASK_NAME%".
    exit /b 3
)
del /q "%TASK_XML%" >nul 2>&1
call schtasks /delete /tn "%TASK_NAME%" /f
if errorlevel 1 exit /b 1
echo Removed POST PULSAR background startup. No process was stopped.
exit /b 0

:usage
1>&2 echo Usage: "%~f0" ^<install^|remove^|dry-run install^|dry-run remove^>
exit /b 2
