@echo off
rem Copyright (C) 2024 Anson Phong
rem SPDX-License-Identifier: GPL-3.0-only
setlocal EnableExtensions DisableDelayedExpansion
cd /d "%~dp0" || exit /b 1
set "TASK_NAME=PostPulsar"
set "LAUNCHER=%~dp0run-post-pulsar.bat"
set "TASK_ACTION=\"%LAUNCHER%\" daemon"
set "MODE=%~1"
shift /1

if /i "%MODE%"=="dry-run" goto parse-dry-run
rem Test the raw argument so even an explicit empty trailing argument is rejected.
if not [%1]==[] goto usage
if /i "%MODE%"=="install" goto install
if /i "%MODE%"=="remove" goto remove
goto usage

:parse-dry-run
set "ACTION=%~1"
shift /1
if not [%1]==[] goto usage
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
call :verify-ownership
if errorlevel 1 goto install-unrelated
set "CREATE_FORCE=/f"

:install-create
del /q "%TASK_XML%" >nul 2>&1
rem On query failure, omit /f and close stdin: an existing-task prompt cannot be accepted.
call schtasks /create /tn "%TASK_NAME%" /tr "%TASK_ACTION%" /sc ONLOGON /rl LIMITED /it %CREATE_FORCE% <nul
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
call :verify-ownership
if errorlevel 1 goto remove-unrelated
del /q "%TASK_XML%" >nul 2>&1
call schtasks /delete /tn "%TASK_NAME%" /f
if errorlevel 1 exit /b 1
echo Removed POST PULSAR background startup. No process was stopped.
exit /b 0

:remove-unrelated
del /q "%TASK_XML%" >nul 2>&1
1>&2 echo Refusing to remove an unrelated task named "%TASK_NAME%".
exit /b 3

:verify-ownership
rem Parse task XML without DTDs or external resources. Only one exact Exec action is owned.
rem Environment variables carry paths as data, never interpolated PowerShell source.
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoLogo -NoProfile -NonInteractive -Command ^
 "$ErrorActionPreference='Stop'; try {" ^
 "$settings=[System.Xml.XmlReaderSettings]::new();" ^
 "$settings.DtdProcessing=[System.Xml.DtdProcessing]::Prohibit; $settings.XmlResolver=$null;" ^
 "$reader=[System.Xml.XmlReader]::Create($env:TASK_XML,$settings);" ^
 "try { $xml=[System.Xml.XmlDocument]::new(); $xml.XmlResolver=$null; $xml.Load($reader) } finally { $reader.Dispose() };" ^
 "$ns=[System.Xml.XmlNamespaceManager]::new($xml.NameTable);" ^
 "$uri='http://schemas.microsoft.com/windows/2004/02/mit/task'; $ns.AddNamespace('t',$uri);" ^
 "$groups=$xml.SelectNodes('/t:Task/t:Actions',$ns); if ($groups.Count -ne 1) { exit 1 };" ^
 "$actions=$groups[0].SelectNodes('*');" ^
 "if ($actions.Count -ne 1 -or $actions[0].LocalName -cne 'Exec' -or $actions[0].NamespaceURI -cne $uri) { exit 1 };" ^
 "$commands=$actions[0].SelectNodes('t:Command',$ns); $arguments=$actions[0].SelectNodes('t:Arguments',$ns);" ^
 "if ($commands.Count -ne 1 -or $arguments.Count -ne 1) { exit 1 };" ^
 "if ($commands[0].SelectNodes('*').Count -ne 0 -or $arguments[0].SelectNodes('*').Count -ne 0 -or $arguments[0].InnerText -cne 'daemon') { exit 1 };" ^
 "$command=$commands[0].InnerText;" ^
 "if ($command.Length -ge 2 -and $command[0] -eq [char]34 -and $command[$command.Length-1] -eq [char]34) { $command=$command.Substring(1,$command.Length-2) };" ^
 "if (-not [System.IO.Path]::IsPathRooted($command)) { exit 1 };" ^
 "$actual=[System.IO.Path]::GetFullPath($command); $expected=[System.IO.Path]::GetFullPath($env:LAUNCHER);" ^
 "if (-not [System.StringComparer]::OrdinalIgnoreCase.Equals($actual,$expected)) { exit 1 };" ^
 "exit 0 } catch { exit 1 }"
exit /b %errorlevel%

:usage
1>&2 echo Usage: "%~f0" ^<install^|remove^|dry-run install^|dry-run remove^>
exit /b 2
