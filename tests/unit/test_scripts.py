"""Operational launcher, setup, service, and identity-boundary contracts."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from xml.sax.saxutils import escape

import pytest

ROOT = Path(__file__).resolve().parents[2]
LEGACY_PATHS = (
    "setup.sh",
    "setup.bat",
    "run-bot.sh",
    "run-bot.bat",
    "task-setup.bat",
    "venv.bat",
    "phong-bot.py",
    "post_base.py",
    "post_x.py",
    "post_instagram.py",
    "requirements.txt",
    "config-sample.json",
    "update_config.py",
)
NEW_PATHS = (
    "setup-post-pulsar.sh",
    "setup-post-pulsar.bat",
    "run-post-pulsar.sh",
    "run-post-pulsar.bat",
    "task-setup-post-pulsar.bat",
    "service/post-pulsar.service",
    "service/com.post-pulsar.daemon.plist",
)


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _windows_path(path: Path) -> str:
    if os.name == "nt":
        return str(path)
    return subprocess.run(
        ["wslpath", "-w", str(path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _run_batch(
    path: Path,
    *arguments: str,
    dry_run: bool = False,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
):
    run_env = dict(os.environ if env is None else env)
    if dry_run:
        run_env["POST_PULSAR_DRY_RUN"] = "1"
        if os.name != "nt":
            names = run_env.get("WSLENV", "")
            entry = "POST_PULSAR_DRY_RUN/w"
            run_env["WSLENV"] = f"{names}:{entry}" if names else entry
    # Resolve the command before every batch invocation. Never probe an
    # unverified schtasks: that could reach the actual Windows scheduler.
    command_processor = shutil.which("cmd.exe")
    assert command_processor is not None
    expected_stub = _windows_path(Path(run_env["POST_PULSAR_SCHTASKS_STUB"]))
    resolution = subprocess.run(
        [command_processor, "/d", "/c", "where", "schtasks"],
        cwd=path.parent,
        env=run_env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert resolution.returncode == 0, resolution.stderr
    assert resolution.stdout.splitlines()[0].casefold() == expected_stub.casefold()
    probe = subprocess.run(
        [command_processor, "/d", "/c", "call", "schtasks", "/post-pulsar-stub-probe"],
        cwd=path.parent,
        env=run_env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert probe.returncode == 0
    assert probe.stdout.strip() == "POST_PULSAR_SCHTASKS_STUB_PROBE"
    return subprocess.run(
        [command_processor, "/d", "/c", "call", _windows_path(path), *arguments],
        cwd=path.parent,
        env=run_env,
        capture_output=True,
        text=True,
        input=input_text,
        timeout=30,
        check=False,
    )


def _stubbed_schtasks_env(stub_dir: Path, marker: Path) -> dict[str, str]:
    """Model schtasks collision prompts without touching Task Scheduler."""
    stub = stub_dir / "schtasks.bat"
    stub_dir.mkdir()
    stub.write_text(
        "@echo off\n"
        "setlocal EnableExtensions DisableDelayedExpansion\n"
        'if /i "%~1"=="/post-pulsar-stub-probe" (\n'
        "    echo POST_PULSAR_SCHTASKS_STUB_PROBE\n"
        "    exit /b 0\n"
        ")\n"
        '>> "%POST_PULSAR_SCHTASKS_LOG%" echo %*\n'
        'if /i "%~1"=="/query" goto query\n'
        'if /i "%~1"=="/create" goto create\n'
        'if /i "%~1"=="/delete" goto delete\n'
        "exit /b 99\n"
        ":query\n"
        'if not "%POST_PULSAR_QUERY_EXIT%"=="0" exit /b %POST_PULSAR_QUERY_EXIT%\n'
        'type "%POST_PULSAR_SCHTASKS_XML%"\n'
        "exit /b 0\n"
        ":create\n"
        'if not exist "%POST_PULSAR_SCHTASKS_MARKER%" goto write-task\n'
        'echo %* | findstr /i /r /c:" /f " /c:" /f$" >nul\n'
        "if not errorlevel 1 goto write-task\n"
        'set "REPLACE="\n'
        'set /p "REPLACE=WARNING: task already exists. Replace (Y/N)? "\n'
        'if /i "%REPLACE%"=="Y" goto write-task\n'
        "exit /b 1\n"
        ":write-task\n"
        '> "%POST_PULSAR_SCHTASKS_MARKER%" echo recorded daemon task\n'
        "exit /b 0\n"
        ":delete\n"
        'del /q "%POST_PULSAR_SCHTASKS_MARKER%"\n'
        "exit /b 0\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["POST_PULSAR_SCHTASKS_MARKER"] = str(marker)
    env["POST_PULSAR_SCHTASKS_STUB"] = str(stub)
    env["POST_PULSAR_SCHTASKS_LOG"] = str(marker.with_suffix(".calls"))
    env["POST_PULSAR_SCHTASKS_XML"] = str(marker.with_suffix(".xml"))
    env["POST_PULSAR_QUERY_EXIT"] = "5"
    env["PATH"] = f"{stub_dir}{os.pathsep}{env['PATH']}"
    names = env.get("WSLENV", "")
    entries = [entry for entry in names.split(":") if entry]
    entries.extend(
        (
            "POST_PULSAR_SCHTASKS_MARKER/p",
            "POST_PULSAR_SCHTASKS_LOG/p",
            "POST_PULSAR_SCHTASKS_XML/p",
            "POST_PULSAR_QUERY_EXIT/w",
            "PATH/l",
        )
    )
    env["WSLENV"] = ":".join(entries)
    return env


@pytest.fixture
def windows_task():
    with tempfile.TemporaryDirectory(
        prefix="post pulsar task ownership ", dir=ROOT.parent
    ) as raw:
        install = Path(raw)
        task = install / "task-setup-post-pulsar.bat"
        shutil.copy2(ROOT / task.name, task)
        shutil.copy2(ROOT / "run-post-pulsar.bat", install / "run-post-pulsar.bat")
        marker = install / "task-ownership.txt"
        marker.write_text("existing task ownership\n", encoding="utf-8")
        env = _stubbed_schtasks_env(install / "scheduler-shim", marker)
        yield task, marker, env


def _task_xml(launcher: Path, case: str) -> str:
    command = escape(_windows_path(launcher))
    action = f"<Exec><Command>{command}</Command><Arguments>daemon</Arguments></Exec>"
    description = "POST PULSAR"
    if case == "normalized":
        command = escape(
            _windows_path(launcher)
            .replace(launcher.name, f".\\{launcher.name}")
            .upper()
        )
        action = f"<Exec><Command>&quot;{command}&quot;</Command><Arguments>daemon</Arguments></Exec>"
    elif case == "misleading":
        description = f"{command} daemon"
        action = "<Exec><Command>C:\\unrelated.bat</Command><Arguments>status</Arguments></Exec>"
    elif case == "extra-arguments":
        action = action.replace(
            "<Arguments>daemon</Arguments>", "<Arguments>daemon run-now</Arguments>"
        )
    elif case == "missing-arguments":
        action = action.replace("<Arguments>daemon</Arguments>", "")
    elif case == "multiple-exec":
        action += action
    elif case == "other-action":
        description = f"{command} daemon"
        action = "<ComHandler><ClassId>unrelated</ClassId></ComHandler>"
    elif case == "conflicting-action":
        action += "<ComHandler><ClassId>unrelated</ClassId></ComHandler>"
    elif case == "malformed":
        return f"<Task><Description>{command} daemon</Description><Actions>"
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">'
        f"<RegistrationInfo><Description>{description}</Description></RegistrationInfo>"
        f'<Actions Context="Author">{action}</Actions></Task>'
    )


def test_cutover_has_only_post_pulsar_operational_paths() -> None:
    assert all(not (ROOT / path).exists() for path in LEGACY_PATHS)
    assert all((ROOT / path).is_file() for path in NEW_PATHS)

    active_text = "\n".join(_read(path).lower() for path in NEW_PATHS)
    for stale_name in ("phong-bot", "phong_bot", "run-bot", "/root/"):
        assert stale_name not in active_text


def test_posix_setup_is_local_frozen_non_root_and_guarded() -> None:
    script = _read("setup-post-pulsar.sh")
    assert "SCRIPT_DIR=" in script
    assert 'PYTHON="$VENV_DIR/bin/python"' in script
    assert '"$PYTHON"' in script
    assert "uv==0.12.10" in script
    assert "uv sync --frozen" in script
    assert all(word not in script for word in ("apt-get", "dnf ", "brew install"))
    assert "foreign Windows layout" in script
    assert "never removed automatically" in script
    assert "convenience mode; it is not an agent sandbox" in script


def test_posix_launcher_resolves_spaces_forwards_args_and_returns_exit() -> None:
    with tempfile.TemporaryDirectory(
        prefix="post pulsar launcher ", dir=ROOT.parent
    ) as raw:
        install = Path(raw)
        launcher = install / "run-post-pulsar.sh"
        shutil.copy2(ROOT / "run-post-pulsar.sh", launcher)
        interpreter = install / ".venv/bin/python"
        interpreter.parent.mkdir(parents=True)
        capture = install / "invocation.txt"
        interpreter.write_text(
            '#!/bin/sh\nprintf \'%s\\n\' "$PWD|$*" > "$POST_PULSAR_CAPTURE"\nexit 37\n',
            encoding="utf-8",
        )
        interpreter.chmod(0o755)
        result = subprocess.run(
            [launcher, "status", "--profile", "profile-one"],
            cwd=ROOT.parent,
            env={**os.environ, "POST_PULSAR_CAPTURE": str(capture)},
            check=False,
        )
        assert result.returncode == 37
        assert capture.read_text(encoding="utf-8").strip() == (
            f"{install}|-m post_pulsar status --profile profile-one"
        )


def test_posix_launcher_maps_daemon_to_foreground() -> None:
    script = _read("run-post-pulsar.sh")
    assert '"daemon" "foreground"' in script
    assert 'exec "$PYTHON" -m post_pulsar' in script


def test_posix_setup_rejects_windows_venv_without_deleting_it() -> None:
    with tempfile.TemporaryDirectory(
        prefix="post pulsar posix setup ", dir=ROOT.parent
    ) as raw:
        install = Path(raw)
        shutil.copy2(ROOT / "setup-post-pulsar.sh", install / "setup-post-pulsar.sh")
        foreign_python = install / ".venv/Scripts/python.exe"
        foreign_python.parent.mkdir(parents=True)
        foreign_python.write_bytes(b"foreign-layout")
        result = subprocess.run(
            ["bash", str(install / "setup-post-pulsar.sh")],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 2
        assert "foreign Windows layout" in result.stderr
        assert "remove it manually" in result.stderr
        assert foreign_python.read_bytes() == b"foreign-layout"


def test_service_templates_run_only_the_foreground_daemon() -> None:
    unit = _read("service/post-pulsar.service")
    plist = _read("service/com.post-pulsar.daemon.plist")
    assert 'ExecStart="@POST_PULSAR_ROOT@/run-post-pulsar.sh" daemon' in unit
    assert "@POST_PULSAR_ROOT@/run-post-pulsar.sh" in plist
    assert "<string>daemon</string>" in plist
    combined = f"{unit}\n{plist}"
    assert "run-now" not in combined
    assert "post-pulsar run " not in combined
    assert "OnCalendar" not in combined


def test_hardened_guide_preserves_the_separate_identity_boundary() -> None:
    combined = _read("setup-post-pulsar.sh") + _read("setup-post-pulsar.bat")
    for contract in (
        "DRAFTS",
        "bootstrap",
        "endpoint",
        "agent-capability",
        "operator-verifier",
        "QUEUE",
        "RANDOM",
        "REELS",
        ".ready",
        "MCP auto-start",
    ):
        assert contract in combined
    for mutation in ("useradd ", "adduser ", "net user "):
        assert mutation not in combined.lower()
    assert "validate and report only" in combined


@pytest.mark.skipif(
    shutil.which("cmd.exe") is None,
    reason="Windows command processor is unavailable",
)
def test_windows_setup_rejects_posix_venv_without_deleting_it() -> None:
    with tempfile.TemporaryDirectory(
        prefix="post pulsar windows setup ", dir=ROOT.parent
    ) as raw:
        install = Path(raw)
        setup = install / "setup-post-pulsar.bat"
        shutil.copy2(ROOT / "setup-post-pulsar.bat", setup)
        foreign_python = install / ".venv/bin/python"
        foreign_python.parent.mkdir(parents=True)
        foreign_python.write_bytes(b"foreign-layout")
        env = _stubbed_schtasks_env(
            install / "scheduler-shim", install / "scheduler.txt"
        )
        result = _run_batch(setup, env=env)
        output = result.stdout + result.stderr
        assert result.returncode == 2
        assert "foreign POSIX layout" in output
        assert "remove it manually" in output
        assert foreign_python.read_bytes() == b"foreign-layout"


def test_windows_scripts_quote_paths_and_task_is_daemon_only() -> None:
    setup = _read("setup-post-pulsar.bat")
    launcher = _read("run-post-pulsar.bat")
    task = _read("task-setup-post-pulsar.bat")
    assert 'cd /d "%~dp0"' in setup
    assert 'set "PYTHON=%SCRIPT_DIR%.venv\\Scripts\\python.exe"' in setup
    assert '"%PYTHON%"' in setup
    assert "uv sync --frozen" in setup
    assert 'cd /d "%~dp0"' in launcher
    assert '"%PYTHON%" -m post_pulsar' in launcher
    assert 'set "TASK_NAME=PostPulsar"' in task
    assert "/sc ONLOGON" in task
    assert "/rl LIMITED" in task
    assert "run-post-pulsar.bat" in task
    assert " daemon" in task
    assert "run-now" not in task


@pytest.mark.skipif(
    shutil.which("cmd.exe") is None,
    reason="Windows command processor is unavailable",
)
def test_windows_task_modes_fail_closed_without_calling_schtasks() -> None:
    with tempfile.TemporaryDirectory(
        prefix="post pulsar task ", dir=ROOT.parent
    ) as raw:
        install = Path(raw)
        task = install / "task-setup-post-pulsar.bat"
        shutil.copy2(ROOT / "task-setup-post-pulsar.bat", task)
        shutil.copy2(ROOT / "run-post-pulsar.bat", install / "run-post-pulsar.bat")
        marker = install / "schtasks-invoked.txt"
        env = _stubbed_schtasks_env(install / "scheduler-shim", marker)
        assert env["PATH"].startswith(f"{install / 'scheduler-shim'}{os.pathsep}")
        assert not marker.exists()

        for arguments in ((), ("unexpected",)):
            result = _run_batch(task, *arguments, env=env)
            output = result.stdout + result.stderr
            assert result.returncode == 2
            assert "Usage:" in output
            assert not marker.exists()
            assert not Path(env["POST_PULSAR_SCHTASKS_LOG"]).exists()

        expected_commands = {
            ("dry-run", "install"): '/create /tn "PostPulsar"',
            ("dry-run", "remove"): '/delete /tn "PostPulsar" /f',
        }
        for arguments, command in expected_commands.items():
            result = _run_batch(task, *arguments, env=env)
            output = result.stdout + result.stderr
            assert result.returncode == 0
            assert "DRY RUN" in output
            assert "schtasks" in output
            assert command in output
            if arguments == ("dry-run", "install"):
                assert "run-post-pulsar.bat" in output
                assert "daemon" in output
            assert not marker.exists()
            assert not Path(env["POST_PULSAR_SCHTASKS_LOG"]).exists()

        for arguments in (
            ("dry-run", "install", "unexpected"),
            ("dry-run", "remove", "unexpected"),
            ("dry-run", "install", "", "unexpected"),
        ):
            result = _run_batch(task, *arguments, env=env)
            output = result.stdout + result.stderr
            assert result.returncode == 2
            assert "Usage:" in output
            assert not marker.exists()
            assert not Path(env["POST_PULSAR_SCHTASKS_LOG"]).exists()


@pytest.mark.skipif(
    shutil.which("cmd.exe") is None,
    reason="Windows command processor is unavailable",
)
@pytest.mark.parametrize("query_failure", ("missing", "denied", "redirection"))
def test_windows_task_install_query_failure_cannot_accept_overwrite(
    windows_task, query_failure: str
) -> None:
    task, marker, env = windows_task
    if query_failure == "missing":
        env["POST_PULSAR_QUERY_EXIT"] = "1"
    elif query_failure == "redirection":
        env["TEMP"] = str(task.parent / "nonexistent-temp-directory")
        env["WSLENV"] += ":TEMP/p"

    result = _run_batch(task, "install", env=env, input_text="Y\n")

    assert result.returncode != 0
    assert marker.read_text() == "existing task ownership\n"
    if query_failure == "redirection":
        # cmd can preserve the previous errorlevel after failed redirection;
        # parsing the absent XML must still refuse without calling the scheduler.
        assert not Path(env["POST_PULSAR_SCHTASKS_LOG"]).exists()
        return
    invocations = Path(env["POST_PULSAR_SCHTASKS_LOG"]).read_text().lower().splitlines()
    create_calls = [line for line in invocations if line.startswith("/create ")]
    assert create_calls
    assert all("/f" not in line.split() for line in create_calls)
    assert "WARNING: task already exists" in result.stdout


@pytest.mark.skipif(
    shutil.which("cmd.exe") is None, reason="Windows command processor is unavailable"
)
def test_windows_scheduler_stub_models_interactive_replacement(windows_task) -> None:
    _task, marker, env = windows_task
    result = _run_batch(
        Path(env["POST_PULSAR_SCHTASKS_STUB"]),
        "/create",
        "/tn",
        "PostPulsar",
        env=env,
        input_text="Y\n",
    )
    assert result.returncode == 0
    assert "WARNING: task already exists" in result.stdout
    assert marker.read_text() == "recorded daemon task\n"


@pytest.mark.skipif(
    shutil.which("cmd.exe") is None, reason="Windows command processor is unavailable"
)
def test_windows_task_can_create_absent_task(windows_task) -> None:
    task, marker, env = windows_task
    marker.unlink()
    env["POST_PULSAR_QUERY_EXIT"] = "1"
    result = _run_batch(task, "install", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert marker.read_text() == "recorded daemon task\n"
    calls = Path(env["POST_PULSAR_SCHTASKS_LOG"]).read_text().lower().splitlines()
    assert len(calls) == 2
    assert calls[0].startswith("/query ")
    assert calls[1].startswith("/create ")
    assert "/f" not in calls[1].split()


@pytest.mark.skipif(
    shutil.which("cmd.exe") is None, reason="Windows command processor is unavailable"
)
@pytest.mark.parametrize("mode", ("install", "remove"))
@pytest.mark.parametrize(
    "case",
    (
        "owned",
        "normalized",
        "misleading",
        "malformed",
        "extra-arguments",
        "missing-arguments",
        "multiple-exec",
        "other-action",
        "conflicting-action",
    ),
)
def test_windows_task_requires_structured_exact_ownership(
    windows_task, mode: str, case: str
) -> None:
    task, marker, env = windows_task
    env["POST_PULSAR_QUERY_EXIT"] = "0"
    Path(env["POST_PULSAR_SCHTASKS_XML"]).write_text(
        _task_xml(task.parent / "run-post-pulsar.bat", case),
        encoding="utf-8",
    )

    result = _run_batch(task, mode, env=env, input_text="Y\n")

    calls = Path(env["POST_PULSAR_SCHTASKS_LOG"]).read_text().lower().splitlines()
    assert calls[0].startswith("/query ")
    if case in ("owned", "normalized"):
        assert result.returncode == 0, result.stdout + result.stderr
        assert len(calls) == 2
        assert calls[1].startswith("/create " if mode == "install" else "/delete ")
        assert "/f" in calls[1].split()
        if mode == "install":
            assert marker.read_text() == "recorded daemon task\n"
        else:
            assert not marker.exists()
    else:
        assert result.returncode == 3, result.stdout + result.stderr
        assert len(calls) == 1
        assert marker.read_text() == "existing task ownership\n"


@pytest.mark.skipif(
    shutil.which("cmd.exe") is None, reason="Windows command processor is unavailable"
)
@pytest.mark.parametrize(
    "arguments",
    (
        ("install", ""),
        ("remove", ""),
        ("dry-run", "install", ""),
        ("dry-run", "remove", ""),
        ("install", *([""] * 9), "unexpected"),
        ("remove", *([""] * 9), "unexpected"),
        ("dry-run", "install", *([""] * 8), "unexpected"),
        ("dry-run", "remove", *([""] * 8), "unexpected"),
    ),
)
def test_windows_task_rejects_every_trailing_argument(windows_task, arguments) -> None:
    task, marker, env = windows_task
    result = _run_batch(task, *arguments, env=env)
    assert result.returncode == 2, result.stdout + result.stderr
    assert "Usage:" in result.stdout + result.stderr
    assert not Path(env["POST_PULSAR_SCHTASKS_LOG"]).exists()
    assert marker.read_text() == "existing task ownership\n"


def test_package_is_the_only_active_python_entry_surface() -> None:
    assert (ROOT / "src/post_pulsar/__main__.py").is_file()
    assert (ROOT / "uv.lock").is_file()
    readme = _read("README.md")
    assert "POST PULSAR" in readme
    assert "setup-post-pulsar" in readme
    assert "run-post-pulsar" in readme
    assert "`post-pulsar` console command" in readme
    for stale_name in ("PHONG-BOT", "phong-bot.py", "requirements.txt", "config.json"):
        assert stale_name not in readme
