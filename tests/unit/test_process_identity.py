"""Native process incarnation checks never send process signals."""

from __future__ import annotations

import ctypes
import importlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


def _identity():
    return importlib.import_module("post_pulsar.process_identity")


@pytest.fixture(autouse=True)
def _deny_process_signals(monkeypatch):
    def forbidden(*args):
        pytest.fail("process identity must not send signals, including signal zero")

    monkeypatch.setattr(os, "kill", forbidden)


def test_linux_stat_name_can_contain_spaces_parentheses_and_newlines(monkeypatch):
    identity = _identity()
    tail = [b"S", *([b"0"] * 18), b"998877", b"0"]
    stat = b"73 (publisher ) with (odd)\nname) " + b" ".join(tail)
    monkeypatch.setattr(identity.sys, "platform", "linux")
    monkeypatch.setattr(Path, "read_bytes", lambda self: stat)
    assert identity.process_start_identity(73) == 998877


@pytest.mark.parametrize("pid", [0, -1, True, "73"])
def test_invalid_pid_never_reads_native_process_state(monkeypatch, pid):
    identity = _identity()

    def forbidden(*args, **kwargs):
        pytest.fail("invalid PID reached native process lookup")

    monkeypatch.setattr(Path, "read_bytes", forbidden)
    with pytest.raises(identity.ProcessIdentityError):
        identity.process_start_identity(pid)


def test_missing_process_differs_from_unverifiable_identity(monkeypatch):
    identity = _identity()
    monkeypatch.setattr(identity.sys, "platform", "linux")

    def missing(self):
        raise FileNotFoundError

    monkeypatch.setattr(Path, "read_bytes", missing)
    assert not identity.process_identity_matches(73, 123)

    def denied(self):
        raise PermissionError

    monkeypatch.setattr(Path, "read_bytes", denied)
    with pytest.raises(identity.ProcessIdentityError):
        identity.process_identity_matches(73, 123)


def test_unknown_platform_fails_closed_even_for_current_process(monkeypatch):
    identity = _identity()
    monkeypatch.setattr(identity.sys, "platform", "unknown")
    with pytest.raises(identity.ProcessIdentityError):
        identity.process_start_identity(73)


def test_same_pid_with_new_start_time_is_not_the_same_process(monkeypatch):
    identity = _identity()
    monkeypatch.setattr(identity, "process_start_identity", lambda pid: 200)
    assert identity.process_identity_matches(73, 200)
    assert not identity.process_identity_matches(73, 100)


@pytest.mark.parametrize("stat", [b"73 (name) S 0", b"73 missing delimiter", b"wrong"])
def test_malformed_linux_stat_fails_closed(monkeypatch, stat):
    identity = _identity()
    monkeypatch.setattr(identity.sys, "platform", "linux")
    monkeypatch.setattr(Path, "read_bytes", lambda self: stat)
    with pytest.raises(identity.ProcessIdentityError):
        identity.process_start_identity(73)


class _NativeCall:
    def __init__(self, callback):
        self.callback = callback

    def __call__(self, *args):
        return self.callback(*args)


def test_darwin_process_start_uses_native_proc_pidinfo(monkeypatch):
    identity = _identity()
    calls = []

    def proc_pidinfo(pid, flavor, arg, buffer, size):
        calls.append((pid, flavor, arg, size))
        info = ctypes.cast(buffer, ctypes.POINTER(identity._ProcBsdInfo)).contents
        info.pbi_pid = pid
        info.pbi_start_tvsec = 123456
        info.pbi_start_tvusec = 789
        return size

    native = SimpleNamespace(proc_pidinfo=_NativeCall(proc_pidinfo))
    monkeypatch.setattr(identity.sys, "platform", "darwin")
    monkeypatch.setattr(identity.ctypes, "CDLL", lambda *a, **k: native)
    assert identity.process_start_identity(73) == 123456000789
    assert calls == [(73, 3, 0, 136)]


def test_darwin_short_native_result_fails_closed(monkeypatch):
    identity = _identity()
    native = SimpleNamespace(proc_pidinfo=_NativeCall(lambda *a: 1))
    monkeypatch.setattr(identity.sys, "platform", "darwin")
    monkeypatch.setattr(identity.ctypes, "CDLL", lambda *a, **k: native)
    with pytest.raises(identity.ProcessIdentityError):
        identity.process_start_identity(73)


def test_windows_identity_reads_creation_time_and_closes_handle(monkeypatch):
    identity = _identity()
    opened = []
    closed = []

    def open_process(access, inherit, pid):
        opened.append((access, inherit, pid))
        return 456

    def get_process_times(handle, created, exited, kernel, user):
        assert handle == 456
        value = ctypes.cast(created, ctypes.POINTER(identity._FileTime)).contents
        value.dwLowDateTime = 234
        value.dwHighDateTime = 12
        return 1

    native = SimpleNamespace(
        OpenProcess=_NativeCall(open_process),
        GetProcessTimes=_NativeCall(get_process_times),
        CloseHandle=_NativeCall(lambda handle: closed.append(handle) or 1),
    )
    monkeypatch.setattr(identity.sys, "platform", "win32")
    monkeypatch.setattr(identity.ctypes, "WinDLL", lambda *a, **k: native, raising=False)
    assert identity.process_start_identity(73) == (12 << 32) | 234
    assert opened == [(0x1000, False, 73)]
    assert closed == [456]


def test_windows_failed_time_lookup_still_closes_handle(monkeypatch):
    identity = _identity()
    closed = []
    native = SimpleNamespace(
        OpenProcess=_NativeCall(lambda *a: 456),
        GetProcessTimes=_NativeCall(lambda *a: 0),
        CloseHandle=_NativeCall(lambda handle: closed.append(handle) or 1),
    )
    monkeypatch.setattr(identity.sys, "platform", "win32")
    monkeypatch.setattr(identity.ctypes, "WinDLL", lambda *a, **k: native, raising=False)
    with pytest.raises(identity.ProcessIdentityError):
        identity.process_start_identity(73)
    assert closed == [456]
