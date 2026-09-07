"""Read native process creation identities without sending process signals."""

from __future__ import annotations

import ctypes
import errno
import sys
from pathlib import Path


class ProcessIdentityError(RuntimeError):
    """The process incarnation could not be established safely."""


class ProcessNotFoundError(ProcessIdentityError):
    """The native process table positively identified an absent process."""


def process_start_identity(pid: int) -> int:
    """Return the OS creation value; unavailable identity is never guessed."""
    if type(pid) is not int or not 0 < pid <= 0xFFFFFFFF:
        raise ProcessIdentityError("process PID is invalid")
    try:
        if sys.platform == "linux":
            return _linux_start_identity(pid)
        if sys.platform == "darwin":
            return _darwin_start_identity(pid)
        if sys.platform == "win32":
            return _windows_start_identity(pid)
    except (OSError, ValueError, IndexError, AttributeError):
        raise ProcessIdentityError("process identity is unavailable") from None
    raise ProcessIdentityError("native process identity is unsupported")


def process_identity_matches(pid: int, started_at: int) -> bool:
    """Reject stale identities, while preserving uncertainty as an error."""
    if type(started_at) is not int or started_at < 0:
        raise ProcessIdentityError("process creation identity is invalid")
    try:
        return process_start_identity(pid) == started_at
    except ProcessNotFoundError:
        return False


def _linux_start_identity(pid: int) -> int:
    try:
        record = Path(f"/proc/{pid}/stat").read_bytes()
    except FileNotFoundError:
        raise ProcessNotFoundError("process is absent") from None
    # comm (field 2) can itself contain spaces, newlines, and parentheses.
    # The numeric tail contains no closing parentheses, so its last ')' is
    # the only reliable delimiter before the state field (field 3).
    if not record.startswith(f"{pid} (".encode("ascii")):
        raise ProcessIdentityError("process stat identity is invalid")
    _, separator, tail = record.rpartition(b")")
    fields = tail.split()
    if not separator or len(fields) < 20 or len(fields[0]) != 1:
        raise ProcessIdentityError("process stat identity is invalid")
    started_at = int(fields[19])
    if started_at < 0:
        raise ProcessIdentityError("process stat identity is invalid")
    return started_at


class _ProcBsdInfo(ctypes.Structure):
    # Darwin libproc's PROC_PIDTBSDINFO ABI (sys/proc_info.h).
    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


def _darwin_start_identity(pid: int) -> int:
    library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    proc_pidinfo = library.proc_pidinfo
    proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    proc_pidinfo.restype = ctypes.c_int
    info = _ProcBsdInfo()
    ctypes.set_errno(0)
    size = proc_pidinfo(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info))
    if size != ctypes.sizeof(info):
        if size == 0 and ctypes.get_errno() == errno.ESRCH:
            raise ProcessNotFoundError("process is absent")
        raise ProcessIdentityError("process identity is unavailable")
    if info.pbi_pid != pid or not 0 <= info.pbi_start_tvusec < 1000000:
        raise ProcessIdentityError("process identity is invalid")
    return int(info.pbi_start_tvsec) * 1000000 + int(info.pbi_start_tvusec)


class _FileTime(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", ctypes.c_uint32),
        ("dwHighDateTime", ctypes.c_uint32),
    ]


def _windows_start_identity(pid: int) -> int:
    loader = getattr(ctypes, "WinDLL", None)
    if loader is None:
        raise ProcessIdentityError("native process identity is unavailable")
    kernel = loader("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.GetProcessTimes.argtypes = [
        ctypes.c_void_p,
        *([ctypes.POINTER(_FileTime)] * 4),
    ]
    kernel.GetProcessTimes.restype = ctypes.c_int
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel.CloseHandle.restype = ctypes.c_int
    # PROCESS_QUERY_LIMITED_INFORMATION never grants termination or mutation.
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        get_error = getattr(ctypes, "get_last_error", lambda: 0)
        if get_error() == 87:  # ERROR_INVALID_PARAMETER: valid PID is absent.
            raise ProcessNotFoundError("process is absent")
        raise ProcessIdentityError("process identity is unavailable")
    try:
        created, exited, kernel_time, user_time = (_FileTime() for _ in range(4))
        if not kernel.GetProcessTimes(
            handle,
            ctypes.byref(created),
            ctypes.byref(exited),
            ctypes.byref(kernel_time),
            ctypes.byref(user_time),
        ):
            raise ProcessIdentityError("process identity is unavailable")
        return (int(created.dwHighDateTime) << 32) | int(created.dwLowDateTime)
    finally:
        kernel.CloseHandle(handle)


__all__ = [
    "ProcessIdentityError",
    "ProcessNotFoundError",
    "process_identity_matches",
    "process_start_identity",
]
