"""Private local records with POSIX modes and protected current-user Windows ACLs."""

from __future__ import annotations

import ctypes
import os
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path


class SecureFileError(RuntimeError):
    """A local record cannot be kept private to the current user."""


def _is_windows() -> bool:
    return os.name == "nt"


def assert_owner_file(path: Path, *, allow_missing: bool = False) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        if allow_missing:
            return
        raise
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_ISLNK(metadata.st_mode)
        or getattr(metadata, "st_file_attributes", 0) & 0x400
    ):
        raise SecureFileError("local record is unsafe")
    if _is_windows():
        _windows_validate(path)
    elif metadata.st_mode & 0o077 or metadata.st_uid != os.getuid():
        raise SecureFileError("local record permissions are too broad")


def secure_directory(path: Path) -> None:
    if _is_windows():
        if not path.exists():
            if not path.parent.exists():
                secure_directory(path.parent)
            _windows_create_directory(path)
    else:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata = path.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or getattr(metadata, "st_file_attributes", 0) & 0x400
    ):
        raise SecureFileError("private directory is unsafe")
    if _is_windows():
        _windows_secure(path, True)
        _windows_validate(path)
    else:
        if metadata.st_uid != os.getuid():
            raise SecureFileError("private directory has another owner")
        path.chmod(0o700)


def fsync_directory(path: Path) -> None:
    # Windows atomic replacement is provided by the filesystem. Directory handles
    # do not support the CRT fsync operation used for POSIX directory durability.
    if _is_windows():
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_owner_write(path: Path, payload: bytes) -> None:
    assert_owner_file(path, allow_missing=True)
    secure_directory(path.parent)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        if _is_windows():
            _windows_secure(temporary, False)
            _windows_validate(temporary)
        else:
            os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        assert_owner_file(path, allow_missing=True)
        os.replace(temporary, path)
        assert_owner_file(path)
        fsync_directory(path.parent)
    finally:
        if descriptor != -1:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _windows_api():
    """Declare pointer-sized Win32 security signatures; never invoke a shell."""
    from ctypes import wintypes as w

    adv = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    pointer = ctypes.c_void_p
    signatures = {
        "OpenProcessToken": ([w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE)], w.BOOL),
        "GetTokenInformation": (
            [w.HANDLE, ctypes.c_int, pointer, w.DWORD, ctypes.POINTER(w.DWORD)],
            w.BOOL,
        ),
        "ConvertSidToStringSidW": ([pointer, ctypes.POINTER(pointer)], w.BOOL),
        "ConvertStringSecurityDescriptorToSecurityDescriptorW": (
            [w.LPCWSTR, w.DWORD, ctypes.POINTER(pointer), pointer],
            w.BOOL,
        ),
        "GetSecurityDescriptorDacl": (
            [
                pointer,
                ctypes.POINTER(w.BOOL),
                ctypes.POINTER(pointer),
                ctypes.POINTER(w.BOOL),
            ],
            w.BOOL,
        ),
        "GetSecurityDescriptorOwner": (
            [pointer, ctypes.POINTER(pointer), ctypes.POINTER(w.BOOL)],
            w.BOOL,
        ),
        "SetNamedSecurityInfoW": (
            [w.LPWSTR, ctypes.c_int, w.DWORD, pointer, pointer, pointer, pointer],
            w.DWORD,
        ),
        "GetNamedSecurityInfoW": (
            [
                w.LPWSTR,
                ctypes.c_int,
                w.DWORD,
                ctypes.POINTER(pointer),
                pointer,
                ctypes.POINTER(pointer),
                pointer,
                ctypes.POINTER(pointer),
            ],
            w.DWORD,
        ),
        "GetSecurityDescriptorControl": (
            [pointer, ctypes.POINTER(w.WORD), ctypes.POINTER(w.DWORD)],
            w.BOOL,
        ),
        "GetAclInformation": ([pointer, pointer, w.DWORD, ctypes.c_int], w.BOOL),
        "GetAce": ([pointer, w.DWORD, ctypes.POINTER(pointer)], w.BOOL),
        "EqualSid": ([pointer, pointer], w.BOOL),
    }
    for name, (args, result) in signatures.items():
        function = getattr(adv, name)
        function.argtypes, function.restype = args, result
    for name, args, result in (
        ("GetCurrentProcess", [], w.HANDLE),
        ("CloseHandle", [w.HANDLE], w.BOOL),
        ("LocalFree", [pointer], pointer),
        ("CreateDirectoryW", [w.LPCWSTR, pointer], w.BOOL),
    ):
        function = getattr(kernel, name)
        function.argtypes, function.restype = args, result
    return adv, kernel


def _win_check(success: object) -> None:
    if not success:
        raise SecureFileError("Windows private security operation failed")


@contextmanager
def _windows_descriptor(directory: bool):
    from ctypes import wintypes as w

    adv, kernel = _windows_api()
    token = w.HANDLE()
    sid_string = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    _win_check(
        adv.OpenProcessToken(kernel.GetCurrentProcess(), 0x0008, ctypes.byref(token))
    )
    try:
        size = w.DWORD()
        adv.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
        _win_check(size.value > 0)
        token_user = ctypes.create_string_buffer(size.value)
        _win_check(
            adv.GetTokenInformation(token, 1, token_user, size, ctypes.byref(size))
        )
        sid = ctypes.c_void_p.from_buffer(token_user)
        _win_check(adv.ConvertSidToStringSidW(sid, ctypes.byref(sid_string)))
        identifier = ctypes.wstring_at(sid_string)
        inheritance = "OICI" if directory else ""
        sddl = f"O:{identifier}D:P(A;{inheritance};FA;;;{identifier})"
        _win_check(
            adv.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl, 1, ctypes.byref(descriptor), None
            )
        )
        yield adv, kernel, descriptor, sid
    finally:
        if descriptor:
            kernel.LocalFree(descriptor)
        if sid_string:
            kernel.LocalFree(sid_string)
        kernel.CloseHandle(token)


def _windows_create_directory(path: Path) -> None:
    from ctypes import wintypes as w

    class SecurityAttributes(ctypes.Structure):
        _fields_ = [
            ("length", w.DWORD),
            ("descriptor", ctypes.c_void_p),
            ("inherit", w.BOOL),
        ]

    with _windows_descriptor(True) as (_, kernel, descriptor, _sid):
        attributes = SecurityAttributes(
            ctypes.sizeof(SecurityAttributes), descriptor, False
        )
        _win_check(kernel.CreateDirectoryW(str(path), ctypes.byref(attributes)))


def _windows_secure(path: Path, directory: bool) -> None:
    from ctypes import wintypes as w

    with _windows_descriptor(directory) as (adv, _kernel, descriptor, sid):
        present, defaulted = w.BOOL(), w.BOOL()
        dacl = ctypes.c_void_p()
        _win_check(
            adv.GetSecurityDescriptorDacl(
                descriptor,
                ctypes.byref(present),
                ctypes.byref(dacl),
                ctypes.byref(defaulted),
            )
        )
        _win_check(present and dacl)
        # OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION |
        # PROTECTED_DACL_SECURITY_INFORMATION disables inherited grants.
        _win_check(
            adv.SetNamedSecurityInfoW(str(path), 1, 0x80000005, sid, None, dacl, None)
            == 0
        )


def _windows_validate(path: Path) -> None:
    from ctypes import wintypes as w

    class AclSize(ctypes.Structure):
        _fields_ = [("count", w.DWORD), ("used", w.DWORD), ("free", w.DWORD)]

    class AceHeader(ctypes.Structure):
        _fields_ = [
            ("kind", w.BYTE),
            ("flags", w.BYTE),
            ("size", w.WORD),
            ("mask", w.DWORD),
        ]

    with _windows_descriptor(False) as (adv, kernel, _expected, sid):
        owner, dacl, descriptor = (
            ctypes.c_void_p(),
            ctypes.c_void_p(),
            ctypes.c_void_p(),
        )
        _win_check(
            adv.GetNamedSecurityInfoW(
                str(path),
                1,
                5,
                ctypes.byref(owner),
                None,
                ctypes.byref(dacl),
                None,
                ctypes.byref(descriptor),
            )
            == 0
        )
        try:
            _win_check(owner and dacl and adv.EqualSid(owner, sid))
            control, revision = w.WORD(), w.DWORD()
            _win_check(
                adv.GetSecurityDescriptorControl(
                    descriptor, ctypes.byref(control), ctypes.byref(revision)
                )
            )
            _win_check(control.value & 0x1000)  # SE_DACL_PROTECTED
            size = AclSize()
            _win_check(
                adv.GetAclInformation(dacl, ctypes.byref(size), ctypes.sizeof(size), 2)
            )
            _win_check(size.count == 1)
            ace = ctypes.c_void_p()
            _win_check(adv.GetAce(dacl, 0, ctypes.byref(ace)))
            header = AceHeader.from_address(ace.value)
            # ACCESS_ALLOWED_ACE, no inherited/inherit-only ACE, FILE_ALL_ACCESS.
            _win_check(
                header.kind == 0 and not header.flags & 0x18 and header.mask == 0x1F01FF
            )
            _win_check(header.size >= ctypes.sizeof(AceHeader) + 8)
            _win_check(adv.EqualSid(ctypes.c_void_p(ace.value + 8), sid))
        finally:
            kernel.LocalFree(descriptor)
