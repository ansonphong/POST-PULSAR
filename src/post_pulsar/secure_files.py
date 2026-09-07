"""Private local records with POSIX modes and protected current-user Windows ACLs."""

from __future__ import annotations

import ctypes
import errno
import os
import re
import stat
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast


class SecureFileError(RuntimeError):
    """A local record cannot be kept private to the current user."""


@dataclass(frozen=True, slots=True)
class RecordPolicy:
    """Pinned OS identities for core mutation and optional agent discovery reads."""

    core_principal: str
    agent_principal: str
    agent_group: int | None = None

    def validate(self, *, write: bool = False) -> None:
        if self.core_principal == self.agent_principal:
            raise SecureFileError("hardened mode requires distinct principals")
        if _is_windows():
            if self.agent_group is not None:
                raise SecureFileError(
                    "Windows hardened policy cannot use a POSIX group"
                )
            _windows_validate_user_sid(self.core_principal)
            _windows_validate_user_sid(self.agent_principal)
            current = _windows_current_sid()
        else:
            import grp
            import pwd

            if any(
                not re.fullmatch(r"0|[1-9][0-9]*", value)
                for value in (self.core_principal, self.agent_principal)
            ):
                raise SecureFileError("hardened POSIX principals must be numeric UIDs")
            core, agent = int(self.core_principal), int(self.agent_principal)
            if agent == 0 or type(self.agent_group) is not int or self.agent_group <= 0:
                raise SecureFileError("hardened agent identity or group is invalid")
            try:
                users = pwd.getpwall()
                core_user, agent_user = pwd.getpwuid(core), pwd.getpwuid(agent)
                group = grp.getgrgid(self.agent_group)
                if any(
                    sum(user.pw_uid == uid for user in users) != 1
                    for uid in (core, agent)
                ):
                    raise SecureFileError("hardened principal identity is ambiguous")
                members = set(group.gr_mem) | {
                    u.pw_name for u in users if u.pw_gid == self.agent_group
                }
                if agent_user.pw_name not in members or not members <= {
                    core_user.pw_name,
                    agent_user.pw_name,
                }:
                    raise SecureFileError("hardened discovery group is not dedicated")
                # Reject supplementary grants to the agent: the dedicated group
                # must be its sole group, making traversal decisions unambiguous.
                if set(os.getgrouplist(agent_user.pw_name, agent_user.pw_gid)) != {
                    self.agent_group
                }:
                    raise SecureFileError("hardened agent has additional groups")
            except (KeyError, OSError, AttributeError):
                raise SecureFileError(
                    "hardened identity cannot be established"
                ) from None
            if os.getuid() != os.geteuid():
                raise SecureFileError("hardened set-user-ID execution is unsupported")
            current = str(os.geteuid())
            if current == self.agent_principal and (
                os.getegid() != self.agent_group
                or set(os.getgroups()) - {self.agent_group}
            ):
                raise SecureFileError(
                    "hardened agent process has stale group membership"
                )
        if current not in (
            {self.core_principal}
            if write
            else {self.core_principal, self.agent_principal}
        ):
            raise SecureFileError(
                "process does not match the hardened principal policy"
            )


def _reject_posix_acl(path: Path) -> None:
    if sys.platform != "linux" or not hasattr(os, "getxattr"):
        raise SecureFileError("POSIX ACL validation is unavailable")
    for name in ("system.posix_acl_access", "system.posix_acl_default"):
        try:
            os.getxattr(path, name, follow_symlinks=False)
        except OSError as error:
            if error.errno in {errno.ENODATA, errno.ENOTSUP}:
                continue
            raise SecureFileError("POSIX ACL validation failed") from None
        raise SecureFileError("hardened records cannot have extended POSIX ACLs")


def _validate_ancestors(path: Path, policy: RecordPolicy) -> None:
    """Validate pre-provisioned traversal without broadening existing directories."""
    for ancestor in path.absolute().parents:
        metadata = ancestor.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or getattr(metadata, "st_file_attributes", 0) & 0x400
        ):
            raise SecureFileError("hardened ancestor is unsafe")
        if _is_windows():
            # Require the same protected core/agent policy on every ancestor
            # below the volume root; an operator must provision this hierarchy.
            if ancestor != Path(ancestor.anchor):
                _windows_validate(ancestor, policy=policy)
        else:
            _reject_posix_acl(ancestor)
            mode = stat.S_IMODE(metadata.st_mode)
            if metadata.st_uid not in {0, int(policy.core_principal)} or mode & 0o022:
                raise SecureFileError(
                    "hardened ancestor can be replaced by another principal"
                )
            traversal = 0o010 if metadata.st_gid == policy.agent_group else 0o001
            if not mode & traversal:
                raise SecureFileError("hardened agent cannot traverse an ancestor")


def _core_group(policy: RecordPolicy) -> int:
    import pwd

    return pwd.getpwuid(int(policy.core_principal)).pw_gid


def _agent_group(policy: RecordPolicy) -> int:
    if policy.agent_group is None:
        raise SecureFileError("hardened POSIX agent group is unavailable")
    return policy.agent_group


def _validate_directory(
    path: Path, policy: RecordPolicy, *, agent_read: bool = True
) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or getattr(metadata, "st_file_attributes", 0) & 0x400
    ):
        raise SecureFileError("hardened directory is unsafe")
    if _is_windows():
        _windows_validate(path, policy=policy, agent_read=agent_read)
    else:
        _reject_posix_acl(path)
        if (
            metadata.st_uid != int(policy.core_principal)
            or metadata.st_gid
            != (policy.agent_group if agent_read else _core_group(policy))
            or stat.S_IMODE(metadata.st_mode) != (0o750 if agent_read else 0o700)
        ):
            raise SecureFileError("hardened directory policy does not match")
    if agent_read:
        _validate_ancestors(path, policy)


def _is_windows() -> bool:
    return os.name == "nt"


def assert_owner_file(
    path: Path,
    *,
    allow_missing: bool = False,
    policy: RecordPolicy | None = None,
    agent_read: bool = True,
) -> None:
    if policy is not None:
        policy.validate(write=not agent_read)
        if path.parent.exists():
            _validate_directory(path.parent, policy, agent_read=agent_read)
        elif agent_read:
            _validate_ancestors(path.parent, policy)
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
        if policy is None:
            _windows_validate(path)
        else:
            _windows_validate(path, policy=policy, agent_read=agent_read)
    elif policy is not None:
        _reject_posix_acl(path)
        if (
            metadata.st_uid != int(policy.core_principal)
            or metadata.st_gid
            != (policy.agent_group if agent_read else _core_group(policy))
            or stat.S_IMODE(metadata.st_mode) != (0o640 if agent_read else 0o600)
        ):
            raise SecureFileError("hardened file policy does not match")
    elif metadata.st_mode & 0o077 or metadata.st_uid != os.getuid():
        raise SecureFileError("local record permissions are too broad")


def secure_directory(path: Path, *, policy: RecordPolicy | None = None) -> None:
    if policy is not None:
        policy.validate(write=True)
        _validate_ancestors(path, policy)
        if not path.exists():
            if _is_windows():
                _windows_create_directory(path, policy=policy)
            else:
                path.mkdir(mode=0o700)
                os.chown(path, int(policy.core_principal), _agent_group(policy))
                path.chmod(0o750)
        _validate_directory(path, policy)
        return
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


def atomic_owner_write(
    path: Path,
    payload: bytes,
    *,
    policy: RecordPolicy | None = None,
    agent_read: bool = True,
) -> None:
    if policy is not None:
        policy.validate(write=True)
    assert_owner_file(path, allow_missing=True, policy=policy, agent_read=agent_read)
    secure_directory(path.parent, policy=policy if agent_read else None)
    if policy is not None and not agent_read:
        _validate_directory(path.parent, policy, agent_read=False)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        if _is_windows():
            if policy is None:
                _windows_secure(temporary, False)
                _windows_validate(temporary)
            else:
                _windows_secure(temporary, False, policy=policy, agent_read=agent_read)
                _windows_validate(temporary, policy=policy, agent_read=agent_read)
        else:
            if policy is not None:
                os.fchown(
                    descriptor,
                    int(policy.core_principal),
                    _agent_group(policy) if agent_read else _core_group(policy),
                )
            os.fchmod(descriptor, 0o640 if policy is not None and agent_read else 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        assert_owner_file(temporary, policy=policy, agent_read=agent_read)
        assert_owner_file(
            path, allow_missing=True, policy=policy, agent_read=agent_read
        )
        os.replace(temporary, path)
        assert_owner_file(path, policy=policy, agent_read=agent_read)
        fsync_directory(path.parent)
    finally:
        if descriptor != -1:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _windows_api() -> tuple[Any, Any]:
    """Declare pointer-sized Win32 security signatures; never invoke a shell."""
    from ctypes import wintypes as w

    win_dll = cast(Any, getattr(ctypes, "WinDLL"))  # noqa: B009 - absent on POSIX
    adv = win_dll("advapi32", use_last_error=True)
    kernel = win_dll("kernel32", use_last_error=True)
    pointer = ctypes.c_void_p
    signatures: dict[str, tuple[list[Any], Any]] = {
        "OpenProcessToken": ([w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE)], w.BOOL),
        "GetTokenInformation": (
            [w.HANDLE, ctypes.c_int, pointer, w.DWORD, ctypes.POINTER(w.DWORD)],
            w.BOOL,
        ),
        "ConvertSidToStringSidW": ([pointer, ctypes.POINTER(pointer)], w.BOOL),
        "ConvertStringSidToSidW": ([w.LPCWSTR, ctypes.POINTER(pointer)], w.BOOL),
        "LookupAccountSidW": (
            [
                w.LPCWSTR,
                pointer,
                w.LPWSTR,
                ctypes.POINTER(w.DWORD),
                w.LPWSTR,
                ctypes.POINTER(w.DWORD),
                ctypes.POINTER(w.DWORD),
            ],
            w.BOOL,
        ),
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


def _windows_validate_user_sid(identifier: str) -> None:
    """Resolve an explicit account SID, rejecting groups and built-in identities."""
    from ctypes import wintypes as w

    if (
        not re.fullmatch(
            r"S-1-5-21-[1-9][0-9]*-[1-9][0-9]*-[1-9][0-9]*-[1-9][0-9]*", identifier
        )
        or int(identifier.rsplit("-", 1)[1]) < 1000
    ):
        raise SecureFileError("hardened Windows identity must be a distinct user SID")
    try:
        adv, kernel = _windows_api()
    except (AttributeError, OSError):
        raise SecureFileError(
            "native Windows identity validation is unavailable"
        ) from None
    sid = ctypes.c_void_p()
    _win_check(adv.ConvertStringSidToSidW(identifier, ctypes.byref(sid)))
    try:
        name_size, domain_size, kind = w.DWORD(), w.DWORD(), w.DWORD()
        adv.LookupAccountSidW(
            None,
            sid,
            None,
            ctypes.byref(name_size),
            None,
            ctypes.byref(domain_size),
            ctypes.byref(kind),
        )
        _win_check(0 < name_size.value <= 32768 and domain_size.value <= 32768)
        name, domain = (
            ctypes.create_unicode_buffer(name_size.value),
            ctypes.create_unicode_buffer(max(1, domain_size.value)),
        )
        _win_check(
            adv.LookupAccountSidW(
                None,
                sid,
                name,
                ctypes.byref(name_size),
                domain,
                ctypes.byref(domain_size),
                ctypes.byref(kind),
            )
        )
        _win_check(kind.value == 1)  # SidTypeUser, never a group SID.
    finally:
        kernel.LocalFree(sid)


def _windows_current_sid() -> str:
    from ctypes import wintypes as w

    try:
        adv, kernel = _windows_api()
    except (AttributeError, OSError):
        raise SecureFileError(
            "native Windows identity validation is unavailable"
        ) from None
    token, sid_string = w.HANDLE(), ctypes.c_void_p()
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
        _win_check(
            adv.ConvertSidToStringSidW(
                ctypes.c_void_p.from_buffer(token_user), ctypes.byref(sid_string)
            )
        )
        return ctypes.wstring_at(sid_string)
    finally:
        if sid_string:
            kernel.LocalFree(sid_string)
        kernel.CloseHandle(token)


def _windows_sddl(
    directory: bool, policy: RecordPolicy | None = None, *, agent_read: bool = True
) -> str:
    if policy is None:
        identifier = _windows_current_sid()
        inheritance = "OICI" if directory else ""
        return f"O:{identifier}D:P(A;{inheritance};FA;;;{identifier})"
    policy.validate(write=not agent_read)
    # No inheritance: every record receives and verifies its exact protected ACL.
    inheritance = "OICI" if directory and not agent_read else ""
    result = (
        f"O:{policy.core_principal}D:P(A;{inheritance};FA;;;{policy.core_principal})"
    )
    if agent_read:
        mask = "0x1200a9" if directory else "0x120089"
        result += f"(A;;{mask};;;{policy.agent_principal})"
    return result


@contextmanager
def _windows_descriptor(
    directory: bool, policy: RecordPolicy | None = None, *, agent_read: bool = True
) -> Iterator[tuple[Any, Any, ctypes.c_void_p, ctypes.c_void_p]]:
    from ctypes import wintypes as w

    adv, kernel = _windows_api()
    descriptor = ctypes.c_void_p()
    try:
        sddl = _windows_sddl(directory, policy, agent_read=agent_read)
        _win_check(
            adv.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl, 1, ctypes.byref(descriptor), None
            )
        )
        sid, defaulted = ctypes.c_void_p(), w.BOOL()
        _win_check(
            adv.GetSecurityDescriptorOwner(
                descriptor, ctypes.byref(sid), ctypes.byref(defaulted)
            )
        )
        yield adv, kernel, descriptor, sid
    finally:
        if descriptor:
            kernel.LocalFree(descriptor)


def _windows_create_directory(
    path: Path, *, policy: RecordPolicy | None = None
) -> None:
    from ctypes import wintypes as w

    class SecurityAttributes(ctypes.Structure):
        _fields_ = [
            ("length", w.DWORD),
            ("descriptor", ctypes.c_void_p),
            ("inherit", w.BOOL),
        ]

    with _windows_descriptor(True, policy) as (_, kernel, descriptor, _sid):
        attributes = SecurityAttributes(
            ctypes.sizeof(SecurityAttributes), descriptor, False
        )
        _win_check(kernel.CreateDirectoryW(str(path), ctypes.byref(attributes)))


def _windows_secure(
    path: Path,
    directory: bool,
    *,
    policy: RecordPolicy | None = None,
    agent_read: bool = True,
) -> None:
    from ctypes import wintypes as w

    with _windows_descriptor(directory, policy, agent_read=agent_read) as (
        adv,
        _kernel,
        descriptor,
        sid,
    ):
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


def _windows_validate(
    path: Path, *, policy: RecordPolicy | None = None, agent_read: bool = True
) -> None:
    from ctypes import wintypes as w

    class AclSize(ctypes.Structure):
        _fields_ = [("count", w.DWORD), ("used", w.DWORD), ("free", w.DWORD)]

    class AceHeader(ctypes.Structure):
        _fields_ = [
            ("kind", ctypes.c_uint8),
            ("flags", ctypes.c_uint8),
            ("size", ctypes.c_uint16),
            ("mask", ctypes.c_uint32),
        ]

    context = (
        _windows_descriptor(False)
        if policy is None
        else _windows_descriptor(path.is_dir(), policy, agent_read=agent_read)
    )
    with context as (adv, kernel, expected, sid):
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
            _win_check(size.count == (2 if policy is not None and agent_read else 1))
            expected_acl = ctypes.c_void_p()
            if policy is not None:
                present, defaulted = w.BOOL(), w.BOOL()
                _win_check(
                    adv.GetSecurityDescriptorDacl(
                        expected,
                        ctypes.byref(present),
                        ctypes.byref(expected_acl),
                        ctypes.byref(defaulted),
                    )
                )
                _win_check(present and expected_acl)
            for index in range(size.count):
                ace = ctypes.c_void_p()
                _win_check(adv.GetAce(dacl, index, ctypes.byref(ace)))
                ace_address = cast(int, ace.value)
                header = AceHeader.from_address(ace_address)
                _win_check(header.size >= ctypes.sizeof(AceHeader) + 8)
                if policy is None:
                    _win_check(
                        header.kind == 0
                        and not header.flags & 0x18
                        and header.mask == 0x1F01FF
                    )
                    _win_check(adv.EqualSid(ctypes.c_void_p(ace_address + 8), sid))
                else:
                    expected_ace = ctypes.c_void_p()
                    _win_check(
                        adv.GetAce(expected_acl, index, ctypes.byref(expected_ace))
                    )
                    expected_address = cast(int, expected_ace.value)
                    expected_header = AceHeader.from_address(expected_address)
                    _win_check(
                        header.kind == 0
                        and header.flags == expected_header.flags
                        and header.mask == expected_header.mask
                        and header.size == expected_header.size
                    )
                    _win_check(
                        adv.EqualSid(
                            ctypes.c_void_p(ace_address + 8),
                            ctypes.c_void_p(expected_address + 8),
                        )
                    )
        finally:
            kernel.LocalFree(descriptor)
