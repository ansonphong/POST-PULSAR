"""Owner-only filesystem boundaries, including the Windows API dispatch."""

from pathlib import Path
import os
from types import SimpleNamespace

import pytest


@pytest.fixture
def hardened_policy(monkeypatch):
    import grp
    import pwd
    from post_pulsar import secure_files

    core, agent, group = os.getuid(), os.getuid() + 12345, os.getgid()
    accounts = [
        SimpleNamespace(pw_uid=core, pw_name="fixture-core", pw_gid=group),
        SimpleNamespace(pw_uid=agent, pw_name="fixture-agent", pw_gid=group),
    ]
    monkeypatch.setattr(pwd, "getpwall", lambda: accounts)
    monkeypatch.setattr(
        pwd, "getpwuid", lambda uid: next(a for a in accounts if a.pw_uid == uid)
    )
    monkeypatch.setattr(
        grp,
        "getgrgid",
        lambda gid: SimpleNamespace(gr_gid=group, gr_mem=["fixture-agent"]),
    )
    monkeypatch.setattr(os, "getgrouplist", lambda name, gid: [group])
    monkeypatch.setattr(os, "getgroups", lambda: [group])
    monkeypatch.setattr(os, "getegid", lambda: group)
    return secure_files.RecordPolicy(str(core), str(agent), group)


def test_hardened_posix_replacement_preserves_exact_read_policy(
    tmp_path, monkeypatch, hardened_policy
):
    from post_pulsar import secure_files

    # pytest's private ancestor directories are deliberately not shared OS accounts.
    monkeypatch.setattr(secure_files, "_validate_ancestors", lambda path, policy: None)
    path = tmp_path / "discovery" / "capability"
    secure_files.atomic_owner_write(path, b"fixture-first", policy=hardened_policy)
    secure_files.atomic_owner_write(path, b"fixture-second", policy=hardened_policy)
    assert path.stat().st_mode & 0o7777 == 0o640
    assert path.parent.stat().st_mode & 0o7777 == 0o750
    assert path.stat().st_uid == int(hardened_policy.core_principal)
    assert path.stat().st_gid == hardened_policy.agent_group
    monkeypatch.setattr(os, "getuid", lambda: int(hardened_policy.agent_principal))
    monkeypatch.setattr(os, "geteuid", lambda: int(hardened_policy.agent_principal))
    secure_files.assert_owner_file(path, policy=hardened_policy)
    with pytest.raises(secure_files.SecureFileError):
        secure_files.atomic_owner_write(
            path, b"fixture-forbidden", policy=hardened_policy
        )
    assert path.read_bytes() == b"fixture-second"


@pytest.mark.parametrize(
    "target,mode",
    [("file", 0o660), ("file", 0o600), ("directory", 0o770), ("directory", 0o700)],
)
def test_hardened_posix_rejects_widened_or_stale_modes(
    tmp_path, monkeypatch, hardened_policy, target, mode
):
    from post_pulsar import secure_files

    monkeypatch.setattr(secure_files, "_validate_ancestors", lambda path, policy: None)
    path = tmp_path / "discovery" / "capability"
    secure_files.atomic_owner_write(path, b"fixture", policy=hardened_policy)
    (path if target == "file" else path.parent).chmod(mode)
    with pytest.raises(secure_files.SecureFileError):
        secure_files.assert_owner_file(path, policy=hardened_policy)
    with pytest.raises(secure_files.SecureFileError):
        secure_files.atomic_owner_write(
            path, b"fixture-forbidden", policy=hardened_policy
        )


def test_hardened_posix_rejects_same_identity_extra_members_and_private_ancestors(
    tmp_path, monkeypatch, hardened_policy
):
    import dataclasses
    import grp
    from post_pulsar import secure_files

    with pytest.raises(secure_files.SecureFileError):
        dataclasses.replace(
            hardened_policy, agent_principal=hardened_policy.core_principal
        ).validate()
    monkeypatch.setattr(
        grp,
        "getgrgid",
        lambda gid: SimpleNamespace(
            gr_gid=gid, gr_mem=["fixture-agent", "fixture-stranger"]
        ),
    )
    with pytest.raises(secure_files.SecureFileError):
        hardened_policy.validate()


def test_hardened_posix_requires_traversable_trusted_ancestors(
    tmp_path, hardened_policy
):
    from post_pulsar import secure_files

    tmp_path.chmod(0o700)
    with pytest.raises(secure_files.SecureFileError):
        secure_files.atomic_owner_write(
            tmp_path / "discovery" / "capability", b"fixture", policy=hardened_policy
        )


@pytest.mark.parametrize(
    "directory,agent_read,mask",
    [(False, True, "0x120089"), (True, True, "0x1200a9"), (False, False, None)],
)
def test_windows_hardened_descriptor_has_only_core_and_minimum_agent_aces(
    monkeypatch, directory, agent_read, mask
):
    from post_pulsar import secure_files

    policy = secure_files.RecordPolicy("S-1-5-21-1-2-3-1001", "S-1-5-21-1-2-3-1002")
    monkeypatch.setattr(secure_files, "_is_windows", lambda: True)
    monkeypatch.setattr(
        secure_files, "_windows_current_sid", lambda: policy.core_principal
    )
    monkeypatch.setattr(secure_files, "_windows_validate_user_sid", lambda sid: None)
    sddl = secure_files._windows_sddl(directory, policy, agent_read=agent_read)
    assert f"O:{policy.core_principal}D:P(A;;FA;;;{policy.core_principal})" in sddl
    if mask:
        assert sddl.endswith(f"(A;;{mask};;;{policy.agent_principal})")
    else:
        assert policy.agent_principal not in sddl


def test_windows_acl_creation_and_validation_are_required(
    monkeypatch, tmp_path: Path
) -> None:
    from post_pulsar import secure_files

    calls = []
    monkeypatch.setattr(secure_files, "_is_windows", lambda: True)
    monkeypatch.setattr(
        secure_files,
        "_windows_secure",
        lambda path, directory: calls.append(("set", path, directory)),
    )
    monkeypatch.setattr(
        secure_files, "_windows_validate", lambda path: calls.append(("check", path))
    )
    monkeypatch.setattr(
        secure_files, "_windows_create_directory", lambda path: path.mkdir()
    )
    directory = tmp_path / "private"
    secure_files.secure_directory(directory)
    secret = directory / "secret"
    secure_files.atomic_owner_write(secret, b"sensitive")
    secure_files.assert_owner_file(secret)
    assert ("set", directory, True) in calls
    assert any(call[0] == "set" and call[2] is False for call in calls)
    assert ("check", secret) in calls


def test_owner_write_rejects_linked_targets(tmp_path: Path) -> None:
    from post_pulsar.secure_files import SecureFileError, atomic_owner_write

    target = tmp_path / "target"
    target.write_bytes(b"original")
    (tmp_path / "link").symlink_to(target)
    with pytest.raises(SecureFileError):
        atomic_owner_write(tmp_path / "link", b"changed")
    assert target.read_bytes() == b"original"


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "agent-write",
        "agent-delete",
        "agent-acl-write",
        "foreign-agent",
        "core-read",
        "extra",
        "inheritance",
        "unprotected",
        "owner",
        "missing-agent",
        "directory-mask",
    ],
)
def test_windows_hardened_native_acl_validation_rejects_drift(
    monkeypatch, tmp_path, fault
):
    import ctypes
    import struct
    from contextlib import contextmanager
    from post_pulsar import secure_files

    def ace(mask, sid, flags=0):
        return ctypes.create_string_buffer(
            struct.pack("<BBHIQ", 0, flags, 16, mask, sid)
        )

    expected = [ace(0x1F01FF, 1), ace(0x120089, 2)]
    actual = [ace(0x1F01FF, 1), ace(0x120089, 2)]
    if fault in {"agent-write", "agent-delete", "agent-acl-write"}:
        actual[1] = ace(
            0x120089
            | {"agent-write": 2, "agent-delete": 0x10000, "agent-acl-write": 0x40000}[
                fault
            ],
            2,
        )
    elif fault == "foreign-agent":
        actual[1] = ace(0x120089, 3)
    elif fault == "core-read":
        actual[0] = ace(0x120089, 1)
    elif fault == "extra":
        actual.append(ace(0x120089, 3))
    elif fault == "missing-agent":
        actual.pop()
    elif fault == "inheritance":
        actual[1] = ace(0x120089, 2, 0x10)
    elif fault == "directory-mask":
        actual[1] = ace(0x1200A9, 2)
    wrong_owner = ace(0x1F01FF, 3)
    freed = []

    class API:
        def GetNamedSecurityInfoW(
            self, path, kind, flags, owner, group, acl, sacl, descriptor
        ):
            owner._obj.value = (
                ctypes.addressof(wrong_owner if fault == "owner" else expected[0]) + 8
            )
            acl._obj.value, descriptor._obj.value = 20, 30
            return 0

        def EqualSid(self, left, right):
            return (
                ctypes.c_uint64.from_address(left.value).value
                == ctypes.c_uint64.from_address(right.value).value
            )

        def GetSecurityDescriptorControl(self, descriptor, control, revision):
            control._obj.value = 0 if fault == "unprotected" else 0x1000
            return True

        def GetAclInformation(self, acl, size, length, info):
            size._obj.count = len(actual)
            return True

        def GetSecurityDescriptorDacl(self, descriptor, present, acl, defaulted):
            present._obj.value, acl._obj.value = 1, 10
            return True

        def GetAce(self, acl, index, pointer):
            pointer._obj.value = ctypes.addressof(
                (expected if acl.value == 10 else actual)[index]
            )
            return True

        def LocalFree(self, descriptor):
            freed.append(descriptor.value)

    @contextmanager
    def descriptor(*args, **kwargs):
        api = API()
        yield api, api, None, ctypes.c_void_p(ctypes.addressof(expected[0]) + 8)

    monkeypatch.setattr(secure_files, "_windows_descriptor", descriptor)
    policy = secure_files.RecordPolicy("S-1-5-21-1-2-3-1001", "S-1-5-21-1-2-3-1002")
    if fault is None:
        secure_files._windows_validate(tmp_path / "fixture", policy=policy)
    else:
        with pytest.raises(secure_files.SecureFileError):
            secure_files._windows_validate(tmp_path / "fixture", policy=policy)
    assert freed == [30]


def test_hardened_posix_rejects_extended_acl_and_wrong_owner_group(
    tmp_path, monkeypatch, hardened_policy
):
    from post_pulsar import secure_files

    monkeypatch.setattr(secure_files, "_validate_ancestors", lambda path, policy: None)
    path = tmp_path / "discovery" / "fixture"
    secure_files.atomic_owner_write(path, b"fixture", policy=hardened_policy)
    actual_lstat = Path.lstat
    for changed in (
        {"st_uid": int(hardened_policy.agent_principal)},
        {"st_gid": hardened_policy.agent_group + 1},
    ):

        def metadata(candidate):
            original = actual_lstat(candidate)
            return (
                SimpleNamespace(
                    **{
                        name: changed.get(name, getattr(original, name))
                        for name in ("st_uid", "st_gid", "st_mode", "st_nlink")
                    }
                )
                if candidate == path
                else original
            )

        with monkeypatch.context() as patch:
            patch.setattr(Path, "lstat", metadata)
            with pytest.raises(secure_files.SecureFileError):
                secure_files.assert_owner_file(path, policy=hardened_policy)
    monkeypatch.setattr(os, "getxattr", lambda *args, **kwargs: b"fixture-extended-acl")
    with pytest.raises(secure_files.SecureFileError):
        secure_files.atomic_owner_write(
            path, b"fixture-forbidden", policy=hardened_policy
        )


def test_windows_hardened_identity_validation_fails_closed(monkeypatch):
    import dataclasses
    from post_pulsar import secure_files

    policy = secure_files.RecordPolicy("S-1-5-21-1-2-3-1001", "S-1-5-21-1-2-3-1002")
    monkeypatch.setattr(secure_files, "_is_windows", lambda: True)
    monkeypatch.setattr(secure_files, "_windows_validate_user_sid", lambda sid: None)
    monkeypatch.setattr(
        secure_files, "_windows_current_sid", lambda: policy.agent_principal
    )
    policy.validate()
    with pytest.raises(secure_files.SecureFileError):
        policy.validate(write=True)
    with pytest.raises(secure_files.SecureFileError):
        dataclasses.replace(policy, agent_principal=policy.core_principal).validate()
    monkeypatch.setattr(
        secure_files, "_windows_current_sid", lambda: "S-1-5-21-1-2-3-1003"
    )
    with pytest.raises(secure_files.SecureFileError):
        policy.validate()


def test_windows_hardened_native_identity_unavailable_is_an_error(monkeypatch):
    from post_pulsar import secure_files

    def unavailable():
        raise OSError("fixture-native-api-unavailable")

    monkeypatch.setattr(secure_files, "_windows_api", unavailable)
    with pytest.raises(secure_files.SecureFileError):
        secure_files._windows_validate_user_sid("S-1-5-21-1-2-3-1001")


@pytest.mark.parametrize(
    "owner_matches,protected,ace_count",
    [(False, True, 1), (True, False, 1), (True, True, 2)],
)
def test_windows_acl_validator_rejects_foreign_owner_inheritance_and_extra_grants(
    monkeypatch, tmp_path: Path, owner_matches: bool, protected: bool, ace_count: int
) -> None:
    from contextlib import contextmanager
    from post_pulsar import secure_files

    freed = []

    class API:
        def GetNamedSecurityInfoW(
            self, path, kind, flags, owner, group, acl, sacl, descriptor
        ):
            owner._obj.value = 1
            acl._obj.value = 2
            descriptor._obj.value = 3
            return 0

        def EqualSid(self, left, right):
            return owner_matches

        def GetSecurityDescriptorControl(self, descriptor, control, revision):
            control._obj.value = 0x1000 if protected else 0
            return True

        def GetAclInformation(self, acl, size, length, info):
            size._obj.count = ace_count
            return True

        def LocalFree(self, descriptor):
            freed.append(descriptor.value)

    @contextmanager
    def descriptor(directory):
        api = API()
        yield api, api, None, 1

    monkeypatch.setattr(secure_files, "_windows_descriptor", descriptor)
    with pytest.raises(secure_files.SecureFileError):
        secure_files._windows_validate(tmp_path / "secret")
    assert freed == [3]
