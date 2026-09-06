"""Owner-only filesystem boundaries, including the Windows API dispatch."""

from pathlib import Path

import pytest


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
