"""Owner-only filesystem boundaries, including the Windows API dispatch."""

from pathlib import Path

import pytest


def test_windows_acl_creation_and_validation_are_required(monkeypatch, tmp_path: Path) -> None:
    from post_pulsar import secure_files

    calls = []
    monkeypatch.setattr(secure_files, "_is_windows", lambda: True)
    monkeypatch.setattr(secure_files, "_windows_secure", lambda path, directory: calls.append(("set", path, directory)))
    monkeypatch.setattr(secure_files, "_windows_validate", lambda path: calls.append(("check", path)))
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
