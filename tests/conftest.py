"""Repository-wide test safety fixtures."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _deny_outbound_sockets(socket_disabled: None) -> None:
    """Make every test opt in to a fully mocked transport instead of the network."""
