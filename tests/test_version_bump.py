"""DEV-1549: pin the package version bump.

The compact-default flip is documented as a breaking change. This test
fails until ``pyproject.toml`` is bumped to ``0.7.3``.
"""

from __future__ import annotations

from importlib.metadata import version


def test_package_version_is_0_7_3() -> None:
    assert version("motley-slayer") == "0.7.3"
