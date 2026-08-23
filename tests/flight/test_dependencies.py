"""Packaging contract for the Flight SQL runtime."""

from __future__ import annotations

import tomllib
from pathlib import Path

from packaging.specifiers import SpecifierSet
from packaging.version import Version


def test_flight_extra_includes_compatible_runtime_dependencies() -> None:
    data = tomllib.loads((Path(__file__).resolve().parents[2] / "pyproject.toml").read_text())
    dependencies = data["tool"]["poetry"]["dependencies"]
    extras = data["tool"]["poetry"]["extras"]

    protobuf = dependencies["protobuf"]
    assert protobuf["optional"] is True
    assert Version("6.31.1") in SpecifierSet(protobuf["version"])
    assert {"pyarrow", "protobuf"} <= set(extras["flight"])
    assert {"pyarrow", "protobuf"} <= set(extras["all"])
