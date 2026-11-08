"""SQLAlchemy's async runtime must be installed on every supported platform."""

import tomllib
from pathlib import Path


def test_greenlet_is_a_core_dependency() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    dependencies = tomllib.loads(pyproject.read_text())["tool"]["poetry"]["dependencies"]

    greenlet = dependencies["greenlet"]
    if isinstance(greenlet, dict):
        assert not greenlet.get("optional")
        assert not greenlet.get("markers")
