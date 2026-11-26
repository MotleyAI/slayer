"""``duckdb`` is capped below 2.0 in every dependency table that declares it."""

from __future__ import annotations

import pathlib
import tomllib
from importlib.metadata import version
from typing import Any

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

PYPROJECT = pathlib.Path(__file__).parent.parent / "pyproject.toml"


def _tables() -> dict[str, dict[str, Any]]:
    with open(PYPROJECT, "rb") as fh:
        poetry = tomllib.load(fh)["tool"]["poetry"]
    groups = {f"group.{name}": group["dependencies"] for name, group in poetry.get("group", {}).items()}
    return {name: deps for name, deps in {"dependencies": poetry["dependencies"], **groups}.items() if "duckdb" in deps}


def _specifier(deps: dict[str, Any]) -> SpecifierSet:
    spec = deps["duckdb"]
    return SpecifierSet(spec["version"] if isinstance(spec, dict) else spec)


TABLES = _tables()


def test_the_main_dependencies_declare_duckdb() -> None:
    assert "dependencies" in TABLES


@pytest.mark.parametrize("table", sorted(TABLES))
@pytest.mark.parametrize("admitted", ["1.1.0", "1.5.2", "1.9.9"])
def test_the_tested_1_x_line_is_admitted(table: str, admitted: str) -> None:
    assert Version(admitted) in _specifier(TABLES[table])


@pytest.mark.parametrize("table", sorted(TABLES))
@pytest.mark.parametrize("excluded", ["1.0.0", "2.0.0", "2.0.1"])
def test_untested_lines_are_excluded(table: str, excluded: str) -> None:
    assert Version(excluded) not in _specifier(TABLES[table])


def test_the_installed_duckdb_is_in_range() -> None:
    assert Version(version("duckdb")) in _specifier(TABLES["dependencies"])
