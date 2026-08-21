"""Docs must not advertise a `motley-slayer[duckdb]` extra: duckdb is a core dep, so the extra is warned about and ignored."""
from __future__ import annotations

import tomllib
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
_DOCS = _REPO_ROOT / "docs"


def _doc_files() -> list[Path]:
    return sorted(_DOCS.rglob("*.md"))


@pytest.mark.parametrize(
    "path", _doc_files(), ids=lambda p: str(p.relative_to(_REPO_ROOT))
)
def test_no_doc_advertises_a_duckdb_extra(path: Path) -> None:
    """the exact string a user would copy-paste."""
    text = path.read_text()
    assert "motley-slayer[duckdb]" not in text, (
        f"{path.relative_to(_REPO_ROOT)} advertises a non-existent extra"
    )


def test_duckdb_remains_a_core_dependency() -> None:
    """Pins the docs premise: if duckdb ever becomes an extra, this fails so the docs must change back."""
    data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())
    deps = data["tool"]["poetry"]["dependencies"]

    for name in ("duckdb", "duckdb-engine"):
        spec = deps[name]
        assert not isinstance(spec, dict) or not spec.get("optional"), (
            f"{name} became optional; docs/getting-started/index.md and "
            f"docs/configuration/datasources.md must be updated to match"
        )

    extras = data["tool"]["poetry"].get("extras", {})
    assert "duckdb" not in extras, (
        "a duckdb extra now exists; the docs fix here assumed it did not"
    )


def test_getting_started_marks_duckdb_as_included() -> None:
    text = (_DOCS / "getting-started" / "index.md").read_text()
    assert "DuckDB" in text
    row = next(
        line for line in text.splitlines()
        if line.strip().startswith("|") and "DuckDB" in line
    )
    assert "[duckdb]" not in row


def test_datasources_reference_marks_duckdb_as_builtin() -> None:
    text = (_DOCS / "configuration" / "datasources.md").read_text()
    row = next(
        line for line in text.splitlines()
        if line.strip().startswith("|") and "`duckdb`" in line
    )
    assert "[duckdb]" not in row
