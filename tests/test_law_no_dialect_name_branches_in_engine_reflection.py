"""Law (python.sql.arc42.md §3.2): engine-side reflection never branches on a dialect name.

Passing a dialect name through to ``schema_scope`` (schema identity) is allowed; comparing it,
or keying a table by it, is not.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_FILES = ("slayer/engine/ingestion.py", "slayer/engine/introspect_utils.py")
_BACKENDS = frozenset({
    "sqlite", "postgres", "postgresql", "duckdb", "mysql", "mariadb", "snowflake", "clickhouse",
    "bigquery", "mssql", "tsql", "sqlserver", "trino", "presto", "redshift", "oracle",
    "databricks", "spark", "athena",
})


def _is_backend_literal(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str) and node.value.lower() in _BACKENDS
    if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        return any(_is_backend_literal(e) for e in node.elts)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {
        "frozenset", "set", "tuple",
    }:
        return any(_is_backend_literal(a) for a in node.args)
    return False


def _is_dialect_name(node: ast.AST) -> bool:
    """``<x>.dialect.name`` or ``getattr(<x>.dialect | getattr(<x>, "dialect"), "name", ...)``."""
    if isinstance(node, ast.Attribute) and node.attr == "name":
        inner = node.value
        return isinstance(inner, ast.Attribute) and inner.attr == "dialect"
    if (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[1], ast.Constant) and node.args[1].value == "name"
    ):
        target = node.args[0]
        if isinstance(target, ast.Attribute) and target.attr == "dialect":
            return True
        return (
            isinstance(target, ast.Call) and isinstance(target.func, ast.Name)
            and target.func.id == "getattr" and len(target.args) >= 2
            and isinstance(target.args[1], ast.Constant) and target.args[1].value == "dialect"
        )
    return False


def _offenders(rel: str) -> list[str]:
    tree = ast.parse((_ROOT / rel).read_text(), filename=rel)
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            operands = [node.left, *node.comparators]
            if any(_is_backend_literal(o) or _is_dialect_name(o) for o in operands):
                out.append(f"{rel}:{node.lineno}: dialect-name comparison")
        elif isinstance(node, ast.Dict):
            if any(k is not None and _is_backend_literal(k) for k in node.keys):
                out.append(f"{rel}:{node.lineno}: dialect-name-keyed table")
        elif isinstance(node, ast.Subscript):
            if _is_backend_literal(node.slice):
                out.append(f"{rel}:{node.lineno}: dialect-name-keyed lookup")
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            if node.value is not None and _is_backend_literal(node.value):
                out.append(f"{rel}:{node.lineno}: dialect-name set")
    return out


@pytest.mark.parametrize("rel", _FILES)
def test_no_dialect_name_branches(rel: str) -> None:
    assert _offenders(rel) == []


@pytest.mark.parametrize(
    ("source", "flagged"),
    [
        ('if e.dialect.name != "sqlite": pass', True),
        ('if getattr(getattr(e, "dialect", None), "name", "") == "bigquery": pass', True),
        ("if e.dialect.name in KNOWN: pass", True),
        ('X = frozenset({"sqlite", "bigquery"})', True),
        ('T = {"mysql": "q"}', True),
        ('T["mariadb"] = 1', True),
        ('ok = (ds.type or "").lower() == "sqlite"', True),
        ("ref = schema_ref_from_token(t, dialect_name=e.dialect.name)", False),
        ('sql = node.sql(dialect="bigquery")', False),
    ],
)
def test_detector(source: str, flagged: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    probe = tmp_path / "probe.py"
    probe.write_text(source)
    monkeypatch.setattr(f"{__name__}._ROOT", tmp_path)
    assert bool(_offenders("probe.py")) is flagged
