"""Law (sql.arc42.md §3.2): the policy rewrite and the engine branch on dialect capabilities, never on names."""

from __future__ import annotations

import ast
import inspect
from types import ModuleType

import pytest

import slayer.engine.query_engine as query_engine_module
import slayer.sql.session_policy as session_policy_module
from slayer.sql.dialects import SQLGLOT_NAMES


def _is_dialect_ref(node: ast.expr) -> bool:
    name = node.id if isinstance(node, ast.Name) else node.attr if isinstance(node, ast.Attribute) else ""
    return "dialect" in name.lower() or name == "sqlglot_name"


def _dialect_name_uses(module: ModuleType) -> list[str]:
    names = {n.lower() for n in SQLGLOT_NAMES}
    hits: list[str] = []
    for node in ast.walk(ast.parse(inspect.getsource(module))):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.lower() == "clickhouse":
            hits.append(f"line {node.lineno}: literal {node.value!r}")
        if isinstance(node, ast.Compare):
            operands = [node.left, *node.comparators]
            named = any(
                isinstance(o, ast.Constant) and isinstance(o.value, str) and o.value.lower() in names
                for o in operands
            )
            if named and any(_is_dialect_ref(o) for o in operands):
                hits.append(f"line {node.lineno}: {ast.unparse(node)}")
    return hits


@pytest.mark.parametrize("module", [session_policy_module, query_engine_module], ids=lambda m: m.__name__)
def test_no_dialect_name_branching(module: ModuleType) -> None:
    assert _dialect_name_uses(module) == []
