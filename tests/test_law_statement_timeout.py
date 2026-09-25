"""sql §3.2 / §3.13 guard: ``client.py`` picks no timeout by ``db_type`` and never re-parses a statement it executes."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from slayer.sql import client as sql_client

# The one sqlglot parse allowed in client.py: save-time model classification, never executed.
_PARSE_ALLOWED_IN = {"classify_model_sql"}
_TIMEOUT_SQL = re.compile(
    r"\bSET\s+(LOCAL\s+)?(max_execution_time|max_statement_time|statement_timeout)|getSetting\(",
    re.IGNORECASE,
)


def _tree() -> ast.Module:
    return ast.parse(Path(sql_client.__file__).read_text())


def _str_constants(node: ast.expr) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return [e.value for e in node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
    return []


def _names_db_type(node: ast.expr) -> bool:
    return (isinstance(node, ast.Name) and node.id == "db_type") or (
        isinstance(node, ast.Attribute) and node.attr in {"db_type", "type"}
    )


def test_no_db_type_literal_dispatch() -> None:
    offenders = [
        node.lineno
        for node in ast.walk(_tree())
        if isinstance(node, ast.Compare)
        and _names_db_type(node.left)
        and any(isinstance(op, (ast.Eq, ast.NotEq, ast.In, ast.NotIn)) for op in node.ops)
        and any(_str_constants(c) for c in node.comparators)
    ]
    assert not offenders, f"db_type compared to a literal at lines {offenders}"


def _docstring_ids(tree: ast.Module) -> set[int]:
    return {
        id(body[0].value)
        for n in ast.walk(tree)
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and (body := n.body)
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
    }


def test_no_timeout_statement_text() -> None:
    tree = _tree()
    docstrings = _docstring_ids(tree)
    offenders = [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and _TIMEOUT_SQL.search(node.value)
    ]
    assert not offenders, f"timeout SQL spelled in client.py (belongs in dialects/): {offenders}"


def _is_sqlglot_parse(func: ast.expr) -> bool:
    return (
        isinstance(func, ast.Attribute)
        and func.attr in {"parse", "parse_one", "transpile"}
        and isinstance(func.value, ast.Name)
        and func.value.id == "sqlglot"
    ) or (isinstance(func, ast.Name) and func.id in {"parse_one", "transpile"})


def test_sqlglot_parse_only_in_classification() -> None:
    tree = _tree()
    allowed = [
        (fn.lineno, fn.end_lineno or fn.lineno)
        for fn in ast.walk(tree)
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and fn.name in _PARSE_ALLOWED_IN
    ]
    offenders = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and _is_sqlglot_parse(node.func)
        and not any(lo <= node.lineno <= hi for lo, hi in allowed)
    ]
    assert not offenders, f"sqlglot parse on the execution path at lines {offenders}"


def test_no_statement_rewrite_helpers() -> None:
    names: set[str] = set()
    for n in ast.walk(_tree()):
        if isinstance(n, ast.Name):
            names.add(n.id)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.alias)):
            names.add(n.name)
    assert not names & {"_settings_holder", "_with_ch_statement_timeout", "_exec_clickhouse"}
