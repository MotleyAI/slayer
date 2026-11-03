"""Law (system.arc42.md §3.6): SQL is built as sqlglot AST, never by composing statement text."""

from __future__ import annotations

import ast
import re
import textwrap
from pathlib import Path

import pytest

import slayer

_ROOT = Path(slayer.__file__).parent
_KEYWORDS = (
    "SELECT WITH INSERT UPDATE DELETE MERGE CREATE ALTER DROP TRUNCATE SET USE DESCRIBE EXPLAIN PRAGMA ATTACH "
    "DETACH COPY GRANT REVOKE CALL EXEC EXECUTE SHOW VALUES BEGIN COMMIT ROLLBACK ANALYZE VACUUM REPLACE UPSERT"
).split()
_STATEMENT = re.compile(rf"^[\s(]*(?:{'|'.join(_KEYWORDS)})(?:\s|$)")
_LABEL = re.compile(rf"^[\s(]*(?:{'|'.join(_KEYWORDS)}) \w+:")
_LOG_METHODS = frozenset({"debug", "info", "warning", "warn", "error", "exception", "critical", "log"})
_LOGGERS = frozenset({"logger", "logging", "log", "_log", "_logger", "LOGGER"})
_PARSERS = frozenset({"parse_one", "parse"})


def _name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    return node.attr if isinstance(node, ast.Attribute) else ""


def _leading(node: ast.expr) -> str | None:
    """The literal text a composed string starts with."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.JoinedStr):
        first = node.values[0] if node.values else None
        return first.value if isinstance(first, ast.Constant) and isinstance(first.value, str) else ""
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod)):
        return _leading(node.left)
    return None


def _holes(node: ast.expr) -> int:
    """The number of interpolated operands in a composed string."""
    if isinstance(node, ast.JoinedStr):
        return sum(isinstance(v, ast.FormattedValue) for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return sum(_holes(side) if isinstance(side, (ast.JoinedStr, ast.BinOp)) else
                   (0 if isinstance(side, ast.Constant) else 1) for side in (node.left, node.right))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        return len(node.right.elts) if isinstance(node.right, ast.Tuple) else 1
    if isinstance(node, ast.Call):
        return len(node.args) + len(node.keywords)
    return 0


def _composed(node: ast.AST) -> str | None:
    """The leading literal of ``node`` when it composes a string, else ``None``."""
    if isinstance(node, ast.JoinedStr):
        return _leading(node)
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod)):
        literal = isinstance(node.left, ast.Constant) and isinstance(node.right, ast.Constant)
        return None if literal else _leading(node)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format" \
            and isinstance(node.func.value, ast.Constant) and isinstance(node.func.value.value, str):
        return node.func.value.value
    return None


def _exempt_call(call: ast.Call, *, arg: ast.AST) -> bool:
    func = call.func
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        if func.attr in _LOG_METHODS and func.value.id in _LOGGERS:
            return True
        if func.attr == "warn" and func.value.id == "warnings":
            return True
    is_parser = _name(func) in _PARSERS and (
        isinstance(func, ast.Name) or (isinstance(func, ast.Attribute) and _name(func.value) == "sqlglot"))
    text_arg = call.args[0] if call.args else next((k.value for k in call.keywords if k.arg == "sql"), None)
    return is_parser and text_arg is arg and isinstance(arg, ast.expr) and _holes(arg) == 1


def violations(source: str, *, path: str = "<src>") -> list[str]:
    """``path:line`` of every composed string that starts a SQL statement in ``source``."""
    tree = ast.parse(source)
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(parents.get(node), ast.BinOp) and _composed(parents[node]) is not None:
            continue  # the outermost composition is the one judged
        text = _composed(node)
        if text is None or not _STATEMENT.match(text) or _LABEL.match(text):
            continue
        chain: list[ast.AST] = [node]
        while chain[-1] in parents:
            chain.append(parents[chain[-1]])
        chain = [n for n in chain if not isinstance(n, ast.keyword)]
        if any(isinstance(n, ast.Raise) for n in chain):
            continue
        if any(isinstance(n, ast.Call) and _exempt_call(n, arg=prev) for prev, n in zip(chain, chain[1:])):
            continue
        found.append(f"{path}:{getattr(node, 'lineno', 0)}")
    return found


def _tree_violations() -> list[str]:
    out: list[str] = []
    for file in sorted(_ROOT.rglob("*.py")):
        out += violations(file.read_text(encoding="utf-8"), path=str(file.relative_to(_ROOT.parent)))
    return out


def test_no_statement_text_is_composed() -> None:
    assert _tree_violations() == []


def _flags(code: str) -> bool:
    return bool(violations(textwrap.dedent(code)))


@pytest.mark.parametrize("code", [
    "q = f'SELECT * FROM {t}'",
    "q = 'SELECT * FROM ' + t",
    "q = 'SELECT * FROM %s' % t",
    "q = 'SELECT * FROM {}'.format(t)",
    "q = f'  (SELECT a FROM {t})'",
    "q = 'WITH x AS (' + inner + ') SELECT 1'",
    "q = f'SET LOCAL statement_timeout = {ms}'",
    "q = f'INSERT INTO {t} VALUES (?)'",
    "q = f'USE {db}'",
    "q = f'SELECT\\n{a}'",
    "q = 'SELECT ' + a + ' FROM ' + b",
    "sqlglot.parse_one(f'SELECT {a} FROM {b}')",
    "sqlglot.transpile(f'SELECT {a}')",
    "conn.execute(f'SELECT {a}')",
    "print(f'SELECT {a}')",
    "catalog.info(f'SELECT {a}')",
    "warning_service.warn(f'SELECT {a}')",
    "self.logger.helper(f'SELECT {a}')",
])
def test_planted_composition_is_flagged(code: str) -> None:
    assert _flags(code), code


@pytest.mark.parametrize("code", [
    "q = 'SELECT 1'",
    "raise ValueError(f'SELECT {x} is not allowed')",
    "raise ValueError('SELECT ' + x)",
    "logger.warning(f'SELECT probe failed: {e}')",
    "logging.info('SELECT %s', x)",
    "warnings.warn(f'SELECT {x} skipped')",
    "q = f'SELECT list: {x}'",
    "sqlglot.parse_one(f'SELECT {fragment}')",
    "parse_one('SELECT ' + fragment, dialect=d)",
    "sqlglot.parse(f'SELECT {fragment}')",
    "sqlglot.parse_one(sql=f'SELECT {fragment}')",
    "q = f'Select {x} from the list'",
    "q = f'selected {x}'",
    "q = f'SETTINGS {x}'",
    "q = f'{a} SELECT {b}'",
    "q = f'Missing column {x}'",
])
def test_exempt_shapes_pass(code: str) -> None:
    assert not _flags(code), code
