"""LAW one relation door (python.sql P13): a model reaches SQL only through ``_emit_relation``, as its filtered dataset.

AST scan of ``slayer/sql/``: the backing-relation builders are called only from ``_backing_relation``,
``_backing_relation`` only from ``_emit_relation``, and a model's ``.filters`` is read only by the door
(plus the variable-surface collector, which emits nothing).
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SQL = REPO / "slayer" / "sql"

DOOR = "_emit_relation"
BACKING = "_backing_relation"
BUILDERS = frozenset({"_to_table", "_embed_model_sql"})
#: Readers of ``.filters`` allowed beside the door: the variable-surface collector.
FILTER_READERS = frozenset({DOOR, "_user_authored_exemptions"})
#: ``.filters`` receivers that are a plan's or render context's filters, never a model's.
PLAN_RECEIVERS = frozenset({"lowered", "ctx"})


class _Site(ast.NodeVisitor):
    """``(file:line, enclosing function, node)`` for every call / attribute read of interest."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.stack: list[str] = []
        self.calls: list[tuple[str, str, str]] = []
        self.filter_reads: list[tuple[str, str, ast.Attribute]] = []

    def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = _function
    visit_AsyncFunctionDef = _function

    def _where(self, node: ast.AST) -> str:
        return f"{self.path.relative_to(REPO)}:{getattr(node, 'lineno', 0)}"

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else func.id if isinstance(func, ast.Name) else None
        if name is not None:
            self.calls.append((self._where(node), self.stack[-1] if self.stack else "<module>", name))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr == "filters" and isinstance(node.ctx, ast.Load):
            self.filter_reads.append((self._where(node), self.stack[-1] if self.stack else "<module>", node))
        self.generic_visit(node)


def _sites() -> list[_Site]:
    sites = []
    for path in sorted(SQL.rglob("*.py")):
        site = _Site(path)
        site.visit(ast.parse(path.read_text(encoding="utf-8")))
        sites.append(site)
    return sites


def _calls_to(names: frozenset[str]) -> list[tuple[str, str, str]]:
    return [c for s in _sites() for c in s.calls if c[2] in names]


def _is_plan_receiver(node: ast.expr) -> bool:
    if isinstance(node, ast.Name):
        return node.id in PLAN_RECEIVERS
    if isinstance(node, ast.Attribute):
        return _is_plan_receiver(node.value)
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_lower_positions"


def test_builders_only_in_the_backing_relation():
    stray = [(where, fn) for where, fn, _ in _calls_to(BUILDERS) if fn != BACKING]
    assert _calls_to(BUILDERS), "no backing-relation builder call found"
    assert stray == [], f"backing-relation builders called outside {BACKING}: {stray}"


def test_backing_relation_only_through_the_door():
    calls = _calls_to(frozenset({BACKING}))
    assert calls, f"{BACKING} is never called"
    assert [(where, fn) for where, fn, _ in calls if fn != DOOR] == []


def test_model_filters_read_only_by_the_door():
    reads = [r for s in _sites() for r in s.filter_reads if not _is_plan_receiver(r[2].value)]
    assert [(where, fn) for where, fn, _ in reads if fn not in FILTER_READERS] == []
    assert any(fn == DOOR for _, fn, _ in reads), f"{DOOR} never reads a model's filters"
