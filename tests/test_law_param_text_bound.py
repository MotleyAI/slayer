"""Law (engine.arc42.md §3.1): aggregation parameter text is analysed once, by the binder.

No module of ``slayer/engine``, ``slayer/sql`` or ``slayer/ir`` reads
``Aggregation.params`` (hence ``AggregationParam.sql``) outside the binder module,
save-time validation and the binder's parameter-name lookup; the renderer never
receives a definition's parameters and never renders a string kwarg as SQL.
"""

from __future__ import annotations

import ast
import typing
from pathlib import Path
from typing import Iterator, Optional, Tuple

import pytest

from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Aggregation
from slayer.sql.generator import AggRenderSpec, SQLGenerator
from tests._dev1901_fixtures import dev1901_models

_ROOT = Path(__file__).resolve().parents[1]
_SCANNED = ("slayer/engine", "slayer/sql", "slayer/ir")
_BINDER = "slayer/engine/param_binding.py"
#: (module, enclosing function) pairs allowed to read ``.params``.
_ALLOWED = {
    ("slayer/sql/sql_template.py", "check_aggregation_definition"),
    ("slayer/engine/binding.py", "_declared_agg_param_names"),
}


def _violations(source: str, *, module: str) -> Iterator[str]:
    """``.params`` reads and ``AggregationParam`` imports in ``source``."""
    tree = ast.parse(source)

    def walk(node: ast.AST, func: Optional[str]) -> Iterator[Tuple[ast.AST, Optional[str]]]:
        for child in ast.iter_child_nodes(node):
            inner = child.name if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) else func
            yield child, inner
            yield from walk(child, inner)

    for node, func in walk(tree, None):
        if _reads_params(node) and (module, func) not in _ALLOWED:
            yield f"{module}:{node.lineno} ({func}) reads .params"
        if isinstance(node, ast.ImportFrom) and any(
                a.name == "AggregationParam" for a in node.names):
            yield f"{module}:{node.lineno} imports AggregationParam"


def _reads_params(node: ast.AST) -> bool:
    """``x.params`` or ``getattr(x, "params", ...)``."""
    if isinstance(node, ast.Attribute):
        return node.attr == "params"
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "getattr" and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant) and node.args[1].value == "params")


def _scanned_modules() -> Iterator[Tuple[str, str]]:
    for pkg in _SCANNED:
        for path in sorted((_ROOT / pkg).rglob("*.py")):
            rel = path.relative_to(_ROOT).as_posix()
            if rel != _BINDER:
                yield rel, path.read_text()


def test_binder_module_exists() -> None:
    assert (_ROOT / _BINDER).is_file()


def test_parameter_text_is_read_only_by_the_binder() -> None:
    found = [v for rel, src in _scanned_modules() for v in _violations(src, module=rel)]
    assert found == []


@pytest.mark.parametrize("snippet,expected", [
    ("def f(agg):\n    return agg.params\n", 1),
    ("def check_aggregation_definition(agg):\n    return agg.params\n", 0),
    ("from slayer.core.models import AggregationParam\n", 1),
    ("def f(agg):\n    return agg.formula\n", 0),
    ("def f(agg):\n    return getattr(agg, 'params', [])\n", 1),
])
def test_detector(snippet: str, expected: int) -> None:
    module = "slayer/sql/sql_template.py"
    assert len(list(_violations(snippet, module=module))) == expected


def test_render_spec_carries_no_definition() -> None:
    for name, field in AggRenderSpec.model_fields.items():
        hint = field.annotation
        assert Aggregation not in {hint, *typing.get_args(hint)}, name


def test_a_string_kwarg_is_never_rendered_as_sql() -> None:
    orders = next(m for m in dev1901_models() if m.name == "orders")
    key = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="wsum",
                       kwargs=(("weight", "store_no"),))
    with pytest.raises((TypeError, ValueError), match="weight"):
        _render(key, source_model=orders)


def _render(key: AggregateKey, *, source_model) -> None:
    gen = SQLGenerator(dialect="postgres")
    gen._build_agg(gen._build_agg_render_spec_from_planned(
        slot=None, key=key, source_model=source_model, source_relation="orders",
        full_alias="orders.amount_wsum"))
