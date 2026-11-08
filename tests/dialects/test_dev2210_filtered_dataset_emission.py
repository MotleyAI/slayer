"""Every dialect renders a model's filtered dataset: a derived table, inlined to-one joins, an ``EXISTS`` association."""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr

from tests._dev2210_fixtures import render

DIALECTS = ["postgres", "duckdb", "sqlite", "clickhouse", "bigquery", "tsql", "trino", "mysql", "snowflake"]

SHAPES: dict[str, dict[str, Any]] = {
    "filtered-root": {"source_model": "done_orders", "dimensions": ["id"]},
    "inlined-to-one-filter": {"source_model": "returns_of_done", "dimensions": ["id"]},
    "association-filter": {"source_model": "ups_orders", "dimensions": ["id"]},
    "join-target": {"source_model": "returns", "dimensions": ["id", "done_orders.status"]},
    "association-join-target": {"source_model": "returns", "dimensions": ["id", "ups_orders.id"]},
    "pushed-down": {"source_model": "orders", "dimensions": ["id"], "filters": ["late_returns.id is null"]},
    "forced-association": {"source_model": "returns_of_done", "dimensions": ["id"]},
}


def _tree(shape: str, dialect: str) -> Expr:
    sql = render(SHAPES[shape], dialect=dialect, force_association=shape == "forced-association")
    statements = sqlglot.parse(sql, read=dialect)
    assert len(statements) == 1, sql
    assert statements[0] is not None, sql
    return statements[0]


def _mentions(node: Expr, literal: str) -> bool:
    return any(isinstance(n, exp.Literal) and n.this == literal for n in node.walk())


def _restricting_selects(tree: Expr, literal: str) -> list[exp.Select]:
    """SELECTs whose own WHERE tests ``literal``."""
    return [
        s for s in tree.find_all(exp.Select)
        if (where := s.args.get("where")) is not None and _mentions(where, literal)
    ]


def _derived_tables(tree: Expr, literal: str) -> list[exp.Select]:
    """Restricting SELECTs that are derived tables."""
    return [s for s in _restricting_selects(tree, literal) if isinstance(s.parent, exp.Subquery)]


def _exists(tree: Expr, literal: str) -> list[exp.Exists]:
    return [e for e in tree.find_all(exp.Exists) if _mentions(e, literal)]


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("shape", list(SHAPES))
def test_renders_valid_sql(shape, dialect):
    _tree(shape, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
class TestShapes:
    def test_filtered_root_is_a_derived_table(self, dialect):
        tree = _tree("filtered-root", dialect)
        assert _derived_tables(tree, "done")
        assert all(isinstance(s.parent, exp.Subquery) for s in _restricting_selects(tree, "done"))

    def test_to_one_filter_read_is_inlined(self, dialect):
        tree = _tree("inlined-to-one-filter", dialect)
        assert not list(tree.find_all(exp.Exists))
        joins = [j for j in tree.find_all(exp.Join) if isinstance(j.this, exp.Subquery) and _mentions(j.this, "done")]
        assert any(isinstance(j.parent_select.parent, exp.Subquery) for j in joins if j.parent_select is not None)

    def test_unproven_filter_read_is_an_association(self, dialect):
        assert _exists(_tree("association-filter", dialect), "ups")

    def test_forced_association_of_a_to_one_read(self, dialect):
        assert _exists(_tree("forced-association", dialect), "done")

    def test_join_target_restricted_before_the_join(self, dialect):
        tree = _tree("join-target", dialect)
        assert any(isinstance(j.this, exp.Subquery) and _mentions(j.this, "done") for j in tree.find_all(exp.Join))
        assert all(isinstance(s.parent, exp.Subquery) for s in _restricting_selects(tree, "done"))

    def test_association_join_target(self, dialect):
        tree = _tree("association-join-target", dialect)
        assert _exists(tree, "ups")
        assert any(isinstance(j.this, exp.Subquery) and _mentions(j.this, "ups") for j in tree.find_all(exp.Join))

    def test_pushed_down_filter_reads_the_filtered_target(self, dialect):
        assert _mentions(_tree("pushed-down", dialect), "late")
