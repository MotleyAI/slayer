"""DEV-1452 Stage B — shared ``build_flat_rename_wrapper`` helper.

Extracted from ``slayer.sql.generator._stage_rename_wrapper`` (decision B
of the Stage B plan). Both the multi-stage CTE chaining in
``generate_planned_stages`` AND the migrated
``_expand_query_backed_model`` virtual-model wrap call it.

The helper takes no planner shapes — pure (source_relation, inner,
expected_columns, dialect) -> sqlglot.expression. Stage rename wrapper
uses ``named_selects`` to read the inner statement's aliases, strips
the ``<source_relation>.`` prefix, ``__``-flattens the remainder, and
asserts the produced set matches ``expected_columns``.
"""
from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.sql.stage_wrapper import (
    build_flat_rename_wrapper,
    unmangle_dotted_table_refs,
)


def _select(sql: str, dialect: str) -> exp.Select:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    assert isinstance(tree, exp.Select)
    return tree


def test_module_surface_exists() -> None:
    assert callable(build_flat_rename_wrapper)


def test_strips_source_relation_prefix_and_flattens_dots() -> None:
    """``orders.customers.region`` -> ``customers__region`` after strip + flatten."""
    stage_sql = (
        'SELECT "orders.status" AS "orders.status", '
        '"orders.customers.region" AS "orders.customers.region" '
        'FROM orders_t AS orders'
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        inner=_select(stage_sql, "postgres"),
        expected_columns=["status", "customers__region"],
        dialect="postgres",
    )
    out_sql = ast.sql(dialect="postgres")
    parsed = sqlglot.parse_one(out_sql, dialect="postgres")
    names = sorted(parsed.named_selects)
    assert names == ["customers__region", "status"], names


def test_mismatch_between_rendered_and_expected_raises() -> None:
    """If the rendered stage's output columns don't line up with the
    declared StageSchema, fail fast — silent divergence is the bug we're
    guarding against.
    """
    stage_sql = (
        'SELECT "orders.status" AS "orders.status" FROM orders_t AS orders'
    )
    with pytest.raises(ValueError, match="do not match"):
        build_flat_rename_wrapper(
            source_relation="orders",
            inner=_select(stage_sql, "postgres"),
            expected_columns=["status", "missing_extra"],
            dialect="postgres",
        )


def test_keeps_unprefixed_aliases_verbatim() -> None:
    """Result-key aliases that don't carry the ``<source_relation>.``
    prefix (legitimately possible for hoisted / synthetic columns) pass
    through ``__``-flatten only.
    """
    stage_sql = (
        'SELECT "orders.amount_sum" AS "orders.amount_sum", '
        '"bare_synth" AS "bare_synth" '
        'FROM orders_t AS orders'
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        inner=_select(stage_sql, "postgres"),
        expected_columns=["amount_sum", "bare_synth"],
        dialect="postgres",
    )
    parsed = sqlglot.parse_one(ast.sql(dialect="postgres"), dialect="postgres")
    assert sorted(parsed.named_selects) == ["amount_sum", "bare_synth"]


def test_bigquery_canonical_dotted_aliases_flatten() -> None:
    """The wrapper sees canonical dotted names on BigQuery too, and references them as rendered."""
    inner = _select(
        "SELECT status AS `orders.status`, COUNT(*) AS `orders._count`\n"
        "FROM orders AS orders\nGROUP BY status",
        "bigquery",
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        inner=inner,
        expected_columns=["status", "_count"],
        dialect="bigquery",
    )
    assert sorted(ast.named_selects) == ["_count", "status"]
    sql = ast.sql(dialect="bigquery")
    assert "`orders.status`" in sql
    assert "`orders._count`" in sql


def test_postgres_dotted_aliases_flatten() -> None:
    """Postgres dotted aliases strip / flatten."""
    stage_sql = (
        'SELECT status AS "orders.status", COUNT(*) AS "orders._count" '
        "FROM orders_t AS orders"
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        inner=_select(stage_sql, "postgres"),
        expected_columns=["status", "_count"],
        dialect="postgres",
    )
    parsed = sqlglot.parse_one(ast.sql(dialect="postgres"), dialect="postgres")
    assert sorted(parsed.named_selects) == ["_count", "status"]


_GOLDEN_NO_PARAM = (
    'SELECT\n  _stage_inner."orders.status" AS "status",\n'
    '  _stage_inner."orders._count" AS "_count"\nFROM (\n  SELECT\n'
    '    status AS "orders.status",\n    COUNT(*) AS "orders._count"\n'
    "  FROM orders_t AS orders\n  GROUP BY\n    status\n) AS _stage_inner"
)


def test_no_param_output_is_byte_identical() -> None:
    stage_sql = (
        'SELECT status AS "orders.status", COUNT(*) AS "orders._count" '
        "FROM orders_t AS orders GROUP BY status"
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        inner=_select(stage_sql, "postgres"),
        expected_columns=["status", "_count"],
        dialect="postgres",
    )
    assert ast.sql(dialect="postgres", pretty=True) == _GOLDEN_NO_PARAM


def test_unmangle_folds_four_segment_dotted_column() -> None:
    """A quoted four-segment dotted column under a stage alias re-parses on
    BigQuery with the overflow segments as a ``Dot`` in ``this``; the repair
    folds every segment back into one column name."""
    sql = (
        "SELECT `_stage_inner`.`orders.customers.regions.name` AS x "
        "FROM (SELECT 1 AS y) AS _stage_inner"
    )
    tree = sqlglot.parse_one(sql, dialect="bigquery")
    unmangle_dotted_table_refs(tree)  # pyright: ignore[reportArgumentType] — parse_one Expr/Expression stub gap
    col = next(tree.find_all(exp.Column))
    assert col.table == "_stage_inner"
    assert col.name == "orders.customers.regions.name"
    assert "`_stage_inner`.`orders.customers.regions.name`" in tree.sql(dialect="bigquery")


def test_unmangle_keeps_spine_correlation_to_outer_root() -> None:
    """A derived table nested in an EXISTS (the semi-join spine) correlates to
    the outer root: ``customers.id`` is a real qualified column, never folded
    into a dotted result key."""
    sql = (
        "SELECT customers.tier FROM customers AS customers WHERE EXISTS("
        "SELECT 1 FROM (SELECT customers.id AS id) AS __slayer_spine "
        "LEFT JOIN orders AS orders ON __slayer_spine.id = orders.customer_id)"
    )
    tree = sqlglot.parse_one(sql, dialect="sqlite")
    unmangle_dotted_table_refs(tree)  # pyright: ignore[reportArgumentType] — parse_one Expr/Expression stub gap
    spine = next(
        c for c in tree.find_all(exp.Column) if c.parent and c.parent.alias == "id"
    )
    assert (spine.table, spine.name) == ("customers", "id")
    assert tree.sql(dialect="sqlite") == sql


def test_unmangle_folds_inside_derived_table_of_root() -> None:
    """A derived table in the root sees no outer source: a re-parsed dotted key
    there still folds back into one column name."""
    sql = (
        "SELECT d.x FROM customers AS customers "
        "CROSS JOIN (SELECT `orders.region` AS x FROM t) AS d"
    )
    tree = sqlglot.parse_one(sql, dialect="bigquery")
    unmangle_dotted_table_refs(tree)  # pyright: ignore[reportArgumentType] — parse_one Expr/Expression stub gap
    (folded,) = [c for c in tree.find_all(exp.Column) if c.name == "orders.region"]
    assert folded.table == ""
