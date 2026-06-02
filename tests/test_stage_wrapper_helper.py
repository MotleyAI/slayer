"""DEV-1452 Stage B — shared ``build_flat_rename_wrapper`` helper.

Extracted from ``slayer.sql.generator._stage_rename_wrapper`` (decision B
of the Stage B plan). Both the multi-stage CTE chaining in
``generate_planned_stages`` AND the migrated
``_expand_query_backed_model`` virtual-model wrap call it.

The helper takes no planner shapes — pure (source_relation, stage_sql,
expected_columns, dialect) -> sqlglot.expression. Stage rename wrapper
uses ``named_selects`` to read the rendered stage's actual aliases, strips
the ``<source_relation>.`` prefix, ``__``-flattens the remainder, and
asserts the produced set matches ``expected_columns``.
"""
from __future__ import annotations

import pytest
import sqlglot


def test_module_surface_exists() -> None:
    from slayer.sql.stage_wrapper import build_flat_rename_wrapper  # noqa: F401

    assert callable(build_flat_rename_wrapper)


def test_strips_source_relation_prefix_and_flattens_dots() -> None:
    """``orders.customers.region`` -> ``customers__region`` after strip + flatten."""
    from slayer.sql.stage_wrapper import build_flat_rename_wrapper

    stage_sql = (
        'SELECT "orders.status" AS "orders.status", '
        '"orders.customers.region" AS "orders.customers.region" '
        'FROM orders_t AS orders'
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        stage_sql=stage_sql,
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
    from slayer.sql.stage_wrapper import build_flat_rename_wrapper

    stage_sql = (
        'SELECT "orders.status" AS "orders.status" FROM orders_t AS orders'
    )
    with pytest.raises(ValueError, match="do not match"):
        build_flat_rename_wrapper(
            source_relation="orders",
            stage_sql=stage_sql,
            expected_columns=["status", "missing_extra"],
            dialect="postgres",
        )


def test_keeps_unprefixed_aliases_verbatim() -> None:
    """Result-key aliases that don't carry the ``<source_relation>.``
    prefix (legitimately possible for hoisted / synthetic columns) pass
    through ``__``-flatten only.
    """
    from slayer.sql.stage_wrapper import build_flat_rename_wrapper

    stage_sql = (
        'SELECT "orders.amount_sum" AS "orders.amount_sum", '
        '"bare_synth" AS "bare_synth" '
        'FROM orders_t AS orders'
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        stage_sql=stage_sql,
        expected_columns=["amount_sum", "bare_synth"],
        dialect="postgres",
    )
    parsed = sqlglot.parse_one(ast.sql(dialect="postgres"), dialect="postgres")
    assert sorted(parsed.named_selects) == ["amount_sum", "bare_synth"]
