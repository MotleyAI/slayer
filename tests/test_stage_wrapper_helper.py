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

from slayer.sql.dialects import get_dialect
from slayer.sql.stage_wrapper import build_flat_rename_wrapper


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


def test_decodes_bigquery_mangled_aliases() -> None:
    """DEV-1716 (Codex review): BigQuery/T-SQL rendered stage SQL exposes
    alias-mangled output names (``orders___status``); the wrapper must decode
    them to the canonical dotted form to strip the ``orders.`` prefix and match
    the expected flat schema, while still referencing the ACTUAL mangled inner
    column. Without the decode the prefix-strip misses and the produced/expected
    assertion raises for BigQuery/T-SQL query-backed models."""
    # ``orders.status`` -> ``orders___status``; ``orders._count`` -> ``orders____count``.
    stage_sql = (
        "SELECT status AS `orders___status`, COUNT(*) AS `orders____count`\n"
        "FROM orders AS orders\nGROUP BY status"
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        stage_sql=stage_sql,
        expected_columns=["status", "_count"],
        dialect="bigquery",
    )
    sql = ast.sql(dialect="bigquery")
    # Flat output names produced from the decoded canonical form.
    parsed = sqlglot.parse_one(sql, dialect="bigquery")
    assert sorted(parsed.named_selects) == ["_count", "status"]
    # And the wrapper references the ACTUAL mangled inner columns.
    assert "orders___status" in sql
    assert "orders____count" in sql


def test_non_mangling_dialect_unaffected_by_decode() -> None:
    """The decode is identity for Postgres — dotted aliases still strip/flatten
    exactly as before (guards against the decode altering non-mangled input)."""
    stage_sql = (
        'SELECT status AS "orders.status", COUNT(*) AS "orders._count" '
        "FROM orders_t AS orders"
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        stage_sql=stage_sql,
        expected_columns=["status", "_count"],
        dialect="postgres",
    )
    parsed = sqlglot.parse_one(ast.sql(dialect="postgres"), dialect="postgres")
    assert sorted(parsed.named_selects) == ["_count", "status"]


# DEV-1756 for query-backed models: with ``projection_aliases`` the wrapper
# decodes length-fitted rendered names back to canonical for the schema match
# and fits its own output aliases. Without the argument it is byte-identical.

_LONGCOL = "a_very_long_column_name_that_certainly_exceeds_sixty_three_bytes_for_fit"

_GOLDEN_NO_PARAM = (
    'SELECT\n  _stage_inner."orders.status" AS "status",\n'
    '  _stage_inner."orders._count" AS "_count"\nFROM (\n  SELECT\n'
    '    status AS "orders.status",\n    COUNT(*) AS "orders._count"\n'
    "  FROM orders_t AS orders\n  GROUP BY\n    status\n) AS _stage_inner"
)


def _fitted_stage_sql() -> tuple[str, str]:
    """(stage_sql, fitted_inner_alias) with a length-fitted rendered alias."""
    fitted_in = get_dialect("postgres").fit_alias(f"orders.{_LONGCOL}")
    return (
        f'SELECT orders.amount AS "{fitted_in}" FROM orders_t AS orders',
        fitted_in,
    )


def test_projection_aliases_decode_fitted_input_and_fit_output() -> None:
    stage_sql, fitted_in = _fitted_stage_sql()
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        stage_sql=stage_sql,
        expected_columns=[_LONGCOL],
        dialect="postgres",
        projection_aliases=[f"orders.{_LONGCOL}"],
    )
    sql = ast.sql(dialect="postgres")
    fitted_out = get_dialect("postgres").fit_alias(_LONGCOL)
    parsed = sqlglot.parse_one(sql, dialect="postgres")
    assert parsed.named_selects == [fitted_out]
    assert fitted_in in sql  # inner reference is the ACTUAL rendered name
    assert _LONGCOL not in sql


def test_fitted_input_without_projection_aliases_still_raises() -> None:
    """The naive fix (fit the render, no wrapper decode) must keep failing loudly."""
    stage_sql, _ = _fitted_stage_sql()
    with pytest.raises(ValueError, match="do not match"):
        build_flat_rename_wrapper(
            source_relation="orders",
            stage_sql=stage_sql,
            expected_columns=[_LONGCOL],
            dialect="postgres",
        )


def test_no_param_output_is_byte_identical() -> None:
    stage_sql = (
        'SELECT status AS "orders.status", COUNT(*) AS "orders._count" '
        "FROM orders_t AS orders GROUP BY status"
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        stage_sql=stage_sql,
        expected_columns=["status", "_count"],
        dialect="postgres",
    )
    assert ast.sql(dialect="postgres", pretty=True) == _GOLDEN_NO_PARAM


def test_under_limit_projection_aliases_change_nothing() -> None:
    stage_sql = (
        'SELECT status AS "orders.status", COUNT(*) AS "orders._count" '
        "FROM orders_t AS orders GROUP BY status"
    )
    ast = build_flat_rename_wrapper(
        source_relation="orders",
        stage_sql=stage_sql,
        expected_columns=["status", "_count"],
        dialect="postgres",
        projection_aliases=["orders.status", "orders._count"],
    )
    assert ast.sql(dialect="postgres", pretty=True) == _GOLDEN_NO_PARAM


def test_fitted_output_alias_collision_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two flats fitting to one identifier must raise, not silently collide."""
    import slayer.sql._identifier_fit as fitmod

    from slayer.core.errors import IdentifierCollisionError

    monkeypatch.setattr(fitmod, "_digest", lambda name: "deadbeef")
    twin_a = "SandboxAlpha__" * 3 + "111" + "__SandboxOmega" * 3
    twin_b = "SandboxAlpha__" * 3 + "222" + "__SandboxOmega" * 3
    stage_sql = f'SELECT a AS "{twin_a}", b AS "{twin_b}" FROM t'
    with pytest.raises(IdentifierCollisionError):
        build_flat_rename_wrapper(
            source_relation="orders",
            stage_sql=stage_sql,
            expected_columns=[twin_a, twin_b],
            dialect="postgres",
            projection_aliases=[twin_a, twin_b],
        )
