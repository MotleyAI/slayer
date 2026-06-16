"""Direct unit tests for the empty-string-to-non-text-column classifier
that powers DEV-1570's Bind-time rewrite.

The classifier returns the subset of $N indices whose AST occurrences land
in a comparison / IN / BETWEEN predicate against a column that resolves
via the FacadeCatalog to a non-TEXT ``DataType``. Whole-parameter granularity:
if ANY occurrence of $N targets a non-text column, $N is in the result set.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.facade.catalog import build_catalog
from slayer.pg_facade.connection import (
    PUBLIC_SCHEMA,
    _build_column_type_index,
    _classify_empty_string_param_targets,
)


# --- fixtures ---------------------------------------------------------------


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="order_date", type=DataType.DATE),
            Column(name="is_paid", type=DataType.BOOLEAN),
        ],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="jaffle",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


@pytest.fixture
def catalog():
    return build_catalog(
        models_by_datasource={PUBLIC_SCHEMA: [_orders_model(), _customers_model()]},
    )


@pytest.fixture
def column_type_index(catalog):
    return _build_column_type_index(catalog, "jaffle")


def _classify(sql: str, idx, candidates):
    return _classify_empty_string_param_targets(sql, idx, candidates)


# --- pg_catalog int column --------------------------------------------------


def test_pg_catalog_int_eq_classifies(column_type_index):
    sql = "SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


def test_pg_catalog_int_eq_with_alias_classifies(column_type_index):
    sql = (
        "SELECT d.objoid FROM pg_catalog.pg_description AS d "
        "WHERE d.objsubid = $1"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


def test_pg_catalog_text_column_not_classified(column_type_index):
    sql = "SELECT * FROM pg_catalog.pg_class WHERE relname = $1"
    assert _classify(sql, column_type_index, [1]) == set()


def test_pg_catalog_boolean_column_classified(column_type_index):
    sql = "SELECT * FROM pg_catalog.pg_class WHERE relhasindex = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


def test_pg_catalog_double_column_classified(column_type_index):
    sql = "SELECT * FROM pg_catalog.pg_class WHERE reltuples = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


# --- inequality / null-safe operators ---------------------------------------


@pytest.mark.parametrize("op", ["<>", "!=", "<", "<=", ">", ">="])
def test_inequality_operators_classified(op, column_type_index):
    sql = f"SELECT objoid FROM pg_catalog.pg_description WHERE objsubid {op} $1"
    assert _classify(sql, column_type_index, [1]) == {1}


def test_is_not_distinct_from_classified(column_type_index):
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE objsubid IS NOT DISTINCT FROM $1"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


def test_is_distinct_from_classified(column_type_index):
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE objsubid IS DISTINCT FROM $1"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


# --- reversed operand order -------------------------------------------------


def test_reversed_operand_order_classified(column_type_index):
    sql = "SELECT objoid FROM pg_catalog.pg_description WHERE $1 = objsubid"
    assert _classify(sql, column_type_index, [1]) == {1}


# --- BETWEEN / IN -----------------------------------------------------------


def test_between_both_bounds_classified(column_type_index):
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE objsubid BETWEEN $1 AND $2"
    )
    assert _classify(sql, column_type_index, [1, 2]) == {1, 2}


def test_in_list_all_classified(column_type_index):
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE objsubid IN ($1, $2, $3)"
    )
    assert _classify(sql, column_type_index, [1, 2, 3]) == {1, 2, 3}


def test_in_list_against_text_column_not_classified(column_type_index):
    sql = "SELECT relname FROM pg_catalog.pg_class WHERE relname IN ($1, $2)"
    assert _classify(sql, column_type_index, [1, 2]) == set()


# --- JOIN ON ----------------------------------------------------------------


def test_join_on_predicate_classified(column_type_index):
    sql = (
        "SELECT c.relname FROM pg_catalog.pg_class AS c "
        "INNER JOIN pg_catalog.pg_description AS d "
        "ON c.oid = d.objoid AND d.objsubid = $1"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


# --- user model tables (public schema) --------------------------------------


def test_user_model_int_pk_classified_via_public_qualifier(column_type_index):
    sql = "SELECT id FROM public.orders WHERE id = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


def test_user_model_int_pk_classified_bare_table(column_type_index):
    sql = "SELECT id FROM orders WHERE id = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


def test_user_model_text_column_not_classified(column_type_index):
    sql = "SELECT id FROM orders WHERE status = $1"
    assert _classify(sql, column_type_index, [1]) == set()


def test_user_model_date_column_classified(column_type_index):
    sql = "SELECT id FROM orders WHERE order_date = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


def test_user_model_timestamp_column_classified(column_type_index):
    sql = "SELECT id FROM orders WHERE ordered_at = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


def test_user_model_boolean_column_classified(column_type_index):
    sql = "SELECT id FROM orders WHERE is_paid = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


def test_user_model_double_column_classified(column_type_index):
    sql = "SELECT id FROM orders WHERE revenue = $1"
    assert _classify(sql, column_type_index, [1]) == {1}


# --- information_schema (schema-qualified name remap) -----------------------


def test_information_schema_ordinal_position_classified(column_type_index):
    """`information_schema.columns.ordinal_position` is INT — must classify
    when referenced via the qualified `information_schema.columns` name
    (mapped to `_is_columns` internally)."""
    sql = (
        "SELECT column_name FROM information_schema.columns "
        "WHERE ordinal_position = $1"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


def test_information_schema_text_column_not_classified(column_type_index):
    sql = (
        "SELECT column_name FROM information_schema.columns "
        "WHERE column_name = $1"
    )
    assert _classify(sql, column_type_index, [1]) == set()


def test_bare_columns_does_not_resolve_as_information_schema(column_type_index):
    """Bare `columns` must NOT resolve to information_schema.columns (per the
    catalog-executor convention at slayer/facade/catalog_sql.py:712)."""
    sql = "SELECT * FROM columns WHERE ordinal_position = $1"
    assert _classify(sql, column_type_index, [1]) == set()


# --- subqueries -------------------------------------------------------------


def test_subquery_predicate_classified(column_type_index):
    sql = (
        "SELECT * FROM pg_catalog.pg_class "
        "WHERE oid IN ("
        "  SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
        ")"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


# --- CTE-derived columns (out of scope) -------------------------------------


def test_cte_aliased_column_does_not_classify(column_type_index):
    """CTE projections lose physical-column lineage; resolver doesn't track
    aliases through derived sources. Codex finding #3: explicitly out of
    scope. Skip silently."""
    sql = (
        "WITH d AS (SELECT objsubid AS x FROM pg_catalog.pg_description) "
        "SELECT * FROM d WHERE x = $1"
    )
    assert _classify(sql, column_type_index, [1]) == set()


# --- expression-wrapped column (out of scope) -------------------------------


def test_cast_wrapped_column_not_classified(column_type_index):
    """`CAST(col AS BIGINT) = $1` — left side is not a bare Column ref;
    we leave $1 as the empty-string literal so DuckDB surfaces the
    original conversion error. Documented scope limit."""
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE CAST(objsubid AS BIGINT) = $1"
    )
    assert _classify(sql, column_type_index, [1]) == set()


def test_arithmetic_wrapped_column_not_classified(column_type_index):
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE objsubid + 1 = $1"
    )
    assert _classify(sql, column_type_index, [1]) == set()


def test_function_wrapped_column_not_classified(column_type_index):
    """Codex round 2: explicit coverage that function wrappers (ABS, COALESCE,
    etc.) keep the comparison unresolved."""
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE ABS(objsubid) = $1"
    )
    assert _classify(sql, column_type_index, [1]) == set()


# --- LIKE / ILIKE (out of scope: text-only operators) ----------------------


def test_like_predicate_not_classified(column_type_index):
    """LIKE is text-only; even against a TEXT column, $N stays `''` since
    the walker only covers comparison / IN / BETWEEN nodes."""
    sql = "SELECT relname FROM pg_catalog.pg_class WHERE relname LIKE $1"
    assert _classify(sql, column_type_index, [1]) == set()


def test_ilike_predicate_not_classified(column_type_index):
    sql = "SELECT relname FROM pg_catalog.pg_class WHERE relname ILIKE $1"
    assert _classify(sql, column_type_index, [1]) == set()


# --- CASE WHEN / HAVING / projection (any-clause coverage) -----------------


def test_case_when_comparison_classified(column_type_index):
    """Comparison nodes inside CASE WHEN are walked the same as WHERE ones."""
    sql = (
        "SELECT CASE WHEN objsubid = $1 THEN 1 ELSE 0 END "
        "FROM pg_catalog.pg_description"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


def test_having_comparison_classified(column_type_index):
    """Codex round 2: HAVING is `any comparison node anywhere`."""
    sql = (
        "SELECT relhasindex, COUNT(*) FROM pg_catalog.pg_class "
        "GROUP BY relhasindex HAVING relhasindex = $1"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


def test_projection_comparison_classified(column_type_index):
    """Codex round 2: a comparison sitting in the projection list (no
    CASE WHEN) must still classify."""
    sql = "SELECT objsubid = $1 FROM pg_catalog.pg_description"
    assert _classify(sql, column_type_index, [1]) == {1}


# --- derived subquery columns (out of scope, distinct from CTE) ------------


def test_derived_subquery_alias_does_not_classify(column_type_index):
    """Codex round 2: a derived-table alias (no CTE syntax) loses physical
    column lineage; the classifier must skip."""
    sql = (
        "SELECT * FROM ("
        "  SELECT objsubid AS x FROM pg_catalog.pg_description"
        ") d WHERE x = $1"
    )
    assert _classify(sql, column_type_index, [1]) == set()


# --- mixed-use $N (whole-param swap) ---------------------------------------


def test_mixed_use_param_whole_param_swap(column_type_index):
    """$1 appears against BOTH a non-text and a text column — whole-param
    swap is in effect: $1 is in the result set (so Bind will substitute
    NULL everywhere)."""
    sql = (
        "SELECT * FROM pg_catalog.pg_description AS d "
        "INNER JOIN pg_catalog.pg_class AS c ON c.oid = d.objoid "
        "WHERE d.objsubid = $1 OR c.relname = $1"
    )
    assert _classify(sql, column_type_index, [1]) == {1}


# --- candidate filter -------------------------------------------------------


def test_only_returns_indices_in_candidate_set(column_type_index):
    sql = (
        "SELECT * FROM pg_catalog.pg_description "
        "WHERE objsubid = $1 AND classoid = $2"
    )
    # Only $2 is a candidate (e.g. only $2 was bound to b''); even if $1
    # also targets a non-text column it must NOT appear in the result.
    assert _classify(sql, column_type_index, [2]) == {2}


def test_empty_candidate_set_returns_empty(column_type_index):
    sql = "SELECT * FROM pg_catalog.pg_description WHERE objsubid = $1"
    assert _classify(sql, column_type_index, []) == set()


# --- robustness -------------------------------------------------------------


def test_unparseable_sql_returns_empty(column_type_index):
    assert _classify("SELECT this is not valid sql !!!!", column_type_index, [1]) == set()


def test_unknown_table_returns_empty(column_type_index):
    sql = "SELECT * FROM does_not_exist WHERE x = $1"
    assert _classify(sql, column_type_index, [1]) == set()


def test_unknown_column_returns_empty(column_type_index):
    sql = "SELECT * FROM pg_catalog.pg_description WHERE no_such_col = $1"
    assert _classify(sql, column_type_index, [1]) == set()


def test_classifier_does_not_raise_on_unexpected_shapes(column_type_index):
    """Defensive: pathological SQL shouldn't propagate exceptions out of
    the helper. Codex finding #1: the helper must catch sqlglot's
    SqlglotError family and any defensive AST-walk failure."""
    pathological = [
        "",                          # empty
        "  ",                        # whitespace only
        "$1",                        # bare parameter, no statement
        "SELECT $1",                 # parameter in projection, no FROM
        "SELECT * FROM (SELECT $1)", # parameter in derived table projection
    ]
    for sql in pathological:
        _classify(sql, column_type_index, [1])  # must not raise


# --- column_type_index shape -----------------------------------------------


def test_column_type_index_includes_pg_catalog_tables(column_type_index):
    """The index must surface pg_catalog tables so the classifier resolves
    `pg_catalog.X.Y` refs."""
    assert column_type_index.get(("pg_catalog", "pg_description", "objsubid")) == DataType.INT
    assert column_type_index.get(("pg_catalog", "pg_description", "description")) == DataType.TEXT


def test_column_type_index_includes_information_schema_tables(column_type_index):
    """`_is_columns` builder maps to SQL-visible `information_schema.columns`.
    Codex finding #4: the index key must use the SQL-visible name, not the
    builder-internal `_is_columns`."""
    assert column_type_index.get(("information_schema", "columns", "ordinal_position")) == DataType.INT
    assert column_type_index.get(("information_schema", "columns", "column_name")) == DataType.TEXT
    # The internal builder name must NOT be a key.
    assert ("information_schema", "_is_columns", "ordinal_position") not in column_type_index


def test_column_type_index_includes_user_models_under_public(column_type_index):
    assert column_type_index.get(("public", "orders", "id")) == DataType.INT
    assert column_type_index.get(("public", "orders", "status")) == DataType.TEXT
    assert column_type_index.get(("public", "orders", "is_paid")) == DataType.BOOLEAN
    assert column_type_index.get(("public", "orders", "ordered_at")) == DataType.TIMESTAMP
