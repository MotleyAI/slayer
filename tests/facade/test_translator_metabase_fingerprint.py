"""Translator tests for Metabase v0.62 fingerprint queries (DEV-1558 B5).

Metabase's field-value rescan emits two SQL shapes against the model path
(not the catalog executor):

* ``SELECT SUBSTRING("public"."customers"."name", 1, 1234) AS "substring4476"
  FROM "public"."customers" LIMIT 10000`` — hygiene-scalar wrapper around a
  base column, three-part qualified.
* ``SELECT "public"."orders"."customer_id" AS "customer_id"
  FROM "public"."orders" LIMIT 10000`` — three-part qualified bare column.

The translator must:
  (a) Strip the ``<schema>.<table>.`` prefix from column refs when the leading
      pair matches the resolved FROM table.
  (b) Recognise an allowlist of hygiene scalars wrapping a single bare column
      and treat them as a bare dimension projection with the alias preserved,
      logging a WARNING that the wrapper was dropped.

The hygiene allowlist (Phase 1): ``SUBSTRING``, ``SUBSTR``, ``LEFT``,
``RIGHT``, ``UPPER``, ``LOWER``, ``TRIM``, ``LENGTH``. Same set DEV-1378 lifted
into the DSL allowlist for WHERE-clause use.
"""

from __future__ import annotations

import logging

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.translator import QueryResult, TranslationError, translate


def _catalog() -> FacadeCatalog:
    customers = SlayerModel(
        name="customers", data_source="jaffle", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="email", type=DataType.TEXT),
        ],
    )
    orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="total", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [customers, orders]})


def _translate(sql: str):
    return translate(sql, _catalog(), dialect="postgres")


# --- 3-part qualified column refs -------------------------------------------


def test_three_part_qualified_column_in_select_resolves() -> None:
    sql = 'SELECT "public"."orders"."customer_id" AS "customer_id" FROM "public"."orders" LIMIT 10000'
    result = _translate(sql)
    assert isinstance(result, QueryResult)
    aliases = [projected for _alias, projected in result.column_name_mapping]
    assert "customer_id" in aliases


def test_three_part_qualified_column_multiple_columns() -> None:
    sql = (
        'SELECT "public"."orders"."customer_id" AS "customer_id", '
        '"public"."orders"."total" AS "total", '
        '"public"."orders"."created_at" AS "created_at" '
        'FROM "public"."orders" LIMIT 10000'
    )
    result = _translate(sql)
    assert isinstance(result, QueryResult)
    aliases = [projected for _alias, projected in result.column_name_mapping]
    assert aliases == ["customer_id", "total", "created_at"]
    assert result.query.limit == 10000


def test_three_part_qualified_column_with_wrong_schema_errors() -> None:
    # If the leading schema/table prefix doesn't match the FROM, the ref doesn't
    # resolve and the translator raises a clear error.
    with pytest.raises(TranslationError):
        _translate('SELECT "wrong"."orders"."customer_id" FROM "public"."orders"')


def test_three_part_qualified_column_with_wrong_table_errors() -> None:
    with pytest.raises(TranslationError):
        _translate('SELECT "public"."customers"."name" FROM "public"."orders"')


def test_two_part_qualified_unchanged_behaviour() -> None:
    # Bare table.column still works; the 3-part path is additive.
    sql = "SELECT orders.customer_id FROM orders LIMIT 10"
    result = _translate(sql)
    assert isinstance(result, QueryResult)


# --- Hygiene-scalar projection wrappers -------------------------------------


@pytest.mark.parametrize("fn", [
    "SUBSTRING", "SUBSTR", "LEFT", "RIGHT", "UPPER", "LOWER", "TRIM", "LENGTH",
])
def test_hygiene_wrapper_dropped_around_bare_column(fn: str, caplog) -> None:
    # Wrapper around a 3-part-qualified column with a user alias.
    args = '"public"."customers"."name"'
    if fn in {"SUBSTRING", "SUBSTR", "LEFT", "RIGHT"}:
        # Multi-arg variants
        if fn in {"SUBSTRING", "SUBSTR"}:
            args += ", 1, 1234"
        else:
            args += ", 1234"
    sql = f'SELECT {fn}({args}) AS "out" FROM "public"."customers" LIMIT 10000'
    with caplog.at_level(logging.WARNING):
        result = _translate(sql)
    assert isinstance(result, QueryResult)
    aliases = [projected for _alias, projected in result.column_name_mapping]
    assert aliases == ["out"]
    # The wrapper is silently dropped — the underlying projection is just the
    # bare `name` dimension; the engine response key is `customers.name`.
    engine_alias = next(a for a, _ in result.column_name_mapping)
    assert engine_alias.endswith(".name")
    # WARNING logged so a downstream operator notices.
    assert any("hygiene" in r.getMessage().lower() for r in caplog.records)


def test_hygiene_wrapper_around_unknown_column_errors() -> None:
    # If the wrapped column doesn't resolve, we don't silently drop — error.
    sql = 'SELECT SUBSTRING("public"."customers"."nope", 1, 100) FROM "public"."customers"'
    with pytest.raises(TranslationError):
        _translate(sql)


def test_non_hygiene_function_projection_still_rejected() -> None:
    # A non-allowlisted function (e.g. CONCAT) is NOT dropped — current
    # translator behaviour preserved.
    sql = 'SELECT MD5("public"."customers"."name") FROM "public"."customers"'
    with pytest.raises(TranslationError):
        _translate(sql)


def test_metabase_fingerprint_customers_corpus_16() -> None:
    """The literal #16 from the captured Metabase corpus."""
    sql = (
        '-- Metabase\n'
        'SELECT SUBSTRING("public"."customers"."name", 1, 1234) AS "substring4476", '
        'SUBSTRING("public"."customers"."email", 1, 1234) AS "substring4477" '
        'FROM "public"."customers" LIMIT 10000'
    )
    result = _translate(sql)
    assert isinstance(result, QueryResult)
    aliases = [projected for _alias, projected in result.column_name_mapping]
    assert aliases == ["substring4476", "substring4477"]
    assert result.query.limit == 10000


def test_metabase_fingerprint_orders_corpus_17() -> None:
    """The literal #17 from the captured Metabase corpus."""
    sql = (
        '-- Metabase\n'
        'SELECT "public"."orders"."customer_id" AS "customer_id", '
        '"public"."orders"."total" AS "total", '
        '"public"."orders"."created_at" AS "created_at" '
        'FROM "public"."orders" LIMIT 10000'
    )
    result = _translate(sql)
    assert isinstance(result, QueryResult)
    aliases = [projected for _alias, projected in result.column_name_mapping]
    assert aliases == ["customer_id", "total", "created_at"]
    assert result.query.limit == 10000
