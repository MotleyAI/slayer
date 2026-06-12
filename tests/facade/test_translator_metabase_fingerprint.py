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


def test_four_part_catalog_qualified_column_resolves() -> None:
    """CR/Codex review: ``slayer.<schema>.<table>.<col>`` 4-part refs
    must strip the leading 3 elements (catalog + schema + table) and
    leave just the bare column. Previously the strip removed only the
    last schema.table pair, leaving ``slayer.col`` which broke
    everything."""
    sql = (
        'SELECT slayer.jaffle.orders.customer_id AS "customer_id" '
        'FROM jaffle.orders'
    )
    result = _translate(sql)
    assert isinstance(result, QueryResult)
    aliases = [projected for _alias, projected in result.column_name_mapping]
    assert aliases == ["customer_id"]


def test_four_part_foreign_catalog_rejected() -> None:
    """A 4-part ref naming a non-SLayer catalog is NOT this catalog's
    column — leave it unstripped so resolution fails loudly."""
    sql = (
        'SELECT other_catalog.jaffle.orders.customer_id '
        'FROM jaffle.orders'
    )
    with pytest.raises(TranslationError):
        _translate(sql)


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


def test_qualified_table_exact_match_wins_over_public_alias() -> None:
    """CR review: when a catalog has a real ``public`` schema with a
    distinct table, ``public.X`` must resolve via exact match instead of
    short-circuiting to the bare-name lookup (which would silently pick
    a different schema's same-named table)."""
    from slayer.core.enums import DataType
    from slayer.core.models import Column, SlayerModel
    from slayer.facade.catalog import build_catalog
    public_orders = SlayerModel(
        name="orders", data_source="public", sql_table="orders",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="public_only_col", type=DataType.TEXT)],
    )
    jaffle_orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="jaffle_only_col", type=DataType.TEXT)],
    )
    catalog = build_catalog(models_by_datasource={
        "public": [public_orders], "jaffle": [jaffle_orders],
    })
    # ``public.orders`` should resolve to the actual ``public`` schema
    # entry — i.e. carry the ``public_only_col`` dimension.
    result = translate(
        'SELECT "public"."orders"."public_only_col" FROM "public"."orders"',
        catalog, dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.schema_name == "public"


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
    # Plan: WARNING includes the specific function name; exactly one warning
    # per dropped wrapper (so a multi-projection SQL gets multiple lines).
    hygiene_warnings = [
        r for r in caplog.records
        if "hygiene" in r.getMessage().lower() and fn.lower() in r.getMessage().lower()
    ]
    assert len(hygiene_warnings) == 1


def test_hygiene_wrapper_logs_one_warning_per_projection(caplog) -> None:
    """Two SUBSTRINGs in one SELECT → two WARNING lines."""
    sql = (
        'SELECT SUBSTRING("public"."customers"."name", 1, 1234) AS "n", '
        'SUBSTRING("public"."customers"."email", 1, 1234) AS "e" '
        'FROM "public"."customers" LIMIT 10000'
    )
    with caplog.at_level(logging.WARNING):
        _translate(sql)
    hygiene_warnings = [
        r for r in caplog.records if "hygiene" in r.getMessage().lower()
    ]
    assert len(hygiene_warnings) == 2


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


def test_aggregate_over_three_part_qualified_column() -> None:
    """DEV-1558 Codex review fold: SUM("public"."orders"."total") must
    resolve to the `total:sum` metric on the orders model, not to a
    bogus `public.orders.total:sum` lookup."""
    sql = (
        'SELECT SUM("public"."orders"."total") AS "total_sum" '
        'FROM "public"."orders"'
    )
    result = _translate(sql)
    assert isinstance(result, QueryResult)
    aliases = [projected for _alias, projected in result.column_name_mapping]
    assert aliases == ["total_sum"]
    def _formula(m):
        return m["formula"] if isinstance(m, dict) else m.formula
    measure_formulas = [_formula(m) for m in (result.query.measures or [])]
    assert "total:sum" in measure_formulas


def test_order_by_three_part_qualified_column() -> None:
    """ORDER BY through a 3-part-qualified column must resolve via
    strip_prefix the same way the projection does."""
    sql = (
        'SELECT "public"."orders"."customer_id" AS "customer_id" '
        'FROM "public"."orders" ORDER BY "public"."orders"."customer_id"'
    )
    result = _translate(sql)
    assert isinstance(result, QueryResult)


def test_where_filter_on_three_part_qualified_column_passes_through() -> None:
    """WHERE filters on 3-part-qualified columns are normalised via
    strip_prefix before they land in SlayerQuery.filters, because the
    engine's Mode-B DSL only accepts single-dot paths. Pin the resulting
    filter so a regression where the unstripped 3-part form leaks
    through would fail loudly."""
    sql = (
        'SELECT "public"."orders"."customer_id" AS "customer_id" '
        'FROM "public"."orders" WHERE "public"."orders"."total" > 0'
    )
    result = _translate(sql)
    assert isinstance(result, QueryResult)
    filters = result.query.filters or []
    # The serialised filter must NOT carry the 3-part-qualified column
    # ref; the leading public.orders prefix is stripped.
    assert filters
    assert all("public" not in f for f in filters)
    assert all("orders.total" not in f for f in filters)
    # The bare column name survives.
    assert any("total" in f for f in filters)


def test_hygiene_wrapper_around_bare_column_rejected() -> None:
    """CR review: dropping ``LENGTH(name)`` would silently change query
    results. The gate now requires the inner column to be 3-part
    qualified AND match the FROM table prefix, so bare-column wrappers
    are rejected just like any other unsupported projection."""
    sql = 'SELECT LENGTH("name") FROM "public"."customers"'
    with pytest.raises(TranslationError):
        _translate(sql)


def test_hygiene_wrapper_around_two_part_column_rejected() -> None:
    """A 2-part-qualified column ref doesn't carry enough information
    to disambiguate fingerprint shape from user computation, so it's
    also rejected."""
    sql = 'SELECT UPPER(customers.name) FROM customers'
    with pytest.raises(TranslationError):
        _translate(sql)


def test_metabase_gui_question_count_orders_corpus_20() -> None:
    """Corpus #20 — Metabase's compiled GUI question (`COUNT of orders`):
    ``SELECT COUNT(*) AS "count" FROM "public"."orders"``.  This rides on
    DEV-1486's aggregate-SQL → metric mapping (`COUNT(*)` → `*:count`) plus
    the 3-part / 2-part qualified `"public"."orders"` table reference."""
    sql = (
        '-- Metabase:: userID: 1 queryType: MBQL queryHash: a54de39466cbf3e8\n'
        'SELECT COUNT(*) AS "count" FROM "public"."orders"'
    )
    result = _translate(sql)
    assert isinstance(result, QueryResult)
    aliases = [projected for _alias, projected in result.column_name_mapping]
    assert aliases == ["count"]
    assert result.query.source_model == "orders"
    # Underlying engine measure is `*:count` (the canonical count-all form).
    # The measures list carries pydantic ModelMeasure-shaped entries; accept
    # both dict and attribute access.
    def _formula(m):
        return m["formula"] if isinstance(m, dict) else m.formula
    measure_formulas = [_formula(m) for m in (result.query.measures or [])]
    assert "*:count" in measure_formulas
