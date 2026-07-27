"""DEV-1567: translator guard against cross-model metric projections.

After DEV-1567 the catalog-flatten step hides cross-model metrics from
``INFORMATION_SCHEMA.COLUMNS`` and ``pg_catalog.pg_attribute`` so BI tools
don't discover them and don't emit fingerprint SQL that would put dotted
names into ``SlayerQuery.measures[*].name`` (Pydantic rejects the dot).
But a hand-written SQL can still reach the translator with a cross-model
metric reference. To avoid the verbose Pydantic cascade and to keep result
keying honest (DEV-1448 renames a cross-model leaf but preserves the hop
path on the engine side — see CLAUDE.md), the translator rejects them with
a clear ``TranslationError`` at the point of resolution.

The guard fires for:
  * bare column refs that resolve to a cross-model metric
    (``SELECT "customers.row_count" FROM orders``);
  * aggregate calls that resolve to a cross-model metric formula
    (``SELECT MAX("customers.name") FROM orders``);
  * the same shapes with an arbitrary alias (``AS cr``) — the guard must
    test the resolved metric's catalog name, not ``projected_name``;
  * the HAVING path (``_apply_having`` shares the resolver chokepoint).

Cross-model dimensions are unaffected — they translate to multi-hop
``ColumnRef`` instances that the engine handles natively.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.translator import QueryResult, TranslationError, translate


def _catalog() -> FacadeCatalog:
    orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="total", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    customers = SlayerModel(
        name="customers", data_source="jaffle", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="email", type=DataType.TEXT),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders, customers]})


def _translate(sql: str) -> QueryResult:
    return translate(sql, _catalog(), dialect="postgres")


# --- Bare cross-model metric projection --------------------------------------


def test_bare_cross_model_metric_projection_rejected() -> None:
    sql = 'SELECT "customers.row_count" FROM "public"."orders"'
    with pytest.raises(TranslationError) as exc_info:
        _translate(sql)
    msg = str(exc_info.value)
    assert "cross-model" in msg.lower()
    assert "customers.row_count" in msg


def test_bare_cross_model_metric_projection_with_alias_rejected() -> None:
    """The guard must test the underlying catalog metric name, not the
    user-supplied alias — otherwise a one-character alias slips by and
    silently mis-keys the engine result (DEV-1448 keeps the hop path on
    the engine side; the SlayerQuery alias drops it)."""
    sql = 'SELECT "customers.row_count" AS "cr" FROM "public"."orders"'
    with pytest.raises(TranslationError) as exc_info:
        _translate(sql)
    msg = str(exc_info.value)
    assert "cross-model" in msg.lower()


# --- Aggregate over cross-model column ---------------------------------------


def test_aggregate_over_cross_model_column_rejected() -> None:
    sql = 'SELECT MAX("customers"."name") FROM "public"."orders"'
    with pytest.raises(TranslationError) as exc_info:
        _translate(sql)
    msg = str(exc_info.value)
    assert "cross-model" in msg.lower()


def test_aggregate_over_cross_model_column_with_alias_rejected() -> None:
    sql = 'SELECT MAX("customers"."name") AS "name_max" FROM "public"."orders"'
    with pytest.raises(TranslationError) as exc_info:
        _translate(sql)
    msg = str(exc_info.value)
    assert "cross-model" in msg.lower()


def test_count_distinct_over_cross_model_column_rejected() -> None:
    sql = 'SELECT COUNT(DISTINCT "customers"."email") FROM "public"."orders"'
    with pytest.raises(TranslationError) as exc_info:
        _translate(sql)
    assert "cross-model" in str(exc_info.value).lower()


# --- HAVING path -------------------------------------------------------------


def test_having_cross_model_aggregate_rejected() -> None:
    """HAVING calls ``_metric_for_aggregate`` — same chokepoint as the
    projection aggregate path, so the guard catches it once placed
    inside ``_metric_for_aggregate``."""
    sql = (
        'SELECT COUNT(*) AS "n" FROM "public"."orders" '
        'HAVING MAX("customers"."name") > \'X\''
    )
    with pytest.raises(TranslationError) as exc_info:
        _translate(sql)
    assert "cross-model" in str(exc_info.value).lower()


# --- Sanity: same-model and cross-model dimension paths still work ----------


def test_same_model_metric_still_works() -> None:
    sql = 'SELECT COUNT(*) AS "n" FROM "public"."orders"'
    result = _translate(sql)
    assert isinstance(result, QueryResult)


def test_same_model_aggregate_still_works() -> None:
    sql = 'SELECT MAX("total") AS "max_total" FROM "public"."orders"'
    result = _translate(sql)
    assert isinstance(result, QueryResult)


def test_cross_model_dimension_projection_still_works() -> None:
    """Cross-model dimensions translate to multi-hop ``ColumnRef`` which
    the engine handles natively (per CLAUDE.md result-column-naming
    rules). The guard MUST NOT fire on dimensions."""
    sql = 'SELECT "customers"."name" FROM "public"."orders" LIMIT 10'
    result = _translate(sql)
    assert isinstance(result, QueryResult)


def test_cross_model_time_dimension_truncation_still_works() -> None:
    """Time-grain over a cross-model TIMESTAMP dimension stays a
    dimension path, not a metric — guard must not fire."""
    customers_with_time = SlayerModel(
        name="customers", data_source="jaffle", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="signed_up_at", type=DataType.TIMESTAMP),
        ],
    )
    orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    cat = build_catalog(
        models_by_datasource={"jaffle": [orders, customers_with_time]},
    )
    sql = (
        'SELECT DATE_TRUNC(\'day\', "customers"."signed_up_at") AS "d" '
        'FROM "public"."orders"'
    )
    result = translate(sql, cat, dialect="postgres")
    assert isinstance(result, QueryResult)
