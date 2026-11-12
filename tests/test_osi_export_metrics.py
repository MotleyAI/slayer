"""Ossie export: measures become metrics with Ossie component semantics; portable vs dialect-only expressions."""

import re
from pathlib import Path
from typing import Any, LiteralString, cast

import psycopg
import pytest
import sqlglot
from pytest_postgresql import factories
from sqlglot import exp

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import SlayerError
from slayer.core.models import Aggregation, DatasourceConfig, ModelJoin
from slayer.storage.yaml_storage import YAMLStorage
from tests._osi_export_fixtures import (
    BASIC_MEASURES, DATASOURCE, PG_DDL, col, customers, measure, orders, orders_columns, to_customers, visits,
)
from tests._osi_export_helpers import (
    Env, all_metrics, carries, dataset, export, export_models, expression_sql, field, make_env, metric, reimport,
    relationships, rows, run_sql, same_sql, save,
)
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


def _sql(entity: dict[str, Any] | None) -> str:
    assert entity is not None
    return entity["expression"]["sql"]


async def test_dataset_scoped_metric(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    assert same_sql(_sql(metric(dataset(result.document, "orders"), "revenue")), "SUM(amount)")


async def test_measure_reference_expanded(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    assert same_sql(_sql(metric(dataset(result.document, "orders"), "aov")), "SUM(amount) / COUNT(*)")


async def test_measure_over_another_model_is_a_component_on_that_model(env) -> None:
    model = orders(measures=[*BASIC_MEASURES, measure("customer_discount", "sum(customers.discount)")])
    result = await export_models(env, customers(), model)
    cust_metrics = dataset(result.document, "customers").get("metrics") or []
    assert any(same_sql(_sql(m), "SUM(discount)") for m in cust_metrics)
    assert metric(dataset(result.document, "orders"), "customer_discount") is None
    imported, _ = await reimport(env, result.document)
    data = await rows(imported, {"source_model": "orders", "measures": ["customer_discount"]})
    assert data[0]["orders.customer_discount"] == pytest.approx(3.5)


async def test_cross_model_ratio_is_model_scoped(env) -> None:
    ratio = measure("aov_per_customer", "sum(amount) / count_distinct(customers.id)")
    result = await export_models(env, customers(), orders(measures=[*BASIC_MEASURES, ratio]))
    assert same_sql(_sql(metric(result.document, "aov_per_customer")),
                    "SUM(orders.amount) / COUNT(DISTINCT customers.id)")


async def test_ambiguous_route_measure_is_slayer_only(env) -> None:
    second = ModelJoin(target_model="customers", join_pairs=[["region_code", "id"]], name="referrer",
                       cardinality=None)
    model = orders(columns=[*orders_columns(), col("region_code", DataType.INT, "quantity")],
                   joins=[to_customers(name="buyer"), second],
                   measures=[*BASIC_MEASURES, measure("ratio", "sum(amount) / count_distinct(buyer.id)")])
    result = await export_models(env, customers(), model)
    assert all(m["name"] != "ratio" for m in all_metrics(result.document))
    assert carries(dataset(result.document, "orders"), "sum(amount) / count_distinct(buyer.id)")


_AGGREGATES = [
    pytest.param("sum(amount)", "SUM(amount)", id="sum"),
    pytest.param("avg(amount)", "AVG(amount)", id="avg"),
    pytest.param("min(amount)", "MIN(amount)", id="min"),
    pytest.param("max(amount)", "MAX(amount)", id="max"),
    pytest.param("count(*)", "COUNT(*)", id="count-star"),
    pytest.param("count(amount)", "COUNT(amount)", id="count"),
    pytest.param("count_distinct(customer_id)", "COUNT(DISTINCT customer_id)", id="count-distinct"),
    pytest.param("percentile(amount, 0.9)", "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)", id="percentile"),
    pytest.param("median(amount)", "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)", id="median"),
    pytest.param("stddev_samp(amount)", "STDDEV_SAMP(amount)", id="stddev-samp"),
    pytest.param("stddev_pop(amount)", "STDDEV_POP(amount)", id="stddev-pop"),
    pytest.param("var_samp(amount)", "VAR_SAMP(amount)", id="var-samp"),
    pytest.param("var_pop(amount)", "VAR_POP(amount)", id="var-pop"),
    pytest.param("corr(amount, other=quantity)", "CORR(amount, quantity)", id="corr"),
    pytest.param("covar_samp(amount, other=quantity)", "COVAR_SAMP(amount, quantity)", id="covar-samp"),
    pytest.param("covar_pop(amount, other=quantity)", "COVAR_POP(amount, quantity)", id="covar-pop"),
]


@pytest.mark.parametrize(("formula", "expected"), _AGGREGATES)
async def test_aggregate_sql(env, formula: str, expected: str) -> None:
    result = await export_models(env, customers(), orders(measures=[measure("m", formula)]))
    assert same_sql(_sql(metric(dataset(result.document, "orders"), "m")), expected)


@pytest.mark.parametrize("formula", ["sum(amount)", "count(amount)", "count_distinct(customer_id)",
                                     "corr(amount, other=quantity)", "max(amount)"])
async def test_masked_column_aggregates_read_the_masked_field(env, formula: str) -> None:
    masked = col("amount", sql="amount", filter="status = 'completed'")
    columns = [masked if c.name == "amount" else c for c in orders_columns()]
    result = await export_models(env, customers(), orders(columns=columns, measures=[measure("m", formula)]))
    orders_ds = dataset(result.document, "orders")
    tree = sqlglot.parse_one(_sql(metric(orders_ds, "m")), read="snowflake")
    assert tree.find(exp.Filter) is None
    assert "CASE" in expression_sql(field(orders_ds, "amount") or {}, target="duckdb").upper()


async def test_percentile_on_sqlite_is_standard(tmp_path: Path) -> None:
    lite = await make_env(tmp_path, dialect="sqlite")
    result = await export_models(lite, customers(), orders(measures=[measure("p90", "percentile(amount, 0.9)")]))
    assert same_sql(_sql(metric(dataset(result.document, "orders"), "p90")),
                    "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)")


def _paid_metric_sql(document: dict[str, Any], *, target: str) -> str:
    return expression_sql(metric(dataset(document, "orders"), "paid") or {}, target=target)


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
async def test_boolean_sum_counts_trues(tmp_path: Path, dialect: str) -> None:
    local = await make_env(tmp_path, dialect=dialect)
    result = await export_models(local, customers(), orders(measures=[measure("paid", "sum(is_paid)")]))
    target = "duckdb" if dialect == "duckdb" else "sqlite"
    assert run_sql(local, f"SELECT {_paid_metric_sql(result.document, target=target)} FROM orders") == [(3,)]


async def test_boolean_sum_counts_trues_on_postgres(env, postgresql_proc: Any) -> None:
    result = await export_models(env, customers(), orders(measures=[measure("paid", "sum(is_paid)")]))
    with pg_database(postgresql_proc, statements=PG_DDL, name="osi_export_pg") as pg:
        with psycopg.connect(host=pg.host, port=pg.port, user=pg.username, dbname=pg.database) as conn:
            sql = f"SELECT {_paid_metric_sql(result.document, target='postgres')} FROM orders"
            value = conn.execute(cast(LiteralString, sql))
            assert value.fetchone() == (3,)


async def test_approximate_distinct_count_is_dialect_only(env) -> None:
    result = await export_models(env, customers(), orders(measures=[measure("approx", "count_distinct_approx(customer_id)")]))
    approx = metric(dataset(result.document, "orders"), "approx")
    assert approx is not None
    assert "sql" not in approx["expression"]
    [entry] = approx["expression"]["dialects"]
    assert entry["dialect"] == "DUCKDB"
    assert "approx_count_distinct" in entry["expression"].lower()


async def test_custom_aggregation_inlined(env) -> None:
    model = orders(measures=[measure("trimmed", "trimmed_sum(amount)")])
    model.aggregations = [Aggregation(name="trimmed_sum", formula="SUM(CASE WHEN {value} < 15 THEN {value} END)")]
    result = await export_models(env, customers(), model)
    assert same_sql(_sql(metric(dataset(result.document, "orders"), "trimmed")),
                    "SUM(CASE WHEN amount < 15 THEN amount END)")


async def test_generated_metric_names_unique_in_scope(env) -> None:
    model = orders(measures=[*BASIC_MEASURES, measure("customer_discount", "sum(customers.discount)"),
                             measure("discount_total", "sum(customers.discount) * 2")])
    cust = customers(measures=[measure("customer_discount", "sum(discount)")])
    result = await export_models(env, cust, model)
    for container in [*result.document["datasets"], result.document]:
        names = [m["name"] for m in container.get("metrics") or []]
        assert len(names) == len(set(names))


async def test_relocated_metric_name_unique_under_case_folding(env) -> None:
    model = orders(measures=[*BASIC_MEASURES, measure("customer_discount", "sum(customers.discount)")])
    cust = customers(measures=[measure("CUSTOMER_DISCOUNT", "sum(discount)")])
    result = await export_models(env, cust, model)
    names = [m["name"].lower() for m in dataset(result.document, "customers").get("metrics") or []]
    assert len(names) == 2
    assert len(set(names)) == 2


async def test_relationship_and_metric_names_unique_document_wide(env) -> None:
    to_cust = ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                        cardinality=JoinCardinality.MANY_TO_ONE)
    ratio = measure("aov_per_customer", "sum(amount) / count_distinct(customers.id)")
    result = await export_models(env, customers(), orders(measures=[*BASIC_MEASURES, ratio]),
                                 visits(joins=[to_cust]))
    names = [r["name"].lower() for r in relationships(result.document)]
    names += [m["name"].lower() for m in result.document.get("metrics") or []]
    assert len(names) == 3
    assert len(set(names)) == 3


async def test_case_ambiguous_identifier_is_quoted(env) -> None:
    columns = [*orders_columns(), col("Status", DataType.TEXT, '"LegalType"')]
    await save(env, customers())
    # Unvalidated save: a grandfathered ``status``/``Status`` collision a validated save would refuse.
    await YAMLStorage(base_dir=str(env.store)).save_model(
        orders(columns=columns, measures=[measure("n_types", "count_distinct(Status)")]), _validate=False)
    result = await export(env)
    tree = sqlglot.parse_one(_sql(metric(dataset(result.document, "orders"), "n_types")), read="snowflake")
    [ident] = [c.this for c in tree.find_all(exp.Column)]
    assert ident.quoted
    assert ident.name == "Status"
    # Re-import saves a new model, so the collision is no longer grandfathered.
    with pytest.raises(SlayerError) as raised:
        await reimport(env, result.document)
    assert re.search(r"\bstatus\b", str(raised.value))
    assert re.search(r"\bStatus\b", str(raised.value))


# ─────────────────────── portable vs dialect-only fields ───────────────────

async def test_portable_field_expression(env) -> None:
    model = orders(columns=[*orders_columns(), col("gross", sql="price * quantity")], measures=BASIC_MEASURES)
    gross = field(dataset((await export_models(env, customers(), model)).document, "orders"), "gross")
    assert gross is not None
    assert same_sql(gross["expression"]["sql"], "price * quantity")
    assert "dialects" not in gross["expression"]


async def test_dialect_only_field_expression(tmp_path: Path) -> None:
    store = tmp_path / "store"
    pg = Env(db=tmp_path / "unused", store=store, dialect="postgres")
    storage = YAMLStorage(base_dir=str(store))
    await storage.save_datasource(DatasourceConfig(name=DATASOURCE, type="postgres", host="127.0.0.1", port=1,
                                                   database="none"))
    model = orders(columns=[*orders_columns(), col("month_label", DataType.TEXT, "to_char(ordered_on, 'YYYY-MM')")],
                   joins=[], measures=BASIC_MEASURES)
    await storage.save_model(model, _validate=False)
    label = field(dataset((await export(pg)).document, "orders"), "month_label")
    assert label is not None
    assert "sql" not in label["expression"]
    assert label["expression"]["dialects"] == [{"dialect": "POSTGRES", "expression": "to_char(ordered_on, 'YYYY-MM')"}]

