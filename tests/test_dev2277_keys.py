"""Result keys never depend on the population (spec: queries/references, queries/measure-naming, queries/saved-measures)."""

from __future__ import annotations

import tempfile
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.models import DatasetMetric, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.refs import auto_name_from_expression
from slayer.core.syntax import parse_expr
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql.naming import expression_text_alias
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1879_helpers import mart_dataset, targets_dataset
from tests._dev2277_fixtures import BACKENDS, by, customers_dataset, seed_datasets, seed_engine, table


@pytest.fixture(params=BACKENDS)
def backend(request) -> str:
    return request.param


@pytest.fixture
async def engine(backend: str):
    async with seed_engine(backend) as eng:
        yield eng


def outer_aliases(resp: SlayerResponse, *, dialect: str) -> list[str]:
    assert resp.sql is not None
    tree = sqlglot.parse_one(resp.sql, dialect=dialect)
    assert isinstance(tree, exp.Query), resp.sql
    return [s.alias_or_name for s in tree.selects]


def assert_keys(resp: SlayerResponse, expected: list[str], *, dialect: str) -> None:
    """Columns, row keys, SQL projection aliases and attribute keys all follow ``expected``."""
    assert resp.columns == expected
    assert all(list(r) == expected for r in resp.data)
    assert outer_aliases(resp, dialect=dialect) == expected
    assert set(resp.attributes.dimensions) | set(resp.attributes.metrics) <= set(expected)


def sanitized(text: str) -> str:
    """The product's derived name of an unaliased expression."""
    return expression_text_alias(parse_expr(text))


# --------------------------------------------------------------------------- #
# One key under every population
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("population", ["customers", "orders"])
async def test_the_same_query_keys_alike_under_every_population(engine, backend: str, population: str) -> None:
    resp = await engine.execute({
        "population": population, "dimensions": ["customers.region"],
        "metrics": ["sum(orders.amount)", "orders.total_revenue"],
    })
    assert_keys(resp, ["customers.region", "orders.amount_sum", "orders.total_revenue"], dialect=backend)
    assert by(resp, "customers.region", "orders.amount_sum")["EAST"] == 350.0


async def test_response_attributes_carry_the_keys(engine, backend: str) -> None:
    resp = await engine.execute({
        "population": "orders", "dimensions": [{"field": "customers.region", "label": "Region"}],
        "metrics": [{"metric": "sum(orders.amount)", "label": "Amount"}, {"metric": "count(customers.*)", "label": "N"},
                    {"metric": "count(orders.*)", "alias": "n_orders", "label": "Orders"}],
    })
    assert_keys(resp, ["customers.region", "orders.amount_sum", "customers._count", "n_orders"], dialect=backend)
    assert set(resp.attributes.dimensions) == {"customers.region"}
    assert set(resp.attributes.metrics) == {"orders.amount_sum", "customers._count", "n_orders"}


async def test_a_row_count_keys_by_its_anchor(engine, backend: str) -> None:
    resp = await engine.execute({"population": "customers", "metrics": ["count(orders.*)", "count(customers.*)"]})
    assert_keys(resp, ["orders._count", "customers._count"], dialect=backend)
    assert table(resp, "orders._count", "customers._count") == {(4.0, 4.0)}


@pytest.mark.parametrize("population", ["orders", "customers"])
async def test_plain_aggregate_references_key_by_their_anchor(engine, backend: str, population: str) -> None:
    resp = await engine.execute({"population": population, "metrics": ["sum(orders.amount)", "count(orders.*)"]})
    assert_keys(resp, ["orders.amount_sum", "orders._count"], dialect=backend)


async def test_an_alias_keys_as_itself(engine, backend: str) -> None:
    resp = await engine.execute({
        "population": "customers", "metrics": [{"metric": "sum(orders.amount)", "alias": "revenue", "label": "Revenue"}],
    })
    assert_keys(resp, ["revenue"], dialect=backend)
    assert "revenue" in resp.attributes.metrics


async def test_an_aliased_dimension_keys_as_itself(engine, backend: str) -> None:
    resp = await engine.execute({
        "population": "orders", "dimensions": [{"field": "customers.region", "alias": "reg"}],
        "metrics": ["count(orders.*)"],
    })
    assert_keys(resp, ["reg", "orders._count"], dialect=backend)


# --------------------------------------------------------------------------- #
# Expressions
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("population", ["orders", "customers"])
async def test_expression_keys_carry_their_common_anchor(engine, backend: str, population: str) -> None:
    resp = await engine.execute({
        "population": population,
        "metrics": ["sum(orders.amount) / count(orders.*)", "sum(orders.amount) / count(customers.*)"],
    })
    assert_keys(resp, [
        f"orders.{sanitized('sum(amount) / count(*)')}",
        sanitized("sum(orders.amount) / count(customers.*)"),
    ], dialect=backend)


async def test_computed_dimension_keys_carry_their_common_anchor(engine, backend: str) -> None:
    resp = await engine.execute({
        "population": "orders",
        "dimensions": [{"field": "orders.amount * 2"}, {"field": "upper(customers.region)"}],
        "metrics": ["count(orders.*)"],
    })
    assert_keys(resp, [
        f"orders.{auto_name_from_expression('amount * 2')}",
        f"customers.{auto_name_from_expression('upper(region)')}",
        "orders._count",
    ], dialect=backend)


# --------------------------------------------------------------------------- #
# Anchors and spellings
# --------------------------------------------------------------------------- #
async def test_different_anchors_are_different_columns(engine, backend: str) -> None:
    resp = await engine.execute({
        "population": "orders", "dimensions": ["customers.region", "orders.customers.region"],
    })
    assert_keys(resp, ["customers.region", "orders.customers.region"], dialect=backend)
    assert all(r["customers.region"] == r["orders.customers.region"] for r in resp.data)


async def test_a_role_playing_route_keys_by_its_route(backend: str) -> None:
    async with seed_engine(backend, buyer=True, seller=True) as eng:
        resp = await eng.execute({"dimensions": ["orders.buyer.region", "orders.seller.region"]})
    assert_keys(resp, ["orders.buyer.region", "orders.seller.region"], dialect=backend)


async def test_hop_spelling_twins_are_one_value(backend: str) -> None:
    async with seed_engine(backend, buyer=True) as eng:
        resp = await eng.execute({
            "population": "orders", "dimensions": ["orders.customers.region", "orders.buyer.region"],
        })
    assert_keys(resp, ["orders.buyer.region"], dialect=backend)


# --------------------------------------------------------------------------- #
# Cached results
# --------------------------------------------------------------------------- #
async def test_a_cached_result_keeps_the_keys(engine, backend: str) -> None:
    q = {"population": "customers", "dimensions": ["customers.region"], "metrics": ["sum(orders.amount)"]}
    first = await engine.execute(q, cache=True)
    hit = await engine.execute(q, cache=True)
    assert engine.cache_size == 1
    assert first.columns == hit.columns == ["customers.region", "orders.amount_sum"]
    assert list(hit.attributes.metrics) == list(first.attributes.metrics)


async def test_spelling_twins_share_one_cached_result(backend: str) -> None:
    async with seed_engine(backend, buyer=True) as eng:
        one = await eng.execute({"population": "orders", "dimensions": ["orders.customers.region"]}, cache=True)
        two = await eng.execute({"population": "orders", "dimensions": ["orders.buyer.region"]}, cache=True)
        assert eng.cache_size == 1
    assert one.columns == two.columns == ["orders.buyer.region"]


# --------------------------------------------------------------------------- #
# Stage schemas
# --------------------------------------------------------------------------- #
S1 = {
    "name": "s1", "population": "orders",
    "dimensions": ["orders.status", "customers.region"],
    "metrics": [{"metric": "sum(orders.amount)", "alias": "tot"}, "count(orders.*)"],
}


async def test_a_stage_keys_without_its_population(engine, backend: str) -> None:
    resp = await engine.execute({k: v for k, v in S1.items() if k != "name"})
    assert_keys(resp, ["orders.status", "customers.region", "tot", "orders._count"], dialect=backend)


async def test_stage_columns_keep_their_names(engine, backend: str) -> None:
    resp = await engine.execute([
        S1, {"population": "s1", "dimensions": ["s1.status", "s1.customers__region", "s1.tot", "s1._count"]},
    ])
    assert_keys(resp, ["s1.status", "s1.customers__region", "s1.tot", "s1._count"], dialect=backend)
    assert table(resp, "s1.status", "s1.customers__region", "s1.tot", "s1._count") == {
        ("completed", "EAST", 150.0, 2.0), ("open", "EAST", 200.0, 1.0), ("open", "WEST", 300.0, 1.0),
    }


async def test_an_anchorless_expression_names_its_stage_column_by_its_key(engine, backend: str) -> None:
    key = sanitized("sum(orders.amount) / count(customers.*)")
    resp = await engine.execute([
        {"name": "s1", "population": "orders", "metrics": ["sum(orders.amount) / count(customers.*)"]},
        {"population": "s1", "dimensions": [f"s1.{key}"]},
    ])
    assert_keys(resp, [f"s1.{key}"], dialect=backend)


# --------------------------------------------------------------------------- #
# Saved metrics
# --------------------------------------------------------------------------- #
def _with_aov() -> list[SlayerDataset]:
    return seed_datasets(customers=customers_dataset(metrics=[DatasetMetric(name="aov", expression="count(*)")]))


async def test_a_saved_metric_reference_keys_by_its_anchor(backend: str) -> None:
    async with seed_engine(backend, datasets=_with_aov()) as eng:
        plain = await eng.execute({"population": "orders", "metrics": ["customers.aov"]})
        aliased = await eng.execute({"population": "orders", "metrics": [{"metric": "customers.aov", "alias": "x"}]})
    assert_keys(plain, ["customers.aov"], dialect=backend)
    assert "customers.aov" in plain.attributes.metrics
    assert_keys(aliased, ["x"], dialect=backend)
    assert "x" in aliased.attributes.metrics


# --------------------------------------------------------------------------- #
# Measure naming (dry-run key emission over the formula-naming mart)
# --------------------------------------------------------------------------- #
async def _dry(body: dict[str, Any]) -> SlayerResponse:
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
        await storage.save_dataset(mart_dataset())
        await storage.save_dataset(targets_dataset())
        return await SlayerQueryEngine(storage=storage).execute(SlayerQuery.model_validate(body), dry_run=True)


@pytest.mark.parametrize("population", ["mart", "targets"])
async def test_an_arithmetic_composite_keys_alike_under_every_population(population: str) -> None:
    resp = await _dry({"population": population, "metrics": ["sum(mart.logo_churn) / sum(mart.logo_bop)"]})
    assert resp.columns == ["mart.logo_churn_sum_logo_bop_sum"]
    assert 'AS "mart.logo_churn_sum_logo_bop_sum"' in (resp.sql or "")


async def test_an_explicit_name_wins() -> None:
    resp = await _dry({"population": "mart", "metrics": [
        {"metric": "sum(mart.logo_churn) / sum(mart.logo_bop)", "alias": "churn_ratio"},
    ]})
    assert resp.columns == ["churn_ratio"]
    assert 'AS "churn_ratio"' in (resp.sql or "")


MONTH = [{"field": "mart.created_at", "granularity": "month"}]


async def test_a_metric_named_change_keys_as_change() -> None:
    named = await _dry({"population": "mart", "time_dimensions": MONTH,
                        "metrics": [{"metric": "change(sum(mart.logo_churn))", "alias": "change"}]})
    chg = await _dry({"population": "mart", "time_dimensions": MONTH,
                      "metrics": [{"metric": "change(sum(mart.logo_churn))", "alias": "chg"}]})
    assert named.columns[-1] == "change"
    assert (named.sql or "") == (chg.sql or "").replace("chg", "change")


async def test_a_metric_named_change_is_referenceable() -> None:
    resp = await _dry({
        "population": "mart", "time_dimensions": MONTH,
        "metrics": [{"metric": "change(sum(mart.logo_churn))", "alias": "change"}],
        "filter": ["change > 0"], "order": [{"field": "change", "direction": "desc"}],
    })
    assert resp.columns[-1] == "change"
