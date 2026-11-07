"""Expression ranking keys, values and column-valued parameters (spec: aggregations/functional-form, aggregations/expression-aggregation)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ParameterGrainError, UnsafeJoinInputError
from slayer.core.models import Aggregation, AggregationParam, Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev2119_fixtures import BACKENDS, EFF, by_bucket, by_dims, dev2119_models, engine_for, m, num, q, td

# Avg price per product: A (rows 1, 3, 5) 7/3, B (rows 2, 4, 6) 14/3.
AVG_A = 7 / 3
AVG_B = 14 / 3


def _models() -> list[SlayerModel]:
    """The dev2119 models plus a ``wsum`` custom aggregation and an ``order_facts`` host reaching ``accounts``."""
    models = dev2119_models()
    orders = next(x for x in models if x.name == "orders")
    orders.aggregations.append(Aggregation(
        name="wsum", formula="SUM({value} * {weight})", params=[AggregationParam(name="weight", sql="quantity")],
    ))
    models.append(SlayerModel(
        name="order_facts", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="accounts", join_pairs=[["customer_id", "id"]])],
    ))
    return models


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with engine_for(request.param, models=_models()) as e:
        yield e


@pytest.fixture
async def sqlite_engine():
    async with engine_for("sqlite", models=_models()) as e:
        yield e


def _by_region(resp: SlayerResponse, value: str = "orders.v") -> dict:
    return {k[0]: v for k, v in by_dims(resp, ["orders.region"], value).items()}


def _single(resp: SlayerResponse) -> float | None:
    assert len(resp.data) == 1, resp.data
    return num(next(iter(resp.data[0].values())))


async def _sql(engine: SlayerQueryEngine, query: SlayerQuery) -> str:
    sql = (await engine.execute(query, dry_run=True)).sql
    assert sql is not None
    return sql


# --------------------------------------------------------------------------- #
# Default ranking key
# --------------------------------------------------------------------------- #
class TestDefaultRankingKey:
    async def test_temporal_computed_dimension_ranks_each_cell(self, engine: SlayerQueryEngine) -> None:
        """Spec scenario: each (EFF, month) cell holds one order, so it carries that order's amount."""
        resp = await engine.execute(q(
            dimensions=["id", {"expression": EFF, "name": "e"}], time_dimensions=[td("created_at")],
            measures=[m("last(amount)")],
        ))
        assert by_dims(resp, ["orders.id"]) == {(i,): float(10 * i) for i in range(1, 7)}

    async def test_expression_time_dimension_source(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(time_dimensions=[td(EFF)], measures=[m("last(amount)")]))
        key = "orders.coalesce_shipped_at_created_at"
        assert by_bucket(resp, key, "orders.v") == {"2025-01": 10.0, "2025-02": 30.0, "2025-03": 60.0, "2025-04": 50.0}


# --------------------------------------------------------------------------- #
# Expression ranking keys
# --------------------------------------------------------------------------- #
class TestExpressionRankingKey:
    async def test_coalesced_ranking_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "balances", "dimensions": ["customer_id"],
            "measures": [{"formula": "last(balance, coalesce(updated_at, created_at))", "name": "v"}],
        }))
        assert by_dims(resp, ["balances.customer_id"], "balances.v") == {(1,): 150.0, (2,): 70.0}

    async def test_cross_model_ranked_aggregate(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            dimensions=["customers.region"],
            measures=[m("last(customers.spend, coalesce(customers.updated_at, customers.signup_ts), "
                        "partition_by=customers.region)")],
        ))
        assert by_dims(resp, ["orders.customers.region"]) == {("North",): 300.0, ("South",): 200.0}

    async def test_fanning_leaf_local_producer(self, sqlite_engine: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "accounts",
            "measures": [{"formula": "last(spend, coalesce(orders.created_at, signup_ts))", "name": "v"}],
        })
        with pytest.raises(UnsafeJoinInputError) as ei:
            await sqlite_engine.execute(query)
        assert "created_at" in str(ei.value)
        assert "orders" in str(ei.value)

    async def test_fanning_leaf_target_rooted_producer(self, sqlite_engine: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "order_facts",
            "measures": [{"formula": "last(accounts.spend, coalesce(accounts.orders.created_at, accounts.signup_ts))",
                          "name": "v"}],
        })
        with pytest.raises(UnsafeJoinInputError) as ei:
            await sqlite_engine.execute(query)
        assert "created_at" in str(ei.value)
        assert "orders" in str(ei.value)

    async def test_distinct_ranking_keys_stay_separate_producers(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["region"], measures=[m(f"last(amount, {EFF})"), m("last(amount, created_at)", "c")])
        assert (await _sql(engine, query)).upper().count("ROW_NUMBER") == 2
        resp = await engine.execute(query)
        assert _by_region(resp) == {"EU": 50.0, "US": 60.0}
        assert _by_region(resp, "orders.c") == {"EU": 50.0, "US": 40.0}

    async def test_equal_ranking_keys_share_one_producer(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["region"], measures=[
            m(f"last(amount, {EFF})"), m("last(amount, COALESCE(orders.shipped_at,created_at))", "c"),
        ])
        assert (await _sql(engine, query)).upper().count("ROW_NUMBER") == 1
        resp = await engine.execute(query)
        assert _by_region(resp) == _by_region(resp, "orders.c") == {"EU": 50.0, "US": 60.0}

    @pytest.mark.parametrize("formula", [
        "last(amount, coalesce(created_at, max(created_at, partition_by=region)))",
        "last(amount, 1 + 1)",
    ])
    async def test_non_row_level_ranking_key_rejected(self, sqlite_engine: SlayerQueryEngine, formula: str) -> None:
        query = q(measures=[m(formula)])
        with pytest.raises(ValueError, match="ranks by a column"):
            await sqlite_engine.execute(query)


# --------------------------------------------------------------------------- #
# first / last over expression values
# --------------------------------------------------------------------------- #
class TestRankedExpressionValue:
    async def test_first_over_cross_model_expression(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m("first(amount - customers.discount)")]))
        got = _by_region(resp)
        assert got == {"EU": pytest.approx(9.9), "US": pytest.approx(59.9)}

    async def test_first_of_a_literal(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m("first(1)")]))
        assert _by_region(resp) == {"EU": 1.0, "US": 1.0}

    async def test_first_over_mixed_source(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            dimensions=["region"], measures=[m("first(quantity * avg(price, partition_by=product))")],
        ))
        assert _by_region(resp) == {"EU": pytest.approx(AVG_A), "US": pytest.approx(AVG_B)}


# --------------------------------------------------------------------------- #
# Column-valued parameters
# --------------------------------------------------------------------------- #
class TestExpressionParameters:
    async def test_expression_weight_named_and_positional(self, engine: SlayerQueryEngine) -> None:
        baseline = _single(await engine.execute(q(measures=[m("weighted_avg(price, weight=quantity)")])))
        assert baseline == pytest.approx(40 / 13)
        named = await engine.execute(q(measures=[{"formula": "weighted_avg(price, weight=quantity * 2)"}]))
        positional = await engine.execute(q(measures=[{"formula": "weighted_avg(price, quantity * 2)"}]))
        assert _single(named) == pytest.approx(baseline)
        assert _single(positional) == pytest.approx(baseline)
        assert named.columns == positional.columns

    async def test_expression_second_input_of_corr(self, engine: SlayerQueryEngine) -> None:
        plain = _single(await engine.execute(q(measures=[m("corr(amount, quantity)")])))
        shifted = _single(await engine.execute(q(measures=[m("corr(amount, quantity + 1)")])))
        assert shifted == pytest.approx(plain)

    async def test_cross_model_expression_weight(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("weighted_avg(amount, weight=coalesce(customers.discount, 0))")]))
        assert _single(resp) == pytest.approx(34 / 0.8)

    @pytest.mark.parametrize("formula", ["wsum(amount, weight=quantity + 1)", "wsum(amount, quantity + 1)"])
    async def test_custom_aggregation_expression_parameter(self, engine: SlayerQueryEngine, formula: str) -> None:
        assert _single(await engine.execute(q(measures=[m(formula)]))) == 690.0

    async def test_undetermined_expression_parameter(self, sqlite_engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["region"],
                  measures=[m("weighted_avg(sum(amount, partition_by=[city, region]), weight=id * 2)")])
        with pytest.raises(ParameterGrainError, match="weight"):
            await sqlite_engine.execute(query)
