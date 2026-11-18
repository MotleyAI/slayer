"""``name = value`` inside an aggregation or transform call is a keyword argument only for a declared parameter,
and ``agg`` is a reserved function name (specs: queries/expression-grammar, models/aggregation-names)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.models import Aggregation, DatasetField, DatasourceConfig, DatasetMetric, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2215_fixtures import BACKENDS, dev2215_engine, m, num, orders_dataset, q, single

TD = [{"field": "created_at", "granularity": "month"}]


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as eng:
        yield eng


async def _value(engine: SlayerQueryEngine, expression: str) -> float | None:
    return num(single(await engine.execute(q(metrics=[m(expression)]))))


# --------------------------------------------------------------------------- #
# Keyword arguments versus comparison
# --------------------------------------------------------------------------- #
class TestKeywordOrComparison:
    async def test_declared_parameter_is_a_keyword(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "percentile(amount, p = 0.5)") == await _value(engine, "percentile(amount, 0.5)")

    @pytest.mark.parametrize(("formula", "expected"), [
        ("sum(status = 'paid')", 4.0),
        ("avg(status = 'paid')", 4 / 6),
        ("share(status = 'paid')", 4 / 6),
        ("count_distinct(status = 'paid')", 2.0),
    ])
    async def test_boolean_aggregate(self, engine: SlayerQueryEngine, formula: str, expected: float) -> None:
        assert await _value(engine, formula) == pytest.approx(expected)

    async def test_a_parameter_name_wins(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "weighted_avg(amount, weight = qty)") == pytest.approx(
            await _value(engine, "weighted_avg(amount, weight=qty)"))

    @pytest.mark.parametrize("formula", ["sum((weight = 1))", 'sum("weight" = 1)'])
    async def test_parenthesised_or_quoted_left_side_is_a_comparison(self, engine: SlayerQueryEngine,
                                                                      formula: str) -> None:
        assert await _value(engine, formula) == 3.0

    async def test_model_aggregation_parameters_in_any_case(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "trimmed(amount, CUTOFF = 25)") == pytest.approx(
            await _value(engine, "trimmed(amount, cutoff = 25)"))

    async def test_classification_before_argument_order_checks(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "weighted_avg(status = 'paid', weight = qty)") == pytest.approx(
            await _value(engine, "weighted_avg(iif(status = 'paid', 1, 0), weight = qty)"))

    async def test_comparison_in_a_later_position(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "corr(amount, qty = 1)") == pytest.approx(
            await _value(engine, "corr(amount, iif(qty = 1, 1, 0))"))

    async def test_duplicate_keyword_after_classification(self, engine: SlayerQueryEngine) -> None:
        query = q(metrics=[m("weighted_avg(amount, weight = qty, WEIGHT = qty)")])
        with pytest.raises(SlayerError, match="(?i)weight"):
            await engine.execute(query)

    async def test_positional_after_keyword_after_classification(self, engine: SlayerQueryEngine) -> None:
        query = q(metrics=[m("weighted_avg(weight = qty, amount)")])
        with pytest.raises(SlayerError):
            await engine.execute(query)

    async def test_transform_parameters(self, engine: SlayerQueryEngine) -> None:
        keyword = await engine.execute(q(time_dimensions=TD, metrics=[m("time_shift(sum(amount), PERIODS = -1)")]))
        plain = await engine.execute(q(time_dimensions=TD, metrics=[m("time_shift(sum(amount), -1)")]))
        assert keyword.data == plain.data

    async def test_comparison_inside_a_scalar_call(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "sum(iif(status = 'paid', amount, 0))") == 130.0


# --------------------------------------------------------------------------- #
# agg is reserved for measure evaluation
# --------------------------------------------------------------------------- #
class TestAggIsReserved:
    @pytest.mark.parametrize("query", [
        {"metrics": [{"expression": "AGG(amount)", "name": "v"}]},
        {"metrics": [{"expression": "agg(amount) + 1", "name": "v"}]},
        {"metrics": [{"expression": "count(*)", "name": "n"}], "filter": ["Agg(amount) > 1"]},
        {"dimensions": [{"expression": "agg(amount, partition_by=status)", "name": "a"}],
         "metrics": [{"expression": "count(*)", "name": "n"}]},
    ], ids=["measure", "arithmetic", "filter", "dimension"])
    async def test_a_call_is_refused(self, engine: SlayerQueryEngine, query: dict) -> None:
        built = q(**query)
        with pytest.raises(SlayerError, match="(?i)AGG.*reserved"):
            await engine.execute(built)

    async def test_a_new_aggregation_is_refused(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        model = orders_dataset(aggregations=[Aggregation(name="Agg", expression="SUM({value})")])
        with pytest.raises(SlayerError, match="agg"):
            await storage.save_dataset(model)

    async def test_a_measure_or_column_named_agg_is_legal(self) -> None:
        with_column = orders_dataset().model_copy(update={"fields": [
            *orders_dataset().fields, DatasetField(name="agg", sql="amount", datatype=DataType.DOUBLE)]})
        with_measure = SlayerDataset(name="totals", sql_table="orders", data_source="test",
                                   fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                                            DatasetField(name="amount", datatype=DataType.DOUBLE)],
                                   metrics=[DatasetMetric(name="agg", expression="sum(amount)")])
        async with dev2215_engine("sqlite", datasets=[with_column, with_measure]) as eng:
            await eng.storage.save_dataset(with_column)
            await eng.storage.save_dataset(with_measure)
            column = await eng.execute(q(metrics=[m("sum(agg)")]))
            measure = await eng.execute(q(population="totals", metrics=["agg"]))
        assert num(single(column)) == 160.0
        assert num(measure.data[0]["totals.agg"]) == 160.0
