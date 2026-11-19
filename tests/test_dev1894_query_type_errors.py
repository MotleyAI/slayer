"""Every checker type error raised by a real query is a concrete ``QueryTypeError`` in the stable layout."""

from __future__ import annotations

from typing import Callable, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import (
    AssociationError,
    CanonicalAliasShadowsFieldError,
    ComputedDimensionError,
    DimensionTypeError,
    DistinctDimensionValuesError,
    DuplicateMetricNameError,
    MetricNameCollidesWithFieldError,
    DatasetFilterError,
    NameCollisionError,
    ParameterGrainError,
    PartitionKeyError,
    PositionTypingError,
    QueryTypeError,
    ReaggregationError,
    SlayerError,
    TimeAxisError,
    TimeDimensionFieldError,
    TransformInputError,
    UnanalyzableDependencyError,
    UnsafeRelationshipInputError,
    WindowDurationError,
)
from slayer.core.models import DatasetField, DatasourceConfig, DatasetMetric, SlayerDataset
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.storage.yaml_storage import YAMLStorage

from tests import _dev1739_fixtures as f1739
from tests import _dev1740_fixtures as f1740
from tests import _dev1832_fixtures as f1832
from tests import _dev1841_fixtures as f1841
from tests import _dev1846_fixtures as f1846
from tests import _dev1847_fixtures as f1847
from tests import _dev1945_fixtures as f1945
from tests._dev1824_fixtures import q as orders_q
from tests._engine_helpers import _engine_generate

Case = Callable[[], Tuple[SlayerQuery, List[SlayerDataset]]]

_MONTH = [TimeDimension(field="orders.created_at", granularity=TimeGranularity.MONTH)]
_NO_AXIS = "sum(cumsum(sum(monthly.amount, partition_by=monthly.region)))"


def _m(metric: str, alias: str) -> QueryMetric:
    return QueryMetric(metric=metric, alias=alias)


def _orders(*, extra_columns: Tuple[DatasetField, ...] = (), **kw) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="revenue", datatype=DataType.FLOAT),
            DatasetField(name="profit", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            *extra_columns,
        ],
        **kw,
    )


async def _raise_of(case: Case) -> QueryTypeError:
    query, models = case()
    source = next(m for m in models if m.name == query.population)
    with pytest.raises(QueryTypeError) as ei:
        await _engine_generate(
            query=query, dataset=source, extra_datasets=[m for m in models if m is not source],
            dialect="duckdb", validate=False,
        )
    return ei.value


def _time_axis() -> Tuple[SlayerQuery, List[SlayerDataset]]:
    return f1832.monthly_q(metrics=[_m(_NO_AXIS, "m")], time_dimensions=f1832.month_td()), f1832.dev1832_datasets()


def _no_time_dimension() -> Tuple[SlayerQuery, List[SlayerDataset]]:
    return orders_q(dimensions=["region"], metrics=[_m("sum(orders.amount, window='90d')", "w")]), f1739.dev1739_datasets()


def _bad_window() -> Tuple[SlayerQuery, List[SlayerDataset]]:
    query = SlayerQuery(
        population="sales", time_dimensions=f1846.month_td(),
        metrics=[_m("sum(sales.revenue, window='90x')", "w")],
    )
    return query, f1846.dev1846_datasets()


def _rank_partition() -> Tuple[SlayerQuery, List[SlayerDataset]]:
    return orders_q(dimensions=["region"], metrics=[_m("rank(sum(orders.amount), partition_by=orders.city, direction='desc')", "r")]), f1739.dev1739_datasets()


#: (id, case, class, location, has suggestion)
FAMILY_CASES = [
    ("time-axis", _time_axis, TimeAxisError, "transform 'cumsum'", True),
    ("no-time-dimension", _no_time_dimension, TimeAxisError, None, True),
    ("window-duration", _bad_window, WindowDurationError, None, True),
    ("rank-partition-key", _rank_partition, PartitionKeyError, "transform 'rank'", True),
    ("aggregate-partition-key",
     lambda: (orders_q(dimensions=["region"], metrics=[_m("sum(orders.amount, partition_by=orders.city)", "p")]), f1739.dev1739_datasets()),
     PartitionKeyError, "metric 'p'", True),
    ("unsafe-join-input",
     lambda: (f1945.orders_q(metrics=[f1945.metric("last(orders.amount, orders.li_ts)")]), f1945.dev1945_datasets(orders_default="created_at")),
     UnsafeRelationshipInputError, "metric 'amount_last_li_ts'", True),
    ("unanalyzable-dependency",
     lambda: (f1945.orders_q(metrics=[f1945.metric("last(orders.amount)")]), f1945.dev1945_datasets(orders_default="bad_ts")),
     UnanalyzableDependencyError, "metric 'amount_last'", True),
    ("transform-input",
     lambda: (f1832.orders_q(metrics=[_m("sum(cumsum(orders.weight) - 1)", "m")], time_dimensions=f1832.month_td()), f1832.dev1832_datasets()),
     TransformInputError, "transform 'cumsum'", True),
    ("computed-dimension",
     lambda: (SlayerQuery.model_validate({
         "population": "orders", "metrics": [{"metric": "sum(orders.amount)", "alias": "t"}],
         "dimensions": ["orders.region", {"field": "CASE WHEN sum(orders.amount) > 5000 THEN 1 ELSE 0 END", "alias": "band"}],
     }), f1740.dev1740_datasets()),
     ComputedDimensionError, "dimension 'band'", False),
    ("association",
     lambda: (f1841.assoc_q(dimensions=["orders.status"], metrics=[_m("sum(customers.spend, window='90d')", "cm")],
                            time_dimensions=[TimeDimension(field="customers.signup_at", granularity=TimeGranularity.MONTH)]),
              f1841.dev1840_datasets()),
     AssociationError, "metric 'cm'", True),
    ("parameter-grain",
     lambda: (f1847.sales_q(dimensions=["region"], metrics=[_m(f"wavg({f1847.INNER_CR}, weight=id)", "w")]), f1847.dev1847_datasets()),
     ParameterGrainError, "metric 'w'", True),
    ("reaggregation",
     lambda: (orders_q(
         dimensions=["region"], time_dimensions=f1739.month_td(),
         metrics=[_m("sum(sum(orders.amount, partition_by=[orders.region, orders.city]), window='90d')", "w")],
     ), f1739.dev1739_datasets()),
     ReaggregationError, "metric 'w'", True),
    ("name-collision",
     lambda: (SlayerQuery.model_validate({
         "population": "orders", "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
         "dimensions": ["orders.region", {"field": "lower(orders.city)", "alias": "rev"}],
     }), f1740.dev1740_datasets()),
     NameCollisionError, "dimension 'rev'", True),
    ("stage-flatten-collision",
     lambda: (SlayerQuery(
         population="orders", time_dimensions=_MONTH,
         metrics=[_m("cumsum(sum(orders.amount))", "dup"), _m("cumsum(sum(orders.amount))", "dup")],
     ), [_orders(default_time_dimension="created_at")]),
     NameCollisionError, None, True),
    ("dimension-type",
     lambda: (SlayerQuery(population="orders", dimensions=[QueryDimension(field="orders.loc")]),
              [_orders(extra_columns=(DatasetField(name="loc", datatype=DataType.OPAQUE, db_type="point"),))]),
     DimensionTypeError, "dimension 'loc'", True),
    ("model-filter",
     lambda: (SlayerQuery(population="orders", metrics=[QueryMetric(metric="count(orders.*)")]),
              [_orders(metrics=[DatasetMetric(name="tot", expression="sum(amount)")], filters=["tot > 100"])]),
     DatasetFilterError, "dataset filter 'tot > 100'", True),
    ("position-typing",
     lambda: (SlayerQuery(
         population="orders", time_dimensions=_MONTH, metrics=[QueryMetric(metric="count(orders.*)")],
         filter=["orders._count > 1 or orders.status == 'ok'"],
     ), [_orders()]),
     PositionTypingError, None, True),
    ("raw-rows",
     lambda: (SlayerQuery(
         population="orders", dimensions=[QueryDimension(field="orders.status")],
         filter=["sum(orders.amount) > 100"], distinct_dimension_values=False,
     ), [_orders()]),
     DistinctDimensionValuesError, "filter 'sum(amount) > 100'", True),
    ("time-dimension-column",
     lambda: (SlayerQuery(
         population="orders", metrics=[_m("sum(orders.amount)", "t")],
         time_dimensions=[TimeDimension(field="orders.status", granularity=TimeGranularity.MONTH)],
     ), [_orders()]),
     TimeDimensionFieldError, "time dimension 'status'", False),
]


@pytest.mark.parametrize(
    ("case", "cls", "location", "has_suggestion"),
    [pytest.param(*row[1:], id=row[0]) for row in FAMILY_CASES],
)
async def test_family_error_layout(
    case: Case, cls: type, location: Optional[str], has_suggestion: bool,
) -> None:
    exc = await _raise_of(case)
    assert type(exc) is cls, f"{type(exc).__name__}: {exc}"
    assert isinstance(exc, SlayerError)
    assert isinstance(exc, ValueError)
    assert isinstance(exc, QueryTypeError)
    lines = str(exc).splitlines()
    assert lines[0] == f"{cls.__name__}: {exc.summary}"
    assert exc.location == location
    at_lines = [line for line in lines if line.startswith("  at ")]
    assert at_lines == ([f"  at {location}"] if location is not None else [])
    if has_suggestion:
        assert exc.suggestion
        assert lines[-1] == f"  suggestion: {exc.suggestion}"
    else:
        assert exc.suggestion is None
        assert not any(line.startswith("  suggestion: ") for line in lines)


@pytest.mark.parametrize(("case", "cls", "location"), [
    pytest.param(lambda: (SlayerQuery(
        population="orders", dimensions=[QueryDimension(field="orders.id")], metrics=[_m("sum(orders.amount)", "status")],
    ), [_orders()]), MetricNameCollidesWithFieldError, "metric 'status'", id="measure-collides-with-column"),
    pytest.param(lambda: (SlayerQuery(
        population="orders", dimensions=[QueryDimension(field="orders.status")], metrics=[_m("sum(orders.amount)", "rev2")],
    ), [_orders(extra_columns=(DatasetField(name="amount_sum", datatype=DataType.FLOAT),))]),
        CanonicalAliasShadowsFieldError, "metric 'rev2'", id="canonical-alias-shadows"),
    pytest.param(lambda: (SlayerQuery(
        population="orders", dimensions=[QueryDimension(field="orders.status")],
        metrics=[_m("sum(orders.revenue)", "profit_avg"), QueryMetric(metric="avg(orders.profit)")],
    ), [_orders()]), DuplicateMetricNameError, "metric 'profit_avg'", id="duplicate-measure-name"),
])
async def test_structured_name_collisions(case: Case, cls: type, location: str) -> None:
    exc = await _raise_of(case)
    assert type(exc) is cls, f"{type(exc).__name__}: {exc}"
    assert isinstance(exc, NameCollisionError)
    assert str(exc).splitlines()[:2] == [f"{cls.__name__}: {exc.summary}", f"  at {location}"]
    assert exc.location == location


async def test_partition_key_suggestion_names_remedy_and_dimensions() -> None:
    exc = await _raise_of(_rank_partition)
    assert isinstance(exc, PartitionKeyError)
    assert exc.suggestion is not None
    assert "dimensions/time_dimensions" in exc.suggestion
    assert "region" in exc.suggestion


async def test_time_axis_violation_is_a_type_error_not_unimplemented() -> None:
    exc = await _raise_of(_time_axis)
    assert isinstance(exc, TimeAxisError)
    assert not isinstance(exc, NotImplementedError)
    assert exc.suggestion is not None
    assert "partition_by=" in exc.suggestion
    assert "time key" in exc.suggestion


async def test_malformed_query_window_names_value_and_syntax() -> None:
    exc = await _raise_of(_bad_window)
    assert isinstance(exc, WindowDurationError)
    assert "90x" in exc.summary
    assert exc.suggestion is not None
    assert "1y2m3w5d6h7min8s" in exc.suggestion


async def test_rest_answers_time_axis_violation_with_400(tmp_path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(DatasourceConfig(name="test", type="duckdb"))
    await storage.save_dataset(f1832.monthly_dataset(), _validate=False)
    body = {
        "population": "monthly",
        "time_dimensions": [{"field": "monthly.ordered_at", "granularity": "month"}],
        "metrics": [{"metric": _NO_AXIS, "alias": "m"}],
    }
    resp = TestClient(create_app(storage=storage)).post("/query", json=body)
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"].startswith("TimeAxisError:")
