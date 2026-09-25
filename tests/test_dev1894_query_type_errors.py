"""Every checker type error raised by a real query is a concrete ``QueryTypeError`` in the stable layout."""

from __future__ import annotations

from typing import Callable, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import (
    AssociationError,
    CanonicalAliasShadowsColumnError,
    ComputedDimensionError,
    DimensionTypeError,
    DistinctDimensionValuesError,
    DuplicateMeasureNameError,
    MeasureNameCollidesWithColumnError,
    ModelFilterError,
    NameCollisionError,
    ParameterGrainError,
    PartitionKeyError,
    PositionTypingError,
    QueryTypeError,
    ReaggregationError,
    SlayerError,
    TimeAxisError,
    TimeDimensionColumnError,
    TransformInputError,
    UnanalyzableDependencyError,
    UnsafeJoinInputError,
    WindowDurationError,
)
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
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

Case = Callable[[], Tuple[SlayerQuery, List[SlayerModel]]]

_MONTH = [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)]
_NO_AXIS = "sum(cumsum(amount:sum(partition_by=region)))"


def _m(formula: str, name: str) -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def _orders(*, extra_columns: Tuple[Column, ...] = (), **kw) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="profit", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            *extra_columns,
        ],
        **kw,
    )


async def _raise_of(case: Case) -> QueryTypeError:
    query, models = case()
    source = next(m for m in models if m.name == query.source_model)
    with pytest.raises(QueryTypeError) as ei:
        await _engine_generate(
            query=query, model=source, extra_models=[m for m in models if m is not source],
            dialect="duckdb", validate=False,
        )
    return ei.value


def _time_axis() -> Tuple[SlayerQuery, List[SlayerModel]]:
    return f1832.monthly_q(measures=[_m(_NO_AXIS, "m")], time_dimensions=f1832.month_td()), f1832.dev1832_models()


def _no_time_dimension() -> Tuple[SlayerQuery, List[SlayerModel]]:
    return orders_q(dimensions=["region"], measures=[_m("amount:sum(window='90d')", "w")]), f1739.dev1739_models()


def _bad_window() -> Tuple[SlayerQuery, List[SlayerModel]]:
    query = SlayerQuery(
        source_model="sales", time_dimensions=f1846.month_td(),
        measures=[_m("sum(revenue, window='90x')", "w")],
    )
    return query, f1846.dev1846_models()


def _rank_partition() -> Tuple[SlayerQuery, List[SlayerModel]]:
    return orders_q(dimensions=["region"], measures=[_m("rank(amount:sum, partition_by=city)", "r")]), f1739.dev1739_models()


#: (id, case, class, location, has suggestion)
FAMILY_CASES = [
    ("time-axis", _time_axis, TimeAxisError, "transform 'cumsum'", True),
    ("no-time-dimension", _no_time_dimension, TimeAxisError, None, True),
    ("window-duration", _bad_window, WindowDurationError, None, True),
    ("rank-partition-key", _rank_partition, PartitionKeyError, "transform 'rank'", True),
    ("aggregate-partition-key",
     lambda: (orders_q(dimensions=["region"], measures=[_m("amount:sum(partition_by=city)", "p")]), f1739.dev1739_models()),
     PartitionKeyError, "measure 'p'", True),
    ("unsafe-join-input",
     lambda: (f1945.orders_q(measures=[f1945.measure("amount:last(li_ts)")]), f1945.dev1945_models(orders_default="created_at")),
     UnsafeJoinInputError, "measure 'amount_last_li_ts'", True),
    ("unanalyzable-dependency",
     lambda: (f1945.orders_q(measures=[f1945.measure("amount:last")]), f1945.dev1945_models(orders_default="bad_ts")),
     UnanalyzableDependencyError, "measure 'amount_last'", True),
    ("transform-input",
     lambda: (f1832.orders_q(measures=[_m("sum(cumsum(weight) - 1)", "m")], time_dimensions=f1832.month_td()), f1832.dev1832_models()),
     TransformInputError, "transform 'cumsum'", True),
    ("computed-dimension",
     lambda: (SlayerQuery.model_validate({
         "source_model": "orders", "measures": [{"formula": "amount:sum", "name": "t"}],
         "dimensions": ["region", {"expression": "CASE WHEN amount:sum > 5000 THEN 1 ELSE 0 END", "name": "band"}],
     }), f1740.dev1740_models()),
     ComputedDimensionError, "dimension 'band'", False),
    ("association",
     lambda: (f1841.assoc_q(dimensions=["status"], measures=[_m("customers.spend:sum", "cm")]), f1841.keyless_root_models()),
     AssociationError, "measure 'cm'", True),
    ("parameter-grain",
     lambda: (f1847.sales_q(dimensions=["region"], measures=[_m(f"wavg({f1847.INNER_CR}, weight=id)", "w")]), f1847.dev1847_models()),
     ParameterGrainError, "measure 'w'", True),
    ("reaggregation",
     lambda: (orders_q(
         dimensions=["region"], time_dimensions=f1739.month_td(),
         measures=[_m("sum(amount:sum(partition_by=[region, city]), window='90d')", "w")],
     ), f1739.dev1739_models()),
     ReaggregationError, "measure 'w'", True),
    ("name-collision",
     lambda: (SlayerQuery.model_validate({
         "source_model": "orders", "measures": [{"formula": "amount:sum", "name": "rev"}],
         "dimensions": ["region", {"expression": "lower(city)", "name": "rev"}],
     }), f1740.dev1740_models()),
     NameCollisionError, "dimension 'rev'", True),
    ("stage-flatten-collision",
     lambda: (SlayerQuery(
         source_model="orders", time_dimensions=_MONTH,
         measures=[_m("cumsum(amount:sum)", "dup"), _m("cumsum(amount:sum)", "dup")],
     ), [_orders(default_time_dimension="created_at")]),
     NameCollisionError, None, True),
    ("dimension-type",
     lambda: (SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="loc")]),
              [_orders(extra_columns=(Column(name="loc", type=DataType.UNKNOWN, db_type="point"),))]),
     DimensionTypeError, "dimension 'loc'", True),
    ("model-filter",
     lambda: (SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="*:count")]),
              [_orders(measures=[_m("amount:sum", "tot")], filters=["tot > 100"])]),
     ModelFilterError, "model filter 'tot > 100'", True),
    ("position-typing",
     lambda: (SlayerQuery(
         source_model="orders", time_dimensions=_MONTH, measures=[ModelMeasure(formula="*:count")],
         filters=["_count > 1 or status == 'ok'"],
     ), [_orders()]),
     PositionTypingError, None, True),
    ("raw-rows",
     lambda: (SlayerQuery(
         source_model="orders", dimensions=[ColumnRef(name="status")],
         filters=["amount:sum > 100"], distinct_dimension_values=False,
     ), [_orders()]),
     DistinctDimensionValuesError, "filter 'amount:sum > 100'", True),
    ("time-dimension-column",
     lambda: (SlayerQuery(
         source_model="orders", measures=[_m("amount:sum", "t")],
         time_dimensions=[TimeDimension(dimension=ColumnRef(name="status"), granularity=TimeGranularity.MONTH)],
     ), [_orders()]),
     TimeDimensionColumnError, "time dimension 'status'", False),
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
        source_model="orders", dimensions=[ColumnRef(name="id")], measures=[_m("amount:sum", "status")],
    ), [_orders()]), MeasureNameCollidesWithColumnError, "measure 'status'", id="measure-collides-with-column"),
    pytest.param(lambda: (SlayerQuery(
        source_model="orders", dimensions=[ColumnRef(name="status")], measures=[_m("amount:sum", "rev2")],
    ), [_orders(extra_columns=(Column(name="amount_sum", type=DataType.DOUBLE),))]),
        CanonicalAliasShadowsColumnError, "measure 'rev2'", id="canonical-alias-shadows"),
    pytest.param(lambda: (SlayerQuery(
        source_model="orders", dimensions=[ColumnRef(name="status")],
        measures=[_m("revenue:sum", "profit_avg"), ModelMeasure(formula="profit:avg")],
    ), [_orders()]), DuplicateMeasureNameError, "measure 'profit_avg'", id="duplicate-measure-name"),
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
    await storage.save_model(f1832.monthly_model(), _validate=False)
    body = {
        "source_model": "monthly",
        "time_dimensions": [{"dimension": {"name": "ordered_at"}, "granularity": "month"}],
        "measures": [{"formula": _NO_AXIS, "name": "m"}],
    }
    resp = TestClient(create_app(storage=storage)).post("/query", json=body)
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"].startswith("TimeAxisError:")
