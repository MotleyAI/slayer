"""Stored definitions read their references from their declaring dataset, never as query references.

Spec: queries/references › A query reference is anchored at its first segment (stored definitions stay
dataset-local); queries/unrelated-datasets › Stored definitions keep refusing.

Matrix: {field ``sql``, dataset filter, saved metric, custom-aggregation default} × {bare field, ``*``, dotted
join, list element, quoted name, unreachable dataset}, each read by a qualified query whose population is not
the declaring dataset. Cells with no such definition form (``*`` in a field or default) are absent.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError, UnreachableDefinitionError, UnresolvableDimensionRelationshipError
from slayer.core.models import Aggregation, AggregationParam, DatasetField, DatasetMetric, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2137_fixtures import all_datasets, assignments_dataset, contractors_dataset, list_engine
from tests._dev2277_fixtures import (
    BACKENDS,
    DS,
    SEED_TABLES,
    customers_dataset,
    execute_recording,
    future_warnings,
    orders_dataset,
    seed_datasets,
)
from tests._engine_helpers import seeded_exec_engine

EVENTS = TableSpec(name="events", columns=[("id", "INT"), ("amount", "DOUBLE")], rows=[(1, 5.0), (2, 7.0)])


def events_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="events", sql_table="events", data_source=DS,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="amount", datatype=DataType.FLOAT)],
    )


def _f(name: str, sql: str, datatype: DataType = DataType.STRING) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, sql=sql)


def _agg(name: str, weight: str) -> Aggregation:
    return Aggregation(name=name, expression="SUM({value} * {weight})",
                       params=[AggregationParam(name="weight", sql=weight)])


#: ``customers`` definitions read by ``orders``-rooted queries (``managers`` reached from ``customers`` only).
CUSTOMER_FIELDS = [
    _f("region_label", "upper(region)"),
    _f("quoted_label", 'lower("region")'),
    _f("mgr_name", "managers.name"),
    _f("weight", "CASE WHEN region = 'EAST' THEN 2 ELSE 1 END", DataType.INTEGER),
]
CUSTOMER_METRICS = [
    DatasetMetric(name="aov", expression="sum(orders.amount) / count(*)"),
    DatasetMetric(name="n_rows", expression="count(*)"),
    DatasetMetric(name="spent", expression="sum(orders.amount)"),
    DatasetMetric(name="n_regions", expression='count("region")'),
]
#: ``orders`` custom aggregations read by ``customers``-rooted queries.
ORDER_AGGREGATIONS = [_agg("w_bare", "amount"), _agg("w_quoted", '"amount"'), _agg("w_join", "customers.weight")]


def definition_datasets(*, customer_filters: tuple[str, ...] = (), orders_managers: bool = True) -> list[SlayerDataset]:
    customers = customers_dataset(fields=CUSTOMER_FIELDS, metrics=CUSTOMER_METRICS, filters=list(customer_filters))
    orders = orders_dataset(managers=orders_managers, aggregations=ORDER_AGGREGATIONS)
    return [*seed_datasets(managers=True, customers=customers, orders=orders), events_dataset()]


@contextlib.asynccontextmanager
async def definitions_engine(backend: str, *, datasets: list[SlayerDataset] | None = None,
                             ) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [*SEED_TABLES, EVENTS]),
        datasets=definition_datasets() if datasets is None else datasets,
    ) as (engine, _db):
        yield engine


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncGenerator[SlayerQueryEngine]:
    async with definitions_engine(request.param) as eng:
        yield eng


def _num(v: Any) -> Any:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def column(resp: SlayerResponse, name: str) -> str:
    """The result key named ``name`` (an alias, or a dotted key ending in it), whatever the key prefix."""
    hits = [k for k in resp.columns if k == name or k.endswith(f".{name}")]
    assert len(hits) == 1, (name, resp.columns)
    return hits[0]


def values(resp: SlayerResponse, key: str, value: str) -> dict[Any, Any]:
    k, v = column(resp, key), column(resp, value)
    return {r[k]: _num(r[v]) for r in resp.data}


async def run_clean(engine: SlayerQueryEngine, body: dict[str, Any]) -> SlayerResponse:
    """Execute a qualified ``body``; no deprecation is raised for any reference."""
    resp, caught = await execute_recording(engine, body)
    assert future_warnings(caught) == []
    assert [w for w in resp.warnings or [] if getattr(w, "kind", None) == "deprecated_input"] == []
    return resp


# --------------------------------------------------------------------------- #
# The spec scenario
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("backend", BACKENDS)
async def test_stored_definitions_stay_dataset_local(backend: str) -> None:
    # One route from the ``customers`` home to ``orders``: no direct ``orders → managers`` edge.
    async with definitions_engine(backend, datasets=definition_datasets(orders_managers=False)) as engine:
        resp = await run_clean(engine, {
            "population": "orders",
            "dimensions": ["orders.order_id", "customers.region_label"],
            "metrics": [{"metric": "customers.aov", "alias": "aov"}],
        })
    assert {(r[column(resp, "order_id")], r[column(resp, "region_label")]) for r in resp.data} == {
        ("O1", "EAST"), ("O2", "EAST"), ("O3", "EAST"), ("O4", "WEST"),
    }
    # aov per order row: the order's own amount (its home reads ``orders.order_id`` as itself) over the customer's own
    # row count (1).
    assert values(resp, "order_id", "aov") == {"O1": 100.0, "O2": 200.0, "O3": 50.0, "O4": 300.0}


# --------------------------------------------------------------------------- #
# Field sql
# --------------------------------------------------------------------------- #
FIELD_CELLS = {
    "bare-field": ("region_label", {"O1": "EAST", "O2": "EAST", "O3": "EAST", "O4": "WEST"}),
    "quoted-name": ("quoted_label", {"O1": "east", "O2": "east", "O3": "east", "O4": "west"}),
    # The customer's manager through customers' own edge, never orders' direct manager (O3 differs).
    "dotted-join": ("mgr_name", {"O1": "mia", "O2": "mia", "O3": "mia", "O4": "max"}),
}


@pytest.mark.parametrize("cell", sorted(FIELD_CELLS))
async def test_field_sql(engine: SlayerQueryEngine, cell: str) -> None:
    field, expected = FIELD_CELLS[cell]
    resp = await run_clean(engine, {"population": "orders", "dimensions": ["orders.order_id", f"customers.{field}"]})
    assert values(resp, "order_id", field) == expected


# --------------------------------------------------------------------------- #
# Saved metrics
# --------------------------------------------------------------------------- #
METRIC_CELLS = {
    "bare-field-and-star": ("aov", {"EAST": 175.0, "WEST": 300.0}),
    "star": ("n_rows", {"EAST": 2.0, "WEST": 1.0}),
    "dotted-join": ("spent", {"EAST": 350.0, "WEST": 300.0}),
    "quoted-name": ("n_regions", {"EAST": 2.0, "WEST": 1.0}),
}


@pytest.mark.parametrize("cell", sorted(METRIC_CELLS))
async def test_saved_metric(engine: SlayerQueryEngine, cell: str) -> None:
    metric, expected = METRIC_CELLS[cell]
    resp = await run_clean(engine, {
        "population": "orders", "dimensions": ["customers.region"],
        "metrics": [{"metric": f"customers.{metric}", "alias": metric}],
    })
    assert values(resp, "region", metric) == expected


# --------------------------------------------------------------------------- #
# Custom-aggregation defaults
# --------------------------------------------------------------------------- #
AGGREGATION_CELLS = {
    "bare-field": ("w_bare", {"EAST": 100.0 ** 2 + 200.0 ** 2 + 50.0 ** 2, "WEST": 300.0 ** 2}),
    "quoted-name": ("w_quoted", {"EAST": 100.0 ** 2 + 200.0 ** 2 + 50.0 ** 2, "WEST": 300.0 ** 2}),
    "dotted-join": ("w_join", {"EAST": (100.0 + 200.0 + 50.0) * 2, "WEST": 300.0}),
}


@pytest.mark.parametrize("cell", sorted(AGGREGATION_CELLS))
async def test_custom_aggregation_default(engine: SlayerQueryEngine, cell: str) -> None:
    aggregation, expected = AGGREGATION_CELLS[cell]
    resp = await run_clean(engine, {
        "population": "customers", "dimensions": ["customers.region"],
        "metrics": [{"metric": f"{aggregation}(customers.orders.amount)", "alias": "w"}],
    })
    got = values(resp, "region", "w")
    assert {k: v for k, v in got.items() if v is not None} == expected


# --------------------------------------------------------------------------- #
# Dataset filters
# --------------------------------------------------------------------------- #
FILTER_CELLS = {
    "bare-field": "region <> 'WEST'",
    "quoted-name": "\"region\" <> 'WEST'",
    "dotted-join": "managers.name <> 'max'",
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("cell", sorted(FILTER_CELLS))
async def test_dataset_filter(backend: str, cell: str) -> None:
    datasets = definition_datasets(customer_filters=(FILTER_CELLS[cell],))
    async with definitions_engine(backend, datasets=datasets) as eng:
        own = await run_clean(eng, {"population": "customers", "metrics": [{"metric": "count(customers.*)", "alias": "n"}]})
        assert _num(own.data[0][column(own, "n")]) == (3.0 if cell != "dotted-join" else 2.0)
        joined = await run_clean(eng, {"population": "orders", "dimensions": ["orders.order_id", "customers.region"]})
        assert values(joined, "order_id", "region") == {"O1": "EAST", "O2": "EAST", "O3": "EAST", "O4": None}


# --------------------------------------------------------------------------- #
# List elements (``contractors.skills``, read from ``assignments``)
# --------------------------------------------------------------------------- #
def _list_datasets(**contractors_extra: Any) -> list[SlayerDataset]:
    base = contractors_dataset()
    contractors = base.model_copy(update={
        "fields": [*base.fields, *contractors_extra.get("fields", [])],
        "metrics": [*base.metrics, *contractors_extra.get("metrics", [])],
        "filters": contractors_extra.get("filters", []),
    })
    return [contractors, *(d for d in all_datasets() if d.name != "contractors")]


async def _outcome(engine: SlayerQueryEngine, body: dict[str, Any]) -> Any:
    """The single value ``body`` returns, or its error type."""
    try:
        resp = await run_clean(engine, body)
    except SlayerError as exc:
        return type(exc)
    assert len(resp.data) == 1, resp.data
    return _num(resp.data[0][column(resp, "w")])


@pytest.mark.parametrize(("weight", "template"), [
    ("*", "SUM({value} * {weight})"),
    pytest.param("skills.value", "SUM({value} * length({weight}))", id="list-element"),
], ids=["star", "list-element"])
async def test_custom_aggregation_default_reads_like_from_its_home(weight: str, template: str) -> None:
    base = contractors_dataset()
    agg = Aggregation(name="w", expression=template, params=[AggregationParam(name="weight", sql=weight)])
    datasets = [base.model_copy(update={"aggregations": [agg]}), *(d for d in all_datasets() if d.name != "contractors")]
    metric = {"metric": "w(contractors.rate)", "alias": "w"}
    async with list_engine(datasets=datasets) as eng:
        home = await _outcome(eng, {"population": "contractors", "metrics": [metric]})
        other = await _outcome(eng, {"population": "assignments", "metrics": [metric]})
    assert other == home


async def test_list_element_saved_metric() -> None:
    datasets = _list_datasets(metrics=[DatasetMetric(name="n_skills", expression="count(skills.*)"),
                                       DatasetMetric(name="n_go", expression="count(skills.*) FILTER (WHERE skills.value = 'go')")])
    async with list_engine(datasets=datasets) as eng:
        resp = await run_clean(eng, {
            "population": "assignments", "dimensions": ["assignments.id"],
            "metrics": [{"metric": "contractors.n_skills", "alias": "n"}, {"metric": "contractors.n_go", "alias": "g"}],
        })
        assert values(resp, "id", "n") == {1: 3.0, 2: 3.0, 3: 1.0, 4: 0.0, 5: 1.0, 6: 0.0}
        assert values(resp, "id", "g") == {1: 2.0, 2: 2.0, 3: 0.0, 4: 0.0, 5: 1.0, 6: 0.0}


async def test_list_element_field_and_filter() -> None:
    datasets = _list_datasets(fields=[_f("knows_go", "list_contains(skills, 'go')", DataType.BOOLEAN)],
                              filters=["list_contains(skills, 'sql')"])
    async with list_engine(datasets=datasets) as eng:
        resp = await run_clean(eng, {"population": "assignments",
                                     "dimensions": ["assignments.id", "contractors.knows_go"]})
        assert values(resp, "id", "knows_go") == {1: True, 2: True, 3: False, 4: None, 5: None, 6: None}


# --------------------------------------------------------------------------- #
# Unreachable datasets keep refusing
# --------------------------------------------------------------------------- #
async def test_unreachable_saved_metric(engine: SlayerQueryEngine) -> None:
    await engine.save_dataset(customers_dataset(
        fields=CUSTOMER_FIELDS, metrics=[DatasetMetric(name="ev", expression="count(events.id)")]))
    with pytest.raises(UnresolvableDimensionRelationshipError, match="no route to target"):
        await engine.execute({"population": "orders", "metrics": [{"metric": "customers.ev", "alias": "e"}]})


@pytest.mark.parametrize("definition", ["field", "filter", "aggregation-default"])
async def test_unreachable_definition(engine: SlayerQueryEngine, definition: str) -> None:
    if definition == "field":
        dataset = customers_dataset(fields=[_f("ev_amount", "events.amount", DataType.FLOAT)])
        use = {"population": "orders", "dimensions": ["orders.order_id", "customers.ev_amount"]}
    elif definition == "filter":
        dataset = customers_dataset(filters=["events.amount > 1"])
        use = {"population": "orders", "dimensions": ["orders.order_id", "customers.region"]}
    else:
        dataset = orders_dataset(managers=True, aggregations=[_agg("w_ev", "events.amount")])
        use = {"population": "customers", "metrics": [{"metric": "w_ev(customers.orders.amount)", "alias": "w"}]}
    if definition != "aggregation-default":
        with pytest.raises(SlayerError) as saved:
            await engine.save_dataset(dataset)
        assert "events" in str(saved.value)
    await engine.storage.save_dataset(dataset, _validate=False)
    with pytest.raises((UnreachableDefinitionError, UnresolvableDimensionRelationshipError)) as used:
        await engine.execute(use)
    assert "events" in str(used.value)


async def test_a_definition_never_becomes_an_unrelated_product(engine: SlayerQueryEngine) -> None:
    """A query may read ``events`` as an unrelated anchor; a definition reading it still refuses."""
    await engine.storage.save_dataset(
        customers_dataset(fields=[_f("ev_amount", "events.amount", DataType.FLOAT)]), _validate=False)
    ok = await run_clean(engine, {"population": "customers",
                                  "metrics": [{"metric": "sum(events.amount)", "alias": "s"}]})
    assert _num(ok.data[0][column(ok, "s")]) == 12.0
    with pytest.raises(SlayerError, match="events"):
        await engine.execute({"population": "customers", "metrics": [{"metric": "sum(customers.ev_amount)", "alias": "s"}]})


def test_assignments_reach_contractors_to_one() -> None:
    """Fixture guard: the list cells read ``contractors`` across one many-to-one edge."""
    (edge,) = assignments_dataset().relationships
    assert edge.to == "contractors"
