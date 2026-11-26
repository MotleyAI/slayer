"""Executed DISTINCT aggregates and discrete / approximate percentiles on SQLite and DuckDB.

Spec: aggregations/distinct-and-percentiles.
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator

import pytest

from slayer.core.enums import DEFAULT_AGGREGATIONS_BY_TYPE, DataType
from slayer.core.errors import AggregationNotAllowedError, SlayerError
from slayer.core.models import Aggregation, DatasetMetric
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2458_fixtures import BACKENDS, NEW_BUILTINS, engine_for, m, num, orders_dataset, plan_type, q


@pytest.fixture(params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with engine_for(request.param) as e:
        yield e


async def _single(engine: SlayerQueryEngine, *metrics: dict[str, str]) -> dict:
    resp = await engine.execute(q(metrics=list(metrics)))
    assert len(resp.data) == 1, resp.data
    return resp.data[0]


async def _by(engine: SlayerQueryEngine, dimension: str, *metrics: dict[str, str]) -> dict:
    resp = await engine.execute(q(dimensions=[dimension], metrics=list(metrics)))
    return {r[dimension]: r for r in resp.data}


# --------------------------------------------------------------------------- #
# DISTINCT sum and average
# --------------------------------------------------------------------------- #
async def test_values_over_duplicated_inputs(engine: SlayerQueryEngine) -> None:
    """Scenario: Values over duplicated inputs."""
    row = await _single(engine, m("SUM(DISTINCT orders.qty)", "a"), m("sum_distinct(orders.qty)", "b"),
                        m("AVG(DISTINCT orders.qty)", "c"), m("MIN(DISTINCT orders.qty)", "d"),
                        m("MAX(DISTINCT orders.qty)", "e"))
    assert [num(row[k]) for k in "abcde"] == [6, 6, 2.0, 1, 3]


@pytest.mark.parametrize(("ossie", "native"), [
    ("SUM(DISTINCT orders.qty)", "sum_distinct(orders.qty)"),
    ("sum(distinct orders.qty)", "sum_distinct(orders.qty)"),
    ("AVG(DISTINCT orders.qty)", "avg_distinct(orders.qty)"),
    ("MIN(DISTINCT orders.qty)", "min(orders.qty)"),
    ("max(Distinct orders.qty)", "max(orders.qty)"),
    ("PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY orders.amount)", "percentile_disc(orders.amount, p=0.25)"),
    ("APPROX_PERCENTILE(orders.amount, 0.5)", "percentile_approx(orders.amount, p=0.5)"),
])
async def test_ossie_spelling_equals_the_native_spelling(engine: SlayerQueryEngine, ossie: str, native: str) -> None:
    """Scenario: Ossie spelling equals the native spelling — same result key, SQL and values."""
    a = await engine.execute(q(metrics=[ossie]))
    b = await engine.execute(q(metrics=[native]))
    assert (a.columns, a.sql, a.data) == (b.columns, b.sql, b.data)


async def test_filtered_distinct_aggregate(engine: SlayerQueryEngine) -> None:
    """Scenario: Filtered DISTINCT aggregate."""
    got = await _by(engine, "orders.status", m("SUM(DISTINCT orders.qty) FILTER (WHERE orders.region = 'EU')"))
    assert {k: num(r["v"]) for k, r in got.items()} == {"paid": 3, "pending": 1, "shipped": None, "void": None}


async def test_all_null_group(engine: SlayerQueryEngine) -> None:
    """Scenario: All-NULL group."""
    got = await _by(engine, "orders.status", m("sum_distinct(orders.qty)", "s"), m("avg_distinct(orders.qty)", "a"))
    assert (got["void"]["s"], got["void"]["a"]) == (None, None)
    assert (num(got["paid"]["s"]), num(got["paid"]["a"])) == (3, 1.5)


async def test_over_no_rows_is_null(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(metrics=[m(f"{agg}(orders.qty)", agg) for agg in ("sum_distinct", "avg_distinct")]
                                  + [m("percentile_disc(orders.qty, 0.5)", "pd"), m("percentile_approx(orders.qty, 0.5)", "pa")],
                                  filter=["orders.id > 100"]))
    assert resp.data == [{"sum_distinct": None, "avg_distinct": None, "pd": None, "pa": None}]


async def test_boolean_input(engine: SlayerQueryEngine) -> None:
    """Scenario: Boolean input."""
    row = await _single(engine, m("sum_distinct(orders.is_paid)"))
    assert row["v"] == 1 and not isinstance(row["v"], bool)
    assert plan_type("sum_distinct(orders.is_paid)") == DataType.INTEGER


def test_result_types() -> None:
    assert plan_type("sum_distinct(orders.qty)") == DataType.INTEGER
    assert plan_type("sum_distinct(orders.amount)") == DataType.FLOAT
    assert plan_type("avg_distinct(orders.qty)") == plan_type("avg(orders.qty)") == DataType.FLOAT


async def test_partition_by(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(dimensions=["orders.status", "orders.region"], metrics=[
        m("sum_distinct(orders.qty, partition_by=orders.region)", "s"),
        m("percentile_disc(orders.amount, 0.5, partition_by=orders.region)", "p")]))
    got = {(r["orders.status"], r["orders.region"]): (num(r["s"]), num(r["p"])) for r in resp.data}
    assert got == {("paid", "EU"): (3, 20), ("pending", "EU"): (3, 20), ("pending", "US"): (5, 30),
                   ("shipped", "US"): (5, 30), ("void", None): (None, None)}


async def test_association_dimension(engine: SlayerQueryEngine) -> None:
    """Scenarios: A dimension the home does not determine (sum_distinct and percentile_disc)."""
    got = await _by(engine, "order_items.product", m("sum_distinct(orders.qty)", "s"),
                    m("percentile_disc(orders.amount, 0.5)", "p"))
    assert {k: (num(r["s"]), num(r["p"])) for k, r in got.items()} == {"A": (6, 20), "B": (3, 10)}


# --------------------------------------------------------------------------- #
# Discrete percentile
# --------------------------------------------------------------------------- #
async def test_ossie_value(engine: SlayerQueryEngine) -> None:
    """Scenario: Ossie's value."""
    row = await _single(engine, m("PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY orders.amount)"))
    assert num(row["v"]) == 20


async def test_endpoints_and_duplicates(engine: SlayerQueryEngine) -> None:
    """Scenario: Endpoints and duplicates."""
    resp = await engine.execute({"population": "points", "metrics": [
        m("percentile_disc(points.x, 0)", "p0"), m("percentile_disc(points.x, 0.5)", "p5"),
        m("percentile_disc(points.x, 1)", "p1")]})
    assert [num(resp.data[0][k]) for k in ("p0", "p5", "p1")] == [5, 5, 9]


@pytest.mark.parametrize("agg", ["percentile_disc", "percentile_approx"])
async def test_positional_p_equals_named(engine: SlayerQueryEngine, agg: str) -> None:
    """Scenarios: Positional p equals named; Positional percentile equals named."""
    a = await engine.execute(q(metrics=[f"{agg}(orders.amount, 0.9)"]))
    b = await engine.execute(q(metrics=[f"{agg}(orders.amount, p=0.9)"]))
    assert (a.columns, a.sql, a.data) == (b.columns, b.sql, b.data)


@pytest.mark.parametrize("spelling", ["percentile_disc(orders.amount, P=0.9)", "percentile_approx(orders.amount, P = 0.9)"])
async def test_p_is_a_keyword_in_any_case(engine: SlayerQueryEngine, spelling: str) -> None:
    """Scenario: Keyword argument (queries/expression-grammar)."""
    a = await engine.execute(q(metrics=[spelling]))
    b = await engine.execute(q(metrics=[spelling.replace("P=", "p=").replace("P = ", "p=")]))
    assert (a.columns, a.sql, a.data) == (b.columns, b.sql, b.data)


async def test_text_and_date_inputs(engine: SlayerQueryEngine) -> None:
    """Scenario: Text and date inputs."""
    row = await _single(engine, m("percentile_disc(orders.status, 0.5)", "s"),
                        m("percentile_disc(orders.order_date, 0.5)", "d"))
    assert (row["s"], str(row["d"])[:10]) == ("pending", "2024-03-05")
    assert plan_type("percentile_disc(orders.status, 0.5)") == DataType.STRING
    assert plan_type("percentile_disc(orders.order_date, 0.5)") == DataType.DATE


def test_percentile_disc_preserves_numeric_type() -> None:
    assert plan_type("percentile_disc(orders.qty, 0.5)") == DataType.INTEGER
    assert plan_type("percentile_disc(orders.is_paid, 0.5)") == DataType.BOOLEAN


async def test_null_over_all_null_group(engine: SlayerQueryEngine) -> None:
    got = await _by(engine, "orders.status", m("percentile_disc(orders.amount, 0.5)", "d"),
                    m("percentile_approx(orders.amount, 0.5)", "a"))
    assert (got["void"]["d"], got["void"]["a"]) == (None, None)


INVALID_P = [
    "percentile_disc(amount, 1.5)", "percentile_disc(amount, -0.1)", "percentile_disc(amount, NULL)",
    "percentile_disc(amount, 'x')", "percentile_disc(amount, TRUE)", "percentile_disc(amount, amount)",
    "percentile_disc(amount, 0.5, p=0.5)",
]


@pytest.mark.parametrize("text", INVALID_P)
async def test_invalid_p_is_refused_at_save_and_at_query(engine: SlayerQueryEngine, text: str) -> None:
    """Scenario: Invalid p — the same typed error naming ``p`` at save time and at query time."""
    with pytest.raises(SlayerError, match=r"\bp\b") as saved:
        await engine.storage.save_dataset(orders_dataset(metrics=[DatasetMetric(name="bad", expression=text)]))
    with pytest.raises(SlayerError, match=r"\bp\b") as queried:
        await engine.execute(q(metrics=[m(re.sub(r"\bamount\b", "orders.amount", text))]))
    assert type(saved.value) is type(queried.value)


# --------------------------------------------------------------------------- #
# Approximate percentile
# --------------------------------------------------------------------------- #
async def test_approx_within_tolerance(engine: SlayerQueryEngine) -> None:
    """Scenario: Within tolerance on every Tier-1 database (SQLite and DuckDB)."""
    row = await _single(engine, m("APPROX_PERCENTILE(orders.amount, 0.5)"))
    assert abs(num(row["v"]) - 30) <= 1


async def test_approx_is_exact_on_sqlite() -> None:
    """Scenario: Exact where the database has no approximate percentile (SQLite)."""
    async with engine_for("sqlite") as engine:
        row = await _single(engine, m("percentile_approx(orders.amount, 0.9)", "a"),
                            m("percentile(orders.amount, 0.9)", "e"))
    assert row["a"] == row["e"]


@pytest.mark.parametrize("field", ["qty", "amount", "is_paid"])
def test_approx_result_type_is_percentiles(field: str) -> None:
    """Scenario: Float result over integer and decimal inputs (the plan's type)."""
    assert plan_type(f"percentile_approx(orders.{field}, 0.5)") == plan_type(f"percentile(orders.{field}, 0.5)")
    assert plan_type(f"percentile_approx(orders.{field}, 0.5)") == DataType.FLOAT


# --------------------------------------------------------------------------- #
# Default aggregations and types of the new built-ins
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("datatype", [DataType.INTEGER, DataType.FLOAT, DataType.DECIMAL, DataType.BOOLEAN])
def test_numeric_and_boolean_defaults_admit_all_four(datatype: DataType) -> None:
    assert set(NEW_BUILTINS) <= DEFAULT_AGGREGATIONS_BY_TYPE[datatype]


@pytest.mark.parametrize("datatype", [DataType.STRING, DataType.DATE, DataType.DATETIME, DataType.DATETIME_TZ])
def test_ordered_defaults_admit_only_percentile_disc(datatype: DataType) -> None:
    assert set(NEW_BUILTINS) & DEFAULT_AGGREGATIONS_BY_TYPE[datatype] == {"percentile_disc"}


async def test_text_field_defaults(engine: SlayerQueryEngine) -> None:
    """Scenario: Text field defaults."""
    row = await _single(engine, m("percentile_disc(orders.status, 0.5)"))
    assert row["v"] == "pending"
    with pytest.raises(AggregationNotAllowedError) as refused:
        await engine.execute(q(metrics=["sum_distinct(orders.status)"]))
    assert (refused.value.field.endswith("status"), refused.value.agg) == (True, "sum_distinct")


async def test_boolean_field_defaults(engine: SlayerQueryEngine) -> None:
    """Scenario: Boolean field defaults."""
    row = await _single(engine, m("percentile_disc(orders.is_paid, 0.5)"))
    assert row["v"] == True  # noqa: E712 — SQLite returns booleans as 1, as for max
    assert plan_type("percentile_disc(orders.is_paid, 0.5)") == DataType.BOOLEAN


@pytest.mark.parametrize("agg", NEW_BUILTINS)
async def test_a_sole_primary_key_admits_none(engine: SlayerQueryEngine, agg: str) -> None:
    args = ", 0.5" if agg.startswith("percentile") else ""
    with pytest.raises(AggregationNotAllowedError):
        await engine.execute(q(metrics=[f"{agg}(orders.id{args})"]))


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_models_own_sum_distinct(backend: str) -> None:
    """Scenario: A model's own sum_distinct."""
    orders = orders_dataset(aggregations=[Aggregation(name="sum_distinct", expression="SUM({value}) * 100")])
    async with engine_for(backend, orders=orders) as engine:
        row = await _single(engine, m("sum_distinct(orders.qty)"))
    assert num(row["v"]) == 900
