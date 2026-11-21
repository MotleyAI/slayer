"""A dataset with no to-one route to a time axis reaches the spine by association across its nearest fanning axis
(spec: queries/time-spine › Spine routes are nearest-axis and the spine is never crossed)."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError, UnresolvableDimensionRelationshipError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2015_fixtures import (
    DS_GRANULARITIES,
    JAN_MAR,
    MONTHS,
    broadcast_warnings,
    by_bucket,
    m,
    spine_datasets,
    spine_engine,
    spine_query,
)
from tests._dev2212_fixtures import (
    BACKENDS,
    DS,
    HIDE_C3,
    NOTES,
    notes_dataset,
    orders_dataset,
    run,
    seed_datasets,
    seed_engine,
    seed_tables,
    with_policy,
)
from tests._engine_helpers import _engine_generate, seeded_exec_engine
from tests._golden_harness import bind_golden_tests, record_raise
from tests._time_points_fixtures import PinnedClock

R = "customers.region"
SPINE = "time_spine.timestamp"
SPINE_MONTH = {"field": SPINE, "granularity": "month", "date_range": JAN_MAR}
CUSTOMERS = {"metric": "count(customers.*)", "alias": "c"}


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _monthly(resp: SlayerResponse, name: str, *, by: tuple[str, ...] = ()) -> dict:
    out: dict = {}
    for r in resp.data:
        key = (*(r[d] for d in by), str(r[SPINE])[:7]) if by else str(r[SPINE])[:7]
        assert key not in out, key
        v = r[name]
        out[key] = None if v is None else float(v)
    return out


def _associated(resp: SlayerResponse) -> list:
    return [w for w in resp.warnings or [] if getattr(w, "kind", None) == "associated"]


# ---------------------------------------------------------------------------
# Active entities per period
# ---------------------------------------------------------------------------

async def test_active_customers_per_month_through_a_fanning_axis(backend: str) -> None:
    async with seed_engine(backend) as eng:
        total = await run(eng, time_dimensions=[SPINE_MONTH], metrics=[CUSTOMERS])
        by_region = await run(eng, dimensions=[R], time_dimensions=[SPINE_MONTH], metrics=[CUSTOMERS])
    assert _monthly(total, "c") == {"2025-01": 2.0, "2025-02": 2.0, "2025-03": 0.0}
    assert _associated(total)
    assert not broadcast_warnings(total)
    assert _monthly(by_region, "c", by=(R,)) == {
        ("EAST", "2025-01"): 2.0, ("EAST", "2025-02"): 1.0, ("EAST", "2025-03"): 0.0,
        ("WEST", "2025-01"): 0.0, ("WEST", "2025-02"): 1.0, ("WEST", "2025-03"): 0.0,
    }


async def test_a_model_filter_on_the_axis_dataset_limits_the_association(backend: str) -> None:
    datasets = seed_datasets(orders=orders_dataset(filters=["status = 'completed'"]))
    async with seed_engine(backend, datasets=datasets) as eng:
        resp = await run(eng, time_dimensions=[SPINE_MONTH], metrics=[CUSTOMERS])
    assert _monthly(resp, "c") == {"2025-01": 2.0, "2025-02": 0.0, "2025-03": 0.0}


async def test_axis_rows_hidden_by_a_policy_associate_nothing(backend: str) -> None:
    open_hidden = SessionPolicy(ruleset=ColumnFilterRuleset(column="status", value="completed",
                                                           on_unapplicable="pass"))
    async with seed_engine(backend) as eng, with_policy(eng, open_hidden) as scoped:
        resp = await run(scoped, time_dimensions=[SPINE_MONTH], metrics=[CUSTOMERS])
    assert _monthly(resp, "c") == {"2025-01": 2.0, "2025-02": 0.0, "2025-03": 0.0}


async def test_dimension_rows_hidden_by_a_policy(backend: str) -> None:
    async with seed_engine(backend) as eng, with_policy(eng, HIDE_C3) as scoped:
        active = await run(scoped, time_dimensions=[SPINE_MONTH], metrics=[CUSTOMERS])
        revenue = await run(scoped, dimensions=[R], time_dimensions=[SPINE_MONTH],
                            metrics=[{"metric": "sum(orders.amount)", "alias": "o"}])
    assert _monthly(active, "c") == {"2025-01": 2.0, "2025-02": 1.0, "2025-03": 0.0}
    # C3 is hidden: its February order's dimension path is broken, so it lands in the NULL tuple
    assert _monthly(revenue, "o", by=(R,)) == {
        ("EAST", "2025-01"): 150.0, ("EAST", "2025-02"): 200.0, ("EAST", "2025-03"): None,
        (None, "2025-01"): None, (None, "2025-02"): 300.0, (None, "2025-03"): None,
    }


# ---------------------------------------------------------------------------
# Ties, precedence and modes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("shape", ["measure", "dimension-only"])
async def test_equal_distance_fanning_axes_fail_loudly(backend: str, shape: str) -> None:
    query = (spine_query(metrics=[m("count(customers.id)", "c")], date_range=JAN_MAR) if shape == "measure"
             else spine_query(metrics=[], date_range=JAN_MAR, dimensions=["customers.region"]))
    async with spine_engine(backend) as eng:
        with pytest.raises(UnresolvableDimensionRelationshipError) as exc:
            await eng.execute(query)
    msg = str(exc.value)
    assert "customers.orders.order_date" in msg
    assert "customers.returns.return_date" in msg
    assert "time dimension" in msg.lower() or "time_dimensions" in msg


@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_tie_remedies(backend: str) -> None:
    async with spine_engine(backend) as eng:
        broadcast = await eng.execute(spine_query(metrics=[m("count(customers.id)", "c")], date_range=JAN_MAR,
                                                  to_many_handling="broadcast"))
        spelled = await eng.execute(SlayerQuery.model_validate({
            "metrics": [m("count(customers.id)", "c")],
            "time_dimensions": [{"field": "customers.orders.order_date", "granularity": "month"}],
        }))
        facts = await eng.execute(spine_query(metrics=[m("sum(orders.amount)", "o")], date_range=JAN_MAR))
    assert by_bucket(broadcast, ["c"]) == {k: (3.0,) for k in MONTHS[:3]}
    assert [w.metric for w in broadcast_warnings(broadcast)] == ["c"]
    # customer 3 has no order: its home row lands in the NULL bucket
    assert {str(r["customers.orders.order_date"])[:7]: float(r["c"]) for r in spelled.data} == {
        "2025-01": 2.0, "2025-02": 1.0, "None": 1.0}
    assert by_bucket(facts, ["o"]) == {"2025-01": (150.0,), "2025-02": (70.0,), "2025-03": (None,)}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_to_one_axis_beats_a_fanning_axis(backend: str) -> None:
    async with spine_engine(backend, datasets=spine_datasets(signed_up_at=True)) as eng:
        resp = await eng.execute(spine_query(metrics=[m("count(customers.id)", "c")]))
    assert by_bucket(resp, ["c"]) == {k: ((1.0,) if k in ("2025-01", "2025-04") else (0.0,)) for k in MONTHS}
    assert not _associated(resp)


async def test_broadcast_and_error_modes_follow_axiom_8(backend: str) -> None:
    async with seed_engine(backend) as eng:
        broadcast = await run(eng, time_dimensions=[SPINE_MONTH], metrics=[CUSTOMERS], to_many_handling="broadcast")
        with pytest.raises(SlayerError):
            await run(eng, time_dimensions=[SPINE_MONTH], metrics=[CUSTOMERS], to_many_handling="error")
    assert _monthly(broadcast, "c") == {k: 4.0 for k in ("2025-01", "2025-02", "2025-03")}
    assert broadcast_warnings(broadcast)


# ---------------------------------------------------------------------------
# Identity: keyless homes, composite and NULL keys
# ---------------------------------------------------------------------------

def _with_orders(*extra: tuple) -> list[TableSpec]:
    tables = seed_tables()
    orders = next(t for t in tables if t.name == "orders")
    return [orders.model_copy(update={"rows": [*orders.rows, *extra]}) if t is orders else t for t in tables]


async def test_a_keyless_home_counts_each_row_once_per_bucket(backend: str) -> None:
    tables = [*_with_orders(("O6", "C1", 40.0, "open", "2025-01-25")), NOTES]
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, tables),
                                  datasets=seed_datasets(extra=(notes_dataset(),))) as (eng, _):
        resp = await run(eng, time_dimensions=[SPINE_MONTH],
                         metrics=[{"metric": "sum(notes.amount)", "alias": "s"},
                                  {"metric": "count(notes.*)", "alias": "n"}])
    # C1 has two January orders: its note counts once there
    assert _monthly(resp, "s") == {"2025-01": 5.0, "2025-02": 11.0, "2025-03": None}
    assert _monthly(resp, "n") == {"2025-01": 1.0, "2025-02": 2.0, "2025-03": 0.0}


ACCOUNTS = TableSpec(name="accounts", columns=[("region", "TEXT"), ("acct", "INT"), ("name", "TEXT")],
                     rows=[("EU", 1, "a"), ("EU", 2, "b"), ("US", 1, "c"), ("US", None, "d")])
EVENTS = TableSpec(name="events", columns=[("event_id", "TEXT"), ("region", "TEXT"), ("acct", "INT"), ("happened_on", "DATE")],
                   rows=[("E1", "EU", 1, "2025-01-05"), ("E2", "EU", 2, "2025-02-01"),
                         ("E3", "US", None, "2025-02-02"), ("E4", "US", 1, "2025-03-03")])
COMPOSITE = [
    SlayerDataset(name="accounts", sql_table="accounts", data_source=DS, fields=[
        DatasetField(name="region", datatype=DataType.STRING, primary_key=True),
        DatasetField(name="acct", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="name", datatype=DataType.STRING)]),
    SlayerDataset(name="events", sql_table="events", data_source=DS, fields=[
        DatasetField(name="event_id", datatype=DataType.STRING, primary_key=True),
        DatasetField(name="region", datatype=DataType.STRING), DatasetField(name="acct", datatype=DataType.INTEGER),
        DatasetField(name="happened_on", datatype=DataType.DATE)],
        relationships=[DatasetRelationship(to="accounts", from_columns=["region", "acct"],
                                           to_columns=["region", "acct"])]),
]


@contextlib.asynccontextmanager
async def composite_engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, [ACCOUNTS, EVENTS]),
                                  datasets=COMPOSITE) as (eng, _):
        yield eng


async def test_composite_keys_associate_on_every_component(backend: str) -> None:
    async with composite_engine(backend) as eng:
        resp = await run(eng, time_dimensions=[SPINE_MONTH], metrics=[{"metric": "count(accounts.*)", "alias": "c"}])
    # a match on acct alone would count (US, 1) in January; NULL keys never match
    assert _monthly(resp, "c") == {"2025-01": 1.0, "2025-02": 1.0, "2025-03": 1.0}


async def test_composite_keys_by_a_dimension(backend: str) -> None:
    async with composite_engine(backend) as eng:
        resp = await run(eng, dimensions=["accounts.name"], time_dimensions=[SPINE_MONTH],
                         metrics=[{"metric": "count(accounts.*)", "alias": "c"}])
    got = _monthly(resp, "c", by=("accounts.name",))
    assert {k[0] for k in got} == {"a", "b", "c"}
    assert {k: v for k, v in got.items() if v} == {("a", "2025-01"): 1.0, ("b", "2025-02"): 1.0,
                                                   ("c", "2025-03"): 1.0}


# ---------------------------------------------------------------------------
# Routing without a spine dimension is unchanged: SQL pinned on the pre-change tree
# ---------------------------------------------------------------------------

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev2218_non_spine_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}


def _cases() -> dict[str, dict[str, Any]]:
    return {
        "appendix_b/customers_by_order_month": {
            "metrics": [CUSTOMERS], "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}]},
        "appendix_b/customers_by_region_filtered": {
            "dimensions": [R], "metrics": [CUSTOMERS, {"metric": "sum(returns.amount)", "alias": "r"}],
            "filter": ["orders.amount > 60"]},
        "appendix_b/notes_by_order_month": {
            "metrics": [{"metric": "sum(notes.amount)", "alias": "s"}],
            "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}]},
        "spine_seed/returns_by_order_month": {
            "population": "orders", "metrics": [m("sum(returns.amount)", "r")],
            "time_dimensions": [{"field": "orders.order_date", "granularity": "month"}]},
        "spine_seed/customers_by_spelled_axis": {
            "metrics": [m("count(customers.id)", "c")],
            "time_dimensions": [{"field": "customers.orders.order_date", "granularity": "month"}]},
    }


async def _generate_one(query: dict[str, Any], dialect: str):
    models = spine_datasets() if "order_date" in str(query) else seed_datasets(extra=(notes_dataset(),))
    try:
        return await _engine_generate(
            query=SlayerQuery.model_validate(query), dataset=models[0], extra_datasets=models[1:], dialect=dialect,
            validate=False, clock=PinnedClock(), datasource_fields=DS_GRANULARITIES,
        )
    except Exception as exc:  # noqa: BLE001 — the exception itself is contract
        return record_raise(exc)


bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def test_no_case_records_an_error(baseline) -> None:
    assert not [k for k, v in baseline.items() if isinstance(v, dict) and "error" in v]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_non_spine_association_results_are_unchanged(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, metrics=[CUSTOMERS], time_dimensions=[{"field": "orders.ordered_at",
                                                                     "granularity": "month"}])
    assert {str(r["orders.ordered_at"])[:7]: float(r["c"]) for r in resp.data} == {
        "2025-01": 2.0, "2025-02": 2.0, "None": 1.0}
