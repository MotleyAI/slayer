"""An unattributable explicit ``partition_by=`` key resolves by the one mode gate, local or cross-model.

Executed on SQLite and DuckDB over the ``_dev1900_fixtures`` reference data. Covers
``queries/attribution-modes``, ``queries/cross-model-aggregates``, ``queries/transforms`` and
``queries/partitioned-aggregates``.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ModeASqlParseError, PartitionKeyError, SlayerError
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerResponse
from tests._dev1840_fixtures import month_key
from tests._dev1892_fixtures import assert_grain_residue, assert_ref_free
from tests._dev1900_fixtures import (
    ASSOC_AMOUNT_BY_BAD_POP,
    BAD_POP,
    dev1900_datasets,
    make_exec_engine,
    unparseable_derived_datasets,
)
from tests._dev1914_fixtures import (
    LOCAL_LAST_BY_STATUS,
    ORDERED_MONTH,
    SIGNUP_MONTH,
    WINDOW_BY_STATUS_SIGNUP,
    associated_warnings,
    broadcast_warnings,
    keyless,
    make_same_row_engine,
)
from tests._dev1919_fixtures import (
    OWN_FAN_BADPOP,
    OWN_FAN_STATUS,
    RANKED,
    local_wavg_status_assoc_by_status,
    ranked_by_tier,
)
from tests._dev1946_fixtures import ranked_assoc_by_status
from tests._dev2137_fixtures import list_engine

ASSOCIATING = [pytest.param(None, id="default"), pytest.param("associate", id="associate")]
REFUSING = ["broadcast", "error"]
HOP = "region_events"
EVENT_VALUE = "customers.regions.region_events.value"

#: ``sum(pop)`` by ``bad_pop``: North (150) and South (230), each region once.
POP_BY_BAD_POP = {150.0: 100.0, 230.0: 200.0}
POP_BY_BAD_POP2 = {300.0: 100.0, 460.0: 200.0}
#: Customers-rooted ``sum(spend, partition_by=orders.status)``; the orderless c7 forms the NULL cell.
SPEND_BY_STATUS = {"ok": 420.0, "new": 290.0, None: 55.0}
SPEND_BY_TIER_STATUS = {
    ("gold", "ok"): 190.0, ("gold", "new"): 100.0, ("gold", None): 55.0,
    ("silver", "ok"): 230.0, ("silver", "new"): 150.0, ("bronze", "new"): 40.0,
}
#: Customers-rooted ``sum(spend, partition_by=orders.ordered_at)`` by order month.
SPEND_BY_ORDER_MONTH = {"2024-01": 100.0, "2024-02": 150.0, "2024-03": 100.0, "2024-04": 110.0, None: 55.0}
#: The same with c8 (spend 1000; orders 2023-01 ok and 2024-03 new): it counts in both buckets.
SAME_ROW_SPEND_BY_ORDER_MONTH = {**SPEND_BY_ORDER_MONTH, "2023-01": 1000.0, "2024-03": 1100.0}
SAME_ROW_CUMSUM_BY_ORDER_MONTH = {
    "2023-01": 1000.0, "2024-01": 1100.0, "2024-02": 150.0, "2024-03": 2200.0, "2024-04": 2420.0,
}
#: ``sum(spend, partition_by=[orders.status, orders.ordered_at])`` with c8: one order supplies both keys.
SAME_ROW_SPEND_BY_STATUS_MONTH = {
    ("ok", "2024-01"): 100.0, ("new", "2024-01"): 100.0, ("ok", "2024-02"): 150.0, ("new", "2024-02"): 150.0,
    ("ok", "2024-03"): 60.0, ("new", "2024-03"): 1040.0, ("ok", "2024-04"): 110.0, ("ok", "2023-01"): 1000.0,
    (None, None): 55.0,
}
#: ``sum(spend, partition_by=[orders.status, customers.regions.region_events.value])``: separate branches.
SPEND_BY_STATUS_EVENT = {
    ("ok", 50.0): 280.0, ("new", 50.0): 250.0, ("ok", 30.0): 140.0, ("new", None): 40.0, (None, 30.0): 55.0,
}
#: ``sum(amount, window='90d', partition_by=bad_amt)``: each order its own cell but the two region-less ones.
WINDOWED_AMOUNT_BY_BAD_AMT_MONTH = {
    (60.0, "2024-01"): 10.0, (70.0, "2024-01"): 20.0, (53.0, "2024-01"): 3.0, (80.0, "2024-02"): 30.0,
    (75.0, "2024-02"): 25.0, (35.0, "2024-03"): 5.0, (None, "2024-03"): 47.0, (45.0, "2024-04"): 15.0,
    (62.0, "2024-04"): 12.0,
}
#: ``time_shift(sum(spend, partition_by=[orders.status, signup_at]), -1)`` by status and signup month.
SHIFTED_BY_STATUS_SIGNUP = {
    ("ok", "2024-01"): None, ("ok", "2024-02"): 100.0, ("ok", "2024-03"): 150.0, ("ok", "2024-04"): 60.0,
    ("new", "2024-01"): None, ("new", "2024-02"): 100.0, ("new", "2024-03"): 150.0, (None, "2024-04"): None,
}
#: ``last(spend, signup_at, window='1y', partition_by=orders.status)``: the latest signup among the cell's frame.
WINDOWED_LAST_BY_STATUS_SIGNUP = {
    ("ok", "2024-01"): 100.0, ("ok", "2024-02"): 150.0, ("ok", "2024-03"): 60.0, ("ok", "2024-04"): 30.0,
    ("new", "2024-01"): 100.0, ("new", "2024-02"): 150.0, ("new", "2024-03"): 40.0, (None, "2024-04"): 55.0,
}
#: Keyless customers: ``sum(amount, partition_by=customers.regions.name)`` across the unproven orders → customers hop.
AMOUNT_BY_REGION_NAME = {"North": 100.0, "South": 20.0, None: 47.0}
#: ``sum(rate, partition_by=contractors.skills.value)``: go = Ann + Eve, sql = Ann + Bob.
RATE_BY_SKILL = {"go": 60.0, "sql": 30.0}


# --------------------------------------------------------------------------- #
# Datasets and engines
# --------------------------------------------------------------------------- #
def _fan_datasets() -> list[SlayerDataset]:
    """An orders-local derived key crossing the fanning hop, plus a saved partitioned measure on regions."""
    models = dev1900_datasets()
    orders = next(m for m in models if m.name == "orders")
    orders.fields.append(DatasetField(
        name="bad_amt", datatype=DataType.FLOAT, expression="amount + customers.regions.region_events.value"))
    regions = next(m for m in models if m.name == "regions")
    regions.metrics.append(DatasetMetric(name="pop_by_bad_pop", expression="sum(pop, partition_by=bad_pop)"))
    return models


def _unparse_datasets() -> list[SlayerDataset]:
    models = unparseable_derived_datasets()
    regions = next(m for m in models if m.name == "regions")
    regions.fields.append(DatasetField(name="unparse2", datatype=DataType.FLOAT, expression="regions.unparseable * 2"))
    return models


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request, datasets=_fan_datasets()):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def same_row_engine(request):
    async for e in make_same_row_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def keyless_engine(request):
    async for e in make_exec_engine(request, datasets=keyless(dev1900_datasets(), name="customers")):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_engine(request):
    async for e in make_exec_engine(request, datasets=_unparse_datasets()):
        yield e


@pytest.fixture(params=[True, False], ids=["keyed", "keyless"])
async def lists(request):
    async with list_engine(keyed=request.param) as e:
        yield e


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def q(mode: Optional[str], population: str, **kw: Any) -> SlayerQuery:
    if mode is not None:
        kw["to_many_handling"] = mode
    return SlayerQuery.model_validate({"population": population, **kw})


def m(expression: str, alias: str = "w") -> QueryMetric:
    return QueryMetric(metric=expression, alias=alias)


def num(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def month(value: Any) -> Optional[str]:
    return None if value is None else month_key(value)


def cells(resp: SlayerResponse, *keys: str, value: str = "w", months: tuple[str, ...] = ()) -> dict:
    """``{key or key tuple: value}``; ``months`` names keys rendered as ``YYYY-MM``."""
    def k(row: dict) -> Any:
        parts = tuple(month(row[c]) if c in months else (num(row[c]) if isinstance(row[c], (int, float)) else row[c])
                      for c in keys)
        return parts[0] if len(parts) == 1 else parts
    out = {k(r): num(r[value]) for r in resp.data}
    assert len(out) == len(resp.data), resp.data  # a duplicate cell must not hide behind the last row
    return out


def assert_cells(got: dict, expected: dict) -> None:
    assert set(got) == set(expected), (got, expected)
    for key, value in expected.items():
        assert got[key] == (None if value is None else pytest.approx(value)), (key, got[key], value)


def assert_subset(got: dict, expected: dict) -> None:
    for key, value in expected.items():
        assert key in got, (key, got)
        assert got[key] == (None if value is None else pytest.approx(value)), (key, got[key], value)


def assert_no_mode_warnings(resp: SlayerResponse) -> None:
    assert not associated_warnings(resp), resp.warnings
    assert not broadcast_warnings(resp), resp.warnings


def assert_refusal(msg: str, *, key: str, home: str, hop: Optional[str] = None) -> None:
    """The shared partition-key refusal: key, hop and home named, both remedies, generic wording."""
    assert key in msg, msg
    assert home in msg, msg
    if hop is not None:
        assert hop in msg, msg
    assert "to_many_handling" in msg, msg
    assert "'associate'" in msg, msg
    assert "cardinality" in msg.lower(), msg
    assert "unique key" in msg, msg
    assert "cross-dataset" not in msg.lower(), msg
    assert "__regroup__" not in msg, msg
    assert_ref_free(msg)


async def refused(eng, query) -> str:
    with pytest.raises(PartitionKeyError) as ei:
        await eng.execute(query)
    return str(ei.value)


def regions_bad_pop(mode: Optional[str], expression: str = "sum(regions.pop, partition_by=regions.bad_pop)",
                    dim: str = "regions.bad_pop") -> SlayerQuery:
    return q(mode, "regions", dimensions=[dim], metrics=[m(expression)])


def customers_by_status(mode: Optional[str], expression: str, **kw: Any) -> SlayerQuery:
    kw.setdefault("dimensions", ["orders.status"])
    return q(mode, "customers", metrics=[m(expression)], **kw)


# --------------------------------------------------------------------------- #
# queries/attribution-modes › Unattributable explicit partition keys resolve per the mode
# --------------------------------------------------------------------------- #
class TestPathlessDerivedKey:
    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_associates_like_the_dimension_spelling(self, engine, mode):
        resp = await engine.execute(regions_bad_pop(mode))
        assert_cells(cells(resp, "regions.bad_pop"), POP_BY_BAD_POP)
        assert_no_mode_warnings(resp)
        dim = await engine.execute(regions_bad_pop(mode, "sum(regions.pop)"))
        assert_cells(cells(dim, "regions.bad_pop"), POP_BY_BAD_POP)

    @pytest.mark.parametrize("mode", REFUSING)
    async def test_refuses_outside_association(self, engine, mode):
        assert_refusal(await refused(engine, regions_bad_pop(mode)), key="bad_pop", home="regions", hop=HOP)


class TestPathBearingDerivedKey:
    @staticmethod
    def _q(mode, expression="sum(orders.amount, partition_by=customers.regions.bad_pop)"):
        return q(mode, "orders", dimensions=[BAD_POP], metrics=[m(expression)])

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_associates_like_the_dimension_spelling(self, engine, mode):
        resp = await engine.execute(self._q(mode))
        assert_cells(cells(resp, BAD_POP), ASSOC_AMOUNT_BY_BAD_POP)
        assert_no_mode_warnings(resp)
        dim = await engine.execute(self._q(mode, "sum(orders.amount)"))
        assert_cells(cells(dim, BAD_POP), ASSOC_AMOUNT_BY_BAD_POP)

    @pytest.mark.parametrize("mode", REFUSING)
    async def test_refuses_outside_association(self, engine, mode):
        assert_refusal(await refused(engine, self._q(mode)), key="bad_pop", home="orders", hop=HOP)


class TestChainedDerivedKey:
    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_resolves_like_its_base(self, engine, mode):
        resp = await engine.execute(regions_bad_pop(
            mode, "sum(regions.pop, partition_by=regions.bad_pop2)", dim="regions.bad_pop2"))
        assert_cells(cells(resp, "regions.bad_pop2"), POP_BY_BAD_POP2)
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", REFUSING)
    async def test_refuses_outside_association(self, engine, mode):
        msg = await refused(engine, regions_bad_pop(
            mode, "sum(regions.pop, partition_by=regions.bad_pop2)", dim="regions.bad_pop2"))
        assert_refusal(msg, key="bad_pop2", home="regions", hop=HOP)


class TestUnprovenHopKey:
    @staticmethod
    def _q(mode):
        return q(mode, "orders", dimensions=["customers.regions.name"],
                 metrics=[m("sum(orders.amount, partition_by=customers.regions.name)")])

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_associates_like_a_fanning_hop(self, keyless_engine, mode):
        resp = await keyless_engine.execute(self._q(mode))
        assert_cells(cells(resp, "customers.regions.name"), AMOUNT_BY_REGION_NAME)
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", REFUSING)
    async def test_refuses_outside_association(self, keyless_engine, mode):
        assert_refusal(await refused(keyless_engine, self._q(mode)), key="regions.name", home="orders", hop="customers")


class TestEveryPosition:
    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_filter_target(self, engine, mode):
        resp = await engine.execute(q(mode, "regions", dimensions=["regions.bad_pop"],
                                      metrics=[m("sum(regions.pop)")],
                                      filter=["sum(regions.pop, partition_by=regions.bad_pop) > 150"]))
        assert_cells(cells(resp, "regions.bad_pop"), {230.0: 200.0})

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_order_target(self, engine, mode):
        resp = await engine.execute(q(
            mode, "regions", dimensions=["regions.bad_pop"], metrics=[m("sum(regions.pop)")],
            order=[{"field": "sum(regions.pop, partition_by=regions.bad_pop)", "direction": "desc"}]))
        assert [num(r["regions.bad_pop"]) for r in resp.data] == [230.0, 150.0]

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_computed_dimension_aggregate(self, engine, mode):
        band = "CASE WHEN sum(regions.pop, partition_by=regions.bad_pop) > 150 THEN 1 ELSE 0 END"
        resp = await engine.execute(q(mode, "regions", dimensions=[{"field": band, "alias": "band"}],
                                      metrics=[m("sum(regions.pop)")]))
        assert_cells(cells(resp, "band"), {0.0: 100.0, 1.0: 200.0})

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_reaggregation_inner(self, engine, mode):
        resp = await engine.execute(regions_bad_pop(mode, "avg(sum(regions.pop, partition_by=regions.bad_pop))"))
        assert_cells(cells(resp, "regions.bad_pop"), POP_BY_BAD_POP)

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_windowed_aggregate(self, engine, mode):
        resp = await engine.execute(q(
            mode, "orders", dimensions=["orders.bad_amt"], time_dimensions=ORDERED_MONTH,
            metrics=[m("sum(orders.amount, window='90d', partition_by=orders.bad_amt)")]))
        got = cells(resp, "orders.bad_amt", "orders.ordered_at", months=("orders.ordered_at",))
        assert_cells(got, WINDOWED_AMOUNT_BY_BAD_AMT_MONTH)
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_attached_parameter_keyed_by_a_host_column(self, engine, mode):
        resp = await engine.execute(q(mode, "orders", dimensions=["orders.status"], metrics=[m(OWN_FAN_STATUS)]))
        assert_cells(cells(resp, "orders.status"), local_wavg_status_assoc_by_status())

    @pytest.mark.parametrize("mode", REFUSING)
    @pytest.mark.parametrize("position", ["filter", "order", "computed", "reagg", "windowed", "parameter"])
    async def test_every_position_refuses_outside_association(self, engine, mode, position):
        key = "sum(regions.pop, partition_by=regions.bad_pop)"
        query = {
            "filter": q(mode, "regions", dimensions=["regions.bad_pop"], metrics=[m("sum(regions.pop)")],
                        filter=[f"{key} > 150"]),
            "order": q(mode, "regions", dimensions=["regions.bad_pop"], metrics=[m("sum(regions.pop)")],
                       order=[{"field": key, "direction": "desc"}]),
            "computed": q(mode, "regions", metrics=[m("sum(regions.pop)")],
                          dimensions=[{"field": f"CASE WHEN {key} > 150 THEN 1 ELSE 0 END", "alias": "band"}]),
            "reagg": regions_bad_pop(mode, f"avg({key})"),
            "windowed": q(mode, "orders", dimensions=["orders.bad_amt"], time_dimensions=ORDERED_MONTH,
                          metrics=[m("sum(orders.amount, window='90d', partition_by=orders.bad_amt)")]),
            "parameter": q(mode, "regions", dimensions=["regions.bad_pop"],
                           metrics=[m(f"weighted_avg(regions.pop, weight={key})")]),
        }[position]
        msg = await refused(engine, query)
        assert_refusal(msg, key="bad_amt" if position == "windowed" else "bad_pop",
                       home="orders" if position == "windowed" else "regions", hop=HOP)


class TestMixedAndMultiKeyGrains:
    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_attributable_beside_unattributable(self, engine, mode):
        resp = await engine.execute(customers_by_status(
            mode, "sum(customers.spend, partition_by=[customers.tier, orders.status])",
            dimensions=["customers.tier", "orders.status"]))
        assert_cells(cells(resp, "customers.tier", "orders.status"), SPEND_BY_TIER_STATUS)
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", REFUSING)
    async def test_mixed_refusal_names_the_unattributable_key(self, engine, mode):
        msg = await refused(engine, customers_by_status(
            mode, "sum(customers.spend, partition_by=[customers.tier, orders.status])",
            dimensions=["customers.tier", "orders.status"]))
        assert_refusal(msg, key="orders.status", home="customers", hop="orders")

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_two_keys_on_one_branch_read_one_related_row(self, same_row_engine, mode):
        resp = await same_row_engine.execute(customers_by_status(
            mode, "sum(customers.spend, partition_by=[orders.status, orders.ordered_at])",
            time_dimensions=ORDERED_MONTH))
        got = cells(resp, "orders.status", "orders.ordered_at", months=("orders.ordered_at",))
        assert_cells(got, SAME_ROW_SPEND_BY_STATUS_MONTH)
        assert ("new", "2023-01") not in got

    # A partition-key list is a set: both spellings bind one aggregate, refused naming its canonically first key.
    @pytest.mark.parametrize("mode", REFUSING)
    @pytest.mark.parametrize("keys", ["orders.status, orders.ordered_at", "orders.ordered_at, orders.status"],
                             ids=["status-first", "time-first"])
    async def test_one_branch_refusal_names_one_key_deterministically(self, engine, mode, keys):
        msg = await refused(engine, customers_by_status(
            mode, f"sum(customers.spend, partition_by=[{keys}])", time_dimensions=ORDERED_MONTH))
        assert_refusal(msg, key="orders.status", home="customers")
        assert "orders.ordered_at" not in msg, msg

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_keys_on_separate_branches(self, engine, mode):
        resp = await engine.execute(customers_by_status(
            mode, f"sum(customers.spend, partition_by=[orders.status, {EVENT_VALUE}])",
            dimensions=["orders.status", EVENT_VALUE]))
        assert_cells(cells(resp, "orders.status", EVENT_VALUE), SPEND_BY_STATUS_EVENT)

    @pytest.mark.parametrize("mode", REFUSING)
    @pytest.mark.parametrize("keys", [f"orders.status, {EVENT_VALUE}", f"{EVENT_VALUE}, orders.status"],
                             ids=["status-first", "event-first"])
    async def test_separate_branch_refusal_names_one_key_deterministically(self, engine, mode, keys):
        msg = await refused(engine, customers_by_status(
            mode, f"sum(customers.spend, partition_by=[{keys}])", dimensions=["orders.status", EVENT_VALUE]))
        assert_refusal(msg, key="orders.status", home="customers")
        assert HOP not in msg, msg


class TestRepresentationParity:
    async def test_keyless_home_associates(self, keyless_engine):
        resp = await keyless_engine.execute(q(None, "orders", dimensions=["orders.status"], metrics=[m(RANKED)]))
        assert_cells(cells(resp, "orders.status"), ranked_assoc_by_status())

    async def test_keyless_home_by_tier_associates(self, keyless_engine):
        resp = await keyless_engine.execute(q(None, "orders", dimensions=["customers.tier"], metrics=[m(RANKED)]))
        assert_cells(cells(resp, "customers.tier"), {**ranked_by_tier(), None: None})

    async def test_keyless_home_by_tier_refuses_in_error_mode(self, keyless_engine):
        msg = await refused(keyless_engine, q("error", "orders", dimensions=["customers.tier"], metrics=[m(RANKED)]))
        assert_refusal(msg, key="regions.name", home="orders", hop="customers")

    @staticmethod
    def _skills(mode, *metrics: QueryMetric) -> SlayerQuery:
        return q(mode, "contractors", dimensions=["contractors.skills.value"], metrics=list(metrics))

    async def test_list_child_associates(self, lists):
        sibling = m("max(contractors.rate)", "mx")
        resp = await lists.execute(self._skills(
            None, m("sum(contractors.rate, partition_by=contractors.skills.value)"), sibling))
        assert_cells(cells(resp, "contractors.skills.value"), RATE_BY_SKILL)
        plain = await lists.execute(self._skills(None, sibling))
        assert cells(resp, "contractors.skills.value", value="mx") == cells(
            plain, "contractors.skills.value", value="mx")

    async def test_list_child_refuses_in_error_mode(self, lists):
        msg = await refused(lists, self._skills(
            "error", m("sum(contractors.rate, partition_by=contractors.skills.value)")))
        assert_refusal(msg, key="skills", home="contractors")

    async def test_saved_measure_associates(self, engine):
        sibling = m("max(regions.pop)", "mx")
        resp = await engine.execute(q(None, "regions", dimensions=["regions.bad_pop"],
                                      metrics=[m("regions.pop_by_bad_pop"), sibling]))
        assert_cells(cells(resp, "regions.bad_pop"), POP_BY_BAD_POP)
        plain = await engine.execute(q(None, "regions", dimensions=["regions.bad_pop"], metrics=[sibling]))
        assert cells(resp, "regions.bad_pop", value="mx") == cells(plain, "regions.bad_pop", value="mx")

    async def test_saved_measure_refuses_in_error_mode(self, engine):
        msg = await refused(engine, q("error", "regions", dimensions=["regions.bad_pop"],
                                      metrics=[m("regions.pop_by_bad_pop")]))
        assert_refusal(msg, key="bad_pop", home="regions", hop=HOP)

    @staticmethod
    def _stages(mode) -> list[SlayerQuery]:
        inner = q(mode, "customers", name="inner", dimensions=["orders.status"],
                  metrics=[m("sum(customers.spend, partition_by=orders.status)", "v"),
                           m("count(*)", "n")])
        return [inner, q(mode, "inner", metrics=[m("sum(inner.v)", "t"), m("sum(inner.n)", "rows")])]

    async def test_inner_stage_associates(self, engine):
        resp = await engine.execute(self._stages(None))
        assert len(resp.data) == 1
        assert num(resp.data[0]["t"]) == pytest.approx(sum(SPEND_BY_STATUS.values()))
        plain = await engine.execute([
            q(None, "customers", name="inner", dimensions=["orders.status"], metrics=[m("count(*)", "n")]),
            q(None, "inner", metrics=[m("sum(inner.n)", "rows")])])
        assert num(resp.data[0]["rows"]) == num(plain.data[0]["rows"])

    async def test_inner_stage_refuses_in_error_mode(self, engine):
        stages = self._stages("error")
        with pytest.raises(PartitionKeyError) as ei:
            await engine.execute(stages)
        assert_refusal(str(ei.value), key="orders.status", home="customers")


class TestUnanalysableKey:
    @pytest.mark.parametrize("mode", [None, "broadcast", "associate", "error"])
    @pytest.mark.parametrize("shape, dims, metric, exc", [
        ("direct", ["regions.name"], "sum(regions.pop, partition_by=regions.unparseable)", PartitionKeyError),
        ("chained", ["regions.name"], "sum(regions.pop, partition_by=regions.unparse2)", PartitionKeyError),
        ("transform-inner", ["regions.name"],
         "rank(sum(regions.pop, partition_by=[regions.name, regions.unparseable]), "
         "partition_by=regions.unparseable, direction='desc')", PartitionKeyError),
        ("transform-dimension", ["regions.unparseable"],
         "rank(sum(regions.pop), partition_by=regions.unparseable, direction='desc')", ModeASqlParseError),
        ("parameter", ["regions.name"],
         "weighted_avg(regions.pop, weight=sum(regions.pop, partition_by=regions.unparseable))", PartitionKeyError),
    ], ids=lambda v: v if isinstance(v, str) and " " not in v and "(" not in v else None)
    async def test_fails_closed_in_every_mode(self, unparse_engine, mode, shape, dims, metric, exc):
        query = q(mode, "regions", dimensions=dims, metrics=[m(metric)])
        with pytest.raises(exc) as ei:
            await unparse_engine.execute(query)
        msg = str(ei.value)
        assert "unparse" in msg, msg
        assert HOP not in msg, msg
        assert_ref_free(msg)


class TestUnparseableDimension:
    async def test_plain_dimension_raises_the_typed_parse_error(self, unparse_engine):
        query = q(None, "regions", dimensions=["regions.unparseable"], metrics=[m("sum(regions.pop)")])
        with pytest.raises(ModeASqlParseError) as ei:
            await unparse_engine.execute(query)
        assert "regions.unparseable" in str(ei.value), str(ei.value)
        assert_ref_free(str(ei.value))

    async def test_chained_dimension_names_the_unparseable_field(self, unparse_engine):
        query = q(None, "regions", dimensions=["regions.unparse2"], metrics=[m("sum(regions.pop)")])
        with pytest.raises(ModeASqlParseError) as ei:
            await unparse_engine.execute(query)
        assert "regions.unparseable" in str(ei.value), str(ei.value)


class TestTransformPartitionKeys:
    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_rank_partitions_the_associated_cells(self, engine, mode):
        resp = await engine.execute(regions_bad_pop(
            mode, "rank(sum(regions.pop), partition_by=regions.bad_pop, direction='desc')"))
        assert_cells(cells(resp, "regions.bad_pop"), {150.0: 1.0, 230.0: 1.0})

    @pytest.mark.parametrize("mode", [None, "broadcast", "associate", "error"])
    async def test_key_outside_the_operand_grain_fails(self, engine, mode):
        msg = await refused(engine, q(
            mode, "regions", dimensions=["regions.name"],
            metrics=[m("rank(sum(regions.pop), partition_by=regions.bad_pop, direction='desc')")]))
        assert "bad_pop" in msg, msg
        assert HOP not in msg, msg
        assert "to_many_handling" not in msg, msg


class TestHostSafeKey:
    @staticmethod
    def _q(mode):
        return q(mode, "orders", dimensions=["orders.status"],
                 metrics=[m("sum(customers.spend, partition_by=orders.status)")])

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_associates(self, engine, mode):
        resp = await engine.execute(self._q(mode))
        assert_cells(cells(resp, "orders.status"), {"new": 290.0, "ok": 420.0})
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", REFUSING)
    async def test_refuses_with_the_shared_wording(self, engine, mode):
        assert_refusal(await refused(engine, self._q(mode)), key="status", home="customers")


# --------------------------------------------------------------------------- #
# queries/cross-model-aggregates › local aggregates with an unattributable explicit key
# --------------------------------------------------------------------------- #
class TestLocalAggregates:
    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_plain(self, engine, mode):
        resp = await engine.execute(customers_by_status(mode, "sum(customers.spend, partition_by=orders.status)"))
        assert_cells(cells(resp, "orders.status"), SPEND_BY_STATUS)
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_first_last(self, engine, mode):
        resp = await engine.execute(customers_by_status(
            mode, "last(customers.spend, customers.signup_at, partition_by=orders.status)"))
        assert_cells(cells(resp, "orders.status"), LOCAL_LAST_BY_STATUS)
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_windowed(self, engine, mode):
        resp = await engine.execute(customers_by_status(
            mode, "sum(customers.spend, window='1y', partition_by=orders.status)", time_dimensions=SIGNUP_MONTH))
        got = cells(resp, "orders.status", "customers.signup_at", months=("customers.signup_at",))
        assert_cells(got, WINDOW_BY_STATUS_SIGNUP)
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_windowed_last(self, engine, mode):
        resp = await engine.execute(customers_by_status(
            mode, "last(customers.spend, customers.signup_at, window='1y', partition_by=orders.status)",
            time_dimensions=SIGNUP_MONTH))
        got = cells(resp, "orders.status", "customers.signup_at", months=("customers.signup_at",))
        assert_cells(got, WINDOWED_LAST_BY_STATUS_SIGNUP)
        assert_no_mode_warnings(resp)

    async def test_fanning_key_and_fanning_window_axis_read_one_row(self, same_row_engine):
        resp = await same_row_engine.execute(customers_by_status(
            None, "sum(customers.spend, window='1y', partition_by=orders.status)", time_dimensions=ORDERED_MONTH))
        got = cells(resp, "orders.status", "orders.ordered_at", months=("orders.ordered_at",))
        assert_subset(got, {("new", "2024-03"): 1290.0, ("ok", "2024-03"): 310.0, ("ok", "2023-01"): 1000.0})

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_time_key_associates_per_bucket(self, engine, mode):
        resp = await engine.execute(q(mode, "customers", time_dimensions=ORDERED_MONTH,
                                      metrics=[m("sum(customers.spend, partition_by=orders.ordered_at)")]))
        assert_cells(cells(resp, "orders.ordered_at", months=("orders.ordered_at",)), SPEND_BY_ORDER_MONTH)
        assert_no_mode_warnings(resp)

    async def test_home_row_spanning_buckets_counts_in_each(self, same_row_engine):
        local = await same_row_engine.execute(q(None, "customers", time_dimensions=ORDERED_MONTH,
                                                metrics=[m("sum(customers.spend, partition_by=orders.ordered_at)")]))
        assert_cells(cells(local, "orders.ordered_at", months=("orders.ordered_at",)),
                     SAME_ROW_SPEND_BY_ORDER_MONTH)
        cross = await same_row_engine.execute(q(None, "orders", time_dimensions=ORDERED_MONTH, metrics=[m(
            "sum(cumsum(sum(customers.spend, partition_by=[customers.tier, orders.ordered_at])))")]))
        assert_cells(cells(cross, "orders.ordered_at", months=("orders.ordered_at",)),
                     SAME_ROW_CUMSUM_BY_ORDER_MONTH)

    @pytest.mark.parametrize("mode", REFUSING)
    @pytest.mark.parametrize("shape, kw, key", [
        ("sum(customers.spend, partition_by=orders.status)", {}, "orders.status"),
        ("last(customers.spend, customers.signup_at, partition_by=orders.status)", {}, "orders.status"),
        ("sum(customers.spend, window='1y', partition_by=orders.status)", {"time_dimensions": SIGNUP_MONTH},
         "orders.status"),
        ("last(customers.spend, customers.signup_at, window='1y', partition_by=orders.status)",
         {"time_dimensions": SIGNUP_MONTH}, "orders.status"),
        ("sum(customers.spend, partition_by=orders.ordered_at)",
         {"dimensions": [], "time_dimensions": ORDERED_MONTH}, "ordered_at"),
    ], ids=["plain", "last", "windowed", "windowed-last", "time"])
    async def test_refuses_outside_association(self, engine, mode, shape, kw, key):
        assert_refusal(await refused(engine, customers_by_status(mode, shape, **kw)), key=key, home="customers")


# --------------------------------------------------------------------------- #
# queries/transforms › composite time_shift leaf with an unattributable key
# --------------------------------------------------------------------------- #
class TestShiftedLeaf:
    SHIFT = "time_shift(sum(customers.spend, partition_by=[orders.status, customers.signup_at]), -1)"

    @pytest.mark.parametrize("mode", ASSOCIATING)
    async def test_reads_the_associated_cell_at_the_shifted_bucket(self, engine, mode):
        resp = await engine.execute(customers_by_status(mode, self.SHIFT, time_dimensions=SIGNUP_MONTH))
        got = cells(resp, "orders.status", "customers.signup_at", months=("customers.signup_at",))
        assert_cells(got, SHIFTED_BY_STATUS_SIGNUP)
        assert_no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", REFUSING)
    async def test_refuses_outside_association(self, engine, mode):
        msg = await refused(engine, customers_by_status(mode, self.SHIFT, time_dimensions=SIGNUP_MONTH))
        assert_refusal(msg, key="orders.status", home="customers")


# --------------------------------------------------------------------------- #
# queries/partitioned-aggregates › attached parameter with its own unattributable key
# --------------------------------------------------------------------------- #
class TestAttachedParameterOwnKey:
    @pytest.mark.parametrize("mode", ASSOCIATING)
    @pytest.mark.parametrize("dim", ["orders.status", BAD_POP])
    async def test_undetermined_grain_keeps_the_determination_error(self, engine, mode, dim):
        query = q(mode, "orders", dimensions=[dim], metrics=[m(OWN_FAN_BADPOP)])
        with pytest.raises(SlayerError) as ei:
            await engine.execute(query)
        assert not isinstance(ei.value, PartitionKeyError), str(ei.value)
        assert_grain_residue(ei.value, param="weight")

    @pytest.mark.parametrize("mode", REFUSING)
    @pytest.mark.parametrize("dim", ["orders.status", BAD_POP])
    async def test_refuses_outside_association(self, engine, mode, dim):
        msg = await refused(engine, q(mode, "orders", dimensions=[dim], metrics=[m(OWN_FAN_BADPOP)]))
        assert_refusal(msg, key="bad_pop", home="regions", hop=HOP)


# --------------------------------------------------------------------------- #
# The re-aggregation outer-key refusal keeps its own operand-grain remedy
# --------------------------------------------------------------------------- #
class TestReaggregationOuterKeyMessage:
    async def test_outer_key_message(self, engine):
        msg = await refused(engine, q(
            "error", "customers", dimensions=["customers.id"],
            metrics=[m("sum(sum(customers.spend, partition_by=customers.tier), partition_by=[customers.id])")]))
        assert "operand grain" in msg, msg
        assert "inner partition_by=" in msg, msg
        assert "to_many_handling" in msg, msg
        assert "cross-dataset" not in msg.lower(), msg
        assert_ref_free(msg)
