"""A query-backed model is spliced only when a stage reads it; reach is never a dependency.

Spec: openspec …/specs/queries/query-backed-inline — "Reaching is not reading", "Reads through
stored joins at any depth".
"""

from __future__ import annotations

import warnings
from typing import Any, List

import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.errors import QueryBackedCycleError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.sql.render.cte_assembly import CteEntry, reachable_cte_entries
from tests._dev1966_fixtures import (
    AMOUNT_BY_STATUS,
    AMOUNT_BY_TIER,
    CUST_TOP,
    bcast_qb_model,
    clients_model,
    cust_rev_model,
    customers_model,
    dev1966_engine,
    m,
    orders_model,
    query,
    rows_by,
    sorted_rows,
)


def _qb(name: str, *stages: Any) -> SlayerModel:
    return SlayerModel(name=name, data_source="test", source_queries=list(stages))


def _joined(model: SlayerModel, *targets: "tuple[str, List[List[str]]]") -> SlayerModel:
    return model.model_copy(update={"joins": [
        *model.joins, *(ModelJoin(target_model=t, join_pairs=p) for t, p in targets)]})


STATUS_REV = _qb("status_rev", query(
    source_model="orders", dimensions=["status"], measures=[m("amount:sum", "rev")]))
READS_STATUS_REV = _qb("reads_status_rev", query(
    source_model="orders", dimensions=["status", "status_rev.rev"],
    measures=[m("amount:sum", "a")]))
BROKEN = _qb("broken_qb", query(
    source_model="orders", dimensions=["nope"], measures=[m("amount:sum", "a")]))
BAD_SELF = _qb("bad_self", query(
    source_model="orders", dimensions=["status"], measures=[m("nope:sum", "a")]))


def _orders_joining(*names: str) -> SlayerModel:
    return _joined(orders_model(), *((n, [["status", "status"]]) for n in names))


@pytest.fixture(params=["sqlite", "duckdb"])
def dialect(request) -> str:
    return request.param


class TestReachIsNotReading:
    async def test_a_model_over_a_base_that_joins_it(self, dialect) -> None:
        models = [_orders_joining("status_rev"), customers_model(), clients_model(),
                  cust_rev_model(), STATUS_REV]
        async with dev1966_engine(dialect, models=models) as e:
            resp = await e.execute(query(source_model="orders",
                                         dimensions=["status", "status_rev.rev"],
                                         measures=[m("amount:sum", "a")]))
        assert resp.columns == ["orders.status", "orders.status_rev.rev", "orders.a"]
        assert sorted_rows(resp.data) == sorted_rows([
            {"orders.status": s, "orders.status_rev.rev": v, "orders.a": v}
            for s, v in AMOUNT_BY_STATUS.items()])

    async def test_an_unread_broken_model_is_inert(self, dialect) -> None:
        models = [orders_model(), _joined(customers_model(), ("broken_qb", [["id", "a"]])),
                  clients_model(), cust_rev_model(), BROKEN]
        async with dev1966_engine(dialect, models=models) as e:
            resp = await e.execute(query(source_model="orders", measures=[m("amount:sum", "t")]))
            dry = await e.execute(query(source_model="orders", measures=[m("amount:sum", "t")]),
                                  dry_run=True)
        assert resp.data == [{"orders.t": 145.0}]
        assert resp.warnings == []
        assert dry.sql is not None
        assert "broken_qb" not in dry.sql

    async def test_an_unread_warning_model_stays_silent(self, dialect) -> None:
        models = [orders_model(), _joined(customers_model(), ("bcast_qb", [["tier", "status"]])),
                  clients_model(), cust_rev_model(), bcast_qb_model()]
        async with dev1966_engine(dialect, models=models) as e:
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                resp = await e.execute(query(source_model="customers",
                                             measures=[m("spend:sum", "t")]))
        assert resp.data == [{"customers.t": 430.0}]
        assert resp.warnings == []


class TestStoredJoinReads:
    async def test_a_non_root_stage(self, dialect) -> None:
        s = query(name="s", source_model="clients", dimensions=["tier"],
                  measures=[m("cust_rev.rev:sum", "r")])
        root = query(source_model="s", dimensions=["tier"], measures=[m("r:sum", "t")])
        async with dev1966_engine(dialect) as e:
            resp = await e.execute([s, root])
        assert resp.columns == ["s.tier", "s.t"]
        assert rows_by(resp.data, key="s.tier", value="s.t") == AMOUNT_BY_TIER

    async def test_a_derived_column_definition(self, dialect) -> None:
        clients = clients_model()
        clients = clients.model_copy(update={"columns": [
            *clients.columns, Column(name="top2", sql="cust_rev.top * 2", type=DataType.DOUBLE)]})
        models = [orders_model(), customers_model(), clients, cust_rev_model()]
        async with dev1966_engine(dialect, models=models) as e:
            resp = await e.execute(query(source_model="clients", dimensions=["id", "top2"]))
        assert rows_by(resp.data, key="clients.id", value="clients.top2") == {
            c: 2 * v for c, v in CUST_TOP.items()}

    async def test_one_model_reads_another_consumer_reads_either(self, dialect) -> None:
        models = [_orders_joining("status_rev", "reads_status_rev"), customers_model(),
                  clients_model(), cust_rev_model(), STATUS_REV, READS_STATUS_REV]
        async with dev1966_engine(dialect, models=models) as e:
            only_a = await e.execute(query(source_model="status_rev", dimensions=["status"],
                                           measures=[m("rev:sum", "t")]))
            b = await e.execute(query(source_model="reads_status_rev", dimensions=["status"],
                                      measures=[m("status_rev__rev:max", "rr")]))
        assert rows_by(only_a.data, key="status_rev.status", value="status_rev.t") == AMOUNT_BY_STATUS
        assert rows_by(b.data, key="reads_status_rev.status",
                       value="reads_status_rev.rr") == AMOUNT_BY_STATUS


class TestFailedAttemptKeepsItsError:
    async def test_invalid_expression_survives_an_incident_in_flight_model(self) -> None:
        models = [_orders_joining("bad_self"), customers_model(), clients_model(),
                  cust_rev_model(), BAD_SELF]
        consumer = query(source_model="bad_self", measures=[m("a:sum", "t")])
        async with dev1966_engine("sqlite", models=models) as e:
            with pytest.raises(ValueError) as exc:
                await e.execute(consumer)
        assert not isinstance(exc.value, QueryBackedCycleError)
        assert "nope" in str(exc.value)


class TestPruning:
    def test_a_producer_reused_by_a_reachable_stage_survives(self) -> None:
        body = sqlglot.select("1 AS x")
        entries = [
            CteEntry(name="_cm_p", query=body),
            CteEntry(name="__slayer_qb__m__s", query=body, depends_on=["_cm_p"]),
            CteEntry(name="__slayer_stage_u", query=body, depends_on=["_cm_p"]),
        ]
        kept = reachable_cte_entries(entries=entries, seeds={"__slayer_stage_u"})
        assert [e.name for e in kept] == ["_cm_p", "__slayer_stage_u"]
