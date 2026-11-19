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
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.sql.render.cte_assembly import CteEntry, reachable_cte_entries
from tests._dev1966_fixtures import (
    AMOUNT_BY_STATUS,
    AMOUNT_BY_TIER,
    CUST_TOP,
    bcast_qb_dataset,
    clients_dataset,
    cust_rev_dataset,
    customers_dataset,
    dev1966_engine,
    m,
    orders_dataset,
    query,
    rows_by,
    sorted_rows,
)


def _qb(name: str, *stages: Any) -> SlayerDataset:
    return SlayerDataset(name=name, data_source="test", source_queries=list(stages))


def _joined(dataset: SlayerDataset, *targets: "tuple[str, List[List[str]]]") -> SlayerDataset:
    return dataset.model_copy(update={"relationships": [
        *dataset.relationships, *(DatasetRelationship(to=t, from_columns=[a for a, _ in p], to_columns=[b for _, b in p]) for t, p in targets)]})


STATUS_REV = _qb("status_rev", query(
    population="orders", dimensions=["orders.status"], metrics=[m("sum(amount)", "rev")]))
READS_STATUS_REV = _qb("reads_status_rev", query(
    population="orders", dimensions=["orders.status", "status_rev.rev"],
    metrics=[m("sum(amount)", "a")]))
BROKEN = _qb("broken_qb", query(
    population="orders", dimensions=["nope"], metrics=[m("sum(amount)", "a")]))
BAD_SELF = _qb("bad_self", query(
    population="orders", dimensions=["orders.status"], metrics=[m("sum(orders.nope)", "a")]))


def _orders_joining(*names: str) -> SlayerDataset:
    return _joined(orders_dataset(), *((n, [["status", "status"]]) for n in names))


@pytest.fixture(params=["sqlite", "duckdb"])
def dialect(request) -> str:
    return request.param


class TestReachIsNotReading:
    async def test_a_model_over_a_base_that_joins_it(self, dialect) -> None:
        models = [_orders_joining("status_rev"), customers_dataset(), clients_dataset(),
                  cust_rev_dataset(), STATUS_REV]
        async with dev1966_engine(dialect, datasets=models) as e:
            resp = await e.execute(query(population="orders",
                                         dimensions=["orders.status", "status_rev.rev"],
                                         metrics=[m("sum(orders.amount)", "a")]))
        assert resp.columns == ["orders.status", "status_rev.rev", "a"]
        assert sorted_rows(resp.data) == sorted_rows([
            {"orders.status": s, "status_rev.rev": v, "a": v}
            for s, v in AMOUNT_BY_STATUS.items()])

    async def test_an_unread_broken_model_is_inert(self, dialect) -> None:
        models = [orders_dataset(), _joined(customers_dataset(), ("broken_qb", [["id", "a"]])),
                  clients_dataset(), cust_rev_dataset(), BROKEN]
        async with dev1966_engine(dialect, datasets=models) as e:
            resp = await e.execute(query(population="orders", metrics=[m("sum(orders.amount)", "t")]))
            dry = await e.execute(query(population="orders", metrics=[m("sum(orders.amount)", "t")]),
                                  dry_run=True)
        assert resp.data == [{"t": 145.0}]
        assert resp.warnings == []
        assert dry.sql is not None
        assert "broken_qb" not in dry.sql

    async def test_an_unread_warning_model_stays_silent(self, dialect) -> None:
        models = [orders_dataset(), _joined(customers_dataset(), ("bcast_qb", [["tier", "status"]])),
                  clients_dataset(), cust_rev_dataset(), bcast_qb_dataset()]
        async with dev1966_engine(dialect, datasets=models) as e:
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                resp = await e.execute(query(population="customers",
                                             metrics=[m("sum(spend)", "t")]))
        assert resp.data == [{"customers.t": 430.0}]
        assert resp.warnings == []


class TestStoredJoinReads:
    async def test_a_non_root_stage(self, dialect) -> None:
        s = query(name="s", population="clients", dimensions=["clients.tier"],
                  metrics=[m("sum(cust_rev.rev)", "r")])
        root = query(population="s", dimensions=["s.tier"], metrics=[m("sum(r)", "t")])
        async with dev1966_engine(dialect) as e:
            resp = await e.execute([s, root])
        assert resp.columns == ["s.tier", "t"]
        assert rows_by(resp.data, key="s.tier", value="t") == AMOUNT_BY_TIER

    async def test_a_derived_column_definition(self, dialect) -> None:
        clients = clients_dataset()
        clients = clients.model_copy(update={"fields": [
            *clients.fields, DatasetField(name="top2", sql="cust_rev.top * 2", datatype=DataType.FLOAT)]})
        models = [orders_dataset(), customers_dataset(), clients, cust_rev_dataset()]
        async with dev1966_engine(dialect, datasets=models) as e:
            resp = await e.execute(query(population="clients", dimensions=["clients.id", "clients.top2"]))
        assert rows_by(resp.data, key="clients.id", value="clients.top2") == {
            c: 2 * v for c, v in CUST_TOP.items()}

    async def test_one_model_reads_another_consumer_reads_either(self, dialect) -> None:
        models = [_orders_joining("status_rev", "reads_status_rev"), customers_dataset(),
                  clients_dataset(), cust_rev_dataset(), STATUS_REV, READS_STATUS_REV]
        async with dev1966_engine(dialect, datasets=models) as e:
            only_a = await e.execute(query(population="status_rev", dimensions=["status_rev.status"],
                                           metrics=[m("sum(status_rev.rev)", "t")]))
            b = await e.execute(query(population="reads_status_rev", dimensions=["reads_status_rev.status"],
                                      metrics=[m("max(reads_status_rev.status_rev__rev)", "rr")]))
        assert rows_by(only_a.data, key="status_rev.status", value="t") == AMOUNT_BY_STATUS
        assert rows_by(b.data, key="reads_status_rev.status",
                       value="rr") == AMOUNT_BY_STATUS


class TestFailedAttemptKeepsItsError:
    async def test_invalid_expression_survives_an_incident_in_flight_model(self) -> None:
        models = [_orders_joining("bad_self"), customers_dataset(), clients_dataset(),
                  cust_rev_dataset(), BAD_SELF]
        consumer = query(population="bad_self", metrics=[m("sum(bad_self.a)", "t")])
        async with dev1966_engine("sqlite", datasets=models) as e:
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


def _floored() -> SlayerDataset:
    """``sql``-backed over ``orders`` with a ``{floor}`` threshold (default 0)."""
    return SlayerDataset(
        name="floored", data_source="test", query_variables={"floor": 0},
        sql="SELECT id, customer_id, amount FROM orders WHERE amount >= {floor}",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                 DatasetField(name="customer_id", datatype=DataType.INTEGER),
                 DatasetField(name="amount", datatype=DataType.FLOAT)])


FLOORED_REV = _qb("floored_rev", query(
    population="floored", dimensions=["floored.customer_id"], metrics=[m("sum(amount)", "rev")]))


def _wrap(name: str, floor: int) -> SlayerDataset:
    return SlayerDataset(name=name, data_source="test", query_variables={"floor": floor},
                       source_queries=[query(population="floored_rev", dimensions=["floored_rev.customer_id"],
                                             metrics=[m("sum(floored_rev.rev)", "r")])])


class TestModeAVariables:
    async def test_a_spliced_stage_source_sql_takes_the_consumer_variables(self, dialect) -> None:
        models = [orders_dataset(), customers_dataset(), _floored(), FLOORED_REV]
        consumer = query(population="floored_rev", metrics=[m("sum(floored_rev.rev)", "t")])
        async with dev1966_engine(dialect, datasets=models) as e:
            default = await e.execute(consumer)
            floored = await e.execute(consumer, variables={"floor": 20})
        assert default.data == [{"t": 145.0}]
        assert floored.data == [{"t": 115.0}]

    async def test_a_user_stage_source_sql_takes_its_variables(self, dialect) -> None:
        s = query(name="s", population="floored", metrics=[m("sum(amount)", "a")],
                  variables={"floor": 30})
        async with dev1966_engine(dialect, datasets=[orders_dataset(), _floored()]) as e:
            resp = await e.execute([s, query(population="s", metrics=[m("sum(s.a)", "t")])])
        assert resp.data == [{"t": 70.0}]

    async def test_conflicting_mode_a_contexts_fail_closed(self) -> None:
        models = [orders_dataset(), customers_dataset(), _floored(), FLOORED_REV,
                  _wrap("wrap_a", 20), _wrap("wrap_b", 30)]
        s1 = query(name="s1", population="wrap_a", dimensions=["wrap_a.customer_id"], metrics=[m("sum(r)", "a")])
        s2 = query(name="s2", population="wrap_b", dimensions=["wrap_b.customer_id"], metrics=[m("sum(r)", "b")])
        root = query(population="s1", extensions=[{"dataset": "s1", "relationships": [
            {"to": "s2", "from_columns": ["customer_id"], "to_columns": ["customer_id"]}]}],
            dimensions=["s1.customer_id", "s1.a", "s2.b"])
        async with dev1966_engine("sqlite", datasets=models) as e:
            with pytest.raises(ValueError, match="floored_rev"):
                await e.execute([s1, s2, root])


CYC_A = _qb("cyc_a", query(population="cyc_b", metrics=[m("sum(amount)", "amount")]))
CYC_B = _qb("cyc_b", query(population="cyc_a", metrics=[m("sum(amount)", "amount")]))


class TestCyclesThroughStoredJoins:
    async def test_a_read_through_a_stored_join_raises_the_cycle(self) -> None:
        orders = _joined(orders_dataset(), ("cyc_a", [["id", "amount"]]))
        consumer = query(population="orders", dimensions=["orders.status"],
                         metrics=[m("sum(cyc_a.amount)", "t")])
        async with dev1966_engine("sqlite", datasets=[orders, customers_dataset(), CYC_A, CYC_B]) as e:
            with pytest.raises(QueryBackedCycleError) as exc:
                await e.execute(consumer)
        assert "cyc_a -> cyc_b -> cyc_a" in str(exc.value)

    async def test_an_unread_cyclic_model_is_inert(self) -> None:
        orders = _joined(orders_dataset(), ("cyc_a", [["id", "amount"]]))
        async with dev1966_engine("sqlite", datasets=[orders, customers_dataset(), CYC_A, CYC_B]) as e:
            resp = await e.execute(query(population="orders", metrics=[m("sum(orders.amount)", "t")]))
        assert resp.data == [{"t": 145.0}]
