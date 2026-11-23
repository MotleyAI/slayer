"""Stage names are query-local: they override same-named datasets only inside their own list.

Spec: openspec …/specs/queries/multi-stage — "Stage names are query-local".
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests import _dev1948_fixtures as f48
from tests._dev1954_fixtures import make_exec_engine as make_dev1954_engine
from tests._dev1954_fixtures import slayer_query, stale_warnings
from tests._dev1966_fixtures import (
    AMOUNT_BY_TIER,
    CUST_REV,
    CUST_TOP,
    SPEND_BY_TIER,
    customers_dataset,
    dev1966_engine,
    m,
    orders_dataset,
    query,
    rows_by,
    sorted_rows,
)

INTERNAL = "__slayer_"


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev1966_engine(request.param) as e:
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine48(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in f48.make_dev1948_engine(request):
        yield e


def _b_over(dim: str, source, **kw) -> SlayerQuery:
    return query(name="b", population=source, dimensions=[dim], metrics=[m("sum(orders.amount)", "amt")],
                 **kw)


def _root_over_b(flat_dim: str) -> SlayerQuery:
    return query(population="b", dimensions=[f"b.{flat_dim}"], metrics=[m("sum(b.amt)", "total")])


class TestStoredDefinitionsNeverSeeStages:
    async def test_stored_join_resolves_to_the_model_beside_a_same_named_stage(self, engine: SlayerQueryEngine) -> None:
        customers_stage = query(name="customers", population="orders", dimensions=["orders.status"],
                                metrics=[m("sum(orders.amount)", "a")])
        resp = await engine.execute(
            [customers_stage, _b_over("orders.customers.tier", "orders"), _root_over_b("customers__tier")])
        assert resp.columns == ["b.customers__tier", "total"]
        assert rows_by(resp.data, key="b.customers__tier", value="total") == AMOUNT_BY_TIER

    async def test_query_written_join_to_a_stage_wins_over_a_same_named_stored_edge(
        self, engine,
    ) -> None:
        customers_stage = query(name="customers", population="orders", dimensions=["orders.customer_id"],
                                metrics=[m("max(orders.status)", "tier")])
        b = _b_over("customers.tier", "orders", extensions=[{
            "dataset": "orders",
            "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["customer_id"]}],
        }])
        resp = await engine.execute([customers_stage, b, _root_over_b("customers__tier")])
        assert rows_by(resp.data, key="b.customers__tier", value="total") == {"ok": 105.0, "new": 40.0}

    async def test_a_stage_named_like_a_physical_table_does_not_hide_it(self, engine) -> None:
        acct_stage = query(name="acct", population="orders", dimensions=["orders.status"],
                           metrics=[m("sum(orders.amount)", "a")])
        resp = await engine.execute(
            [acct_stage, _b_over("clients.tier", "orders"), _root_over_b("clients__tier")])
        assert rows_by(resp.data, key="b.clients__tier", value="total") == AMOUNT_BY_TIER


class TestQueryBackedPrivateStageNames:
    async def test_private_stage_named_like_a_model_does_not_capture_its_stored_join(
        self, engine: SlayerQueryEngine,
    ) -> None:
        resp = await engine.execute(query(
            population="qb_shadow", dimensions=["qb_shadow.customers__tier"],
            metrics=[m("sum(qb_shadow.amt)", "total")]))
        assert resp.columns == ["qb_shadow.customers__tier", "total"]
        assert rows_by(resp.data, key="qb_shadow.customers__tier", value="total") \
            == AMOUNT_BY_TIER

    async def test_consumer_name_reaches_the_model_not_a_private_stage(self, engine: SlayerQueryEngine) -> None:
        s = query(name="s", population="qb_shadow", dimensions=["qb_shadow.customers__tier"],
                  metrics=[m("sum(qb_shadow.amt)", "t")])
        root = query(
            population="customers",
            extensions=[{"dataset": "customers", "relationships": [
                {"to": "s", "from_columns": ["tier"], "to_columns": ["customers__tier"]}]}],
            dimensions=["customers.tier", "s.t"], metrics=[m("sum(customers.spend)", "sp")])
        resp = await engine.execute([s, root])
        assert resp.columns == ["customers.tier", "s.t", "sp"]
        assert sorted_rows(resp.data) == sorted_rows([
            {"customers.tier": t, "s.t": AMOUNT_BY_TIER[t], "sp": SPEND_BY_TIER[t]}
            for t in SPEND_BY_TIER
        ])

    async def test_private_stage_is_unreachable_from_the_consumer(self, engine) -> None:
        s = query(name="s", population="cust_rev", dimensions=["cust_rev.customer_id"],
                  metrics=[m("sum(cust_rev.rev)", "r")])
        stages = [s, query(population="x", metrics=[m("sum(x.amt)", "t")])]
        with pytest.raises(ValueError, match=r"'x'") as exc:
            await engine.execute(stages)
        assert INTERNAL not in str(exc.value)


class TestUserSpelling:
    async def test_chain_result_keys_unchanged(self, engine48: SlayerQueryEngine) -> None:
        resp = await engine48.execute([*f48.chain_list()])
        assert resp.columns == ["b.c__tr", "total"]
        assert rows_by(resp.data, key="b.c__tr", value="total") == f48.AMOUNT_BY_TIER

    async def test_run_by_name_result_keys_unchanged(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("cust_rev")
        assert resp.columns == ["x.customer_id", "rev", "top"]
        assert sorted_rows(resp.data) == sorted_rows([
            {"x.customer_id": c, "rev": CUST_REV[c], "top": CUST_TOP[c]} for c in CUST_REV])

    async def test_stage_warning_names_the_user_stage(self, engine) -> None:
        b = query(name="b", population="orders", dimensions=["orders.status"],
                  metrics=[m("sum(orders.amount)", "amt"), m("sum(customers.spend)", "cs")],
                  to_many_handling="broadcast")
        stages = [b, query(population="b", dimensions=["b.status"], metrics=[m("sum(b.amt)", "t")],
                           to_many_handling="broadcast")]
        with pytest.warns(UserWarning):
            resp = await engine.execute(stages)
        assert [w.location for w in resp.warnings if w.kind == "broadcast"] == ["stage 'b'"]

    async def test_stage_binding_error_names_the_user_stage(self, engine48) -> None:
        stages = [f48.stage_x(), query(name="c", population="x", dimensions=["x.nope"],
                                       metrics=[m("sum(x.total_spend)", "a")]),
                  query(population="c", metrics=[m("sum(c.a)", "t")])]
        with pytest.raises(ValueError) as exc:
            await engine48.execute(stages)
        assert "stage 'x'" in str(exc.value)
        assert INTERNAL not in str(exc.value)

    async def test_stale_spelling_warnings_name_user_stages(self) -> None:
        class _Req:
            param = "sqlite"

        stale, canon = "customers__regions__rname", "customers__hr__rname"
        s1 = slayer_query(name="s1", population="orders",
                          dimensions=["customers.regions.rname"], metrics=["sum(orders.amount)"])
        s2 = slayer_query(name="s2", population="s1", dimensions=[f"s1.{stale}"],
                          metrics=["sum(s1.amount_sum)"])
        root = slayer_query(population="s2", dimensions=[f"s2.{stale}"], metrics=["sum(s2.amount_sum_sum)"])
        async for e in make_dev1954_engine(_Req()):
            resp = await e.execute([s1, s2, root])
            assert f"s2.{canon}" in resp.columns
            locations = [w.location for w in stale_warnings(resp)]
            assert len(locations) == 2
            assert any("'s2'" in loc for loc in locations), locations
            assert not any(INTERNAL in loc for loc in locations), locations


class TestInferredPopulationIsNeverAStage:
    @staticmethod
    def _customers_stage() -> SlayerQuery:
        return query(name="customers", population="orders", dimensions=["orders.status"],
                     metrics=[m("sum(orders.amount)", "a")])

    async def test_rootless_stage_by_dimension(self, engine48) -> None:
        t = query(name="t", dimensions=["customers.tier"], metrics=[m("sum(customers.spend)", "sp")])
        stages = [self._customers_stage(), t,
                  query(population="t", dimensions=["t.tier"], metrics=[m("sum(t.sp)", "total")])]
        with pytest.raises(PopulationInferenceError) as ei:
            await engine48.execute(stages, data_source="test")
        assert ei.value.reason is PopulationErrorReason.SIBLING_STAGE

    async def test_rootless_stage_by_filter(self, engine48) -> None:
        t = query(name="t", filter=["customers.tier = 'gold'"], metrics=[m("sum(customers.spend)", "sp")])
        stages = [self._customers_stage(), t, query(population="t", metrics=[m("sum(t.sp)", "total")])]
        with pytest.raises(PopulationInferenceError) as ei:
            await engine48.execute(stages, data_source="test")
        assert ei.value.reason is PopulationErrorReason.SIBLING_STAGE

    async def test_rootless_root(self, engine48) -> None:
        stages = [self._customers_stage(),
                  query(dimensions=["customers.tier"], metrics=[m("sum(customers.spend)", "sp")])]
        with pytest.raises(PopulationInferenceError) as ei:
            await engine48.execute(stages, data_source="test")
        assert ei.value.reason is PopulationErrorReason.SIBLING_STAGE


class TestIdentifierLimit:
    async def test_near_limit_stage_name_fits_postgres(self) -> None:
        long = "s" * 60
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=os.path.join(d, "store"))
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            for model in (orders_dataset(), customers_dataset()):
                await storage.save_dataset(model, _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            resp = await engine.execute([
                query(name=long, population="orders", dimensions=["orders.status"],
                      metrics=[m("sum(orders.amount)", "a")]),
                query(population=long, dimensions=[f"{long}.status"], metrics=[m(f"sum({long}.a)", "t")]),
            ], dry_run=True)
        assert resp.columns == [f"{long}.status", "t"]
        assert resp.sql is not None
        tree = sqlglot.parse_one(resp.sql, dialect="postgres")
        names = [cte.alias for cte in tree.find_all(exp.CTE)]
        assert names, resp.sql
        assert len(set(names)) == len(names), names
        assert all(len(n.encode()) <= 63 for n in names), names


class TestReverseHopSpelling:
    async def test_reverse_hop_out_of_a_stage_is_spelled_by_its_name(self, engine) -> None:
        s = query(name="s", population="customers", dimensions=["customers.id", "customers.tier"],
                  metrics=[m("sum(customers.spend)", "sp")], to_many_handling="broadcast")
        root = query(population="s",
                     extensions=[{"dataset": "s", "relationships": [
                         {"to": "orders", "from_columns": ["id"], "to_columns": ["customer_id"]}]}],
                     dimensions=["s.tier"], metrics=[m("sum(orders.amount)", "a")], to_many_handling="broadcast")
        with pytest.warns(UserWarning):
            resp = await engine.execute([s, root])
        assert rows_by(resp.data, key="s.tier", value="a") == {t: 145.0 for t in AMOUNT_BY_TIER}
        (w,) = [w for w in resp.warnings if w.kind == "broadcast"]
        assert [d.reason for d in w.dimensions] == ["crosses a fanning or unproven relationship hop to s"]
        assert INTERNAL not in str([x.model_dump() for x in resp.warnings])

    async def test_producer_rooted_at_a_stage_aliases_its_host_by_model(self, engine) -> None:
        k = query(name="kpis", population="orders", dimensions=["orders.customer_id"],
                  metrics=[m("sum(orders.amount)", "amt")])
        root = query(population="customers",
                     extensions=[{"dataset": "customers", "relationships": [
                         {"to": "kpis", "from_columns": ["id"], "to_columns": ["customer_id"]}]}],
                     dimensions=["customers.tier"], metrics=[m("sum(kpis.amt)", "t")])
        resp = await engine.execute([k, root])
        assert rows_by(resp.data, key="customers.tier", value="t") == AMOUNT_BY_TIER
        dry = await engine.execute([k, root], dry_run=True)
        assert dry.sql is not None
        joins = [j for j in sqlglot.parse_one(dry.sql).find_all(exp.Join)
                 if isinstance(j.this, exp.Table) and j.this.name == "customers"]
        assert joins, dry.sql
        assert all(j.this.alias == "customers" for j in joins), dry.sql
