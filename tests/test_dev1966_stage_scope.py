"""Stage names are query-local: they override same-named models only inside their own list.

Spec: openspec …/specs/queries/multi-stage — "Stage names are query-local".
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest
import sqlglot
from sqlglot import exp

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
    customers_model,
    dev1966_engine,
    m,
    orders_model,
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


def _b_over(dim: str, source) -> SlayerQuery:
    return query(name="b", source_model=source, dimensions=[dim], measures=[m("amount:sum", "amt")])


def _root_over_b(flat_dim: str) -> SlayerQuery:
    return query(source_model="b", dimensions=[flat_dim], measures=[m("amt:sum", "total")])


class TestStoredDefinitionsNeverSeeStages:
    async def test_stored_join_resolves_to_the_model_beside_a_same_named_stage(self, engine) -> None:
        customers_stage = query(name="customers", source_model="orders", dimensions=["status"],
                                measures=[m("amount:sum", "a")])
        resp = await engine.execute(
            [customers_stage, _b_over("customers.tier", "orders"), _root_over_b("customers__tier")])
        assert resp.columns == ["b.customers__tier", "b.total"]
        assert rows_by(resp.data, key="b.customers__tier", value="b.total") == AMOUNT_BY_TIER

    async def test_query_written_join_to_a_stage_wins_over_a_same_named_stored_edge(
        self, engine,
    ) -> None:
        customers_stage = query(name="customers", source_model="orders", dimensions=["customer_id"],
                                measures=[m("status:max", "tier")])
        b = _b_over("customers.tier", {
            "source_name": "orders",
            "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "customer_id"]]}],
        })
        resp = await engine.execute([customers_stage, b, _root_over_b("customers__tier")])
        assert rows_by(resp.data, key="b.customers__tier", value="b.total") == {"ok": 105.0, "new": 40.0}

    async def test_a_stage_named_like_a_physical_table_does_not_hide_it(self, engine) -> None:
        acct_stage = query(name="acct", source_model="orders", dimensions=["status"],
                           measures=[m("amount:sum", "a")])
        resp = await engine.execute(
            [acct_stage, _b_over("clients.tier", "orders"), _root_over_b("clients__tier")])
        assert rows_by(resp.data, key="b.clients__tier", value="b.total") == AMOUNT_BY_TIER


class TestQueryBackedPrivateStageNames:
    async def test_private_stage_named_like_a_model_does_not_capture_its_stored_join(
        self, engine,
    ) -> None:
        resp = await engine.execute(query(
            source_model="qb_shadow", dimensions=["customers__tier"],
            measures=[m("amt:sum", "total")]))
        assert resp.columns == ["qb_shadow.customers__tier", "qb_shadow.total"]
        assert rows_by(resp.data, key="qb_shadow.customers__tier", value="qb_shadow.total") \
            == AMOUNT_BY_TIER

    async def test_consumer_name_reaches_the_model_not_a_private_stage(self, engine) -> None:
        s = query(name="s", source_model="qb_shadow", dimensions=["customers__tier"],
                  measures=[m("amt:sum", "t")])
        root = query(
            source_model={"source_name": "customers", "joins": [
                {"target_model": "s", "join_pairs": [["tier", "customers__tier"]]}]},
            dimensions=["tier", "s.t"], measures=[m("spend:sum", "sp")])
        resp = await engine.execute([s, root])
        assert resp.columns == ["customers.tier", "customers.s.t", "customers.sp"]
        assert sorted_rows(resp.data) == sorted_rows([
            {"customers.tier": t, "customers.s.t": AMOUNT_BY_TIER[t], "customers.sp": SPEND_BY_TIER[t]}
            for t in SPEND_BY_TIER
        ])

    async def test_private_stage_is_unreachable_from_the_consumer(self, engine) -> None:
        s = query(name="s", source_model="cust_rev", dimensions=["customer_id"],
                  measures=[m("rev:sum", "r")])
        with pytest.raises(ValueError, match=r"'x'") as exc:
            await engine.execute([s, query(source_model="x", measures=[m("amt:sum", "t")])])
        assert INTERNAL not in str(exc.value)


class TestUserSpelling:
    async def test_chain_result_keys_unchanged(self, engine48) -> None:
        resp = await engine48.execute(f48.chain_list())
        assert resp.columns == ["b.c__tr", "b.total"]
        assert rows_by(resp.data, key="b.c__tr", value="b.total") == f48.AMOUNT_BY_TIER

    async def test_run_by_name_result_keys_unchanged(self, engine) -> None:
        resp = await engine.execute("cust_rev")
        assert resp.columns == ["x.customer_id", "x.rev", "x.top"]
        assert sorted_rows(resp.data) == sorted_rows([
            {"x.customer_id": c, "x.rev": CUST_REV[c], "x.top": CUST_TOP[c]} for c in CUST_REV])

    async def test_stage_warning_names_the_user_stage(self, engine) -> None:
        b = query(name="b", source_model="orders", dimensions=["status"],
                  measures=[m("amount:sum", "amt"), m("customers.spend:sum", "cs")])
        with pytest.warns(UserWarning):
            resp = await engine.execute(
                [b, query(source_model="b", dimensions=["status"], measures=[m("amt:sum", "t")])])
        assert [w.location for w in resp.warnings if w.kind == "broadcast"] == ["stage 'b'"]

    async def test_stage_binding_error_names_the_user_stage(self, engine48) -> None:
        stages = [f48.stage_x(), query(name="c", source_model="x", dimensions=["nope"],
                                       measures=[m("total_spend:sum", "a")]),
                  query(source_model="c", measures=[m("a:sum", "t")])]
        with pytest.raises(ValueError) as exc:
            await engine48.execute(stages)
        assert "stage 'x'" in str(exc.value)
        assert INTERNAL not in str(exc.value)

    async def test_stale_spelling_warnings_name_user_stages(self) -> None:
        class _Req:
            param = "sqlite"

        stale, canon = "customers__regions__rname", "customers__hr__rname"
        s1 = slayer_query(name="s1", source_model="orders",
                          dimensions=["customers.regions.rname"], measures=["amount:sum"])
        s2 = slayer_query(name="s2", source_model="s1", dimensions=[stale],
                          measures=["amount_sum:sum"])
        root = slayer_query(source_model="s2", dimensions=[stale], measures=["amount_sum_sum:sum"])
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
        return query(name="customers", source_model="orders", dimensions=["status"],
                     measures=[m("amount:sum", "a")])

    async def test_rootless_stage_by_dimension(self, engine48) -> None:
        t = query(name="t", dimensions=["tier"], measures=[m("spend:sum", "sp")])
        resp = await engine48.execute(
            [self._customers_stage(), t,
             query(source_model="t", dimensions=["tier"], measures=[m("sp:sum", "total")])],
            data_source="test")
        assert rows_by(resp.data, key="t.tier", value="t.total") == f48.SPEND_BY_TIER

    async def test_rootless_stage_by_filter(self, engine48) -> None:
        t = query(name="t", filters=["tier = 'gold'"], measures=[m("spend:sum", "sp")])
        resp = await engine48.execute(
            [self._customers_stage(), t, query(source_model="t", measures=[m("sp:sum", "total")])],
            data_source="test")
        assert resp.data == [{"t.total": 160.0}]

    async def test_rootless_root(self, engine48) -> None:
        resp = await engine48.execute(
            [self._customers_stage(), query(dimensions=["tier"], measures=[m("spend:sum", "sp")])],
            data_source="test")
        assert rows_by(resp.data, key="customers.tier", value="customers.sp") == f48.SPEND_BY_TIER


class TestIdentifierLimit:
    async def test_near_limit_stage_name_fits_postgres(self) -> None:
        long = "s" * 60
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=os.path.join(d, "store"))
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            for model in (orders_model(), customers_model()):
                await storage.save_model(model, _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            resp = await engine.execute([
                query(name=long, source_model="orders", dimensions=["status"],
                      measures=[m("amount:sum", "a")]),
                query(source_model=long, dimensions=["status"], measures=[m("a:sum", "t")]),
            ], dry_run=True)
        assert resp.columns == [f"{long}.status", f"{long}.t"]
        assert resp.sql is not None
        tree = sqlglot.parse_one(resp.sql, dialect="postgres")
        names = [cte.alias for cte in tree.find_all(exp.CTE)]
        assert names and len(set(names)) == len(names), names
        assert all(len(n.encode()) <= 63 for n in names), names
