"""DEV-1954 — stale path spellings resolve across stage boundaries (slack rule).

A flat name written against a non-canonical spelling of an upstream column's path
(``customers__regions__rname`` for the canonical ``customers__hr__rname``) binds to
that column with one ``STALE_PATH_SPELLING`` warning per referencing position; an
exact name wins and an ambiguous respelling fails as an unknown reference.
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.errors import UnknownReferenceError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.scope import StageColumn
from slayer.engine.plan import plan_stages
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1954_fixtures import (
    dev1954_models,
    make_exec_engine,
    orders_q,
    slayer_query,
    stale_warnings,
)

STALE = "customers__regions__rname"
CANON = "customers__hr__rname"
AMOUNT_BY_NAME = {"North": 100.0, "South": 20.0, None: 47.0}


def _with_query_backed(models: List[SlayerModel]) -> List[SlayerModel]:
    """+ ``region_amounts`` (query-backed on orders) and ``regions.qb_label``
    reading its column by the stale spelling over a ``regions`` join."""
    regions = next(m for m in models if m.name == "regions")
    regions.joins.append(ModelJoin(
        target_model="region_amounts", join_pairs=[["id", "customers__region_id"]]))
    regions.columns.append(Column(
        name="qb_label", sql=f"region_amounts.{STALE}"))
    return [*models, SlayerModel(
        name="region_amounts", data_source="test",
        source_queries=[orders_q(
            dimensions=["customers.region_id", "customers.regions.rname"],
            measures=[{"formula": "amount:sum", "name": "amt"}])])]


def _parallel_models() -> List[SlayerModel]:
    models = dev1954_models()
    customers = next(m for m in models if m.name == "customers")
    customers.joins.append(ModelJoin(
        target_model="regions", join_pairs=[["region_id", "id"]], name="hr2"))
    return models


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_exec_engine(request, models=_with_query_backed(dev1954_models())):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unnamed_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_exec_engine(
        request, models=_with_query_backed(dev1954_models(named=False)),
    ):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def parallel_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_exec_engine(request, models=_parallel_models()):
        yield e


def _stage1(**kw) -> SlayerQuery:
    kw.setdefault("dimensions", ["customers.regions.rname"])
    kw.setdefault("measures", ["amount:sum"])
    return slayer_query(name="s1", source_model="orders", **kw)


def _stage2(dim: str, **kw) -> SlayerQuery:
    return slayer_query(source_model="s1", dimensions=[dim],
                       measures=["amount_sum:sum"], **kw)


def _amounts(resp, key: str) -> dict:
    return {r[key]: r["s1.amount_sum_sum"] for r in resp.data}


def _assert_one_stale(resp) -> None:
    (w,) = stale_warnings(resp)
    assert (w.original, w.normalized) == (STALE, CANON)


# --------------------------------------------------------------------------- #
# Emitted stage schemas carry each column's respellings.
# --------------------------------------------------------------------------- #
class TestStageColumnRespellings:
    @staticmethod
    def _schema(models, stage: SlayerQuery):
        bundle = ResolvedSourceBundle(source_model=models[0],
                                      referenced_models=models[1:])
        planned = plan_stages(
            queries=[stage, slayer_query(source_model=stage.name, measures=["*:count"])],
            bundle=bundle)
        schema = planned[0].stage_schema
        assert schema is not None
        return {c.name: c for c in schema.columns}

    def test_one_named_hop(self) -> None:
        cols = self._schema(dev1954_models(), _stage1(
            measures=["customers.regions.pop:max",
                      {"formula": "customers.hr.pop:min", "name": "explicit"}]))
        assert set(cols) == {CANON, "customers__hr__pop_max", "explicit"}
        assert set(cols[CANON].respellings) == {STALE}
        assert set(cols["customers__hr__pop_max"].respellings) == \
            {"customers__regions__pop_max"}
        assert cols["explicit"].respellings == ()

    @pytest.mark.parametrize("stage", [
        _stage1(dimensions=[], measures=["max(customers.regions.pop)"]),
        _stage1(dimensions=[], measures=["customers.regions.*:count"]),
        _stage1(dimensions=[], measures=["customers.regions.maxpop"]),
        _stage1(dimensions=[], measures=["amount:sum"], time_dimensions=[
            {"dimension": "customers.regions.founded_at", "granularity": "year"}]),
    ], ids=["functional", "star", "saved", "time"])
    def test_every_path_derived_form(self, stage) -> None:
        cols = self._schema(dev1954_models(), stage)
        derived = [c for c in cols.values() if c.name.startswith("customers__hr__")]
        assert derived
        for col in derived:
            assert set(col.respellings) == {
                col.name.replace("customers__hr__", "customers__regions__", 1)}

    def test_two_named_hops(self) -> None:
        models = dev1954_models()
        orders = models[0]
        orders.joins = [
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                      name="buyer") if j.target_model == "customers" else j
            for j in orders.joins]
        cols = self._schema(models, _stage1(measures=["customers.regions.pop:max"]))
        assert set(cols["buyer__hr__rname"].respellings) == {
            "customers__hr__rname", "buyer__regions__rname", STALE}
        assert set(cols["buyer__hr__pop_max"].respellings) == {
            "customers__hr__pop_max", "buyer__regions__pop_max",
            "customers__regions__pop_max"}

    def test_explicit_name_with_a_path_prefix_has_none(self) -> None:
        cols = self._schema(dev1954_models(), _stage1(measures=[
            {"formula": "customers.hr.pop:min", "name": "customers__hr__custom"}]))
        assert cols["customers__hr__custom"].respellings == ()

    def test_pass_through_inherits(self) -> None:
        stage1 = _stage1(measures=["customers.regions.pop:max"])
        stage2 = slayer_query(name="s2", source_model="s1", dimensions=[CANON],
                              measures=["customers__hr__pop_max:max",
                                        {"formula": "customers__hr__pop_max:min",
                                         "name": "customers__hr__lo"}])
        bundle = ResolvedSourceBundle(source_model=dev1954_models()[0],
                                      referenced_models=dev1954_models()[1:])
        planned = plan_stages(queries=[stage1, stage2, slayer_query(
            source_model="s2", measures=["*:count"])], bundle=bundle)
        schema = planned[1].stage_schema
        assert schema is not None
        cols = {c.name: c for c in schema.columns}
        assert set(cols[CANON].respellings) == {STALE}
        assert set(cols["customers__hr__pop_max_max"].respellings) == \
            {"customers__regions__pop_max_max"}
        assert cols["customers__hr__lo"].respellings == ()

    def test_unnamed_path_has_none(self) -> None:
        cols = self._schema(dev1954_models(named=False), _stage1())
        assert cols[STALE].respellings == ()

    def test_default_is_empty(self) -> None:
        assert StageColumn(name="x", sql_alias="x").respellings == ()


# --------------------------------------------------------------------------- #
# Multistage.
# --------------------------------------------------------------------------- #
class TestDownstreamStageStaleReference:
    async def test_binds_to_the_canonical_column(self, engine) -> None:
        stale = await engine.execute([_stage1(), _stage2(STALE)])
        canon = await engine.execute([_stage1(), _stage2(CANON)])
        assert stale.columns == canon.columns
        assert _amounts(stale, f"s1.{CANON}") == AMOUNT_BY_NAME
        _assert_one_stale(stale)
        assert stale_warnings(canon) == []

    async def test_extension_over_the_sibling_stage(self, engine) -> None:
        def ext(dim: str) -> SlayerQuery:
            return slayer_query(source_model={"source_name": "s1"}, dimensions=[dim],
                                measures=["amount_sum:sum"])

        stale = await engine.execute([_stage1(), ext(STALE)])
        canon = await engine.execute([_stage1(), ext(CANON)])
        assert stale.columns == canon.columns
        assert _amounts(stale, f"s1.{CANON}") == AMOUNT_BY_NAME
        _assert_one_stale(stale)

    async def test_survives_a_chain_of_stages(self, engine) -> None:
        stage2 = slayer_query(name="s2", source_model="s1", dimensions=[STALE],
                              measures=["amount_sum:sum"])
        resp = await engine.execute([_stage1(), stage2, slayer_query(
            source_model="s2", dimensions=[STALE], measures=["amount_sum_sum:sum"])])
        assert {r[f"s2.{CANON}"]: r["s2.amount_sum_sum_sum"] for r in resp.data} \
            == AMOUNT_BY_NAME
        warnings = stale_warnings(resp)
        assert len(warnings) == 2
        assert all((w.original, w.normalized) == (STALE, CANON) for w in warnings)

    async def test_ambiguous_respelling_fails_closed(self, parallel_engine) -> None:
        stage1 = _stage1(dimensions=["customers.hr.rname", "customers.hr2.rname"])
        stages = [stage1, _stage2(STALE)]
        with pytest.raises(UnknownReferenceError, match=STALE):
            await parallel_engine.execute(stages)

    async def test_exact_name_wins(self, engine) -> None:
        stage1 = _stage1(measures=[{"formula": "amount:sum", "name": STALE}])
        resp = await engine.execute([stage1, slayer_query(
            source_model="s1", measures=[{"formula": f"{STALE}:sum", "name": "t"}])])
        assert resp.data[0]["s1.t"] == pytest.approx(167.0)
        assert stale_warnings(resp) == []

    async def test_one_warning_per_referencing_position(self, engine) -> None:
        stage2 = _stage2(STALE, filters=[
            f"{STALE} = 'North' or {STALE} = 'South'"])
        resp = await engine.execute([_stage1(), stage2])
        assert _amounts(resp, f"s1.{CANON}") == {"North": 100.0, "South": 20.0}
        warnings = stale_warnings(resp)
        assert len(warnings) == 2
        assert len({w.location for w in warnings}) == 2
        assert all((w.original, w.normalized) == (STALE, CANON) for w in warnings)


# --------------------------------------------------------------------------- #
# Query-backed consumers.
# --------------------------------------------------------------------------- #
class TestQueryBackedConsumer:
    async def test_query_on_the_query_backed_model(self, engine) -> None:
        stale = await engine.execute(slayer_query(
            source_model="region_amounts", dimensions=[STALE], measures=["amt:sum"]))
        canon = await engine.execute(slayer_query(
            source_model="region_amounts", dimensions=[CANON], measures=["amt:sum"]))
        assert stale.columns == canon.columns
        assert sorted(map(repr, stale.data)) == sorted(map(repr, canon.data))
        _assert_one_stale(stale)

    async def test_other_models_column_sql(self, engine) -> None:
        resp = await engine.execute(slayer_query(
            source_model="regions", dimensions=["qb_label"]))
        assert {r["regions.qb_label"] for r in resp.data} == {"North", "South"}
        _assert_one_stale(resp)


    async def test_query_backed_over_query_backed(self, engine) -> None:
        await engine.storage.save_model(SlayerModel(
            name="ra2", data_source="test", source_queries=[slayer_query(
                source_model="region_amounts", dimensions=[CANON],
                measures=[{"formula": "amt:sum", "name": "amt2"}])]))
        resp = await engine.execute(slayer_query(
            source_model="ra2", dimensions=[STALE], measures=["amt2:sum"]))
        assert {r[f"ra2.{CANON}"] for r in resp.data} == {"North", "South", None}
        _assert_one_stale(resp)

    async def test_stored_query_keeps_its_spelling(self, engine) -> None:
        await engine.execute(slayer_query(
            source_model="region_amounts", dimensions=[STALE], measures=["amt:sum"]))
        model = await engine.storage.get_model("region_amounts", data_source="test")
        assert model is not None
        assert model.source_queries
        dims = [d.full_name for d in model.source_queries[0].dimensions]
        assert dims == ["customers.region_id", "customers.regions.rname"]

    async def test_authored_columns_stay_exact(self, engine) -> None:
        orders = await engine.storage.get_model("orders", data_source="test")
        assert orders is not None
        orders.columns = [*orders.columns, Column(name=CANON, sql="status")]
        await engine.storage.save_model(orders)
        query = orders_q(dimensions=[STALE])
        with pytest.raises(UnknownReferenceError, match=STALE):
            await engine.execute(query)


class TestEdgeNamedLater:
    async def test_existing_reference_keeps_resolving(self, unnamed_engine) -> None:
        query = slayer_query(source_model="region_amounts", dimensions=[STALE],
                            measures=["amt:sum"])
        before = await unnamed_engine.execute(query)
        assert stale_warnings(before) == []

        storage = unnamed_engine.storage
        customers = await storage.get_model("customers", data_source="test")
        assert customers is not None
        customers.joins = [
            j.model_copy(update={"name": "hr"}) if j.target_model == "regions" else j
            for j in customers.joins]
        await storage.save_model(customers)

        after = await unnamed_engine.execute(query)
        assert sorted(map(repr, (r[f"region_amounts.{CANON}"] for r in after.data))) \
            == sorted(map(repr, (r[f"region_amounts.{STALE}"] for r in before.data)))
        _assert_one_stale(after)


# --------------------------------------------------------------------------- #
# Result cache.
# --------------------------------------------------------------------------- #
class TestCacheHitCarriesTheRequestersWarnings:
    async def test_canonical_first_then_stale(self, engine) -> None:
        await engine.execute([_stage1(), _stage2(CANON)], cache=True)
        stale = await engine.execute([_stage1(), _stage2(STALE)], cache=True)
        assert engine.cache_size == 1
        _assert_one_stale(stale)

    async def test_stale_first_then_canonical(self, engine) -> None:
        await engine.execute([_stage1(), _stage2(STALE)], cache=True)
        canon = await engine.execute([_stage1(), _stage2(CANON)], cache=True)
        assert engine.cache_size == 1
        assert stale_warnings(canon) == []
