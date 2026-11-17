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
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import FieldRef, SlayerQuery
from slayer.core.scope import StageField
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


def _with_query_backed(models: List[SlayerDataset]) -> List[SlayerDataset]:
    """+ ``region_amounts`` (query-backed on orders) and ``regions.qb_label``
    reading its column by the stale spelling over a ``regions`` join."""
    regions = next(m for m in models if m.name == "regions")
    regions.relationships.append(DatasetRelationship(
        to="region_amounts", join_pairs=[["id", "customers__region_id"]]))
    regions.fields.append(DatasetField(
        name="qb_label", sql=f"region_amounts.{STALE}"))
    return [*models, SlayerDataset(
        name="region_amounts", data_source="test",
        source_queries=[orders_q(
            dimensions=["customers.region_id", "customers.regions.rname"],
            measures=[{"formula": "sum(amount)", "name": "amt"}])])]


def _parallel_models() -> List[SlayerDataset]:
    models = dev1954_models()
    customers = next(m for m in models if m.name == "customers")
    customers.relationships.append(DatasetRelationship(
        to="regions", join_pairs=[["region_id", "id"]], name="hr2"))
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


def _ambiguous_qb_models() -> List[SlayerDataset]:
    """+ ``pq`` projecting both parallel edges and ``regions.amb`` reading it by the
    stale spelling (a respelling of two ``pq`` columns)."""
    models = _parallel_models()
    regions = next(m for m in models if m.name == "regions")
    regions.relationships.append(DatasetRelationship(
        to="pq", join_pairs=[["id", "customers__region_id"]]))
    regions.fields.append(DatasetField(name="amb", sql=f"pq.{STALE}"))
    return [*models, SlayerDataset(
        name="pq", data_source="test", source_queries=[orders_q(
            dimensions=["customers.region_id", "customers.hr.rname", "customers.hr2.rname"],
            measures=[{"formula": "sum(amount)", "name": "amt"}])])]


@pytest.fixture(params=["sqlite", "duckdb"])
async def ambiguous_qb_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_exec_engine(request, models=_ambiguous_qb_models()):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def parallel_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_exec_engine(request, models=_parallel_models()):
        yield e


def _stage1(**kw) -> SlayerQuery:
    kw.setdefault("dimensions", ["customers.regions.rname"])
    kw.setdefault("measures", ["sum(amount)"])
    return slayer_query(name="s1", source_model="orders", **kw)


def _stage2(*, dim: str, **kw) -> SlayerQuery:
    return slayer_query(source_model="s1", dimensions=[dim],
                       measures=["sum(amount_sum)"], **kw)


def _amounts(*, resp, key: str) -> dict:
    return {r[key]: r["s1.amount_sum_sum"] for r in resp.data}


def _assert_one_stale(resp) -> None:
    (w,) = stale_warnings(resp)
    assert (w.original, w.normalized) == (STALE, CANON)


# --------------------------------------------------------------------------- #
# Emitted stage schemas carry each column's respellings.
# --------------------------------------------------------------------------- #
class TestStageColumnRespellings:
    @staticmethod
    def _schema(*, models, stage: SlayerQuery):
        bundle = ResolvedSourceBundle(dialect="postgres", population=models[0],
                                      referenced_datasets=models[1:])
        planned = plan_stages(
            queries=[stage, slayer_query(source_model=stage.name, measures=["count(*)"])],
            bundle=bundle)
        schema = planned[0].stage_schema
        assert schema is not None
        return {c.name: c for c in schema.fields}

    def test_one_named_hop(self) -> None:
        cols = self._schema(models=dev1954_models(), stage=_stage1(
            measures=["max(customers.regions.pop)",
                      {"formula": "min(customers.hr.pop)", "name": "explicit"}]))
        assert set(cols) == {CANON, "customers__hr__pop_max", "explicit"}
        assert set(cols[CANON].respellings) == {STALE}
        assert set(cols["customers__hr__pop_max"].respellings) == \
            {"customers__regions__pop_max"}
        assert cols["explicit"].respellings == ()

    @pytest.mark.parametrize("stage", [
        _stage1(dimensions=[], measures=["max(customers.regions.pop)"]),
        _stage1(dimensions=[], measures=["count(customers.regions.*)"]),
        _stage1(dimensions=[], measures=["customers.regions.maxpop"]),
        _stage1(dimensions=[], measures=["sum(amount)"], time_dimensions=[
            {"dimension": "customers.regions.founded_at", "granularity": "year"}]),
    ], ids=["functional", "star", "saved", "time"])
    def test_every_path_derived_form(self, stage) -> None:
        cols = self._schema(models=dev1954_models(), stage=stage)
        derived = [c for c in cols.values() if c.name.startswith("customers__hr__")]
        assert derived
        for col in derived:
            assert set(col.respellings) == {
                col.name.replace("customers__hr__", "customers__regions__", 1)}

    def test_two_named_hops(self) -> None:
        models = dev1954_models()
        orders = models[0]
        orders.relationships = [
            DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]],
                      name="buyer") if j.to == "customers" else j
            for j in orders.relationships]
        cols = self._schema(models=models, stage=_stage1(measures=["max(customers.regions.pop)"]))
        assert set(cols["buyer__hr__rname"].respellings) == {
            "customers__hr__rname", "buyer__regions__rname", STALE}
        assert set(cols["buyer__hr__pop_max"].respellings) == {
            "customers__hr__pop_max", "buyer__regions__pop_max",
            "customers__regions__pop_max"}

    def test_explicit_name_with_a_path_prefix_has_none(self) -> None:
        cols = self._schema(models=dev1954_models(), stage=_stage1(measures=[
            {"formula": "min(customers.hr.pop)", "name": "customers__hr__custom"}]))
        assert cols["customers__hr__custom"].respellings == ()

    def test_pass_through_inherits(self) -> None:
        stage1 = _stage1(measures=["max(customers.regions.pop)"])
        stage2 = slayer_query(name="s2", source_model="s1", dimensions=[CANON],
                              measures=["max(customers__hr__pop_max)",
                                        {"formula": "min(customers__hr__pop_max)",
                                         "name": "customers__hr__lo"}])
        bundle = ResolvedSourceBundle(dialect="postgres", population=dev1954_models()[0],
                                      referenced_datasets=dev1954_models()[1:])
        planned = plan_stages(queries=[stage1, stage2, slayer_query(
            source_model="s2", measures=["count(*)"])], bundle=bundle)
        schema = planned[1].stage_schema
        assert schema is not None
        cols = {c.name: c for c in schema.fields}
        assert set(cols[CANON].respellings) == {STALE}
        assert set(cols["customers__hr__pop_max_max"].respellings) == \
            {"customers__regions__pop_max_max"}
        assert cols["customers__hr__lo"].respellings == ()

    def test_unnamed_path_has_none(self) -> None:
        cols = self._schema(models=dev1954_models(named=False), stage=_stage1())
        assert cols[STALE].respellings == ()

    def test_default_is_empty(self) -> None:
        assert StageField(name="x", sql_alias="x").respellings == ()


# --------------------------------------------------------------------------- #
# Multistage.
# --------------------------------------------------------------------------- #
class TestDownstreamStageStaleReference:
    async def test_binds_to_the_canonical_column(self, engine: SlayerQueryEngine) -> None:
        stale = await engine.execute([_stage1(), _stage2(dim=STALE)])
        canon = await engine.execute([_stage1(), _stage2(dim=CANON)])
        assert stale.columns == canon.columns
        assert _amounts(resp=stale, key=f"s1.{CANON}") == AMOUNT_BY_NAME
        _assert_one_stale(stale)
        assert stale_warnings(canon) == []

    async def test_extension_over_the_sibling_stage(self, engine: SlayerQueryEngine) -> None:
        def ext(dim: str) -> SlayerQuery:
            return slayer_query(source_model="s1", extensions=[{"source_name": "s1"}], dimensions=[dim],
                                measures=["sum(amount_sum)"])

        stale = await engine.execute([_stage1(), ext(STALE)])
        canon = await engine.execute([_stage1(), ext(CANON)])
        assert stale.columns == canon.columns
        assert _amounts(resp=stale, key=f"s1.{CANON}") == AMOUNT_BY_NAME
        _assert_one_stale(stale)

    async def test_survives_a_chain_of_stages(self, engine) -> None:
        stage2 = slayer_query(name="s2", source_model="s1", dimensions=[STALE],
                              measures=["sum(amount_sum)"])
        resp = await engine.execute([_stage1(), stage2, slayer_query(
            source_model="s2", dimensions=[STALE], measures=["sum(amount_sum_sum)"])])
        assert {r[f"s2.{CANON}"]: r["s2.amount_sum_sum_sum"] for r in resp.data} \
            == AMOUNT_BY_NAME
        warnings = stale_warnings(resp)
        assert len(warnings) == 2
        assert all((w.original, w.normalized) == (STALE, CANON) for w in warnings)

    async def test_ambiguous_respelling_fails_closed(self, parallel_engine) -> None:
        stage1 = _stage1(dimensions=["customers.hr.rname", "customers.hr2.rname"])
        stages = [stage1, _stage2(dim=STALE)]
        with pytest.raises(UnknownReferenceError, match=STALE):
            await parallel_engine.execute(stages)

    async def test_joined_query_backed_leaf_downstream(self, engine) -> None:
        stage1 = slayer_query(name="s1", source_model="regions",
                              dimensions=[f"region_amounts.{CANON}"])
        resp = await engine.execute([stage1, slayer_query(
            source_model="s1", dimensions=[f"region_amounts__{STALE}"])])
        assert {r[f"s1.region_amounts__{CANON}"] for r in resp.data} == {"North", "South"}
        (w,) = stale_warnings(resp)
        assert (w.original, w.normalized) == (
            f"region_amounts__{STALE}", f"region_amounts__{CANON}")

    async def test_exact_name_wins(self, engine) -> None:
        stage1 = _stage1(measures=[{"formula": "sum(amount)", "name": STALE}])
        resp = await engine.execute([stage1, slayer_query(
            source_model="s1", measures=[{"formula": f"sum({STALE})", "name": "t"}])])
        assert resp.data[0]["s1.t"] == pytest.approx(167.0)
        assert stale_warnings(resp) == []

    async def test_one_warning_per_referencing_position(self, engine) -> None:
        stage2 = _stage2(dim=STALE, filters=[
            f"{STALE} = 'North' or {STALE} = 'South'"])
        resp = await engine.execute([_stage1(), stage2])
        assert _amounts(resp=resp, key=f"s1.{CANON}") == {"North": 100.0, "South": 20.0}
        warnings = stale_warnings(resp)
        assert len(warnings) == 2
        assert len({w.location for w in warnings}) == 2
        assert all((w.original, w.normalized) == (STALE, CANON) for w in warnings)


# --------------------------------------------------------------------------- #
# Query-backed consumers.
# --------------------------------------------------------------------------- #
class TestQueryBackedConsumer:
    async def test_ambiguous_column_sql_fails_closed(self, ambiguous_qb_engine) -> None:
        query = slayer_query(source_model="regions", dimensions=["amb"])
        with pytest.raises(UnknownReferenceError, match=STALE):
            await ambiguous_qb_engine.execute(query)

    async def test_query_on_the_query_backed_model(self, engine: SlayerQueryEngine) -> None:
        stale = await engine.execute(slayer_query(
            source_model="region_amounts", dimensions=[STALE], measures=["sum(amt)"]))
        canon = await engine.execute(slayer_query(
            source_model="region_amounts", dimensions=[CANON], measures=["sum(amt)"]))
        assert stale.columns == canon.columns
        assert sorted(map(repr, stale.data)) == sorted(map(repr, canon.data))
        _assert_one_stale(stale)

    async def test_other_models_column_sql(self, engine) -> None:
        resp = await engine.execute(slayer_query(
            source_model="regions", dimensions=["qb_label"]))
        assert {r["regions.qb_label"] for r in resp.data} == {"North", "South"}
        _assert_one_stale(resp)


    async def test_joined_stale_leaf_surfaces_the_canonical_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(slayer_query(
            source_model="regions", dimensions=[f"region_amounts.{STALE}"]))
        assert resp.columns == [f"regions.region_amounts.{CANON}"]
        _assert_one_stale(resp)

    async def test_query_backed_over_query_backed(self, engine: SlayerQueryEngine) -> None:
        await engine.storage.save_dataset(SlayerDataset(
            name="ra2", data_source="test", source_queries=[slayer_query(
                source_model="region_amounts", dimensions=[CANON],
                measures=[{"formula": "sum(amt)", "name": "amt2"}])]))
        resp = await engine.execute(slayer_query(
            source_model="ra2", dimensions=[STALE], measures=["sum(amt2)"]))
        assert {r[f"ra2.{CANON}"] for r in resp.data} == {"North", "South", None}
        _assert_one_stale(resp)

    async def test_stored_query_keeps_its_spelling(self, engine: SlayerQueryEngine) -> None:
        await engine.execute(slayer_query(
            source_model="region_amounts", dimensions=[STALE], measures=["sum(amt)"]))
        model = await engine.storage.get_dataset("region_amounts", data_source="test")
        assert model is not None
        assert model.source_queries
        dimensions = model.source_queries[0].dimensions
        assert dimensions is not None and all(isinstance(d, FieldRef) for d in dimensions)
        dims = [d.full_name for d in dimensions if isinstance(d, FieldRef)]
        assert dims == ["customers.region_id", "customers.regions.rname"]

    async def test_authored_columns_stay_exact(self, engine: SlayerQueryEngine) -> None:
        orders = await engine.storage.get_dataset("orders", data_source="test")
        assert orders is not None
        orders.fields = [*orders.fields, DatasetField(name=CANON, sql="status")]
        await engine.storage.save_dataset(orders)
        query = orders_q(dimensions=[STALE])
        with pytest.raises(UnknownReferenceError, match=STALE):
            await engine.execute(query)


class TestEdgeNamedLater:
    async def test_existing_reference_keeps_resolving(self, unnamed_engine: SlayerQueryEngine) -> None:
        query = slayer_query(source_model="region_amounts", dimensions=[STALE],
                            measures=["sum(amt)"])
        before = await unnamed_engine.execute(query)
        assert stale_warnings(before) == []

        storage = unnamed_engine.storage
        customers = await storage.get_dataset("customers", data_source="test")
        assert customers is not None
        customers.relationships = [
            j.model_copy(update={"name": "hr"}) if j.to == "regions" else j
            for j in customers.relationships]
        await storage.save_dataset(customers)

        after = await unnamed_engine.execute(query)
        assert sorted(map(repr, (r[f"region_amounts.{CANON}"] for r in after.data))) \
            == sorted(map(repr, (r[f"region_amounts.{STALE}"] for r in before.data)))
        _assert_one_stale(after)


# --------------------------------------------------------------------------- #
# Result cache.
# --------------------------------------------------------------------------- #
class TestCacheHitCarriesTheRequestersWarnings:
    async def test_canonical_first_then_stale(self, engine) -> None:
        await engine.execute([_stage1(), _stage2(dim=CANON)], cache=True)
        stale = await engine.execute([_stage1(), _stage2(dim=STALE)], cache=True)
        assert engine.cache_size == 1
        _assert_one_stale(stale)

    async def test_stale_first_then_canonical(self, engine) -> None:
        await engine.execute([_stage1(), _stage2(dim=STALE)], cache=True)
        canon = await engine.execute([_stage1(), _stage2(dim=CANON)], cache=True)
        assert engine.cache_size == 1
        assert stale_warnings(canon) == []
