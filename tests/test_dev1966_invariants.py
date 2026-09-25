"""Internal invariants: one per-stage model universe, collision fail-closed, no deferral arm.

Spec: openspec …/changes/dev-1966-… design decisions 4, 5 and 13.
"""

from __future__ import annotations

import pathlib
from typing import Any, List

import pytest
import yaml

import slayer.engine.query_engine as query_engine_module
from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.scope import StageColumn, StageSchema
from slayer.ir.planned import PlannedQuery
from slayer.ir.source_bundle import ResolvedSourceBundle, stage_bundle_with_siblings
from slayer.sql.generator import SQLGenerator

from tests import _dev1948_fixtures as f48
from tests._dev1966_fixtures import customers_model, dev1966_engine, m, orders_model, query
from tests._law_harness import DEFERRAL_SITES

_INDEX_YAML = pathlib.Path(__file__).parent.parent / "architecture" / "index.yaml"


class TestGeneratorInvariant:
    def test_unspliced_query_backed_model_raises_a_non_deferral_value_error(self) -> None:
        qb = SlayerModel(name="qb_unspliced", data_source="test",
                         source_queries=[query(source_model="orders", measures=[m("amount:sum", "a")])])
        with pytest.raises(ValueError) as exc:
            SQLGenerator(dialect="postgres")._build_from_clause_from_planned(
                source_model=qb, source_relation="qb_unspliced")
        msg = str(exc.value)
        assert "qb_unspliced" in msg and "query-backed" in msg, msg
        assert "deferred" not in msg.lower() and "DEV-" not in msg, msg

    def test_no_query_backed_deferral_site(self) -> None:
        assert [s for s in DEFERRAL_SITES if "source_queries" in s.fragment] == []

    def test_guards_baseline_is_zero(self) -> None:
        assert yaml.safe_load(_INDEX_YAML.read_text())["guards"]["baseline"] == 0


def _schema(name: str) -> StageSchema:
    return StageSchema(relation_name=name, grain=["id"], columns=[
        StageColumn(name="id", sql_alias="id", type=DataType.INT),
        StageColumn(name="tr", sql_alias="tr", type=DataType.TEXT),
    ])


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres", source_model=orders_model(),
        referenced_models=[orders_model(), customers_model()])


class TestStageBundleCollision:
    def test_identical_host_entry_is_deduplicated(self) -> None:
        out = stage_bundle_with_siblings(
            bundle=_bundle(), source_model=orders_model(),
            sibling_schemas={"c": _schema("c")}, data_source="test")
        names = [x.name for x in out.referenced_models]
        assert sorted(names) == ["c", "customers", "orders"]

    def test_an_extended_host_replaces_its_base(self) -> None:
        extended = orders_model().model_copy(update={"columns": [
            *orders_model().columns, Column(name="double_amount", sql="amount * 2", type=DataType.DOUBLE)]})
        out = stage_bundle_with_siblings(
            bundle=_bundle(), source_model=extended, sibling_schemas={}, data_source="test")
        (host,) = [x for x in out.referenced_models if x.name == "orders"]
        assert host.get_column("double_amount") is not None

    def test_a_stage_identity_meeting_a_different_model_raises(self) -> None:
        with pytest.raises(ValueError, match="customers"):
            stage_bundle_with_siblings(
                bundle=_bundle(), source_model=orders_model(),
                sibling_schemas={"customers": _schema("customers")}, data_source="test")


def _stamped_bundle(planned: PlannedQuery) -> ResolvedSourceBundle:
    (bundle,) = [v for v in planned.__dict__.values() if isinstance(v, ResolvedSourceBundle)]
    return bundle


def _assert_no_later_stage(planned: List[PlannedQuery]) -> None:
    relations = [p.stage_schema.relation_name if p.stage_schema else None for p in planned[:-1]]
    for i, p in enumerate(planned):
        universe = set(_stamped_bundle(p).models_by_name)
        later = {r for r in relations[i + 1:] if r}
        assert not universe & later, (i, universe & later)
        assert set(p.stage_reads) <= universe, (i, p.stage_reads, universe)


class TestStampedBundle:
    @pytest.fixture
    def planned(self, monkeypatch) -> List[Any]:
        captured: List[Any] = []
        real = query_engine_module.plan_stages

        def spy(*, queries, bundle):
            out = real(queries=queries, bundle=bundle)
            captured[:] = out
            return out

        monkeypatch.setattr(query_engine_module, "plan_stages", spy)
        return captured

    async def test_user_chain(self, planned) -> None:
        class _Req:
            param = "sqlite"

        async for e in f48.make_dev1948_engine(_Req()):
            queries: List[Any] = list(f48.chain_list())
            await e.execute(queries, dry_run=True)
        assert len(planned) == 4
        _assert_no_later_stage(planned)

    async def test_spliced_consumer(self, planned) -> None:
        async with dev1966_engine("sqlite") as e:
            await e.execute(query(source_model="clients", dimensions=["tier"],
                                  measures=[m("cust_rev.rev:sum", "r")]), dry_run=True)
        assert len(planned) == 3
        _assert_no_later_stage(planned)
