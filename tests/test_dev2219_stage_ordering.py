"""Stage ordering and relationship-graph membership read every sibling a stage names, expressions included.

Spec: openspec …/specs/queries/multi-stage — "Stages are ordered by every sibling reference".
"""

from __future__ import annotations

import itertools
from typing import Any

import pytest

from slayer.core.query import SlayerQuery
from slayer.engine import query_engine as query_engine_module
from slayer.engine.stage_ordering import stage_sibling_reads, topologically_order_stages

from tests._dev2219_fixtures import (
    DIALECTS,
    EVENT_COUNT,
    by,
    customers,
    ext,
    join,
    m,
    orders,
    outcome,
    over,
    q,
    shop,
    with_datasets,
)


def _reader(name: str, sibling: str) -> SlayerQuery:
    """Over ``customers``; reads ``sibling`` only through ``max(<sibling>.ne)``."""
    return q(name=name, population="customers", dimensions=["tier"], metrics=[m(f"max({sibling}.ne)", "ev")])


def _events(name: str) -> SlayerQuery:
    return q(name=name, population="events", metrics=[m("count(*)", "ne")])


def _expression_list(reader: str, sibling: str, *, reader_first: bool = True) -> list[SlayerQuery | dict]:
    stages: list[SlayerQuery | dict] = [_reader(reader, sibling), _events(sibling)]
    if not reader_first:
        stages.reverse()
    return [*stages, over(reader, dimensions=["tier"], metric="max(ev)", alias="ev_max")]


class TestExpressionOnlySibling:
    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize(("reader", "sibling"), [("a_reader", "z_ev"), ("z_reader", "a_ev")])
    @pytest.mark.parametrize("reader_first", [True, False])
    async def test_executes_under_either_name_order(self, dialect: str, reader: str, sibling: str,
                                                    reader_first: bool) -> None:
        async with shop(dialect) as engine:
            resp = await engine.execute(_expression_list(reader, sibling, reader_first=reader_first))
        assert by(resp, key="tier", value="ev_max") == {"gold": EVENT_COUNT, "silver": EVENT_COUNT,
                                                         "bronze": EVENT_COUNT}

    def test_extractor_returns_the_expression_sibling(self) -> None:
        reads = stage_sibling_reads(query=_reader("a_reader", "z_ev"), siblings={"a_reader", "z_ev"})
        assert reads == {"z_ev"}

    @pytest.mark.parametrize("field", ["dimensions", "filter", "order"])
    def test_extractor_reads_every_expression_position(self, field: str) -> None:
        value = {
            "dimensions": ["z_ev.ne"],
            "filter": ["z_ev.ne > 0"],
            "order": [{"field": "max(z_ev.ne)", "direction": "desc"}],
        }[field]
        query = q(name="r", population="customers", metrics=[m("count(*)", "n")], **{field: value})
        assert stage_sibling_reads(query=query, siblings={"r", "z_ev"}) == {"z_ev"}

    def test_a_dataset_named_like_no_sibling_is_not_a_read(self) -> None:
        query = q(name="r", population="orders", dimensions=["customers.tier"], metrics=[m("count(*)", "n")])
        assert stage_sibling_reads(query=query, siblings={"r", "z_ev"}) == set()

    @pytest.mark.parametrize("names", [("a_reader", "z_ev"), ("z_reader", "a_ev")])
    def test_sibling_is_ordered_before_its_reader(self, names: tuple[str, str]) -> None:
        reader, sibling = names
        ordered = topologically_order_stages(_expression_list(reader, sibling))
        assert [s.name for s in ordered[:-1]] == [sibling, reader]

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_permutations_agree(self, dialect: str) -> None:
        stages = {"r": _reader("r", "z_ev"), "z_ev": _events("z_ev"),
                  "pc": q(name="pc", population="orders", dimensions=["customer_id"],
                          metrics=[m("sum(amount)", "revenue")])}
        root = over("r", dimensions=["tier"], metric="max(ev)", alias="ev_max")
        async with shop(dialect) as engine:
            results = {perm: await outcome(engine, [*(stages[n] for n in perm), root])
                       for perm in itertools.permutations(stages)}
        assert len(set(results.values())) == 1, results
        assert next(iter(results.values()))[0] == "ok"


class TestJoinGraphMembership:
    """Only siblings a stage names are route candidates."""

    def _datasets(self):
        return with_datasets(customers(to_regions=False), orders(ship_region=True))

    def _sib(self) -> SlayerQuery:
        return q(name="sib", population="orders", dimensions=["customer_id", "region_id"],
                 metrics=[m("sum(amount)", "revenue")])

    def _root(self) -> SlayerQuery:
        return q(population="customers", dimensions=["regions.name"], metrics=[m("count(*)", "n")])

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_routing_never_passes_through_an_unnamed_sibling(self, dialect: str) -> None:
        async with shop(dialect, self._datasets()) as engine:
            alone = await outcome(engine, self._root())
            with_sibling = await outcome(engine, [self._sib(), self._root()])
        assert with_sibling == alone

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_named_sibling_is_a_route_candidate(self, dialect: str) -> None:
        root = q(population="customers", dimensions=["name", "sib.ship_region.name"],
                 metrics=[m("count(*)", "n")])
        async with shop(dialect, self._datasets()) as engine:
            resp = await engine.execute([self._sib(), root])
        got = {(r["customers.name"], r["customers.sib.ship_region.name"]) for r in resp.data}
        assert got == {("Ann", "North"), ("Ann", "South"), ("Bob", "South"), ("Cid", "North"),
                       ("Dee", "East"), ("Eve", None)}


class TestEdgeAwareExtractor:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_a_relationship_named_like_a_sibling_is_not_a_sibling_reference(self, dialect: str) -> None:
        a = q(name="a", population="orders", dimensions=["ship_region.name"], metrics=[m("count(*)", "n")])
        sibling = q(name="ship_region", population="a", dimensions=["ship_region__name"], metrics=[m("sum(n)", "nn")])
        root = q(population="ship_region", metrics=[m("sum(nn)", "t")])
        async with shop(dialect, with_datasets(orders(ship_region=True))) as engine:
            resp = await engine.execute([a, sibling, root])
        assert [float(v) for v in resp.data[0].values()] == [7.0]

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_an_inherited_edge_reaches_its_sibling_target(self, dialect: str) -> None:
        totals = q(name="totals", population="orders", dimensions=["region_id"], metrics=[m("sum(amount)", "total")])
        keyed = q(name="keyed", population="orders", dimensions=["region_id"], metrics=[m("count(*)", "n")],
                  extensions=[ext("orders", join("totals", [("region_id", "region_id")], name="totals_link"))])
        root = q(population="keyed", dimensions=["region_id", "totals_link.total"], metrics=[m("sum(n)", "nn")])
        async with shop(dialect) as engine:
            resp = await engine.execute([totals, keyed, root])
        assert by(resp, key="region_id", value="totals_link.total") == {1: 22.0, 2: 50.0, 3: 40.0, None: None}

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_a_relationship_named_like_a_planned_sibling_is_no_recorded_read(
            self, dialect: str, monkeypatch: pytest.MonkeyPatch) -> None:
        planned: list[Any] = []
        real = query_engine_module.plan_stages

        def spy(*, queries, bundle):
            planned[:] = real(queries=queries, bundle=bundle)
            return planned

        monkeypatch.setattr(query_engine_module, "plan_stages", spy)
        sibling = q(name="ship_region", population="customers", metrics=[m("count(*)", "nc")])
        z = q(name="z", population="orders", dimensions=["ship_region.name"], metrics=[m("count(*)", "n")])
        async with shop(dialect, with_datasets(orders(ship_region=True))) as engine:
            await engine.execute([sibling, z, q(population="z", metrics=[m("sum(n)", "t")])], dry_run=True)
        (reader,) = [p for p in planned if p.stage_schema is not None and p.stage_schema.relation_name.endswith("_z")]
        assert reader.stage_reads == []

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_a_relationship_path_is_never_a_sibling_reference(self, dialect: str) -> None:
        a = q(name="a", population="orders", dimensions=["customers.regions.name"], metrics=[m("count(*)", "n")])
        sibling = q(name="regions", population="a", dimensions=["customers__regions__name"],
                    metrics=[m("sum(n)", "nn")])
        root = q(population="regions", metrics=[m("sum(nn)", "t")])
        async with shop(dialect) as engine:
            resp = await engine.execute([a, sibling, root])
        assert [float(v) for v in resp.data[0].values()] == [7.0]


class TestExpressionReadErrors:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_an_expression_self_reference_is_rejected_by_its_name(self, dialect: str) -> None:
        a = q(name="a", population="orders", dimensions=["status"], metrics=[m("count(*)", "n"), m("max(a.n)", "x")])
        stages: list[SlayerQuery | dict] = [a, q(population="a", metrics=[m("sum(n)", "t")])]
        async with shop(dialect) as engine:
            with pytest.raises(ValueError, match="Stage 'a' references itself"):
                await engine.execute(stages)

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_an_expression_cycle_names_the_stages_as_written(self, dialect: str) -> None:
        b = q(name="b", population="orders", dimensions=["status"], metrics=[m("max(c.n)", "x")])
        c = q(name="c", population="customers", dimensions=["tier"], metrics=[m("count(*)", "n"), m("max(b.x)", "y")])
        stages: list[SlayerQuery | dict] = [b, c, q(population="b", metrics=[m("sum(x)", "t")])]
        async with shop(dialect) as engine:
            with pytest.raises(ValueError, match=r"stages \['b', 'c'\]"):
                await engine.execute(stages)


class TestReferenceHeads:
    def test_a_relationship_hop_after_a_sibling_is_no_sibling_read(self) -> None:
        query = q(name="cur", population="events", metrics=[m("max(z_orders.customers.id)", "mx")])
        assert stage_sibling_reads(query=query, siblings={"cur", "z_orders", "customers"}) == {"z_orders"}

    def test_the_segment_after_a_root_qualifier_is_a_reference_head(self) -> None:
        query = q(name="cur", population="customers", metrics=[m("max(customers.z_ev.ne)", "mx")])
        assert stage_sibling_reads(query=query, siblings={"cur", "z_ev"}) == {"z_ev"}
