"""A resolved join path is spelled canonically (edge name, else model).

Graph: ``orders → customers →(hr) regions → region_events``; ``hr`` may be typed
as ``hr`` or ``regions``. Every resolution door emits the canonical ``hr`` spelling,
so keys, joins, homes and result keys are spelling-invariant.
"""

from __future__ import annotations

from typing import AsyncIterator, Dict

import pytest
import sqlglot
from sqlglot import exp

from slayer.core import relationship_walker
from slayer.core.enums import TimeGranularity
from slayer.core.errors import UnknownReferenceError
from slayer.core.relationship_walker import walk, walk_cancelling
from slayer.core.keys import AggregateKey, FieldKey, FieldSqlKey, StarKey, TimeTruncKey
from slayer.core.models import DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.core.scope import DatasetScope
from slayer.engine import bind_inputs, param_binding
from slayer.live_schema import schema_drift
from slayer.engine.binding import bind_expr, bind_time_dimension
from slayer.engine.extensions import QueryScope, query_scopes
from slayer.engine.cardinality.relationship_safety import attributable_from_root, reroot_from_root
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.core.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql import field_expansion

from tests._dev1836_fixtures import broadcast_warnings
from tests._dev1954_fixtures import (
    AMOUNT_BY_RNAME,
    EVENT_MAX_BY_RNAME,
    POP_MAX_BY_RNAME,
    RNAME,
    SPEND_BY_RNAME,
    WPOP_BY_RNAME,
    by_key,
    dev1954_datasets,
    execution_spy,
    make_exec_engine,
    orders_q,
    slayer_query,
    table_count,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_exec_engine(request):
        yield e


def _mbn(datasets=None) -> Dict[str, SlayerDataset]:
    return {m.name: m for m in (datasets or dev1954_datasets())}


def _bundle(root: str = "orders") -> ResolvedSourceBundle:
    models = dev1954_datasets()
    src = next(m for m in models if m.name == root)
    return ResolvedSourceBundle(
        dialect="postgres",
        population=src, referenced_datasets=[m for m in models if m.name != root])


def _scope(root: str = "orders") -> DatasetScope:
    return DatasetScope(population=_mbn()[root])


def _bind(text: str, *, root: str = "orders"):
    return bind_expr(parse_expr(text), scope=_scope(root), bundle=_bundle(root))


def _parallel_datasets():
    """customers → regions on two named edges ``hr`` and ``hr2``."""
    models = dev1954_datasets()
    customers = next(m for m in models if m.name == "customers")
    customers.relationships.append(DatasetRelationship(
        to="regions", from_columns=["region_id"], to_columns=["id"], name="hr2"))
    return models


async def _dry(engine: SlayerQueryEngine, query) -> str:
    sql = (await engine.execute(query, dry_run=True)).sql
    assert sql is not None
    return sql


async def _respell_orders(engine: SlayerQueryEngine, *, column: str, field: str,
                          value: str) -> None:
    """Re-save ``orders`` with ``column.field`` set to ``value``."""
    orders = await engine.storage.get_dataset("orders", data_source="test")
    assert orders is not None
    orders.fields = [c.model_copy(update={field: value}) if c.name == column else c
                      for c in orders.fields]
    await engine.storage.save_dataset(orders)


async def _outcome(engine: SlayerQueryEngine, query):
    """``(columns, sorted rows)`` or ``(error type, message)``."""
    try:
        resp = await engine.execute(query)
    except Exception as exc:  # noqa: BLE001 — parity compares the failure itself
        return type(exc).__name__, str(exc)
    return resp.columns, sorted(map(repr, resp.data))


# --------------------------------------------------------------------------- #
# The canonical helper and the cancelling walk.
# --------------------------------------------------------------------------- #
class TestCanonicalPathHelper:
    @pytest.mark.parametrize(("root", "path", "expected"), [
        ("customers", ("hr",), ("hr",)),
        ("customers", ("regions",), ("hr",)),
        ("regions", ("customers",), ("hr",)),
        ("orders", ("customers",), ("customers",)),
        ("region_events", ("regions",), ("regions",)),
        ("orders", ("customers", "regions", "region_events"),
         ("customers", "hr", "region_events")),
        ("orders", (), ()),
    ])
    def test_canonical_path(self, root, path, expected) -> None:
        M = _mbn()
        chain = walk(root=M[root], path=path, datasets_by_name=M)
        assert chain is not None
        assert relationship_walker.canonical_path(chain) == expected


class TestWalkCancellingIsCanonical:
    def test_model_name_hop_appends_the_edge_name(self) -> None:
        M = _mbn()
        assert walk_cancelling(root=M["orders"], owner_path=("customers",),
                               tokens=("regions",), datasets_by_name=M) == ("customers", "hr")

    def test_cancel_then_model_name_hop(self) -> None:
        M = _mbn()
        assert walk_cancelling(root=M["orders"], owner_path=("customers", "hr"),
                               tokens=("customers", "regions"),
                               datasets_by_name=M) == ("customers", "hr")

    def test_root_default_spelled_by_model_name(self) -> None:
        M = _mbn()
        assert walk_cancelling(root=M["orders"], owner_path=(),
                               tokens=("customers", "regions"),
                               datasets_by_name=M) == ("customers", "hr")


# --------------------------------------------------------------------------- #
# Binding.
# --------------------------------------------------------------------------- #
class TestBoundKeysAreCanonical:
    def test_column_key(self) -> None:
        assert _bind("customers.regions.rname").value_key == \
            FieldKey(path=("customers", "hr"), leaf="rname")

    def test_column_sql_key(self) -> None:
        assert _bind("customers.regions.bad_pop").value_key == FieldSqlKey(
            path=("customers", "hr"), dataset="regions", field_name="bad_pop")

    def test_star_key(self) -> None:
        key = _bind("count(customers.regions.*)").value_key
        assert isinstance(key, AggregateKey)
        assert key.source == StarKey(path=("customers", "hr"))

    def test_time_trunc_key(self) -> None:
        bound = bind_time_dimension(
            TimeDimension(field="customers.regions.founded_at",
                          granularity=TimeGranularity.YEAR),
            scope=_scope(), bundle=_bundle())
        assert isinstance(bound.bound.value_key, TimeTruncKey)
        assert bound.bound.value_key.source == \
            FieldKey(path=("customers", "hr"), leaf="founded_at")
        assert bound.bound.routed_dotted == "customers.hr.founded_at"

    def test_respelled_dimension_names_the_canonical_path(self) -> None:
        assert _bind("customers.regions.rname").routed_dotted == "customers.hr.rname"

    def test_auto_routed_short_form(self) -> None:
        bound = _bind("regions.rname")
        assert bound.value_key == FieldKey(path=("customers", "hr"), leaf="rname")
        assert bound.routed_dotted == "customers.hr.rname"

    def test_reverse_named_hop(self) -> None:
        assert _bind("customers.spend", root="regions").value_key == \
            FieldKey(path=("hr",), leaf="spend")

    def test_canonical_spellings_unchanged(self) -> None:
        assert _bind("customers.hr.rname").routed_dotted is None
        bound = _bind("customers.tier")
        assert bound.value_key == FieldKey(path=("customers",), leaf="tier")
        assert bound.routed_dotted is None


class TestSavedMeasureRefIsCanonical:
    @pytest.mark.parametrize("formula", [
        "customers.regions.maxpop", "customers.hr.maxpop", "regions.maxpop"])
    def test_canonical_ref(self, formula) -> None:
        ref = bind_inputs._resolve_saved_metric_ref(  # noqa: SLF001
            scope=_scope(), bundle=_bundle(), expression=formula)
        assert ref is not None
        assert ref[2] == "customers.hr.maxpop"


# --------------------------------------------------------------------------- #
# Mode-A qualifier doors.
# --------------------------------------------------------------------------- #
class TestModeAQualifiersAreCanonical:
    def test_strict_qualifiers(self) -> None:
        M = _mbn()
        assert field_expansion._resolve_qualifiers(  # noqa: SLF001
            qualifiers=("customers", "regions"), leaf="rname",
            source_dataset=M["orders"], owner_alias="orders",
            datasets_by_name=M) == ("customers", "hr")

    @pytest.mark.parametrize("quals", [
        ("customers", "regions"), ("customers", "hr"),
        ("customers__regions",), ("customers__hr",)])
    def test_lenient_path_walked_forms(self, quals) -> None:
        M = _mbn()
        assert field_expansion._lenient_path(  # noqa: SLF001
            qualifiers=quals, source_dataset=M["orders"], owner_alias="orders",
            datasets_by_name=M) == ("customers", "hr")

    def test_lenient_path_leaves_a_fitted_alias_opaque(self) -> None:
        M = _mbn()
        assert field_expansion._lenient_path(  # noqa: SLF001
            qualifiers=("customers__regions_1a2b",), source_dataset=M["orders"],
            owner_alias="orders", datasets_by_name=M) is None

    def test_default_qualifier_path(self) -> None:
        M = _mbn()
        assert param_binding.resolve_default_qualifier_path(
            qualifiers=("customers", "regions"), leaf="pop",
            root_dataset=M["orders"], owner_path=(), datasets_by_name=M,
        ) == ("customers", "hr")

    def test_default_reference_paths(self) -> None:
        M = _mbn()
        parsed = sqlglot.parse_one("customers.regions.pop * 1")
        assert isinstance(parsed, exp.Expression)  # pyright: ignore[reportPrivateImportUsage] — sqlglot ships no __all__
        assert param_binding.resolve_default_reference_paths(
            parsed=parsed, owner_path=(), root_dataset=M["orders"], root_path=(),
            bundle=_bundle(),
        ) == [(("customers", "hr"), "pop")]


# --------------------------------------------------------------------------- #
# End to end — the join-traversal scenarios.
# --------------------------------------------------------------------------- #
class TestNamedEdgeTypedByModelName:
    async def test_result_keys_follow_the_edge_name(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["customers.regions.rname"],
            metrics=["max(customers.regions.pop)"]))
        assert resp.columns == [RNAME, "customers.hr.pop_max"]
        assert by_key(resp, RNAME, "customers.hr.pop_max") == POP_MAX_BY_RNAME
        assert not broadcast_warnings(resp)

    async def test_metadata_matches_the_edge_name_spelling(self, engine: SlayerQueryEngine) -> None:
        typed = await engine.execute(orders_q(
            dimensions=["customers.regions.rname"],
            metrics=["max(customers.regions.pop)"]))
        canon = await engine.execute(orders_q(
            dimensions=["customers.hr.rname"], metrics=["max(customers.hr.pop)"]))
        assert typed.attributes == canon.attributes
        assert typed.attributes.dimensions[RNAME].label == "Region name"
        assert "customers.hr.pop_max" in typed.attributes.metrics

    async def test_named_edge_time_dimension(self, engine: SlayerQueryEngine) -> None:
        query = orders_q(
            time_dimensions=[TimeDimension(
                field="customers.regions.founded_at",
                granularity=TimeGranularity.YEAR)],
            metrics=["sum(orders.amount)"],
            order=[{"field": "customers.regions.founded_at", "direction": "asc"}])
        resp = await engine.execute(query)
        key = "customers.hr.founded_at"
        assert resp.columns == [key, "orders.amount_sum"]
        assert resp.attributes.dimensions[key].label == "Founded"
        years = [str(r[key])[:4] for r in resp.data if r[key] is not None]
        assert years == ["2020", "2021"]


class TestMixedSpellingsJoinOnce:
    async def test_filter_and_dimension_share_one_join(self, engine) -> None:
        mixed = orders_q(dimensions=["customers.hr.rname"], metrics=["sum(orders.amount)"],
                         filter=["customers.regions.rname = 'North'"])
        canon = orders_q(dimensions=["customers.hr.rname"], metrics=["sum(orders.amount)"],
                         filter=["customers.hr.rname = 'North'"])
        sql = await _dry(engine, mixed)
        assert table_count(sql, "regions") == 1
        assert sql == await _dry(engine, canon)
        resp = await engine.execute(mixed)
        assert by_key(resp, RNAME, "orders.amount_sum") == {"North": 100.0}

    async def test_order_by_the_other_spelling(self, engine) -> None:
        mixed = orders_q(dimensions=["customers.hr.rname"], metrics=["sum(orders.amount)"],
                         order=[{"field": "customers.regions.rname",
                                 "direction": "desc"}])
        canon = orders_q(dimensions=["customers.hr.rname"], metrics=["sum(orders.amount)"],
                         order=[{"field": "customers.hr.rname", "direction": "desc"}])
        assert await _dry(engine, mixed) == await _dry(engine, canon)

    async def test_aggregate_filter_interns_to_the_selected_slot(self, engine) -> None:
        mixed = orders_q(dimensions=["customers.hr.rname"],
                         metrics=["max(customers.hr.pop)"],
                         filter=["max(customers.regions.pop) > 150"])
        canon = orders_q(dimensions=["customers.hr.rname"],
                         metrics=["max(customers.hr.pop)"],
                         filter=["max(customers.hr.pop) > 150"])
        assert await _dry(engine, mixed) == await _dry(engine, canon)
        resp = await engine.execute(mixed)
        assert by_key(resp, RNAME, "customers.hr.pop_max") == {"South": 200.0}

    async def test_functional_order_on_a_time_dimension(self, engine) -> None:
        td = [TimeDimension(field="customers.hr.founded_at",
                            granularity=TimeGranularity.YEAR)]
        mixed = orders_q(time_dimensions=td, metrics=["sum(orders.amount)"], order=[
            {"field": "max(customers.regions.founded_at)", "direction": "desc"}])
        canon = orders_q(time_dimensions=td, metrics=["sum(orders.amount)"], order=[
            {"field": "max(customers.hr.founded_at)", "direction": "desc"}])
        assert await _dry(engine, mixed) == await _dry(engine, canon)
        assert not broadcast_warnings(await engine.execute(mixed))


class TestAggregateGroupedThroughTheOtherSpelling:
    async def test_measure_is_per_region(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["customers.hr.rname"], metrics=["max(customers.regions.pop)"]))
        assert by_key(resp, RNAME, "customers.hr.pop_max") == POP_MAX_BY_RNAME
        assert not broadcast_warnings(resp)


class TestReverseHopRouteUnderDivergentSpelling:
    async def test_reverse_suffix_determines(self, engine) -> None:
        key = "customers.hr.region_events.value_max"
        mixed = orders_q(dimensions=["customers.regions.rname"],
                         metrics=["max(customers.hr.region_events.value)"])
        canon = orders_q(dimensions=["customers.hr.rname"],
                         metrics=["max(customers.hr.region_events.value)"])
        resp = await engine.execute(mixed)
        assert by_key(resp, RNAME, key) == EVENT_MAX_BY_RNAME
        assert not broadcast_warnings(resp)
        assert await _dry(engine, mixed) == await _dry(engine, canon)


class TestTwoSpellingsEqualADuplicate:
    async def test_parity_with_the_identical_dimension_twice(self, engine) -> None:
        mixed = orders_q(dimensions=["customers.hr.rname", "customers.regions.rname"])
        dup = orders_q(dimensions=["customers.hr.rname", "customers.hr.rname"])
        assert await _outcome(engine, mixed) == await _outcome(engine, dup)

    async def test_parity_for_a_measure(self, engine) -> None:
        mixed = orders_q(metrics=["max(customers.hr.pop)", "max(customers.regions.pop)"])
        dup = orders_q(metrics=["max(customers.hr.pop)", "max(customers.hr.pop)"])
        assert await _outcome(engine, mixed) == await _outcome(engine, dup)


class TestModelSqlSpelledByModelName:
    async def test_column_sql_joins_once(self, engine) -> None:
        query = orders_q(dimensions=["orders.region_label", "customers.hr.rname"])
        sql = await _dry(engine, query)
        assert table_count(sql, "regions") == 1
        resp = await engine.execute(query)
        assert {(r["orders.region_label"], r[RNAME]) for r in resp.data} == \
            {("North", "North"), ("South", "South"), (None, None)}

    async def test_model_filter_joins_once(self, engine: SlayerQueryEngine) -> None:
        orders = await engine.storage.get_dataset("orders", data_source="test")
        assert orders is not None
        orders.filters = [*orders.filters, "customers.regions.pop > 0"]
        await engine.storage.save_dataset(orders)
        query = orders_q(dimensions=["customers.hr.rname"], metrics=["sum(orders.amount)"])
        sql = await _dry(engine, query)
        # Once inside orders' filtered dataset, once for the dimension.
        assert table_count(sql, "regions") == 2
        resp = await engine.execute(query)
        assert by_key(resp, RNAME, "orders.amount_sum") == \
            {"North": 100.0, "South": 20.0}

    async def test_column_filter_joins_once(self, engine) -> None:
        query = orders_q(dimensions=["customers.hr.rname"],
                         metrics=["sum(orders.north_amount)"])
        sql = await _dry(engine, query)
        assert "customers__regions" not in sql
        resp = await engine.execute(query)
        assert by_key(resp, RNAME, "orders.north_amount_sum") == \
            {"North": 100.0, "South": None, None: None}
        await _respell_orders(engine, column="north_amount", field="filter",
                              value="customers.hr.rname = 'North'")
        assert sql == await _dry(engine, query)

    async def test_persisted_model_keeps_its_spelling(self, engine: SlayerQueryEngine) -> None:
        await engine.execute(orders_q(dimensions=["orders.region_label"]))
        orders = await engine.storage.get_dataset("orders", data_source="test")
        assert orders is not None
        label = next(c for c in orders.fields if c.name == "region_label")
        assert label.sql == "customers.regions.rname"


class TestDefinitionDefaultSpelledByModelName:
    async def test_default_equals_the_edge_name_spelling(self, engine: SlayerQueryEngine) -> None:
        default = orders_q(dimensions=["customers.hr.rname"],
                           metrics=[{"metric": "wpop(orders.amount)", "alias": "w"}])
        explicit = orders_q(dimensions=["customers.hr.rname"], metrics=[
            {"metric": "wpop(orders.amount, weight=customers.hr.pop)", "alias": "w"}])
        resp = await engine.execute(default)
        assert by_key(resp, RNAME, "w") == WPOP_BY_RNAME
        assert not broadcast_warnings(resp)
        assert by_key(await engine.execute(explicit), RNAME, "w") == WPOP_BY_RNAME
        sql = await _dry(engine, default)
        assert sql == await _dry(engine, explicit)
        orders = await engine.storage.get_dataset("orders", data_source="test")
        assert orders is not None
        wpop = next(a for a in orders.aggregations if a.name == "wpop")
        wpop.params = [p.model_copy(update={"sql": "customers.hr.pop"}) for p in wpop.params]
        await engine.storage.save_dataset(orders)
        assert sql == await _dry(engine, default)


class TestAutoRoutedShortForm:
    async def test_result_key_is_canonical(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["regions.rname"], metrics=["sum(orders.amount)"]))
        assert resp.columns == [RNAME, "orders.amount_sum"]
        assert by_key(resp, RNAME, "orders.amount_sum") == AMOUNT_BY_RNAME


class TestSavedMeasures:
    @pytest.mark.parametrize("formula", [
        "customers.regions.maxpop", "orders.customers.regions.maxpop",
        "regions.maxpop"])
    async def test_surfaces_under_the_canonical_path(self, engine: SlayerQueryEngine, formula) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["customers.hr.rname"], metrics=[formula]))
        key = "orders.customers.hr.maxpop"
        assert resp.columns == [RNAME, key]
        assert by_key(resp, RNAME, key) == POP_MAX_BY_RNAME
        assert key in resp.attributes.metrics
        assert not broadcast_warnings(resp)

    async def test_explicit_name_wins(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["customers.hr.rname"],
            metrics=[{"metric": "customers.regions.maxpop", "alias": "m"}]))
        assert by_key(resp, RNAME, "m") == POP_MAX_BY_RNAME

    async def test_bare_measure_with_a_model_name_body(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["customers.hr.rname"], metrics=["orders.rp"]))
        assert by_key(resp, RNAME, "orders.rp") == POP_MAX_BY_RNAME
        assert not broadcast_warnings(resp)

    async def test_reverse_hop(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(slayer_query(
            population="regions", dimensions=["regions.rname"],
            metrics=["customers.tot_spend"]))
        assert resp.columns == ["regions.rname", "customers.tot_spend"]
        assert by_key(resp, "regions.rname", "customers.tot_spend") == SPEND_BY_RNAME


class TestSpellingTwinsShareOneCachedResult:
    async def test_one_db_execution(self, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch) -> None:
        first = await engine.execute(orders_q(
            dimensions=["customers.regions.rname"], metrics=["sum(orders.amount)"]),
            cache=True)
        calls = execution_spy(monkeypatch)
        second = await engine.execute(orders_q(
            dimensions=["customers.hr.rname"], metrics=["sum(orders.amount)"]), cache=True)
        assert calls == []
        assert engine.cache_size == 1
        assert first.columns == second.columns
        assert first.data == second.data
        assert first.attributes == second.attributes


# --------------------------------------------------------------------------- #
# Pins: canonical spellings, errors, lookup-only consumers, the D9 route.
# --------------------------------------------------------------------------- #
class TestCanonicalSpellingsUnchanged:
    async def test_self_prefixed_and_unnamed_paths(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["orders.customers.hr.rname", "customers.tier"]))
        assert resp.columns == [RNAME, "customers.tier"]

    async def test_error_quotes_the_typed_spelling(self, engine) -> None:
        query = orders_q(dimensions=["customers.regions.nope"])
        with pytest.raises(UnknownReferenceError, match="customers.regions.nope"):
            await engine.execute(query)



def _stage_scope(stage: SlayerQuery, datasets_by_name: dict) -> QueryScope:
    return query_scopes([stage], datasets_by_ds={"ds": datasets_by_name}, data_source="ds")[0][0]


class TestSchemaDriftAttribution:
    @pytest.mark.parametrize("ref", [
        "customers.regions.rname", "customers.hr.rname", "regions.rname"])
    def test_every_spelling_attributes_to_regions(self, ref) -> None:
        M = _mbn()
        graph = _stage_scope(orders_q(), M)
        assert schema_drift._attribute_ref_to_base(  # noqa: SLF001
            ref=ref, base_name="regions", scope=graph) == "rname"

    @pytest.mark.parametrize("ref", ["customers.regions.rname", "regions.rname"])
    def test_ambiguous_attributes_to_nothing(self, ref) -> None:
        M = _mbn(_parallel_datasets())
        graph = _stage_scope(orders_q(), M)
        assert schema_drift._attribute_ref_to_base(  # noqa: SLF001
            ref=ref, base_name="regions", scope=graph) is None


class TestDeterminationRouteOnCanonicalPaths:
    def test_issue_repro_with_canonical_spelling(self) -> None:
        M = _mbn()
        assert attributable_from_root(
            host_path=("customers", "hr"), target_path=("customers", "hr"),
            root_dataset=M["regions"], datasets_by_name=M, host_name="orders") is True

    def test_to_one_reverse_suffix_reroots(self) -> None:
        M = _mbn()
        target = ("customers", "hr", "region_events")
        assert attributable_from_root(
            host_path=("customers", "hr"), target_path=target,
            root_dataset=M["region_events"], datasets_by_name=M, host_name="orders") is True
        assert reroot_from_root(
            FieldKey(path=("customers", "hr"), leaf="pop"), target_path=target,
            root_dataset=M["region_events"], datasets_by_name=M, host_name="orders",
        ) == FieldKey(path=("regions",), leaf="pop")
