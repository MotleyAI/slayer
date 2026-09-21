"""DEV-1908 — reverse-hop cancellation for definition defaults.

A definition default whose qualifier names a dataset already on the query's path
to the owner cancels the path back to that dataset (`a.b.a ≡ a`), reading its row
there. Home paths come from `Aggregate.home_path`; executed values compare each
default to its explicit `weight=` twin AND the hand-derived oracle on SQLite and
DuckDB. A reverse hop to a dataset NOT on the path still fans and fails closed.
Cancellation cases fail on the current tree; the already-holding pins are marked.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import UnresolvableDimensionJoinError
from slayer.core.keys import AggregateKey
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine.elaborate import elaborate_query
from slayer.ir.terms import Aggregate

from tests._engine_helpers import _engine_generate, seeded_exec_engine
from tests._dev1908_fixtures import (
    ASSOC_EXPR,
    CANCEL_FORWARD_PLAN,
    CANCEL_FORWARD_POP,
    DOUBLE_CANCEL,
    EVENTS_CANCEL,
    ColumnRef,
    ORACLE_LITERALS,
    SECOND_ORDER,
    SELF,
    SINGLE_CANCEL,
    TWO_FRAME,
    WINDOW_EXPR,
    _seed_duckdb,
    _seed_sqlite,
    bundle1908,
    derive_oracles,
    dev1908_models,
    make_exec_engine,
    month_key,
    orders_q,
    ship_region_extension,
    signup_month_td,
)


def _cells(resp) -> dict:
    """`{dimension-value tuple: measure value}` over a response's rows."""
    return {tuple(v for k, v in sorted(r.items()) if k != "orders.w"): r["orders.w"]
            for r in resp.data}


def _home_path(formula: str, *, dimensions: list[str] | None = None) -> tuple:
    """`home_path` of a measure's outer aggregate over the DEV-1908 graph."""
    elab = elaborate_query(
        query=orders_q(dimensions=dimensions or [],
                       measures=[ModelMeasure(formula=formula, name="m")]),
        bundle=bundle1908())
    assert elab.prebound is not None
    root = elab.prebound.declared_measures[-1].bound.value_key
    assert isinstance(root, AggregateKey), root
    term = elab.terms[root]
    assert isinstance(term, Aggregate), term
    return term.home_path


async def _gen(formula: str, *, source_model="orders", **kw) -> str:
    models = dev1908_models()
    return await _engine_generate(
        query=SlayerQuery(source_model=source_model,
                          measures=[ModelMeasure(formula=formula, name="w")], **kw),
        model=models[0], extra_models=models[1:], dialect="sqlite", validate=False)


def test_oracles_re_derive_from_the_raw_rows():
    """Smoke: every oracle literal equals its independent re-derivation."""
    assert derive_oracles() == ORACLE_LITERALS


class TestHomePaths:
    """Cancelled defaults home at the dataset they cancel to (raise on current tree)."""

    def test_single_cancel_homes_at_customers_regions(self):
        assert _home_path(
            "customers.regions.countries.gdp:wsum_region_pop") == ("customers", "regions")

    def test_double_cancel_homes_at_customers(self):
        assert _home_path(
            "customers.regions.countries.gdp:wsum_cust_spend2") == ("customers",)

    def test_cancel_then_forward_deep_homes_at_customers(self):
        assert _home_path(
            "customers.regions.countries.gdp:wsum_plan_fee") == ("customers",)

    def test_two_frame_homes_at_customers(self):
        assert _home_path(
            "customers.regions.countries.gdp:wsum_two") == ("customers",)

    def test_events_cancel_homes_at_region_events(self):
        assert _home_path("customers.regions.region_events.value:wsum_rp") \
            == ("customers", "regions", "region_events")

    # --- pins that already hold (root-fallback / root-name resolution) ---
    def test_cancel_then_forward_shallow_homes_at_customers_pin(self):
        assert _home_path("customers.regions.pop:wsum_cust_plan_fee") == ("customers",)

    def test_owner_at_root_self_name_homes_at_root_pin(self):
        assert _home_path("amount:wself") == ()
        assert _home_path("amount:wself_expr") == ()


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


async def _value(engine, formula: str) -> float:
    resp = await engine.execute(
        orders_q(measures=[ModelMeasure(formula=formula, name="w")]))
    return float(resp.data[0]["orders.w"])


class TestExecutedValues:
    """default == explicit `weight=` twin == oracle, on SQLite and DuckDB."""

    async def test_single_cancel(self, engine):
        explicit = await _value(
            engine, "customers.regions.countries.gdp:wsum_region_pop"
            "(weight=customers.regions.pop)")
        assert explicit == pytest.approx(SINGLE_CANCEL)
        default = await _value(engine, "customers.regions.countries.gdp:wsum_region_pop")
        assert default == pytest.approx(explicit)

    async def test_double_cancel(self, engine):
        explicit = await _value(
            engine, "customers.regions.countries.gdp:wsum_cust_spend2"
            "(weight=customers.spend)")
        assert explicit == pytest.approx(DOUBLE_CANCEL)
        default = await _value(engine, "customers.regions.countries.gdp:wsum_cust_spend2")
        assert default == pytest.approx(explicit)

    async def test_cancel_then_forward_deep(self, engine):
        explicit = await _value(
            engine, "customers.regions.countries.gdp:wsum_plan_fee"
            "(weight=customers.plans.fee)")
        assert explicit == pytest.approx(CANCEL_FORWARD_PLAN)
        default = await _value(engine, "customers.regions.countries.gdp:wsum_plan_fee")
        assert default == pytest.approx(explicit)

    async def test_cancel_then_forward_shallow_pin(self, engine):
        explicit = await _value(
            engine, "customers.regions.pop:wsum_cust_plan_fee"
            "(weight=customers.plans.fee)")
        assert explicit == pytest.approx(CANCEL_FORWARD_POP)
        default = await _value(engine, "customers.regions.pop:wsum_cust_plan_fee")
        assert default == pytest.approx(explicit)

    async def test_two_frame(self, engine):
        explicit = await _value(
            engine, "customers.regions.countries.gdp:wsum_two"
            "(w1=customers.regions.pop, w2=customers.plans.fee)")
        assert explicit == pytest.approx(TWO_FRAME)
        default = await _value(engine, "customers.regions.countries.gdp:wsum_two")
        assert default == pytest.approx(explicit)

    async def test_events_cancel_over_a_to_one_reverse_hop(self, engine):
        # The default, the explicit twin AND the plain product all home at
        # region_events over the to-one reverse hop back to regions (D9).
        explicit = await _value(
            engine, "customers.regions.region_events.value:wsum_rp"
            "(weight=customers.regions.pop)")
        assert explicit == pytest.approx(EVENTS_CANCEL)
        default = await _value(engine, "customers.regions.region_events.value:wsum_rp")
        assert default == pytest.approx(explicit)
        plain = await _value(
            engine, "sum(customers.regions.region_events.value * customers.regions.pop)")
        assert plain == pytest.approx(EVENTS_CANCEL)

    async def test_owner_at_root_self_name_pin(self, engine):
        for agg in ("wself", "wself_expr"):
            explicit = await _value(engine, f"amount:{agg}(weight=cost)")
            assert explicit == pytest.approx(SELF)
            default = await _value(engine, f"amount:{agg}")
            assert default == pytest.approx(explicit)


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
async def test_ship_region_extension_edge_name_spelling_survives(dialect):
    # An inline ModelExtension names the regions hop `ship_region`; the default
    # cancels to that spelling (ship_region.pop), matching the explicit twin. Each
    # query runs on its own engine (an extension mutates the shared stored model).
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite

    async def run(formula: str) -> tuple[float, str]:
        async with seeded_exec_engine(
            dialect=dialect, seed=seed, models=dev1908_models()) as (e, _db):
            query = SlayerQuery(source_model=ship_region_extension(),
                                measures=[ModelMeasure(formula=formula, name="w")])
            resp = await e.execute(query)
            dry = await e.execute(query, dry_run=True)
            assert dry.sql is not None
            return float(resp.data[0]["orders.w"]), dry.sql

    ev, explicit_sql = await run(
        "ship_region.countries.gdp:wsum_region_pop(weight=ship_region.pop)")
    assert ev == pytest.approx(SINGLE_CANCEL)
    dv, default_sql = await run("ship_region.countries.gdp:wsum_region_pop")
    assert dv == pytest.approx(ev)
    # Spelling survives: the default reads pop through the ship_region-joined
    # regions, never a second `regions` join (which the current fan renders).
    assert default_sql.count("regions AS ") == explicit_sql.count("regions AS ")


class TestKernelExpressionDefaults:
    """A cancelled/root-frame expression default renders canonically in every
    producer kind — association, trailing-window, second-order."""

    async def test_association_expression_default(self, engine):
        # Each response's cells are read immediately (response data reflects the
        # engine's current state, so a later execute would otherwise mask it).
        base = "customers.regions.countries.gdp:wsum_region_pop_expr"
        default = _cells(await engine.execute(orders_q(
            dimensions=["status"], to_many_handling="associate",
            measures=[ModelMeasure(formula=base, name="w")])))
        explicit = _cells(await engine.execute(orders_q(
            dimensions=["status"], to_many_handling="associate",
            measures=[ModelMeasure(formula=f"{base}(weight=customers.regions.pop)",
                                   name="w")])))
        assert default == explicit
        assert {k[0]: float(v) for k, v in default.items()} == ASSOC_EXPR

    async def test_trailing_window_expression_default(self, engine):
        base = "customers.regions.countries.gdp:wsum_cust_spend2_expr"
        default = _cells(await engine.execute(orders_q(
            time_dimensions=signup_month_td(),
            measures=[ModelMeasure(formula=f"{base}(window='1y')", name="w")])))
        explicit = _cells(await engine.execute(orders_q(
            time_dimensions=signup_month_td(),
            measures=[ModelMeasure(
                formula=f"{base}(window='1y', weight=customers.spend)", name="w")])))
        assert default == explicit
        months = {month_key(k[0]): float(v) for k, v in default.items()
                  if k[0] is not None}
        assert months == WINDOW_EXPR

    async def test_second_order_root_frame_expression_default_pin(self, engine):
        inner = "sum(amount, partition_by=customers.regions.id)"
        default = await _value(engine, f"wavg_pop_expr({inner})")
        explicit = await _value(
            engine, f"wavg_pop_expr({inner}, weight=customers.regions.pop)")
        assert explicit == pytest.approx(SECOND_ORDER)
        assert default == pytest.approx(explicit)


class TestStageQueryUnaffected:
    async def test_two_stage_query_has_no_model_host(self, engine):
        """A later stage aggregating a stage's output resolves no definition
        default (a stage has no model host) — executes exactly as before."""
        stage1 = orders_q(name="stage1", dimensions=["status"],
                          measures=[ModelMeasure(formula="amount:sum", name="amount_sum")])
        root = SlayerQuery(source_model="stage1", dimensions=[ColumnRef(name="status")],
                           measures=[ModelMeasure(formula="amount_sum:max", name="mx")])
        resp = await engine.execute([stage1, root])
        vals = sorted(float(next(v for k, v in r.items() if "mx" in k))
                      for r in resp.data)
        assert vals == [82.0, 85.0]  # ok=82, new=85


class TestFailClosed:
    async def test_cancel_then_fanning_hop_names_region_events(self):
        with pytest.raises(ValueError, match="(?i)unproven join hop|fanning") as ei:
            await _gen("customers.regions.countries.gdp:wsum_fan")
        assert "region_events" in str(ei.value)

    async def test_cancel_then_miss_fails_closed(self):
        # D3: `regions` cancels, then `plans` misses (not a hop from regions) —
        # never re-anchored at the root.
        with pytest.raises(UnresolvableDimensionJoinError):
            await _gen("customers.regions.countries.gdp:wsum_cancel_miss")

    async def test_second_order_nowhere_default_raises_unresolvable(self):
        with pytest.raises(UnresolvableDimensionJoinError, match="(?i)nowhere"):
            await _gen("wnowhere(sum(amount, partition_by=status))")

    async def test_query_typed_revisit_raises_circular_join(self):
        # DEV-1952 owns the model-SQL derived-column revisit; only the query-typed
        # revisit is pinned here.
        with pytest.raises(ValueError, match="(?i)circular"):
            await _gen("customers.regions.customers.spend:sum")

    @pytest.mark.parametrize("formula", [
        "pop:wsum_cust_spend",
        "pop:wsum_cust_spend(weight=customers.spend)"])
    async def test_reverse_hop_not_on_the_path_stays_refused(self, formula):
        # Rooted at regions, `customers` is not on the path: nothing cancels, and
        # BOTH the default and its explicit twin fail with the same input-safety
        # error naming customers.
        with pytest.raises(ValueError, match="(?i)unproven join hop") as ei:
            await _gen(formula, source_model="regions")
        assert "customers" in str(ei.value)
