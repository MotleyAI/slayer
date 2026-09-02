"""DEV-1838 stage 1 — producer interning (design D3) + warning pins (D6).

Spec: openspec …/specs/queries/partitioned-aggregates — "Structurally identical
producers render once" (the cross-scope extension). A producer needed by
several scopes — including a nested sub-plan AND the top level — is ONE shared
relation; producers differing in any part of their spec stay separate; sharing
never changes response warnings.

Feature-missing today: the shared-producer asserts (the ``_2`` twin exists)
and the D3 identity API. The executed values and warning pins are green today
and must stay green through interning.
"""

from __future__ import annotations

import pytest

from slayer.engine import stage_planner
from slayer.engine.source_bundle import ResolvedSourceBundle

from tests._dev1838_fixtures import (
    BAND,
    BAND_WM,
    ModelMeasure,
    SPEND_BAND,
    cte_aliases,
    dev1838_models,
    dropped_filter_warnings,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
)

M = ModelMeasure(formula="amount:sum", name="m")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _cm_count(sql: str, dialect: str) -> int:
    """Top-level producer count by the D3-pinned uniform ``_cm_`` prefix —
    deliberately name-scheme-agnostic beyond the prefix (class-(b) renames
    re-bless goldens, not these counts)."""
    return len(cte_aliases(sql, "_cm_", dialect=dialect))


def _band_wm_query(*windows: str, filters=None):
    measures = [M] + [
        ModelMeasure(formula=f"amount:sum(window='{w}')", name=f"w{w[:-1]}")
        for w in windows
    ]
    kw = {"filters": filters} if filters else {}
    return q(dimensions=["region", BAND], time_dimensions=month_td(),
             measures=measures, **kw)


class TestSharedAcrossScopes:
    async def test_nested_and_top_level_share_one_city_producer(
        self, exec_backend,
    ) -> None:
        """The flagship (DEV-1835 carry-over): band × wm needs the city totals
        at the top level AND inside the windowed producer's sub-plan — one
        relation, no ``_2`` twin, executed values unchanged."""
        dialect, engine = exec_backend
        query = _band_wm_query("1y")
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], int(r["orders.band"]),
             month_key(r["orders.ordered_at"])):
                (float(r["orders.m"]), float(r["orders.w1"]))
            for r in resp.data
        }
        assert set(got) == set(BAND_WM)
        for key, (m, w) in BAND_WM.items():
            assert got[key][0] == pytest.approx(m), key
            assert got[key][1] == pytest.approx(w), key
        dry = await engine.execute(query, dry_run=True)
        # One city producer + one windowed producer — no ``_2`` twin.
        assert _cm_count(dry.sql, dialect) == 2, cte_aliases(
            dry.sql, "_cm_", dialect=dialect,
        )

    async def test_two_nested_scopes_and_top_level_share_one_producer(
        self, exec_backend,
    ) -> None:
        """Consumers at three depths/coordinates (base row join on city; each
        windowed producer joins on region/band/month) keep their own attach
        coordinates while sharing the one city relation."""
        dialect, engine = exec_backend
        query = _band_wm_query("90d", "45d")
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], int(r["orders.band"]),
             month_key(r["orders.ordered_at"])): r
            for r in resp.data
        }
        cell = got[("North", 1, "2024-02")]
        assert float(cell["orders.w90"]) == pytest.approx(60.0)
        # 45d back from Feb's end (~Jan 15) drops the Jan 10 row.
        assert float(cell["orders.w45"]) == pytest.approx(50.0)
        dry = await engine.execute(query, dry_run=True)
        # One shared city producer + the two windowed producers.
        assert _cm_count(dry.sql, dialect) == 3, cte_aliases(
            dry.sql, "_cm_", dialect=dialect,
        )

    async def test_inherited_filter_shared_by_all_scopes_interns(
        self, exec_backend,
    ) -> None:
        """A row filter inherited identically into every scope's city producer
        (frame bound + population conjunct): the bodies are identical, so one
        relation serves both scopes; ``_src`` alone carries the frame-bound
        rewrite."""
        dialect, engine = exec_backend
        query = _band_wm_query(
            "90d", filters=["ordered_at >= '2024-02-01' and status = 'ok'"],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], int(r["orders.band"]),
             month_key(r["orders.ordered_at"])): float(r["orders.w90"])
            for r in resp.data
        }
        assert got[("North", 1, "2024-02")] == pytest.approx(30.0)
        dry = await engine.execute(query, dry_run=True)
        assert _cm_count(dry.sql, dialect) == 2, cte_aliases(
            dry.sql, "_cm_", dialect=dialect,
        )

    async def test_same_aggregate_in_both_roles_shares_one_producer(
        self, exec_backend,
    ) -> None:
        """Green pin — the cross-model dual-role shape (per-tier spend banded
        AND selected, tier in the grain) already shares one producer; interning
        must keep it that way with both roles' values correct."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier", {"expression": SPEND_BAND,
                                           "name": "sband"}],
            measures=[M, ModelMeasure(
                formula="customers.spend:sum(partition_by=customers.tier)",
                name="rt")],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.customers.tier"], r["orders.sband"]):
                (float(r["orders.m"]),
                 None if r["orders.rt"] is None else float(r["orders.rt"]))
            for r in resp.data
        }
        assert got == {
            ("gold", "lo"): (30.0, 100.0), ("silver", "hi"): (40.0, 210.0),
            ("bronze", "lo"): (40.0, 40.0), (None, "lo"): (7.0, None),
        }
        dry = await engine.execute(query, dry_run=True)
        assert _cm_count(dry.sql, dialect) == 1, cte_aliases(
            dry.sql, "_cm_", dialect=dialect,
        )

    async def test_dual_role_without_partition_key_in_grain_unsupported(
        self,
    ) -> None:
        """Keyless-grain dual-role (partition key absent from the query
        dimensions) has no host slot to join the producer back on; pinned
        unsupported pending its own issue."""
        query = q(
            dimensions=[{"expression": SPEND_BAND, "name": "sband"}],
            measures=[ModelMeasure(
                formula="customers.spend:sum(partition_by=customers.tier)",
                name="rt",
            )],
        )
        with pytest.raises(RuntimeError, match="missing a host / producer"):
            await gen(query)


class TestProducersThatMustStaySeparate:
    async def test_different_window_durations_stay_separate(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        dry = await engine.execute(_band_wm_query("90d", "45d"), dry_run=True)
        # One shared city producer + one windowed producer per duration.
        assert _cm_count(dry.sql, dialect) == 3, cte_aliases(
            dry.sql, "_cm_", dialect=dialect,
        )

    async def test_different_ranking_columns_stay_separate(
        self, exec_backend,
    ) -> None:
        """Default ``ordered_at`` vs an explicit joined ranking column: two
        producers, distinct values (green pin)."""
        dialect, engine = exec_backend
        query = q(dimensions=["status"], measures=[
            ModelMeasure(formula="amount:last", name="lb"),
            ModelMeasure(formula="amount:last(customers.signup_at)", name="ls"),
        ])
        resp = await engine.execute(query)
        by = {r["orders.status"]: r for r in resp.data}
        assert float(by["ok"]["orders.lb"]) == pytest.approx(7.0)
        assert float(by["ok"]["orders.ls"]) == pytest.approx(5.0)
        dry = await engine.execute(query, dry_run=True)
        assert _cm_count(dry.sql, dialect) == 2, cte_aliases(
            dry.sql, "_cm_", dialect=dialect,
        )

    async def test_different_measure_filters_stay_separate(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", BAND], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(window='90d')", name="w"),
                ModelMeasure(formula="ok_amount:sum(window='90d')", name="wok"),
            ],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], int(r["orders.band"]),
             month_key(r["orders.ordered_at"])): r
            for r in resp.data
        }
        cell = got[("North", 1, "2024-01")]
        assert float(cell["orders.w"]) == pytest.approx(30.0)
        # ok_amount drops the Jan 20 'new' row.
        assert float(cell["orders.wok"]) == pytest.approx(10.0)
        dry = await engine.execute(query, dry_run=True)
        # One shared city producer + one windowed producer per source column.
        assert _cm_count(dry.sql, dialect) == 3, cte_aliases(
            dry.sql, "_cm_", dialect=dialect,
        )

    def test_identity_separates_differing_inherited_filter_context(self) -> None:
        """D3 negative pin at the identity layer: two structurally equal
        producer bodies whose plans differ only in an inherited row-filter
        conjunct must carry different interning identities. (The renderer
        cannot produce this divergence inside one stage today, so the pin
        lives on the identity function itself.)"""
        identity = getattr(stage_planner, "regroup_producer_identity", None)
        assert identity is not None, (
            "DEV-1838 D3: stage_planner.regroup_producer_identity is not "
            "implemented yet"
        )
        models = dev1838_models()
        bundle = ResolvedSourceBundle(
            source_model=models[0], referenced_models=models[1:],
        )
        base = stage_planner.plan_query(query=q(
            dimensions=["region", BAND],
            measures=[M],
        ), bundle=bundle)
        filtered = stage_planner.plan_query(query=q(
            dimensions=["region", BAND],
            measures=[M], filters=["status = 'ok'"],
        ), bundle=bundle)
        (attach_a,) = base.regroup_attach_plans
        (attach_b,) = filtered.regroup_attach_plans
        assert identity(attach_a) != identity(attach_b)
        # Determinism: the same attach yields the same identity twice.
        first = identity(attach_a)
        again = identity(attach_a)
        assert first == again


class TestWarningsUnchangedBySharing:
    """D6 — warnings are keyed by semantic event, never by producer identity;
    interning must neither drop nor double-surface them (green pins)."""

    async def test_dropped_filter_warns_once_across_producers(
        self, exec_backend,
    ) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=[{"expression": SPEND_BAND, "name": "sband"}],
            measures=[M, ModelMeasure(formula="customers.spend:sum", name="cm")],
            filters=["city = 'CityA'"],
        ))
        dropped = dropped_filter_warnings(resp)
        assert len(dropped) == 1, [w.filter_text for w in dropped]
        assert "city" in dropped[0].filter_text
        (row,) = resp.data
        assert row["orders.sband"] == "lo"
        assert float(row["orders.m"]) == pytest.approx(30.0)
        assert float(row["orders.cm"]) == pytest.approx(140.0)

    async def test_shared_producer_dropped_filter_warns_once(
        self, exec_backend,
    ) -> None:
        """The dual-role shape's ONE producer, consumed by both roles, drops
        the host filter: exactly one warning — never one per consumer."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier", {"expression": SPEND_BAND,
                                           "name": "sband"}],
            measures=[M, ModelMeasure(
                formula="customers.spend:sum(partition_by=customers.tier)",
                name="rt")],
            filters=["city = 'CityA'"],
        ))
        dropped = dropped_filter_warnings(resp)
        assert len(dropped) == 1, [w.filter_text for w in dropped]
        (row,) = resp.data
        assert (row["orders.customers.tier"], row["orders.sband"]) == (
            "gold", "lo",
        )
        assert float(row["orders.m"]) == pytest.approx(30.0)
        assert float(row["orders.rt"]) == pytest.approx(100.0)

    async def test_shared_producer_shape_carries_no_spurious_warning(
        self, exec_backend,
    ) -> None:
        _, engine = exec_backend
        resp = await engine.execute(
            _band_wm_query("1y", filters=["status = 'ok'"]),
        )
        assert not resp.warnings, [
            getattr(w, "kind", "?") for w in resp.warnings
        ]
