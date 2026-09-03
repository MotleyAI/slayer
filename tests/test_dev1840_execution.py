"""DEV-1840 task 1.4 — executed semi-join pushdown values (SQLite + DuckDB).

Spec: openspec …/specs/queries/cross-model-aggregates — "Producer filter
inheritance". Every oracle is hand-computed in ``tests/_dev1840_fixtures.py``;
each defect value the dataset can distinguish (join fan-out, split-EXISTS,
single-pair correlation, inline-through-1:N) is asserted against explicitly.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1840_fixtures import (
    ModelMeasure,
    RENT_APP,
    RENT_APP_GOLD_SAMEROW,
    RENT_APP_GOLD_SPLIT_DEFECT,
    RENT_SINGLE_PAIR_DEFECT,
    SPEND_ALL_BY_TIER,
    SPEND_APP_1Y_BY_SIGNUP_MONTH,
    SPEND_APP_BY_TIER,
    SPEND_APP_FIRST_BY_TIER,
    SPEND_APP_LAST_BY_TIER,
    SPEND_APP_TOTAL,
    SPEND_BAND_170,
    SPEND_BASIC_BY_TIER,
    SPEND_CORRELATED_GT,
    SPEND_CUST_TIER_GOLD,
    SPEND_LAST_STATUS_OK,
    SPEND_LAST_STATUS_OK_INLINE_FAN_GOLD,
    SPEND_NEW_1Y_BY_SIGNUP_MONTH,
    SPEND_OK_APP_SAMEROW,
    SPEND_OK_APP_SPLIT_DEFECT,
    SPEND_WEB_AND_BASIC,
    SPEND_WEB_BY_TIER,
    dev1840_models,
    dropped_filter_warnings,
    make_exec_engine,
    month_key,
    q,
    rows_by,
    signup_month_td,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_weak(request):
    async for engine in make_exec_engine(
        request, models=dev1840_models(strong_plans=False),
    ):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_rev(request):
    async for engine in make_exec_engine(
        request, models=dev1840_models(declare_reverse=True),
    ):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")
RM = ModelMeasure(formula="stores.rent:sum", name="rm")


async def _dry_has_exists_and_closes(engine, query, dialect) -> None:
    dry = await engine.execute(query, dry_run=True)
    assert "EXISTS" in dry.sql.upper(), dry.sql
    assert "__regroup__" not in dry.sql
    assert_scope_closed(dry.sql, dialect=dialect)


class TestReverseHopPushdown:
    async def test_pushdown_restricts_population_not_cardinality(
        self, exec_backend,
    ):
        """Scenario: unsafe filter no longer fans out the producer."""
        dialect, engine = exec_backend
        query = q(dimensions=["customers.tier"], measures=[M, CM],
                  filters=["channel = 'app'"])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        for tier, spend in SPEND_APP_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier
        # Not the unfiltered slice, and never the join-multiplied value.
        assert float(by[("gold",)]["orders.cm"]) != pytest.approx(
            SPEND_ALL_BY_TIER["gold"],
        )
        # The metric's presence leaves the spine untouched.
        control = await engine.execute(q(
            dimensions=["customers.tier"], measures=[M],
            filters=["channel = 'app'"],
        ))
        control_by = rows_by(control, "orders.customers.tier")
        assert set(control_by) == set(by)
        for key, row in by.items():
            assert row["orders.m"] == control_by[key]["orders.m"], key
        assert dropped_filter_warnings(resp) == []
        await _dry_has_exists_and_closes(engine, query, dialect)

    async def test_zero_passing_customers_leave_a_null_group_value(
        self, exec_backend,
    ):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[M, CM],
            filters=["channel = 'web'"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",), ("bronze",), (None,)}
        for tier, spend in SPEND_WEB_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier
        # No NULL-tier customer has a web order: the attach finds no row.
        assert by[(None,)]["orders.cm"] is None
        assert float(by[(None,)]["orders.m"]) == pytest.approx(7.0)

    async def test_pushed_filter_still_restricts_result_rows(self, exec_backend):
        """Scenario: pushed filter still restricts the result rows."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"], measures=[M, CM],
            filters=["channel = 'app'"],
        ))
        by = rows_by(resp, "orders.status")
        assert set(by) == {("ok",), ("new",)}
        assert float(by[("ok",)]["orders.m"]) == pytest.approx(20.0)
        assert float(by[("new",)]["orders.m"]) == pytest.approx(45.0)
        for row in resp.data:
            assert float(row["orders.cm"]) == pytest.approx(SPEND_APP_TOTAL)
        assert dropped_filter_warnings(resp) == []

    async def test_negated_cross_path_conjunct_pushes(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["NOT (channel = 'app')"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        # NOT app ≡ web on this dataset — the population matches the web pin.
        for tier, spend in SPEND_WEB_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier
        assert dropped_filter_warnings(resp) == []


class TestSameRowGrouping:
    async def test_conjuncts_sharing_the_branch_bind_one_row(self, exec_backend):
        """Scenario: filters sharing a branch bind to the same related row —
        c1 and c2 have an ok order and an app order but no single ok app one."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[M, CM],
            filters=["status = 'ok'", "channel = 'app'"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        for tier, spend in SPEND_OK_APP_SAMEROW.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier
            assert float(by[(tier,)]["orders.cm"]) != pytest.approx(
                SPEND_OK_APP_SPLIT_DEFECT[tier],
            ), tier
        assert dropped_filter_warnings(resp) == []

    async def test_multi_hop_union_tree_same_row(self, exec_backend):
        """One store qualifies only through a single order that is both app
        AND by a gold customer — across the composite hop plus one more."""
        dialect, engine = exec_backend
        query = q(dimensions=["status"], measures=[M, RM],
                  filters=["channel = 'app'", "customers.tier = 'gold'"])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.status")
        assert set(by) == {("ok",), ("new",)}
        for row in resp.data:
            assert float(row["orders.rm"]) == pytest.approx(RENT_APP_GOLD_SAMEROW)
            assert float(row["orders.rm"]) != pytest.approx(
                RENT_APP_GOLD_SPLIT_DEFECT,
            )
        assert dropped_filter_warnings(resp) == []
        await _dry_has_exists_and_closes(engine, query, dialect)


class TestCompositeCorrelation:
    async def test_composite_first_hop_uses_every_pair(self, exec_backend):
        """(B,2) shares one pair column with each qualifying store; only the
        full composite correlation excludes it."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"], measures=[M, RM],
            filters=["channel = 'app'"],
        ))
        for row in resp.data:
            assert float(row["orders.rm"]) == pytest.approx(RENT_APP)
            assert float(row["orders.rm"]) != pytest.approx(
                RENT_SINGLE_PAIR_DEFECT,
            )
        assert dropped_filter_warnings(resp) == []


class TestInlineEquivalence:
    async def test_unproven_m2one_matches_the_proven_inline_values(
        self, exec_backend_weak,
    ):
        """EXISTS ≡ inline on genuinely m:1 data: the weak variant (no PK
        claim on plans.code) must take the EXISTS path yet produce the strong
        variant's inline values (the same oracle the smoke suite pins)."""
        dialect, engine = exec_backend_weak
        query = q(dimensions=["customers.tier"], measures=[CM],
                  filters=["customers.plans.level = 'basic'"])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        for tier, spend in SPEND_BASIC_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier
        assert dropped_filter_warnings(resp) == []
        await _dry_has_exists_and_closes(engine, query, dialect)

    async def test_declared_reverse_matches_the_inverted_edge(
        self, exec_backend_rev,
    ):
        _, engine = exec_backend_rev
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["channel = 'app'"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        for tier, spend in SPEND_APP_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier
        assert dropped_filter_warnings(resp) == []


class TestBranchIndependence:
    async def test_two_branches_satisfied_independently(self, exec_backend_weak):
        _, engine = exec_backend_weak
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[M, CM],
            filters=["channel = 'web'", "customers.plans.level = 'basic'"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",)}
        assert float(by[("gold",)]["orders.cm"]) == pytest.approx(
            SPEND_WEB_AND_BASIC["gold"],
        )
        # Neither branch may be lost: web-only 130, basic-only 160, none 245.
        assert float(by[("gold",)]["orders.cm"]) not in (130.0, 160.0, 245.0)
        assert float(by[("gold",)]["orders.m"]) == pytest.approx(13.0)
        assert dropped_filter_warnings(resp) == []


class TestCorrelatedOuterReference:
    async def test_root_local_ref_correlates_into_the_exists(self, exec_backend):
        """``customers.spend > amount``: c4 fails its only comparison and c7
        has no orders — both drop from the population."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[M, CM],
            filters=["customers.spend > amount"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        for tier, spend in SPEND_CORRELATED_GT.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier
        assert float(by[("gold",)]["orders.m"]) == pytest.approx(50.0)
        assert float(by[("silver",)]["orders.m"]) == pytest.approx(70.0)
        assert dropped_filter_warnings(resp) == []


class TestExpandedDependencies:
    async def test_host_declared_column_pushes_by_its_dependencies(
        self, exec_backend,
    ):
        """Scenario: derived-column dependencies drive classification —
        c7 (gold, zero orders) separates the semi-join from a root-local
        rewrite; the broadcast total 515 marks the dropped-filter defect."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"], measures=[M, CM],
            filters=["cust_tier = 'gold'"],
        ))
        by = rows_by(resp, "orders.status")
        assert set(by) == {("ok",), ("new",)}
        assert float(by[("ok",)]["orders.m"]) == pytest.approx(30.0)
        assert float(by[("new",)]["orders.m"]) == pytest.approx(20.0)
        for row in resp.data:
            assert float(row["orders.cm"]) == pytest.approx(SPEND_CUST_TIER_GOLD)
        assert dropped_filter_warnings(resp) == []

    async def test_target_declared_column_never_fans_the_producer(
        self, exec_backend_rev,
    ):
        """The latent inline hole, executed: c1 has TWO ok orders; inlining
        ``last_status = 'ok'`` counts its spend twice (290)."""
        dialect, engine = exec_backend_rev
        query = q(dimensions=["customers.tier"], measures=[CM],
                  filters=["customers.last_status = 'ok'"])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        for tier, spend in SPEND_LAST_STATUS_OK.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier
        assert float(by[("gold",)]["orders.cm"]) != pytest.approx(
            SPEND_LAST_STATUS_OK_INLINE_FAN_GOLD,
        )
        assert dropped_filter_warnings(resp) == []
        await _dry_has_exists_and_closes(engine, query, dialect)


class TestProducerKinds:
    async def test_ranked_first_last_over_the_filtered_population(
        self, exec_backend,
    ):
        """Scenario: pushdown reaches every producer kind (ranked)."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier"],
            measures=[
                ModelMeasure(formula="customers.spend:first(customers.signup_at)",
                             name="f"),
                ModelMeasure(formula="customers.spend:last(customers.signup_at)",
                             name="l"),
            ],
            filters=["channel = 'app'"],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        for tier in ("gold", "silver"):
            assert float(by[(tier,)]["orders.f"]) == pytest.approx(
                SPEND_APP_FIRST_BY_TIER[tier],
            ), f"first:{tier}"
            assert float(by[(tier,)]["orders.l"]) == pytest.approx(
                SPEND_APP_LAST_BY_TIER[tier],
            ), f"last:{tier}"
        dry = await engine.execute(query, dry_run=True)
        assert_scope_closed(dry.sql, dialect=dialect)

    async def test_windowed_over_the_filtered_population(self, exec_backend):
        """Scenario: pushdown reaches every producer kind (windowed)."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            time_dimensions=signup_month_td(),
            measures=[ModelMeasure(formula="customers.spend:sum(window='1y')",
                                   name="w")],
            filters=["channel = 'app'"],
        ))
        got = {month_key(r["orders.customers.signup_at"]): r["orders.w"]
               for r in resp.data}
        assert set(got) == set(SPEND_APP_1Y_BY_SIGNUP_MONTH)
        for month, expected in SPEND_APP_1Y_BY_SIGNUP_MONTH.items():
            assert float(got[month]) == pytest.approx(expected), month

    async def test_windowed_grain_spine_carries_the_exists(self, exec_backend):
        """Scenario: the pushed filter reaches BOTH windowed-producer legs —
        the grain spine (``_base``) and the window source (``_src``) — so an
        excluded bucket vanishes from the producer instead of surfacing with
        a NULL window value."""
        dialect, engine = exec_backend
        dry = await engine.execute(q(
            time_dimensions=signup_month_td(),
            measures=[ModelMeasure(formula="customers.spend:sum(window='1y')",
                                   name="w")],
            filters=["channel = 'app'"],
        ), dry_run=True)
        tree = sqlglot.parse_one(dry.sql, read=dialect)
        assert len(list(tree.find_all(exp.Exists))) == 2, dry.sql

    async def test_windowed_grain_spine_drops_filtered_out_buckets(
        self, exec_backend,
    ):
        """Scenario: a bucket with no passing customer (2024-04 has no
        new-status order) vanishes from the result, and surviving window
        values sum the kept population only."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            time_dimensions=signup_month_td(),
            measures=[ModelMeasure(formula="customers.spend:sum(window='1y')",
                                   name="w")],
            filters=["status = 'new'"],
        ))
        got = {month_key(r["orders.customers.signup_at"]): r["orders.w"]
               for r in resp.data}
        assert set(got) == set(SPEND_NEW_1Y_BY_SIGNUP_MONTH)
        for month, expected in SPEND_NEW_1Y_BY_SIGNUP_MONTH.items():
            assert float(got[month]) == pytest.approx(expected), month

    async def test_nested_computed_dimension_producer(self, exec_backend):
        """Scenario: pushdown reaches every producer kind (nested computed
        dimension) — the app population flips gold to 'lo' (160 ≤ 170)."""
        dialect, engine = exec_backend
        query = q(
            dimensions=[{"expression": SPEND_BAND_170, "name": "sband"}],
            measures=[M],
            filters=["channel = 'app'"],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.sband")
        assert set(by) == {("hi",), ("lo",)}
        assert float(by[("lo",)]["orders.m"]) == pytest.approx(25.0)
        assert float(by[("hi",)]["orders.m"]) == pytest.approx(40.0)
        assert dropped_filter_warnings(resp) == []
        await _dry_has_exists_and_closes(engine, query, dialect)
