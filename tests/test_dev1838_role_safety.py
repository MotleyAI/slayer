"""DEV-1838 stage 2 — per-role crossing-input safety on host-rooted producers
(design D5; class-(d) ledger in the change's divergences.md).

Spec: openspec …/specs/queries/cross-model-aggregates — "Unsafe aggregate
inputs fail closed" extended to every root. A *filter reference* or *argument*
crossing an unproven hop fails closed; a *host-grain wrap over a joined
source* stays legal; proven-hop shapes keep their exact values.

Feature-missing today: the fail-closed tests (these shapes currently compute
silently over the fanned join — values recorded in divergences.md). The
proven-hop and host-grain pins are green today and must stay green.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError

from slayer.core.query import QueryMetric

from tests._dev1838_fixtures import (
    FACTOR_MAX_BY_STATUS,
    FACTOR_MIN_BY_STATUS,
    GOLD_BY_STATUS,
    LAST_BY_SIGNUP_BY_STATUS,
    OrderItem,
    WSCALED_BY_STATUS,
    make_exec_engine,
    month_td,
    q,
    rows_by,
)

RAISES = (SlayerError, ValueError, NotImplementedError)
M = QueryMetric(metric="sum(amount)", alias="m")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _assert_clear(ei, *needles: str, remedy: bool = True) -> None:
    message = str(ei.value)
    for needle in needles:
        assert needle in message, f"{needle!r} missing from: {message}"
    assert "__regroup__" not in message
    if remedy:
        lowered = message.lower()
        assert any(w in lowered for w in ("cardinality", "unique", "declare")), (
            f"no remedy named in: {message}"
        )


class TestCrossedPredicateFailsClosed:
    """A ``Column.filter`` crossing an unproven hop — plain, ranked, and
    windowed kernels alike — never the silently multiplied aggregate."""

    async def test_filter_over_unproven_second_hop_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="sum(alpha_amount)", alias="a")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "segments", "alpha_amount")

    async def test_filter_over_unproven_to_many_hop_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="sum(rush_amount)", alias="r")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "tags", "rush_amount")

    async def test_ranked_kernel_with_unproven_filter_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="last(alpha_amount)", alias="al")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "segments", "alpha_amount")

    async def test_windowed_kernel_with_unproven_filter_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"], time_dimensions=month_td(),
            metrics=[QueryMetric(metric="sum(rush_amount, window='90d')",
                                   alias="rw")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "tags", "rush_amount")


class TestCrossedArgumentFailsClosed:
    """An aggregation param / ranking arg crossing an unproven hop."""

    async def test_aggregation_param_over_fanning_hop_homes_on_tags(self, exec_backend):
        # tags determines orders.amount: the SUM runs over tag rows, each tag once.
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="tscaled_sum(amount)", alias="t")],
        ))
        from_tags = await engine.execute(q(
            population="tags", dimensions=["orders.status"],
            metrics=[QueryMetric(metric="sum(orders.amount * factor)", alias="t")],
        ))
        got = {s: float(r["orders.t"]) for (s,), r in rows_by(resp, "orders.status").items()}
        twin = {s: float(r["tags.t"]) for (s,), r in rows_by(from_tags, "tags.orders.status").items()
                if r["tags.t"] is not None}
        assert {s: got[s] for s in ("ok", "new")} == pytest.approx({"ok": 135.0, "new": 8.0})
        assert {s: got[s] for s in twin} == pytest.approx(twin)

    async def test_ranking_arg_over_unproven_hop_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"],
            metrics=[QueryMetric(
                metric="last(amount, customers.segments.updated_at)",
                alias="lu")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "segments", "updated_at")


class TestProvenHopsKeepExactValues:
    """The same roles over provably many-to-one hops stay legal, values
    identical to pre-unification behavior (green pins)."""

    async def test_filtered_local_over_proven_hop(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            metrics=[M, QueryMetric(metric="sum(gold_amount)", alias="g")],
        ))
        by = rows_by(resp, "orders.status")
        for status, expected in GOLD_BY_STATUS.items():
            assert float(by[(status,)]["orders.g"]) == pytest.approx(expected)

    async def test_ranked_filtered_over_proven_hop(self, exec_backend):
        # DEV-1832: Column.filter masks the value; it never restricts the ranking.
        # gold_amount masks amount to NULL unless the order's customer is gold (c1).
        # The newest 'ok' order (#7, NULL customer) and newest 'new' order (#6, c4
        # bronze) are both non-gold, so gold_amount:last masks each to NULL — the
        # older gold orders are not reached (a WHERE would belong in the query).
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="last(gold_amount)", alias="gl")],
        ))
        by = rows_by(resp, "orders.status")
        assert by[("ok",)]["orders.gl"] is None
        assert by[("new",)]["orders.gl"] is None

    async def test_ranked_filtered_picks_masked_value_when_newest_matches(self, exec_backend):
        # CityA's newest order (#2, gold c1) → gold_amount:last = 20; every other
        # city's newest order is non-gold → NULL.
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["city"],
            metrics=[QueryMetric(metric="last(gold_amount)", alias="gl")],
        ))
        by = rows_by(resp, "orders.city")
        assert float(by[("CityA",)]["orders.gl"]) == pytest.approx(20.0)
        assert by[("CityB",)]["orders.gl"] is None
        assert by[("CityD",)]["orders.gl"] is None

    async def test_aggregation_param_over_proven_hops(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            metrics=[M, QueryMetric(metric="wscaled_sum(amount)", alias="w")],
        ))
        by = rows_by(resp, "orders.status")
        for status, expected in WSCALED_BY_STATUS.items():
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)

    async def test_ranking_arg_over_proven_hop(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="last(amount, customers.signup_at)",
                                   alias="ls")],
        ))
        by = rows_by(resp, "orders.status")
        for status, expected in LAST_BY_SIGNUP_BY_STATUS.items():
            assert float(by[(status,)]["orders.ls"]) == pytest.approx(expected)


class TestHostGrainWrapStaysLegal:
    """An aggregate explicitly evaluated at host grain over a joined source is
    defined over the join result: its crossing source path stays legal, even
    across a to-many or unproven hop (green pins)."""

    @staticmethod
    def _order_query(*, dataset: str, name: str, direction: str):
        return q(dimensions=["status"], metrics=[M],
                 order=[OrderItem(field=f"{dataset}.{name}",
                                  direction=direction)])

    async def test_wrap_over_to_many_hop_asc(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(self._order_query(
            dataset="tags", name="factor", direction="asc"))
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["new", "ok"], (statuses, FACTOR_MIN_BY_STATUS)

    async def test_wrap_over_to_many_hop_desc(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(self._order_query(
            dataset="tags", name="factor", direction="desc"))
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["ok", "new"], (statuses, FACTOR_MAX_BY_STATUS)

    async def test_wrap_over_proven_hop(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(self._order_query(
            dataset="customers", name="tier", direction="asc"))
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["new", "ok"], statuses

    async def test_wrap_source_over_unproven_hop_stays_legal(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(self._order_query(
            dataset="customers.segments", name="label", direction="desc"))
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["ok", "new"], statuses
        by = rows_by(resp, "orders.status")
        assert float(by[("ok",)]["orders.m"]) == pytest.approx(57.0)
        assert float(by[("new",)]["orders.m"]) == pytest.approx(60.0)
