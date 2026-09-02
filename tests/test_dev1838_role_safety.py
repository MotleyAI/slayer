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

from tests._dev1838_fixtures import (
    ColumnRef,
    FACTOR_MAX_BY_STATUS,
    FACTOR_MIN_BY_STATUS,
    GOLD_BY_STATUS,
    GOLD_LAST_BY_STATUS,
    LAST_BY_SIGNUP_BY_STATUS,
    ModelMeasure,
    OrderItem,
    WSCALED_BY_STATUS,
    make_exec_engine,
    month_td,
    q,
    rows_by,
)

RAISES = (SlayerError, ValueError, NotImplementedError)
M = ModelMeasure(formula="amount:sum", name="m")


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
            measures=[ModelMeasure(formula="alpha_amount:sum", name="a")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "segments", "alpha_amount")

    async def test_filter_over_unproven_to_many_hop_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="rush_amount:sum", name="r")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "tags", "rush_amount")

    async def test_ranked_kernel_with_unproven_filter_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="alpha_amount:last", name="al")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "segments", "alpha_amount")

    async def test_windowed_kernel_with_unproven_filter_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="rush_amount:sum(window='90d')",
                                   name="rw")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "tags", "rush_amount")


class TestCrossedArgumentFailsClosed:
    """An aggregation param / ranking arg crossing an unproven hop."""

    async def test_aggregation_param_over_unproven_hop_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:tscaled_sum", name="t")],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        _assert_clear(ei, "tags")

    async def test_ranking_arg_over_unproven_hop_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="amount:last(customers.segments.updated_at)",
                name="lu")],
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
            measures=[M, ModelMeasure(formula="gold_amount:sum", name="g")],
        ))
        by = rows_by(resp, "orders.status")
        for status, expected in GOLD_BY_STATUS.items():
            assert float(by[(status,)]["orders.g"]) == pytest.approx(expected)

    async def test_ranked_filtered_over_proven_hop(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="gold_amount:last", name="gl")],
        ))
        by = rows_by(resp, "orders.status")
        for status, expected in GOLD_LAST_BY_STATUS.items():
            assert float(by[(status,)]["orders.gl"]) == pytest.approx(expected)

    async def test_aggregation_param_over_proven_hops(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            measures=[M, ModelMeasure(formula="amount:wscaled_sum", name="w")],
        ))
        by = rows_by(resp, "orders.status")
        for status, expected in WSCALED_BY_STATUS.items():
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)

    async def test_ranking_arg_over_proven_hop(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:last(customers.signup_at)",
                                   name="ls")],
        ))
        by = rows_by(resp, "orders.status")
        for status, expected in LAST_BY_SIGNUP_BY_STATUS.items():
            assert float(by[(status,)]["orders.ls"]) == pytest.approx(expected)


class TestHostGrainWrapStaysLegal:
    """An aggregate explicitly evaluated at host grain over a joined source is
    defined over the join result: its crossing source path stays legal, even
    across a to-many or unproven hop (green pins)."""

    @staticmethod
    def _order_query(*, model: str, name: str, direction: str):
        return q(dimensions=["status"], measures=[M],
                 order=[OrderItem(column=ColumnRef(name=name, model=model),
                                  direction=direction)])

    async def test_wrap_over_to_many_hop_asc(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(self._order_query(
            model="tags", name="factor", direction="asc"))
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["new", "ok"], (statuses, FACTOR_MIN_BY_STATUS)

    async def test_wrap_over_to_many_hop_desc(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(self._order_query(
            model="tags", name="factor", direction="desc"))
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["ok", "new"], (statuses, FACTOR_MAX_BY_STATUS)

    async def test_wrap_over_proven_hop(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(self._order_query(
            model="customers", name="tier", direction="asc"))
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["new", "ok"], statuses

    async def test_wrap_source_over_unproven_hop_stays_legal(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(self._order_query(
            model="customers.segments", name="label", direction="desc"))
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["ok", "new"], statuses
        by = rows_by(resp, "orders.status")
        assert float(by[("ok",)]["orders.m"]) == pytest.approx(57.0)
        assert float(by[("new",)]["orders.m"]) == pytest.approx(60.0)
