"""DEV-1746 §5.1 — ``resolve(consumer=…)`` gets its first production caller.

``ScopeFrame.resolve(consumer=…)`` and ``apply_materializations`` implement the
projection-boundary principle: a value that crosses a join inside a producing
scope is projected there under a ``_val_<n>`` alias and referenced by that alias
from the consumer. Both are fully unit-tested — and both have **zero production
callers**. An API nobody calls does not establish a principle, so §5.1 requires
a production-path test.

Meanwhile the generator carries its own second materialiser for exactly the same
job (``allocate_val()`` + a ``value_alias_by_sql`` dict, deduped by resolved SQL
text). Two mechanisms, one purpose. "The ``consumer=`` materializer becomes the
ONLY one on the cross-model path" is therefore read as: it REPLACES that
generator-local flow. Slot-backed public columns keep resolving through
projected aliases — consuming another scope's projected column by its alias
already *is* exchanging data through projected columns.

The two sites migrated here are the ones inside ``_render_cross_model_cte``:

* a **crossing grain** — grouping a first/last cross-model aggregate by a
  derived dimension whose SQL reaches a further join. The ranked subquery
  re-exports only ``target.*``, so the grain must be projected inside it::

      regions.population AS _val_0        -- inside the ranked subquery
      ...
      GROUP BY _val_0                     -- the outer CTE reads the alias

* a **crossing value** — the aggregated expression itself crosses a join.

``_build_first_last_base_select``'s copy of this flow is deliberately NOT
migrated here: PR 5 rewrites that machinery wholesale as ``RankedAggregatePlan``,
and moving the same state twice is what this PR's sequencing exists to avoid.
That corner is recorded in the PR-5 handoff, and the test at the bottom of this
module pins it as a known, deliberate exception rather than leaving it silent.

Both dedup on the resolved SQL text — ``ScopeFrame``'s key is
``(scope_id, rendered_ast, dialect)`` — so the migration is byte-preserving for
every shape that materialises through ONE of the two sites. That is asserted
directly below.

One shape is NOT byte-preserving, and it is a defect the migration fixes:
when a first/last cross-model aggregate is grouped by the same crossing
expression it aggregates, both sites fire and each keeps its own dedup map, so
the expression is projected twice (``_val_0`` and ``_val_1``). ``ScopeFrame``
holds one table per scope, so they collapse to one. See
``TestDedupParity::test_one_expression_is_materialised_once_per_scope`` — it is
a newly surfaced divergence for the PR's approval list, not a ratified B-item.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.sql.scope import ScopeFrame

from tests._cross_model_chain import _gen
from tests._engine_helpers import _extract_cte_body, _norm

# --------------------------------------------------------------------------- #
# Byte-parity baselines — the emitted SQL these shapes produce today. The
# migration swaps WHICH materialiser mints ``_val_0``; it must not change the
# SQL, so these are pinned verbatim (normalised for whitespace only).
# --------------------------------------------------------------------------- #
CROSSING_GRAIN_CTE = _norm(
    """
    SELECT _val_0 AS "orders_x.customers_v2.deep_pop",
      MAX(CASE WHEN _first_rn = 1 THEN customers_v2.lifetime_value END)
        AS "orders_x.customers_v2.lifetime_value_first"
    FROM ( SELECT customers_v2.*, regions.population AS _val_0,
      ROW_NUMBER() OVER (PARTITION BY regions.population
        ORDER BY customers_v2.signup_at ASC) AS _first_rn
      FROM customers AS customers_v2
      LEFT JOIN regions AS regions ON customers_v2.region_id = regions.id
    ) AS customers_v2 GROUP BY _val_0
    """
)

CROSSING_VALUE_CTE = _norm(
    """
    SELECT customers_v2.status AS "orders_x.customers_v2.status",
      MAX(CASE WHEN _first_rn = 1 THEN customers_v2._val_0 END)
        AS "orders_x.customers_v2.deep_weight_first"
    FROM ( SELECT customers_v2.*, regions.weight AS _val_0,
      ROW_NUMBER() OVER (PARTITION BY customers_v2.status
        ORDER BY customers_v2.signup_at ASC) AS _first_rn
      FROM customers AS customers_v2
      LEFT JOIN regions AS regions ON customers_v2.region_id = regions.id
    ) AS customers_v2 GROUP BY customers_v2.status
    """
)


def _crossing_grain_query() -> SlayerQuery:
    """First/last cross-model aggregate grouped by a CROSSING derived grain."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="customers_v2.deep_pop")],
        measures=[ModelMeasure(
            formula="customers_v2.lifetime_value:first", name="f",
        )],
    )


def _crossing_value_query() -> SlayerQuery:
    """First/last cross-model aggregate whose VALUE crosses a join."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="customers_v2.status")],
        measures=[ModelMeasure(formula="customers_v2.deep_weight:first", name="f")],
    )


def _two_distinct_crossing_values_query() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="customers_v2.status")],
        measures=[
            ModelMeasure(formula="customers_v2.deep_weight:first", name="w"),
            ModelMeasure(formula="customers_v2.deep_pop:first", name="p"),
        ],
    )


class _ResolveSpy:
    """Records every ``ScopeFrame.resolve`` call and whether it named a consumer.

    A spy rather than a grep: the point of §5.1 is that the branch runs on the
    PRODUCTION path, which only an observed call can establish.
    """

    def __init__(self) -> None:
        self.calls: List[Tuple[str, bool]] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original = ScopeFrame.resolve
        spy = self

        def _wrapped(self_frame, ref, *, consumer: Optional[ScopeFrame] = None):
            spy.calls.append((type(ref).__name__, consumer is not None))
            return original(self_frame, ref, consumer=consumer)

        monkeypatch.setattr(ScopeFrame, "resolve", _wrapped, raising=True)

    @property
    def consumer_calls(self) -> int:
        return sum(1 for _, had_consumer in self.calls if had_consumer)


class _MaterializeSpy:
    def __init__(self) -> None:
        self.aliases: List[str] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original = ScopeFrame._materialize
        spy = self

        def _wrapped(self_frame, template):
            alias = original(self_frame, template)
            spy.aliases.append(alias)
            return alias

        monkeypatch.setattr(ScopeFrame, "_materialize", _wrapped, raising=True)


# =========================================================================== #
# The production-path proof.
# =========================================================================== #
class TestConsumerMaterializationOnTheProductionPath:

    @pytest.mark.parametrize(
        "query_factory",
        [_crossing_grain_query, _crossing_value_query],
        ids=["crossing_grain", "crossing_value"],
    )
    async def test_resolve_is_called_with_a_consumer(
        self, query_factory, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """NEW (§5.1): generating a real query exercises the materialisation
        branch of ``ScopeFrame.resolve``."""
        spy = _ResolveSpy()
        spy.install(monkeypatch)
        await _gen(query_factory(), dialect="postgres")
        assert spy.calls, "ScopeFrame.resolve was never called at all"
        assert spy.consumer_calls > 0, (
            "no production call passed `consumer=` — the cross-model CTE is "
            "still minting `_val_` aliases through the generator's own "
            "materialiser, so `resolve`'s projection-boundary branch remains "
            f"unexercised in production ({len(spy.calls)} consumer-less calls)."
        )

    @pytest.mark.parametrize(
        "query_factory",
        [_crossing_grain_query, _crossing_value_query],
        ids=["crossing_grain", "crossing_value"],
    )
    async def test_scope_frame_mints_the_val_alias(
        self, query_factory, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The alias in the emitted SQL is the one ``ScopeFrame`` minted — not a
        coincidentally-identical one from the generator's own allocator."""
        spy = _MaterializeSpy()
        spy.install(monkeypatch)
        sql = await _gen(query_factory(), dialect="postgres")
        assert spy.aliases, (
            "ScopeFrame._materialize never ran, so the `_val_` alias in the "
            f"emitted SQL came from the superseded materialiser:\n{sql}"
        )
        for alias in spy.aliases:
            assert alias in sql, (
                f"materialised alias {alias!r} is absent from the emitted SQL — "
                f"the scope materialised a value nobody projected:\n{sql}"
            )

    async def test_apply_materializations_has_a_production_caller(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The other half of the contract: what the scope materialises must be
        projected into the producing SELECT by ``apply_materializations``."""
        seen: List[int] = []
        original = ScopeFrame.apply_materializations

        def _wrapped(self_frame, select):
            seen.append(len(self_frame.materializations))
            return original(self_frame, select)

        monkeypatch.setattr(
            ScopeFrame, "apply_materializations", _wrapped, raising=True,
        )
        await _gen(_crossing_grain_query(), dialect="postgres")
        assert seen, (
            "apply_materializations was never called on the production path"
        )
        assert any(count > 0 for count in seen), (
            "apply_materializations ran but the scope held no materialisations"
        )


# =========================================================================== #
# Byte-parity: swapping the materialiser must not change emitted SQL.
# =========================================================================== #
class TestMigrationIsBytePreserving:

    async def test_crossing_grain_cte_is_unchanged(self) -> None:
        sql = await _gen(_crossing_grain_query(), dialect="postgres")
        body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert body == CROSSING_GRAIN_CTE, (
            "the crossing-grain CTE changed shape. The two materialisers dedup "
            "on the same key (resolved SQL text), so the migration must be "
            f"byte-preserving.\n\nactual:\n{body}\n\nexpected:\n"
            f"{CROSSING_GRAIN_CTE}"
        )

    async def test_crossing_value_cte_is_unchanged(self) -> None:
        sql = await _gen(_crossing_value_query(), dialect="postgres")
        body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert body == CROSSING_VALUE_CTE, (
            f"the crossing-value CTE changed shape.\n\nactual:\n{body}\n\n"
            f"expected:\n{CROSSING_VALUE_CTE}"
        )


# =========================================================================== #
# Dedup parity with the flow being replaced.
# =========================================================================== #
class TestDedupParity:

    async def test_two_distinct_crossing_values_get_distinct_aliases(
        self,
    ) -> None:
        """Different values must never collapse onto one alias — that would
        silently aggregate the wrong column."""
        sql = await _gen(_two_distinct_crossing_values_query(), dialect="postgres")
        assert "_val_0" in sql and "_val_1" in sql, (
            f"expected two distinct materialisations:\n{sql}"
        )

    async def test_one_expression_is_materialised_once_per_scope(self) -> None:
        """NEWLY SURFACED DIVERGENCE — grouping a first/last cross-model
        aggregate by the SAME crossing expression it aggregates materialises it
        TWICE today::

            regions.weight AS _val_0,      -- minted by the grain loop
            regions.weight AS _val_1,      -- minted by the value branch

        because the two sites keep SEPARATE dedup maps (the grain loop calls
        ``allocate_val()`` without consulting the value branch's
        ``value_alias_by_sql``). ``ScopeFrame`` holds ONE materialisation table
        per scope keyed on the rendered template, so routing both through
        ``resolve(consumer=…)`` collapses them to a single projection.

        This is a redundant column rather than a wrong answer, but it is an
        emitted-SQL change beyond the ratified B-items and belongs on the PR's
        approval list.
        """
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="customers_v2.deep_weight")],
            measures=[ModelMeasure(
                formula="customers_v2.deep_weight:first", name="f",
            )],
        )
        sql = await _gen(query, dialect="postgres")
        body = _extract_cte_body(sql, r"_cm_\w+")
        assert body.count("AS _val_") == 1, (
            "the same crossing expression was materialised more than once in "
            f"one scope — the two materialisers still hold separate dedup "
            f"tables:\n{body}"
        )

    async def test_separate_scopes_keep_independent_materialisations(
        self,
    ) -> None:
        """Dedup is per SCOPE, not global: two ``_cm_`` CTEs are two scopes, so
        each materialises its own copy. Pinned so the dedup unification above
        is not over-applied into cross-scope sharing, which would reference an
        alias that does not exist in the consuming CTE."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="customers_v2.status")],
            measures=[
                ModelMeasure(formula="customers_v2.deep_weight:first", name="a"),
                ModelMeasure(formula="customers_v2.deep_weight:last", name="b"),
            ],
        )
        sql = await _gen(query, dialect="postgres")
        for pattern in (
            r"_cm_\w*deep_weight_first\w*",
            r"_cm_\w*deep_weight_last\w*",
        ):
            body = _extract_cte_body(sql, pattern)
            assert body.count("AS _val_") == 1, (
                f"each scope must project its own single materialisation:\n{body}"
            )


# =========================================================================== #
# The deliberate PR-5 exception, pinned rather than left implicit.
# =========================================================================== #
class TestRankedBaseMaterialiserStillDeferred:
    """``_build_first_last_base_select`` keeps its own ``_val_`` flow until PR 5
    replaces it with ``RankedAggregatePlan``.

    To reach that code the aggregate must be rooted at the HOST — a
    ``customers_v2.…`` measure is cross-model and goes through
    ``_render_cross_model_cte`` instead. So the host model gets a derived column
    whose SQL crosses into the joined model, and the first/last aggregate is
    taken over THAT.
    """

    _HOST_CROSSING_COLUMN = [
        Column(
            name="cust_ltv",
            sql="customers_v2.lifetime_value",
            type=DataType.DOUBLE,
        ),
    ]

    def _query(self) -> SlayerQuery:
        return SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="cust_ltv:first", name="f")],
        )

    async def test_local_first_last_still_materialises_correctly(self) -> None:
        """The corner keeps WORKING while it waits — pinned so PR 5 inherits a
        test rather than a silent assumption."""
        sql = await _gen(
            self._query(), orders_extra=self._HOST_CROSSING_COLUMN,
            dialect="postgres",
        )
        assert "_val_" in sql, (
            f"expected a materialised crossing value in this shape:\n{sql}"
        )
        assert "ROW_NUMBER() OVER" in sql, sql

    async def test_the_ranked_base_path_is_still_consumer_free(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The deferral itself, pinned rather than assumed.

        This shape's ``_val_`` alias must still come from the generator's own
        materialiser, NOT from ``ScopeFrame``. When PR 5 migrates it this test
        flips — which is the point: the boundary between the two PRs is
        asserted, so it cannot drift silently.
        """
        spy = _MaterializeSpy()
        spy.install(monkeypatch)
        sql = await _gen(
            self._query(), orders_extra=self._HOST_CROSSING_COLUMN,
            dialect="postgres",
        )
        assert "_val_" in sql, sql
        assert not spy.aliases, (
            "ScopeFrame materialised for the ranked BASE path. That migration "
            "belongs to PR 5 (RankedAggregatePlan); if it landed early, move "
            f"this test rather than deleting it. aliases={spy.aliases}"
        )
