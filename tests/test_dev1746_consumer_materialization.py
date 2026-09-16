"""``ScopeFrame.resolve(consumer=…)`` becomes the single materialiser on the cross-model path.
It projects a join-crossing value inside the producing scope under a ``_val_<n>`` alias and references it from the consumer, replacing the generator's own ``allocate_val()`` flow. Migrated sites (inside ``_render_cross_model_cte``): a crossing grain (grouping a first/last cross-model aggregate by a derived dimension whose SQL reaches a further join) and a crossing value (the aggregated expression itself crosses a join). Both dedup on resolved SQL text, so the swap is byte-preserving — except one fixed defect: grouping a first/last aggregate by the same expression it aggregates used to project it twice; one per-scope table collapses it to one.
"""

from __future__ import annotations

import re as _re
from typing import List, Optional, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.sql.scope import ScopeFrame

from tests._cross_model_chain import _gen
from tests._engine_helpers import _extract_cte_body, _norm

# Byte-parity baselines: swapping which materialiser mints ``_val_0`` must not change this SQL.
CROSSING_GRAIN_CTE = _norm(
    """
    SELECT _val_0 AS "customers_v2.deep_pop",
      CAST(MAX(CASE WHEN _ranked_rn = 1 THEN _val_1 END) AS DOUBLE PRECISION)
        AS "customers_v2.lifetime_value_first"
    FROM ( SELECT regions.population AS _val_0,
      customers_v2.lifetime_value AS _val_1,
      ROW_NUMBER() OVER (PARTITION BY regions.population
        ORDER BY customers_v2.signup_at) AS _ranked_rn
      FROM customers AS customers_v2
      LEFT JOIN regions AS regions ON customers_v2.region_id = regions.id
    ) AS _ranked_src GROUP BY _val_0
    """
)

CROSSING_VALUE_CTE = _norm(
    """
    SELECT _val_0 AS "customers_v2.status",
      CAST(MAX(CASE WHEN _ranked_rn = 1 THEN _val_1 END) AS DOUBLE PRECISION)
        AS "customers_v2.deep_weight_first"
    FROM ( SELECT customers_v2.status AS _val_0,
      regions.weight AS _val_1,
      ROW_NUMBER() OVER (PARTITION BY customers_v2.status
        ORDER BY customers_v2.signup_at) AS _ranked_rn
      FROM customers AS customers_v2
      LEFT JOIN regions AS regions ON customers_v2.region_id = regions.id
    ) AS _ranked_src GROUP BY _val_0
    """
)


def _crossing_grain_query() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="customers_v2.deep_pop")],
        measures=[ModelMeasure(
            formula="customers_v2.lifetime_value:first", name="f",
        )],
    )


def _crossing_value_query() -> SlayerQuery:
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
    """Records every close of a scoped value and whether it named a consumer.
    Spies on ``ScopeFrame._close`` (the branch both ``resolve`` and ``materialize_for`` funnel through), so a named-consumer close is observable on the production path.
    """

    def __init__(self) -> None:
        self.calls: List[Tuple[str, bool]] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original = ScopeFrame._close
        spy = self

        def _wrapped(self_frame, template, *, consumer: Optional[ScopeFrame] = None):
            spy.calls.append((type(template).__name__, consumer is not None))
            return original(self_frame, template, consumer=consumer)

        monkeypatch.setattr(ScopeFrame, "_close", _wrapped, raising=True)

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


class TestConsumerMaterializationOnTheProductionPath:

    @pytest.mark.parametrize(
        "query_factory",
        [_crossing_grain_query, _crossing_value_query],
        ids=["crossing_grain", "crossing_value"],
    )
    async def test_a_value_is_closed_for_a_named_consumer(
        self, query_factory, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spy = _ResolveSpy()
        spy.install(monkeypatch)
        await _gen(query_factory(), dialect="postgres")
        assert spy.calls, "no value was closed through a scope at all"
        assert spy.consumer_calls > 0, (
            "no production call named a `consumer=` — the cross-model CTE is "
            "still minting `_val_` aliases through the generator's own "
            "materialiser, so the projection-boundary branch remains "
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

    async def test_what_the_scope_materialises_is_what_gets_projected(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Every entry in the scope's materialisation table is projected under its own alias in the producing SELECT, and nothing else invents one."""
        captured: List[List[Tuple[str, str]]] = []
        original = ScopeFrame._materialize

        def _wrapped(self_frame, template):
            alias = original(self_frame, template)
            captured.append([
                (m.alias, m.expr.sql(dialect="postgres"))
                for m in self_frame.materializations
            ])
            return alias

        monkeypatch.setattr(ScopeFrame, "_materialize", _wrapped, raising=True)
        sql = await _gen(_crossing_grain_query(), dialect="postgres")
        assert captured, "no scope materialised anything on the production path"
        final = captured[-1]
        for alias, expr_sql in final:
            assert f"{expr_sql} AS {alias}" in sql, (
                f"materialisation {alias}={expr_sql!r} is not projected in the "
                f"producing SELECT:\n{sql}"
            )


class TestMigrationIsBytePreserving:

    async def test_crossing_grain_cte_is_unchanged(self) -> None:
        sql = await _gen(_crossing_grain_query(), dialect="postgres")
        body = _norm(_extract_cte_body(sql=sql, cte_name_pattern=r"_cm_\w+"))
        assert body == CROSSING_GRAIN_CTE, (
            "the crossing-grain CTE changed shape. The two materialisers dedup "
            "on the same key (resolved SQL text), so the migration must be "
            f"byte-preserving.\n\nactual:\n{body}\n\nexpected:\n"
            f"{CROSSING_GRAIN_CTE}"
        )

    async def test_crossing_value_cte_is_unchanged(self) -> None:
        sql = await _gen(_crossing_value_query(), dialect="postgres")
        body = _norm(_extract_cte_body(sql=sql, cte_name_pattern=r"_cm_\w+"))
        assert body == CROSSING_VALUE_CTE, (
            f"the crossing-value CTE changed shape.\n\nactual:\n{body}\n\n"
            f"expected:\n{CROSSING_VALUE_CTE}"
        )


class TestDedupParity:

    async def test_two_distinct_crossing_values_get_distinct_aliases(
        self,
    ) -> None:
        """Different values must never collapse onto one alias (would aggregate the wrong column)."""
        sql = await _gen(_two_distinct_crossing_values_query(), dialect="postgres")
        assert "_val_0" in sql, f"first materialisation missing:\n{sql}"
        assert "_val_1" in sql, f"second materialisation missing:\n{sql}"

    async def test_one_expression_is_materialised_once_per_scope(self) -> None:
        """One per-scope ``ScopeFrame`` table collapses to a single projection what the two old dedup maps materialised twice (grouping a first/last aggregate by the same expression it aggregates)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="customers_v2.deep_weight")],
            measures=[ModelMeasure(
                formula="customers_v2.deep_weight:first", name="f",
            )],
        )
        sql = await _gen(query, dialect="postgres")
        body = _extract_cte_body(sql=sql, cte_name_pattern=r"_cm_\w+")
        assert body.count("regions.weight AS _val_") == 1, (
            "the same crossing expression was materialised more than once in "
            f"one scope — the two materialisers still hold separate dedup "
            f"tables:\n{body}"
        )

    async def test_separate_scopes_keep_independent_materialisations(
        self,
    ) -> None:
        """Dedup is per scope: two ``_cm_`` CTEs each materialise their own copy (guards against over-sharing into an alias absent from the consuming CTE)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="customers_v2.status")],
            measures=[
                ModelMeasure(formula="customers_v2.deep_weight:first", name="a"),
                ModelMeasure(formula="customers_v2.deep_weight:last", name="b"),
            ],
        )
        sql = await _gen(query, dialect="postgres")
        for pattern in (r"_cm_\w*first\b", r"_cm_\w*last\b"):
            body = _extract_cte_body(sql=sql, cte_name_pattern=pattern)
            assert body.count("regions.weight AS _val_") == 1, (
                f"each scope must project its own single materialisation:\n{body}"
            )


class TestRankedPathUsesTheOneMaterialiser:
    """The host-rooted ranked path uses the one per-scope materialiser too.
    Reaching it needs a HOST-rooted aggregate (a ``customers_v2.…`` measure roots at the target instead), so the host gets a derived column crossing into the joined model and first/last runs over that.
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
        sql = await _gen(
            self._query(), orders_extra=self._HOST_CROSSING_COLUMN,
            dialect="postgres",
        )
        assert "_val_" in sql, (
            f"expected a materialised crossing value in this shape:\n{sql}"
        )
        assert "ROW_NUMBER() OVER" in sql, sql

    async def test_every_val_alias_here_comes_from_scopeframe(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Every ``_val_`` alias this shape emits must be one ``ScopeFrame`` minted; asserted by SET so a returning generator-local materialiser (an alias the spy never saw) fails."""
        spy = _MaterializeSpy()
        spy.install(monkeypatch)
        sql = await _gen(
            self._query(), orders_extra=self._HOST_CROSSING_COLUMN,
            dialect="postgres",
        )
        emitted = set(_re.findall(r"\b_val_\d+\b", sql))
        assert emitted, sql
        assert emitted <= set(spy.aliases), (
            f"a _val_ alias in the emitted SQL was not minted by ScopeFrame — "
            f"a second materialiser is back. emitted={sorted(emitted)} "
            f"scopeframe={sorted(set(spy.aliases))}\n{sql}"
        )
