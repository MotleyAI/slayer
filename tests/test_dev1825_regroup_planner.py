"""DEV-1825 — regroup-planner contracts beyond the DEV-1740 B2 acceptance:
placeholder minting is injective by structural key identity, one producer
serves N outputs, the filter classifier is total over AND/OR/NOT/scalar-call
trees, and the discovery guards (joined aggregate source, reserved-prefix
column) fail loudly.
"""

from __future__ import annotations

import re

import pytest

from slayer.core.keys import AggregateKey, ColumnKey, SqlExprKey
from slayer.core.query import ModelMeasure, SlayerQuery
from slayer.engine.regroup_planner import (
    REGROUP_LEAF_PREFIX,
    RegroupPlaceholderRegistry,
)

from tests._dev1740_fixtures import cm_cte_bodies, gen

BAND = "CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END"


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _count_isolated_ctes(sql: str) -> int:
    return len({m.group(1) for m in re.finditer(r"(_cm_\w+)\s+AS\s*\(", sql)})


def _consumer_body(sql: str) -> str:
    """The statement with every ``_cm_*`` CTE body removed — i.e. what the
    final consumer SELECT sees (so a token there is routed to the consumer,
    not the producer)."""
    return sql.replace(cm_cte_bodies(sql), "")


# --------------------------------------------------------------------------- #
# Placeholder registry — injective by STRUCTURAL identity (Codex F2): the
# canonical alias omits column_filter_key, so two distinct keys can share it.
# --------------------------------------------------------------------------- #
class TestPlaceholderRegistry:
    def _keys(self):
        base = dict(
            source=ColumnKey(path=(), leaf="amount"),
            agg="sum",
            partition_keys=frozenset({ColumnKey(path=(), leaf="city")}),
        )
        plain = AggregateKey(**base)
        filtered = AggregateKey(
            **base,
            column_filter_key=SqlExprKey(
                canonical_sql="status = 'ok'", referenced_join_paths=(),
            ),
        )
        return plain, filtered

    def test_distinct_keys_sharing_a_canonical_alias_get_distinct_leaves(self) -> None:
        plain, filtered = self._keys()
        reg = RegroupPlaceholderRegistry()
        a, b = reg.placeholder_for(plain), reg.placeholder_for(filtered)
        assert a != b
        assert a.leaf != b.leaf
        for p in (a, b):
            assert isinstance(p, ColumnKey)
            assert p.path == ()
            # Codex F2 exact format: deterministic index + readable seed.
            assert re.fullmatch(re.escape(REGROUP_LEAF_PREFIX) + r"\d+__.+", p.leaf)

    def test_indices_are_sequential_by_first_mint(self) -> None:
        plain, filtered = self._keys()
        reg = RegroupPlaceholderRegistry()
        first = reg.placeholder_for(plain)
        second = reg.placeholder_for(filtered)
        idx = re.compile(re.escape(REGROUP_LEAF_PREFIX) + r"(\d+)__")
        assert idx.match(first.leaf).group(1) == "0"
        assert idx.match(second.leaf).group(1) == "1"

    def test_same_key_mints_one_placeholder(self) -> None:
        plain, _ = self._keys()
        reg = RegroupPlaceholderRegistry()
        first = reg.placeholder_for(plain)
        second = reg.placeholder_for(plain)
        assert first == second

    def test_minting_is_deterministic_across_registries(self) -> None:
        plain, filtered = self._keys()
        one = RegroupPlaceholderRegistry()
        two = RegroupPlaceholderRegistry()
        assert [one.placeholder_for(plain), one.placeholder_for(filtered)] == [
            two.placeholder_for(plain), two.placeholder_for(filtered),
        ]


# --------------------------------------------------------------------------- #
# Multi-output producers (Codex F5) — the IR's defining case: one partition
# set, N aggregate outputs, ONE synthesized stage.
# --------------------------------------------------------------------------- #
class TestMultiOutputProducer:
    async def test_two_aggregates_one_partition_set_share_one_cte(self) -> None:
        band = ("CASE WHEN amount:sum(partition_by=city) > 5000 "
                "AND amount:count(partition_by=city) > 1 THEN 1 ELSE 0 END")
        sql = await gen(_q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert _count_isolated_ctes(sql) == 1

    async def test_two_dimensions_share_one_producer(self) -> None:
        sql = await gen(_q(
            dimensions=[
                "region",
                {"expression": BAND, "name": "band_hi"},
                {"expression": "CASE WHEN amount:count(partition_by=city) > 1 "
                               "THEN 1 ELSE 0 END", "name": "band_n"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert _count_isolated_ctes(sql) == 1


# --------------------------------------------------------------------------- #
# Filter classifier (Codex F3) — total over the PRE-substitution tree.
# --------------------------------------------------------------------------- #
class TestFilterClassifier:
    async def test_not_over_computed_dim_stays_final(self) -> None:
        sql = await gen(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["not (band == 1)"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert "5000" not in cm_cte_bodies(sql)

    async def test_mixed_and_in_one_filter_raises_directive(self) -> None:
        query = _q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["band == 1 and status == 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(NotImplementedError, match=r"separate filters"):
            await gen(query)

    async def test_mixed_or_in_one_filter_raises_directive(self) -> None:
        query = _q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["band == 1 or status == 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(NotImplementedError, match=r"separate filters"):
            await gen(query)

    async def test_scalar_call_row_filter_copies_into_producer(self) -> None:
        sql = await gen(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["coalesce(city, 'x') != 'Lyon'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert "Lyon" in cm_cte_bodies(sql)

    async def test_base_row_filter_copied_into_producer_AND_retained_in_consumer(self) -> None:
        # Classifier branch (a): a ROW predicate must appear in BOTH the
        # producer CTE (so per-city totals see only ok rows) AND the consumer
        # (so the final GROUP BY counts only ok rows).
        sql = await gen(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["status == 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert "status" in cm_cte_bodies(sql)
        assert "status" in _consumer_body(sql)

    async def test_plain_aggregate_filter_routes_to_having_not_producer(self) -> None:
        # AGGREGATE-phase filter with NO dim-aggregate — untouched routing (d):
        # a HAVING on the final stage, never inside the producer.
        sql = await gen(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["amount:sum > 0"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert "> 0" not in cm_cte_bodies(sql)
        assert "HAVING" in _consumer_body(sql).upper()


# --------------------------------------------------------------------------- #
# Discovery guards.
# --------------------------------------------------------------------------- #
class TestDiscoveryGuards:
    async def test_joined_aggregate_source_raises_dev1824(self) -> None:
        # The SOURCE crosses a join (would need a target-rooted producer —
        # Codex F1); distinct from the supported joined PARTITION KEY.
        q = _q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN customers.spend:sum(partition_by=city) "
                               "> 100 THEN 1 ELSE 0 END", "name": "band"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    async def test_reserved_prefix_column_rejected_when_regroup_active(self) -> None:
        q = SlayerQuery(
            source_model={
                "source_name": "orders",
                "columns": [{"name": "__regroup__x", "sql": "amount",
                             "type": "DOUBLE"}],
            },
            dimensions=["region", {"expression": BAND, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(ValueError, match=r"__regroup__"):
            await gen(q)

    async def test_reserved_prefix_column_fine_without_regroup(self) -> None:
        q = SlayerQuery(
            source_model={
                "source_name": "orders",
                "columns": [{"name": "__regroup__x", "sql": "amount",
                             "type": "DOUBLE"}],
            },
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        sql = await gen(q)
        assert "SELECT" in sql
