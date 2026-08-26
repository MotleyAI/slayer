"""DEV-1739 guards + key/binder unit tests. Each guarded shape must raise
loudly (never degrade); the key layer must carry ``partition_keys`` through
identity, structural traversal, rerooting, and canonical naming."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    reroot_value_key,
)
from slayer.core.models import Column, SlayerModel
from slayer.core.query import ColumnRef, ModelMeasure, SlayerQuery, TimeDimension
from slayer.engine.binding import walk_value_keys
from slayer.sql.naming import canonical_aggregate_alias

from tests._dev1739_fixtures import gen, month_td
from tests._engine_helpers import _engine_generate


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


# --------------------------------------------------------------------------- #
# Guards (raise, don't degrade)
# --------------------------------------------------------------------------- #
class TestGrainGuards:
    async def test_non_dimension_partition_key_raises(self) -> None:
        q = _q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum(partition_by=city)")],
        )
        with pytest.raises(ValueError, match=r"partition_by.*not a query dimension") as ei:
            await gen(q)
        assert "region" in str(ei.value)  # message lists the available dims

    async def test_ambiguous_time_granularity_raises(self) -> None:
        q = _q(
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="ordered_at"),
                              granularity=TimeGranularity.MONTH),
                TimeDimension(dimension=ColumnRef(name="ordered_at"),
                              granularity=TimeGranularity.DAY),
            ],
            measures=[ModelMeasure(formula="amount:sum(partition_by=ordered_at)")],
        )
        with pytest.raises(ValueError, match=r"ambiguous"):
            await gen(q)

    async def test_cross_model_partition_outside_derived_grain_raises(self) -> None:
        q = _q(
            dimensions=["region", "customers.tier"],
            measures=[
                ModelMeasure(formula="customers.spend:sum(partition_by=region)"),
            ],
        )
        with pytest.raises(ValueError, match=r"partition_by") as ei:
            await gen(q)
        assert "customers.tier" in str(ei.value)


class TestDeferredShapeGuards:
    async def test_window_plus_partition_raises(self) -> None:
        q = _q(
            dimensions=["region"],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(window='90d', partition_by=region)"),
            ],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    @pytest.mark.parametrize("agg", ["first", "last"])
    async def test_first_last_plus_partition_raises(self, agg: str) -> None:
        q = _q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=f"amount:{agg}(partition_by=region)")],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    async def test_partitioned_aggregate_nested_in_transform_raises(self) -> None:
        q = _q(
            dimensions=["region"],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="cumsum(amount:sum(partition_by=region))"),
            ],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    async def test_partitioned_aggregate_in_filter_raises(self) -> None:
        q = _q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50"],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)


_NON_RANK_FORMULAS = {
    "cumsum": "cumsum(amount:sum, partition_by=region)",
    "lag": "lag(amount:sum, 1, partition_by=region)",
    "lead": "lead(amount:sum, 1, partition_by=region)",
    "time_shift": "time_shift(amount:sum, -1, partition_by=region)",
    "change": "change(amount:sum, partition_by=region)",
    "change_pct": "change_pct(amount:sum, partition_by=region)",
    "consecutive_periods": "consecutive_periods(amount:sum > 20, partition_by=region)",
}


class TestDriveByNonRankTransform:
    @pytest.mark.parametrize("formula", list(_NON_RANK_FORMULAS.values()),
                             ids=list(_NON_RANK_FORMULAS))
    async def test_partition_by_rejected_on_non_rank_transforms(self, formula: str) -> None:
        q = _q(
            dimensions=["region"],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula=formula)],
        )
        with pytest.raises(ValueError, match=r"partition_by"):
            await gen(q)

    async def test_partition_by_still_accepted_on_rank(self) -> None:
        sql = await gen(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="rank(amount:sum, partition_by=region)")],
        ))
        upper = sql.upper()
        assert "RANK()" in upper
        assert "PARTITION BY" in upper


# --------------------------------------------------------------------------- #
# Key identity + structural traversal + naming
# --------------------------------------------------------------------------- #
def _agg(**kw) -> AggregateKey:
    return AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum", **kw)


class TestPartitionKeyIdentity:
    def test_absent_differs_from_explicit_empty(self) -> None:
        absent = _agg()
        empty = _agg(partition_keys=frozenset())
        assert absent != empty
        assert len({absent, empty}) == 2

    def test_different_partition_sets_differ(self) -> None:
        a = _agg(partition_keys=frozenset({ColumnKey(path=(), leaf="region")}))
        b = _agg(partition_keys=frozenset({ColumnKey(path=(), leaf="city")}))
        assert a != b

    def test_same_partition_set_is_equal_and_interns(self) -> None:
        a = _agg(partition_keys=frozenset({ColumnKey(path=(), leaf="region")}))
        b = _agg(partition_keys=frozenset({ColumnKey(path=(), leaf="region")}))
        assert a == b
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    def test_absent_default_is_none(self) -> None:
        assert _agg().partition_keys is None


class TestStructuralTraversal:
    def test_walk_yields_partition_keys(self) -> None:
        region = ColumnKey(path=(), leaf="region")
        key = _agg(partition_keys=frozenset({region}))
        assert region in list(walk_value_keys(key))

    def test_reroot_strips_partition_key_prefix(self) -> None:
        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
            partition_keys=frozenset({ColumnKey(path=("customers",), leaf="tier")}),
        )
        rerooted = reroot_value_key(key, target_path=("customers",))
        assert rerooted.partition_keys == frozenset({ColumnKey(path=(), leaf="tier")})

    def test_reroot_preserves_absent_and_empty(self) -> None:
        absent = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
        )
        empty = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
            partition_keys=frozenset(),
        )
        assert reroot_value_key(absent, target_path=("customers",)).partition_keys is None
        assert reroot_value_key(empty, target_path=("customers",)).partition_keys == frozenset()


class TestCanonicalAliasSuffix:
    def _alias(self, key: AggregateKey) -> str:
        alias = canonical_aggregate_alias(key, profile="declared_name")
        assert alias is not None
        return alias

    def test_single_key_suffix(self) -> None:
        base = self._alias(_agg())
        part = self._alias(_agg(partition_keys=frozenset({ColumnKey(path=(), leaf="region")})))
        assert part == f"{base}_partition_by_region"

    def test_multi_key_suffix_is_sorted(self) -> None:
        base = self._alias(_agg())
        part = self._alias(_agg(partition_keys=frozenset({
            ColumnKey(path=(), leaf="region"),
            ColumnKey(path=(), leaf="channel"),
        })))
        assert part == f"{base}_partition_by_channel_region"

    def test_empty_set_suffix(self) -> None:
        base = self._alias(_agg())
        part = self._alias(_agg(partition_keys=frozenset()))
        assert part == f"{base}_partition_by"

    def test_dotted_key_flattened(self) -> None:
        base = self._alias(_agg())
        part = self._alias(_agg(partition_keys=frozenset({
            ColumnKey(path=("customers",), leaf="tier"),
        })))
        assert part == f"{base}_partition_by_customers_tier"


class TestNamingCollision:
    async def test_colliding_auto_names_raise(self) -> None:
        model = SlayerModel(
            name="t", data_source="test", sql_table="t",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="region", type=DataType.TEXT),
                Column(name="channel", type=DataType.TEXT),
                Column(name="channel_region", type=DataType.TEXT),
            ],
        )
        q = SlayerQuery(
            source_model="t",
            dimensions=["region", "channel", "channel_region"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=[channel, region])"),
                ModelMeasure(formula="amount:sum(partition_by=channel_region)"),
            ],
        )
        with pytest.raises(ValueError):
            await _engine_generate(query=q, model=model, dialect="duckdb", validate=False)
