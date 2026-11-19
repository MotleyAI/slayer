"""Fail-closed guards for equivalent time dimensions (Option A).

Two same-column buckets are now legal, so equivalence decided by column-name text
leaves two silent-wrong holes; both fail closed here until DEV-1925 resolves by bound
``TimeTruncKey`` identity:
  * a bare ``main_time_dimension`` naming a column with several granularities;
  * time dimensions that bind to one ``TimeTruncKey`` but disagree on date range/label.
"""
import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.errors import AmbiguousReferenceError, GranularityCallError
from slayer.core.keys import FieldKey, TimeTruncKey
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.engine.bind_inputs import (
    _assert_equivalent_tds_agree,
    _resolve_main_time_dimension,
)
from slayer.ir.bound import BoundExpr
from tests import _dev1883_fixtures as fx
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture
async def engine(tmp_path):
    return await fx.build_engine(tmp_path)


def _mtd(*grans_and_cols: tuple) -> SlayerQuery:
    return fx.q(
        population="orders",
        time_dimensions=[{"field": c, "granularity": g} for g, c in grans_and_cols],
        metrics=[{"metric": "sum(orders.amount)"}],
        main_time_dimension="orders.created_at",
    )


async def _orders_dataset(engine: SlayerQueryEngine, **overrides):
    """The orders model (asserted present), with optional field overrides applied."""
    model = await engine.storage.get_dataset("orders")
    assert model is not None
    return model.model_copy(update=overrides) if overrides else model


class TestMainTimeDimensionAmbiguity:
    async def test_two_same_column_buckets_raise_via_execute(self, engine) -> None:
        q = fx.q(
            population="orders",
            dimensions=["date_trunc('month', orders.created_at)", "date_trunc('year', orders.created_at)"],
            main_time_dimension="orders.created_at",
            metrics=[{"metric": "cumsum(sum(orders.amount))", "alias": "cs"}],
        )
        with pytest.raises(AmbiguousReferenceError) as ei:
            await engine.execute(q, dry_run=True)
        msg = str(ei.value)
        assert "date_trunc('month', created_at)" in msg
        assert "date_trunc('year', created_at)" in msg

    async def test_unit_resolve_raises_on_same_column_buckets(self, engine) -> None:
        model = await _orders_dataset(engine)
        q = _mtd(("month", "created_at"), ("year", "created_at"))
        with pytest.raises(AmbiguousReferenceError):
            _resolve_main_time_dimension(query=q, dataset=model)

    async def test_single_bucket_still_resolves(self, engine) -> None:
        model = await _orders_dataset(engine)
        td = TimeDimension(field="orders.created_at", granularity=TimeGranularity.MONTH)
        q = SlayerQuery(population="orders", time_dimensions=[td], main_time_dimension="orders.created_at")
        # A lone TD ignores main_time_dimension and resolves regardless.
        assert _resolve_main_time_dimension(query=q, dataset=model) is td

    async def test_distinct_columns_still_resolve(self, engine) -> None:
        model = await _orders_dataset(engine)
        q = _mtd(("month", "created_at"), ("year", "customers.created_at"))
        resolved = _resolve_main_time_dimension(query=q, dataset=model)
        assert resolved is not None
        assert resolved.field == "created_at"

    async def test_ambiguous_default_resolves_to_none(self, engine) -> None:
        # A passive model default can't pick a bucket: resolve to None (lenient) so
        # no-transform queries still run; a transform then fails at point of use.
        model = await _orders_dataset(engine, default_time_dimension="created_at")
        q = fx.q(
            population="orders",
            time_dimensions=[
                {"field": "orders.created_at", "granularity": "month"},
                {"field": "orders.created_at", "granularity": "year"},
            ],
            metrics=[{"metric": "sum(orders.amount)"}],
        )
        assert _resolve_main_time_dimension(query=q, dataset=model) is None

    async def test_default_time_dimension_single_host_match_resolves(self, engine) -> None:
        model = await _orders_dataset(engine, default_time_dimension="created_at")
        q = fx.q(
            population="orders",
            time_dimensions=[
                {"field": "orders.created_at", "granularity": "month"},
                {"field": "customers.created_at", "granularity": "year"},
            ],
            metrics=[{"metric": "sum(orders.amount)"}],
        )
        resolved = _resolve_main_time_dimension(query=q, dataset=model)
        assert resolved is not None
        assert resolved.field == "created_at"

    async def _orders_with_default_axis(self, tmp_path):
        engine = await fx.build_engine(tmp_path)
        await engine.storage.save_dataset(
            await _orders_dataset(engine, default_time_dimension="created_at")
        )
        return engine

    async def test_ambiguous_default_transform_fails_closed(self, tmp_path) -> None:
        engine = await self._orders_with_default_axis(tmp_path)
        q = fx.q(
            population="orders",
            time_dimensions=[
                {"field": "orders.created_at", "granularity": "month"},
                {"field": "orders.created_at", "granularity": "year"},
            ],
            metrics=[{"metric": "cumsum(sum(orders.amount))", "alias": "cs"}],
        )
        with pytest.raises(ValueError, match="unambiguous time dimension"):
            await engine.execute(q, dry_run=True)

    async def test_ambiguous_default_without_transform_runs(self, tmp_path) -> None:
        engine = await self._orders_with_default_axis(tmp_path)
        q = fx.q(
            population="orders",
            time_dimensions=[
                {"field": "orders.created_at", "granularity": "month"},
                {"field": "orders.created_at", "granularity": "year"},
            ],
            metrics=[{"metric": "sum(orders.amount)"}],
        )
        resp = await engine.execute(q, dry_run=True)
        assert resp.sql


def _bound_td(col: str, gran: str, key: TimeTruncKey, **meta) -> tuple:
    td = TimeDimension(field=col, granularity=TimeGranularity(gran), **meta)
    return (td, BoundExpr(value_key=key), col)


class TestEquivalentTimeDimensionMetadataGuard:
    K_MONTH = TimeTruncKey(source=FieldKey(leaf="created_at"), granularity=TimeGranularity.MONTH)
    K_YEAR = TimeTruncKey(source=FieldKey(leaf="created_at"), granularity=TimeGranularity.YEAR)

    def test_conflicting_date_range_raises(self) -> None:
        tds = [
            _bound_td("created_at", "month", self.K_MONTH, date_range=["2024-01-01", "2024-12-31"]),
            _bound_td("orders.created_at", "month", self.K_MONTH, date_range=["2025-01-01", "2025-12-31"]),
        ]
        with pytest.raises(GranularityCallError, match="date range"):
            _assert_equivalent_tds_agree(tds)

    def test_conflicting_label_raises(self) -> None:
        tds = [
            _bound_td("created_at", "month", self.K_MONTH, label="A"),
            _bound_td("orders.created_at", "month", self.K_MONTH, label="B"),
        ]
        with pytest.raises(GranularityCallError):
            _assert_equivalent_tds_agree(tds)

    def test_agreeing_metadata_passes(self) -> None:
        _assert_equivalent_tds_agree([
            _bound_td("created_at", "month", self.K_MONTH, date_range=["2024-01-01", "2024-12-31"]),
            _bound_td("orders.created_at", "month", self.K_MONTH, date_range=["2024-01-01", "2024-12-31"]),
        ])

    def test_distinct_keys_pass(self) -> None:
        _assert_equivalent_tds_agree([
            _bound_td("created_at", "month", self.K_MONTH, date_range=["2024-01-01", "2024-12-31"]),
            _bound_td("created_at", "year", self.K_YEAR, date_range=["2025-01-01", "2025-12-31"]),
        ])
