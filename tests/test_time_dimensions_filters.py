"""Stage 7b.3c (DEV-1450) — date_range → filter + main-TD disambiguation.

Two pieces:

1. ``_resolve_main_time_dimension(query, model) -> Optional[TimeDimension]``
   resolves the active TD for transform / windowing semantics:
   single TD → it; multiple TDs with ``query.main_time_dimension`` →
   match by full_name or leaf; multiple TDs with model
   ``default_time_dimension`` → match by leaf; neither → None;
   unrecognised name → ``UnknownReferenceError``.

2. ``plan_query`` converts each ``TimeDimension.date_range = [start,
   end]`` into a row-phase ``BoundFilter`` on the bare underlying
   column (``column >= start AND column <= end``, matching legacy
   ``BETWEEN start AND end`` semantics). The filter binds against the
   raw ``ColumnKey``, NOT the ``TimeTruncKey`` — so the generator slice
   7b.11 can render the shifted self-join CTE on raw data while the
   outer projection applies the date filter.

``snap_to_whole_periods`` ownership stays with
``SlayerQuery.snap_to_whole_periods`` (already called pre-normalization
in ``query_engine._execute_pipeline``); the planner consumes already-
snapped queries and never re-snaps.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import AmbiguousReferenceError, UnknownReferenceError
from slayer.core.keys import (
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    LiteralKey,
    Phase,
)
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import _resolve_main_time_dimension, plan_query


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders_model(default_td: str | None = None) -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="reviewed_at", type=DataType.DATE),
        ],
        default_time_dimension=default_td,
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="signed_up_at", type=DataType.TIMESTAMP),
        ],
    )


def _orders_with_customers_join() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _bundle_local() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_model(),
        referenced_models=[],
    )


def _bundle_joined() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_with_customers_join(),
        referenced_models=[_customers_model()],
    )


# ---------------------------------------------------------------------------
# _resolve_main_time_dimension
# ---------------------------------------------------------------------------


class TestResolveMainTimeDimension:
    def test_no_time_dimensions_returns_none(self) -> None:
        model = _orders_model()
        q = SlayerQuery(source_model="orders")
        assert _resolve_main_time_dimension(query=q, model=model) is None

    def test_single_td_returns_it(self) -> None:
        model = _orders_model()
        td = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(source_model="orders", time_dimensions=[td])
        resolved = _resolve_main_time_dimension(query=q, model=model)
        assert resolved is td

    def test_multi_td_no_disambiguator_returns_none(self) -> None:
        model = _orders_model()
        td_a = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(source_model="orders", time_dimensions=[td_a, td_b])
        assert _resolve_main_time_dimension(query=q, model=model) is None

    def test_multi_td_main_time_dimension_matches_by_leaf(self) -> None:
        model = _orders_model()
        td_a = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="reviewed_at",
        )
        resolved = _resolve_main_time_dimension(query=q, model=model)
        assert resolved is td_b

    def test_multi_td_main_time_dimension_matches_by_full_name(self) -> None:
        model = _orders_with_customers_join()
        td_a = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="customers.signed_up_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="customers.signed_up_at",
        )
        resolved = _resolve_main_time_dimension(query=q, model=model)
        assert resolved is td_b

    def test_full_name_wins_over_leaf_match(self) -> None:
        # Conflict case: one local TD with leaf == `signed_up_at`, one
        # joined TD with full_name == `customers.signed_up_at`. The
        # user wrote `main_time_dimension="signed_up_at"` — the local
        # full-name match wins over the joined leaf match because
        # full_name is the more specific reference.
        model = SlayerModel(
            name="orders",
            data_source="prod",
            sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="signed_up_at", type=DataType.TIMESTAMP),
            ],
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
        td_local = TimeDimension(
            dimension=ColumnRef(name="signed_up_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_joined = TimeDimension(
            dimension=ColumnRef(name="customers.signed_up_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td_local, td_joined],
            main_time_dimension="signed_up_at",
        )
        resolved = _resolve_main_time_dimension(query=q, model=model)
        # Local full-name "signed_up_at" matches td_local exactly; the
        # joined TD only matches by leaf. Full-name wins.
        assert resolved is td_local

    def test_single_td_ignores_main_time_dimension(self) -> None:
        # Matches legacy: when there's exactly one TD, the resolver
        # returns it regardless of main_time_dimension. main_time_dimension
        # is only consulted as a disambiguator for 2+ TDs.
        model = _orders_model()
        td = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td],
            main_time_dimension="something_else",
        )
        resolved = _resolve_main_time_dimension(query=q, model=model)
        assert resolved is td

    def test_multi_td_default_time_dimension_falls_back_when_no_main(self) -> None:
        model = _orders_model(default_td="reviewed_at")
        td_a = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(source_model="orders", time_dimensions=[td_a, td_b])
        resolved = _resolve_main_time_dimension(query=q, model=model)
        assert resolved is td_b

    def test_query_main_td_wins_over_model_default(self) -> None:
        model = _orders_model(default_td="reviewed_at")
        td_a = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="created_at",
        )
        resolved = _resolve_main_time_dimension(query=q, model=model)
        assert resolved is td_a

    def test_unknown_main_time_dimension_raises(self) -> None:
        model = _orders_model()
        td_a = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="not_a_td",
        )
        with pytest.raises(UnknownReferenceError):
            _resolve_main_time_dimension(query=q, model=model)

    def test_ambiguous_leaf_match_raises(self) -> None:
        # Two joined TDs with the same leaf — ``customers.created_at``
        # and ``payments.created_at``. ``main_time_dimension="created_at"``
        # is ambiguous; the resolver must raise rather than pick the
        # first by query order.
        host = SlayerModel(
            name="orders",
            data_source="prod",
            sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="payment_id", type=DataType.INT),
            ],
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
                ModelJoin(
                    target_model="payments",
                    join_pairs=[["payment_id", "id"]],
                ),
            ],
        )
        td_a = TimeDimension(
            dimension=ColumnRef(name="customers.created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="payments.created_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="created_at",
        )
        with pytest.raises(AmbiguousReferenceError) as excinfo:
            _resolve_main_time_dimension(query=q, model=host)
        # The error should surface both candidates so the user can
        # disambiguate.
        assert "customers.created_at" in str(excinfo.value)
        assert "payments.created_at" in str(excinfo.value)

    def test_default_td_picks_host_local_over_joined(self) -> None:
        # Legacy ``_resolve_time_alias`` returns
        # f"{model.name}.{default_time_dimension}" — the host-local form.
        # When the query includes a joined TD ``customers.created_at``
        # AND a local TD ``reviewed_at``, and the host's default is set
        # to ``created_at`` (which doesn't match a local TD), the
        # resolver must return None rather than the joined match.
        model = _orders_with_customers_join()
        model = model.model_copy(update={"default_time_dimension": "created_at"})
        td_joined = TimeDimension(
            dimension=ColumnRef(name="customers.signed_up_at"),
            granularity=TimeGranularity.MONTH,
        )
        # No local TD whose leaf matches "created_at" in the query —
        # legacy would return f"orders.created_at" which doesn't exist
        # in time_dimensions[*]. Our new pipeline returns None.
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td_joined],
            # Add a second TD so the resolver doesn't auto-return td_joined.
            # Use reviewed_at — but wait, reviewed_at doesn't exist on
            # _orders_with_customers_join. Add it to the fixture below.
        )
        # With only one TD, the helper returns that TD (single-TD path
        # bypasses default_time_dimension). To exercise the default
        # path we need 2+ TDs. Build a fresh model + bundle inline:
        host = SlayerModel(
            name="orders",
            data_source="prod",
            sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="reviewed_at", type=DataType.TIMESTAMP),
            ],
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
            default_time_dimension="created_at",  # doesn't match any LOCAL TD
        )
        td_local = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_joined2 = TimeDimension(
            dimension=ColumnRef(name="customers.signed_up_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[td_local, td_joined2],
        )
        # ``created_at`` doesn't match any local-only TD; the only TDs
        # with leaf == "created_at" are joined, and legacy never returns
        # a joined TD via the default path. Result: None.
        resolved = _resolve_main_time_dimension(query=q, model=host)
        assert resolved is None

    def test_default_td_matches_local_td(self) -> None:
        # Sanity check the host-local default path still works.
        host = SlayerModel(
            name="orders",
            data_source="prod",
            sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="created_at", type=DataType.TIMESTAMP),
                Column(name="reviewed_at", type=DataType.TIMESTAMP),
            ],
            default_time_dimension="created_at",
        )
        td_a = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(source_model="orders", time_dimensions=[td_a, td_b])
        resolved = _resolve_main_time_dimension(query=q, model=host)
        assert resolved is td_a

    def test_unknown_default_td_does_not_raise_but_returns_none(self) -> None:
        # If model.default_time_dimension is set but doesn't match any
        # entry in query.time_dimensions, the resolver returns None
        # (legacy behaviour — the default points at a column the user
        # didn't include this query).
        model = _orders_model(default_td="not_in_query")
        td_a = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(source_model="orders", time_dimensions=[td_a, td_b])
        assert _resolve_main_time_dimension(query=q, model=model) is None


# ---------------------------------------------------------------------------
# date_range → BoundFilter
# ---------------------------------------------------------------------------


def _find_date_range_filter_on(*, planned, leaf: str) -> BetweenKey:
    """Find the auto-generated date_range filter for a given column leaf.

    The planner emits exactly one filter per TD with a date_range; its
    top-level shape is ``BetweenKey(column=ColumnKey, low=LiteralKey,
    high=LiteralKey)`` (DEV-1450 stage 7b.9 — closes the parity gap
    with legacy ``BETWEEN``).
    """
    for f in planned.filters_by_phase:
        if f.expression is None:
            continue
        key = f.expression.value_key
        if (
            isinstance(key, BetweenKey)
            and isinstance(key.column, ColumnKey)
            and key.column.leaf == leaf
        ):
            return key
    raise AssertionError(
        f"no BetweenKey filter found over column {leaf!r}; "
        f"filters present: "
        f"{[type(f.expression.value_key).__name__ if f.expression else None for f in planned.filters_by_phase]}"
    )


class TestDateRangeFilter:
    def test_date_range_emits_filter_on_underlying_column(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        bk = _find_date_range_filter_on(planned=planned, leaf="created_at")
        # The BetweenKey targets the BARE underlying column, not the
        # TimeTruncKey — so generator slice 7b.11 can apply the filter on
        # the outer projection while the shifted self-join CTE reads raw.
        assert bk.column == ColumnKey(path=(), leaf="created_at")
        # Literal bounds match the date_range strings verbatim — they
        # flow through ``normalize_scalar`` which leaves strings alone.
        assert isinstance(bk.low, LiteralKey)
        assert bk.low.value == "2024-01-01"
        assert isinstance(bk.high, LiteralKey)
        assert bk.high.value == "2024-03-01"

    def test_date_range_filter_is_row_phase(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        # Exactly one filter — the date_range — phase=ROW.
        assert len(planned.filters_by_phase) == 1
        assert planned.filters_by_phase[0].phase == Phase.ROW

    def test_no_date_range_no_filter(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        assert planned.filters_by_phase == []

    def test_date_range_with_joined_td(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="customers.signed_up_at"),
                    granularity=TimeGranularity.DAY,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_joined())
        bk = _find_date_range_filter_on(
            planned=planned, leaf="signed_up_at",
        )
        # Joined TD: path carries the join hop on the underlying column.
        expected_col = ColumnKey(path=("customers",), leaf="signed_up_at")
        assert bk.column == expected_col

    def test_user_filter_and_date_range_coexist(self) -> None:
        # A user-supplied filter and the auto-generated date_range filter
        # both appear, both row-phase. DEV-1450 stage 7b.9 enforces
        # legacy WHERE order: date_range first, user filter second.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            filters=["amount > 0"],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        row_filters = [
            f for f in planned.filters_by_phase if f.phase == Phase.ROW
        ]
        assert len(row_filters) == 2
        # date_range filter first (BetweenKey shape).
        first = row_filters[0]
        assert first.expression is not None
        assert isinstance(first.expression.value_key, BetweenKey)
        # user filter last (ArithmeticKey > 0 shape).
        last = row_filters[-1]
        assert last.expression is not None
        assert isinstance(last.expression.value_key, ArithmeticKey)
        assert last.expression.value_key.op == ">"

    def test_multiple_tds_each_with_date_range(self) -> None:
        # Two TDs, each with date_range — two separate filters, one per
        # underlying column.
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
                TimeDimension(
                    dimension=ColumnRef(name="reviewed_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-02-01", "2024-04-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        # Two row-phase auto-generated filters.
        row_filters = [
            f for f in planned.filters_by_phase if f.phase == Phase.ROW
        ]
        assert len(row_filters) == 2
        # Each one targets a different underlying column.
        and_created = _find_date_range_filter_on(
            planned=planned, leaf="created_at",
        )
        and_reviewed = _find_date_range_filter_on(
            planned=planned, leaf="reviewed_at",
        )
        assert and_created is not and_reviewed

    def test_malformed_date_range_empty_emits_no_filter(self) -> None:
        # Legacy: `if td.date_range and len(td.date_range) == 2` —
        # malformed entries silently no-op. Match that.
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=[],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        assert planned.filters_by_phase == []

    def test_malformed_date_range_single_element_emits_no_filter(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        assert planned.filters_by_phase == []

    def test_date_range_filter_carries_expression(self) -> None:
        # The FilterPhase.expression payload must be populated so the
        # generator can render the filter without re-parsing.
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        assert len(planned.filters_by_phase) == 1
        fp = planned.filters_by_phase[0]
        assert fp.expression is not None
        assert isinstance(fp.expression.value_key, BetweenKey)


# ---------------------------------------------------------------------------
# date_range filter on the underlying column (NOT TimeTruncKey)
# ---------------------------------------------------------------------------


class TestDateRangeHiddenSlot:
    def test_raw_column_slot_materialized_hidden(self) -> None:
        # The date_range filter binds against the raw ColumnKey. For
        # the generator to render that column expression independently
        # of the TimeTruncKey (so the outer WHERE references the raw
        # column while the truncated form goes in the SELECT), the
        # planner MUST materialise the raw ColumnKey as a hidden slot
        # alongside the public TimeTruncKey slot — they are two
        # distinct slot identities even though they target the same
        # underlying column.
        from slayer.core.keys import TimeTruncKey

        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())

        # Public projection: exactly one slot — the TimeTruncKey.
        assert len(planned.projection) == 1
        all_slots = (
            planned.row_slots
            + planned.aggregate_slots
            + planned.combined_expression_slots
        )
        td_slots = [s for s in all_slots if isinstance(s.key, TimeTruncKey)]
        col_slots = [
            s for s in all_slots
            if isinstance(s.key, ColumnKey)
            and s.key == ColumnKey(path=(), leaf="created_at")
        ]
        assert len(td_slots) == 1, td_slots
        assert td_slots[0].hidden is False
        assert len(col_slots) == 1, col_slots
        assert col_slots[0].hidden is True


class TestDateRangeBindsToUnderlyingColumn:
    def test_filter_does_not_reference_timetrunckey(self) -> None:
        # The auto-generated date_range filter MUST bind against the
        # raw ColumnKey, not the TimeTruncKey — that lets the generator
        # apply the filter to the outer projection while the shifted
        # self-join input reads unfiltered raw data (legacy semantics
        # for change / change_pct / time_shift on edge periods).
        from slayer.core.keys import TimeTruncKey

        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        fp = planned.filters_by_phase[0]
        assert fp.expression is not None
        # Walk the predicate; no TimeTruncKey should appear in the tree.
        from slayer.engine.binding import walk_value_keys

        keys = list(walk_value_keys(fp.expression.value_key))
        assert not any(isinstance(k, TimeTruncKey) for k in keys), (
            f"date_range filter should target raw column, not TimeTruncKey; "
            f"got keys: {[type(k).__name__ for k in keys]}"
        )
