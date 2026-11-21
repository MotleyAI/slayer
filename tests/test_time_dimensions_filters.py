"""date_range → filter + main-TD disambiguation.

Two pieces:

1. ``_resolve_main_time_dimension(query, dataset) -> Optional[TimeDimension]``
   resolves the active TD for transform / windowing semantics:
   single TD → it; multiple TDs with ``query.main_time_dimension`` →
   match by full_name or leaf; multiple TDs with dataset
   ``default_time_dimension`` → match by leaf; neither → None;
   unrecognised name → ``UnknownReferenceError``.

2. ``plan_query`` converts each ``TimeDimension.date_range = [start,
   end]`` into a row-phase ``BoundFilter`` on the bare underlying
   field (``field >= start(start) AND field < next_start(end)``). The
   filter binds against the raw ``FieldKey``, NOT the ``TimeTruncKey`` —
   so the shifted self-join CTE reads raw data while the outer projection
   applies the date filter.
"""

from __future__ import annotations

from datetime import date, datetime

import pydantic
import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import AmbiguousReferenceError, UnknownReferenceError
from slayer.core.keys import ArithmeticKey, FieldKey, LiteralKey, Phase, walk_value_keys
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.bind_inputs import _resolve_main_time_dimension
from slayer.engine.plan import plan_query
from slayer.core.keys import TimeTruncKey
from slayer.sql.generator import _lower_positions


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders_dataset(default_td: str | None = None) -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(name="reviewed_at", datatype=DataType.DATE),
        ],
        default_time_dimension=default_td,
    )


def _customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        data_source="prod",
        sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="signed_up_at", datatype=DataType.DATETIME),
        ],
    )


def _orders_with_customers_relationship() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
        ],
    )


def _bundle_local() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres",
        population=_orders_dataset(),
        referenced_datasets=[],
    )


def _bundle_joined() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres",
        population=_orders_with_customers_relationship(),
        referenced_datasets=[_customers_dataset()],
    )


# ---------------------------------------------------------------------------
# _resolve_main_time_dimension
# ---------------------------------------------------------------------------


class TestResolveMainTimeDimension:
    def test_no_time_dimensions_returns_none(self) -> None:
        model = _orders_dataset()
        q = SlayerQuery(population="orders")
        assert _resolve_main_time_dimension(query=q, dataset=model, root="orders") is None

    def test_single_td_returns_it(self) -> None:
        model = _orders_dataset()
        td = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(population="orders", time_dimensions=[td])
        resolved = _resolve_main_time_dimension(query=q, dataset=model, root="orders")
        assert resolved is td

    def test_multi_td_no_disambiguator_returns_none(self) -> None:
        model = _orders_dataset()
        td_a = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="orders.reviewed_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(population="orders", time_dimensions=[td_a, td_b])
        assert _resolve_main_time_dimension(query=q, dataset=model, root="orders") is None

    def test_multi_td_main_time_dimension_matches_by_leaf(self) -> None:
        model = _orders_dataset()
        td_a = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="orders.reviewed_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="orders.reviewed_at",
        )
        resolved = _resolve_main_time_dimension(query=q, dataset=model, root="orders")
        assert resolved is td_b

    def test_multi_td_main_time_dimension_matches_by_full_name(self) -> None:
        model = _orders_with_customers_relationship()
        td_a = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="customers.signed_up_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="customers.signed_up_at",
        )
        resolved = _resolve_main_time_dimension(query=q, dataset=model, root="orders")
        assert resolved is td_b

    def test_full_name_wins_over_leaf_match(self) -> None:
        # Conflict case: one local TD with leaf == `signed_up_at`, one
        # joined TD with full_name == `customers.signed_up_at`. The
        # user wrote `main_time_dimension="signed_up_at"` — the local
        # full-name match wins over the joined leaf match because
        # full_name is the more specific reference.
        model = SlayerDataset(
            name="orders",
            data_source="prod",
            sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="signed_up_at", datatype=DataType.DATETIME),
            ],
            relationships=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],
                ),
            ],
        )
        td_local = TimeDimension(
            field="orders.signed_up_at",
            granularity=TimeGranularity.MONTH,
        )
        td_joined = TimeDimension(
            field="customers.signed_up_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td_local, td_joined],
            main_time_dimension="orders.signed_up_at",
        )
        resolved = _resolve_main_time_dimension(query=q, dataset=model, root="orders")
        # Local full-name "signed_up_at" matches td_local exactly; the
        # joined TD only matches by leaf. Full-name wins.
        assert resolved is td_local

    def test_single_td_ignores_main_time_dimension(self) -> None:
        # Matches legacy: when there's exactly one TD, the resolver
        # returns it regardless of main_time_dimension. main_time_dimension
        # is only consulted as a disambiguator for 2+ TDs.
        model = _orders_dataset()
        td = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td],
            main_time_dimension="orders.something_else",
        )
        resolved = _resolve_main_time_dimension(query=q, dataset=model, root="orders")
        assert resolved is td

    def test_multi_td_default_time_dimension_falls_back_when_no_main(self) -> None:
        model = _orders_dataset(default_td="reviewed_at")
        td_a = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="orders.reviewed_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(population="orders", time_dimensions=[td_a, td_b])
        resolved = _resolve_main_time_dimension(query=q, dataset=model, root="orders")
        assert resolved is td_b

    def test_query_main_td_wins_over_model_default(self) -> None:
        model = _orders_dataset(default_td="reviewed_at")
        td_a = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="orders.reviewed_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="orders.created_at",
        )
        resolved = _resolve_main_time_dimension(query=q, dataset=model, root="orders")
        assert resolved is td_a

    def test_unknown_main_time_dimension_raises(self) -> None:
        model = _orders_dataset()
        td_a = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="orders.reviewed_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="orders.not_a_td",
        )
        with pytest.raises(UnknownReferenceError):
            _resolve_main_time_dimension(query=q, dataset=model, root="orders")

    def test_ambiguous_leaf_match_raises(self) -> None:
        # Two joined TDs with the same leaf — ``customers.created_at``
        # and ``payments.created_at``. ``main_time_dimension="created_at"``
        # is ambiguous; the resolver must raise rather than pick the
        # first by query order.
        host = SlayerDataset(
            name="orders",
            data_source="prod",
            sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="payment_id", datatype=DataType.INTEGER),
            ],
            relationships=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],
                ),
                DatasetRelationship(
                    to="payments",
                    from_columns=["payment_id"], to_columns=["id"],
                ),
            ],
        )
        td_a = TimeDimension(
            field="customers.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="payments.created_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td_a, td_b],
            main_time_dimension="created_at",
        )
        with pytest.raises(AmbiguousReferenceError) as excinfo:
            _resolve_main_time_dimension(query=q, dataset=host, root="orders")
        # The error should surface both candidates so the user can
        # disambiguate.
        assert "customers.created_at" in str(excinfo.value)
        assert "payments.created_at" in str(excinfo.value)

    def test_default_td_picks_host_local_over_joined(self) -> None:
        # Legacy ``_resolve_time_alias`` returns
        # f"{dataset.name}.{default_time_dimension}" — the host-local form.
        # When the query includes a joined TD ``customers.created_at``
        # AND a local TD ``reviewed_at``, and the host's default is set
        # to ``created_at`` (which doesn't match a local TD), the
        # resolver must return None rather than the joined match.
        model = _orders_with_customers_relationship()
        model = model.model_copy(update={"default_time_dimension": "created_at"})
        td_joined = TimeDimension(
            field="customers.signed_up_at",
            granularity=TimeGranularity.MONTH,
        )
        # No local TD whose leaf matches "created_at" in the query —
        # legacy would return f"orders.created_at" which doesn't exist
        # in time_dimensions[*]. Our new pipeline returns None.
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td_joined],
            # Add a second TD so the resolver doesn't auto-return td_joined.
            # Use reviewed_at — but wait, reviewed_at doesn't exist on
            # _orders_with_customers_join. Add it to the fixture below.
        )
        # With only one TD, the helper returns that TD (single-TD path
        # bypasses default_time_dimension). To exercise the default
        # path we need 2+ TDs. Build a fresh dataset + bundle inline:
        host = SlayerDataset(
            name="orders",
            data_source="prod",
            sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="reviewed_at", datatype=DataType.DATETIME),
            ],
            relationships=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],
                ),
            ],
            default_time_dimension="created_at",  # doesn't match any LOCAL TD
        )
        td_local = TimeDimension(
            field="orders.reviewed_at",
            granularity=TimeGranularity.MONTH,
        )
        td_joined2 = TimeDimension(
            field="customers.signed_up_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(
            population="orders",
            time_dimensions=[td_local, td_joined2],
        )
        # ``created_at`` doesn't match any local-only TD; the only TDs
        # with leaf == "created_at" are joined, and legacy never returns
        # a joined TD via the default path. Result: None.
        resolved = _resolve_main_time_dimension(query=q, dataset=host, root="orders")
        assert resolved is None

    def test_default_td_matches_local_td(self) -> None:
        # Sanity check the host-local default path still works.
        host = SlayerDataset(
            name="orders",
            data_source="prod",
            sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="created_at", datatype=DataType.DATETIME),
                DatasetField(name="reviewed_at", datatype=DataType.DATETIME),
            ],
            default_time_dimension="created_at",
        )
        td_a = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="orders.reviewed_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(population="orders", time_dimensions=[td_a, td_b])
        resolved = _resolve_main_time_dimension(query=q, dataset=host, root="orders")
        assert resolved is td_a

    def test_unknown_default_td_does_not_raise_but_returns_none(self) -> None:
        # If dataset.default_time_dimension is set but doesn't match any
        # entry in query.time_dimensions, the resolver returns None
        # (legacy behaviour — the default points at a field the user
        # didn't include this query).
        model = _orders_dataset(default_td="not_in_query")
        td_a = TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
        )
        td_b = TimeDimension(
            field="orders.reviewed_at",
            granularity=TimeGranularity.MONTH,
        )
        q = SlayerQuery(population="orders", time_dimensions=[td_a, td_b])
        assert _resolve_main_time_dimension(query=q, dataset=model, root="orders") is None


# ---------------------------------------------------------------------------
# date_range → BoundFilter
# ---------------------------------------------------------------------------


def _plan_filters(planned):
    """The placed-filter view is reconstructed by the emission lowering."""
    return _lower_positions(planned).filters


def _find_date_range_filter_on(*, planned, leaf: str) -> tuple[ArithmeticKey, ArithmeticKey]:
    """The ``(>= lower, < upper)`` comparisons of the one date_range filter on ``leaf``."""
    for f in _plan_filters(planned):
        if f.expression is None:
            continue
        key = f.expression.value_key
        if not (isinstance(key, ArithmeticKey) and key.op == "and" and len(key.operands) == 2):
            continue
        lower, upper = key.operands
        if (
            isinstance(lower, ArithmeticKey) and isinstance(upper, ArithmeticKey)
            and (lower.op, upper.op) == (">=", "<")
            and isinstance(lower.operands[0], FieldKey) and lower.operands[0].leaf == leaf
            and upper.operands[0] == lower.operands[0]
        ):
            return lower, upper
    raise AssertionError(
        f"no date_range bound pair found over column {leaf!r}; "
        f"filters present: "
        f"{[type(f.expression.value_key).__name__ if f.expression else None for f in _plan_filters(planned)]}"
    )


class TestDateRangeFilter:
    def test_date_range_emits_filter_on_underlying_column(self) -> None:
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        lower, upper = _find_date_range_filter_on(planned=planned, leaf="created_at")
        # The bound targets the BARE underlying field, not the TimeTruncKey.
        assert lower.operands[0] == FieldKey(path=(), leaf="created_at")
        # TIMESTAMP operand: datetime literals; the day-period upper bound is exclusive.
        assert lower.operands[1] == LiteralKey(value=datetime(2024, 1, 1))
        assert upper.operands[1] == LiteralKey(value=datetime(2024, 3, 2))

    def test_date_operand_gets_date_literals(self) -> None:
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.reviewed_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        lower, upper = _find_date_range_filter_on(planned=planned, leaf="reviewed_at")
        assert lower.operands[1] == LiteralKey(value=date(2024, 1, 1))
        assert upper.operands[1] == LiteralKey(value=date(2024, 4, 1))

    def test_date_range_filter_is_row_phase(self) -> None:
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        # Exactly one filter — the date_range — phase=ROW.
        assert len(_plan_filters(planned)) == 1
        assert _plan_filters(planned)[0].phase == Phase.ROW

    def test_no_date_range_no_filter(self) -> None:
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        assert _plan_filters(planned) == []

    def test_date_range_with_joined_td(self) -> None:
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="customers.signed_up_at",
                    granularity=TimeGranularity.DAY,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_joined())
        lower, _upper = _find_date_range_filter_on(
            planned=planned, leaf="signed_up_at",
        )
        # Joined TD: path carries the relationship hop on the underlying field.
        expected_col = FieldKey(path=("customers",), leaf="signed_up_at")
        assert lower.operands[0] == expected_col

    def test_user_filter_and_date_range_coexist(self) -> None:
        # A user-supplied filter and the auto-generated date_range filter
        # both appear, both row-phase, in the
        # legacy WHERE order: date_range first, user filter second.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.amount)"}],
            "filter": ["orders.amount > 0"],
            "time_dimensions": [
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        })
        planned = plan_query(query=q, bundle=_bundle_local())
        row_filters = [
            f for f in _plan_filters(planned) if f.phase == Phase.ROW
        ]
        assert len(row_filters) == 2
        # date_range filter first (the half-open pair).
        first = row_filters[0]
        assert first.expression is not None
        assert isinstance(first.expression.value_key, ArithmeticKey)
        assert first.expression.value_key.op == "and"
        # user filter last (ArithmeticKey > 0 shape).
        last = row_filters[-1]
        assert last.expression is not None
        assert isinstance(last.expression.value_key, ArithmeticKey)
        assert last.expression.value_key.op == ">"

    def test_multiple_tds_each_with_date_range(self) -> None:
        # Two TDs, each with date_range — two separate filters, one per
        # underlying field.
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
                TimeDimension(
                    field="orders.reviewed_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-02-01", "2024-04-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        # Two row-phase auto-generated filters.
        row_filters = [
            f for f in _plan_filters(planned) if f.phase == Phase.ROW
        ]
        assert len(row_filters) == 2
        # Each one targets a different underlying field.
        and_created = _find_date_range_filter_on(
            planned=planned, leaf="created_at",
        )
        and_reviewed = _find_date_range_filter_on(
            planned=planned, leaf="reviewed_at",
        )
        assert and_created is not and_reviewed

    def test_empty_date_range_is_rejected_at_construction(self) -> None:
        dimension = "created_at"
        with pytest.raises(pydantic.ValidationError):
            TimeDimension(
                field=dimension,
                granularity=TimeGranularity.MONTH,
                date_range=[],
            )

    def test_single_element_date_range_emits_one_period_filter(self) -> None:
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        [fp] = _plan_filters(planned)
        assert fp.phase == Phase.ROW

    def test_date_range_filter_carries_expression(self) -> None:
        # The lowered entry's expression payload must be populated so the
        # generator can render the filter without re-parsing.
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        assert len(_plan_filters(planned)) == 1
        fp = _plan_filters(planned)[0]
        assert fp.expression is not None
        assert isinstance(fp.expression.value_key, ArithmeticKey)


# ---------------------------------------------------------------------------
# date_range filter on the underlying field (NOT TimeTruncKey)
# ---------------------------------------------------------------------------


class TestDateRangeHiddenSlot:
    def test_raw_column_slot_materialized_hidden(self) -> None:
        # The date_range filter binds against the raw FieldKey. For
        # the generator to render that field expression independently
        # of the TimeTruncKey (so the outer WHERE references the raw
        # field while the truncated form goes in the SELECT), the
        # planner MUST materialise the raw FieldKey as a hidden slot
        # alongside the public TimeTruncKey slot — they are two
        # distinct slot identities even though they target the same
        # underlying field.
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
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
            if isinstance(s.key, FieldKey)
            and s.key == FieldKey(path=(), leaf="created_at")
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
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-01-01", "2024-03-01"],
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle_local())
        fp = _plan_filters(planned)[0]
        assert fp.expression is not None
        # Walk the predicate; no TimeTruncKey should appear in the tree.
        keys = list(walk_value_keys(fp.expression.value_key))
        assert not any(isinstance(k, TimeTruncKey) for k in keys), (
            f"date_range filter should target raw field, not TimeTruncKey; "
            f"got keys: {[type(k).__name__ for k in keys]}"
        )
