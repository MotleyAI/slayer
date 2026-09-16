"""DEV-1748 §5.8 — the first/last pinning matrix.

Execution-based (SQL shapes live in ``tests/test_dev1748_golden_sql.py``); ties
and NULL ordering assert membership, not a pinned value. Corpus and named
expectations in ``tests/_dev1748_fixtures.py``.
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1748_fixtures import (
    BIG_AMOUNT_THRESHOLD,
    CUSTOMER_SPEND_FIRST,
    CUSTOMER_SPEND_LAST,
    FAN_FIRST,
    FAN_LAST,
    FAN_RUSH_SUM,
    FILT_MATCHING,
    FILT_NEWER_NONMATCHING,
    NULL_STATUS_FIRST,
    NULL_STATUS_LAST,
    NULLTIME_DATED_ROW_AMOUNT,
    NULLTIME_NULL_ROW_AMOUNT,
    NULLVAL_OLDER,
    PAID_BY_JOINED_SIGNUP,
    PAID_FIRST,
    PAID_LAST,
    TIE_CANDIDATES,
    by_group,
    make_sqlite_engine,
    seed_dev1748_sqlite,
)


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "dev1748.db")
        seed_dev1748_sqlite(db_path)
        yield await make_sqlite_engine(d, db_path)


async def _rows(engine: SlayerQueryEngine, **kwargs) -> list:
    kwargs.setdefault("source_model", "orders")
    response = await engine.execute(SlayerQuery(**kwargs))
    return response.data


# Grouped and ungrouped — the shape every other case varies on.


class TestGroupedAndUngrouped:
    async def test_first_and_last_pick_opposite_ends_of_the_group(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """first != last; the ``filt`` group rules out min/max (its newer row is the smaller value)."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        )
        first = by_group(rows, key="orders.status", value="orders.f")
        last = by_group(rows, key="orders.status", value="orders.l")

        assert first["paid"] == PAID_FIRST
        assert last["paid"] == PAID_LAST
        assert first["filt"] == FILT_MATCHING
        assert last["filt"] == FILT_NEWER_NONMATCHING

    async def test_ungrouped_ranks_over_the_whole_table(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """One partition, one answer; order 16 is the newest row."""
        rows = await _rows(engine, measures=[{"formula": "amount:last", "name": "l"}])
        assert rows == [{"orders.l": FAN_LAST}]

    async def test_a_null_grain_member_gets_its_own_group_and_a_real_value(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """The NULL-status group survives the grain join-back with its own first/last (P-I)."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        )
        first = by_group(rows, key="orders.status", value="orders.f")
        last = by_group(rows, key="orders.status", value="orders.l")

        assert None in last, f"the NULL-status group vanished: {rows}"
        assert first[None] == NULL_STATUS_FIRST
        assert last[None] == NULL_STATUS_LAST

    async def test_a_time_truncated_grain_partitions_by_the_truncated_value(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A month grain ranks WITHIN each month; NULL-timestamp rows form their own bucket."""
        rows = await _rows(
            engine,
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        by_month = by_group(rows, key="orders.created_at", value="orders.l")

        assert by_month["2024-02-01"] == FILT_NEWER_NONMATCHING
        assert by_month["2024-03-01"] == FAN_LAST
        assert by_month[None] == NULLTIME_NULL_ROW_AMOUNT

    async def test_a_joined_dimension_grain_ranks_within_each_joined_group(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Grouping by a two-hop joined dimension; region 2's NULL name is a nullable grain member."""
        rows = await _rows(
            engine, dimensions=["customers.regions.name"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        by_region = by_group(
            rows, key="orders.customers.regions.name", value="orders.l",
        )
        assert by_region["Alpha"] == PAID_LAST
        assert by_region[None] == FAN_LAST


# NULLs — in the ranking key and in the ranked value.


class TestNulls:
    async def test_a_null_ranked_value_is_returned_as_null(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``last`` returns the NULL amount OF the winning row, not an aggregate over the group."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        )
        assert by_group(rows, key="orders.status", value="orders.l")["nullval"] is None
        assert by_group(
            rows, key="orders.status", value="orders.f",
        )["nullval"] == NULLVAL_OLDER

    async def test_a_null_ranking_timestamp_sorts_per_the_engine(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """DIALECT-dependent: SQLite sorts NULLs first ascending, so the dated row wins ``last``."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        )
        assert by_group(
            rows, key="orders.status", value="orders.l",
        )["nulltime"] == NULLTIME_DATED_ROW_AMOUNT
        assert by_group(
            rows, key="orders.status", value="orders.f",
        )["nulltime"] == NULLTIME_NULL_ROW_AMOUNT

    async def test_an_explicit_time_arg_with_its_own_nulls_ranks_by_that_column(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``shipped_at`` is inverted against ``created_at``, so the explicit arg gives the other answer."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(shipped_at)", "name": "l"}],
        )
        assert by_group(
            rows, key="orders.status", value="orders.l",
        )["nulltime"] == NULLTIME_NULL_ROW_AMOUNT


# Ties — documented as nondeterministic.


class TestTiesAreNondeterministic:
    async def test_equal_timestamps_yield_one_of_the_tied_values(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Tied timestamps: the answer is one of the tied rows' values, never a blend or NULL."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        )
        first = by_group(rows, key="orders.status", value="orders.f")["tie"]
        last = by_group(rows, key="orders.status", value="orders.l")["tie"]

        assert first in TIE_CANDIDATES
        assert last in TIE_CANDIDATES

    async def test_breaking_the_tie_with_an_explicit_arg_is_deterministic(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Ranking the same group by distinct ``shipped_at`` restores determinism."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(shipped_at)", "name": "l"}],
        )
        assert by_group(
            rows, key="orders.status", value="orders.l",
        )["tie"] == TIE_CANDIDATES[1]


# Filtered first/last — Column.filter on the measure.


class TestFilteredFirstLast:
    async def test_a_filter_selects_the_newest_MATCHING_row(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``filt``'s newest row is below the threshold, so a filtered ``last`` returns the older 61.0."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "big_amount:last", "name": "l"}],
        )
        assert by_group(
            rows, key="orders.status", value="orders.l",
        )["filt"] == FILT_MATCHING

    async def test_a_group_with_no_matching_row_survives_carrying_null(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """No row clears the threshold: the group stays PRESENT carrying NULL (grain join-back)."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "big_amount:last", "name": "l"}],
        )
        by_status = by_group(rows, key="orders.status", value="orders.l")

        assert "nomatch" in by_status, f"the group was dropped entirely: {rows}"
        assert by_status["nomatch"] is None

    async def test_no_group_is_lost_when_a_measure_filter_matches_nothing(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A filtered measure must not change WHICH groups the query returns."""
        unfiltered = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        filtered = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "big_amount:last", "name": "l"}],
        )
        assert (
            {row["orders.status"] for row in filtered}
            == {row["orders.status"] for row in unfiltered}
        )

    async def test_a_filter_on_a_joined_column_ranks_the_matching_rows(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A filter on ``customers.tier`` (a hop away): only customer 100's groups rank; others NULL."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "gold_amount:last", "name": "l"}],
        )
        by_status = by_group(rows, key="orders.status", value="orders.l")

        assert by_status["paid"] == PAID_LAST
        assert by_status["nulltime"] == NULLTIME_DATED_ROW_AMOUNT
        assert by_status["filt"] is None
        assert by_status["fan"] is None

    async def test_a_filter_over_a_derived_expression_ranks_the_matching_rows(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A filter on the expression ``amount * 2`` selects the same rows as ``big_amount``."""
        derived = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "doubled_big:last", "name": "l"}],
        )
        plain = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "big_amount:last", "name": "l"}],
        )
        derived_by_status = by_group(derived, key="orders.status", value="orders.l")
        assert (
            derived_by_status
            == by_group(plain, key="orders.status", value="orders.l")
        )
        assert derived_by_status["filt"] == FILT_MATCHING
        assert derived_by_status["nomatch"] is None

    async def test_an_ungrouped_filter_matching_nothing_still_returns_one_row(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Over the EMPTY table, an ungrouped filtered first/last returns exactly one NULL row."""
        rows = await _rows(
            engine, source_model="empty_orders",
            measures=[{"formula": "big_amount:last", "name": "l"}],
        )
        assert rows == [{"empty_orders.l": None}]


# The empty source — the one-row-or-no-row contract.


class TestEmptySource:
    async def test_ungrouped_over_an_empty_table_returns_one_null_row(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """One NULL row — the same thing ``amount:sum`` does over no rows."""
        rows = await _rows(
            engine, source_model="empty_orders",
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        assert rows == [{"empty_orders.l": None}]

    async def test_ungrouped_first_last_matches_ungrouped_sum_over_no_rows(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A comparison so the contract cannot drift between aggregate families."""
        ranked = await _rows(
            engine, source_model="empty_orders",
            measures=[{"formula": "amount:last", "name": "m"}],
        )
        summed = await _rows(
            engine, source_model="empty_orders",
            measures=[{"formula": "amount:sum", "name": "m"}],
        )
        assert len(ranked) == len(summed) == 1
        assert ranked[0]["empty_orders.m"] is None
        assert summed[0]["empty_orders.m"] is None

    async def test_grouped_over_an_empty_table_returns_no_rows(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """With a grain there are no groups, so there are no rows."""
        rows = await _rows(
            engine, source_model="empty_orders", dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        assert rows == []


# Explicit time args.


class TestExplicitTimeArgs:
    async def test_an_explicit_arg_overrides_the_default_ranking_column(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Only ``shipped_at`` distinguishes the ``tie`` rows, so it picks a row ``created_at`` cannot."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(shipped_at)", "name": "l"}],
        )
        assert by_group(
            rows, key="orders.status", value="orders.l",
        )["tie"] == TIE_CANDIDATES[1]

    async def test_two_measures_with_different_time_args_rank_independently(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Two time args rank independently; ``nulltime`` is where the two rankings disagree."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:last(created_at)", "name": "a"},
                {"formula": "amount:last(shipped_at)", "name": "b"},
            ],
        )
        by_created = by_group(rows, key="orders.status", value="orders.a")
        by_shipped = by_group(rows, key="orders.status", value="orders.b")

        assert by_created["nulltime"] == NULLTIME_DATED_ROW_AMOUNT
        assert by_shipped["nulltime"] == NULLTIME_NULL_ROW_AMOUNT
        assert by_created["nulltime"] != by_shipped["nulltime"]

    async def test_a_local_derived_time_arg_ranks_like_its_underlying_column(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``created_alias`` is ``sql="created_at"``, so ranking by it equals ranking by ``created_at``."""
        derived = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(created_alias)", "name": "l"}],
        )
        plain = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(created_at)", "name": "l"}],
        )
        derived_by_status = by_group(derived, key="orders.status", value="orders.l")
        assert (
            derived_by_status
            == by_group(plain, key="orders.status", value="orders.l")
        )
        assert derived_by_status["paid"] == PAID_LAST
        assert derived_by_status["nulltime"] == NULLTIME_DATED_ROW_AMOUNT

    async def test_a_joined_derived_time_arg_ranks_by_the_joined_expression(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Ranking through a joined derived time arg; ``paid``'s signup order reverses ``created_at``,
        so the winner is the row ``created_at`` would NOT choose."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(customers.signup_alias)", "name": "l"}],
        )
        by_status = by_group(rows, key="orders.status", value="orders.l")
        assert by_status["paid"] == PAID_BY_JOINED_SIGNUP
        assert by_status["paid"] != PAID_LAST


# Cross-model first/last.


class TestCrossModel:
    async def test_a_cross_model_first_last_ranks_in_the_target_scope(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``customers.spend:last`` ranks CUSTOMERS by the target's own ``signup_at`` default."""
        rows = await _rows(
            engine,
            measures=[
                {"formula": "customers.spend:first", "name": "f"},
                {"formula": "customers.spend:last", "name": "l"},
            ],
        )
        assert rows == [{"orders.f": CUSTOMER_SPEND_FIRST, "orders.l": CUSTOMER_SPEND_LAST}]

    async def test_an_explicit_target_time_arg_gives_the_same_answer(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Naming the target's time column explicitly agrees with its default."""
        rows = await _rows(
            engine,
            measures=[
                {"formula": "customers.spend:last(customers.signup_at)", "name": "l"},
            ],
        )
        assert rows == [{"orders.l": CUSTOMER_SPEND_LAST}]

    async def test_a_derived_time_arg_on_the_target_gives_the_same_answer(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A derived time arg LOCAL to the target needs no extra join and agrees with the bare column."""
        rows = await _rows(
            engine,
            measures=[
                {"formula": "customers.spend:last(customers.signup_alias)", "name": "l"},
            ],
        )
        assert rows == [{"orders.l": CUSTOMER_SPEND_LAST}]

    async def test_a_target_time_arg_whose_sql_crosses_a_further_join_works(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A target time arg (``regions.opened_at``) a hop PAST the target: the CTE pulls the further join;
        customers 101/102 tie in region 2, so the query returns one of their spends."""
        rows = await _rows(
            engine,
            measures=[
                {"formula": "customers.spend:last(customers.deep_opened)", "name": "l"},
            ],
        )
        assert rows[0]["orders.l"] in (CUSTOMER_SPEND_LAST, 75.0)


# Crossing inputs, 1:N fan-out, and sibling containment.


class TestCrossingInputsAndFanout:
    async def test_a_first_last_over_a_crossing_derived_value_reads_through_the_join(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A ranked value that itself crosses two joins; customer-100 groups land in ``Alpha``."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "cust_region:last", "name": "l"}],
        )
        by_status = by_group(rows, key="orders.status", value="orders.l")
        assert by_status["paid"] == "Alpha"
        assert by_status["fan"] is None

    async def test_a_sibling_aggregate_is_unchanged_by_an_adjacent_first_last(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """P-C's cardinality clause in rows: adding a first/last measure must not move a sibling ``amount:sum``."""
        with_ranked = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l"},
                {"formula": "amount:sum", "name": "s"},
            ],
        )
        without = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "s"}],
        )
        sums = by_group(with_ranked, key="orders.status", value="orders.s")
        assert sums == by_group(without, key="orders.status", value="orders.s")
        assert sums["paid"] == PAID_FIRST + PAID_LAST
        assert sums["fan"] == FAN_FIRST + FAN_LAST

    async def test_a_star_count_sibling_is_unchanged_too(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``*:count`` has no source column, so it takes a different render path and needs its own pin."""
        with_ranked = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l"},
                {"formula": "*:count", "name": "n"},
            ],
        )
        without = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "*:count", "name": "n"}],
        )
        counts = by_group(with_ranked, key="orders.status", value="orders.n")
        assert counts == by_group(without, key="orders.status", value="orders.n")
        # Every seeded group holds exactly two rows.
        assert set(counts.values()) == {2}

    async def test_a_1n_rush_filter_binds_once_for_sum_and_ranked_pick(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A fanning filter to ``rush`` (order 15 tagged twice) restricts by semi-join: sum counts it once, last is unmoved."""
        rows = await _rows(
            engine, dimensions=["status"],
            filters=["order_tags.name == 'rush'"],
            measures=[
                {"formula": "amount:last", "name": "l"},
                {"formula": "amount:sum", "name": "s"},
            ],
        )
        last = by_group(rows, key="orders.status", value="orders.l")
        summed = by_group(rows, key="orders.status", value="orders.s")

        assert summed["fan"] == FAN_RUSH_SUM
        assert last["fan"] == FAN_LAST

    async def test_grouping_by_a_1n_dimension_ranks_within_each_match(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """With the fan-out column AS the grain, order 15 appears in three tag groups, each ranked."""
        rows = await _rows(
            engine, dimensions=["order_tags.name"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        by_tag = by_group(rows, key="orders.order_tags.name", value="orders.l")

        assert by_tag["gift"] == FAN_FIRST
        assert by_tag["fragile"] == FAN_FIRST
        assert by_tag["rush"] == FAN_LAST


# Composition — several ranked measures, and ranked measures in expressions.


class TestComposition:
    async def test_two_ranked_measures_sharing_a_ranking_column(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``first`` and ``last`` over one column and ranking key, independent of scope layout."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        )
        assert by_group(rows, key="orders.status", value="orders.f")["paid"] == PAID_FIRST
        assert by_group(rows, key="orders.status", value="orders.l")["paid"] == PAID_LAST

    async def test_the_same_ranked_measure_under_two_names(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """C13: one structural key, two declared names — both emitted and equal."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l1"},
                {"formula": "amount:last", "name": "l2"},
            ],
        )
        assert by_group(rows, key="orders.status", value="orders.l1") == by_group(
            rows, key="orders.status", value="orders.l2",
        )
        assert by_group(rows, key="orders.status", value="orders.l1")["paid"] == PAID_LAST

    async def test_an_arithmetic_composite_of_two_ranked_operands(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """An arithmetic composite of two ranked operands; ``filt``'s difference is negative (operand order)."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last - amount:first", "name": "d"}],
        )
        by_status = by_group(rows, key="orders.status", value="orders.d")

        assert by_status["paid"] == PAID_LAST - PAID_FIRST
        assert by_status["filt"] == FILT_NEWER_NONMATCHING - FILT_MATCHING
        # A NULL operand propagates through the arithmetic.
        assert by_status["nullval"] is None

    async def test_a_ranked_measure_under_a_transform_chain(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``cumsum(amount:last)`` — the ranked value must be materialised as a column the chain reads."""
        rows = await _rows(
            engine,
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "cumsum(amount:last)", "name": "c"}],
        )
        # Assert the whole ordered sequence, not sampled points: order matters.
        january = NULLTIME_NULL_ROW_AMOUNT + FAN_FIRST
        february = january + FILT_NEWER_NONMATCHING
        assert [
            (row["orders.created_at"], row["orders.c"]) for row in rows
        ] == [
            (None, pytest.approx(NULLTIME_NULL_ROW_AMOUNT)),
            ("2024-01-01", pytest.approx(january)),
            ("2024-02-01", pytest.approx(february)),
            ("2024-03-01", pytest.approx(february + FAN_LAST)),
        ]


# Filters and ordering that TARGET a ranked measure.


class TestFilteringAndOrderingOnARankedMeasure:
    async def test_a_comparison_on_a_ranked_measure_drops_groups(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """An aggregate-phase filter on a ranked value keeps the row set: groups whose last exceeds 30, NULL excluded."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
            filters=[f"amount:last > {BIG_AMOUNT_THRESHOLD + 10}"],
        )
        assert by_group(rows, key="orders.status", value="orders.l") == {
            None: NULL_STATUS_LAST,
            "fan": FAN_LAST,
            "nulltime": NULLTIME_DATED_ROW_AMOUNT,
        }

    async def test_ordering_by_a_projected_ranked_measure(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Descending by the ranked value; NULLs sort last by SLayer policy."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
            order=[{"column": "l", "direction": "desc"}],
        )
        values = [row["orders.l"] for row in rows]
        # The ``tie`` value is arbitrary between the candidates; only its position is pinned.
        assert values[3] in TIE_CANDIDATES, values
        assert values[:3] + values[4:] == [
            FAN_LAST, NULL_STATUS_LAST, NULLTIME_DATED_ROW_AMOUNT,
            PAID_LAST, FILT_NEWER_NONMATCHING, 2.0, None,
        ]

    async def test_ordering_by_a_ranked_measure_that_is_not_projected(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Order-only: the ranked measure is materialised but trimmed from the projection."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "s"}],
            order=[{"column": "amount:last", "direction": "desc"}],
        )
        assert list(rows[0]) == ["orders.status", "orders.s"]
        assert [row["orders.status"] for row in rows] == [
            "fan", None, "nulltime", "tie", "paid", "filt", "nomatch", "nullval",
        ]
