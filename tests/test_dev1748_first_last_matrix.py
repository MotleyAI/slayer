"""DEV-1748 §5.8 — the first/last pinning matrix.

**Protocol.** This module landed and passed against the OLD code, BEFORE
first/last was rebuilt as ``RankedAggregatePlan`` (B9). Its job is to state what
first/last MEANS, in executed rows, so that a rewrite which changes the emitted
SQL for every first/last query can be shown not to change a single answer. A
test here that needed editing when the rewrite landed is either a bug in the
rewrite or a divergence that needs explicit approval — never routine churn.

**One test was an exception, and it is the point of the exercise.**
``test_a_joined_derived_time_arg_ranks_by_the_joined_expression`` landed
``xfail(strict=True)``: the old code RAISED on a time arg that is a derived
column on a joined model, because the ranking ran in the host base and could
not pull the residual join (the DEV-1476/1526 remnant). The rewrite removes
that limitation, so the xfail was removed with it. Everything else in this
module passed on both sides unchanged.

Assertions are **execution-based**: SQL-shape assertions belong in
``tests/test_dev1748_golden_sql.py``, which pins the emission across five
dialects. The two are complementary — a golden diff shows WHAT changed, this
module shows whether the ANSWER changed.

**Nondeterminism is stated, not papered over** (§5.8: "no false parity"). The
``tie`` group holds two rows with the same ranking timestamp and different
values. ``ROW_NUMBER`` breaks that tie arbitrarily, so those cases assert
MEMBERSHIP in the candidate set. Pinning one value would produce a test that
passes by luck and fails on an engine or planner version bump, while advertising
a guarantee SLayer does not make.

The corpus and every named expectation live in ``tests/_dev1748_fixtures.py``.
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
    FAN_RUSH_MULTIPLIED_SUM,
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


# --------------------------------------------------------------------------- #
# Grouped and ungrouped — the shape every other case is a variation on
# --------------------------------------------------------------------------- #


class TestGroupedAndUngrouped:
    async def test_first_and_last_pick_opposite_ends_of_the_group(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """The plain case. ``paid`` holds 11.0 (older) and 13.0 (newer), so
        first and last differ — and because the group is ordered the same way by
        value and by time, this alone would also pass for min/max. The ``filt``
        group is what rules that out: its newer row is the SMALLER value, so
        ``last`` there is 5.0 while ``max`` would be 61.0."""
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
        # The anti-min/max discriminator.
        assert first["filt"] == FILT_MATCHING
        assert last["filt"] == FILT_NEWER_NONMATCHING

    async def test_ungrouped_ranks_over_the_whole_table(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """No dimensions at all: one partition, one answer. Order 16 is the
        newest row in the table."""
        rows = await _rows(engine, measures=[{"formula": "amount:last", "name": "l"}])
        assert rows == [{"orders.l": FAN_LAST}]

    async def test_a_null_grain_member_gets_its_own_group_and_a_real_value(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """The NULL-status group must survive the grain join-back with its own
        first/last, not collapse or come back NULL (P-I)."""
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
        """A month grain must rank WITHIN each month. February holds orders 8
        (NULL value), 10, 12 and 14; order 14 on the 13th is the newest, so
        February's ``last`` is its amount — not the table-wide newest."""
        rows = await _rows(
            engine,
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        by_month = by_group(rows, key="orders.created_at", value="orders.l")

        assert by_month["2024-02-01"] == FILT_NEWER_NONMATCHING
        assert by_month["2024-03-01"] == FAN_LAST
        # The rows whose ranking timestamp is NULL form their own bucket.
        assert by_month[None] == NULLTIME_NULL_ROW_AMOUNT

    async def test_a_joined_dimension_grain_ranks_within_each_joined_group(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Grouping by a two-hop joined dimension. Region ``Alpha`` holds every
        customer-100 order, whose newest is order 2."""
        rows = await _rows(
            engine, dimensions=["customers.regions.name"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        by_region = by_group(
            rows, key="orders.customers.regions.name", value="orders.l",
        )
        assert by_region["Alpha"] == PAID_LAST
        # Region 2's name is NULL — a joined nullable grain member.
        assert by_region[None] == FAN_LAST


# --------------------------------------------------------------------------- #
# NULLs — in the ranking key and in the ranked value
# --------------------------------------------------------------------------- #


class TestNulls:
    async def test_a_null_ranked_value_is_returned_as_null(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``nullval``'s newest row carries a NULL amount. ``last`` must be
        NULL — the value OF the winning row. Returning 41.0 would mean the
        implementation aggregated over the group instead of selecting a row,
        which is the single most likely way to get first/last subtly wrong."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        )
        assert by_group(rows, key="orders.status", value="orders.l")["nullval"] is None
        # ...and the OTHER end of the same group is a real value, so the NULL
        # above is the winner's value and not a group-wide failure.
        assert by_group(
            rows, key="orders.status", value="orders.f",
        )["nullval"] == NULLVAL_OLDER

    async def test_a_null_ranking_timestamp_sorts_per_the_engine(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``nulltime`` holds one row with a NULL ``created_at`` and one dated.

        SQLite sorts NULLs FIRST ascending, so ``ORDER BY created_at DESC`` puts
        the NULL row LAST and the dated row wins ``last``; ascending, the NULL
        row wins ``first``. That is a DIALECT-dependent answer — Postgres sorts
        NULLs last ascending and would swap them — so this pins SQLite's
        behaviour rather than claiming a portable guarantee. What must not
        change is that the ranking is not silently NULL-blind."""
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
        """``shipped_at`` is NULL on the row whose ``created_at`` is set, and
        set on the row whose ``created_at`` is NULL — deliberately inverted, so
        ranking by the explicit arg gives the OTHER answer than the default. A
        query that ignored the explicit arg would return 32.0 here."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(shipped_at)", "name": "l"}],
        )
        assert by_group(
            rows, key="orders.status", value="orders.l",
        )["nulltime"] == NULLTIME_NULL_ROW_AMOUNT


# --------------------------------------------------------------------------- #
# Ties — documented as nondeterministic
# --------------------------------------------------------------------------- #


class TestTiesAreNondeterministic:
    async def test_equal_timestamps_yield_one_of_the_tied_values(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Two ``tie`` rows share a ``created_at`` and carry different amounts.

        ``ROW_NUMBER() OVER (... ORDER BY created_at)`` assigns rank 1 to one of
        them arbitrarily — SLayer adds no secondary sort key, so first/last
        under a tie is genuinely nondeterministic and is documented as such
        rather than pinned. The contract asserted here is the one that IS real:
        the answer is one of the tied rows' values, never a blend, a NULL, or a
        value from another group."""
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
        """The same group ranked by ``shipped_at``, which is distinct across the
        two rows. Determinism returns the moment the ranking key does — which is
        what makes the nondeterminism above a property of the DATA, not of the
        implementation."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(shipped_at)", "name": "l"}],
        )
        assert by_group(
            rows, key="orders.status", value="orders.l",
        )["tie"] == TIE_CANDIDATES[1]


# --------------------------------------------------------------------------- #
# Filtered first/last — Column.filter on the measure
# --------------------------------------------------------------------------- #


class TestFilteredFirstLast:
    async def test_a_filter_selects_the_newest_MATCHING_row(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``filt``'s newest row (5.0) is below the threshold and the older one
        (61.0) is above it, so a filtered ``last`` must return 61.0.

        Two wrong implementations this rules out: ranking before filtering
        returns NULL (the winner is excluded), and filtering without ranking
        returns whichever matching row the engine happens to reach."""
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
        """``nomatch`` holds 1.0 and 2.0, both under the threshold.

        The group must still be PRESENT, carrying NULL. This is the case that
        separates a filtered first/last computed in its own scope (the rows
        vanish there; the grain join-back restores the group) from one that
        drops the group entirely."""
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
        """The stronger form of the case above, stated over the whole result:
        a filtered measure must not change WHICH groups the query returns."""
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
        """``gold_amount`` filters on ``customers.tier``, a column a hop away.
        Only customer 100's orders qualify, so groups made entirely of other
        customers' orders come back NULL and customer-100 groups rank normally.
        """
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "gold_amount:last", "name": "l"}],
        )
        by_status = by_group(rows, key="orders.status", value="orders.l")

        assert by_status["paid"] == PAID_LAST          # customer 100
        assert by_status["nulltime"] == NULLTIME_DATED_ROW_AMOUNT
        assert by_status["filt"] is None               # customer 102
        assert by_status["fan"] is None                # customer 102

    async def test_a_filter_over_a_derived_expression_ranks_the_matching_rows(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``doubled_big`` filters on ``amount * 2``, an expression rather than
        a bare column. It selects exactly the same rows as ``big_amount``, so
        the two must agree group for group — a derived predicate that silently
        failed to bind would not."""
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
        # A direct oracle as well as the comparison, so the two paths cannot
        # agree by being wrong together.
        assert derived_by_status["filt"] == FILT_MATCHING
        assert derived_by_status["nomatch"] is None

    async def test_an_ungrouped_filter_matching_nothing_still_returns_one_row(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Over the EMPTY table, an ungrouped filtered first/last must return
        exactly one row carrying NULL.

        This is the invariant the empty-grain join-back depends on: a scalar
        isolated aggregate is CROSS JOINed to the host spine, so a CTE that
        returned zero rows instead of one NULL row would erase the result
        entirely."""
        rows = await _rows(
            engine, source_model="empty_orders",
            measures=[{"formula": "big_amount:last", "name": "l"}],
        )
        assert rows == [{"empty_orders.l": None}]


# --------------------------------------------------------------------------- #
# The empty source — the one-row-or-no-row contract
# --------------------------------------------------------------------------- #


class TestEmptySource:
    async def test_ungrouped_over_an_empty_table_returns_one_null_row(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """One row, NULL — the same thing ``amount:sum`` does over no rows.
        Zero rows would be a different answer to a different question."""
        rows = await _rows(
            engine, source_model="empty_orders",
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        assert rows == [{"empty_orders.l": None}]

    async def test_ungrouped_first_last_matches_ungrouped_sum_over_no_rows(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Stated as a comparison so the contract cannot drift for one
        aggregate family and not the other."""
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
        """With a grain there are no groups, so there are no rows. The contrast
        with the ungrouped case above is the point: it is the ABSENCE of a grain
        that forces the single-row answer."""
        rows = await _rows(
            engine, source_model="empty_orders", dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        assert rows == []


# --------------------------------------------------------------------------- #
# Explicit time args
# --------------------------------------------------------------------------- #


class TestExplicitTimeArgs:
    async def test_an_explicit_arg_overrides_the_default_ranking_column(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Ranking ``tie`` by ``shipped_at`` picks a different row than the
        default ``created_at`` can, because only ``shipped_at`` distinguishes
        them."""
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
        """The case the rn-suffix scheme exists for today, and the case that
        forces two separate CTEs after the rewrite. ``nulltime`` is the group
        where the two rankings disagree, so a shared ranking column would
        collapse them onto one wrong answer."""
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
        """``created_alias`` is ``Column.sql = "created_at"``, so ranking by it
        must equal ranking by ``created_at``."""
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
        # A direct oracle too — two paths agreeing proves nothing on its own.
        assert derived_by_status["paid"] == PAID_LAST
        assert derived_by_status["nulltime"] == NULLTIME_DATED_ROW_AMOUNT

    async def test_a_joined_derived_time_arg_ranks_by_the_joined_expression(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``customers.signup_alias`` is ``signup_at`` on the joined model, so
        ranking by it reaches THROUGH the join.

        This raised ``NotImplementedError`` before B9 — the DEV-1476 remnant.
        The ranking ran in the host base, which could not pull the residual
        join, so join discovery skipped the path-bearing ``ColumnSqlKey`` and
        the render seam refused rather than emit a reference to a relation the
        FROM did not have. A ranked CTE resolves its ranking key through its
        OWN scope, so the join is registered where it is needed and the refusal
        has nothing left to protect.

        The ``paid`` group's two rows belong to customers whose signup order is
        the reverse of the rows' own ``created_at`` order, so the two rankings
        disagree and the expected value is exactly the one ``created_at`` would
        NOT choose. An implementation that silently fell back to the default
        ranking column returns ``PAID_LAST`` and fails here."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last(customers.signup_alias)", "name": "l"}],
        )
        by_status = by_group(rows, key="orders.status", value="orders.l")
        assert by_status["paid"] == PAID_BY_JOINED_SIGNUP
        assert by_status["paid"] != PAID_LAST


# --------------------------------------------------------------------------- #
# Cross-model first/last
# --------------------------------------------------------------------------- #


class TestCrossModel:
    async def test_a_cross_model_first_last_ranks_in_the_target_scope(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``customers.spend:last`` ranks CUSTOMERS by the target model's own
        ``default_time_dimension`` (``signup_at``), not orders by
        ``created_at``. Customer 101 signed up last; customer 100 first."""
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
        """Naming the target's time column explicitly must agree with letting
        the target's default supply it."""
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
        """``customers.signup_alias`` is derived but LOCAL to the target, so it
        needs no extra join and must agree with the bare column."""
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
        """``customers.deep_opened`` is ``regions.opened_at`` — a hop PAST the
        target. The CTE must pull ``customers -> regions`` to rank by it.
        Region 2 opened later than region 1, and customers 101/102 both sit in
        region 2, so the ranking is a tie between them; the assertion is that
        the query runs and returns one of their spends."""
        rows = await _rows(
            engine,
            measures=[
                {"formula": "customers.spend:last(customers.deep_opened)", "name": "l"},
            ],
        )
        assert rows[0]["orders.l"] in (CUSTOMER_SPEND_LAST, 75.0)


# --------------------------------------------------------------------------- #
# Crossing inputs, 1:N fan-out, and sibling containment
# --------------------------------------------------------------------------- #


class TestCrossingInputsAndFanout:
    async def test_a_first_last_over_a_crossing_derived_value_reads_through_the_join(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``cust_region`` is ``customers__regions.name`` — the value itself
        crosses two joins. The newest row of each customer-100 group belongs to
        region ``Alpha``; customer-101/102 groups sit in the NULL-named region.
        """
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
        """P-C's cardinality clause, stated in rows: adding a first/last measure
        must not move a sibling ``amount:sum``. The sibling's value is compared
        against the SAME query without the first/last, so this holds the line
        both today and after the rewrite moves the ranking into its own CTE."""
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
        # ...and an oracle, so "both wrong together" is not a way to pass.
        assert sums["paid"] == PAID_FIRST + PAID_LAST
        assert sums["fan"] == FAN_FIRST + FAN_LAST

    async def test_a_star_count_sibling_is_unchanged_too(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``*:count`` has no source column, so it takes a different render path
        than ``amount:sum`` and needs its own containment pin."""
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
        # Every seeded group holds exactly two rows, so the oracle is uniform.
        assert set(counts.values()) == {2}

    async def test_a_1n_join_multiplies_a_sum_but_not_the_ranked_pick(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Order 15 is tagged ``rush`` TWICE, so filtering to ``rush`` matches
        it twice.

        ``amount:sum`` doubles its contribution — the ratified multiply-per-match
        semantics (DEV-1688 keep-list item 6), deliberately NOT changed here.
        ``amount:last`` does not move, because duplicating a row cannot change
        which row is newest. Pinning both together is what proves a later
        cardinality change would be visible."""
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

        assert summed["fan"] == FAN_RUSH_MULTIPLIED_SUM
        assert last["fan"] == FAN_LAST

    async def test_grouping_by_a_1n_dimension_ranks_within_each_match(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """With the fan-out column AS the grain, order 15 legitimately appears
        in three tag groups. Each ranks over the rows that reached it."""
        rows = await _rows(
            engine, dimensions=["order_tags.name"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        by_tag = by_group(rows, key="orders.order_tags.name", value="orders.l")

        assert by_tag["gift"] == FAN_FIRST
        assert by_tag["fragile"] == FAN_FIRST
        assert by_tag["rush"] == FAN_LAST


# --------------------------------------------------------------------------- #
# Composition — several ranked measures, and ranked measures inside expressions
# --------------------------------------------------------------------------- #


class TestComposition:
    async def test_two_ranked_measures_sharing_a_ranking_column(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``first`` and ``last`` over the same column and the same ranking key
        — one ranked scope today, two CTEs after the rewrite. The answers must
        not depend on which."""
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
        """C13: one structural key, two declared names. Both columns must be
        emitted and must agree — a shared CTE must not collapse them to one
        column, and two names must not become two different answers."""
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
        """``amount:last - amount:first`` — inline over one ranked scope today,
        an outer composite over two CTEs after the rewrite. ``filt`` is the
        discriminator: its difference is NEGATIVE, so an implementation that
        swapped the operands would be caught."""
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
        """``cumsum(amount:last)`` — the ranked value feeds a window function in
        a later stage, so the ranked result must be materialised as a column the
        chain can read."""
        rows = await _rows(
            engine,
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "cumsum(amount:last)", "name": "c"}],
        )
        # The WHOLE ordered sequence, not two sampled points: a running total
        # can land on the right final value while every step before it is
        # wrong, and the ORDER the chain accumulates in is part of what a
        # transform over an isolated aggregate has to get right.
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


# --------------------------------------------------------------------------- #
# Filters and ordering that TARGET a ranked measure
# --------------------------------------------------------------------------- #


class TestFilteringAndOrderingOnARankedMeasure:
    async def test_a_comparison_on_a_ranked_measure_drops_groups(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """``amount:last > 30`` is an AGGREGATE-phase filter over a value that
        lives in its own scope after the rewrite, so it moves from HAVING to an
        outer WHERE. The ROW SET must not change: exactly the groups whose last
        exceeds 30, and a group whose last is NULL is not one of them."""
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
        """Descending by the ranked value. NULLs sort last by SLayer policy, so
        the ``nullval`` group is at the end rather than at the front."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
            order=[{"column": "l", "direction": "desc"}],
        )
        values = [row["orders.l"] for row in rows]
        # The ``tie`` group's value is arbitrary between the two candidates, so
        # its POSITION is pinned but not which of the two lands there — both sit
        # between 32.0 and 13.0, so the surrounding order is unaffected either
        # way. Pinning one would be a test that passes by luck and fails on an
        # engine or planner version bump.
        assert values[3] in TIE_CANDIDATES, values
        assert values[:3] + values[4:] == [
            FAN_LAST, NULL_STATUS_LAST, NULLTIME_DATED_ROW_AMOUNT,
            PAID_LAST, FILT_NEWER_NONMATCHING, 2.0, None,
        ]

    async def test_ordering_by_a_ranked_measure_that_is_not_projected(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """The order-only case: the ranked measure is materialised but trimmed
        from the projection, so it must still exist somewhere to sort by. The
        emitted columns are exactly the declared ones, in declaration order."""
        rows = await _rows(
            engine, dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "s"}],
            order=[{"column": "amount:last", "direction": "desc"}],
        )
        assert list(rows[0]) == ["orders.status", "orders.s"]
        assert [row["orders.status"] for row in rows] == [
            "fan", None, "nulltime", "tie", "paid", "filt", "nomatch", "nullval",
        ]
