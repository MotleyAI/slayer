"""DEV-1748 B9 — first/last as a plan-shaped isolated CTE.

This module asserts the NEW behaviour and therefore fails until the rewrite
lands. Its companion ``tests/test_dev1748_first_last_matrix.py`` asserts the
ANSWERS, and passes both before and after; between them they say "the SQL
changes, the results do not".

What P-C requires of a ranked aggregate, and what is pinned here:

* it is **isolated** — a CTE of its own, rooted where its rows live, so the host
  base holds only purely-local aggregates and host cardinality cannot move;
* it is **joined back on the query grain, null-safely** (P-I), from ONE planned
  grain list that both the ``PARTITION BY`` and the join-back derive from — and
  that is asserted on the emitted AST, not merely on the plan, because a plan
  carrying two grain members proves nothing about a renderer that reads one;
* its filtered form is **plan data** — a predicate applied in the CTE — not a
  sentinel rank column plus a match flag consulted by alias lookup;
* it goes through the **one materialisation mechanism** (P-B), so an expression
  that is both the grain and the ranked value is projected once, not twice;
* it never emits a ``WITH`` nested inside a CTE body, which SQL Server rejects.

Assertions here read the AST rather than the SQL string wherever the claim is
structural. A string search cannot tell ``CAST(MAX(x) AS DOUBLE PRECISION)``
from ``MAX(CAST(x AS DOUBLE PRECISION))``, and the difference is exactly what
one of these tests exists to catch.
"""

from __future__ import annotations

import os
import re
import tempfile
from typing import Iterator, List

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.keys import Phase
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.isolation import IsolationKind, classify_isolation
from slayer.engine.planned import OrderScope, RankedAggregatePlan
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.stage_planner import plan_query
from slayer.sql.naming import assert_unique_cte_names

from tests._dev1748_fixtures import (
    BIG_AMOUNT_THRESHOLD,
    FAN_FIRST,
    FAN_LAST,
    NULL_STATUS_LAST,
    PAID_LAST,
    dev1748_bundle,
    dev1748_models,
    make_sqlite_engine,
    seed_dev1748_sqlite,
)
from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

#: Every rank-column spelling the pre-B9 machinery could emit — including the
#: UNSUFFIXED forms, which an earlier version of this pattern missed, so a
#: rewrite that kept the first old rank column would have passed.
_SUPERSEDED_RANK_COLUMNS = re.compile(
    r"\b_(?:first|last)_rn(?:_\d+|_f\d+)?\b|\b_match_f\d+\b",
)


def _plan(query: SlayerQuery):
    return plan_query(query=query, bundle=dev1748_bundle())


def _ranked_plans(planned) -> List[RankedAggregatePlan]:
    """Every ranked plan in a query, in either coordinate system.

    DEV-1835: a LOCAL first/last is desugared into a regroup producer BEFORE
    isolation, so its ranked plan lives in the producer sub-plan rather than at
    the top level. A cross-model first/last still carries a top-level ranked
    plan. This gathers both so the plan-level assertions read the plan wherever
    the desugar put it.
    """
    return [
        rp
        for rap in planned.regroup_attach_plans
        for rp in rap.producer_plan.ranked_aggregate_plans
    ] + list(planned.ranked_aggregate_plans)


def _sole_producer(planned):
    """The single regroup producer sub-plan a LOCAL first/last desugars into."""
    assert len(planned.regroup_attach_plans) == 1, planned.regroup_attach_plans
    return planned.regroup_attach_plans[0].producer_plan


def _placeholder_slot(planned, rap):
    """The consumer row slot a regroup producer's aggregate is consumed through.

    DEV-1835 replaces the partitioned aggregate with a reserved-leaf placeholder
    in the consumer's dimension / order / projection trees; this finds the slot
    that placeholder resolves to.
    """
    placeholder = rap.substitutions[0].placeholder
    return next(s for s in planned.row_slots if s.key == placeholder)


async def _sql(query: SlayerQuery, *, dialect: str = "postgres") -> str:
    models = dev1748_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect=dialect, validate=False,
    )


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _tree(sql: str, *, dialect: str = "postgres") -> exp.Expression:
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    assert parsed is not None, f"SQL failed to parse:\n{sql}"
    return parsed


def _ctes(sql: str, *, dialect: str = "postgres") -> List[exp.CTE]:
    with_node = _tree(sql, dialect=dialect).args.get("with_")
    return [] if with_node is None else list(with_node.expressions)


def _cte_names(sql: str, *, dialect: str = "postgres") -> List[str]:
    return [c.alias_or_name for c in _ctes(sql, dialect=dialect)]


def _cte_body(sql: str, name: str, *, dialect: str = "postgres") -> exp.Expression:
    for cte in _ctes(sql, dialect=dialect):
        if cte.alias_or_name == name:
            return cte.this
    raise AssertionError(f"no CTE named {name!r} in {_cte_names(sql, dialect=dialect)}")


def _ranked_cte_names(sql: str, *, dialect: str = "postgres") -> List[str]:
    # DEV-1835: a first/last CTE is identified by the ROW_NUMBER ranking it
    # carries, not by a name prefix. A LOCAL ranked measure now desugars into a
    # regroup producer that renders as a ``_cm_`` CTE, while a cross-model one
    # keeps its ``_rk_`` name — both carry the ranking window, and nothing else
    # (plain ``_base`` / ``_cm_`` / ``_wm_`` aggregates) does.
    return [
        cte.alias_or_name
        for cte in _ctes(sql, dialect=dialect)
        if any(
            isinstance(w.this, exp.RowNumber) for w in cte.this.find_all(exp.Window)
        )
    ]


def _the_ranked_body(sql: str, *, dialect: str = "postgres") -> exp.Expression:
    names = _ranked_cte_names(sql, dialect=dialect)
    assert len(names) == 1, f"expected exactly one _rk_ CTE, got {names}:\n{sql}"
    return _cte_body(sql, names[0], dialect=dialect)


def _partition_exprs(body: exp.Expression, *, dialect: str) -> List[str]:
    """The rendered ``PARTITION BY`` operands of the body's single window."""
    windows = list(body.find_all(exp.Window))
    assert len(windows) == 1, (
        f"expected exactly one window function, got {len(windows)}:\n"
        f"{body.sql(dialect=dialect)}"
    )
    return [p.sql(dialect=dialect) for p in windows[0].args.get("partition_by") or []]


def _join_onto(sql: str, prefix: str, *, dialect: str = "postgres") -> exp.Join:
    joins = [
        j for j in _tree(sql, dialect=dialect).find_all(exp.Join)
        if str(getattr(j.this, "alias_or_name", "")).startswith(prefix)
    ]
    assert len(joins) == 1, f"expected one join onto a {prefix}* CTE:\n{sql}"
    return joins[0]


def _null_safe_pairs(join: exp.Join, *, dialect: str = "postgres") -> List[str]:
    """Rendered null-safe equality conjuncts of a join's ON predicate."""
    on = join.args.get("on")
    if on is None:
        return []
    return [
        node.sql(dialect=dialect)
        for node in on.find_all(exp.NullSafeEQ)
    ] or [
        # Dialects without a native null-safe operator render the expanded
        # ``a = b OR (a IS NULL AND b IS NULL)`` form, one Paren per pair.
        node.sql(dialect=dialect) for node in on.find_all(exp.Paren)
    ]


@pytest.fixture
def storage_dir() -> Iterator[str]:
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def db_path(storage_dir: str) -> str:
    """The seeded SQLite file, exposed so a test can query it directly.

    One test needs to run a fragment of the GENERATED SQL on its own, which the
    engine's query API cannot express.
    """
    path = os.path.join(storage_dir, "dev1748.db")
    seed_dev1748_sqlite(path)
    return path


@pytest.fixture
async def engine(storage_dir: str, db_path: str) -> SlayerQueryEngine:
    return await make_sqlite_engine(storage_dir, db_path)


# --------------------------------------------------------------------------- #
# Plan level — the classifier
# --------------------------------------------------------------------------- #


class TestIsolationClassification:
    def test_a_local_first_last_is_classified_host_rooted_ranked(self) -> None:
        """A first/last needs its own ROW ORDERING, so it isolates even when
        every one of its inputs is local — the trigger the pre-B9 classifier had
        no case for.

        DEV-1835: the local first/last is desugared into a regroup producer
        before isolation, so the classified slot now lives in the producer
        sub-plan; the classifier's verdict on it is unchanged."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        producer = _sole_producer(planned)
        slot = next(s for s in producer.aggregate_slots if not s.hidden)
        assert classify_isolation(
            slot=slot, windowed_slot_ids=set(), bundle=dev1748_bundle(),
        ) is IsolationKind.RANKED_HOST

    def test_a_cross_model_first_last_is_classified_target_rooted_ranked(self) -> None:
        """Its rows live at the target, so that is where the CTE is rooted."""
        planned = _plan(_q(
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ))
        slot = next(s for s in planned.aggregate_slots if not s.hidden)
        assert classify_isolation(
            slot=slot, windowed_slot_ids=set(), bundle=dev1748_bundle(),
        ) is IsolationKind.RANKED_TARGET

    def test_a_non_ranked_local_aggregate_is_still_not_isolated(self) -> None:
        """The control. Widening the trigger must not sweep ordinary aggregates
        into CTEs of their own."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "s"}],
        ))
        slot = next(s for s in planned.aggregate_slots if not s.hidden)
        assert classify_isolation(
            slot=slot, windowed_slot_ids=set(), bundle=dev1748_bundle(),
        ) is IsolationKind.NONE

    def test_a_non_ranked_crossing_aggregate_is_still_host_rooted(self) -> None:
        """The other control: the pre-existing crossing-input trigger keeps its
        verdict, so the new branch is additive rather than a reroute."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "cust_region:count", "name": "c"}],
        ))
        slot = next(s for s in planned.aggregate_slots if not s.hidden)
        assert classify_isolation(
            slot=slot, windowed_slot_ids=set(), bundle=dev1748_bundle(),
        ) is IsolationKind.HOST_ROOTED

    def test_ranked_isolation_survives_the_sub_plan_recursion_guard(self) -> None:
        """``disable_host_rooted_isolation`` exists to stop a CROSSING-INPUT
        aggregate isolating forever inside its own sub-plan. A ranked aggregate
        is different: it is rendered, never re-planned, and inside a sub-plan it
        still needs its own row ordering. So the guard must not suppress it —
        otherwise a re-rooted first/last would have no ranking at all.

        DEV-1835: the classified slot lives in the desugared regroup producer."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        producer = _sole_producer(planned)
        slot = next(s for s in producer.aggregate_slots if not s.hidden)
        assert classify_isolation(
            slot=slot, windowed_slot_ids=set(), bundle=dev1748_bundle(),
            disable_host_rooted_isolation=True,
        ) is IsolationKind.RANKED_HOST


# --------------------------------------------------------------------------- #
# Plan level — the plan node
# --------------------------------------------------------------------------- #


class TestThePlanNode:
    def test_each_plan_is_bound_to_its_own_aggregate_slot(self) -> None:
        """``first`` and ``last`` over one column are two aggregates, so two
        plans and two CTEs — the same one-per-aggregate rule ``_cm_`` and
        ``_wm_`` already follow. Each plan is bound to its OWN aggregate slot.

        DEV-1835: the two local ranked measures desugar into two regroup
        producers, one ranked plan each, each bound to a slot in its own
        producer's coordinate system."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        ))
        producers = planned.regroup_attach_plans
        assert len(producers) == 2

        aggs = set()
        for rap in producers:
            pp = rap.producer_plan
            assert len(pp.ranked_aggregate_plans) == 1, pp.ranked_aggregate_plans
            plan = pp.ranked_aggregate_plans[0]
            slots = {s.id: s.key.agg for s in pp.aggregate_slots}
            assert slots[plan.aggregate_slot_id] == plan.agg
            aggs.add(plan.agg)
        assert aggs == {"first", "last"}

    def test_a_repeated_structural_key_shares_one_plan_and_both_names(self) -> None:
        """C13: one key declared under two names is ONE aggregate, so it must
        not produce two CTEs computing the same thing — and BOTH names must
        still be projected from the one it produces.

        DEV-1835: the shared key desugars into ONE regroup producer (one ranked
        plan, one CTE), consumed through a single placeholder slot that the
        consumer projects once per declared name."""
        query = _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l1"},
                {"formula": "amount:last", "name": "l2"},
            ],
        )
        planned = _plan(query)
        assert len(planned.regroup_attach_plans) == 1
        rap = planned.regroup_attach_plans[0]
        assert len(rap.producer_plan.ranked_aggregate_plans) == 1

        placeholder_id = _placeholder_slot(planned, rap).id
        assert planned.projection.count(placeholder_id) == 2, (
            "the shared key must be projected once per declared name"
        )

    def test_the_plan_carries_an_explicit_ranking_time_key(self) -> None:
        """P-D: the ranking column is decided at plan time, so the renderer
        never re-derives it."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last(shipped_at)", "name": "l"}],
        ))
        assert getattr(
            _ranked_plans(planned)[0].ranking_time_key, "leaf", None,
        ) == "shipped_at"

    def test_a_temporal_dimension_outranks_the_model_default(self) -> None:
        """Precedence step 2: a DATE/TIMESTAMP row dimension beats the model's
        ``default_time_dimension``. ``shipped_at`` is selected as a dimension
        here while ``created_at`` is the model default, so the two candidates
        disagree and the winner is observable."""
        planned = _plan(_q(
            dimensions=["shipped_at"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        assert getattr(
            _ranked_plans(planned)[0].ranking_time_key, "leaf", None,
        ) == "shipped_at"

    def test_a_time_dimension_supplies_its_raw_column(self) -> None:
        """Precedence step 3: a truncated time dimension contributes its RAW
        column, not the truncated expression — ranking within a month bucket by
        the bucket itself would tie every row in it."""
        planned = _plan(_q(
            time_dimensions=[{"dimension": "shipped_at", "granularity": "month"}],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        assert getattr(
            _ranked_plans(planned)[0].ranking_time_key, "leaf", None,
        ) == "shipped_at"

    def test_the_model_default_is_the_last_resort(self) -> None:
        """Precedence step 4: nothing temporal in the query, so the model's
        ``default_time_dimension`` supplies it."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        assert getattr(
            _ranked_plans(planned)[0].ranking_time_key, "leaf", None,
        ) == "created_at"

    def test_no_resolvable_ranking_column_raises_the_host_message(self) -> None:
        """The end of the precedence. The message is asserted verbatim because
        it is the user-facing contract and this PR moves WHERE it is raised;
        moving it must not reword it."""
        model = SlayerModel(
            name="untimed", sql_table="untimed", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
            ],
        )
        from slayer.engine.source_bundle import ResolvedSourceBundle
        bundle = ResolvedSourceBundle(source_model=model, referenced_models=[])
        query = SlayerQuery(
            source_model="untimed", dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        with pytest.raises(ValueError) as excinfo:
            plan_query(query=query, bundle=bundle)
        assert str(excinfo.value) == (
            "first/last aggregation requires a ranking time column "
            "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
            "model's default_time_dimension); none is resolvable for "
            "model 'untimed'."
        )

    def test_a_target_rooted_plan_ignores_the_host_time_dimension(self) -> None:
        """Precedence is per-scope. A cross-model ranked aggregate ranks TARGET
        rows, so a temporal dimension of the HOST is not a candidate — only the
        target's own default (or an explicit target-side arg) is. Selecting a
        host time dimension here must not change the ranking key."""
        planned = _plan(_q(
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ))
        plan = planned.ranked_aggregate_plans[0]
        assert plan.root_model == "customers"
        assert getattr(plan.ranking_time_key, "leaf", None) == "signup_at"

    def test_a_host_column_as_a_target_ranking_key_is_rejected(self) -> None:
        """A cross-model first/last ranks the TARGET's rows, so a HOST column is
        not one of their attributes — the relationship runs the other way.

        This used to emit ``ORDER BY <target>.<host column>``: a reference to a
        column that does not exist on the relation it names, which fails at the
        database with nothing pointing at the measure that caused it."""
        # Hoisted so the ONLY call that can raise inside ``pytest.raises`` is
        # the one under test (Sonar S5778) — a query-validation error would
        # otherwise pass this test for the wrong reason.
        query = _q(measures=[
            {"formula": "customers.spend:last(created_at)", "name": "l"},
        ])
        with pytest.raises(ValueError) as excinfo:
            _plan(query)
        message = str(excinfo.value)
        assert "'created_at'" in message
        assert "'customers'" in message

    def test_one_grain_list_covers_every_query_dimension(self) -> None:
        """The grain is carried as structural members rather than re-derived at
        each site, which is what makes "the partition IS the join-back grain" a
        property of the plan instead of a coincidence two code paths keep
        agreeing on. The rendered consequence is asserted separately below."""
        planned = _plan(_q(
            dimensions=["status"],
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        # DEV-1835: the grain members are anchored in the desugared producer's
        # own row slots, so both plan and slots are read from the producer.
        producer = _sole_producer(planned)
        plan = producer.ranked_aggregate_plans[0]
        row_slot_ids = {s.id for s in producer.row_slots}

        assert len(plan.grain) == 2, plan.grain
        assert {m.host_slot_id for m in plan.grain} <= row_slot_ids
        assert all(m.ranked_key is not None for m in plan.grain)

    def test_a_scalar_ranked_aggregate_has_an_empty_grain(self) -> None:
        """No dimensions means no partition and no join predicate — the CTE is
        CROSS JOINed, which is only sound because it still returns exactly one
        row."""
        planned = _plan(_q(measures=[{"formula": "amount:last", "name": "l"}]))
        assert _ranked_plans(planned)[0].grain == []

    def test_a_measure_filter_is_carried_as_plan_data(self) -> None:
        """B9: "filtered variants are plan data, not sentinel-alias lookups".
        The predicate belongs to the aggregate's key and the plan names the
        aggregate, so the renderer applies it without consulting an alias map.
        """
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "big_amount:last", "name": "l"}],
        ))
        producer = _sole_producer(planned)
        plan = producer.ranked_aggregate_plans[0]
        slot = next(
            s for s in producer.aggregate_slots if s.id == plan.aggregate_slot_id
        )
        assert slot.key.column_filter_key is not None

    def test_an_aggregate_phase_filter_routes_to_the_outer_where(self) -> None:
        """The ranked value lives in a CTE that is LEFT JOINed back, so a HAVING
        inside that CTE would drop CTE rows and the join would resurrect the
        host row carrying NULL — the DEV-1503 failure. It has to be an outer
        WHERE on the combined SELECT.

        DEV-1835: comparing a first/last consumes it through a regroup
        placeholder, so the filter is now bound as a ROW-phase predicate — but
        the routing contract is unchanged: it lands on the outer combined SELECT
        and never inside the ranked CTE. Asserted as the EXACT id being present
        in the outer WHERE and absent from the ranked producer, so "some filter
        went somewhere" cannot pass."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
            filters=[f"amount:last > {BIG_AMOUNT_THRESHOLD}"],
        ))
        bound_ids = [fp.id for fp in planned.filters_by_phase]
        assert bound_ids, "no filter was bound"
        assert set(bound_ids) <= set(planned.outer_where_filter_ids)

        producer = _sole_producer(planned)
        plan = producer.ranked_aggregate_plans[0]
        assert not (set(bound_ids) & set(plan.having_filter_ids))
        assert not (set(bound_ids) & set(plan.where_filter_ids))
        # the ranked producer never evaluates the comparison at all
        assert not producer.filters_by_phase

    def test_an_order_only_ranked_measure_gets_a_hidden_plan(self) -> None:
        """An order-only first/last is materialised but never projected. It
        still needs its own CTE — and the plan must say so, or the renderer
        would project it and change the emitted column list.

        DEV-1835: it is materialised by a regroup producer and consumed through
        a HIDDEN placeholder slot that the projection omits and the order sorts
        by — the "materialised but not surfaced" contract, one hop over."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "s"}],
            order=[{"column": "amount:last", "direction": "desc"}],
        ))
        assert len(planned.regroup_attach_plans) == 1
        rap = planned.regroup_attach_plans[0]
        assert len(rap.producer_plan.ranked_aggregate_plans) == 1

        ph = _placeholder_slot(planned, rap)
        assert ph.hidden is True
        assert ph.public_name is None
        assert ph.id not in planned.projection
        assert any(e.slot_id == ph.id for e in planned.order)

    def test_an_order_entry_targeting_a_ranked_measure_names_its_scope(self) -> None:
        """P-D again: the order resolver dispatches on the plan-assigned scope
        with no fallback, so a ranked sort target must carry its own scope
        rather than falling through to a default.

        DEV-1835: a LOCAL first/last is consumed from its regroup producer's
        ``_cm_`` CTE, so the sort term's scope is that producing CTE."""
        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
            order=[{"column": "l", "direction": "desc"}],
        ))
        assert [e.scope for e in planned.order] == [OrderScope.CROSS_MODEL_CTE]

    def test_a_rerooted_cross_model_first_last_stays_a_cross_model_plan(self) -> None:
        """D1's exception. A re-rooted cross-model aggregate keeps its
        ``CrossModelAggregatePlan`` — the ranked plan belongs to the nested
        sub-plan, in the sub-plan's coordinate system, not to this one."""
        planned = _plan(_q(
            dimensions=["customers.regions.name"],
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ))
        rerooted = [
            p for p in planned.cross_model_aggregate_plans
            if p.rerooted_plan is not None
        ]
        assert rerooted, "expected a re-rooted cross-model plan"
        top_level_slots = {p.aggregate_slot_id for p in planned.ranked_aggregate_plans}
        assert not any(p.aggregate_slot_id in top_level_slots for p in rerooted)

    def test_the_plan_type_validates_a_minimal_instance(self) -> None:
        """Constructibility as a contract, not just field presence: a required
        field added without a default would break every planner call site, and
        this says so in one line."""
        from slayer.core.keys import ColumnKey

        plan = RankedAggregatePlan(
            aggregate_slot_id="s1", agg="last", root_model="orders",
            datasource="test", ranking_time_key=ColumnKey(leaf="created_at"),
        )
        assert plan.grain == []
        assert plan.hidden is False
        assert plan.model_dump()["agg"] == "last"


# --------------------------------------------------------------------------- #
# Render level — the isolated CTE
# --------------------------------------------------------------------------- #


class TestTheIsolatedCte:
    async def test_a_ranked_aggregate_gets_its_own_cte_and_the_host_reads_it(
        self,
    ) -> None:
        """One CTE, and the combined SELECT actually reads the ranked column
        from it — a CTE nobody references would satisfy a bare count."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        names = _ranked_cte_names(sql)
        assert len(names) == 1, sql

        outer = _tree(sql).find(exp.Select)
        projected = [
            c.sql(dialect="postgres") for c in outer.expressions
        ]
        assert any(names[0] in p for p in projected), projected

    async def test_the_host_base_keeps_only_purely_local_aggregates(self) -> None:
        """P-C's central claim, read off the emitted SQL: ``_base`` computes the
        SUM and nothing else. Both halves matter — the SUM must still be there
        (so a vanished ``_base`` fails) and no ranked aggregation may have
        leaked in, in EITHER of its forms."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l"},
                {"formula": "amount:sum", "name": "s"},
            ],
        ))
        base = _cte_body(sql, "_base")
        rendered = base.sql(dialect="postgres").upper()

        assert list(base.find_all(exp.Sum)), rendered
        assert "ROW_NUMBER" not in rendered, rendered
        assert "CASE WHEN" not in rendered, rendered

    async def test_the_ranking_lives_in_the_ranked_cte(self) -> None:
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        body = _the_ranked_body(sql)
        assert list(body.find_all(exp.Window)), body.sql(dialect="postgres")

    async def test_two_ranked_measures_get_two_distinct_ctes(self) -> None:
        """Two aggregates, two CTEs, two distinct allocator-minted names, and
        two distinct joins — so a naming collision that merged them would be
        caught rather than silently returning one measure twice."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        ))
        names = _ranked_cte_names(sql)
        assert len(names) == 2, sql
        assert len(set(names)) == 2, names

        joined = [
            str(getattr(j.this, "alias_or_name", ""))
            for j in _tree(sql).find_all(exp.Join)
        ]
        assert set(names) <= set(joined), (names, joined)

    async def test_the_grain_drives_both_the_partition_and_the_join_back(
        self,
    ) -> None:
        """The rendered half of the one-grain-list contract. A two-member grain
        must produce a two-operand ``PARTITION BY`` and a two-pair null-safe
        join-back. A renderer that partitioned by the status only and joined on
        the month only would satisfy the plan-level test and be wrong here."""
        sql = await _sql(_q(
            dimensions=["status"],
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        body = _the_ranked_body(sql)

        partition = _partition_exprs(body, dialect="postgres")
        assert len(partition) == 2, partition

        pairs = _null_safe_pairs(_join_onto(sql, _ranked_cte_names(sql)[0]))
        assert len(pairs) == 2, pairs

    async def test_the_join_back_is_null_safe(self) -> None:
        """P-I. A NULL grain member must match its own group rather than
        matching nothing and silently receiving NULL."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        join = _join_onto(sql, _ranked_cte_names(sql)[0])
        on_sql = join.args["on"].sql(dialect="postgres").upper()

        assert join.args.get("side", "").upper() == "LEFT", join.sql(dialect="postgres")
        assert "IS NOT DISTINCT FROM" in on_sql, on_sql

    async def test_no_superseded_rank_column_spelling_survives(self) -> None:
        """The rn-suffix scheme and the filtered sentinel columns existed only
        to disambiguate several rankings sharing one scope. One aggregate per
        CTE removes the need, so every one of their spellings must be gone —
        the unsuffixed ``_last_rn`` included."""
        for query in (
            _q(
                dimensions=["status"],
                measures=[{"formula": "amount:last", "name": "l"}],
            ),
            _q(
                dimensions=["status"],
                measures=[
                    {"formula": "amount:last(created_at)", "name": "a"},
                    {"formula": "amount:last(shipped_at)", "name": "b"},
                ],
            ),
            _q(
                dimensions=["status"],
                measures=[
                    {"formula": "big_amount:last", "name": "b"},
                    {"formula": "gold_amount:last", "name": "g"},
                ],
            ),
        ):
            sql = await _sql(query)
            found = _SUPERSEDED_RANK_COLUMNS.search(sql)
            assert found is None, f"{found.group(0)!r} survives in:\n{sql}"

    async def test_a_measure_filter_becomes_a_where_on_the_ranked_rows(self) -> None:
        """The rank-pushdown form ranked NON-matching rows too and masked them
        with a match flag. In its own scope the predicate simply removes them
        BEFORE the ranking, so it belongs in the inner subquery's WHERE — not in
        the window's ORDER BY, and not as a projected flag."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[{"formula": "big_amount:last", "name": "l"}],
        ))
        body = _the_ranked_body(sql)
        inner = body.find(exp.Subquery)
        assert inner is not None, body.sql(dialect="postgres")

        where = inner.this.args.get("where")
        assert where is not None, inner.sql(dialect="postgres")
        assert str(BIG_AMOUNT_THRESHOLD) in where.sql(dialect="postgres")

        windows = list(body.find_all(exp.Window))
        assert len(windows) == 1
        order = windows[0].args.get("order")
        assert order is not None
        assert not list(order.find_all(exp.Case)), order.sql(dialect="postgres")

    async def test_the_inner_scope_projects_named_columns_not_a_star(self) -> None:
        """P-B: a scope exchanges data through PROJECTED columns. ``source.*``
        is the opposite of a projection boundary, and it is why the pre-B9 path
        needed a bolted-on materialiser for crossing values."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        inner = _the_ranked_body(sql).find(exp.Subquery)
        assert inner is not None
        assert not list(inner.this.find_all(exp.Star)), (
            f"the ranked scope re-exports everything:\n"
            f"{inner.sql(dialect='postgres')}"
        )

    async def test_an_expression_that_is_both_grain_and_value_projects_once(
        self,
    ) -> None:
        """Grouping a ranked aggregate by the very expression it ranks used to
        project that expression TWICE — once for the grain site and once for the
        value site, each keeping its own alias map. One materialisation
        mechanism per scope (P-B) collapses them; this is the same defect PR 3
        fixed on the cross-model path.

        Counted over the inner subquery's PROJECTION LIST rather than by text,
        so quoting and dialect formatting cannot defeat it."""
        sql = await _sql(_q(
            dimensions=["cust_region"],
            measures=[{"formula": "cust_region:last", "name": "l"}],
        ))
        inner = _the_ranked_body(sql).find(exp.Subquery)
        assert inner is not None

        rendered = [
            (e.this if isinstance(e, exp.Alias) else e).sql(dialect="postgres")
            for e in inner.this.expressions
        ]
        crossing = [r for r in rendered if "regions" in r and "ROW_NUMBER" not in r]
        assert len(crossing) == 1, rendered

    async def test_the_declared_type_cast_wraps_the_ranked_aggregate(self) -> None:
        """The CAST enforcing the declared type goes OUTSIDE the aggregate, as
        it does today — ``CAST(MAX(CASE …) AS DOUBLE PRECISION)``. Casting the
        input instead would be a different expression with different overflow
        and rounding behaviour, and a string search for the type name cannot
        tell the two apart."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        body = _the_ranked_body(sql)
        casts = [
            c for c in body.find_all(exp.Cast)
            if c.to.this is exp.DataType.Type.DOUBLE
        ]
        assert casts, body.sql(dialect="postgres")
        assert any(isinstance(c.this, exp.Max) for c in casts), (
            f"the declared-type CAST is not outside the aggregate:\n"
            f"{body.sql(dialect='postgres')}"
        )

    async def test_a_grouped_ranked_cte_selects_the_rank_one_row_by_aggregation(
        self,
    ) -> None:
        """The emitted selection form. ``MAX(CASE WHEN <rank> = 1 THEN v END)``
        with a ``GROUP BY`` over the grain — deliberately NOT ``WHERE rn = 1``,
        which returns zero rows where this returns one NULL row, and that
        difference erases the whole result once the grain is empty."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        body = _the_ranked_body(sql)

        assert body.args.get("group") is not None, body.sql(dialect="postgres")
        maxes = list(body.find_all(exp.Max))
        assert maxes, body.sql(dialect="postgres")
        assert any(isinstance(m.this, exp.Case) for m in maxes), (
            body.sql(dialect="postgres")
        )
        # ...and the outer body does no row filtering of its own.
        assert body.args.get("where") is None, body.sql(dialect="postgres")

    async def test_a_scalar_ranked_aggregate_cross_joins_a_one_row_cte(self) -> None:
        """With no grain there is nothing to join on, so the CTE is CROSS
        JOINed. That is only sound because it returns exactly one row — hence
        the aggregate form with no GROUP BY, rather than a bare ``WHERE rn = 1``
        which returns zero rows over an empty source."""
        sql = await _sql(_q(measures=[{"formula": "amount:last", "name": "l"}]))
        body = _the_ranked_body(sql)

        assert body.args.get("group") is None, body.sql(dialect="postgres")
        assert body.args.get("where") is None, body.sql(dialect="postgres")
        assert list(body.find_all(exp.Max)), body.sql(dialect="postgres")

        join = _join_onto(sql, _ranked_cte_names(sql)[0])
        assert join.args.get("on") is None, join.sql(dialect="postgres")
        assert join.args.get("kind", "").upper() == "CROSS", (
            join.sql(dialect="postgres")
        )


class TestFilterRouting:
    async def test_a_row_filter_applies_at_the_host_and_in_the_ranked_cte(
        self,
    ) -> None:
        """The PR-4 B6 ruling, one route over. A LEFT JOIN back propagates a
        VALUE but never an EXCLUSION, so a ROW-phase filter that only reached
        the CTE would silently become "blank out their measure" instead of
        "exclude these rows". Both copies must be emitted."""
        sql = await _sql(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
            filters=["status != 'paid'"],
        ))
        base = _cte_body(sql, "_base").sql(dialect="postgres")
        ranked = _the_ranked_body(sql).sql(dialect="postgres")

        assert "'paid'" in base, base
        assert "'paid'" in ranked, ranked

    async def test_a_host_model_filter_applies_inside_the_ranked_cte(self) -> None:
        """A ``SlayerModel.filters`` entry on the HOST narrows the rows the
        ranking runs over, not just the host base.

        It reaches the CTE as a ROW-phase filter ID like any other — the model's
        filters are entries of ``filters_by_phase``, unlike a TARGET model's,
        which are not in the host's filter list at all and ride on the plan as
        text. Without it, a row the query excludes could still win rank 1 and
        the answer would come back as a value the user filtered away."""
        models = dev1748_models()
        orders = models[0].model_copy(update={"filters": ["status != 'nomatch'"]})
        sql = await _engine_generate(
            query=_q(
                dimensions=["status"],
                measures=[{"formula": "amount:last", "name": "l"}],
            ),
            model=orders, extra_models=models[1:],
            dialect="postgres", validate=False,
        )
        inner = _the_ranked_body(sql).find(exp.Subquery)
        assert inner is not None, sql
        where = inner.this.args.get("where")
        assert where is not None, sql
        assert "'nomatch'" in where.sql(dialect="postgres"), sql
        # ...and the host base keeps its own copy (B6).
        assert "'nomatch'" in _cte_body(sql, "_base").sql(dialect="postgres"), sql

    async def test_a_target_model_filter_applies_inside_the_ranked_target_cte(
        self,
    ) -> None:
        """A target model's own ``filters`` are always-applied WHERE. They must
        narrow the rows the ranking runs over, not the rows it has already
        picked — otherwise a soft-deleted newest row wins rank 1 and the answer
        comes back NULL."""
        models = dev1748_models()
        customers = models[1].model_copy(update={"filters": ["tier IS NOT NULL"]})
        sql = await _engine_generate(
            query=_q(measures=[{"formula": "customers.spend:last", "name": "l"}]),
            model=models[0],
            extra_models=[customers, *models[2:]],
            dialect="postgres", validate=False,
        )
        inner = _the_ranked_body(sql).find(exp.Subquery)
        assert inner is not None, sql
        where = inner.this.args.get("where")
        assert where is not None, sql
        assert "tier" in where.sql(dialect="postgres"), sql


class TestNoFilterIsSilentlyDropped:
    """A ranked CTE emits no HAVING, so a filter the cross-model strategy routes
    there has to be picked up by another scope or it stops applying — a wrong
    answer with no error.

    The plan therefore does not carry the instruction at all, and the planner
    proves the predicate survives. These pin both halves on the shapes that
    reach the HAVING route: a filter on the ranked aggregate itself, and one on
    a DIFFERENT aggregate on the same target."""

    @staticmethod
    def _routing(planned):
        ranked_having = {
            i for p in planned.ranked_aggregate_plans for i in p.having_filter_ids
        }
        covered = set(planned.outer_where_filter_ids)
        for p in planned.cross_model_aggregate_plans:
            covered.update(p.having_filter_ids)
            covered.update(p.where_filter_ids)
        agg_ids = {
            fp.id for fp in planned.filters_by_phase if fp.phase is Phase.AGGREGATE
        }
        return ranked_having, covered, agg_ids

    def test_a_filter_on_the_ranked_aggregate_reaches_the_outer_where(self) -> None:
        planned = _plan(_q(
            measures=[{"formula": "customers.spend:last", "name": "l"}],
            filters=[f"customers.spend:last > {BIG_AMOUNT_THRESHOLD}"],
        ))
        ranked_having, covered, agg_ids = self._routing(planned)
        assert agg_ids, "no aggregate-phase filter was bound"
        assert ranked_having == set(), planned.ranked_aggregate_plans
        assert agg_ids <= set(planned.outer_where_filter_ids)
        assert agg_ids <= covered

    def test_a_filter_on_a_sibling_aggregate_reaches_that_siblings_cte(
        self,
    ) -> None:
        """The other HAVING-route shape: the predicate names a DIFFERENT
        aggregate on the same target, which has an isolated CTE of its own and
        evaluates it there. The ranked plan must claim none of it."""
        planned = _plan(_q(
            measures=[{"formula": "customers.spend:last", "name": "l"}],
            filters=[f"customers.spend:sum > {BIG_AMOUNT_THRESHOLD}"],
        ))
        ranked_having, covered, agg_ids = self._routing(planned)
        assert agg_ids, "no aggregate-phase filter was bound"
        assert ranked_having == set(), planned.ranked_aggregate_plans
        assert agg_ids <= covered
        cm_having = {
            i for p in planned.cross_model_aggregate_plans
            for i in p.having_filter_ids
        }
        assert agg_ids <= cm_having, (
            "the sibling's own CTE must evaluate it"
        )

    async def test_the_predicate_survives_into_the_emitted_sql(self) -> None:
        """The end of the chain: both shapes emit the comparison."""
        for filter_text in (
            f"customers.spend:last > {BIG_AMOUNT_THRESHOLD}",
            f"customers.spend:sum > {BIG_AMOUNT_THRESHOLD}",
        ):
            sql = await _sql(_q(
                measures=[{"formula": "customers.spend:last", "name": "l"}],
                filters=[filter_text],
            ))
            assert f"> {BIG_AMOUNT_THRESHOLD}" in sql, (filter_text, sql)


class TestCrossModelAndRerooting:
    async def test_a_cross_model_ranked_aggregate_is_rooted_at_the_target(
        self,
    ) -> None:
        sql = await _sql(_q(
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ))
        body = _the_ranked_body(sql)
        from_expr = body.find(exp.From)
        assert from_expr is not None
        assert "customers" in from_expr.sql(dialect="postgres"), (
            from_expr.sql(dialect="postgres")
        )

    @pytest.mark.parametrize("dialect", ["postgres", "tsql"])
    async def test_a_rerooted_ranked_aggregate_emits_no_nested_with(
        self, dialect: str,
    ) -> None:
        """A re-rooted cross-model CTE renders its sub-plan as a complete
        statement and splices it in. If that sub-plan emitted its own ``_base``
        plus a combined SELECT the result would be a ``WITH`` inside a CTE body,
        which SQL Server rejects outright. The sub-plan's only isolated
        aggregate IS its answer at its own grain, so it is emitted directly.

        Asserted on the AST — a line-based check passes when the nested ``WITH``
        lands mid-line, and passes vacuously when nothing emits a WITH at all,
        so both halves are stated."""
        sql = await _sql(
            _q(
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.spend:last", "name": "l"}],
            ),
            dialect=dialect,
        )
        tree = _tree(sql, dialect=dialect)
        assert tree.args.get("with_") is not None, f"no WITH at all:\n{sql}"

        for cte in _ctes(sql, dialect=dialect):
            assert cte.this.args.get("with_") is None, (
                f"CTE {cte.alias_or_name!r} carries a nested WITH:\n{sql}"
            )
            assert not list(cte.this.find_all(exp.With)), (
                f"CTE {cte.alias_or_name!r} contains a WITH node:\n{sql}"
            )

    @pytest.mark.parametrize("dialect", ["postgres", "tsql", "sqlite"])
    async def test_a_rerooted_ranked_plan_with_a_row_filter_stays_one_with(
        self, dialect: str,
    ) -> None:
        """A row filter interns a HIDDEN row slot in the re-rooted sub-plan, and
        that used to stop the sub-plan collapsing to its ranked CTE.

        The consequence was not a nested ``WITH`` — sqlglot flattens one into
        the parent chain — but something a reader would never spot: TWO CTEs
        named ``_base``, which is invalid SQL on every dialect. A hidden row
        slot is filter scaffolding that ``_base`` neither projects nor groups
        by, and the ranked CTE applies the very same ROW filters, so it is not a
        reason to refuse the collapse.

        Asserted as CTE-name uniqueness rather than "no nested WITH", because
        the flattening means the nested WITH is not what a reviewer would
        find."""
        models = dev1748_models()
        sql = await _engine_generate(
            query=_q(
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.spend:last", "name": "l"}],
                filters=["customers.tier == 'gold'"],
            ),
            model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False,
        )
        assert_unique_cte_names(sql=sql, dialect=dialect)
        names = _cte_names(sql, dialect=dialect)
        assert names.count("_base") == 1, names
        for cte in _ctes(sql, dialect=dialect):
            assert not list(cte.this.find_all(exp.With)), (
                f"CTE {cte.alias_or_name!r} carries a WITH:\n{sql}"
            )
        # The ranking is still there, and the filter narrows the ranked rows.
        cm = next(c for c in _ctes(sql, dialect=dialect) if c.alias_or_name.startswith("_cm_"))
        body = cm.this.sql(dialect=dialect)
        assert "ROW_NUMBER" in body, body
        assert "'gold'" in body, body

    async def test_a_subplan_that_cannot_collapse_fails_loudly(self) -> None:
        """The residual, and why it is a raise.

        A sub-plan renders into a CTE BODY, which cannot carry a ``WITH`` of its
        own — SQL Server rejects a nested one outright, and sqlglot's flattening
        turns it into a duplicate ``_base`` instead. Every shape reachable today
        collapses (a 192-shape sweep found none that does not), so this guard is
        a belt. It is a raise rather than a best-effort render because the
        alternative is invalid SQL that no unit test reads.

        DEV-1835: the local first/last now desugars into a regroup attach, so a
        plan that cannot collapse to a single ``_cm_`` CTE trips the matching
        nested-CTE-body guard on the regroup attach path instead — same loud
        failure, one route over."""
        from slayer.sql.generator import SQLGenerator

        planned = _plan(_q(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
            order=[{"column": "l", "direction": "desc"}],
        ))
        # An ORDER BY is one of the collapse's disqualifiers, so this plan is a
        # stand-in for any sub-plan that needs more than its ranked CTE.
        # Generator and bundle are hoisted so the ONLY call that can raise
        # inside ``pytest.raises`` is the one under test (Sonar S5778).
        gen = SQLGenerator(dialect="tsql")
        bundle = dev1748_bundle()
        with pytest.raises(NotImplementedError, match="regroup attach nested in a CTE body"):
            gen.generate_from_planned(planned, bundle=bundle, as_cte_body=True)

    async def test_the_rerooted_cte_body_is_the_ranking_itself(self) -> None:
        """The positive half of the collapse: the re-rooted CTE does not merely
        avoid a nested WITH, it IS the ranked select — so the collapse happened
        rather than the ranking being dropped."""
        sql = await _sql(_q(
            dimensions=["customers.regions.name"],
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ))
        bodies = [c.this for c in _ctes(sql) if c.alias_or_name != "_base"]
        assert bodies, sql
        assert any(list(b.find_all(exp.Window)) for b in bodies), sql


class TestNamingIsPrivate:
    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    async def test_two_measures_whose_aliases_fold_alike_get_distinct_ctes(
        self, dialect: str,
    ) -> None:
        """P-F. CTE names are minted by the collision-aware allocator, which
        case-FOLDS on dialects whose unquoted identifiers do.

        ``Rev`` and ``rev`` are two distinct measures with two distinct result
        keys, so their ``_rk_`` CTEs must be two distinct names — but the
        preferred names differ only in case, which on these dialects is not a
        difference at all. Minting the name from the alias directly would emit a
        duplicate ``WITH`` entry; the allocator walks the second to ``…_2``.
        This is the failure mode DEV-1726 fixed for the transform CTEs and PR 1
        for ``_cm_``, and it must not come back for ``_rk_``."""
        sql = await _sql(
            _q(
                dimensions=["status"],
                measures=[
                    {"formula": "amount:last", "name": "Rev"},
                    {"formula": "amount:first", "name": "rev"},
                ],
            ),
            dialect=dialect,
        )
        names = _ranked_cte_names(sql, dialect=dialect)
        assert len(names) == 2, sql
        assert len({n.lower() for n in names}) == 2, names

    async def test_a_physical_column_named_like_the_rank_column_is_harmless(
        self,
    ) -> None:
        """The ranked scope's internal names are safe precisely BECAUSE it
        projects a named list rather than ``*``: a physical column called
        ``_rk_rn`` is never re-exported, so it cannot capture the rank column's
        reference.

        Executed, not just parsed — the physical ``_rk_rn`` values are seeded to
        disagree with the real ranking, so an implementation that resolved the
        reference to the physical column returns the wrong row rather than
        merely producing SQL that happens to parse."""
        colliding = SlayerModel(
            name="collide", sql_table="collide", data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="created_at", type=DataType.TIMESTAMP),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="_rk_rn", type=DataType.INT),
                Column(name="_rk_value", type=DataType.DOUBLE),
            ],
        )
        with tempfile.TemporaryDirectory() as d:
            import sqlite3

            db_path = os.path.join(d, "collide.db")
            con = sqlite3.connect(db_path)
            con.execute(
                "CREATE TABLE collide (id INTEGER PRIMARY KEY, status TEXT, "
                "created_at TEXT, amount REAL, _rk_rn INTEGER, _rk_value REAL)"
            )
            con.executemany(
                "INSERT INTO collide VALUES (?,?,?,?,?,?)",
                [
                    # The OLDER row carries the physical _rk_rn = 1 and a
                    # _rk_value that is nobody's answer. A correct ``last``
                    # returns 2.0; reading either physical column returns 1.0
                    # or 999.0.
                    (1, "a", "2024-01-01", 1.0, 1, 999.0),
                    (2, "a", "2024-02-01", 2.0, 2, 998.0),
                ],
            )
            con.commit()
            con.close()

            from slayer.core.models import DatasourceConfig
            from slayer.storage.yaml_storage import YAMLStorage

            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="sqlite", database=db_path),
            )
            await storage.save_model(colliding)
            eng = SlayerQueryEngine(storage=storage)
            response = await eng.execute(SlayerQuery(
                source_model="collide", dimensions=["status"],
                measures=[{"formula": "amount:last", "name": "l"}],
            ))
        assert response.data == [{"collide.status": "a", "collide.l": 2.0}]


# --------------------------------------------------------------------------- #
# Execution — the answers the new shape must still produce
# --------------------------------------------------------------------------- #


class TestTheNewShapeExecutes:
    async def test_the_isolated_shape_returns_the_same_grouped_answers(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """The matrix asserts this exhaustively; this is the smoke test that the
        new SQL runs at all on a real engine."""
        response = await engine.execute(SlayerQuery(
            source_model="orders", dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l"},
                {"formula": "amount:sum", "name": "s"},
            ],
        ))
        by_status = {r["orders.status"]: r["orders.l"] for r in response.data}
        assert by_status["paid"] == PAID_LAST
        assert by_status["fan"] == FAN_LAST

    async def test_the_ranked_cte_itself_holds_one_row_per_grain(
        self, engine: SlayerQueryEngine, db_path: str,
    ) -> None:
        """The invariant the grain join-back depends on, asserted on the CTE
        rather than on the final result.

        Counting distinct groups in the OUTPUT proves nothing — the outer query
        groups by the same grain, so its rows are unique however many rows the
        CTE held, and the duplication would show up only as a multiplied
        sibling. This lifts the generated ``_rk_`` CTE out of the statement and
        counts its rows directly, over a grain that crosses a 1:N join so a
        failure to collapse would really produce duplicates."""
        import sqlite3

        response = await engine.execute(
            SlayerQuery(
                source_model="orders", dimensions=["order_tags.name"],
                measures=[{"formula": "amount:last", "name": "l"}],
            ),
            dry_run=True,
        )
        sql = response.sql
        assert sql is not None

        names = _ranked_cte_names(sql, dialect="sqlite")
        assert len(names) == 1, sql
        body = _cte_body(sql, names[0], dialect="sqlite").sql(dialect="sqlite")

        con = sqlite3.connect(db_path)
        try:
            grain = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT 1) FROM ({body})",
            ).fetchone()
            per_grain = con.execute(
                f'SELECT MAX(n) FROM ('
                f'  SELECT COUNT(*) AS n FROM ({body}) AS _probe'
                f'  GROUP BY _probe."orders.order_tags.name"'
                f')',
            ).fetchone()
        finally:
            con.close()

        assert grain[0] > 1, f"the probe found no rows to check:\n{body}"
        assert per_grain[0] == 1, (
            f"the ranked CTE emits {per_grain[0]} rows for some grain value, "
            f"which the LEFT JOIN back would multiply into the host:\n{body}"
        )

    async def test_an_order_only_ranked_measure_sorts_without_being_projected(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """The hidden ranked plan, end to end: it must exist to sort by and be
        absent from the output."""
        response = await engine.execute(SlayerQuery(
            source_model="orders", dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "s"}],
            order=[{"column": "amount:last", "direction": "desc"}],
        ))
        assert list(response.data[0]) == ["orders.status", "orders.s"]
        assert [r["orders.status"] for r in response.data][:2] == ["fan", None]

    async def test_a_scalar_ranked_measure_over_an_empty_source_keeps_the_row(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """The reason the CTE is an aggregate rather than a ``WHERE rn = 1``:
        with an empty grain the join-back is a CROSS JOIN, so a zero-row CTE
        would erase the entire result instead of returning one NULL."""
        response = await engine.execute(SlayerQuery(
            source_model="empty_orders",
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        assert response.data == [{"empty_orders.l": None}]


class TestGrainMembersAreAllUsed:
    async def test_every_grain_member_reaches_the_answer(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """The executed counterpart of the partition/join-back structure test.

        Grouping by status AND month means each ``(status, month)`` pair must
        rank independently. The ``nullval`` status spans two months whose
        answers differ — one is NULL — so a renderer that dropped either grain
        member from the partition or the join-back returns the same value for
        both months and fails."""
        response = await engine.execute(SlayerQuery(
            source_model="orders", dimensions=["status"],
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        nullval = {
            r["orders.created_at"]: r["orders.l"]
            for r in response.data if r["orders.status"] == "nullval"
        }
        assert nullval == {"2024-01-01": 41.0, "2024-02-01": None}

        null_status = {
            r["orders.created_at"]: r["orders.l"]
            for r in response.data if r["orders.status"] is None
        }
        assert null_status["2024-02-01"] == NULL_STATUS_LAST

    async def test_the_fan_group_ranks_within_each_tag(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A 1:N grain member. Order 15 reaches three tag groups; ``rush`` also
        holds the newer order 16, so the two answers differ."""
        response = await engine.execute(SlayerQuery(
            source_model="orders", dimensions=["order_tags.name"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        by_tag = {
            r["orders.order_tags.name"]: r["orders.l"] for r in response.data
        }
        assert by_tag["gift"] == FAN_FIRST
        assert by_tag["rush"] == FAN_LAST
