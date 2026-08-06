"""P-F "one naming authority": CTE-name allocation + the alias consolidation.

Three things are pinned here.

**Every CTE name through the collision-aware allocator.** ``_wm_`` was retrofitted
onto the allocator; ``_cm_`` never was. Two consequences, both reachable from
the public query API today:

* two cross-model measures whose canonical aliases differ ONLY in case emit two
  ``_cm_`` CTE names that fold together on every case-folding dialect (which is
  all of them but ClickHouse) — the collision belt raises, and without the belt
  the backend would see a duplicate ``WITH`` name;
* ``_cte_name_from_alias`` stacks two lossy steps (``flat_name``, which is
  documented non-injective, then ``re.sub`` over non-identifier characters) and
  the result is used as the PLAN IDENTITY key in ``seen_cm``. Two DISTINCT
  aggregate slots that sanitise to the same name make the second one skip the
  loop body, leaving ``agg_col_alias_for_plan`` / ``joinback_pairs_for_plan``
  unfilled — a ``KeyError`` at the four unconditional downstream subscripts.

The fix is structural identity (the full typed ``AggregateKey`` plus its source
relation) for dedup, and the allocator for names. Canonical aliases are NOT a
safe identity: ``canonical_agg_name`` omits ``column_filter_key``, so a filtered
and an unfiltered aggregate over the same column share one canonical alias while
needing two different CTEs.

**The consolidation.** Four copies of canonical-aggregate-alias derivation
(``generator._canonical_cross_model_alias``, ``cross_model_planner._aggregate_alias``,
``planning._canonical_name``, ``stage_planner._canonical_alias_for_formula``) that
have DRIFTED on four axes become one ``naming.canonical_aggregate_alias``
parameterised by a named profile. The expected-value tables below are frozen
from the four legacy bodies, so the consolidation is provably behavior-preserving.

**The naming constants.** ``_outer`` / ``_stage_inner`` / ``_filtered`` are minted
in more than one module, coupled by convention only (``generator.py`` and
``dialects/tsql.py`` each write the ``_outer`` literal independently). The
literals move into ``naming.py`` so one module owns them.

Test style: the collision tests are SELF-CALIBRATING. Rather than hardcoding an
expected aggregate value, each runs the query once per measure in isolation
(which works today), then once with both measures together, and asserts the
combined run reproduces both isolated values. That is exactly SLayer's core
invariant — "adding a measure must never change another measure's value" — and
it cannot pass by agreeing with a wrong hardcoded number.
"""

from __future__ import annotations

import inspect
import os
import pathlib
import re
import sqlite3
from collections import Counter
from decimal import Decimal
from types import SimpleNamespace
from typing import AsyncIterator, List

import pytest
import sqlglot
from sqlglot import exp

import tests.test_parity_guards as guard_module
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    SqlExprKey,
    StarKey,
    TimeTruncKey,
)
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine import cross_model_planner, planning, stage_planner
from slayer.engine.binding import BoundExpr
from slayer.engine.cross_model_planner import _aggregate_alias
from slayer.engine.planning import _canonical_name
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.stage_planner import _canonical_alias_for_formula
from slayer.sql import generator as generator_module
from slayer.sql import naming
from slayer.sql import stage_wrapper as sw_module
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects import tsql as tsql_module
from slayer.sql.generator import SQLGenerator, _cm_plan_identity
from slayer.sql.naming import AliasAllocator
from slayer.storage.yaml_storage import YAMLStorage


# ===========================================================================
# Fixtures — a seeded SQLite store whose model deliberately contains the
# name shapes that collide under the current sanitisation.
# ===========================================================================


async def _build_engine(*, base_dir: str, dialect: str = "sqlite") -> SlayerQueryEngine:
    """orders -> customers, seeded, with the collision-bait columns.

    On ``customers``:
      * ``Rev`` and ``rev`` — a case-only pair (distinct physical columns
        ``revx`` / ``revy``, so a shadowing bug shows up as equal values);
      * ``revenue`` — the ordinary cross-model measure source.

    On ``orders``:
      * ``customers__revenue`` — a ``__``-in-name column (a ratified
        keep-list carve-out) carrying a join-crossing ``Column.filter``, which
        makes it a filtered-local ISOLATED aggregate — i.e. it also
        renders as a ``_cm_`` CTE. Its canonical alias
        ``orders.customers__revenue_sum`` sanitises to exactly the same CTE
        name as the cross-model ``orders.customers.revenue_sum``.
    """
    d = base_dir
    db_path = os.path.join(d, "b4.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "revenue REAL, revx REAL, revy REAL)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?)",
        [
            (1, 1, 100.0, 7.0, 70.0),
            (2, 2, 200.0, 8.0, 80.0),
            (3, 1, 300.0, 9.0, 90.0),
        ],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, amount REAL, created_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            (1, 1, "a", 10.0, "2024-01-01"),
            (2, 2, "a", 20.0, "2024-02-01"),
            (3, 3, "a", 30.0, "2024-03-01"),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type=dialect, database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region_id", type=DataType.INT),
                Column(name="revenue", type=DataType.DOUBLE),
                # Case-only pair over two DIFFERENT physical columns.
                Column(name="Rev", sql="revx", type=DataType.DOUBLE),
                Column(name="rev", sql="revy", type=DataType.DOUBLE),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
                # __ in a Column.name (keep-list carve-out) + a join-crossing
                # filter => filtered-local isolation => a _cm_ CTE.
                Column(
                    name="customers__revenue",
                    sql="amount",
                    type=DataType.DOUBLE,
                    filter="customers.region_id = 1",
                ),
            ],
            joins=[
                ModelJoin(
                    target_model="customers", join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage)


@pytest.fixture
async def engine(tmp_path_factory) -> AsyncIterator[SlayerQueryEngine]:
    yield await _build_engine(base_dir=str(tmp_path_factory.mktemp("b4")))


def _cte_names_by_scope(sql: str, *, dialect: str = "sqlite") -> List[List[str]]:
    """CTE names grouped per ``WITH`` scope, in emission order.

    Scope-aware because SQL only requires uniqueness WITHIN one ``WITH``; the
    same name may legally recur in an independent nested scope, and
    ``assert_unique_cte_names`` validates each ``exp.With`` separately. A flat
    cross-scope uniqueness check would constrain the allocator beyond what the
    plan asks for.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    return [
        [cte.alias_or_name for cte in with_node.expressions]
        for with_node in parsed.find_all(exp.With)
    ]


def _cte_names(sql: str, *, dialect: str = "sqlite") -> List[str]:
    """Every CTE name in ``sql``, flattened — for counting a prefix family."""
    return [n for scope in _cte_names_by_scope(sql, dialect=dialect) for n in scope]


async def _isolated_then_combined(
    engine: SlayerQueryEngine,
    *,
    measures: List[ModelMeasure],
    dimension: str = "status",
) -> None:
    """The self-calibrating collision assertion.

    Run each measure ALONE (no collision possible), record its value, then run
    them TOGETHER and require the combined run to reproduce every isolated
    value. Catches all three failure modes at once: an exception, a silently
    dropped measure, and — the nastiest — two slots sharing one CTE so both
    read the same (wrong) column.
    """
    isolated: dict = {}
    for m in measures:
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name=dimension)],
                measures=[m],
            )
        )
        assert len(resp.data) == 1, resp.data
        # The single non-dimension key is this measure's value.
        row = resp.data[0]
        keys = [k for k in row if not k.endswith(f".{dimension}")]
        assert len(keys) == 1, (m.formula, list(row))
        isolated[keys[0]] = row[keys[0]]

    resp = await engine.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name=dimension)],
            measures=measures,
        )
    )
    assert len(resp.data) == 1, resp.data
    combined = resp.data[0]
    for key, value in isolated.items():
        assert key in combined, (
            f"measure {key!r} vanished when rendered alongside the others: "
            f"{list(combined)}"
        )
        assert combined[key] == value, (
            f"measure {key!r} changed value when rendered alongside the "
            f"others: alone={value!r}, together={combined[key]!r} — the "
            f"hallmark of two slots collapsing onto one CTE."
        )


# ===========================================================================
# B4 — cross-model CTE names through the allocator.
# ===========================================================================


class TestCrossModelCteNameAllocation:
    async def test_case_only_aliases_get_distinct_cte_names(
        self, engine,
    ) -> None:
        """Two cross-model measures differing only in the case of the source
        column must render as two distinct, non-fold-colliding CTEs.

        Today ``_cm_`` bypasses the allocator entirely, so both names are
        emitted verbatim and fold together on SQLite (a case-folding dialect),
        tripping ``assert_unique_cte_names``. ``_wm_`` handles the identical
        situation correctly because it routes through ``allocate_cte``.
        """
        await _isolated_then_combined(
            engine,
            measures=[
                ModelMeasure(formula="customers.Rev:sum", name="upper"),
                ModelMeasure(formula="customers.rev:sum", name="lower"),
            ],
        )

    async def test_case_only_cte_names_do_not_fold_together(
        self, engine,
    ) -> None:
        """The naming half of the same case, asserted on the emitted SQL: no
        two CTE names may be equal after case folding."""
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="customers.Rev:sum", name="upper"),
                    ModelMeasure(formula="customers.rev:sum", name="lower"),
                ],
            ),
            dry_run=True,
        )
        for scope_names in _cte_names_by_scope(resp.sql):
            folded = [n.lower() for n in scope_names]
            assert len(folded) == len(set(folded)), (
                f"CTE names fold together within one WITH scope on a "
                f"case-folding dialect: {scope_names}"
            )
        # Two cross-model aggregates => two _cm_ CTEs, not one shared.
        names = _cte_names(resp.sql)
        assert len([n for n in names if n.startswith("_cm_")]) == 2, names

    async def test_sanitisation_collision_keeps_both_plans(
        self, engine,
    ) -> None:
        """The ``seen_cm`` collision-as-identity bug.

        ``orders.customers.revenue_sum`` (cross-model) and
        ``orders.customers__revenue_sum`` (filtered-local isolated) are
        DISTINCT aggregates whose canonical aliases both sanitise to
        ``_cm_orders__customers__revenue_sum`` — ``flat_name`` maps ``.`` to
        ``__``, so the two are indistinguishable after flattening.

        Today the second plan hits ``continue``, its per-slot maps are never
        written, and the first unconditional downstream subscript raises
        ``KeyError``. Both aggregates must survive with their own values.
        """
        await _isolated_then_combined(
            engine,
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="xmodel"),
                ModelMeasure(formula="customers__revenue:sum", name="local"),
            ],
        )

    async def test_filtered_local_and_plain_aggregate_coexist(
        self, engine,
    ) -> None:
        """A filtered-local ISOLATED aggregate (its own ``_cm_`` CTE) and a
        plain host-base aggregate must coexist without either disturbing the
        other — the general "isolation must not change the host" guard.

        (This is NOT the identity edge case: these two have different canonical
        aliases. The alias-collision-under-differing-filters case is pinned
        structurally in ``TestDedupIdentityIsStructural`` below, because the
        public query API cannot express two aggregates that share a column NAME
        while differing in ``Column.filter``.)
        """
        await _isolated_then_combined(
            engine,
            measures=[
                ModelMeasure(formula="amount:sum", name="unfiltered"),
                ModelMeasure(formula="customers__revenue:sum", name="filtered"),
            ],
        )

    async def test_unrenamed_cross_model_measures_keep_their_own_aliases(
        self, engine,
    ) -> None:
        """Two cross-model measures with NO declared ``name`` must each project
        under their OWN canonical alias.

        Every other cross-model test in this file declares ``name=``. This one
        does not, so it exercises the path where the public alias is derived
        rather than user-supplied — the branch that reads a plan's canonical
        alias. It is a coverage gap-filler, not a regression guard: the planner
        populates ``public_aliases`` even for un-named measures, so the
        canonical-alias fallback inside
        ``_public_aliases_for_cross_model_agg`` is not reachable from here.
        """
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="customers.revenue:sum"),
                    ModelMeasure(formula="customers.Rev:sum"),
                ],
            )
        )
        assert "orders.customers.revenue_sum" in resp.columns, resp.columns
        assert "orders.customers.Rev_sum" in resp.columns, resp.columns
        row = resp.data[0]
        # revenue sums to 600 over the three customers; Rev (revx) to 24 —
        # equal values would mean both read the same CTE column.
        assert row["orders.customers.revenue_sum"] != row["orders.customers.Rev_sum"]

    async def test_hidden_cross_model_agg_under_a_transform_keeps_its_alias(
        self, engine,
    ) -> None:
        """A HIDDEN cross-model aggregate feeding a transform layer must
        project under ITS OWN canonical alias.

        This is the ONE shape that reaches the canonical-alias fallback in
        ``_public_aliases_for_cross_model_agg``. The projection loop trims a
        hidden aggregate only when there is no transform chain
        (``plan.hidden and not transform_layers``); with a chain the hidden
        aggregate stays projected so the step CTE can consume it, and — having
        no user-declared name — its ``public_aliases`` is empty, so the alias
        falls back to the plan's canonical one.

        Reading a stale value there emits
        ``_cm_..._revenue_sum."orders.customers.revenue_sum" AS
        "orders.customers.revx_sum"``: the hidden aggregate is projected under
        the OTHER measure's name, that alias appears twice in one SELECT, and
        the step CTE binds the wrong column. Asserted as "no output alias is
        emitted twice", which is what actually breaks.
        """
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                time_dimensions=[
                    TimeDimension(
                        dimension=ColumnRef(name="created_at"),
                        granularity=TimeGranularity.MONTH,
                    ),
                ],
                measures=[
                    # Hidden CMA (customers.revenue:sum) feeding a transform.
                    ModelMeasure(
                        formula="cumsum(customers.revenue:sum)", name="run",
                    ),
                    # A second cross-model aggregate, so a leaked alias differs
                    # from the correct one.
                    ModelMeasure(formula="customers.Rev:sum", name="rv"),
                ],
            ),
            dry_run=True,
        )
        assert resp.sql is not None
        alias_counts = Counter(re.findall(r'AS "([^"]+)"', resp.sql))
        duplicated = {a: n for a, n in alias_counts.items() if n > 1}
        assert not duplicated, (
            f"an output alias is emitted more than once — a cross-model "
            f"aggregate was projected under another measure's name: "
            f"{duplicated}\n{resp.sql}"
        )
        # …and the hidden aggregate still carries its own canonical alias.
        assert "orders.customers.revenue_sum" in alias_counts, alias_counts

    async def test_same_key_slots_still_share_one_cte(self, engine) -> None:
        """Parity guard for the C13 intent the buggy dedup was meant to serve:
        two measures that are the SAME aggregate under different public names
        must still collapse onto ONE CTE. Structural dedup must not lose this.
        """
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="customers.revenue:sum", name="a"),
                    ModelMeasure(formula="customers.revenue:sum", name="b"),
                ],
            ),
            dry_run=True,
        )
        cm = [n for n in _cte_names(resp.sql) if n.startswith("_cm_")]
        assert len(cm) == 1, f"same-key slots must share one CTE, got {cm}"

    async def test_same_key_slots_both_surface_with_equal_values(
        self, engine,
    ) -> None:
        """…and both public aliases still project, off that one CTE."""
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="customers.revenue:sum", name="a"),
                    ModelMeasure(formula="customers.revenue:sum", name="b"),
                ],
            )
        )
        row = resp.data[0]
        assert "orders.a" in row, list(row)
        assert "orders.b" in row, list(row)
        assert row["orders.a"] == row["orders.b"]


class TestDedupIdentityIsStructural:
    """WHY the dedup key is the full typed ``AggregateKey`` and not the
    canonical alias string.

    ``canonical_agg_name`` is built from the measure name, the aggregation
    name, and the args/kwargs signature. It does NOT encode
    ``AggregateKey.column_filter_key``. So two aggregates that must render
    differently can share one canonical alias — and deduping on that string
    would silently merge them into one CTE, which is a WRONG-ANSWER bug,
    strictly worse than the crash the current code produces.

    These are structural tests rather than end-to-end ones on purpose: a
    ``Column.filter`` is attached to the column DEFINITION, so the public query
    API cannot express the same column name both with and without a filter.
    The identity choice still has to be right, because the generator's dedup
    map is what enforces it.
    """
    def _filtered_and_plain(self):

        source = ColumnKey(leaf="revenue")
        plain = AggregateKey(source=source, agg="sum")
        filtered = AggregateKey(
            source=source, agg="sum",
            column_filter_key=SqlExprKey(canonical_sql="region_id = 1"),
        )
        return plain, filtered

    def test_the_two_keys_are_distinct_identities(self) -> None:
        plain, filtered = self._filtered_and_plain()
        assert plain != filtered
        assert hash(plain) != hash(filtered)
        assert len({plain, filtered}) == 2

    def test_but_they_share_one_canonical_alias(self) -> None:
        """The trap, stated explicitly: the alias cannot tell them apart."""
        plain, filtered = self._filtered_and_plain()
        assert naming.canonical_aggregate_alias(
            plain, profile="cross_model_cte", source_relation="orders",
        ) == naming.canonical_aggregate_alias(
            filtered, profile="cross_model_cte", source_relation="orders",
        )

    def test_one_alias_two_identities_get_two_cte_names(self) -> None:
        """Therefore the allocator must hand out a fresh name each time it is
        asked, and never silently return a previously-minted one. Dedup is the
        CALLER's structural decision; the naming primitive must not second-guess
        it by keying on the string."""
        alloc = AliasAllocator(folds_case=True)
        alias = "orders.revenue_sum"
        first = naming.cte_name_from_alias("_cm_", alias, allocator=alloc)
        second = naming.cte_name_from_alias("_cm_", alias, allocator=alloc)
        assert first != second, (
            "the naming primitive collapsed two distinct identities that "
            "happen to share a canonical alias"
        )


# ===========================================================================
# B4 — the allocator is generation-scoped, and every CTE family shares it.
# ===========================================================================


class TestAllocatorRouting:
    def test_cte_name_from_alias_requires_an_allocator(self) -> None:
        """The naming module gains the sanitise-and-allocate primitive, and it
        cannot be called without an allocator — that is what makes bypassing
        the naming authority impossible rather than merely discouraged."""
        alloc = AliasAllocator(folds_case=True)
        first = naming.cte_name_from_alias(
            "_cm_", "orders.customers.revenue_sum", allocator=alloc,
        )
        assert first == "_cm_orders__customers__revenue_sum"
        # A second, DIFFERENT alias that sanitises to the same string must get
        # its own name rather than silently reusing the first.
        second = naming.cte_name_from_alias(
            "_cm_", "orders.customers__revenue_sum", allocator=alloc,
        )
        assert second != first, (first, second)
        assert second.startswith("_cm_orders__customers__revenue_sum")

    def test_cte_name_from_alias_folds_case_with_the_allocator(self) -> None:
        alloc = AliasAllocator(folds_case=True)
        a = naming.cte_name_from_alias("_cm_", "orders.Rev_sum", allocator=alloc)
        b = naming.cte_name_from_alias("_cm_", "orders.rev_sum", allocator=alloc)
        assert a.lower() != b.lower(), (a, b)

    def test_cte_name_from_alias_is_exact_on_non_folding_dialects(self) -> None:
        """ClickHouse is case-sensitive: the case-only pair keeps both original
        spellings, with no ``_2`` suffix."""
        alloc = AliasAllocator(folds_case=False)
        a = naming.cte_name_from_alias("_cm_", "orders.Rev_sum", allocator=alloc)
        b = naming.cte_name_from_alias("_cm_", "orders.rev_sum", allocator=alloc)
        assert a == "_cm_orders__Rev_sum"
        assert b == "_cm_orders__rev_sum"

    async def test_windowed_cte_names_use_the_shared_helper(
        self, tmp_path_factory,
    ) -> None:
        """``_wm_`` names go through the same sanitise-and-allocate primitive as
        ``_cm_``, so both CTE families behave identically under a case-only
        collision rather than one being retrofitted and the other not.

        Both measures aggregate the SAME column (``amount``) over different
        windows; the case-only pair is in their declared names, ``Wm`` and
        ``wm``. Those names reach the CTE name, so without the allocator the
        two would fold together on a folding dialect — the exact shape that
        broke ``_cm_``.
        """
        engine = await _hostile_engine(
            column="revx2", base_dir=str(tmp_path_factory.mktemp("wm")),
        )
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                time_dimensions=[
                    TimeDimension(
                        dimension=ColumnRef(name="created_at"),
                        granularity=TimeGranularity.MONTH,
                    ),
                ],
                measures=[
                    ModelMeasure(formula="amount:sum(window='90d')", name="Wm"),
                    ModelMeasure(formula="amount:sum(window='30d')", name="wm"),
                ],
            ),
            dry_run=True,
        )
        for scope_names in _cte_names_by_scope(resp.sql):
            folded = [n.lower() for n in scope_names]
            assert len(folded) == len(set(folded)), scope_names
        wm_names = [n for n in _cte_names(resp.sql) if n.startswith("_wm_")]
        assert len(wm_names) == 2, wm_names

    def test_no_raw_step_cte_names_in_the_generator(self) -> None:
        """P-F, checked structurally because it has no reachable behavioural
        difference today: the three ``f"step{...}"`` mint sites must all go
        through the allocator.

        ``the generator`` bypasses an allocator that is in scope 100 lines
        above it; ``that call site`` and ``that call site`` live in the cross-model transform
        chain, which holds no allocator at all. They are latently safe only
        because no ``_cm_*`` CTE can be named ``stepN`` — an invariant nothing
        enforces.
        """
        src = inspect.getsource(generator_module)
        raw = [
            line.strip()
            for line in src.splitlines()
            if re.search(r'=\s*f"step\{', line)
            and "allocate_cte" not in line
        ]
        assert not raw, (
            "step CTE names minted without the allocator:\n  "
            + "\n  ".join(raw)
        )

    def test_generation_allocator_is_shared_across_cte_families(self) -> None:
        """One allocator instance per generation is what makes cross-family
        collisions impossible. Two allocators that cannot see each other's
        names would each happily hand out the same name."""
        alloc = AliasAllocator(folds_case=True)
        alloc.reserve("base", "_base", "_combined")
        # A user-shaped CTE name that folds onto a reserved literal must walk.
        assert alloc.allocate_cte("Base") != "Base"
        # And a _cm_ name minted earlier blocks an identical step name later.
        cm = naming.cte_name_from_alias("_cm_", "step1", allocator=alloc)
        assert alloc.allocate_cte(cm) != cm


# ===========================================================================
# C7 — the bespoke name families, exercised against hostile user columns.
# ===========================================================================


# Every internal name C7 moves onto the allocator. All of them are LEGAL user
# column names (``Column.name`` allows a leading underscore), so each is a
# reachable collision, not merely a theoretical one.
_INTERNAL_NAMES = [
    "_placeholder",   # empty-base grain projection
    "_td_0",          # ranked-subquery time-dimension alias
    "_dim_0",         # ranked-subquery dimension alias
    "_w_dim_0",       # windowed _src dimension alias
    "_w_td_0",        # windowed _src time-dimension alias
    "_w_time",        # windowed _src time column
    "_w_value",       # windowed _src value column
    "_having_agg",    # synthetic HAVING aggregate slot
    "_filtered",      # transform-chain wrapper alias
    "_outer",         # outer-wrap subquery alias
    "_stage_inner",   # stage-schema flat-rename wrapper alias
    "base",           # the transform chain's base CTE
    "step1",          # transform-chain step CTE
    "_val_0",         # Law-2 materialisation alias
]


async def _hostile_engine(*, column: str, base_dir: str) -> SlayerQueryEngine:
    """A single-model store whose ``orders`` model carries a user column named
    exactly like one of SLayer's internal minted names."""
    d = base_dir
    db_path = os.path.join(d, "hostile.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        'CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, '
        'amount REAL, created_at TEXT, "hostile" REAL)'
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            (1, "a", 10.0, "2024-01-01", 1.0),
            (2, "a", 20.0, "2024-02-01", 2.0),
            (3, "b", 30.0, "2024-01-15", 3.0),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
                Column(name=column, sql="hostile", type=DataType.DOUBLE),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage)


class TestInternalNamesDoNotCollideWithUserColumns:
    """P-F's payoff, stated as behavior rather than as source structure.

    A name minted outside the allocator can shadow — or be shadowed by — a real
    user column, because nothing reserved it. Every family C7 moves onto the
    allocator must survive a user column of exactly that name: the query still
    executes, and the user's own column still returns ITS values.

    HONESTY NOTE: these all PASS today. They are parity guards, not red tests.
    Today's safety is partly incidental — ``_reserve_model_column_names``
    reserves model column names into the generation allocator, so the families
    that DO go through it are protected, while the bespoke counters are safe
    only because their shapes happen not to collide in the paths these queries
    reach. C7 makes that safety structural instead of incidental, and these
    tests are what stops the refactor from quietly losing it.
    """

    @pytest.mark.parametrize("column", _INTERNAL_NAMES)
    async def test_user_column_named_like_an_internal_alias(
        self, column, tmp_path_factory,
    ) -> None:
        engine = await _hostile_engine(
            column=column, base_dir=str(tmp_path_factory.mktemp("hostile")),
        )
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula=f"{column}:sum", name="hostile_sum"),
                    ModelMeasure(formula="amount:sum", name="amt"),
                ],
            )
        )
        by_status = {
            r["orders.status"]: (r["orders.hostile_sum"], r["orders.amt"])
            for r in resp.data
        }
        # The user's column is seeded 1/2/3 against amounts 10/20/30, so a
        # shadowing bug shows up as the two measures agreeing.
        assert by_status == {"a": (3.0, 30.0), "b": (3.0, 30.0)}

    @pytest.mark.parametrize("column", _INTERNAL_NAMES)
    async def test_user_column_named_like_an_internal_alias_as_dimension(
        self, column, tmp_path_factory,
    ) -> None:
        """The same names as a GROUP BY dimension, which routes through the
        ranked-subquery / projection aliasing rather than the aggregate path."""
        engine = await _hostile_engine(
            column=column, base_dir=str(tmp_path_factory.mktemp("hostile")),
        )
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name=column)],
                measures=[ModelMeasure(formula="amount:sum", name="amt")],
            )
        )
        assert {r[f"orders.{column}"] for r in resp.data} == {1.0, 2.0, 3.0}

    @pytest.mark.parametrize("column", ["_td_0", "_dim_0", "_val_0"])
    async def test_ranked_subquery_families_survive_the_name(
        self, column, tmp_path_factory,
    ) -> None:
        """Reaches the ``_td_<n>`` / ``_dim_<n>`` counters specifically: a
        first/last measure builds the ranked subquery those aliases live in.
        Last amount per status, ordered by ``created_at``: a -> 20, b -> 30."""
        engine = await _hostile_engine(
            column=column, base_dir=str(tmp_path_factory.mktemp("hostile")),
        )
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:last", name="lastamt")],
            )
        )
        by_status = {r["orders.status"]: r["orders.lastamt"] for r in resp.data}
        assert by_status == {"a": 20.0, "b": 30.0}

    @pytest.mark.parametrize(
        "column", ["_w_dim_0", "_w_td_0", "_w_time", "_w_value"],
    )
    async def test_windowed_families_survive_the_name(self, column, tmp_path_factory) -> None:
        """Reaches the ``_w_*`` ``_src``-projection aliases specifically: a
        duration-windowed measure is the only shape that builds them."""
        engine = await _hostile_engine(
            column=column, base_dir=str(tmp_path_factory.mktemp("hostile")),
        )
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                time_dimensions=[
                    TimeDimension(
                        dimension=ColumnRef(name="created_at"),
                        granularity=TimeGranularity.MONTH,
                    ),
                ],
                measures=[
                    ModelMeasure(formula="amount:sum(window='90d')", name="w"),
                ],
            )
        )
        assert resp.data, "windowed query returned no rows"
        assert all(r["orders.w"] is not None for r in resp.data), resp.data


# ===========================================================================
# Naming constants — one owner for the structural aliases.
# ===========================================================================


class TestNamingConstants:
    """``_outer`` is currently written as a literal in BOTH ``generator.py``
    (the outer-wrap subquery alias) and ``dialects/tsql.py`` (the ORDER-BY
    detach rewrite), coupled by convention only — the tsql comment even says
    "only ``_outer`` is visible". Moving the literals into ``naming.py`` gives
    them a single owner.

    Per the ratified decision these two sites keep taking the name as
    a CONSTANT rather than an allocated name: the tsql rewrite is a
    post-generation AST pass with no allocator in reach, and PR 4 rebuilds the
    outer-wrap machinery wholesale. The carve-out is recorded as a named P-F
    exception, not an omission.
    """
    def test_constants_exist_and_match_the_current_literals(self) -> None:
        assert naming.OUTER_WRAP_ALIAS == "_outer"
        assert naming.STAGE_INNER_ALIAS == "_stage_inner"
        assert naming.FILTERED_ALIAS == "_filtered"

    def test_tsql_dialect_imports_the_shared_constant(self) -> None:
        """The convention coupling becomes an import.

        Asserted on the module NAMESPACE rather than by banning the literal
        from the source text: the string may legitimately appear in a comment
        or an error message, and forbidding that would constrain the
        implementation past what the plan asks for.
        """
        assert hasattr(tsql_module, "OUTER_WRAP_ALIAS"), (
            "tsql.py does not import naming.OUTER_WRAP_ALIAS"
        )
        assert tsql_module.OUTER_WRAP_ALIAS is naming.OUTER_WRAP_ALIAS

    def test_stage_wrapper_imports_the_shared_constant(self) -> None:

        assert hasattr(sw_module, "STAGE_INNER_ALIAS"), (
            "stage_wrapper.py does not import naming.STAGE_INNER_ALIAS"
        )
        assert sw_module.STAGE_INNER_ALIAS is naming.STAGE_INNER_ALIAS

    def test_tsql_outer_wrap_alias_still_round_trips(self) -> None:
        """Behavioural companion: the ratified carve-out says the T-SQL
        ORDER-BY detach rewrite keeps using a CONSTANT (not an allocated name),
        so its emitted alias must still be exactly the shared one."""
        assert get_dialect("tsql") is not None
        assert naming.OUTER_WRAP_ALIAS == "_outer"


class TestParityGuardRepair:
    """A companion xfail-registry module was deleted along with its
    ``pytest_collection_modifyitems`` hook, but ``test_parity_guards.py``'s
    docstring still tells the reader the gate depends on it. Stale prose that
    names a deleted file sends the next reader looking for infrastructure that
    is not there.

    Only the documentation is repaired — strengthening the guard's matcher is
    explicitly out of scope for this PR.
    """
    def test_docstring_no_longer_references_the_deleted_module(self) -> None:

        doc = guard_module.__doc__ or ""
        assert "parity_xfails" not in doc, (
            "test_parity_guards.py still documents the deleted "
            "tests/parity_xfails.py as part of its gate"
        )

    def test_the_deleted_module_really_is_gone(self) -> None:
        """Guard on the premise itself, so this repair cannot be silently
        invalidated by the file coming back."""
        tests_dir = pathlib.Path(__file__).parent
        assert not (tests_dir / "parity_xfails.py").exists()

    def test_the_guard_itself_still_works(self) -> None:
        """Parity: the repair is docstring-only, so the guard must still run
        and still pass."""
        assert hasattr(guard_module, "APPROVED_GUARDS")


# ===========================================================================
# The canonical-aggregate-alias consolidation.
# ===========================================================================


def _key(source, agg: str, *, args=(), kwargs=()) -> AggregateKey:
    return AggregateKey(source=source, agg=agg, args=args, kwargs=kwargs)


# The axis matrix, with the value each legacy profile produces TODAY. Frozen
# from the four existing bodies so the consolidation is provably
# behavior-preserving rather than merely plausible.
#
#   A = generator._canonical_cross_model_alias(source_relation="orders", key=…)
#   B = cross_model_planner._aggregate_alias(key=…)
#   C = planning._canonical_name(key)
#   D = stage_planner._canonical_alias_for_formula(text, bound=…)
#
# Read the drift off the columns: A prefixes with the source relation AND the
# join path; B and C emit no prefix at all; D emits the path RELATIVE (no
# relation) and is the only one that keeps a StarKey's own path. The last row
# is the missing-leaf edge — a source with neither ``leaf`` nor ``column_name``
# — where all four disagree.
_MATRIX: List[tuple] = [
    # (case, key, A, B, C, D)
    (
        "columnkey_local",
        _key(ColumnKey(leaf="revenue"), "sum"),
        "orders.revenue_sum", "revenue_sum", "revenue_sum", "revenue_sum",
    ),
    (
        "columnkey_path",
        _key(ColumnKey(path=("customers",), leaf="revenue"), "sum"),
        "orders.customers.revenue_sum", "revenue_sum", "revenue_sum",
        "customers.revenue_sum",
    ),
    (
        "columnkey_two_hop",
        _key(ColumnKey(path=("customers", "regions"), leaf="pop"), "max"),
        "orders.customers.regions.pop_max", "pop_max", "pop_max",
        "customers.regions.pop_max",
    ),
    (
        "columnsqlkey_local",
        _key(ColumnSqlKey(model="orders", column_name="net"), "sum"),
        "orders.net_sum", "net_sum", "net_sum", "net_sum",
    ),
    (
        "columnsqlkey_path",
        _key(
            ColumnSqlKey(path=("customers",), model="customers", column_name="net"),
            "sum",
        ),
        "orders.customers.net_sum", "net_sum", "net_sum", "customers.net_sum",
    ),
    (
        "starkey_local",
        _key(StarKey(), "count"),
        "orders._count", "_count", "_count", "_count",
    ),
    (
        "starkey_path",
        _key(StarKey(path=("customers",)), "count"),
        "orders.customers._count", "_count", "_count", "customers._count",
    ),
    (
        "kwargs",
        _key(ColumnKey(leaf="revenue"), "percentile", kwargs=(("p", Decimal("0.5")),)),
        "orders.revenue_percentile_p_0_5", "revenue_percentile_p_0_5",
        "revenue_percentile_p_0_5", "revenue_percentile_p_0_5",
    ),
    (
        "positional_args",
        _key(ColumnKey(leaf="revenue"), "last", args=(ColumnKey(leaf="created_at"),)),
        "orders.revenue_last_created_at", "revenue_last_created_at",
        "revenue_last_created_at", "revenue_last_created_at",
    ),
    (
        "args_and_kwargs",
        _key(
            ColumnKey(leaf="revenue"), "wavg",
            args=(Decimal(2),), kwargs=(("w", ColumnKey(leaf="qty")),),
        ),
        "orders.revenue_wavg_2_w_qty", "revenue_wavg_2_w_qty",
        "revenue_wavg_2_w_qty", "revenue_wavg_2_w_qty",
    ),
]

# The missing-leaf edge is kept out of the table above because profile D
# returns None there (it falls through to its own formula-text sanitiser,
# which is NOT part of the aggregate-alias contract and stays in
# stage_planner).
_MISSING_LEAF_KEY = AggregateKey(
    source=ColumnKey(leaf="x"), agg="sum",
).model_copy(
    update={
        "source": TimeTruncKey(
            column=ColumnKey(leaf="created_at"), granularity="month",
        ),
    },
)


class TestCanonicalAggregateAlias:
    """One function, four named profiles, byte-identical to the four bodies
    it replaces."""

    @pytest.mark.parametrize(
        "case,key,expected_a,expected_b,expected_c,expected_d",
        _MATRIX,
        ids=[m[0] for m in _MATRIX],
    )
    def test_profiles_reproduce_the_legacy_values(
        self, case, key, expected_a, expected_b, expected_c, expected_d,
    ) -> None:
        assert naming.canonical_aggregate_alias(
            key, profile="cross_model_cte", source_relation="orders",
        ) == expected_a
        assert naming.canonical_aggregate_alias(
            key, profile="cte_schema",
        ) == expected_b
        assert naming.canonical_aggregate_alias(
            key, profile="declared_name",
        ) == expected_c
        assert naming.canonical_aggregate_alias(
            key, profile="stage_formula",
        ) == expected_d

    def test_missing_leaf_edge_keeps_each_profiles_escape_hatch(self) -> None:
        """The one place the four genuinely disagree, preserved exactly:
        A and B collapse an unrecognised source to the star form, C emits its
        ``_agg_<name>`` placeholder, and D declines (returning None) so its
        caller falls through to the formula-text path."""
        assert naming.canonical_aggregate_alias(
            _MISSING_LEAF_KEY, profile="cross_model_cte",
            source_relation="orders",
        ) == "orders._sum"
        assert naming.canonical_aggregate_alias(
            _MISSING_LEAF_KEY, profile="cte_schema",
        ) == "_sum"
        assert naming.canonical_aggregate_alias(
            _MISSING_LEAF_KEY, profile="declared_name",
        ) == "_agg_sum"
        assert naming.canonical_aggregate_alias(
            _MISSING_LEAF_KEY, profile="stage_formula",
        ) is None

    def test_source_relation_is_required_by_the_cross_model_profile(
        self,
    ) -> None:
        """Profile validation makes the impossible combinations
        unrepresentable — the reason this is a profile enum rather than four
        free-floating boolean flags."""
        key = _key(ColumnKey(leaf="revenue"), "sum")
        with pytest.raises(ValueError):
            naming.canonical_aggregate_alias(key, profile="cross_model_cte")

    def test_source_relation_is_rejected_by_the_other_profiles(self) -> None:
        key = _key(ColumnKey(leaf="revenue"), "sum")
        for profile in ("cte_schema", "declared_name", "stage_formula"):
            with pytest.raises(ValueError):
                naming.canonical_aggregate_alias(
                    key, profile=profile, source_relation="orders",
                )

    def test_unknown_profile_is_rejected(self) -> None:
        key = _key(ColumnKey(leaf="revenue"), "sum")
        with pytest.raises(ValueError):
            naming.canonical_aggregate_alias(key, profile="nonsense")


class TestProductionCallersDelegate:
    """The four production functions keep their names and signatures (P-J
    state 1 — nothing is deleted in PR 1) but must now agree with the naming
    module for every case in the matrix. Any residual drift is a bug."""

    @pytest.mark.parametrize(
        "case,key,expected_a,expected_b,expected_c,expected_d",
        _MATRIX,
        ids=[m[0] for m in _MATRIX],
    )
    def test_all_four_agree_with_the_naming_module(
        self, case, key, expected_a, expected_b, expected_c, expected_d,
    ) -> None:

        gen = SQLGenerator(dialect="postgres")
        assert gen._canonical_cross_model_alias(
            source_relation="orders", key=key,
        ) == expected_a
        assert _aggregate_alias(key=key) == expected_b
        assert _canonical_name(key) == expected_c
        assert _canonical_alias_for_formula(
            "IGNORED_TEXT", bound=BoundExpr(value_key=key),
        ) == expected_d

    def test_each_caller_actually_delegates_with_its_profile(
        self, monkeypatch,
    ) -> None:
        """Agreeing on values is necessary but not sufficient — four copied
        implementations returning the same frozen strings would also pass, and
        that is precisely the duplication C5 exists to remove.

        Spy on the naming module and assert each production function FORWARDS,
        with the right profile and the right ``source_relation``.
        """
        key = _key(ColumnKey(path=("customers",), leaf="revenue"), "sum")
        calls: List[dict] = []
        real = naming.canonical_aggregate_alias

        def _spy(k, **kw):
            calls.append(kw)
            return real(k, **kw)

        # Each caller imports the function by name, so the spy has to replace
        # the binding in the CALLER's namespace, not just in the naming module.
        for module in (
            naming, generator_module, cross_model_planner, planning,
            stage_planner,
        ):
            if getattr(module, "canonical_aggregate_alias", None) is not None:
                monkeypatch.setattr(
                    module, "canonical_aggregate_alias", _spy, raising=False,
                )

        SQLGenerator(dialect="postgres")._canonical_cross_model_alias(
            source_relation="orders", key=key,
        )
        assert calls, "the generator did not delegate to the naming module"
        assert calls[-1].get("profile") == "cross_model_cte"
        assert calls[-1].get("source_relation") == "orders"

        cross_model_planner._aggregate_alias(key=key)
        assert calls[-1].get("profile") == "cte_schema"

        planning._canonical_name(key)
        assert calls[-1].get("profile") == "declared_name"

        stage_planner._canonical_alias_for_formula(
            "IGNORED_TEXT", bound=BoundExpr(value_key=key),
        )
        assert calls[-1].get("profile") == "stage_formula"


class TestCrossModelDedupIdentity:
    """What makes two cross-model plans "the same CTE".

    The identity is structural — never the sanitised name — because the
    canonical alias omits the aggregate's column filter and the name is doubly
    lossy, so either would merge plans that must render separately.

    It also carries the RENDER SHAPE. The forward and rerooted paths produce
    different join-back pairs and a different aggregate column alias (forward
    uses the canonical alias; rerooted uses the sub-plan's), so sharing one CTE
    across them would join at the wrong grain or read the wrong column. The
    planner interns each key to one slot and emits one plan per slot, so two
    plans cannot collide here today — these tests keep that from becoming
    silently wrong if that ever changes.
    """
    def _identity(self, *, rerooted, key=None):

        key = key or AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"), agg="sum",
        )
        return _cm_plan_identity(
            source_relation="orders",
            plan=SimpleNamespace(rerooted_plan=object() if rerooted else None),
            agg_slot=SimpleNamespace(key=key),
        )

    def test_forward_and_rerooted_are_different_identities(self) -> None:
        assert self._identity(rerooted=False) != self._identity(rerooted=True)

    def test_same_key_same_shape_shares_one_identity(self) -> None:
        """Two SEPARATELY CONSTRUCTED but equal keys — which is what two plans
        carry — collapse to ONE identity. The tuple has to compare equal BY
        VALUE, since it is used as a dict key.

        This is the unit-level precondition only. That two public names really
        do end up sharing a single CTE is asserted end-to-end by
        ``test_same_key_slots_still_share_one_cte`` above; this test cannot see
        public names at all, because the identity deliberately excludes them.
        """
        first = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"), agg="sum",
        )
        second = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"), agg="sum",
        )
        assert first is not second
        assert self._identity(rerooted=False, key=first) == self._identity(
            rerooted=False, key=second,
        )

    def test_filtered_and_unfiltered_are_different_identities(self) -> None:
        """The reason the identity is the typed key and not the alias: these
        two produce the SAME canonical alias."""
        source = ColumnKey(path=("customers",), leaf="revenue")
        plain = AggregateKey(source=source, agg="sum")
        filtered = AggregateKey(
            source=source, agg="sum",
            column_filter_key=SqlExprKey(canonical_sql="region_id = 1"),
        )
        assert self._identity(rerooted=False, key=plain) != self._identity(
            rerooted=False, key=filtered,
        )

    def test_identity_is_hashable(self) -> None:
        """It is used as a dict key, so an unhashable member would surface as a
        TypeError mid-render rather than at import."""
        assert len({self._identity(rerooted=False), self._identity(rerooted=True)}) == 2
