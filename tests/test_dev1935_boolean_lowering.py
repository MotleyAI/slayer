"""DEV-1935 — boolean-total semi-join pushdown: a root row survives iff the
conjunct holds on >=1 row of its join product (hops joined as declared, LEFT ->
null-extended). Executed dual-engine (SQLite + DuckDB); structural pins via
``plan_query``."""

from __future__ import annotations

import warnings as _warnings

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import (
    AssociatedGrainWarning,
    BroadcastGrainWarning,
    SlayerError,
)
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1935_fixtures import (
    ASSOC_OR,
    ASSOC_OR_ABSENT,
    ASSOC_OR_ABSENT_BY_STATUS,
    ASSOC_OR_BY_STATUS,
    ATOM_TWO_BRANCH,
    ATOM_TWO_BRANCH_SPEND,
    EVENTLESS_COALESCE_LT40,
    EVENTLESS_EVENT_ONLY,
    EVENTLESS_GOLD_OR_EVENT_INNER,
    EVENTLESS_GOLD_OR_EVENT_LEFT,
    event_less_models,
    GOLD_OR_OK,
    GOLD_OR_OK_SPEND,
    MATERIALISED_OR,
    REDUCED_PUSH_OR,
    REDUCED_PUSH_ORDER_CELLS,
    MATERIALISED_ORDER_CELLS,
    ModelMeasure,
    NEW_OR_EVENT,
    NEW_OR_EVENT_SPEND,
    NO_ORDERS,
    NO_ORDERS_SPEND,
    NOT_GOLD_AND_OK,
    NOT_GOLD_AND_OK_SPEND,
    OR_MIX_LOCAL,
    OR_MIX_PARTITIONED,
    OR_MIX_PRODUCER,
    OR_MIX_PRODUCER_BY_TIER,
    OR_MIX_RAW_ROWS,
    OR_MIX_SPEND,
    SlayerQuery,
    TWO_SPELLINGS,
    TWO_SPELLINGS_BY_TIER,
    cust_q,
    dev1900_models,
    disconnected_model_models,
    make_eventless_engine,
    make_exec_engine,
    orders_q,
    pushed_filter_infos,
    rows_by,
    two_spellings_models,
    unproven_plans_models,
)

MODES = ["broadcast", "associate", "error"]

SPEND = ModelMeasure(formula="spend:sum", name="sp")
SPEND_X = ModelMeasure(formula="customers.spend:sum", name="csp")
PARTITIONED = ModelMeasure(formula="spend:sum(partition_by=tier)", name="pt")

#: Python-warning carriers the boolean-total push must never emit.
_SLAYER_WARNS = (
    BroadcastGrainWarning, AssociatedGrainWarning)


@pytest.fixture(params=["sqlite", "duckdb"])
async def backend(request):
    async for e in make_exec_engine(request):
        yield request.param, e


@pytest.fixture(params=["sqlite", "duckdb"])
async def two_spellings_backend(request):
    async for e in make_exec_engine(request, models=two_spellings_models()):
        yield request.param, e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unproven_backend(request):
    async for e in make_exec_engine(request, models=unproven_plans_models()):
        yield request.param, e


@pytest.fixture(params=["sqlite", "duckdb"])
async def eventless_backend(request):
    async for e in make_eventless_engine(request):
        yield request.param, e


@pytest.fixture(params=["sqlite", "duckdb"])
async def eventless_inner_backend(request):
    async for e in make_eventless_engine(request, inner_events=True):
        yield request.param, e


@pytest.fixture(params=["sqlite", "duckdb"])
async def eventless_derived_backend(request):
    async for e in make_eventless_engine(request, derived_events=True):
        yield request.param, e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unreachable_backend(request):
    async for e in make_exec_engine(request, models=disconnected_model_models()):
        yield request.param, e


def _pop_infos(resp) -> list:
    """Population-push entries: a semi_join_pushed entry naming no aggregate."""
    return [i for i in pushed_filter_infos(resp) if i.measure is None]


async def _dry(engine, query, dialect: str) -> str:
    dry = await engine.execute(query, dry_run=True)
    assert dry.sql is not None, "dry_run returned no SQL"
    assert_scope_closed(dry.sql, dialect=dialect)
    return dry.sql


def _bundle(*, models, root: str) -> ResolvedSourceBundle:
    src = next(x for x in models if x.name == root)
    return ResolvedSourceBundle(
        source_model=src, referenced_models=[x for x in models if x.name != root])


def _base_hops(planned) -> list:
    """Every SemiJoinHop of the base query's pushed conjuncts."""
    return [h for sf in planned.semi_join_filters for h in sf.hops]


def _hop(planned, target_model: str):
    hs = [h for h in _base_hops(planned) if h.target_model == target_model]
    assert hs, f"no {target_model} hop in the base push: {_base_hops(planned)}"
    return hs[0]


# =========================================================================== #
# 1.1 Executed-value oracles.
# =========================================================================== #
class TestOrMixRestrictsPopulation:
    @pytest.mark.parametrize("mode", MODES)
    async def test_local_or_crosspath(self, backend, mode):
        """tier='bronze' or orders.status='ok' · spend:sum = 460 (never 560), any
        mode; one population entry, no dropped warning, no Python warning."""
        _, engine = backend
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(cust_q(
                measures=[SPEND], filters=[OR_MIX_LOCAL], to_many_handling=mode))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(OR_MIX_SPEND)
        (info,) = _pop_infos(resp)
        assert "status" in info.filter_text
        assert [w for w in caught if issubclass(w.category, _SLAYER_WARNS)] == []

    async def test_null_extended_row_kept(self, backend):
        """tier='gold' or orders.status='ok' = 475: the gold customer with no
        orders survives on the null-extended row (never 420, never 675)."""
        _, engine = backend
        resp = await engine.execute(cust_q(measures=[SPEND], filters=[GOLD_OR_OK]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(GOLD_OR_OK_SPEND)

    async def test_negation_keeps_existential_reading(self, backend):
        """not (tier='gold' and orders.status='ok') = 370: non-gold plus gold with
        a non-ok order; an orderless gold customer is not counted (never 520)."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[SPEND], filters=[NOT_GOLD_AND_OK]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(
            NOT_GOLD_AND_OK_SPEND)

    async def test_null_test_reads_as_absence(self, backend):
        """orders.id is null = 55: exactly the customers with no orders; the
        response carries the population entry."""
        _, engine = backend
        resp = await engine.execute(cust_q(measures=[SPEND], filters=[NO_ORDERS]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(NO_ORDERS_SPEND)
        assert _pop_infos(resp), "expected the population semi_join_pushed entry"


class TestMultiBranch:
    async def test_two_branches_under_or(self, backend):
        """orders.status='new' or regions.region_events.value>=50 = 320; one entry."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[SPEND], filters=[NEW_OR_EVENT]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(NEW_OR_EVENT_SPEND)
        assert _pop_infos(resp), "expected a population entry"

    async def test_atom_spanning_two_branches(self, backend):
        """orders.amount < regions.region_events.value = 420 (product), no warning."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[SPEND], filters=[ATOM_TWO_BRANCH]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(
            ATOM_TWO_BRANCH_SPEND)


class TestMaterialisedBranchBinding:
    async def test_materialised_branch_binds_to_grouped_row(self, backend):
        """dims=[orders.id], orders.status='ok' or regions.name='South': one cell
        per order that is itself ok or whose region is South, plus the null-order
        cell for the orderless South customer (kept via the region leg) — never a
        sibling order admitted because another of the customer's orders is ok. Main
        emits this inline, so it is a same-row-binding + null-extension anchor."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["orders.id"], filters=[MATERIALISED_OR]))
        cells = {r["customers.orders.id"] for r in resp.data}
        cells = {int(c) if c is not None else None for c in cells}
        assert cells == MATERIALISED_ORDER_CELLS


class TestReducedPushOnPartialMaterialisation:
    """D6: a conjunct whose grain materialises SOME fanning branches quantifies
    only the rest — the materialised refs bind to the outer query's own join."""

    async def test_materialised_branch_binds_and_the_rest_is_quantified(
            self, backend):
        """dims=[orders.id], orders.status='ok' or regions.region_events.value>=50:
        one cell per order that is itself ok or whose region has an event >= 50
        (never a sibling order admitted through another order of the customer)."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["orders.id"], filters=[REDUCED_PUSH_OR]))
        cells = {r["customers.orders.id"] for r in resp.data}
        assert {int(c) if c is not None else None for c in cells} == \
            REDUCED_PUSH_ORDER_CELLS
        assert _pop_infos(resp), "expected a population entry"

    async def test_orders_joined_once_outside_the_exists(self, backend):
        """The statement joins orders exactly once, in the outer query; the EXISTS
        holds only the unmaterialised branch."""
        dialect, engine = backend
        sql = await _dry(engine, cust_q(
            dimensions=["orders.id"], filters=[REDUCED_PUSH_OR]), dialect)
        tree = sqlglot.parse_one(sql, dialect=dialect)
        (exists_node,) = list(tree.find_all(exp.Exists))
        inside = {t.name for t in exists_node.find_all(exp.Table)}
        assert "region_events" in inside, sql
        assert "orders" not in inside, sql
        assert len([t for t in tree.find_all(exp.Table) if t.name == "orders"]) == 1

    def test_reduced_push_drops_the_materialised_hop(self):
        planned = plan_query(
            query=cust_q(dimensions=["orders.id"], filters=[REDUCED_PUSH_OR]),
            bundle=_bundle(models=dev1900_models(), root="customers"))
        assert {h.target_model for h in _base_hops(planned)} == {
            "regions", "region_events"}


class TestPartitionedOverOrMix:
    @pytest.mark.parametrize("mode", MODES)
    async def test_partitioned_by_tier(self, backend, mode):
        """sum(spend, partition_by=tier) by tier over the OR-mix population =
        gold 190 / silver 230 / bronze 40, any mode; producer + population entries,
        no dropped warning, error mode does not error."""
        _, engine = backend
        q = cust_q(
            dimensions=["tier"], measures=[PARTITIONED], filters=[OR_MIX_LOCAL],
            to_many_handling=mode)
        resp = await engine.execute(q)
        by = rows_by(resp, "customers.tier")
        assert set(by) == {(t,) for t in OR_MIX_PARTITIONED}
        for tier, spend in OR_MIX_PARTITIONED.items():
            assert float(by[(tier,)]["customers.pt"]) == pytest.approx(spend), tier
        measures = {i.measure for i in pushed_filter_infos(resp)}
        assert None in measures, measures
        assert "pt" in measures, measures


class TestRawRowMode:
    async def test_or_mix_raw_rows_not_multiplied(self, backend):
        """distinct_dimension_values=false, OR-mix: one row per population customer
        (6), never one per matching order (7)."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"], filters=[OR_MIX_LOCAL],
            distinct_dimension_values=False))
        assert len(resp.data) == OR_MIX_RAW_ROWS


class TestProducerSideMixedOr:
    @pytest.mark.parametrize("mode", MODES)
    async def test_mixed_or_flips_to_pushed(self, backend, mode):
        """orders-rooted customers.spend:sum by customers.tier with
        customers.tier='bronze' OR channel='app' = gold 160 / silver 230 /
        bronze 40 (never gold 245); the informational entry names the aggregate
        and the filter, no dropped warning, error mode does not error."""
        _, engine = backend
        q = orders_q(
            dimensions=["customers.tier"], measures=[SPEND_X],
            filters=[OR_MIX_PRODUCER], to_many_handling=mode)
        resp = await engine.execute(q)
        by = rows_by(resp, "orders.customers.tier")
        for tier, spend in OR_MIX_PRODUCER_BY_TIER.items():
            assert float(by[(tier,)]["orders.csp"]) == pytest.approx(spend), tier
        named = {(i.measure, "tier" in (i.filter_text or ""))
                 for i in pushed_filter_infos(resp)}
        assert ("csp", True) in named, named


class TestAssociationArmMixedOr:
    async def test_mixed_or_on_association(self, backend):
        """associate customers.spend:sum by status, customers.tier='gold' OR
        channel='app': each status cell counts distinct customers with an order of
        that status that is app or belongs to a gold customer (ok 270, new 250)."""
        _, engine = backend
        resp = await engine.execute(orders_q(
            dimensions=["status"], measures=[SPEND_X], filters=[ASSOC_OR],
            to_many_handling="associate"))
        by = rows_by(resp, "orders.status")
        for status, spend in ASSOC_OR_BY_STATUS.items():
            assert float(by[(status,)]["orders.csp"]) == pytest.approx(spend), status
        assert pushed_filter_infos(resp), "expected the informational entry"

    async def test_mixed_or_branch_absent_from_query(self, unproven_backend):
        """associate customers.spend:sum by status, customers.tier='gold' OR
        customers.plans.level='basic' (plans hop unproven): gold or basic-plan
        customers with an order of that status (ok 270, new 100)."""
        _, engine = unproven_backend
        resp = await engine.execute(orders_q(
            dimensions=["status"], measures=[SPEND_X], filters=[ASSOC_OR_ABSENT],
            to_many_handling="associate"))
        by = rows_by(resp, "orders.status")
        for status, spend in ASSOC_OR_ABSENT_BY_STATUS.items():
            assert float(by[(status,)]["orders.csp"]) == pytest.approx(spend), status


class TestTwoSpellingsOneNode:
    async def test_two_spellings_bind_to_one_related_row(self, two_spellings_backend):
        """purchases.status='ok' and orders.channel='app' (one named edge, two
        spellings) bind to one order: gold 60 / silver 80 (never 160 / 230)."""
        _, engine = two_spellings_backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"], measures=[SPEND],
            filters=list(TWO_SPELLINGS)))
        by = rows_by(resp, "customers.tier")
        for tier, spend in TWO_SPELLINGS_BY_TIER.items():
            assert float(by[(tier,)]["customers.sp"]) == pytest.approx(spend), tier

    async def test_single_orders_relation_correlated(self, two_spellings_backend):
        """The generated SQL correlates a single orders relation for both conjuncts:
        exactly one EXISTS, holding exactly one `orders` table node (AST, so a
        `FROM orders ... JOIN orders` duplicate cannot slip past a text count)."""
        dialect, engine = two_spellings_backend
        sql = await _dry(engine, cust_q(
            dimensions=["tier"], measures=[SPEND], filters=list(TWO_SPELLINGS)),
            dialect)
        (exists_node,) = list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Exists))
        orders = [t for t in exists_node.find_all(exp.Table) if t.name == "orders"]
        assert len(orders) == 1, sql


class TestEventLessNullExtension:
    async def test_left_hop_null_extends_via_saving_leg(self, eventless_backend):
        """tier='gold' or region_events.value>=50 on a LEFT region_events hop:
        the event-less region is null-extended and kept via the gold leg = 190."""
        _, engine = eventless_backend
        resp = await engine.execute(SlayerQuery(
            source_model="customers", measures=[SPEND],
            filters=["tier = 'gold' or regions.region_events.value >= 50"]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(
            EVENTLESS_GOLD_OR_EVENT_LEFT)

    async def test_rejecting_predicate_excludes_event_less(self, eventless_backend):
        """region_events.value>=50 alone rejects the null extension (INNER): the
        event-less region is excluded = 100."""
        _, engine = eventless_backend
        resp = await engine.execute(SlayerQuery(
            source_model="customers", measures=[SPEND],
            filters=["regions.region_events.value >= 50"]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(
            EVENTLESS_EVENT_ONLY)

    async def test_declared_inner_descendant_drops_absent_row(
            self, eventless_inner_backend):
        """A DECLARED-INNER region_events hop stays INNER even under the saving
        leg: the event-less region has no product row and is dropped = 100."""
        _, engine = eventless_inner_backend
        resp = await engine.execute(SlayerQuery(
            source_model="customers", measures=[SPEND],
            filters=["tier = 'gold' or regions.region_events.value >= 50"]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(
            EVENTLESS_GOLD_OR_EVENT_INNER)

    async def test_non_propagating_fragment_null_extends(
            self, eventless_derived_backend):
        """region_events.value_or_zero < 40 (a COALESCE definition) cannot reject
        the null extension: the event-less region's row is 0 < 40 and kept = 170,
        never 80 (an INNER correlation would drop it)."""
        _, engine = eventless_derived_backend
        resp = await engine.execute(SlayerQuery(
            source_model="customers", measures=[SPEND],
            filters=["regions.region_events.value_or_zero < 40"]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(
            EVENTLESS_COALESCE_LT40)


class TestGenuinelyUnreachableFilterRefused:
    """DEV-1935 narrows exclusion to a genuinely unreachable reference (no
    resolvable join path from the root). In the connected reference graph such a
    reference is refused in every mode (`UnresolvableDimensionJoinError`), never
    silently routed as if it crossed nothing — the one retained exclusion. See
    tasks.md §1.5: whether a filter can be dropped-and-warned (rather than hard
    refused) requires a host-reachable/producer-unreachable topology this graph
    lacks; spec-implement reconciles that with the spec scenario's wording."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_unreachable_ref_is_refused(self, unreachable_backend, mode):
        _, engine = unreachable_backend
        q = cust_q(dimensions=["tier"], measures=[PARTITIONED],
                   filters=["promos.discount > 0"], to_many_handling=mode)
        with pytest.raises((SlayerError, ValueError)):
            await engine.execute(q)


# =========================================================================== #
# 1.2 Structural pins.
# =========================================================================== #
class TestGroupingAndDeterminism:
    async def test_generation_is_deterministic(self, backend):
        """Union-find grouping keeps first-appearance order: generating an OR-mix
        query twice yields byte-identical SQL."""
        dialect, engine = backend
        q = cust_q(measures=[SPEND], filters=[OR_MIX_LOCAL])
        first = await _dry(engine, q, dialect)
        second = await _dry(engine, q, dialect)
        assert first == second

    async def test_two_disjoint_branches_yield_two_exists(self, backend):
        """Two disjoint single-branch conjuncts stay two EXISTS (grouping never
        merges them)."""
        dialect, engine = backend
        sql = await _dry(engine, cust_q(
            measures=[SPEND],
            filters=["orders.status = 'ok'", "regions.region_events.value >= 50"]),
            dialect)
        assert sql.upper().count("EXISTS") == 2, sql


class TestExistsIsNeverUnderOr:
    async def test_exists_is_an_and_conjunct(self, backend):
        """The whole conjunct group sits inside ONE EXISTS conjunct of the outer
        WHERE; the disjunction lives INSIDE the EXISTS, and no EXISTS is an operand
        of an OR (Snowflake forbids a correlated EXISTS under OR). Checked on the
        AST, so `EXISTS(...) OR local` cannot slip past a text match."""
        dialect, engine = backend
        sql = await _dry(engine, cust_q(measures=[SPEND], filters=[OR_MIX_LOCAL]),
                         dialect)
        tree = sqlglot.parse_one(sql, dialect=dialect)
        exists_nodes = list(tree.find_all(exp.Exists))
        assert exists_nodes, sql
        for node in exists_nodes:
            anc = node.parent
            while anc is not None and not isinstance(anc, (exp.Where, exp.Select)):
                assert not isinstance(anc, exp.Or), f"EXISTS is an operand of OR:\n{sql}"
                anc = anc.parent


class TestNullExtendedFlag:
    def test_existing_pushes_are_all_inner(self):
        """Every hop of every existing (non-null-rejecting) fixture push keeps
        null_extended False — the byte-identity guarantee for existing shapes."""
        for flt in ["orders.status = 'ok'", "customers.regions.bad_pop > 0"]:
            root = "customers" if flt.startswith("orders") else "orders"
            planned = plan_query(
                query=SlayerQuery(
                    source_model=root,
                    measures=[SPEND if root == "customers" else
                              ModelMeasure(formula="amount:sum", name="amt")],
                    filters=[flt]),
                bundle=_bundle(models=dev1900_models(), root=root))
            hops = _base_hops(planned)
            assert hops, flt
            assert all(h.null_extended is False for h in hops), (flt, hops)

    def test_saving_leg_marks_the_hop_null_extended(self):
        """A predicate whose disjunction can hold on the hop's null-extended row
        marks that hop null_extended (spine shape)."""
        planned = plan_query(
            query=SlayerQuery(source_model="customers", measures=[SPEND],
                              filters=[GOLD_OR_OK]),
            bundle=_bundle(models=dev1900_models(), root="customers"))
        assert _hop(planned, "orders").null_extended is True

    def test_declared_inner_descendant_stays_inner(self):
        """A declared-INNER hop is never null_extended, even under a saving leg."""
        planned = plan_query(
            query=SlayerQuery(
                source_model="customers", measures=[SPEND],
                filters=["tier = 'gold' or regions.region_events.value >= 50"]),
            bundle=_bundle(models=event_less_models(inner_events=True), root="customers"))
        assert _hop(planned, "region_events").null_extended is False


class TestPopulationDroppedArmImpossible:
    async def test_or_mix_population_pushes_never_drops(self, backend):
        """A bound population conjunct always resolves from the host: the OR-mix
        population push produces a base semi-join and never a dropped-filter
        disposition (the two-way disposition's dropped arm is impossible)."""
        dialect, engine = backend
        q = cust_q(measures=[SPEND], filters=[OR_MIX_LOCAL])
        planned = plan_query(query=q, bundle=_bundle(models=dev1900_models(), root="customers"))
        assert planned.semi_join_filters, "the OR-mix population must push"
        await engine.execute(q)
        # the host base carries the EXISTS and never joins orders into the OUTER
        # query — the semi-join (its LEFT/INNER join) lives inside the EXISTS.
        sql = await _dry(engine, q, dialect)
        outer_joins = {
            j.this.alias_or_name
            for j in (sqlglot.parse_one(sql, dialect=dialect).args.get("joins") or [])
            if isinstance(j.this, exp.Table)
        }
        assert "orders" not in outer_joins, sql


# =========================================================================== #
# 1.3 Null-rejection analysis (via the observable null_extended flag).
# =========================================================================== #
#: (filter predicate on the LEFT region_events hop, expected null_extended).
_NULL_REJECTION_CASES = [
    ("regions.region_events.value >= 50", False),          # comparison -> UNKNOWN
    ("regions.region_events.value is null", True),         # is null -> TRUE
    ("regions.region_events.value is not null", False),    # is not null -> FALSE
    ("regions.region_events.value in (50, 60)", False),    # IN -> UNKNOWN
    # (BETWEEN is not expressible in the filter DSL — BetweenKey is date-range-only;
    #  the IN case above covers the same InKey/BetweenKey -> UNKNOWN rule.)
    ("regions.region_events.value >= 50 and regions.region_events.value < 100",
     False),                                               # AND of UNKNOWN -> UNKNOWN
    ("not (regions.region_events.value >= 50)", False),    # NOT UNKNOWN -> UNKNOWN
    ("abs(regions.region_events.value) >= 50", True),      # scalar call -> DEPENDS
    ("tier = 'gold' or regions.region_events.value >= 50", True),  # OR w/ DEPENDS
]

#: (predicate over a derived event column or an IS operand, expected null_extended):
#: the D5 Mode-A fragment rule (on the EXPANDED definition) and the IS rules.
_FRAGMENT_AND_IS_CASES = [
    ("regions.region_events.value_bare >= 50", False),     # bare column -> null-valued
    ("regions.region_events.value_x2 >= 50", False),       # arithmetic -> null-valued
    ("regions.region_events.value_x2_x2 >= 50", False),    # derived-on-derived arithmetic
    ("regions.region_events.value_or_zero >= 50", True),   # COALESCE -> DEPENDS
    ("regions.region_events.value_or_zero_x2 >= 50", True),  # arithmetic over COALESCE
    ("regions.region_events.one >= 50", True),             # literal-only -> DEPENDS
    ("regions.region_events.in_empty is not null", True),  # NULL IN () is FALSE -> DEPENDS
    ("regions.region_events.col_in_list is not null", False),  # NULL IN (..) -> null-valued
    ("regions.region_events.lit_in_list is not null", True),   # 1 IN (NULL, 1) is TRUE
    ("regions.region_events.lit_between is not null", True),   # 1 BETWEEN NULL AND 0 is FALSE
    ("regions.region_events.value is tier", True),         # IS <non-literal> -> DEPENDS
    ("regions.region_events.value is not tier", True),
    ("regions.region_events.value is True", False),        # NULL IS TRUE -> FALSE
    ("regions.region_events.value is not True", True),     # NULL IS NOT TRUE -> TRUE
    # a predicate as a value operand is NULL iff UNKNOWN: (UNKNOWN or DEPENDS) is DEPENDS
    ("(regions.region_events.value >= 50 or tier = 'gold') is not null", True),
    ("(regions.region_events.value >= 50) is not null", False),  # UNKNOWN -> NULL
]


class TestNullRejectionAnalysis:
    @pytest.mark.parametrize("predicate,expected", _FRAGMENT_AND_IS_CASES)
    def test_fragment_and_is_rules(self, predicate, expected):
        """A derived ref is null-valued only when its expanded definition
        propagates NULL; IS / IS NOT with a non-literal operand is DEPENDS."""
        planned = plan_query(
            query=SlayerQuery(source_model="customers", measures=[SPEND],
                              filters=[predicate]),
            bundle=_bundle(models=event_less_models(derived_events=True), root="customers"))
        assert _hop(planned, "region_events").null_extended is expected

    def test_filter_dependency_never_nulls_the_masked_value(self):
        """``CASE WHEN f THEN v END`` is NULL only when ``v`` is: the hop that only
        the filter crosses stays null-extended (its OR leg holds without it), the
        value's own hop does not."""
        planned = plan_query(
            query=SlayerQuery(source_model="customers", measures=[SPEND],
                              filters=["regions.region_events.value_if_kind >= 50"]),
            bundle=_bundle(models=event_less_models(derived_events=True), root="customers"))
        assert _hop(planned, "event_kinds").null_extended is True
        assert _hop(planned, "region_events").null_extended is False

    @pytest.mark.parametrize("predicate,expected", _NULL_REJECTION_CASES)
    def test_region_events_hop_null_extension(self, predicate, expected):
        """Each null-rejection rule sets null_extended on the fanning region_events
        hop: a rejecting predicate (FALSE/UNKNOWN) stays INNER; a non-rejecting one
        (TRUE/DEPENDS) null-extends."""
        planned = plan_query(
            query=SlayerQuery(source_model="customers", measures=[SPEND],
                              filters=[predicate]),
            bundle=_bundle(models=event_less_models(), root="customers"))
        assert _hop(planned, "region_events").null_extended is expected
