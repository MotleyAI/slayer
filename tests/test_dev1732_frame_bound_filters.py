"""DEV-1732 — frame-bound filters must not truncate trailing-window / shifted CTEs.

Stage 10 (DEV-1714) strips the typed ``date_range`` from a windowed measure's
``_src`` subquery so the trailing window can reach rows *before* the visible
frame; without that, the earliest buckets under-count. But it strips ONLY the
typed ``date_range``. An explicit filter expressing the same intent
(``filters=["created_at >= '2025-01-01'"]``) is not a ``date_range``, so it was
applied inside ``_src`` and silently truncated the window — two spellings of one
intent giving different numbers. The identical asymmetry existed in the
``time_shift`` shifted CTE (``_shifted_where_part`` omitted ``BetweenKey`` and
propagated every other ROW filter).

The rule this module pins:

    A ROW-phase filter conjunct that is a relational bound, with a temporal
    literal, on the raw column of one of the query's time dimensions is a FRAME
    bound, not a population filter. Frame bounds constrain the visible buckets
    only; they are removed from any CTE that must reach outside the frame
    (``_wm_``'s ``_src``, ``time_shift``'s shifted CTE). Every other predicate is
    a population filter and is applied to those CTEs unchanged.

Layout:

* ``TestFrameBoundRecognition`` / ``TestFrameBoundComposition`` /
  ``TestTemporalLiteralWhitelist`` — pure unit tests against
  ``slayer.core.time_bounds``, built from hand-rolled ValueKey trees.
* ``TestWindowedSrcFrameBounds`` — ``_wm_`` ``_src`` SQL shape via the typed
  pipeline.
* ``TestTimeShiftFrameBounds`` — the shifted CTE, same rule.
"""

import tempfile
from decimal import Decimal

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    LiteralKey,
    ScalarCallKey,
)
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, ModelMeasure, SlayerQuery, TimeDimension
from slayer.core.time_bounds import is_frame_bound, is_temporal_literal, strip_frame_bounds
from slayer.engine.source_bundle import build_resolved_source_bundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.scope_check import assert_scope_closed
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import (
    _engine_generate,
    _extract_cte_body,
    _extract_src_body,
    _join_aliases,
    _norm,
)

# --------------------------------------------------------------------------- #
# Key-tree fixtures for the pure-helper tests
# --------------------------------------------------------------------------- #

CREATED_AT = ColumnKey(path=(), leaf="created_at")
SHIP_DATE = ColumnKey(path=(), leaf="ship_date")
DELIVERY_AT = ColumnKey(path=(), leaf="delivery_at")
STATUS = ColumnKey(path=(), leaf="status")

# The two query time dimensions' raw columns — the strippable set (§2.2).
TIME_COLS = frozenset({CREATED_AT, SHIP_DATE})


def _cmp(op: str, lhs, rhs) -> ArithmeticKey:
    return ArithmeticKey(op=op, operands=(lhs, rhs))


def _and(*operands) -> ArithmeticKey:
    return ArithmeticKey(op="and", operands=tuple(operands))


def _lit(v) -> LiteralKey:
    return LiteralKey(value=v)


STATUS_PAID = _cmp("==", STATUS, _lit("paid"))


def _strip(key):
    return strip_frame_bounds(key=key, time_columns=TIME_COLS)


# --------------------------------------------------------------------------- #
# §2.1 — recognition
# --------------------------------------------------------------------------- #
class TestFrameBoundRecognition:
    @pytest.mark.parametrize("op", [">=", ">", "<=", "<"])
    async def test_relational_bound_on_td_column_is_stripped(self, op: str) -> None:
        """Every relational operator on a query TD's raw column is a frame
        bound — BOTH directions. `date_range` strips a single ``BetweenKey``
        node, i.e. both bounds, so equivalence requires stripping both."""
        assert _strip(_cmp(op, CREATED_AT, _lit("2024-06-01"))) is None

    async def test_literal_on_the_left_is_recognised(self) -> None:
        """``'2024-06-01' <= created_at`` means the same as
        ``created_at >= '2024-06-01'`` and must be treated identically."""
        assert _strip(_cmp("<=", _lit("2024-06-01"), CREATED_AT)) is None

    async def test_between_key_on_td_column_is_stripped(self) -> None:
        """A ``BetweenKey`` (the ``date_range`` carrier) is subsumed by the same
        helper, so ``time_shift``'s old ``isinstance(..., BetweenKey)`` special
        case collapses into one rule."""
        key = BetweenKey(
            column=CREATED_AT, low=_lit("2024-06-01"), high=_lit("2024-12-31"),
        )
        assert _strip(key) is None

    async def test_between_key_on_non_td_column_is_kept(self) -> None:
        key = BetweenKey(
            column=DELIVERY_AT, low=_lit("2024-06-01"), high=_lit("2024-12-31"),
        )
        assert _strip(key) is key

    async def test_bound_on_second_time_dimension_is_stripped(self) -> None:
        """§2.2: the strippable set is EVERY non-hidden query TD's raw column,
        not just the window axis — that is exactly the set for which a
        ``date_range`` spelling exists."""
        assert _strip(_cmp(">=", SHIP_DATE, _lit("2024-06-01"))) is None

    async def test_bound_on_derived_time_column_is_stripped_by_identity(self) -> None:
        """A derived (``Column.sql``) temporal column binds to a
        ``ColumnSqlKey``; ``_build_date_range_filter`` binds its
        ``BetweenKey.column`` through the same path that produces
        ``TimeTruncKey.column``, so identity matching covers it for free."""
        derived = ColumnSqlKey(path=(), model="orders", column_name="event_at")
        key = _cmp(">=", derived, _lit("2024-06-01"))
        assert strip_frame_bounds(key=key, time_columns=frozenset({derived})) is None

    @pytest.mark.parametrize("op", ["==", "!=", "is", "is not", "in", "not in"])
    async def test_non_relational_ops_are_kept(self, op: str) -> None:
        """Equality/membership on a raw timestamp means "this instant" or "this
        set", never a range. Stripping ``created_at == 'X'`` would sum 90 days
        where one instant was asked for."""
        key = _cmp(op, CREATED_AT, _lit("2024-06-01"))
        assert _strip(key) is key

    async def test_column_rhs_is_kept(self) -> None:
        """``created_at >= ship_date`` is a row-wise correlation between two
        columns, not a frame bound — even though both are query TDs."""
        key = _cmp(">=", CREATED_AT, SHIP_DATE)
        assert _strip(key) is key

    async def test_scalar_call_lhs_is_kept(self) -> None:
        """``date(created_at) >= X`` is a different ValueKey from the raw
        column, so it is not a frame bound."""
        key = _cmp(">=", ScalarCallKey(name="trunc", args=(CREATED_AT,)), _lit("2024-06-01"))
        assert _strip(key) is key

    async def test_bound_on_time_column_that_is_not_a_query_td_is_kept(self) -> None:
        """``delivery_at >= X`` has no ``date_range`` spelling, so it is a
        genuine population filter. Dropping it would make the windowed measure
        over-count against every other measure in the same query."""
        key = _cmp(">=", DELIVERY_AT, _lit("2024-06-01"))
        assert _strip(key) is key

    async def test_is_frame_bound_agrees_with_strip(self) -> None:
        """The public predicate and the rewriter must not drift."""
        bound = _cmp(">=", CREATED_AT, _lit("2024-06-01"))
        other = _cmp(">=", DELIVERY_AT, _lit("2024-06-01"))
        assert is_frame_bound(key=bound, time_columns=TIME_COLS) is True
        assert is_frame_bound(key=other, time_columns=TIME_COLS) is False

    @pytest.mark.parametrize("arity", [1, 3])
    async def test_malformed_comparison_arity_is_kept(self, arity: int) -> None:
        """Rule 1 requires exactly two operands. A relational key with any other
        arity is not a comparison we can reason about — keep it untouched rather
        than index into it."""
        operands = tuple(
            [CREATED_AT] + [_lit("2024-06-01")] * (arity - 1),
        )
        key = ArithmeticKey(op=">=", operands=operands)
        assert is_frame_bound(key=key, time_columns=TIME_COLS) is False
        assert _strip(key) is key

    async def test_empty_time_column_set_strips_nothing(self) -> None:
        """A query with no time dimensions has no frame to bound."""
        key = _cmp(">=", CREATED_AT, _lit("2024-06-01"))
        assert strip_frame_bounds(key=key, time_columns=frozenset()) is key


# --------------------------------------------------------------------------- #
# §2.1.1 — the temporal-literal whitelist (Codex F1 / F6)
# --------------------------------------------------------------------------- #
class TestTemporalLiteralWhitelist:
    """Only a bare ``LiteralKey`` holding a non-``None`` ``str`` qualifies.

    An earlier draft used a blacklist ("no column/aggregate key anywhere in the
    subtree"), which classifies dynamic expressions as frame bounds.
    """

    async def test_none_literal_is_not_temporal(self) -> None:
        """``col < NULL`` matches nothing today; stripping it would turn an
        empty result into the full population."""
        assert is_temporal_literal(_lit(None)) is False
        key = _cmp("<", CREATED_AT, _lit(None))
        assert _strip(key) is key

    async def test_numeric_literal_is_not_temporal(self) -> None:
        assert is_temporal_literal(_lit(Decimal(5))) is False
        key = _cmp(">=", CREATED_AT, _lit(Decimal(5)))
        assert _strip(key) is key

    async def test_bool_literal_is_not_temporal(self) -> None:
        assert is_temporal_literal(_lit(True)) is False
        key = _cmp(">=", CREATED_AT, _lit(True))
        assert _strip(key) is key

    async def test_scalar_call_rhs_is_not_temporal(self) -> None:
        """Codex's motivating case. ``now`` is not in ``SCALAR_PASSTHROUGH`` so
        a Mode-B filter cannot spell it today; the whitelist is defence in depth
        against that allowlist growing."""
        call = ScalarCallKey(name="coalesce", args=(_lit("2024-06-01"),))
        assert is_temporal_literal(call) is False
        key = _cmp(">=", CREATED_AT, call)
        assert _strip(key) is key

    async def test_arithmetic_over_literals_is_not_temporal(self) -> None:
        """Only a *bare* ``LiteralKey`` qualifies — not a tree that happens to
        contain no column reference."""
        tree = ArithmeticKey(op="+", operands=(_lit(Decimal(1)), _lit(Decimal(2))))
        assert is_temporal_literal(tree) is False
        key = _cmp(">=", CREATED_AT, tree)
        assert _strip(key) is key

    async def test_between_with_non_str_endpoint_is_kept(self) -> None:
        key = BetweenKey(column=CREATED_AT, low=_lit("2024-06-01"), high=_lit(None))
        assert _strip(key) is key

    async def test_string_literal_is_temporal(self) -> None:
        assert is_temporal_literal(_lit("2024-06-01")) is True


# --------------------------------------------------------------------------- #
# §2.3 — composition
# --------------------------------------------------------------------------- #
class TestFrameBoundComposition:
    async def test_mixed_conjunction_splits(self) -> None:
        """The equivalent spelling is ``date_range`` PLUS
        ``filters=["status='paid'"]``, which leaves ``_src`` filtered by status
        and unbounded in time. Splitting reproduces that exactly."""
        key = _and(_cmp(">=", CREATED_AT, _lit("2024-06-01")), STATUS_PAID)
        assert _strip(key) == STATUS_PAID

    async def test_nary_conjunction_drops_every_bound(self) -> None:
        key = _and(
            _cmp(">=", CREATED_AT, _lit("2024-06-01")),
            _cmp("<=", CREATED_AT, _lit("2024-12-31")),
            STATUS_PAID,
        )
        assert _strip(key) == STATUS_PAID

    async def test_all_bounds_conjunction_strips_entirely(self) -> None:
        key = _and(
            _cmp(">=", CREATED_AT, _lit("2024-06-01")),
            _cmp("<=", CREATED_AT, _lit("2024-12-31")),
        )
        assert _strip(key) is None

    async def test_two_survivors_rebuild_an_and_preserving_order(self) -> None:
        region = _cmp("==", ColumnKey(path=("customers",), leaf="tier"), _lit("gold"))
        key = _and(STATUS_PAID, _cmp(">=", CREATED_AT, _lit("2024-06-01")), region)
        assert _strip(key) == _and(STATUS_PAID, region)

    async def test_nested_and_is_recursed_into(self) -> None:
        inner = _and(_cmp(">=", CREATED_AT, _lit("2024-06-01")), STATUS_PAID)
        key = _and(inner, _cmp("<=", SHIP_DATE, _lit("2024-12-31")))
        assert _strip(key) == STATUS_PAID

    async def test_or_is_kept_whole(self) -> None:
        """No sound split exists under a disjunction — keeping the predicate
        preserves today's numbers."""
        key = ArithmeticKey(
            op="or", operands=(_cmp(">=", CREATED_AT, _lit("2024-06-01")), STATUS_PAID),
        )
        assert _strip(key) is key

    async def test_not_is_kept_whole(self) -> None:
        key = ArithmeticKey(
            op="not", operands=(_cmp(">=", CREATED_AT, _lit("2024-06-01")),),
        )
        assert _strip(key) is key

    async def test_unchanged_key_returns_the_same_object(self) -> None:
        """Identity return lets the planner skip building a rewrite entry."""
        key = _and(STATUS_PAID, _cmp("==", DELIVERY_AT, _lit("2024-06-01")))
        assert _strip(key) is key

    async def test_strip_is_idempotent(self) -> None:
        key = _and(_cmp(">=", CREATED_AT, _lit("2024-06-01")), STATUS_PAID)
        once = _strip(key)
        assert _strip(once) == once

    async def test_rebuilt_key_equals_a_hand_built_one(self) -> None:
        """``ArithmeticKey`` carries only ``op``/``operands`` today, so a rebuild
        is lossless. This guards a future field being added without the
        rewriter learning to carry it."""
        region = _cmp("==", ColumnKey(path=("customers",), leaf="tier"), _lit("gold"))
        key = _and(STATUS_PAID, _cmp(">=", CREATED_AT, _lit("2024-06-01")), region)
        rebuilt = _strip(key)
        hand = ArithmeticKey(op="and", operands=(STATUS_PAID, region))
        assert rebuilt == hand
        assert hash(rebuilt) == hash(hand)


# --------------------------------------------------------------------------- #
# Models for the SQL-shape tests
# --------------------------------------------------------------------------- #
def _orders(**kw) -> SlayerModel:
    base = dict(
        name="orders",
        sql_table="orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="ship_date", sql="ship_date", type=DataType.TIMESTAMP),
            Column(name="delivery_at", sql="delivery_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
    )
    base.update(kw)
    return SlayerModel(**base)


def _customers(**kw) -> SlayerModel:
    base = dict(
        name="customers",
        sql_table="customers",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="tier", sql="tier", type=DataType.TEXT),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP),
        ],
    )
    base.update(kw)
    return SlayerModel(**base)


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    )


def _joined_orders() -> SlayerModel:
    return _orders(
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


_MONTHLY_CREATED_AT = TimeDimension(
    dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
)


def _windowed_query(**kw) -> SlayerQuery:
    base = dict(
        source_model="orders",
        time_dimensions=[_MONTHLY_CREATED_AT],
        measures=[ModelMeasure(formula="revenue:sum(window='90d')", name="rev_90d")],
    )
    base.update(kw)
    return SlayerQuery(**base)


async def _wm_sql(query: SlayerQuery, model: SlayerModel, **kw) -> str:
    sql = await _engine_generate(query=query, model=model, validate=False, **kw)
    assert "_wm_" in sql, sql
    return sql


async def _plan(query: SlayerQuery, model: SlayerModel, *, extra_models=None):
    """Plan ``query`` against ``model`` and return the ``PlannedQuery``.

    Used by the column-set tests, which assert on planner state rather than
    emitted SQL.
    """
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name="test", type="sqlite", database=":memory:"),
        )
        await storage.save_model(model)
        for extra in extra_models or []:
            await storage.save_model(extra)
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        return plan_query(query=query, bundle=bundle)


# --------------------------------------------------------------------------- #
# The `_wm_` `_src` subquery
# --------------------------------------------------------------------------- #
class TestWindowedSrcFrameBounds:
    """Each test carries a companion predicate that must move the OPPOSITE way
    in the same query.

    Without one, a test asserting only "the bound left ``_src``" would also pass
    under a wrong implementation that drops EVERY row filter from ``_src`` — and
    a test asserting only "this predicate stayed" would pass under one that
    strips nothing at all. Pairing the two makes every assertion discriminating.
    """

    async def test_explicit_lower_bound_is_stripped_from_src(self) -> None:
        """The DEV-1732 headline case, and the inverse of the Stage-10 parity
        pin: the bound must bound ``_base`` and NOT reach ``_src``, while a
        sibling population filter is untouched."""
        sql = await _wm_sql(
            _windowed_query(
                filters=["created_at >= '2024-06-01'", "status = 'paid'"],
            ),
            _orders(),
        )
        src = _extract_src_body(sql)
        assert "2024-06-01" not in src, src
        assert "'paid'" in src, f"sibling population filter must survive.\n{src}"
        assert "2024-06-01" in _extract_cte_body(sql, r"_base"), sql

    async def test_explicit_upper_bound_is_stripped_from_src(self) -> None:
        """§2.1: BOTH bounds are stripped. ``date_range`` strips a single
        ``BetweenKey`` node — i.e. both — so equivalence demands the same."""
        sql = await _wm_sql(
            _windowed_query(
                filters=["created_at <= '2024-12-31'", "status = 'paid'"],
            ),
            _orders(),
        )
        src = _extract_src_body(sql)
        assert "2024-12-31" not in src, src
        assert "'paid'" in src, src
        assert "2024-12-31" in _extract_cte_body(sql, r"_base"), sql

    async def test_both_bounds_stripped_from_src_kept_on_base(self) -> None:
        sql = await _wm_sql(
            _windowed_query(
                filters=[
                    "created_at >= '2024-06-01' and created_at <= '2024-12-31'",
                    "status = 'paid'",
                ],
            ),
            _orders(),
        )
        src = _extract_src_body(sql)
        base = _extract_cte_body(sql, r"_base")
        assert "2024-06-01" not in src, src
        assert "2024-12-31" not in src, src
        assert "'paid'" in src, src
        assert "2024-06-01" in base, base
        assert "2024-12-31" in base, base

    async def test_literal_on_the_left_is_stripped_from_src(self) -> None:
        sql = await _wm_sql(
            _windowed_query(
                filters=["'2024-06-01' <= created_at", "status = 'paid'"],
            ),
            _orders(),
        )
        src = _extract_src_body(sql)
        assert "2024-06-01" not in src, src
        assert "'paid'" in src, src

    async def test_mixed_conjunction_keeps_population_predicate_only(self) -> None:
        """The split in §2.3, end to end: ``_src`` keeps ``status``, loses the
        bound; ``_base`` keeps both."""
        sql = await _wm_sql(
            _windowed_query(filters=["created_at >= '2024-06-01' and status = 'paid'"]),
            _orders(),
        )
        src = _extract_src_body(sql)
        base = _extract_cte_body(sql, r"_base")
        assert "'paid'" in src, src
        assert "2024-06-01" not in src, src
        assert "'paid'" in base, base
        assert "2024-06-01" in base, base

    async def test_disjunction_is_applied_whole_inside_src(self) -> None:
        """A frame bound under ``or`` is kept whole — no sound split exists.

        The sibling bare bound proves the feature is ACTIVE in this query, so
        the test cannot pass under a "never strip anything" implementation.
        """
        sql = await _wm_sql(
            _windowed_query(
                filters=[
                    "created_at >= '2024-06-01' or status = 'paid'",
                    "created_at >= '2024-07-01'",
                ],
            ),
            _orders(),
        )
        src = _extract_src_body(sql)
        assert "2024-06-01" in src, src
        assert "'paid'" in src, src
        assert "2024-07-01" not in src, f"the bare sibling bound must strip.\n{src}"

    async def test_non_time_dimension_time_column_is_kept_in_src(self) -> None:
        """``delivery_at`` is not a query TD, so its bound is a population
        filter — dropping it would over-count. The ``created_at`` sibling, which
        IS a query TD, strips in the same query."""
        sql = await _wm_sql(
            _windowed_query(
                filters=["delivery_at >= '2024-06-01'", "created_at >= '2024-07-01'"],
            ),
            _orders(),
        )
        src = _extract_src_body(sql)
        assert "2024-06-01" in src, src
        assert "2024-07-01" not in src, src

    async def test_equality_on_time_dimension_column_is_kept_in_src(self) -> None:
        """Same column, two operators, opposite outcomes — the cleanest possible
        demonstration that the operator, not the column, decides."""
        sql = await _wm_sql(
            _windowed_query(
                filters=["created_at == '2024-06-01'", "created_at >= '2024-07-01'"],
            ),
            _orders(),
        )
        src = _extract_src_body(sql)
        assert "2024-06-01" in src, src
        assert "2024-07-01" not in src, src

    async def test_mode_a_model_filter_is_kept_verbatim_in_src(self) -> None:
        """§2.5: a ``SlayerModel.filters`` entry defines which rows EXIST. There
        is no ``date_range`` spelling at model level, so there is no
        inconsistency to fix — and analysing raw Mode-A SQL would make a silent
        mis-strip possible.

        The model filter and the query filter name the SAME column, so the only
        thing separating them is their carrier — which is exactly the rule.
        """
        sql = await _wm_sql(
            _windowed_query(filters=["created_at >= '2024-07-01'"]),
            _orders(filters=["created_at >= '2024-01-01'"]),
        )
        src = _extract_src_body(sql)
        assert "2024-01-01" in src, f"Mode-A model filter must survive.\n{src}"
        assert "2024-07-01" not in src, f"the Mode-B query bound must strip.\n{src}"

    async def test_second_time_dimension_bound_stripped_but_join_key_kept(self) -> None:
        """§2.2: stripping a non-window TD's bound is safe precisely because
        that TD is equality-joined into ``_src`` as ``_w_td_<n>``, which
        re-imposes its bucket. Assert BOTH halves."""
        query = _windowed_query(
            time_dimensions=[
                _MONTHLY_CREATED_AT,
                TimeDimension(
                    dimension=ColumnRef(name="ship_date"),
                    granularity=TimeGranularity.DAY,
                ),
            ],
            main_time_dimension="created_at",
            filters=["ship_date >= '2024-06-01'"],
        )
        sql = await _wm_sql(query, _orders())
        src = _extract_src_body(sql)
        assert "2024-06-01" not in src, src
        assert "_w_td_0" in src, f"the other TD must still be grain-joined.\n{src}"
        assert "_w_td_0" in _norm(sql), sql

    async def test_variable_substituted_bound_is_stripped(self) -> None:
        """``{variable}`` substitution runs before planning, so the filter
        arrives as an ordinary ``LiteralKey`` and needs no special casing."""
        query = _windowed_query(
            filters=["created_at >= '{start}'"],
            variables={"start": "2024-06-01"},
        )
        sql = await _wm_sql(query, _orders())
        assert "2024-06-01" not in _extract_src_body(sql), sql
        assert "2024-06-01" in _extract_cte_body(sql, r"_base"), sql

    async def test_residual_keeps_the_join_it_crosses(self) -> None:
        """Join discovery must run over the RESIDUAL: the surviving
        ``customers.tier`` conjunct still needs its join."""
        query = _windowed_query(
            filters=["created_at >= '2024-06-01' and customers.tier = 'gold'"],
        )
        sql = await _wm_sql(query, _joined_orders(), extra_models=[_customers()])
        src = _extract_src_body(sql)
        assert "2024-06-01" not in src, src
        assert "'gold'" in src, src
        assert "customers" in _join_aliases(src), (
            f"_src must keep the join the residual crosses.\n{src}"
        )

    async def test_stripping_a_joined_td_bound_keeps_the_projection_join(self) -> None:
        """Stripping a frame bound can never ORPHAN a join.

        The strippable columns are exactly the query TDs' raw columns, and
        ``_src`` projects every non-hidden TD (``_w_time`` / ``_w_td_<n>``) — so
        a joined TD's join is required by the projection regardless of the
        filter. Codex asked for a "stripped bound was the only reason for the
        join" case; it is unreachable under this rule, and this test pins why:
        the bound goes, the join stays, and the grain column is still projected.
        """
        query = _windowed_query(
            time_dimensions=[
                _MONTHLY_CREATED_AT,
                TimeDimension(
                    dimension=ColumnRef(name="customers.signup_at"),
                    granularity=TimeGranularity.DAY,
                ),
            ],
            main_time_dimension="created_at",
            filters=["customers.signup_at >= '2024-06-01'"],
        )
        sql = await _wm_sql(query, _joined_orders(), extra_models=[_customers()])
        src = _extract_src_body(sql)
        assert "2024-06-01" not in src, src
        assert "customers" in _join_aliases(src), (
            f"the joined TD's projection still needs its join.\n{src}"
        )
        assert "_w_td_0" in src, f"the joined TD must still be grain-projected.\n{src}"

    async def test_stripped_and_residual_cross_different_joins(self) -> None:
        """Codex F4: the stripped conjunct and the residual cross DIFFERENT
        joins. Both joins survive — customers because the joined TD projects it,
        regions because the residual needs it — and only the bound disappears.
        """
        customers = _customers(
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
        query = _windowed_query(
            time_dimensions=[
                _MONTHLY_CREATED_AT,
                TimeDimension(
                    dimension=ColumnRef(name="customers.signup_at"),
                    granularity=TimeGranularity.DAY,
                ),
            ],
            main_time_dimension="created_at",
            filters=[
                "customers.signup_at >= '2024-06-01' "
                "and customers.regions.name = 'North'",
            ],
        )
        sql = await _wm_sql(
            query, _joined_orders(), extra_models=[customers, _regions()],
        )
        src = _extract_src_body(sql)
        aliases = _join_aliases(src)
        assert "2024-06-01" not in src, src
        assert "'North'" in src, src
        assert "customers" in aliases, (
            f"the joined TD's projection join must survive; got {aliases}.\n{src}"
        )
        assert "customers__regions" in aliases, (
            f"the residual's join must survive; got {aliases}.\n{src}"
        )

    async def test_multi_hop_residual_keeps_its_full_join_chain(self) -> None:
        """Codex F4: a residual crossing two hops keeps every hop."""
        customers = _customers(
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
        query = _windowed_query(
            filters=["created_at >= '2024-06-01' and customers.regions.name = 'North'"],
        )
        sql = await _wm_sql(
            query, _joined_orders(), extra_models=[customers, _regions()],
        )
        src = _extract_src_body(sql)
        aliases = _join_aliases(src)
        assert "2024-06-01" not in src, src
        assert "customers" in aliases, (
            f"multi-hop residual must keep its first hop; got {aliases}.\n{src}"
        )
        assert "customers__regions" in aliases, (
            f"multi-hop residual must keep its second hop; got {aliases}.\n{src}"
        )

    async def test_two_windowed_measures_share_the_residual(self) -> None:
        query = _windowed_query(
            measures=[
                ModelMeasure(formula="revenue:sum(window='90d')", name="rev_90d"),
                ModelMeasure(formula="revenue:avg(window='30d')", name="rev_30d"),
            ],
            filters=["created_at >= '2024-06-01' and status = 'paid'"],
        )
        sql = await _wm_sql(query, _orders())
        bodies = [
            _extract_cte_body(sql, r"_wm_orders__rev_90d"),
            _extract_cte_body(sql, r"_wm_orders__rev_30d"),
        ]
        for body in bodies:
            assert "2024-06-01" not in body, body
            assert "'paid'" in body, body

    async def test_split_conjunction_output_is_scope_closed(self) -> None:
        """A rewritten ``_src`` WHERE must still be a closed scope — every alias
        it references bound in its own FROM/JOINs. Paired with the predicate
        assertions so the test is not merely a SQL-validity check that would
        pass without any stripping."""
        query = _windowed_query(
            dimensions=[ColumnRef(name="status")],
            filters=["created_at >= '2024-06-01' and status = 'paid'"],
        )
        sql = await _wm_sql(query, _orders())
        src = _extract_src_body(sql)
        assert "2024-06-01" not in src, src
        assert "'paid'" in src, src
        assert_scope_closed(sql, dialect="postgres")

    async def test_bound_on_unselected_time_column_survives_into_src(self) -> None:
        """Only columns of SELECTED time dimensions are strippable.

        ``ship_date`` is a temporal column of the model but is not a query time
        dimension here, so nothing grain-joins it into ``_src`` and no
        ``date_range`` spelling exists for it — its bound is a population filter.
        The ``created_at`` sibling, which IS selected, strips in the same query.

        This is the query-surface half of the ``not s.hidden`` rule; the
        planner-level half is
        ``TestFrameBoundColumnSet.test_only_non_hidden_time_dimensions_are_strippable``.
        """
        sql = await _wm_sql(
            _windowed_query(
                filters=["ship_date >= '2024-06-01'", "created_at >= '2024-07-01'"],
            ),
            _orders(),
        )
        src = _extract_src_body(sql)
        assert "2024-06-01" in src, src
        assert "2024-07-01" not in src, src

    async def test_date_range_still_stripped_from_src(self) -> None:
        """Stage-10 regression guard — the behaviour DEV-1732 generalises must
        not move."""
        query = _windowed_query(
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-06-01", "2024-12-31"],
                ),
            ],
        )
        sql = await _wm_sql(query, _orders())
        assert "2024-06-01" not in _extract_src_body(sql), sql
        assert "2024-06-01" in sql, sql

    async def test_plain_row_filter_still_applied_inside_src(self) -> None:
        """Stage-10 regression guard: a population filter is untouched."""
        sql = await _wm_sql(_windowed_query(filters=["status = 'paid'"]), _orders())
        assert "'paid'" in _extract_src_body(sql), sql


# --------------------------------------------------------------------------- #
# The strippable column set, at the planner level
# --------------------------------------------------------------------------- #
class TestFrameBoundColumnSet:
    """``PlannedQuery.frame_bound_columns`` is the single authoritative set,
    computed once in the planner and read by BOTH the windowed ``_src`` path and
    the generator's shifted-CTE path (so the two cannot drift)."""

    async def test_holds_every_selected_time_dimension_raw_column(self) -> None:
        plan = await _plan(
            _windowed_query(
                time_dimensions=[
                    _MONTHLY_CREATED_AT,
                    TimeDimension(
                        dimension=ColumnRef(name="ship_date"),
                        granularity=TimeGranularity.DAY,
                    ),
                ],
                main_time_dimension="created_at",
            ),
            _orders(),
        )
        assert set(plan.frame_bound_columns) == {CREATED_AT, SHIP_DATE}, (
            plan.frame_bound_columns
        )

    async def test_only_non_hidden_time_dimensions_are_strippable(self) -> None:
        """Codex F3, inverted — the ``not s.hidden`` guard is load-bearing.

        ``_build_windowed_plans`` skips hidden row slots when building
        ``other_td_slot_ids``, so a hidden ``TimeTruncKey`` is never
        equality-joined into ``_src``. Stripping a bound on such a column would
        leave that axis wholly unconstrained — an unbounded over-count — so the
        set must contain only columns of NON-hidden time-dimension slots.
        """
        from slayer.core.keys import TimeTruncKey

        plan = await _plan(
            _windowed_query(
                time_dimensions=[
                    _MONTHLY_CREATED_AT,
                    TimeDimension(
                        dimension=ColumnRef(name="ship_date"),
                        granularity=TimeGranularity.DAY,
                    ),
                ],
                main_time_dimension="created_at",
                filters=["delivery_at >= '2024-06-01'"],
            ),
            _orders(),
        )
        visible = {
            s.key.column
            for s in plan.row_slots
            if isinstance(s.key, TimeTruncKey) and not s.hidden
        }
        assert set(plan.frame_bound_columns) == visible, (
            plan.frame_bound_columns, visible,
        )
        # A hidden slot's column never leaks into the set.
        hidden_cols = {
            s.key.column
            for s in plan.row_slots
            if isinstance(s.key, TimeTruncKey) and s.hidden
        }
        assert not (set(plan.frame_bound_columns) & hidden_cols), plan.frame_bound_columns
        # …nor does a plain row column that merely happens to be temporal.
        assert DELIVERY_AT not in set(plan.frame_bound_columns), plan.frame_bound_columns


# --------------------------------------------------------------------------- #
# The `time_shift` shifted CTE — same rule
# --------------------------------------------------------------------------- #
def _shift_query(**kw) -> SlayerQuery:
    base = dict(
        source_model="orders",
        time_dimensions=[_MONTHLY_CREATED_AT],
        measures=[ModelMeasure(formula="time_shift(revenue:sum, -1, 'month')", name="prev")],
    )
    base.update(kw)
    return SlayerQuery(**base)


async def _shifted_body(query: SlayerQuery, model: SlayerModel, **kw) -> str:
    sql = await _engine_generate(query=query, model=model, validate=False, **kw)
    return _extract_cte_body(sql, r"shifted_\w+")


class TestTimeShiftFrameBounds:
    async def test_explicit_bounds_do_not_reach_the_shifted_cte(self) -> None:
        """Before DEV-1732 the shifted CTE carried the bound, so the first
        visible bucket's ``prev`` was NULL — while the ``date_range`` spelling
        of the same intent produced a correct value."""
        body = await _shifted_body(
            _shift_query(
                filters=["created_at >= '2024-06-01' and created_at <= '2024-12-31'"],
            ),
            _orders(),
        )
        assert "2024-06-01" not in body, body
        assert "2024-12-31" not in body, body

    async def test_mixed_conjunction_keeps_population_predicate(self) -> None:
        body = await _shifted_body(
            _shift_query(filters=["created_at >= '2024-06-01' and status = 'paid'"]),
            _orders(),
        )
        assert "'paid'" in body, body
        assert "2024-06-01" not in body, body

    async def test_population_filter_still_propagates(self) -> None:
        """Stage-7 regression guard: a non-frame ROW filter must keep reaching
        the shifted CTE so it aggregates the same row population as ``_base``.

        The sibling bound proves the rule is ACTIVE here, so this cannot pass
        under a "never strip anything" implementation.
        """
        body = await _shifted_body(
            _shift_query(
                filters=["status = 'paid'", "created_at >= '2024-07-01'"],
            ),
            _orders(),
        )
        assert "'paid'" in body, body
        assert "2024-07-01" not in body, body

    async def test_date_range_still_omitted(self) -> None:
        """Stage-7 regression guard (the 7b.3c invariant): ``date_range`` was
        already omitted before DEV-1732 and must stay omitted."""
        body = await _shifted_body(
            _shift_query(
                time_dimensions=[
                    TimeDimension(
                        dimension=ColumnRef(name="created_at"),
                        granularity=TimeGranularity.MONTH,
                        date_range=["2024-06-01", "2024-12-31"],
                    ),
                ],
                filters=["status = 'paid'"],
            ),
            _orders(),
        )
        assert "2024-06-01" not in body, body
        assert "'paid'" in body, f"population filters must still propagate.\n{body}"

    async def test_non_time_dimension_bound_still_propagates(self) -> None:
        body = await _shifted_body(
            _shift_query(
                filters=["delivery_at >= '2024-06-01'", "created_at >= '2024-07-01'"],
            ),
            _orders(),
        )
        assert "2024-06-01" in body, body
        assert "2024-07-01" not in body, body
