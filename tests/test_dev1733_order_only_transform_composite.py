"""DEV-1733 — order-only transform / composite / windowed ORDER BY targets.

DEV-1712 (Stage 8) shipped hidden-slot ORDER BY for order-only AGGREGATE refs
and split emission for order-only ROW-COLUMN refs, and rejected inline
transform / composite order targets with a plan-time ``ValueError``. This
module pins the full-support contract that replaces that rejection.

The completed table for an ORDER BY ref matching no declared/public slot:

  target (hidden)          | behaviour
  ------------------------ | --------------------------------------------------
  local aggregate          | materialise hidden, order, strip     (DEV-1712)
  cross-model aggregate    | hidden ``_cm_`` CTE, trimmed, CTE-qualified order
  local row column, raw    | SPLIT emission ``orders.col``        (DEV-1712)
  local row column, grouped| hidden ``<col>:max`` wrap, order, strip (Phase 1)
  joined row column, raw   | Law-1 join pull + SPLIT emission     (Phase 1)
  joined row column, grpd  | UnresolvableOrderColumnError         (DEV-1712)
  transform                | hidden slot -> step CTE -> outer-wrap order  <- NEW
  composite / scalar call  | hidden slot materialised -> outer-wrap order <- NEW
  windowed, top-level      | hidden ``_wm_`` plan, trimmed, CTE-qual order <- NEW
  windowed in a composite  | outer combined SELECT, inline order term      <- NEW

Two silent-wrong-answer bugs are pinned here as regressions:

* **B1** — an order-only windowed aggregate (``amount:sum(window='90d')`` in
  ``order`` with ``amount:sum`` not also declared) rendered a PLAIN ``SUM`` and
  ordered by it, silently dropping the 90-day window. ``OrderItem.raw_formula``
  preserves the ``window=`` kwarg, so the shape was reachable despite the
  planner comment asserting otherwise.
* **B2** — two HIDDEN transform slots of the same op collided on one
  ``declared_name`` (``_cumsum_inner``), so ``cumsum(a:sum) + cumsum(b:sum)``
  emitted two step-CTE columns under an identical alias and computed
  ``cumsum(a) + cumsum(a)``. DEV-1692 fixed this class for ``time_shift`` /
  ``consecutive_periods`` only; uniqueness now lives in the PLAN (registry
  intern), not in any one renderer.

Refs: DEV-1733 (this issue), DEV-1712 (Stage 8), DEV-1692 (hidden-alias
collisions), DEV-1714 (``_wm_`` windowed measures), DEV-1504 (windowed
composites in ``measures``, windowed + transform — still guarded).
"""
from __future__ import annotations

import re
import sqlite3

import pydantic
import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import (
    DistinctDimensionValuesError,
    RenderContextMissingFacilityError,
    UnknownReferenceError,
)
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.core.keys import ColumnKey, Phase
from slayer.engine.planned import OrderEntry, OrderScope, PlannedQuery, ValueSlot
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.sql.scope_check import assert_scope_closed
from slayer.storage.yaml_storage import YAMLStorage

_MONTH = [TimeDimension(dimension="created_at", granularity="month")]

# DEV-1763: the composite renderer routes through ``render_value_key`` on a
# scope-less context, so a bare ROW-column operand fails closed with
# ``RenderContextMissingFacilityError`` (a ``ValueError``) rather than the legacy
# terminal ``NotImplementedError``. Both REJECT — the security-relevant contract
# is "raises, never stringified" — so the row-operand guards below accept either.
# (The legacy renderer still raises ``NotImplementedError``; its direct tests
# stay pinned — P-J state 1.)
_ROW_OPERAND_REJECTED = (NotImplementedError, RenderContextMissingFacilityError)


# ---------------------------------------------------------------------------
# SQL-inspection helpers — walk the AST, never match on formatting.
# ---------------------------------------------------------------------------
def _outermost_select(sql: str, *, dialect: str = "postgres") -> exp.Select:
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    assert isinstance(parsed, exp.Select), f"not a SELECT:\n{sql}"
    return parsed


def _outer_select_columns(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Alias names projected by the OUTERMOST SELECT (the public result)."""
    return [p.alias_or_name for p in _outermost_select(sql, dialect=dialect).expressions]


def _outer_order_by_names(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Leaf identifier names referenced by the outermost ORDER BY."""
    order = _outermost_select(sql, dialect=dialect).args.get("order")
    if order is None:
        return []
    out: list[str] = []
    for ordered in order.expressions:
        col = ordered.this
        out.append(col.name if isinstance(col, exp.Column) else col.sql(dialect=dialect))
    return out


def _outer_order_by_sql(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Full rendered SQL of each outermost ORDER BY term (expressions included)."""
    order = _outermost_select(sql, dialect=dialect).args.get("order")
    if order is None:
        return []
    return [o.this.sql(dialect=dialect) for o in order.expressions]


def _all_quoted_idents(sql: str) -> list[str]:
    return re.findall(r'"([^"]+)"', sql)


def _emitted_aliases(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Every ``<expr> AS <alias>`` alias emitted anywhere in the statement.

    Duplicates are preserved — a repeated alias inside one SELECT is exactly
    the B2 collision this module guards against.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    return [a.alias for a in parsed.find_all(exp.Alias) if a.alias]


def _duplicate_select_aliases(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Aliases emitted more than once WITHIN a single SELECT projection.

    This is the precise B2 signature: one ``SELECT`` projecting two different
    expressions under one identifier, so every later reference is ambiguous.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    dupes: list[str] = []
    for select in parsed.find_all(exp.Select):
        seen: set[str] = set()
        for proj in select.expressions:
            alias = proj.alias_or_name
            if not alias:
                continue
            if alias in seen:
                dupes.append(alias)
            seen.add(alias)
    return dupes


def _src_subquery_sql(sql: str, *, dialect: str = "sqlite") -> str:
    """The rendered SQL of the ``_src`` subquery inside a ``_wm_`` CTE.

    AST-based rather than string-split so it survives formatting and CTE
    renaming: find the ``Subquery``/``Alias`` node whose alias is ``_src``.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    for node in parsed.find_all(exp.Subquery):
        if node.alias == "_src":
            return node.sql(dialect=dialect)
    raise AssertionError(f"no `_src` subquery found — the _wm_ CTE is absent.\n{sql}")


def _cte_aggregate_sql(
    sql: str, *, cte: str, column: str, dialect: str = "sqlite",
) -> list[str]:
    """Rendered SQL of every aggregate over ``column`` inside the named CTE.

    Selects the CTE from the parsed tree by alias rather than splitting on
    separator text — a spacing change in the emitter would make a string split
    silently return the whole statement, and the caller's assertion would stop
    testing anything while still passing.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    target = next(
        (c for c in parsed.find_all(exp.CTE) if c.alias == cte), None,
    )
    assert target is not None, f"no `{cte}` CTE found.\nSQL:\n{sql}"
    return [
        agg.sql(dialect=dialect)
        for agg in target.find_all(exp.AggFunc)
        if column in agg.sql(dialect=dialect)
    ]


def _hidden_order_alias(sql: str, *, dialect: str = "postgres") -> str:
    """The single outer ORDER BY alias, asserted NOT to be publicly projected.

    That pair — ordered on, but absent from the outermost SELECT — is the Law-2
    hidden-slot discipline this issue extends to transforms and composites.
    """
    names = _outer_order_by_names(sql, dialect=dialect)
    assert len(names) == 1, f"expected exactly one ORDER BY term, got {names}\n{sql}"
    alias = names[0]
    public = _outer_select_columns(sql, dialect=dialect)
    assert alias not in public, (
        f"hidden order target {alias!r} must not be projected.\n"
        f"outer SELECT: {public}\nSQL:\n{sql}"
    )
    return alias


# ---------------------------------------------------------------------------
# Fixtures — orders -> customers / suppliers.
# ---------------------------------------------------------------------------
async def _save_models(storage: YAMLStorage) -> None:
    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="revenue", sql="lifetime_revenue", type=DataType.DOUBLE),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="suppliers", sql_table="suppliers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="revenue", sql="supplier_revenue", type=DataType.DOUBLE),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="supplier_id", sql="supplier_id", type=DataType.INT),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="fee", sql="fee", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="suppliers", join_pairs=[["supplier_id", "id"]]),
        ],
    ))


_SEED = """
CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, lifetime_revenue REAL);
CREATE TABLE suppliers (id INTEGER PRIMARY KEY, region TEXT, supplier_revenue REAL);
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, supplier_id INTEGER,
                     status TEXT, created_at TEXT, amount REAL, fee REAL);
INSERT INTO customers VALUES (100,'West',500.0),(101,'East',700.0);
INSERT INTO suppliers VALUES (200,'West',1000.0),(201,'East',2000.0);
INSERT INTO orders VALUES
    (1,100,200,'paid','2025-01-01',10.0,1.0),
    (2,100,200,'paid','2025-01-02',40.0,2.0),
    (3,101,201,'open','2025-02-01',20.0,4.0),
    (4,101,201,'open','2025-02-02', 5.0,8.0);
"""
# Monthly rollup:  Jan amount 50 / count 2   Feb amount 25 / count 2
#   cumsum(amount:sum) -> Jan 50, Feb 75
#   cumsum(id:count)   -> Jan  2, Feb  4
#   sum of the two     -> Jan 52, Feb 79      <- the B2 assertion
# Status rollup:   paid amount 50 / count 2   open amount 25 / count 2
#   amount:sum / id:count -> paid 25.0, open 12.5


@pytest.fixture
async def engine(tmp_path):
    """Dry-run engine — SQL shape only, no database needed."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:"),
    )
    await _save_models(storage)
    return SlayerQueryEngine(storage=storage)


@pytest.fixture
async def exec_engine(tmp_path):
    """On-disk SQLite seeded for execution / ordering / value assertions."""
    db_path = tmp_path / "t.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_SEED)
    conn.commit()
    conn.close()
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=str(db_path)),
    )
    await _save_models(storage)
    return SlayerQueryEngine(storage=storage)


async def _sql(engine: SlayerQueryEngine, query: SlayerQuery) -> str:
    """Render a query and assert scope closure on every statement this module
    produces (DEV-1733 acceptance: suite-wide ``assert_scope_closed`` green)."""
    resp = await engine.execute(query, dry_run=True)
    sql = resp.sql or ""
    assert_scope_closed(sql, dialect="sqlite")
    return sql


# ===========================================================================
# Group 1 — entry point: composite ORDER BY strings become expressible.
# ===========================================================================
class TestOrderItemEntryPoint:
    def test_composite_colon_string_yields_placeholder_and_raw_formula(self) -> None:
        """``revenue:sum / cnt:sum`` is not a ``ColumnRef``; it is carried as a
        formula. The placeholder marks the column ref as "resolve me from
        ``raw_formula``" — the same mechanism func-style calls already use."""
        item = OrderItem(column="amount:sum / id:count", direction="desc")
        assert item.column.name == "_expr_pending", item.column
        assert item.raw_formula == "amount:sum / id:count", item.raw_formula

    def test_transform_call_string_keeps_funcstyle_placeholder(self) -> None:
        item = OrderItem(column="change(amount:sum)", direction="desc")
        assert item.column.name == "_funcstyle_pending", item.column
        assert item.raw_formula == "change(amount:sum)"

    def test_arithmetic_over_transform_yields_placeholder(self) -> None:
        item = OrderItem(column="change(amount:sum) / 2", direction="desc")
        assert item.column.name == "_expr_pending", item.column
        assert item.raw_formula == "change(amount:sum) / 2"

    def test_composite_over_declared_aliases_still_rejected(self) -> None:
        """A composite over declared measure NAMES carries no ``:`` and no
        func-style call, so it is not a formula candidate. Referencing a
        declared measure by alias inside an expression is unsupported
        everywhere in SLayer, so this keeps failing fast at construction with
        the pre-DEV-1733 message rather than deep in the binder."""
        with pytest.raises(pydantic.ValidationError) as ei:
            OrderItem(column="rev / cnt", direction="desc")
        assert "rev / cnt" in str(ei.value)

    def test_plain_column_and_dotted_ref_unaffected(self) -> None:
        assert OrderItem(column="status").column.name == "status"
        dotted = OrderItem(column="customers.region").column
        assert (dotted.model, dotted.name) == ("customers", "region")
        agg = OrderItem(column="amount:sum").column
        assert agg.name == "amount_sum"

    def test_placeholder_survives_strip_source_model_prefix(self) -> None:
        """``strip_source_model_prefix`` rebuilds every ``OrderItem``; the
        placeholder + ``raw_formula`` pair must survive that rebuild or the
        planner loses the expression."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="orders.amount:sum / orders.id:count", direction="desc")],
        )
        stripped = query.strip_source_model_prefix()
        item = stripped.order[0]
        assert item.column.name == "_expr_pending", item.column
        assert item.raw_formula is not None
        assert "orders." not in item.raw_formula, item.raw_formula

    async def test_real_column_named_like_placeholder_is_not_hijacked(
        self, tmp_path,
    ) -> None:
        """The sentinel is a marker, not a magic column name. An ``OrderItem``
        whose column genuinely IS ``_expr_pending`` (and which carries no
        ``raw_formula``) must resolve as a normal column, not be routed to
        formula binding."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(
            DatasourceConfig(name="test", type="sqlite", database=":memory:"),
        )
        await storage.save_model(SlayerModel(
            name="odd", sql_table="odd", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="_expr_pending", sql="weird_col", type=DataType.TEXT),
            ],
        ))
        eng = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="odd",
            dimensions=[ColumnRef(name="_expr_pending")],
            distinct_dimension_values=False,
            order=[OrderItem(column="_expr_pending", direction="asc")],
        )
        sql = await _sql(eng, query)
        assert "weird_col" in sql, sql

    def test_hand_built_order_item_without_raw_formula_is_not_formula_bound(self) -> None:
        """A manually constructed / deserialized ``OrderItem`` carrying the
        sentinel but no ``raw_formula`` must not be treated as an expression —
        the sentinel alone is not the discriminator."""
        item = OrderItem(column=ColumnRef(name="_expr_pending"), direction="asc")
        assert item.raw_formula is None


# ===========================================================================
# Group 2 — order-only plain transforms materialise, order, and are stripped.
# ===========================================================================
class TestOrderOnlyTransform:
    async def test_rank_order_only_hidden_and_ordered(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="rank(amount:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders.amount_sum"], sql
        alias = _hidden_order_alias(sql)
        assert "RANK()" in sql.upper(), sql
        assert alias in _emitted_aliases(sql), (
            f"hidden transform {alias!r} must be materialised in a step CTE.\n{sql}"
        )

    @pytest.mark.parametrize("formula", ["cumsum(amount:sum)", "lag(amount:sum)", "lead(amount:sum)"])
    async def test_time_ordered_transforms_order_only(self, engine, formula: str) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column=formula, direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.amount_sum"], sql
        _hidden_order_alias(sql)

    async def test_ntile_order_only(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="ntile(amount:sum, n=4)", direction="asc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders.amount_sum"], sql
        _hidden_order_alias(sql)
        assert "NTILE" in sql.upper(), sql

    async def test_order_only_transform_alongside_declared_transform(self, engine) -> None:
        """A declared transform already forces the chain; the order-only
        transform must join it as another hidden step column rather than
        disabling either."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="cumsum(amount:sum)", name="cs")],
            order=[OrderItem(column="rank(amount:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.cs"], sql
        _hidden_order_alias(sql)

    async def test_order_only_transform_with_limit_and_offset(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="rank(amount:sum)", direction="desc")],
            limit=2, offset=1,
        )
        sql = await _sql(engine, query)
        outermost = _outermost_select(sql)
        assert outermost.args.get("limit") is not None, sql
        assert outermost.args.get("offset") is not None, sql
        _hidden_order_alias(sql)

    async def test_order_only_transform_with_post_filter(self, engine) -> None:
        """A POST filter on the declared transform wraps the chain; the
        order-only transform's alias must survive that wrap."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="cumsum(amount:sum)", name="cs")],
            filters=["cs > 5"],
            order=[OrderItem(column="rank(amount:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.cs"], sql
        _hidden_order_alias(sql)


# ===========================================================================
# Group 3 — change / change_pct: arithmetic-wrapped transforms.
# Stage 8 dropped these ORDER BY clauses entirely (no slot was interned for
# the top-level ArithmeticKey, so the order entry was silently discarded).
# ===========================================================================
class TestOrderOnlyChangeFamily:
    @pytest.mark.parametrize("formula", ["change(amount:sum)", "change_pct(amount:sum)"])
    async def test_change_family_order_only_emits_order_by(self, engine, formula: str) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column=formula, direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_order_by_names(sql), (
            f"ORDER BY must be emitted, not silently dropped.\nSQL:\n{sql}"
        )
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.amount_sum"], sql
        alias = _hidden_order_alias(sql)
        assert alias in _emitted_aliases(sql), (
            f"the arithmetic wrapper {alias!r} needs its own materialised "
            f"column, not just its time_shift operand.\nSQL:\n{sql}"
        )

    async def test_change_family_emits_shift_join(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="change(amount:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert "shifted_" in sql, sql
        assert "sjoin_" in sql, sql

    async def test_arithmetic_over_transform_order_only(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="cumsum(amount:sum) / 2", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.amount_sum"], sql
        _hidden_order_alias(sql)


# ===========================================================================
# Group 4 — order-only composites / scalar calls, no transform layers.
# These materialise as a hidden column in the base SELECT and are trimmed by
# the outer wrap (D4) — the same discipline hidden aggregates already follow.
# ===========================================================================
class TestOrderOnlyCompositeLocal:
    async def test_composite_arithmetic_order_only(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / id:count", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        alias = _hidden_order_alias(sql)
        assert alias in _emitted_aliases(sql), (
            f"the composite must be materialised (D4), not inlined.\n{sql}"
        )

    async def test_scalar_call_order_only(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="abs(amount:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        _hidden_order_alias(sql)
        assert "ABS(" in sql.upper(), sql

    async def test_composite_operands_are_not_projected(self, engine) -> None:
        """Only the composite is ordered on; neither operand may leak into the
        public projection (Law 2)."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / fee:sum", direction="desc")],
        )
        sql = await _sql(engine, query)
        public = _outer_select_columns(sql)
        assert public == ["orders.status", "orders._count"], sql
        assert not any("amount_sum" in c or "fee_sum" in c for c in public), public

    async def test_composite_order_with_limit(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / id:count", direction="desc")],
            limit=1,
        )
        sql = await _sql(engine, query)
        assert _outermost_select(sql).args.get("limit") is not None, sql
        _hidden_order_alias(sql)


# ===========================================================================
# Group 5 — composites whose operands cross a join.
#
# A cross-model operand lives in a ``_cm_`` CTE, never in ``_base``. The
# composite must therefore be OWNED by the combined SELECT — promoting it into
# ``_base`` would reference an operand that is not in scope there. Both
# operand orders are covered because the routing predicate walks the key tree.
# ===========================================================================
class TestOrderOnlyCompositeCrossModel:
    async def test_local_over_cross_model_order_only(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / customers.revenue:sum", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        assert _outer_order_by_sql(sql), f"ORDER BY must be emitted.\n{sql}"
        assert "_cm_" in sql, sql

    async def test_cross_model_over_local_order_only(self, engine) -> None:
        """Reversed operand order — division is non-commutative, and the
        routing decision must not depend on which side is remote."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="customers.revenue:sum / amount:sum", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        assert _outer_order_by_sql(sql), f"ORDER BY must be emitted.\n{sql}"

    async def test_two_remote_models_in_one_order_composite(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(
                column="customers.revenue:sum / suppliers.revenue:sum", direction="desc",
            )],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        assert sql.count("_cm_orders__customers__revenue_sum") >= 1, sql
        assert sql.count("_cm_orders__suppliers__revenue_sum") >= 1, sql

    async def test_scalar_call_wrapping_cross_model_order_only(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="abs(customers.revenue:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        assert _outer_order_by_sql(sql), f"ORDER BY must be emitted.\n{sql}"

    async def test_transform_over_cross_model_aggregate_order_only(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="cumsum(customers.revenue:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.amount_sum"], sql
        _hidden_order_alias(sql)


# ===========================================================================
# Group 6 — hidden-alias uniqueness (B2). Uniqueness is a property of the
# PLAN: a hidden slot whose canonical name is already taken gets a numeric
# suffix at intern time, so no renderer can reintroduce the collision.
# ===========================================================================
class TestHiddenAliasUniqueness:
    async def test_two_hidden_cumsums_get_distinct_aliases(self, engine) -> None:
        """B2 repro. Both hidden slots canonicalise to ``_cumsum_inner``."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="cumsum(amount:sum) + cumsum(id:count)", name="both")],
        )
        sql = await _sql(engine, query)
        assert not _duplicate_select_aliases(sql), (
            f"duplicate alias within one SELECT — the composite would resolve "
            f"both operands to the same column.\nSQL:\n{sql}"
        )
        idents = _all_quoted_idents(sql)
        assert "orders._cumsum_inner" in idents, sql
        assert "orders._cumsum_inner_2" in idents, sql

    @pytest.mark.parametrize("op", ["rank", "lag", "lead"])
    async def test_two_hidden_transforms_same_op_distinct_aliases(self, engine, op: str) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(
                formula=f"{op}(amount:sum) + {op}(id:count)", name="both",
            )],
        )
        sql = await _sql(engine, query)
        assert not _duplicate_select_aliases(sql), sql

    async def test_two_order_items_same_transform_op(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[
                OrderItem(column="rank(amount:sum)", direction="desc"),
                OrderItem(column="rank(fee:sum)", direction="asc"),
            ],
        )
        sql = await _sql(engine, query)
        assert not _duplicate_select_aliases(sql), sql
        names = _outer_order_by_names(sql)
        assert len(names) == 2, (
            f"both order targets must survive: {names}\n{sql}"
        )
        assert len(set(names)) == 2, (
            f"the two order targets must use distinct aliases: {names}\n{sql}"
        )

    async def test_two_hidden_composites_same_op(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[
                OrderItem(column="amount:sum / id:count", direction="desc"),
                OrderItem(column="fee:sum / id:count", direction="asc"),
            ],
        )
        sql = await _sql(engine, query)
        assert not _duplicate_select_aliases(sql), sql
        names = _outer_order_by_names(sql)
        assert len(set(names)) == 2, f"{names}\n{sql}"

    async def test_two_hidden_time_shifts_stay_distinct(self, engine) -> None:
        """DEV-1692 regression — the time_shift emitter's own allocation must
        keep working once uniqueness also happens at plan time."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[
                ModelMeasure(formula="time_shift(amount:sum, 1)", name="s1"),
                ModelMeasure(formula="time_shift(amount:sum, 2)", name="s2"),
            ],
        )
        sql = await _sql(engine, query)
        assert not _duplicate_select_aliases(sql), sql

    async def test_user_measure_named_like_a_hidden_slot(self, engine) -> None:
        """A user may legitimately name a measure ``_cumsum_inner``. Public
        names are authoritative; the hidden slot must yield, regardless of
        which is interned first."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[
                ModelMeasure(formula="cumsum(amount:sum) + 1", name="shifted"),
                ModelMeasure(formula="id:count", name="_cumsum_inner"),
            ],
        )
        sql = await _sql(engine, query)
        assert not _duplicate_select_aliases(sql), (
            f"a user measure named _cumsum_inner must not collide with the "
            f"hidden transform slot.\nSQL:\n{sql}"
        )
        assert "orders._cumsum_inner" in _outer_select_columns(sql), sql

    async def test_user_measure_named_like_hidden_slot_declared_first(self, engine) -> None:
        """Reversed declaration order — uniquification must not depend on
        which slot the registry interns first."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[
                ModelMeasure(formula="id:count", name="_cumsum_inner"),
                ModelMeasure(formula="cumsum(amount:sum) + 1", name="shifted"),
            ],
        )
        sql = await _sql(engine, query)
        assert not _duplicate_select_aliases(sql), sql
        assert "orders._cumsum_inner" in _outer_select_columns(sql), sql

    async def test_public_names_are_never_renamed(self, engine) -> None:
        """Uniquification touches hidden slots only — a user-declared name is
        the result-key contract and must survive verbatim."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[
                ModelMeasure(formula="cumsum(amount:sum)", name="c1"),
                ModelMeasure(formula="cumsum(id:count)", name="c2"),
            ],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.c1", "orders.c2"], sql


# ===========================================================================
# Group 7 — windowed ORDER BY targets (S-a top-level, S-b inside a composite).
# ===========================================================================
class TestWindowedOrderOnly:
    async def test_order_only_windowed_builds_wm_cte(self, engine) -> None:
        """B1 repro. Before DEV-1733 this rendered a PLAIN ``SUM`` in the base
        SELECT and ordered by it — the 90-day window silently vanished."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        sql = await _sql(engine, query)
        # DEV-1835: windowed producers render under uniform ``_cm_`` naming.
        assert "_cm_" in sql, (
            f"an order-only windowed measure needs its own _cm_ range-join CTE "
            f"— a plain SUM silently drops the window.\nSQL:\n{sql}"
        )
        assert "_w_time" in sql, sql
        assert "_src" in sql, sql
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.id_count"], (
            f"the hidden windowed value must be trimmed from the projection.\n{sql}"
        )
        assert _outer_order_by_sql(sql), f"ORDER BY must be emitted.\n{sql}"

    async def test_order_only_windowed_is_cte_qualified_in_order_by(self, engine) -> None:
        """Trimmed from the projection, so the bare alias no longer names a
        projected column — the ORDER BY term must be CTE-qualified, exactly as
        the hidden cross-model aggregate case is."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        sql = await _sql(engine, query)
        terms = _outer_order_by_sql(sql)
        assert len(terms) == 1, terms
        # DEV-1835: windowed producers render under uniform ``_cm_`` naming.
        assert terms[0].startswith("_cm_"), (
            f"hidden windowed order term must be CTE-qualified: {terms}\n{sql}"
        )

    async def test_windowed_inside_order_only_composite(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="amount:sum(window='90d') / id:count", direction="desc")],
        )
        sql = await _sql(engine, query)
        # DEV-1835: windowed producers render under uniform ``_cm_`` naming.
        assert "_cm_" in sql, sql
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.id_count"], (
            f"nothing from the order-only composite may leak into the "
            f"projection.\n{sql}"
        )
        terms = _outer_order_by_sql(sql)
        assert len(terms) == 1, f"{terms}\n{sql}"
        # The load-bearing assertion: the windowed operand must resolve to the
        # ``_cm_`` CTE column. Asserting only that a `_cm_` CTE EXISTS is not
        # enough — the composite can be rendered in ``_base`` from a PLAIN
        # aggregate while the `_cm_` CTE sits joined but unused, which is the
        # B1 silent-wrong-answer reaching through the composite door.
        assert "_cm_" in terms[0], (
            f"the composite must read the ROLLING value from its _cm_ CTE, "
            f"not a plain aggregate.\nORDER BY: {terms[0]}\nSQL:\n{sql}"
        )
        # ... and no plain aggregate may be materialised for it in ``_base``.
        # AST-selected by CTE name, not string-split on the separator text: a
        # spacing change would make `split` return the whole statement and this
        # assertion would silently stop testing anything.
        base_sums = _cte_aggregate_sql(sql, cte="_base", column="amount")
        assert not base_sums, (
            f"a plain SUM of the windowed column must not be emitted in "
            f"_base.\ngot: {base_sums}\nSQL:\n{sql}"
        )

    async def test_hidden_and_public_windowed_in_one_query(self, engine) -> None:
        """A declared windowed measure plus a DIFFERENT order-only windowed
        one: two ``_wm_`` CTEs, one projected and one trimmed."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w90")],
            order=[OrderItem(column="fee:sum(window='30d')", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.w90"], sql
        assert len(re.findall(r"_cm_\w+ AS \(", sql)) == 2, sql

    async def test_declared_windowed_also_used_as_order_target_stays_public(
        self, engine,
    ) -> None:
        """The same windowed key declared AND ordered on: it is public, so it
        must stay projected and be referenced by its own alias — not be
        reclassified hidden by the order pass."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w90")],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.w90"], sql
        assert len(re.findall(r"_cm_\w+ AS \(", sql)) == 1, sql
        assert _outer_order_by_names(sql) == ["orders.w90"], sql

    async def test_c13_two_aliases_for_one_windowed_key_plus_order(self, engine) -> None:
        """C13: one windowed value declared under two names. Both must project
        and the order target must not collapse them."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[
                ModelMeasure(formula="amount:sum(window='90d')", name="wa"),
                ModelMeasure(formula="amount:sum(window='90d')", name="wb"),
            ],
            order=[OrderItem(column="wa", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.wa", "orders.wb"], sql
        assert len(re.findall(r"_cm_\w+ AS \(", sql)) == 1, sql

    async def test_order_only_windowed_with_grain_dimension(self, engine) -> None:
        """The ``_wm_`` grain partition is derived from the PROJECTED row slots;
        a hidden order target must not add to or remove from that grain."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert "_w_dim_0" in sql, f"the status grain must reach _src.\n{sql}"
        assert _outer_select_columns(sql) == [
            "orders.status", "orders.created_at", "orders.id_count",
        ], sql

    async def test_order_only_windowed_inherits_row_filters_but_not_date_range(
        self, engine,
    ) -> None:
        """DEV-1714 contract: ``_src`` inherits WHERE-phase row filters but NOT
        the typed ``date_range`` (the trailing window must reach rows before the
        range start). A hidden plan follows the same rule."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension="created_at", granularity="month",
                date_range=["2025-02-01", "2025-02-28"],
            )],
            measures=[ModelMeasure(formula="id:count")],
            filters=["status = 'paid'"],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        sql = await _sql(engine, query)
        src = _src_subquery_sql(sql)
        assert "status" in src, f"row filter must reach _src.\n{src}\nSQL:\n{sql}"
        assert "2025-02-01" not in src, (
            f"date_range must NOT reach _src.\n{src}\nSQL:\n{sql}"
        )
        # Control: the date_range IS applied, just not inside ``_src``.
        assert "2025-02-01" in sql, sql

    async def test_order_only_windowed_with_limit(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
            limit=1,
        )
        sql = await _sql(engine, query)
        assert "LIMIT 1" in sql, sql

    async def test_post_filter_and_order_on_same_windowed_key(self, engine) -> None:
        """A filter on a windowed measure requires it to be selected (G6); the
        order target then reuses the SAME plan rather than minting a second."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w90")],
            filters=["w90 > 0"],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert len(re.findall(r"_cm_\w+ AS \(", sql)) == 1, sql


class TestWindowedStillGuarded:
    """DEV-1504 boundaries kept: composite windowed in ``measures``, and any
    windowed/transform combination."""

    async def test_windowed_in_declared_composite_measure_still_raises(self, engine) -> None:
        # DEV-1835: local windowed composite in ``measures`` now renders through
        # the regroup producers (guard lifted); it must be scope-closed and leak
        # no ``__regroup__`` marker.
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="amount:sum(window='90d') / id:count", name="x")],
        )
        sql = await _sql(engine, query)
        assert_scope_closed(sql, dialect="sqlite")
        assert "__regroup__" not in sql

    async def test_transform_over_windowed_order_target_still_raises(self, engine) -> None:
        # DEV-1835: transform over a LOCAL windowed order target now renders
        # (guard lifted); cross-model windowed is still guarded below.
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="cumsum(amount:sum(window='90d'))", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert_scope_closed(sql, dialect="sqlite")
        assert "__regroup__" not in sql

    async def test_cross_model_windowed_order_target_still_raises(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="customers.revenue:sum(window='90d')", direction="desc")],
        )
        with pytest.raises(ValueError) as ei:
            await _sql(engine, query)
        # DEV-1836: cross-model windowed stays guarded — the host TD is not
        # attributable from the target root, so the producer refuses the window.
        assert "Windowed cross-model aggregate" in str(ei.value), ei.value
        assert "attributable from" in str(ei.value), ei.value

    async def test_non_sum_avg_windowed_order_target_still_raises(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="amount:max(window='90d')", direction="desc")],
        )
        with pytest.raises(ValueError) as ei:
            await _sql(engine, query)
        assert "sum and avg" in str(ei.value), ei.value


# ===========================================================================
# Group 8 — widening the hidden-order branch must not cross its boundaries:
# some shapes must KEEP raising; the newly-resolved ones must not change grain.
# ===========================================================================
class TestStillGuarded:
    async def test_joined_row_column_order_resolves_host_rooted(
        self, engine,
    ) -> None:
        """DEV-1747 D2 replaced this rejection with a host-rooted CTE. What
        Group 8 still guards is that widening the hidden-order branch did not
        change the GRAIN: the sort key must not join the base or reach the
        projection."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="customers.region", direction="desc")],
        )
        sql = await _sql(engine, query)
        parsed = _outermost_select(sql, dialect="sqlite")
        assert [e.alias_or_name for e in parsed.expressions] == [
            "orders.status", "orders._count",
        ], sql
        # Grain unchanged: the base CTE still groups on the query dim only —
        # the joined sort key did NOT widen the base GROUP BY nor join the base.
        base_cte = next(
            (c.this for c in sqlglot.parse_one(sql, dialect="sqlite").find_all(exp.CTE)
             if c.alias == "_base"), None,
        )
        assert base_cte is not None, f"expected a `_base` CTE.\nSQL:\n{sql}"
        group = base_cte.args.get("group")
        assert group is not None, f"`_base` has no GROUP BY.\nSQL:\n{sql}"
        assert [g.name for g in group.expressions] == ["status"], (
            f"base GROUP BY grain widened past the query dims.\nSQL:\n{sql}"
        )

    async def test_ungrouped_row_column_still_splits(self, engine) -> None:
        """The DEV-1712 split-emission path must be untouched."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column="created_at", direction="asc")],
        )
        sql = await _sql(engine, query)
        parsed = _outermost_select(sql, dialect="sqlite")
        order = parsed.args.get("order")
        assert order is not None, sql
        col = order.expressions[0].this
        assert isinstance(col, exp.Column), sql
        assert col.table == "orders", sql

    async def test_unslottable_order_expression_raises_not_dropped(
        self, engine,
    ) -> None:
        """An ORDER BY expression with no materialisable slot must RAISE.

        The entry-point relaxation makes key shapes reachable that
        ``_iter_slot_deps`` treats as WHERE-inlined and never slots — a
        top-level ``IN`` predicate is the reachable one. Before this guard the
        order entry was silently discarded: the query ran UNSORTED and returned
        the wrong rows with no error, which is the same failure mode as the
        original ``change(...)`` bug.

        The guard is deliberately shape-agnostic (raise whenever the lookup
        misses) rather than an ``InKey``/``BetweenKey`` blocklist, so a future
        key kind cannot silently reintroduce the class.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum in (1, 2)", direction="desc")],
        )
        with pytest.raises(ValueError) as ei:
            await _sql(engine, query)
        assert "not supported" in str(ei.value), ei.value

    async def test_comparison_composite_order_still_works(self, engine) -> None:
        """Control for the guard above: a boolean composite that DOES slot
        (comparison / boolean arithmetic) must keep working, so the new raise
        cannot be over-broad."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(
                column="amount:sum > 1 and amount:sum < 500", direction="desc",
            )],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        _hidden_order_alias(sql)

    @pytest.mark.parametrize("formula", [
        "coalesce(amount:sum, id)",      # scalar call with a row-column arg
        "coalesce(amount:sum, created_at)",  # ... and a time-dimension arg
    ])
    async def test_scalar_call_with_row_operand_raises_not_stringified(
        self, engine, formula: str,
    ) -> None:
        """A row-column operand inside a scalar call must NOT be stringified.

        The scalar-call renderer's trailing ``else`` coerced anything it did
        not recognise with ``str(a)``, so a ``ColumnKey`` argument rendered as
        the SQL string literal ``'path=() leaf=''id'''`` — structurally valid
        SQL that silently compares an aggregate against a literal. The
        arithmetic path already raised for the same operand; both now behave
        the same.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column=formula, direction="desc")],
        )
        with pytest.raises(_ROW_OPERAND_REJECTED):
            await _sql(engine, query)

    async def test_arithmetic_with_row_operand_raises(self, engine) -> None:
        """The arithmetic sibling of the case above — pinned so the two paths
        cannot drift back apart."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / id", direction="desc")],
        )
        with pytest.raises(_ROW_OPERAND_REJECTED):
            await _sql(engine, query)

    def test_hidden_order_branch_rejects_unlisted_slot_kinds(self) -> None:
        """The generator's hidden-ORDER-BY branch must dispatch on an EXPLICIT
        set of key kinds — ``(AggregateKey, ArithmeticKey, ScalarCallKey,
        TransformKey)`` — not on "any hidden slot that happens to have a
        materialised alias".

        The behavioural rejections above (grouped row column, joined row
        column) are caught by plan-time validation, so they would stay green
        even if this branch were widened too far. This drives the generator
        directly with a hidden ROW slot that DOES carry an alias: the residual
        internal invariant must still fire. An over-broad relaxation would
        silently emit ``ORDER BY "orders.status"`` instead — a row column that
        is not in the GROUP BY.
        """

        slot = ValueSlot(
            id="s0",
            key=ColumnKey(path=(), leaf="status"),
            declared_name="status",
            hidden=True,
            phase=Phase.ROW,
        )
        planned = PlannedQuery(
            source_relation="orders",
            row_slots=[slot],
            order=[OrderEntry(
                slot_id="s0", direction="asc",
                # DEV-1747 §5.10 — classification is required on every entry.
                scope=OrderScope.HOST_BASE_HIDDEN, phase=Phase.ROW,
            )],
        )
        # Everything that can throw is built OUTSIDE the raises block, so the
        # only invocation under test is the call itself.
        generator = SQLGenerator(dialect="sqlite")
        select = exp.Select()
        with pytest.raises(NotImplementedError) as ei:
            generator._apply_planned_order_limit(
                select=select,
                planned_query=planned,
                source_relation="orders",
                slots_by_id={"s0": slot},
                source_model=None,
                bundle=None,
                aliases_by_slot_id={"s0": ["orders.status"]},
            )
        assert "hidden slot" in str(ei.value), ei.value

    async def test_local_aggregate_order_only_unchanged(self, engine) -> None:
        """The DEV-1712 hidden-aggregate contract must be untouched."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum", direction="desc")],
            limit=3,
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        assert _outer_order_by_names(sql) == ["orders.amount_sum"], sql


# ===========================================================================
# Group 9 — dialect coverage. Identifier quoting differs (MySQL backticks),
# so a string-built alias would silently degrade to a literal.
# ===========================================================================
class TestDialectEmission:
    @pytest.mark.parametrize(
        "ds_type,dialect", [
            ("postgres", "postgres"), ("mysql", "mysql"),
            ("duckdb", "duckdb"), ("clickhouse", "clickhouse"),
        ],
    )
    async def test_order_only_transform_quotes_per_dialect(
        self, tmp_path, ds_type: str, dialect: str,
    ) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(
            DatasourceConfig(name="test", type=ds_type, database="db", host="h"),
        )
        await _save_models(storage)
        eng = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="rank(amount:sum)", direction="desc")],
        )
        resp = await eng.execute(query, dry_run=True)
        sql = resp.sql or ""
        quote = "`" if dialect == "mysql" else '"'
        assert f"{quote}orders.status{quote}" in sql, sql
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        assert parsed.args.get("order") is not None, sql

    @pytest.mark.parametrize(
        "ds_type,dialect", [("postgres", "postgres"), ("duckdb", "duckdb")],
    )
    async def test_order_only_composite_quotes_per_dialect(
        self, tmp_path, ds_type: str, dialect: str,
    ) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(
            DatasourceConfig(name="test", type=ds_type, database="db", host="h"),
        )
        await _save_models(storage)
        eng = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / id:count", direction="desc")],
        )
        resp = await eng.execute(query, dry_run=True)
        sql = resp.sql or ""
        assert_scope_closed(sql, dialect=dialect)
        assert _outer_select_columns(sql, dialect=dialect) == [
            "orders.status", "orders._count",
        ], sql


# ===========================================================================
# Group 10 — execution. Both bugs this issue fixes emitted structurally valid
# SQL and returned WRONG NUMBERS, which only value assertions catch.
# ===========================================================================
class TestExecution:
    async def test_hidden_transform_collision_computes_correct_values(
        self, exec_engine,
    ) -> None:
        """B2 regression, the assertion that matters.

        Jan: cumsum(amount)=50, cumsum(count)=2 -> 52
        Feb: cumsum(amount)=75, cumsum(count)=4 -> 79
        With colliding aliases both operands resolved to cumsum(amount),
        yielding 100 / 150.
        """
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="cumsum(amount:sum) + cumsum(id:count)", name="both")],
        )
        resp = await exec_engine.execute(query)
        values = [float(r["orders.both"]) for r in resp.data]
        assert values == [52.0, 79.0], f"rows: {resp.data}"

    async def test_top_n_by_order_only_transform(self, exec_engine) -> None:
        """``rank(amount:sum)`` DESC: paid(50) ranks 1, open(25) ranks 2, so
        ordering by the RANK descending puts ``open`` first — the inverse of
        ordering by the amount itself, which proves the transform (not its
        operand) drives the sort."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="rank(amount:sum)", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert [r["orders.status"] for r in resp.data] == ["open", "paid"], resp.data

    async def test_top_n_by_order_only_composite(self, exec_engine) -> None:
        """paid: 50/2 = 25.0   open: 25/2 = 12.5 -> paid first on DESC."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / id:count", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert [r["orders.status"] for r in resp.data] == ["paid", "open"], resp.data

    async def test_composite_order_direction_is_honoured(self, exec_engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / id:count", direction="asc")],
        )
        resp = await exec_engine.execute(query)
        assert [r["orders.status"] for r in resp.data] == ["open", "paid"], resp.data

    async def test_order_only_change_orders_by_the_delta(self, exec_engine) -> None:
        """``change(amount:sum)``: January has no prior bucket so its delta is
        NULL; February's is 25 - 50 = -25.

        SLayer sorts NULLs LAST on every dialect (``OrderEntry.nulls`` default;
        the dialect strategy owns the spelling), so the NULL bucket is last in
        BOTH directions and February leads either way. That is a real claim
        rather than an accident of SQLite's native ordering, which puts NULLs
        first on ASC — the transform chain used to inherit that, so the same
        query sorted differently depending on whether it carried a transform.

        Non-vacuous because of the CONTROL: the same shape ordered by
        ``amount:sum`` DESC leads with January. A dropped ORDER BY cannot
        produce both leaders, so the sort demonstrably runs and reads the term
        the query asked for.
        """
        def _months(rows) -> list[str]:
            return [str(r["orders.created_at"])[:7] for r in rows]

        def _query(order: OrderItem) -> SlayerQuery:
            return SlayerQuery(
                source_model="orders",
                time_dimensions=_MONTH,
                measures=[ModelMeasure(formula="amount:sum")],
                order=[order],
            )

        resp_desc = await exec_engine.execute(
            _query(OrderItem(column="change(amount:sum)", direction="desc")),
        )
        assert _months(resp_desc.data) == ["2025-02", "2025-01"], resp_desc.data

        resp_asc = await exec_engine.execute(
            _query(OrderItem(column="change(amount:sum)", direction="asc")),
        )
        assert _months(resp_asc.data) == ["2025-02", "2025-01"], resp_asc.data
        assert set(resp_asc.columns) == {"orders.created_at", "orders.amount_sum"}

        resp_control = await exec_engine.execute(
            _query(OrderItem(column="amount_sum", direction="desc")),
        )
        assert _months(resp_control.data) == ["2025-01", "2025-02"], (
            resp_control.data
        )

    async def test_hidden_order_slots_stripped_from_response(self, exec_engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum / id:count", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert resp.columns == ["orders.status", "orders._count"], resp.columns
        for row in resp.data:
            assert set(row) == {"orders.status", "orders._count"}, row
        # ``attributes`` only carries entries for fields with display metadata,
        # so assert ABSENCE of the hidden operands (the DEV-1712 convention)
        # rather than an exact key set.
        keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        assert not any("amount_sum" in k or "id_count" in k for k in keys), keys
        assert keys <= {"orders.status", "orders._count"}, keys

    async def test_hidden_transform_order_slot_stripped_from_response(
        self, exec_engine,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="rank(amount:sum)", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert resp.columns == ["orders.status", "orders.amount_sum"], resp.columns
        assert all("rank" not in k for row in resp.data for k in row), resp.data

    async def test_order_only_windowed_executes_and_is_stripped(
        self, exec_engine,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count")],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert resp.columns == ["orders.created_at", "orders.id_count"], resp.columns
        assert len(resp.data) == 2, resp.data


# ===========================================================================
# Group 11 — multi-stage: the same shapes inside a downstream StageSchema.
# ===========================================================================
class TestMultiStage:
    async def test_downstream_stage_order_only_composite(self, exec_engine) -> None:
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="amount:sum", name="amt"),
                ModelMeasure(formula="*:count", name="cnt"),
            ],
        )
        outer = SlayerQuery(
            source_model="s1",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amt:sum", name="total")],
            order=[OrderItem(column="amt:sum / cnt:sum", direction="desc")],
        )
        resp = await exec_engine.execute(query=[inner, outer])
        assert resp.columns == ["s1.status", "s1.total"], resp.columns
        assert [r["s1.status"] for r in resp.data] == ["paid", "open"], resp.data

    async def test_downstream_stage_order_only_transform(self, exec_engine) -> None:
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum", name="amt")],
        )
        outer = SlayerQuery(
            source_model="s1",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amt:sum", name="total")],
            order=[OrderItem(column="rank(amt:sum)", direction="desc")],
        )
        resp = await exec_engine.execute(query=[inner, outer])
        assert resp.columns == ["s1.status", "s1.total"], resp.columns
        assert [r["s1.status"] for r in resp.data] == ["open", "paid"], resp.data

    async def test_downstream_stage_schema_excludes_hidden_order_slot(
        self, exec_engine,
    ) -> None:
        """A hidden order slot in the INNER stage must not surface in the
        stage schema the outer stage binds against."""
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count", name="cnt")],
            order=[OrderItem(column="amount:sum / id:count", direction="desc")],
        )
        outer = SlayerQuery(
            source_model="s1",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="cnt:sum", name="total")],
        )
        resp = await exec_engine.execute(query=[inner, outer])
        assert resp.columns == ["s1.status", "s1.total"], resp.columns

    async def test_hidden_windowed_order_slot_absent_from_stage_schema(
        self, exec_engine,
    ) -> None:
        """Law 2 for the windowed case: an order-only windowed target in the
        INNER stage must not become a column of the stage schema the outer
        stage binds against — the goal requires every hidden target to be
        stripped from response columns AND StageSchema."""
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count", name="n")],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        outer = SlayerQuery(
            source_model="s1",
            measures=[ModelMeasure(formula="n:sum", name="total")],
        )
        resp = await exec_engine.execute(query=[inner, outer])
        assert resp.columns == ["s1.total"], resp.columns
        assert int(resp.data[0]["s1.total"]) == 4, resp.data

    async def test_outer_stage_cannot_bind_hidden_windowed_order_slot(
        self, exec_engine,
    ) -> None:
        """The complement: the hidden windowed column is genuinely NOT in the
        stage schema, so a downstream reference to it fails to resolve rather
        than silently reading a leaked column."""
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(formula="id:count", name="n")],
            order=[OrderItem(column="amount:sum(window='90d')", direction="desc")],
        )
        outer = SlayerQuery(
            source_model="s1",
            measures=[ModelMeasure(formula="amount_sum_window_90d:sum", name="leaked")],
        )
        with pytest.raises(UnknownReferenceError) as ei:
            await exec_engine.execute(query=[inner, outer])
        # The message also names the stage's real column set, which is the
        # positive half of the assertion: the hidden windowed slot is absent.
        assert "amount_sum_window_90d" in str(ei.value), ei.value

    async def test_hidden_alias_rename_does_not_leak_into_stage_schema(
        self, exec_engine,
    ) -> None:
        """Plan-time hidden renaming must never reach the downstream binding
        surface — only public slots become stage columns."""
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            time_dimensions=_MONTH,
            measures=[ModelMeasure(
                formula="cumsum(amount:sum) + cumsum(id:count)", name="both",
            )],
        )
        outer = SlayerQuery(
            source_model="s1",
            measures=[ModelMeasure(formula="both:max", name="peak")],
        )
        resp = await exec_engine.execute(query=[inner, outer])
        assert resp.columns == ["s1.peak"], resp.columns
        assert float(resp.data[0]["s1.peak"]) == 79.0, resp.data


# ===========================================================================
# Group 9 — two DEV-1733 pins superseded by DEV-1703 Phase 1.
#
# DEV-1733 was written against a base that predates Phase 1, so it pinned the
# Stage-8 contract for two shapes Phase 1 had already changed on the parent
# branch. Both pins are INVERTED here rather than deleted (the same treatment
# DEV-1733 itself gave the two DEV-1501-era tests it superseded) so the
# supersession stays visible and neither contract can silently regress.
# ===========================================================================
class TestSupersededByDev1703Phase1:
    async def test_grouped_row_column_order_max_wraps(self, exec_engine) -> None:
        """Was ``test_grouped_row_column_order_still_raises`` (ValueError).

        Phase 1 replaced that rejection: a LOCAL row column in a GROUPED query
        materialises a hidden ``<col>:max`` aggregate and orders on its alias.
        MAX is order-preserving per group and portable across every Tier-1
        dialect. Pinned by EXECUTED VALUES, not SQL shape — the failure this
        guards against is a sort that silently does nothing.

        Seed: ``paid`` max(created_at) = 2025-01-02, ``open`` = 2025-02-02, so
        ``desc`` puts ``open`` first. Sorting by the wrong thing (or not at
        all) reverses or scrambles that.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="created_at", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert [r["orders.status"] for r in resp.data] == ["open", "paid"], resp.data
        # The sort key is hidden: it orders, but never surfaces as a column.
        assert resp.columns == ["orders.status", "orders._count"], resp.columns

    async def test_order_only_transform_in_raw_rows_mode_rejected(
        self, engine,
    ) -> None:
        """Was ``test_order_only_transform_in_raw_rows_mode`` (permissive).

        That test asserted the typed pipeline deliberately does NOT enforce
        DEV-1543's "no measure references with ``distinct_dimension_values=
        False``" rule, on the grounds that the check lived only in the legacy
        ``enrichment.py``. DEV-1703 restored the check on the typed path after
        the permissive behaviour was shown by execution to return WRONG ROW
        COUNTS (a 4-row table came back with 2 rows — the inner aggregate
        induces a GROUP BY, so the caller does not get the raw rows they
        asked for). Rejecting at compile time matches main.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column="rank(amount:sum)", direction="desc")],
        )
        with pytest.raises(DistinctDimensionValuesError) as ei:
            await _sql(engine, query)
        assert "rank(amount:sum)" in str(ei.value), ei.value
