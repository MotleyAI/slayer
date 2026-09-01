"""DEV-1712 Stage 8 — order-only hidden slots + plan-time validations.

Covers the typed-pipeline contract for ORDER BY refs that are NOT declared
as dimensions/measures, plus the ``rank(..., partition_by=X)`` grain guard.
The behaviour table (a ref reaching ORDER BY that matches no declared/public
slot):

  target (hidden)          | has_grouping = False (raw rows) | has_grouping = True (grouped)
  ------------------------ | ------------------------------- | ----------------------------------
  local aggregate          | works (agg induces grouping)    | materialise hidden, order, strip
  cross-model aggregate    | works                           | hidden CMA plan, trimmed, CTE order
  local row column         | split emission (orders.col)     | hidden <col>:max wrap, order, strip
  joined row column        | Law-1 join pull + split emission| UnresolvableOrderColumnError
  local derived, crossing  | UnresolvableOrderColumnError    | UnresolvableOrderColumnError
  transform (change/…)     | materialise hidden, order, strip| same (DEV-1733)

DEV-1703 Phase 1 replaced two of Stage 8's rejections with resolution: a
GROUPED local row column now MAX-wraps, and an UNGROUPED joined row column
now pulls its join (Law 1) and split-emits. The two rejections that remain
are deliberate: a GROUPED joined column has no host-rooted representation
today (an ``AggregateKey`` with a non-empty ``source.path`` always routes to
a TARGET-rooted CTE, which degenerates to a scalar CROSS JOIN — the sort
would silently do nothing), and a crossing local DERIVED column's join is
likewise never pulled. Both are DEV-1735.

``has_grouping`` = any aggregating measure OR (dims/time-dims present AND
``distinct_dimension_values``). An order-only aggregate never needs
rejection — it always materialises and orders. Composite arithmetic
(``a:sum / b:sum``) became expressible as an ``OrderItem.column`` in
DEV-1733; the full contract for transform / composite / windowed order
targets lives in ``tests/test_dev1733_order_only_transform_composite.py``.

Refs: DEV-1712 (this stage), DEV-1472 (order-only hidden slots), DEV-1495
bug 2 (cross-model leak — the canonical repro lives in
``tests/test_projection_trim.py``), DEV-1497 (partition_by guard), DEV-1645
(split-not-composite ORDER BY), DEV-1733 (transform/composite/windowed
order targets — the deferral this file's Group 8 used to pin).
"""
from __future__ import annotations

import re
import sqlite3

import pydantic
import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import DistinctDimensionValuesError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# SQL-inspection helpers (implementation-agnostic — walk the AST).
# ---------------------------------------------------------------------------
def _outermost_select(sql: str, *, dialect: str = "postgres") -> exp.Select:
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    assert isinstance(parsed, exp.Select), f"not a SELECT:\n{sql}"
    return parsed


def _outer_select_columns(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Alias names projected by the OUTERMOST SELECT."""
    parsed = _outermost_select(sql, dialect=dialect)
    return [proj.alias_or_name for proj in parsed.expressions]


def _all_aliases_in_sql(sql: str) -> list[str]:
    return re.findall(r'"([^"]+)"', sql)


def _outer_order_by_columns(sql: str, *, dialect: str = "postgres") -> list[tuple[str, str]]:
    """Return ``(table, name)`` for each Column in the outermost ORDER BY.

    A SPLIT reference (``orders.created_at``) parses to a Column with
    ``table='orders'`` and ``name='created_at'`` (no dot in the name). A
    COMPOSITE reference (the buggy ``"orders.created_at"`` single quoted
    token) parses to a Column with ``table=''`` and ``name`` containing a
    dot. The distinction is exactly the DEV-1645 Flavor-A fix.
    """
    parsed = _outermost_select(sql, dialect=dialect)
    order = parsed.args.get("order")
    if order is None:
        return []
    out: list[tuple[str, str]] = []
    for ordered in order.expressions:
        col = ordered.this
        if isinstance(col, exp.Column):
            out.append((col.table, col.name))
    return out


def _outer_order_by_names(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Leaf identifier names referenced by the outermost ORDER BY."""
    parsed = _outermost_select(sql, dialect=dialect)
    order = parsed.args.get("order")
    if order is None:
        return []
    names: list[str] = []
    for ordered in order.expressions:
        col = ordered.this
        names.append(col.name if isinstance(col, exp.Column) else col.sql(dialect=dialect))
    return names


# ---------------------------------------------------------------------------
# Fixtures — orders -> customers -> regions.
# ---------------------------------------------------------------------------
async def _save_models(storage: YAMLStorage) -> SlayerModel:
    await storage.save_model(SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.INT),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="revenue", sql="lifetime_revenue", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    ))
    # Second join target exposing the SAME leaf names (``region`` / ``revenue``)
    # as customers — used for the diamond same-leaf/same-agg cross-model test
    # (target-path identity) and the two-same-leaf-dims path.
    await storage.save_model(SlayerModel(
        name="suppliers", sql_table="suppliers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="revenue", sql="supplier_revenue", type=DataType.DOUBLE),
        ],
    ))
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="supplier_id", sql="supplier_id", type=DataType.INT),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            # Mixed-case physical identifier — exercises DEV-1645 split-quoting.
            Column(name="activity_ts", sql="ActivityTs", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            # DEV-1503 host-rooted isolation: a local aggregate source whose
            # ``filter`` references a joined table.
            Column(name="flagged_amount", sql="amount", type=DataType.DOUBLE,
                   filter="customers.region = 'West'"),
            # A LOCAL derived column whose ``sql`` crosses the customers join —
            # ``ColumnSqlKey(path=())`` but its expansion pulls a join.
            Column(name="cust_region", sql="customers.region", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="suppliers", join_pairs=[["supplier_id", "id"]]),
        ],
    )
    await storage.save_model(orders)
    return orders


@pytest.fixture
async def engine(tmp_path):
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    orders = await _save_models(storage)
    return SlayerQueryEngine(storage=storage), orders


async def _sql(engine_and_model, query: SlayerQuery) -> str:
    engine, _ = engine_and_model
    resp = await engine.execute(query, dry_run=True)
    return resp.sql or ""


@pytest.fixture
async def exec_engine(tmp_path):
    """On-disk SQLite seeded for execution / top-N ordering assertions."""
    db_path = tmp_path / "t.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER,
                                region TEXT, lifetime_revenue REAL);
        CREATE TABLE suppliers (id INTEGER PRIMARY KEY, region TEXT,
                                supplier_revenue REAL);
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER,
                             supplier_id INTEGER, status TEXT, created_at TEXT,
                             ActivityTs TEXT, amount REAL);
        INSERT INTO regions VALUES (10,'West'),(11,'East');
        INSERT INTO customers VALUES (100,10,'West',500.0),(101,11,'East',700.0);
        INSERT INTO suppliers VALUES (200,'West',1000.0),(201,'East',2000.0);
        INSERT INTO orders VALUES
            (1,100,200,'paid','2025-01-01','2025-01-05',10.0),
            (2,100,200,'paid','2025-01-02','2025-01-06',40.0),
            (3,101,201,'open','2025-02-01','2025-02-05',20.0),
            (4,101,201,'open','2025-02-02','2025-02-06',5.0);
        """
    )
    conn.commit()
    conn.close()
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=str(db_path))
    )
    await _save_models(storage)
    return SlayerQueryEngine(storage=storage)


# ===========================================================================
# Group 1 — local row column, ungrouped (dedup off) -> SPLIT emission.
# ===========================================================================
class TestUngroupedRowColumnSplit:
    async def test_ungrouped_row_column_order_emits_split(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="desc")],
        )
        sql = await _sql(engine, query)
        cols = _outer_order_by_columns(sql)
        assert cols, f"no ORDER BY column found.\nSQL:\n{sql}"
        table, name = cols[0]
        # SPLIT reference: qualified by the base table, leaf is a single
        # identifier — NOT the composite ``"orders.created_at"`` token.
        split_msg = f"ORDER BY must be the SPLIT reference orders.created_at, got '{table}.{name}'.\nSQL:\n{sql}"
        assert table == "orders", split_msg
        assert name == "created_at", split_msg
        assert "." not in name, split_msg
        assert '"orders.created_at"' not in sql, f"composite token leaked.\nSQL:\n{sql}"

    async def test_ungrouped_mixed_case_row_column_split_quoted(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column=ColumnRef(name="activity_ts"), direction="desc")],
        )
        sql = await _sql(engine, query)
        # The mixed-case physical identifier must be quoted (survive Postgres
        # case-folding) and referenced split (qualifier + quoted leaf) in the
        # ORDER BY itself — not merely present somewhere in the SQL.
        assert '"ActivityTs"' in sql, f"mixed-case leaf must be quoted.\nSQL:\n{sql}"
        cols = _outer_order_by_columns(sql)
        assert cols, f"no ORDER BY column found.\nSQL:\n{sql}"
        table, name = cols[0]
        mc_msg = f"ORDER BY must be the split reference orders.\"ActivityTs\", got '{table}.{name}'.\nSQL:\n{sql}"
        assert table == "orders", mc_msg
        assert name == "ActivityTs", mc_msg

    async def test_split_key_preserves_asc_and_limit(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
            limit=10,
        )
        sql = await _sql(engine, query)
        parsed = _outermost_select(sql)
        order = parsed.args.get("order")
        assert order is not None
        ordered = order.expressions[0]
        assert not ordered.args.get("desc"), f"expected ASC.\nSQL:\n{sql}"
        assert "LIMIT 10" in sql.upper() or parsed.args.get("limit") is not None


# ===========================================================================
# Group 2 — grouped row column -> plan-time ValueError (D-B).
# ===========================================================================
class TestGroupedLocalRowColumnMaxWrapped:
    """DEV-1703 Phase 1 supersedes Stage 8's rejection: a LOCAL row column
    ordered in a GROUPED query materialises as a hidden aggregate wrap and
    orders on that alias. The wrap is a hidden slot, so it is trimmed from the
    public projection.

    DEV-1747 D10 made the wrap DIRECTION-AWARE — ``MIN`` on ASC, ``MAX`` on
    DESC — so each group is ordered by the extreme the direction actually puts
    first. The tests below are DESC unless stated otherwise, hence MAX."""

    async def test_dedup_on_row_column_order_max_wraps(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],  # dedup ON (default) -> GROUP BY
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="desc")],
        )
        sql = await _sql(engine, query)
        assert re.search(r"MAX\(\s*orders\.created_at\s*\)", sql), sql
        # Ordered on the materialised alias, and the wrap is not public.
        assert "orders.created_at_max" in _all_aliases_in_sql(sql), sql
        assert "orders.created_at_max" not in _outer_select_columns(sql), sql
        assert _outer_select_columns(sql) == ["orders.status"], sql

    async def test_measures_present_row_column_order_max_wraps(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="desc")],
        )
        sql = await _sql(engine, query)
        assert re.search(r"MAX\(\s*orders\.created_at\s*\)", sql), sql
        # The declared measure still projects; only the sort wrap is hidden.
        assert "orders.amount_sum" in _outer_select_columns(sql), sql
        assert "orders.created_at_max" not in _outer_select_columns(sql), sql

    async def test_time_dimension_order_wraps_underlying_column(
        self, engine,
    ) -> None:
        """A ``TimeTruncKey`` is not a legal aggregate source, so the wrap
        targets the underlying column. ``DATE_TRUNC`` is monotonic
        non-decreasing, so ``MAX(col)`` orders each group identically to
        ``MAX(trunc(col))``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[
                OrderItem(
                    column=ColumnRef(name="created_at"), direction="desc",
                ),
            ],
        )
        sql = await _sql(engine, query)
        assert re.search(r"MAX\(\s*orders\.created_at\s*\)", sql), sql

    async def test_order_and_filter_on_same_column_share_one_wrap(
        self, engine,
    ) -> None:
        """The wrap is interned by structural key, so a column used by both a
        filter and the sort key does not mint two MAX slots."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            filters=["created_at > '2024-01-01'"],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="desc")],
        )
        sql = await _sql(engine, query)
        assert len(re.findall(r"MAX\(\s*orders\.created_at\s*\)", sql)) == 1, sql

    async def test_two_order_terms_wrap_independently(self, engine) -> None:
        """A wrapped row column and a declared measure coexist as sort terms,
        in the order given."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
            order=[
                OrderItem(column=ColumnRef(name="created_at"), direction="desc"),
                OrderItem(column=ColumnRef(name="rev"), direction="asc"),
            ],
        )
        sql = await _sql(engine, query)
        order_cols = [name for _, name in _outer_order_by_columns(sql)]
        assert order_cols == ["orders.created_at_max", "orders.rev"], sql

    async def test_wrap_bypasses_the_aggregation_gate(self, engine) -> None:
        """The hidden sort wrap is interned post-bind, so a column that does
        not whitelist ``max`` can still be sorted on — the caller asked to
        SORT, not to aggregate. ``id`` is a primary key (restricted to
        count/count_distinct by the bind-time gate)."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column=ColumnRef(name="id"), direction="asc")],
        )
        sql = await _sql(engine, query)
        # DEV-1747 D10 — the wrap is direction-aware: ASC sorts each group by
        # its MINIMUM. The point of this test is the aggregation GATE, which
        # the wrap bypasses regardless of which extreme it takes.
        assert re.search(r"MIN\(\s*orders\.id\s*\)", sql), sql


# ===========================================================================
# Group 3 — joined row column -> Law-1 join pull / host-rooted CTE (resolves).
# ===========================================================================
class TestJoinedRowColumnResolved:
    async def test_joined_row_column_ungrouped_pulls_join_and_splits(
        self, engine,
    ) -> None:
        """DEV-1703 Phase 1: in a RAW-ROWS query the row IS the grain, so a
        joined sort key is legal as a bare reference. Law 1 pulls its join into
        the base FROM — exactly as a filter ref would — and the ORDER BY emits
        the SPLIT ``customers.region`` form under the path alias."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column=ColumnRef(name="customers.region"), direction="desc")],
        )
        sql = await _sql(engine, query)
        # Law 1: the join is bound in the base FROM.
        assert "JOIN" in sql.upper(), sql
        assert re.search(r"customers\b", sql), sql
        # Split reference, never the composite single-token form.
        assert ("customers", "region") in _outer_order_by_columns(sql), sql
        assert '"customers.region"' not in sql, sql
        # The sort key is not projected — ordering must not change the shape.
        assert _outer_select_columns(sql) == ["orders.status"], sql

    async def test_joined_row_column_grouped_resolves_host_rooted(
        self, engine,
    ) -> None:
        """DEV-1747 D2: the GROUPED shape used to be rejected outright, because
        a path-bearing aggregate source always routed to a TARGET-rooted CTE —
        a scalar CROSS JOIN that gives every group the same global value.

        It now resolves via a HOST-rooted CTE: the crossed join is pulled
        INSIDE, the CTE groups on the query grain, and the host base joins the
        per-group extreme back. The sort key is still not projected."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column=ColumnRef(name="customers.region"), direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status", "orders._count"], sql
        assert "WITH" in sql.upper(), sql
        # DESC takes each group's MAXIMUM (D10) of the joined column.
        assert re.search(r"MAX\(\s*customers\.region\s*\)", sql), sql

    async def test_ungrouped_order_by_derived_crossing_column_resolves(
        self, engine,
    ) -> None:
        """A hidden order-only LOCAL DERIVED column whose ``Column.sql`` crosses
        a join (``cust_region`` = ``customers.region``) used to be rejected: it
        is not projected, so its join was never pulled into the base FROM and
        the sort term would have been unbound.

        DEV-1747 D9 pulls it. Law 1 applies to a sort key exactly as it does to
        a filter ref, so the join is bound and the term emits the same split
        reference the BARE joined sort key emits — the two spellings name one
        column, which is the consistency DEV-1735 asked for. Pinned positively
        against the bare form rather than against a literal, so the two cannot
        drift apart again."""
        def _q(column: ColumnRef) -> SlayerQuery:
            return SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                distinct_dimension_values=False,
                order=[OrderItem(column=column, direction="desc")],
            )

        derived_sql = await _sql(engine, _q(ColumnRef(name="cust_region")))
        bare_sql = await _sql(engine, _q(ColumnRef(name="customers.region")))

        assert ("customers", "region") in _outer_order_by_columns(derived_sql), (
            derived_sql
        )
        assert (
            _outer_order_by_columns(derived_sql)
            == _outer_order_by_columns(bare_sql)
        ), f"derived:\n{derived_sql}\nbare:\n{bare_sql}"
        # Law 1 bound the join, and the sort key is still not projected.
        assert re.search(r"(?i)\bJOIN\b", derived_sql), derived_sql
        assert _outer_select_columns(derived_sql) == ["orders.status"], derived_sql

    async def test_joined_order_ref_colliding_local_leaf_stays_joined(
        self, tmp_path,
    ) -> None:
        """A joined order ref whose LEAF collides with a local declared
        dimension (``owners.status`` vs a local ``status``) must NOT silently
        bind to the local column and sort by the wrong field (Codex /
        DEV-1712).

        DEV-1747 D2 turned the rejection into a resolution, so the guarantee is
        now pinned positively: the emitted SQL must actually reach ``owners``.
        A silent rebind to the local column would leave it absent."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(
            DatasourceConfig(name="test", type="sqlite", database=":memory:")
        )
        await storage.save_model(SlayerModel(
            name="owners", sql_table="owners", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="owner_id", sql="owner_id", type=DataType.INT),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="owners", join_pairs=[["owner_id", "id"]])],
        ))
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],  # local status
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="owners.status", direction="desc")],  # joined
        )
        resp = await engine.execute(query, dry_run=True)
        order_cols = _outer_order_by_columns(resp.sql or "")
        assert any(
            "owners" in table or "owners" in name for table, name in order_cols
        ), (
            f"joined order ref silently bound to the local column — the outer "
            f"ORDER BY does not reference owners.\nSQL:\n{resp.sql}"
        )


# ===========================================================================
# Group 4 — local aggregate hidden order (regression; must stay green).
# ===========================================================================
class TestLocalAggregateHiddenOrder:
    async def test_local_aggregate_order_only_hidden_and_ordered(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum", direction="desc")],
            limit=3,
        )
        sql = await _sql(engine, query)
        outer = _outer_select_columns(sql)
        assert outer == ["orders.status", "orders._count"], (
            f"hidden amount_sum must not project.\ngot: {outer}\nSQL:\n{sql}"
        )
        assert "orders.amount_sum" in _all_aliases_in_sql(sql)
        assert _outer_order_by_names(sql) == ["orders.amount_sum"], (
            f"outer ORDER BY must name the hidden alias.\nSQL:\n{sql}"
        )

    async def test_ungrouped_input_local_aggregate_order_rejected(self, engine) -> None:
        """``distinct_dimension_values=False`` asks for RAW ROWS, so ordering
        by an aggregate is a contradiction and is rejected (DEV-1543).

        Stage 8 originally let this through: the aggregate induced a GROUP BY
        on the declared dims, so a 4-row table came back as 2 rows — silently
        performing exactly the dedup the flag turns off. The compile-time half
        of the DEV-1543 contract had no owner on the typed pipeline at the
        time; DEV-1703 Phase 3 restored it. Honouring BOTH (raw rows ordered
        by a per-group aggregate, via SUM(...) OVER (PARTITION BY ...)) is
        DEV-1736."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column="amount:sum", direction="desc")],
        )
        with pytest.raises(DistinctDimensionValuesError):
            await _sql(engine, query)

    async def test_dev1472_joined_dim_repro(self, engine) -> None:
        """The literal DEV-1472 repro — joined dim + *:count + order by a
        local aggregate. Already valid single-stage; pinned as regression."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.region")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum")],
            limit=10,
        )
        sql = await _sql(engine, query)
        outer = _outer_select_columns(sql)
        assert outer == ["orders.customers.region", "orders._count"], (
            f"got: {outer}\nSQL:\n{sql}"
        )
        assert "orders.amount_sum" in _all_aliases_in_sql(sql)

    async def test_order_matches_declared_measure_no_hidden_slot(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
            order=[OrderItem(column="rev", direction="desc")],
            limit=5,
        )
        sql = await _sql(engine, query)
        # Declared alias -> composite quoted reference is correct (it IS the
        # projected output column ``AS "orders.rev"``); no outer trim wrapper.
        assert '"orders.rev"' in sql
        assert _outer_select_columns(sql) == ["orders.status", "orders.rev"]

    async def test_declared_transform_plus_hidden_agg_order(self, engine) -> None:
        """A declared rank() transform plus an order-only hidden aggregate:
        the hidden aggregate threads through the transform chain and orders
        at the outer wrap. Codex F7 positive case — declared transforms must
        NOT disable a valid hidden aggregate order target."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="rank(amount:sum)", name="rk")],
            order=[OrderItem(column="id:count", direction="desc")],
        )
        sql = await _sql(engine, query)
        outer = _outer_select_columns(sql)
        assert outer == ["orders.status", "orders.rk"], (
            f"hidden id_count must not project.\ngot: {outer}\nSQL:\n{sql}"
        )
        assert _outer_order_by_names(sql) == ["orders.id_count"]

    async def test_hidden_cross_model_agg_order_in_cumsum_chain(self, engine) -> None:
        """DEV-1733 shape 3: an order-only CROSS-MODEL aggregate inside a
        transform-chain-wrapped query threads through the chain layers and
        orders at the outer wrap. The hidden CMA stays projected through the
        chain (the step CTE has to carry it) and the transform outer wrap does
        the public-vs-hidden trim."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
            measures=[ModelMeasure(formula="cumsum(amount:sum)", name="cs")],
            order=[OrderItem(column="customers.revenue:sum", direction="desc")],
        )
        sql = await _sql(engine, query)
        outer = _outer_select_columns(sql)
        assert outer == ["orders.created_at", "orders.cs"], (
            f"hidden cross-model agg must not project.\ngot: {outer}\nSQL:\n{sql}"
        )
        assert _outer_order_by_names(sql) == ["customers.revenue_sum"], sql

    async def test_hidden_local_agg_order_in_change_chain(self, engine) -> None:
        """Same shape-3 contract, but the chain is a time_shift self-join pair
        (``change``) rather than a plain window step — a different
        carry-forward path (``sjoin_`` carries the hidden alias forward).

        The order target is a LOCAL aggregate: ``time_shift`` combined with a
        CROSS-MODEL aggregate is an independent deferral (DEV-1450 stage
        7b.15e) that rejects the declared-measure form too, so it is not a
        shape DEV-1733 can exercise.
        """
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
            measures=[ModelMeasure(formula="change(amount:sum)", name="ch")],
            order=[OrderItem(column="id:count", direction="desc")],
        )
        sql = await _sql(engine, query)
        outer = _outer_select_columns(sql)
        assert outer == ["orders.created_at", "orders.ch"], (
            f"hidden local agg must not project.\ngot: {outer}\nSQL:\n{sql}"
        )
        assert _outer_order_by_names(sql) == ["orders.id_count"], sql

    async def test_hidden_local_agg_order_in_cross_model_transform_chain(
        self, engine,
    ) -> None:
        """The chain is rooted on a CROSS-MODEL combined SELECT
        (``cumsum(customers.revenue:sum)``); a hidden LOCAL order aggregate
        must still thread through it."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
            measures=[ModelMeasure(formula="cumsum(customers.revenue:sum)", name="ccr")],
            order=[OrderItem(column="id:count", direction="desc")],
        )
        sql = await _sql(engine, query)
        outer = _outer_select_columns(sql)
        assert outer == ["orders.created_at", "orders.ccr"], (
            f"hidden local agg must not project.\ngot: {outer}\nSQL:\n{sql}"
        )
        assert _outer_order_by_names(sql) == ["orders.id_count"], sql


# ===========================================================================
# Group 5 — execution: top-N by a hidden metric + response-column strip.
# ===========================================================================
class TestExecutionHiddenOrder:
    async def test_execute_top_n_by_hidden_aggregate(self, exec_engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        # paid: 10+40=50, open: 20+5=25 -> paid first (desc).
        statuses = [r["orders.status"] for r in resp.data]
        assert statuses == ["paid", "open"], f"rows: {resp.data}"
        # Hidden order slot must be stripped from the response columns.
        assert "orders.amount_sum" not in resp.columns, f"columns: {resp.columns}"
        assert all("amount_sum" not in k for k in resp.data[0]), f"row: {resp.data[0]}"

    async def test_execute_grouped_row_column_max_wrap_orders_correctly(
        self, exec_engine,
    ) -> None:
        """DEV-1703 Phase 1, executed: the MAX wrap must produce the RIGHT
        order, not merely valid SQL. Latest ``created_at`` per status —
        open (2025-02-02) sorts before paid (2025-01-02) descending — and the
        wrap must not leak into the response or change the row count."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert [r["orders.status"] for r in resp.data] == ["open", "paid"], resp.data
        # Grain preserved: one row per status, not one per (status, created_at).
        assert len(resp.data) == 2, resp.data
        # Sibling measure values are unaffected by the sort key.
        by_status = {r["orders.status"]: r["orders.amount_sum"] for r in resp.data}
        assert by_status == {"paid": 50.0, "open": 25.0}, resp.data
        # The wrap is internal — never surfaced.
        assert all("created_at_max" not in k for k in resp.columns), resp.columns

    async def test_execute_ungrouped_joined_order_pulls_join_and_orders(
        self, exec_engine,
    ) -> None:
        """A raw-rows query sorted by a joined column executes and orders by
        that column's real values (Law-1 pull), without projecting it."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="id"), ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[
                OrderItem(column=ColumnRef(name="customers.region"), direction="asc"),
                OrderItem(column=ColumnRef(name="id"), direction="asc"),
            ],
        )
        resp = await exec_engine.execute(query)
        # customers.region: orders 3,4 -> 'East'; orders 1,2 -> 'West'.
        assert [r["orders.id"] for r in resp.data] == [3, 4, 1, 2], resp.data
        # Row count unchanged by the join pull, sort key not projected.
        assert len(resp.data) == 4, resp.data
        assert all("region" not in k for k in resp.columns), resp.columns

    async def test_hidden_order_slot_absent_from_attributes(self, exec_engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="amount:sum", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        assert not any("amount_sum" in k for k in keys), f"attributes leaked: {keys}"


# ===========================================================================
# Group 6 — cross-model hidden aggregate order (DEV-1495 bug 2 neighbourhood).
# ===========================================================================
class TestCrossModelHiddenOrder:
    async def test_two_hidden_cross_model_aggs_distinct_ctes(self, engine) -> None:
        """Two order-only cross-model aggregates over the same leaf but
        different aggregations (``customers.revenue:sum`` and
        ``customers.revenue:max``) get DISTINCT hidden CTE aliases; neither
        leaks into the outer projection; the outer ORDER BY names them
        unambiguously."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[
                OrderItem(column="customers.revenue:sum", direction="desc"),
                OrderItem(column="customers.revenue:max", direction="asc"),
            ],
            limit=3,
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status"], (
            f"cross-model aggregates must be hidden.\nSQL:\n{sql}"
        )
        aliases = _all_aliases_in_sql(sql)
        assert any("revenue_sum" in a for a in aliases), f"aliases: {aliases}\n{sql}"
        assert any("revenue_max" in a for a in aliases), f"aliases: {aliases}\n{sql}"
        order_names = _outer_order_by_names(sql)
        assert any("revenue_sum" in n for n in order_names), order_names
        assert any("revenue_max" in n for n in order_names), order_names
        # No inline aggregate call in the outermost ORDER BY.
        parsed = _outermost_select(sql)
        order = parsed.args.get("order")
        if order is not None:
            assert "SUM(" not in order.sql(dialect="postgres").upper()

    async def test_cross_model_hidden_order_postgres_shape(self, engine) -> None:
        """Postgres-dialect shape (F9): the hidden cross-model aggregate lives
        in a CTE, is absent from the outer projection, and the ORDER BY names
        its alias with no inline aggregate."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column="customers.revenue:sum", direction="desc")],
            limit=3,
        )
        engine_obj, _ = engine
        resp = await engine_obj.execute(query, dry_run=True)
        sql = resp.sql or ""
        # sanity: parses as postgres
        sqlglot.parse_one(sql, dialect="postgres")
        assert "revenue_sum" in "".join(_all_aliases_in_sql(sql))
        assert _outer_select_columns(sql) == ["orders.status"]

    async def test_diamond_same_leaf_same_agg_distinct_targets(self, engine) -> None:
        """Diamond: two DIFFERENT join targets (``customers`` and
        ``suppliers``) exposing the SAME leaf under the SAME aggregation
        (``customers.revenue:sum`` vs ``suppliers.revenue:sum``). Target-path
        identity must keep them DISTINCT — distinct CTE aliases, both hidden,
        both named unambiguously in the ORDER BY. (Codex F4/F6: guards against
        an AggregateKey identity that omits the target path.)"""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[
                OrderItem(column="customers.revenue:sum", direction="desc"),
                OrderItem(column="suppliers.revenue:sum", direction="asc"),
            ],
            limit=3,
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.status"], (
            f"both cross-model aggregates must be hidden.\nSQL:\n{sql}"
        )
        aliases = "\n".join(_all_aliases_in_sql(sql))
        assert "customers.revenue_sum" in aliases or "customers__revenue_sum" in aliases, aliases
        assert "suppliers.revenue_sum" in aliases or "suppliers__revenue_sum" in aliases, aliases
        order_names = _outer_order_by_names(sql)
        assert any("customers" in n and "revenue_sum" in n for n in order_names), order_names
        assert any("suppliers" in n and "revenue_sum" in n for n in order_names), order_names

    async def test_ungrouped_input_cross_model_aggregate_order_rejected(
        self, engine,
    ) -> None:
        """Same DEV-1543 contradiction as the local case, via a cross-model
        aggregate: raw rows were requested, so a measure sort key is rejected
        rather than silently re-introducing the grouping."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column="customers.revenue:sum", direction="desc")],
        )
        with pytest.raises(DistinctDimensionValuesError):
            await _sql(engine, query)

    async def test_execute_cross_model_hidden_order_stripped(self, exec_engine) -> None:
        """Execution: an order-only cross-model aggregate is absent from the
        response columns, row keys, and attribute metadata — pinning the
        hidden order-only producer-placeholder path at the response level,
        distinct from the local-aggregate path."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="customers.revenue:sum", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert all("revenue_sum" not in c for c in resp.columns), f"columns: {resp.columns}"
        assert all("revenue_sum" not in k for k in resp.data[0]), f"row: {resp.data[0]}"
        keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        assert not any("revenue_sum" in k for k in keys), f"attributes leaked: {keys}"


# ===========================================================================
# Group 7 — partition_by grain guard (DEV-1497).
# ===========================================================================
class TestPartitionByGuard:
    async def test_partition_by_bare_dim_accepted(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="rank(amount:sum, partition_by=status)", name="rk")],
        )
        sql = await _sql(engine, query)
        assert "PARTITION BY" in sql.upper()
        # grain must NOT widen: the only GROUP BY key is status.
        assert "customer_id" not in sql

    async def test_partition_by_qualified_dim_accepted(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="rank(amount:sum, partition_by=orders.status)", name="rk")],
        )
        sql = await _sql(engine, query)
        assert "PARTITION BY" in sql.upper()

    async def test_partition_by_dotted_joined_dim_accepted(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.region")],
            measures=[ModelMeasure(
                formula="rank(amount:sum, partition_by=customers.region)", name="rk")],
        )
        sql = await _sql(engine, query)
        assert "PARTITION BY" in sql.upper()

    async def test_partition_by_time_dimension_uses_bucket(self, engine) -> None:
        """partition_by naming a query time-dimension must partition by the
        TRUNCATED bucket, not the raw column — and must not add the raw
        column to GROUP BY (grain widening) nor emit a duplicate alias."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
            measures=[ModelMeasure(
                formula="rank(amount:sum, partition_by=created_at)", name="rk")],
        )
        sql = await _sql(engine, query)
        # (a) No column in the base SELECT is a raw bare-column projection of
        # created_at — the DEV-1497 grain-widening signature was a second
        # ``orders.created_at AS "orders.created_at"`` projection (raw column)
        # alongside the STRFTIME/DATE_TRUNC bucket, plus a raw GROUP BY key.
        tree = sqlglot.parse_one(sql, dialect="sqlite")
        base_cte0 = next(
            (cte.this for cte in tree.find_all(exp.CTE) if cte.alias == "base"), None
        )
        assert base_cte0 is not None, f"expected a `base` CTE.\nSQL:\n{sql}"
        raw_created_projections = [
            p for p in base_cte0.expressions
            if isinstance(p.unalias(), exp.Column) and p.unalias().name == "created_at"
        ]
        assert not raw_created_projections, (
            f"base SELECT projects the RAW created_at column alongside the "
            f"bucket — grain widened.\nSQL:\n{sql}"
        )
        # (b) The base GROUP BY must contain a TRUNCATION expression for
        # created_at (the bucket), never a bare column reference to the raw
        # timestamp. On SQLite that truncation is STRFTIME / DATE.
        base_cte = next(
            (cte.this for cte in tree.find_all(exp.CTE) if cte.alias == "base"), None
        )
        assert base_cte is not None, f"expected a `base` CTE.\nSQL:\n{sql}"
        group = base_cte.args.get("group")
        assert group is not None, f"base has no GROUP BY.\nSQL:\n{sql}"
        group_exprs = group.expressions
        # No GROUP BY key may be a bare column whose name is the raw created_at.
        for g in group_exprs:
            assert not (isinstance(g, exp.Column) and g.name == "created_at"), (
                f"GROUP BY includes the RAW created_at column — grain widened "
                f"past the month bucket.\nSQL:\n{sql}"
            )
        # (c) The RANK() PARTITION BY must reference the bucket alias, not add
        # an independent partition column.
        window = next(iter(tree.find_all(exp.Window)), None)
        assert window is not None, f"no window function found.\nSQL:\n{sql}"
        partition = window.args.get("partition_by") or []
        assert partition, f"RANK() has no PARTITION BY.\nSQL:\n{sql}"
        part_sql = " ".join(p.sql(dialect="sqlite") for p in partition)
        assert "created_at" in part_sql, f"PARTITION BY lost the bucket.\nSQL:\n{sql}"

    async def test_partition_by_non_dim_raises_rank(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="amount:sum"),
                ModelMeasure(formula="rank(amount:sum, partition_by=customer_id)", name="rk"),
            ],
        )
        with pytest.raises(ValueError) as ei:
            await _sql(engine, query)
        msg = str(ei.value)
        assert "partition_by" in msg
        assert "customer_id" in msg
        assert "rank" in msg.lower(), f"error should name the transform: {msg}"
        assert "status" in msg, "error should list available dimensions"

    async def test_partition_by_ambiguous_time_dim_granularity_raises(self, engine) -> None:
        """A source column carried by two time-dimension granularities
        (``created_at`` at month AND day) makes a bare ``partition_by=created_at``
        ambiguous — reject rather than silently pick a bucket (CodeRabbit / T1)."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension="created_at", granularity="month"),
                TimeDimension(dimension="created_at", granularity="day"),
            ],
            measures=[ModelMeasure(
                formula="rank(amount:sum, partition_by=created_at)", name="rk")],
        )
        with pytest.raises(ValueError) as ei:
            await _sql(engine, query)
        assert "ambiguous" in str(ei.value).lower()

    async def test_partition_by_same_granularity_time_dim_not_flagged_ambiguous(self, engine) -> None:
        """Two time-dimension declarations at the SAME granularity are one
        bucket, not competing granularities — the partition-ambiguity guard must
        NOT misfire (Codex round 4). The query is still invalid (the two
        ``created_at`` columns collide on their downstream name), but the error
        must be that collision, not a bogus 'ambiguous granularities'."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension="created_at", granularity="month", label="A"),
                TimeDimension(dimension="created_at", granularity="month", label="B"),
            ],
            measures=[ModelMeasure(
                formula="rank(amount:sum, partition_by=created_at)", name="rk")],
        )
        with pytest.raises(ValueError) as ei:
            await _sql(engine, query)
        assert "ambiguous" not in str(ei.value).lower()

    async def test_partition_by_non_dim_raises_ntile(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(
                formula="ntile(amount:sum, n=4, partition_by=customer_id)", name="q")],
        )
        with pytest.raises(ValueError) as ei:
            await _sql(engine, query)
        msg = str(ei.value)
        assert "customer_id" in msg
        assert "ntile" in msg.lower(), f"error should name the transform: {msg}"


# ===========================================================================
# Group 8 — transform / composite order targets: SUPPORTED as of DEV-1733.
#
# Stage 8 rejected these with an actionable ValueError and pinned the future
# contract as a strict xfail. DEV-1733 landed hidden TransformKey / composite
# materialisation, so the xfail promoted and the rejection tests inverted.
# The full contract lives in tests/test_dev1733_order_only_transform_composite.py;
# what stays here is the Stage-8 boundary, now asserting support.
# ===========================================================================
class TestTransformOrderSupported:
    async def test_transform_order_only_materializes(self, engine) -> None:
        """Promoted from ``test_transform_order_only_materializes_FUTURE`` — the
        change() value is materialised (hidden) and the outermost ORDER BY
        references it: not dropped, not projected."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="change(amount:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.amount_sum"]
        assert _outer_order_by_names(sql), f"ORDER BY must be present.\nSQL:\n{sql}"

    async def test_cumsum_order_only_materializes(self, engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
            measures=[ModelMeasure(formula="amount:sum")],
            order=[OrderItem(column="cumsum(amount:sum)", direction="desc")],
        )
        sql = await _sql(engine, query)
        assert _outer_select_columns(sql) == ["orders.created_at", "orders.amount_sum"]
        names = _outer_order_by_names(sql)
        assert names, f"ORDER BY must be present.\nSQL:\n{sql}"
        assert names[0] not in _outer_select_columns(sql), (
            f"the cumsum must be ordered on but not projected.\nSQL:\n{sql}"
        )

    def test_composite_order_string_now_accepted_at_construction(self) -> None:
        """Replaces ``test_composite_order_string_rejected_at_construction``.

        DEV-1733 relaxed the entry point: a composite carrying a colon-form
        aggregation becomes a formula (placeholder ColumnRef + ``raw_formula``).
        A composite over declared measure ALIASES carries no colon and is still
        rejected at construction, because alias references inside expressions
        are unsupported everywhere in SLayer.
        """
        item = OrderItem(column="amount:sum / id:count", direction="desc")
        assert item.column.name == "_expr_pending"
        assert item.raw_formula == "amount:sum / id:count"
        with pytest.raises(pydantic.ValidationError):
            OrderItem(column="rev / cnt", direction="desc")


# ===========================================================================
# Group 9 — host-rooted isolated aggregate, order-only (DEV-1503 x Law 2).
# ===========================================================================
class TestHostRootedIsolatedHiddenOrder:
    async def test_order_only_filtered_local_aggregate_hidden(self, engine) -> None:
        """A local aggregate whose ``Column.filter`` crosses a join
        (``flagged_amount`` filtered on ``customers.region``) is host-rooted
        isolated. Used order-only it must stay hidden — not projected in the
        outer SELECT — while still driving the ORDER BY."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column="flagged_amount:sum", direction="desc")],
            limit=3,
        )
        sql = await _sql(engine, query)
        outer = _outer_select_columns(sql)
        assert outer == ["orders.status", "orders._count"], (
            f"filtered-local aggregate must be hidden.\ngot: {outer}\nSQL:\n{sql}"
        )
        assert any("flagged_amount_sum" in a for a in _all_aliases_in_sql(sql))
