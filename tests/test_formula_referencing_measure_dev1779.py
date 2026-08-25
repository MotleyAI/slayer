"""DEV-1779: a formula measure that references sibling saved measures must
emit valid SQL regardless of the order the measures appear in the query.

On main the root cause was an ordering asymmetry in the enrichment
provenance-merge: a formula enriched *before* the sibling measure it
references froze that sibling's canonical alias (``orders.id_count``) into
its expression SQL / transform input, and the later direct selection renamed
the base-CTE column to the declared name (``orders.order_count``) without
following the frozen reference — so the outer SELECT referenced a column no
CTE projected.

The DEV-1450 pipeline replaced order-dependent enrichment with structural
expansion (``measure_expansion.py``, key-based repointing via ``core/keys``,
cycle detection), so the frozen-alias hazard cannot arise: a saved-measure
reference is resolved to a key, not a rendered alias, and every projection
alias is derived from the plan at emission time. This suite verifies the
order-independence property end to end through the public engine.

Test groups:
  A  string-shape invariant over the measure-ordering matrix (dry-run SQL)
  B  end-to-end execution over the same matrix (temp-file SQLite)
  C  the same property through a transform-wrapped reference (cumsum / change_pct)
  D  full reported scenario: joined dimension + ORDER BY the formula
  E  bind-time guard: a formula referencing a *nonexistent* measure is
     rejected (typed error), never emitted as dangling SQL — the branch's
     stronger equivalent of main's generator defense-in-depth guard

Only the formula-first / formula-middle orderings reproduced the original
bug; the forward-order, single-ref, and no-ref cases are non-regression
controls (they assert the invariant is preserved).
"""

from __future__ import annotations

import re
import sqlite3

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import UnknownReferenceError
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# Canonical auto-aliases of the two aggregates the formula expands to. If
# either shows up in the SQL *referenced but not declared*, the bug is live.
_CANON_ORDER = "orders.id_count"
_CANON_UNIQUE = "orders.customer_count_distinct"


def _habit_measures() -> list[ModelMeasure]:
    """order_count / unique_customers, plus the formula that divides them."""
    return [
        ModelMeasure(name="order_count", formula="id:count"),
        ModelMeasure(name="unique_customers", formula="customer:count_distinct"),
        ModelMeasure(name="total_revenue", formula="revenue:sum"),
        ModelMeasure(name="habit_score", formula="order_count / unique_customers"),
    ]


def _orders_model(measures: list[ModelMeasure] | None = None) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer", sql="customer", type=DataType.TEXT),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="store_id", sql="store_id", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="stores", join_pairs=[["store_id", "id"]])],
        measures=_habit_measures() if measures is None else measures,
    )


def _stores_model() -> SlayerModel:
    return SlayerModel(
        name="stores",
        sql_table="stores",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    )


async def _make_engine(
    tmp_path, *, seed: bool, measures: list[ModelMeasure] | None = None
) -> SlayerQueryEngine:
    """Build a YAMLStorage-backed engine over the orders(+stores) model.

    ``seed=True`` materialises the SQLite tables for execution tests;
    ``seed=False`` builds only the datasource + models for dry-run SQL shape.
    """
    db_file = tmp_path / "slayer_test.db"
    if seed:
        conn = sqlite3.connect(db_file)
        conn.executescript(
            """
            CREATE TABLE stores (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO stores VALUES (1, 'North'), (2, 'South');
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY, customer TEXT, amount REAL,
                store_id INTEGER, created_at TEXT
            );
            -- North: 6 orders, 2 distinct customers → habit = 3
            INSERT INTO orders VALUES
                (1, 'A', 10, 1, '2026-01-01'),
                (2, 'A', 20, 1, '2026-01-02'),
                (3, 'A', 30, 1, '2026-01-03'),
                (4, 'B', 40, 1, '2026-02-01'),
                (5, 'B', 50, 1, '2026-02-02'),
                (6, 'B', 60, 1, '2026-02-03'),
            -- South: 2 orders, 2 distinct customers → habit = 1
                (7, 'C', 70, 2, '2026-01-01'),
                (8, 'D', 80, 2, '2026-02-01');
            -- Ungrouped: 8 orders, 4 distinct customers → habit = 2 (exact)
            """
        )
        conn.commit()
        conn.close()

    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=str(db_file))
    )
    await storage.save_model(_stores_model())
    await storage.save_model(_orders_model(measures))
    return SlayerQueryEngine(storage=storage)


# ---------------------------------------------------------------------------
# SQL-shape invariant helper
# ---------------------------------------------------------------------------


def _referenced_but_undeclared(sql: str) -> set[str]:
    """Generated aliases referenced in ``sql`` but never declared with ``AS``.

    Every ``"model.col"`` alias a SELECT/CTE references must be projected
    (declared ``AS "model.col"``) by some layer below it. A non-empty result
    means the SQL references a column no CTE produces — exactly the DEV-1779
    failure (``"orders.id_count"`` referenced, only ``"orders.order_count"``
    declared). Restricted to *dotted* quoted identifiers: SLayer aliases are
    always ``model.col`` (a dot), while base-table columns / physical
    identifiers are bare, so the dot filter avoids false-failing on a quoted
    physical name. This is a heuristic backstop; the execution tests are the
    authoritative check that the SQL is valid end to end.
    """
    declared = set(re.findall(r'AS "([^"]+)"', sql))
    referenced = {ref for ref in re.findall(r'"([^"]+)"', sql) if "." in ref}
    return referenced - declared


async def _dry_sql(tmp_path, measures: list[str], **query_kw: object) -> str:
    """Dry-run an orders query and return the emitted SQL."""
    engine = await _make_engine(tmp_path, seed=False)
    query = SlayerQuery(source_model="orders", measures=measures, **query_kw)
    resp = await engine.execute(query=query, dry_run=True)
    assert resp.sql is not None
    return resp.sql


# ---------------------------------------------------------------------------
# Group A — string-shape invariant over the measure-ordering matrix (dry-run)
# ---------------------------------------------------------------------------


# (id, label, reproduces_bug, both_refs_selected)
_ORDERINGS = [
    ("formula_first", ["habit_score", "order_count", "unique_customers"], True, True),
    ("formula_middle", ["order_count", "habit_score", "unique_customers"], True, True),
    ("formula_last", ["order_count", "unique_customers", "total_revenue", "habit_score"], False, True),
    ("one_ref_selected", ["order_count", "habit_score"], False, False),
    ("no_ref_selected", ["habit_score"], False, False),
]


@pytest.mark.parametrize(
    "label,measures,_bug,both_refs", _ORDERINGS, ids=[c[0] for c in _ORDERINGS]
)
async def test_formula_ref_sql_has_no_dangling_alias(
    tmp_path, label: str, measures: list[str], _bug: bool, both_refs: bool
) -> None:
    sql = await _dry_sql(tmp_path, measures)
    assert _referenced_but_undeclared(sql) == set(), sql
    if both_refs:
        # When both siblings are selected they are both renamed, so neither
        # canonical auto-alias may survive anywhere in the SQL.
        assert f'"{_CANON_ORDER}"' not in sql, sql
        assert f'"{_CANON_UNIQUE}"' not in sql, sql


# ---------------------------------------------------------------------------
# Group B/C/D — execution + join scenarios (temp-file SQLite)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "label,measures,_bug,_both", _ORDERINGS, ids=[c[0] for c in _ORDERINGS]
)
async def test_formula_ref_executes(
    tmp_path, label: str, measures: list[str], _bug: bool, _both: bool
) -> None:
    engine = await _make_engine(tmp_path, seed=True)
    query = SlayerQuery(source_model="orders", measures=measures)
    resp = await engine.execute(query=query)  # runs the real SQL — must not raise
    assert resp.data, resp.sql
    row = resp.data[0]
    # Single ungrouped bucket: order_count=8, unique_customers=4 → habit=2.
    # unique_customers is not always projected (hidden inside the formula for
    # one_ref/no_ref), so assert against the formula result directly.
    assert row["orders.habit_score"] == 2
    if "orders.order_count" in row:
        assert row["orders.order_count"] == 8


async def test_transform_wrapped_reference_follows_rename(tmp_path) -> None:
    """Group C: cumsum(order_count) with order_count selected AFTER — the
    hidden transform's input must follow the rename (not orphan)."""
    engine = await _make_engine(
        tmp_path,
        seed=False,
        measures=[
            ModelMeasure(name="order_count", formula="id:count"),
            ModelMeasure(name="running_orders", formula="cumsum(order_count)"),
        ],
    )
    query = SlayerQuery(
        source_model="orders",
        measures=["running_orders", "order_count"],
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
    )
    resp = await engine.execute(query=query, dry_run=True)
    assert resp.sql is not None
    assert _referenced_but_undeclared(resp.sql) == set(), resp.sql
    assert f'"{_CANON_ORDER}"' not in resp.sql, resp.sql


async def test_change_pct_desugar_reference_follows_rename(tmp_path) -> None:
    """Group C (desugaring): change_pct(order_count) desugars to an
    expression + a hidden time_shift, both referencing the inner measure.
    With order_count selected AFTER, both frozen carriers must follow the
    rename."""
    engine = await _make_engine(
        tmp_path,
        seed=False,
        measures=[
            ModelMeasure(name="order_count", formula="id:count"),
            ModelMeasure(name="mom_orders", formula="change_pct(order_count)"),
        ],
    )
    query = SlayerQuery(
        source_model="orders",
        measures=["mom_orders", "order_count"],
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
    )
    resp = await engine.execute(query=query, dry_run=True)
    assert resp.sql is not None
    assert _referenced_but_undeclared(resp.sql) == set(), resp.sql
    assert f'"{_CANON_ORDER}"' not in resp.sql, resp.sql


async def test_full_reported_scenario(tmp_path) -> None:
    """Group D: the exact shape from the ticket — joined ``stores.name``
    dimension, formula measure listed first, and ORDER BY the formula."""
    engine = await _make_engine(tmp_path, seed=True)
    query = SlayerQuery(
        source_model="orders",
        measures=["habit_score", "order_count", "unique_customers", "total_revenue"],
        dimensions=["stores.name"],
        order=[{"column": "habit_score", "direction": "desc"}],
        limit=100,
    )
    dry = await engine.execute(query=query, dry_run=True)
    assert dry.sql is not None
    assert _referenced_but_undeclared(dry.sql) == set(), dry.sql

    resp = await engine.execute(query=query)  # must execute cleanly
    assert [r["orders.stores.name"] for r in resp.data] == ["North", "South"]
    for r in resp.data:
        assert r["orders.habit_score"] * r["orders.unique_customers"] == (
            r["orders.order_count"]
        )
    assert resp.data[0]["orders.habit_score"] == 3  # North: 6 orders / 2 customers
    assert resp.data[1]["orders.habit_score"] == 1  # South: 2 orders / 2 customers


# ---------------------------------------------------------------------------
# Group E — bind-time guard (branch equivalent of main's generator guard)
# ---------------------------------------------------------------------------


async def test_formula_referencing_nonexistent_measure_raises(tmp_path) -> None:
    """A formula referencing a measure that does not exist is rejected at
    bind time (``UnknownReferenceError``) — never emitted as dangling SQL.
    On the DEV-1450 pipeline the guard fires strictly earlier than main's
    generator-level ValueError: the reference never resolves to a key."""
    engine = await _make_engine(
        tmp_path,
        seed=False,
        measures=[
            ModelMeasure(name="order_count", formula="id:count"),
            ModelMeasure(name="bad", formula="order_count / nonexistent_measure"),
        ],
    )
    query = SlayerQuery(source_model="orders", measures=["bad"])
    with pytest.raises(UnknownReferenceError) as exc:
        await engine.execute(query=query, dry_run=True)
    assert "nonexistent_measure" in str(exc.value)


async def test_transform_referencing_nonexistent_measure_raises(tmp_path) -> None:
    """Same guard through a transform wrapper: cumsum over a nonexistent
    measure is rejected at bind, not rendered with a dangling input."""
    engine = await _make_engine(
        tmp_path,
        seed=False,
        measures=[
            ModelMeasure(name="order_count", formula="id:count"),
            ModelMeasure(name="running_bad", formula="cumsum(nonexistent_measure)"),
        ],
    )
    query = SlayerQuery(
        source_model="orders",
        measures=["running_bad"],
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
    )
    with pytest.raises(UnknownReferenceError) as exc:
        await engine.execute(query=query, dry_run=True)
    assert "nonexistent_measure" in str(exc.value)
