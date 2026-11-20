"""An expression metric that references sibling saved metrics must
emit valid SQL regardless of the order the metrics appear in the query.

On main the root cause was an ordering asymmetry in the enrichment
provenance-merge: an expression enriched *before* the sibling metric it
references froze that sibling's canonical alias (``orders.id_count``) into
its expression SQL / transform input, and the later direct selection renamed
the base-CTE column to the declared name (``orders.order_count``) without
following the frozen reference — so the outer SELECT referenced a column no
CTE projected.

The typed pipeline replaced order-dependent enrichment with structural
expansion (``measure_expansion.py``, key-based repointing via ``core/keys``,
cycle detection), so the frozen-alias hazard cannot arise: a saved-metric
reference is resolved to a key, not a rendered alias, and every projection
alias is derived from the plan at emission time. This suite verifies the
order-independence property end to end through the public engine.

Test groups:
  A  string-shape invariant over the metric-ordering matrix (dry-run SQL)
  B  end-to-end execution over the same matrix (temp-file SQLite)
  C  the same property through a transform-wrapped reference (cumsum / change_pct)
  D  full reported scenario: joined dimension + ORDER BY the expression
  E  bind-time guard: an expression referencing a *nonexistent* metric is
     rejected (typed error), never emitted as dangling SQL — the branch's
     stronger equivalent of main's generator defense-in-depth guard

Only the expression-first / expression-middle orderings reproduced the original
bug; the forward-order, single-ref, and no-ref cases are non-regression
controls (they assert the invariant is preserved).
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import UnknownReferenceError
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

# Canonical auto-aliases of the two aggregates the expression expands to. If
# either shows up in the SQL *referenced but not declared*, the bug is live.
_CANON_ORDER = "orders.id_count"
_CANON_UNIQUE = "orders.customer_count_distinct"


def _habit_metrics() -> list[DatasetMetric]:
    """order_count / unique_customers, plus the expression that divides them."""
    return [
        DatasetMetric(name="order_count", expression="count(id)"),
        DatasetMetric(name="unique_customers", expression="count_distinct(customer)"),
        DatasetMetric(name="total_revenue", expression="sum(revenue)"),
        DatasetMetric(name="habit_score", expression="order_count / unique_customers"),
    ]


def _orders_dataset(metrics: list[DatasetMetric] | None = None) -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="test",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer", sql="customer", datatype=DataType.STRING),
            DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT),
            DatasetField(name="store_id", sql="store_id", datatype=DataType.FLOAT),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
        ],
        relationships=[DatasetRelationship(to="stores", from_columns=["store_id"], to_columns=["id"])],
        metrics=_habit_metrics() if metrics is None else metrics,
    )


def _stores_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="stores",
        sql_table="stores",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),
        ],
    )


async def _make_engine(
    tmp_path, *, seed: bool, metrics: list[DatasetMetric] | None = None
) -> SlayerQueryEngine:
    """Build a YAMLStorage-backed engine over the orders(+stores) dataset.

    ``seed=True`` materialises the SQLite tables for execution tests;
    ``seed=False`` builds only the datasource + datasets for dry-run SQL shape.
    """
    db_file = tmp_path / "slayer_test.db"
    if seed:
        with transaction(db_file) as conn:
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

    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=str(db_file))
    )
    await storage.save_dataset(_stores_dataset())
    await storage.save_dataset(_orders_dataset(metrics))
    return SlayerQueryEngine(storage=storage)


# ---------------------------------------------------------------------------
# SQL-shape invariant helper
# ---------------------------------------------------------------------------


def _referenced_but_undeclared(sql: str) -> set[str]:
    """Generated aliases referenced in ``sql`` but never declared with ``AS``.

    Every ``"dataset.col"`` alias a SELECT/CTE references must be projected
    (declared ``AS "dataset.col"``) by some layer below it. A non-empty result
    means the SQL references a column no CTE produces — exactly the original
    failure (``"orders.id_count"`` referenced, only ``"orders.order_count"``
    declared). Restricted to *dotted* quoted identifiers: SLayer aliases are
    always ``dataset.col`` (a dot), while base-table columns / physical
    identifiers are bare, so the dot filter avoids false-failing on a quoted
    physical name. This is a heuristic backstop; the execution tests are the
    authoritative check that the SQL is valid end to end.
    """
    declared = set(re.findall(r'AS "([^"]+)"', sql))
    referenced = {ref for ref in re.findall(r'"([^"]+)"', sql) if "." in ref}
    return referenced - declared


async def _dry_sql(tmp_path, metrics: list[str], **query_kw: Any) -> str:
    """Dry-run an orders query and return the emitted SQL."""
    engine = await _make_engine(tmp_path, seed=False)
    query = SlayerQuery(population="orders", metrics=metrics, **query_kw)
    resp = await engine.execute(query=query, dry_run=True)
    assert resp.sql is not None
    return resp.sql


# ---------------------------------------------------------------------------
# Group A — string-shape invariant over the metric-ordering matrix (dry-run)
# ---------------------------------------------------------------------------


# (id, label, reproduces_bug, both_refs_selected)
_ORDERINGS = [
    ("formula_first", ["orders.habit_score", "orders.order_count", "orders.unique_customers"], True, True),
    ("formula_middle", ["orders.order_count", "orders.habit_score", "orders.unique_customers"], True, True),
    ("formula_last", ["orders.order_count", "orders.unique_customers", "orders.total_revenue", "orders.habit_score"], False, True),
    ("one_ref_selected", ["orders.order_count", "orders.habit_score"], False, False),
    ("no_ref_selected", ["orders.habit_score"], False, False),
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
# Group B/C/D — execution + relationship scenarios (temp-file SQLite)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "label,measures,_bug,_both", _ORDERINGS, ids=[c[0] for c in _ORDERINGS]
)
async def test_formula_ref_executes(
    tmp_path, label: str, measures: list[str], _bug: bool, _both: bool
) -> None:
    engine = await _make_engine(tmp_path, seed=True)
    query = SlayerQuery(population="orders", metrics=measures)
    resp = await engine.execute(query=query)  # runs the real SQL — must not raise
    assert resp.data, resp.sql
    row = resp.data[0]
    # Single ungrouped bucket: order_count=8, unique_customers=4 → habit=2.
    # unique_customers is not always projected (hidden inside the expression for
    # one_ref/no_ref), so assert against the expression result directly.
    assert row["orders.habit_score"] == 2
    if "orders.order_count" in row:
        assert row["orders.order_count"] == 8


async def test_transform_wrapped_reference_follows_rename(tmp_path) -> None:
    """Group C: cumsum(order_count) with order_count selected AFTER — the
    hidden transform's input must follow the rename (not orphan)."""
    engine = await _make_engine(
        tmp_path,
        seed=False,
        metrics=[
            DatasetMetric(name="order_count", expression="count(id)"),
            DatasetMetric(name="running_orders", expression="cumsum(order_count)"),
        ],
    )
    query = SlayerQuery.model_validate({
        "population": "orders",
        "metrics": ["orders.running_orders", "orders.order_count"],
        "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
    })
    resp = await engine.execute(query=query, dry_run=True)
    assert resp.sql is not None
    assert _referenced_but_undeclared(resp.sql) == set(), resp.sql
    assert f'"{_CANON_ORDER}"' not in resp.sql, resp.sql


async def test_change_pct_desugar_reference_follows_rename(tmp_path) -> None:
    """Group C (desugaring): change_pct(order_count) desugars to an
    expression + a hidden time_shift, both referencing the inner metric.
    With order_count selected AFTER, both frozen carriers must follow the
    rename."""
    engine = await _make_engine(
        tmp_path,
        seed=False,
        metrics=[
            DatasetMetric(name="order_count", expression="count(id)"),
            DatasetMetric(name="mom_orders", expression="change_pct(order_count)"),
        ],
    )
    query = SlayerQuery.model_validate({
        "population": "orders",
        "metrics": ["orders.mom_orders", "orders.order_count"],
        "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
    })
    resp = await engine.execute(query=query, dry_run=True)
    assert resp.sql is not None
    assert _referenced_but_undeclared(resp.sql) == set(), resp.sql
    assert f'"{_CANON_ORDER}"' not in resp.sql, resp.sql


async def test_full_reported_scenario(tmp_path) -> None:
    """Group D: the exact shape from the ticket — joined ``stores.name``
    dimension, expression metric listed first, and ORDER BY the expression."""
    engine = await _make_engine(tmp_path, seed=True)
    query = SlayerQuery.model_validate({
        "population": "orders",
        "metrics": ["orders.habit_score", "orders.order_count", "orders.unique_customers", "orders.total_revenue"],
        "dimensions": ["stores.name"],
        "order": [{"field": "orders.habit_score", "direction": "desc"}],
        "limit": 100,
    })
    dry = await engine.execute(query=query, dry_run=True)
    assert dry.sql is not None
    assert _referenced_but_undeclared(dry.sql) == set(), dry.sql

    resp = await engine.execute(query=query)  # must execute cleanly
    assert [r["stores.name"] for r in resp.data] == ["North", "South"]
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
    """An expression referencing a metric that does not exist is rejected at
    bind time (``UnknownReferenceError``) — never emitted as dangling SQL.
    The guard fires before SQL generation: the reference never resolves to a key."""
    engine = await _make_engine(
        tmp_path,
        seed=False,
        metrics=[
            DatasetMetric(name="order_count", expression="count(id)"),
            DatasetMetric(name="bad", expression="order_count / nonexistent_metric"),
        ],
    )
    query = SlayerQuery(population="orders", metrics=["orders.bad"])
    with pytest.raises(UnknownReferenceError) as exc:
        await engine.execute(query=query, dry_run=True)
    assert "nonexistent_metric" in str(exc.value)


async def test_transform_referencing_nonexistent_measure_raises(tmp_path) -> None:
    """Same guard through a transform wrapper: cumsum over a nonexistent
    metric is rejected at bind, not rendered with a dangling input."""
    engine = await _make_engine(
        tmp_path,
        seed=False,
        metrics=[
            DatasetMetric(name="order_count", expression="count(id)"),
            DatasetMetric(name="running_bad", expression="cumsum(nonexistent_metric)"),
        ],
    )
    query = SlayerQuery.model_validate({
        "population": "orders",
        "metrics": ["orders.running_bad"],
        "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
    })
    with pytest.raises(UnknownReferenceError) as exc:
        await engine.execute(query=query, dry_run=True)
    assert "nonexistent_metric" in str(exc.value)
