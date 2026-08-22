"""Shared fixtures + helpers for the DEV-1746 scope-assembly test modules.

Underscore-prefixed (like ``tests/_engine_helpers.py`` and
``tests/_cross_model_chain.py``) so pytest skips it during collection while
``from tests._dev1746_fixtures import ...`` still works.

What lives here:

* **A NULL-bearing SQLite corpus** (:func:`seed_dev1746_sqlite`) — the execution
  substrate for §5.7. Every grain column it seeds is deliberately nullable and
  actually carries NULLs, because the whole point of B1/B2 is what happens to a
  group whose grain member is NULL. ``orders.status`` has a NULL group spanning
  two months (so a 90-day window over it has something to sum), ``customers.tier``
  has a NULL group, and ``regions.name`` has one too — the composite-grain case
  needs two independently-nullable members.
* **SQLite-shaped model builders** (:func:`dev1746_models`) — bare ``sql_table``
  names, matching the seeded schema.
* :func:`make_sqlite_engine` — the storage+engine wiring every execution fixture
  in this family repeats.
* :func:`outer_select_aliases` — the emitted public projection, in order. B7 is
  an *ordering* contract, so the assertions need the emitted order as a list,
  not the ``set`` that ``_join_aliases`` returns.
* :func:`joinback_on_predicate_for` — the generalisation of
  ``tests/_cross_model_chain._joinback_on_predicate`` to ``_wm_`` as well as
  ``_cm_`` CTEs (B1 touches both join-backs, and the existing helper only finds
  ``_cm_``/``_fm_``).

The seeded numbers are chosen so every expected aggregate is a distinct value —
a test that accidentally reads the wrong column fails rather than coincidentally
matching.
"""

from __future__ import annotations

import sqlite3
from typing import List, Optional

import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine

from tests._engine_helpers import make_seeded_sqlite_engine

# --------------------------------------------------------------------------- #
# The corpus
# --------------------------------------------------------------------------- #
#: ``orders.status`` NULL group, by month, for the 90-day window assertions:
#: 2024-01 has 5.0, 2024-02 has 7.0 — so the February window (which reaches back
#: 90 days) sums to 12.0 while February alone would be 7.0. The two numbers
#: differ, so a test cannot pass by reading the un-windowed sum.
NULL_STATUS_JAN = 5.0
NULL_STATUS_FEB = 7.0
NULL_STATUS_FEB_WINDOW = NULL_STATUS_JAN + NULL_STATUS_FEB  # 12.0

#: ``paid`` group, same shape, different values.
PAID_JAN = 10.0
PAID_FEB = 20.0
PAID_FEB_WINDOW = PAID_JAN + PAID_FEB  # 30.0


def seed_dev1746_sqlite(db_path: str) -> None:
    """Create + seed the DEV-1746 SQLite corpus at ``db_path``."""
    con = sqlite3.connect(db_path)
    try:
        con.executescript(
            """
            CREATE TABLE regions (
                id INTEGER PRIMARY KEY,
                name TEXT,
                population REAL
            );
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                region_id INTEGER,
                tier TEXT,
                spend REAL
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                status TEXT,
                created_at TEXT,
                amount REAL
            );
            """
        )
        con.executemany(
            "INSERT INTO regions VALUES (?,?,?)",
            # Region 2's name is NULL — the joined nullable grain member.
            [(1, "West", 100.0), (2, None, 200.0)],
        )
        con.executemany(
            "INSERT INTO customers VALUES (?,?,?,?)",
            # Customer 101's tier is NULL — the target-side nullable grain member.
            [
                (100, 1, "gold", 1000.0),
                (101, 2, None, 250.0),
                (102, 2, None, 75.0),
            ],
        )
        con.executemany(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            [
                (1, 100, "paid", "2024-01-15", PAID_JAN),
                (2, 100, "paid", "2024-02-15", PAID_FEB),
                # The NULL-status group — two months, so a 90-day window spans both.
                (3, 101, None, "2024-01-20", NULL_STATUS_JAN),
                (4, 101, None, "2024-02-20", NULL_STATUS_FEB),
            ],
        )
        con.commit()
    finally:
        con.close()


def dev1746_models() -> List[SlayerModel]:
    """SQLite-shaped ``orders -> customers -> regions`` models for the corpus.

    Returned host-first; ``[0]`` is the host and the rest are ``extra_models``.
    """
    regions = SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="population", type=DataType.DOUBLE),
        ],
    )
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    return [orders, customers, regions]


async def make_sqlite_engine(base_dir: str, db_path: str) -> SlayerQueryEngine:
    """Storage + engine bound to the seeded SQLite file at ``db_path``."""
    return await make_seeded_sqlite_engine(
        base_dir=base_dir, db_path=db_path, models=dev1746_models()
    )


# --------------------------------------------------------------------------- #
# SQL-shape helpers
# --------------------------------------------------------------------------- #
def outer_select_aliases(sql: str, *, dialect: str = "postgres") -> List[str]:
    """The outermost SELECT's output column names, **in emitted order**.

    B7 is an ordering contract, so this returns a list. Uses sqlglot's
    ``named_selects``, which is what ``response_meta.expected_columns_from_sql``
    reads — so asserting on this asserts on the same thing the response's
    ``columns`` order is derived from.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    assert parsed is not None, f"SQL failed to parse:\n{sql}"
    return list(parsed.named_selects)


def joinback_on_predicate_for(
    sql: str, *, prefix: str, dialect: str = "postgres",
) -> str:
    """Rendered ON predicate of the combined SELECT's join-back to a CTE whose
    alias starts with ``prefix`` (``_cm_`` or ``_wm_``).

    ``tests/_cross_model_chain._joinback_on_predicate`` only finds ``_cm_``/
    ``_fm_``; B1 needs the ``_wm_`` join-back too, and §5.7 requires asserting on
    BOTH ``_wm_`` comparison sites.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    for join in tree.find_all(exp.Join):
        name = getattr(join.this, "alias_or_name", "") or ""
        if name.startswith(prefix):
            on = join.args.get("on")
            if on is not None:
                return on.sql(dialect=dialect)
    raise AssertionError(f"no JOIN onto a {prefix}* CTE with an ON predicate in:\n{sql}")


def joined_cte_names(sql: str, *, dialect: str = "postgres") -> List[str]:
    """Aliases of the CTEs joined into the combined SELECT, in FROM-clause order."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    select = tree.find(exp.Select)
    assert select is not None, f"no SELECT in:\n{sql}"
    names: List[str] = []
    for join in select.args.get("joins") or []:
        name = getattr(join.this, "alias_or_name", "") or ""
        if name:
            names.append(name)
    return names


def cte_names_in_order(sql: str, *, dialect: str = "postgres") -> List[str]:
    """Names of the top-level WITH clause's CTEs, in emitted order."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    with_node = tree.args.get("with_")
    if with_node is None:
        return []
    return [cte.alias_or_name for cte in with_node.expressions]


def src_subquery_on_predicate(sql: str, *, dialect: str = "postgres") -> str:
    """Rendered ON predicate of the ``_src`` subquery join **inside** a ``_wm_``
    CTE — the B1 inner-grain comparison site.

    §5.7 requires both ``_wm_`` sites be asserted; this is the inner one
    (``joinback_on_predicate_for(prefix="_wm_")`` is the outer one).
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    for join in tree.find_all(exp.Join):
        name = getattr(join.this, "alias_or_name", "") or ""
        if name == "_src":
            on = join.args.get("on")
            if on is not None:
                return on.sql(dialect=dialect)
    raise AssertionError(f"no `_src` subquery join with an ON predicate in:\n{sql}")


def outer_statement(sql: str, *, dialect: str = "postgres") -> exp.Select:
    """The OUTERMOST SELECT — the one pagination must land on.

    Asserting pagination by searching the whole statement is unsound: an inner
    CTE, a window function's ``OVER (ORDER BY …)``, or a hidden order slot can
    satisfy a global ``ORDER BY`` search while the paginated SELECT itself has
    none (which is exactly the T-SQL error the rule exists to prevent).
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    assert isinstance(parsed, exp.Select), (
        f"expected the statement to be a SELECT, got {type(parsed).__name__}:\n{sql}"
    )
    return parsed


def outer_clause_sql(sql: str, *, dialect: str = "postgres") -> str:
    """The outermost SELECT rendered WITHOUT its CTEs.

    Keeps the pagination/ordering clauses of the outer statement while dropping
    every inner scope, so a keyword assertion cannot be satisfied by a CTE body.
    """
    outer = outer_statement(sql, dialect=dialect).copy()
    outer.set("with_", None)
    return outer.sql(dialect=dialect)


def join_alias_sequence(sql: str, *, dialect: str = "postgres") -> List[str]:
    """Joined table aliases of the outermost FROM, in emitted JOIN order.

    ``_engine_helpers._join_aliases`` returns a SET; join ORDER assertions need
    the sequence, and building one by testing membership of known names against
    the SQL string would just reproduce the caller's own ordering.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    select = tree.find(exp.Select)
    assert select is not None, f"no SELECT in:\n{sql}"
    names: List[str] = []
    for join in select.args.get("joins") or []:
        target = join.this
        if isinstance(target, exp.Table):
            names.append(target.alias_or_name)
    return names


def base_cte_join_sequence(sql: str, *, dialect: str = "postgres") -> List[str]:
    """Joined aliases of the ``_base`` CTE (or the top-level FROM when the query
    has no CTEs), in emitted order — the B11 subject."""
    base = find_cte(sql, "_base", dialect=dialect)
    if base is None:
        tree = sqlglot.parse_one(sql, dialect=dialect)
        base = tree.find(exp.Select)
    assert base is not None, f"no base scope found in:\n{sql}"
    names: List[str] = []
    for join in base.args.get("joins") or []:
        target = join.this
        if isinstance(target, exp.Table):
            names.append(target.alias_or_name)
    return names


def carried_alias_drops(sql: str, *, dialect: str = "postgres") -> List[str]:
    """Stages that fail to carry forward an alias a LATER stage still needs.

    B8 reorders carry lists; Codex D8 requires it fail closed. Order is checked
    by :func:`carry_list_order_violations`; this is the other half — an alias
    referenced downstream must be projected by every stage between its producer
    and its consumer, or the SQL simply will not bind.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    ctes = list(tree.find_all(exp.CTE))
    if len(ctes) < 2:
        return []
    projected = {c.alias_or_name: set(c.this.named_selects) for c in ctes}
    violations: List[str] = []

    def _scopes():
        for cte in ctes:
            yield cte.alias_or_name, cte.this
        final = tree.find(exp.Select)
        if final is not None:
            yield "<final SELECT>", final

    for label, scope in _scopes():
        # Only columns QUALIFIED by another CTE's name can be checked: an
        # unqualified reference is ambiguous, and sibling CTEs (``_cm_`` next to
        # ``base``) do not read each other at all, so an adjacency-based check
        # would report drops that are not drops.
        for col in scope.find_all(exp.Column):
            source = col.table
            if not source or source not in projected or source == label:
                continue
            if col.name not in projected[source]:
                violations.append(
                    f"{label} references {source}.{col.name!r}, which "
                    f"{source} does not project"
                )
    return sorted(set(violations))


def carry_list_order_violations(
    sql: str, *, dialect: str = "postgres",
) -> List[str]:
    """Inner stages whose carried aliases are not in the base stage's order.

    B8's contract in one invariant: every downstream stage (a ``stepN`` CTE, an
    ``sjoin_``/``cp_reset_`` CTE, the inner SELECT under the ``_outer`` wrap)
    carries forward a subset of the base stage's columns. Those carried aliases
    must appear in the order the BASE stage projects them — that is what "plan
    order" means downstream, since the base projects in ``base_render_order``.
    ``sorted(aliases)`` violates it whenever alphabetical order differs.

    Returns a list of human-readable violations (empty when compliant), so a
    failing assertion can name every offending stage at once.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    ctes = list(tree.find_all(exp.CTE))
    if not ctes:
        return []
    base_order = list(ctes[0].this.named_selects)
    base_rank = {a: i for i, a in enumerate(base_order)}

    def _check(label: str, aliases: List[str]) -> Optional[str]:
        carried = [a for a in aliases if a in base_rank]
        expected = sorted(carried, key=lambda a: base_rank[a])
        if carried != expected:
            return f"{label}: carries {carried}, base order is {expected}"
        return None

    violations: List[str] = []
    for cte in ctes[1:]:
        found = _check(f"CTE {cte.alias_or_name!r}", list(cte.this.named_selects))
        if found:
            violations.append(found)
    # The inner SELECT of a derived-table wrap (``) AS _outer``) is its own site.
    for sub in tree.find_all(exp.Subquery):
        if sub.alias_or_name != "_outer":
            continue
        inner = sub.this
        selects = getattr(inner, "named_selects", None)
        if selects:
            found = _check("inner SELECT under _outer", list(selects))
            if found:
                violations.append(found)
    return violations


def find_cte(sql: str, name: str, *, dialect: str = "postgres") -> Optional[exp.Expression]:
    """The parsed body of the CTE called ``name`` (exact match), or ``None``."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    with_node = tree.args.get("with_")
    if with_node is None:
        return None
    for cte in with_node.expressions:
        if cte.alias_or_name == name:
            return cte.this
    return None


__all__ = [
    "NULL_STATUS_JAN",
    "NULL_STATUS_FEB",
    "NULL_STATUS_FEB_WINDOW",
    "PAID_JAN",
    "PAID_FEB",
    "PAID_FEB_WINDOW",
    "seed_dev1746_sqlite",
    "dev1746_models",
    "make_sqlite_engine",
    "outer_select_aliases",
    "outer_statement",
    "outer_clause_sql",
    "join_alias_sequence",
    "base_cte_join_sequence",
    "carried_alias_drops",
    "carry_list_order_violations",
    "joinback_on_predicate_for",
    "joined_cte_names",
    "cte_names_in_order",
    "src_subquery_on_predicate",
    "find_cte",
]
