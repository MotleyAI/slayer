"""Shared fixtures + helpers for the DEV-1747 rerooting / ORDER BY modules.

Underscore-prefixed (like ``tests/_dev1746_fixtures.py``) so pytest skips it
during collection while ``from tests._dev1747_fixtures import ...`` works.

What lives here:

* **A sort-key corpus** (:func:`seed_dev1747_sqlite`) built so the DIRECTION of
  the order-only aggregate wrap is observable. Group ``A`` spans regions
  ``Alpha`` and ``Zulu``; group ``B`` sits alone on ``Bravo``. Ordering ASC by
  ``MIN`` therefore yields ``[A, B]`` while ordering ASC by ``MAX`` would yield
  ``[B, A]`` — so a test cannot pass under the old unconditional-``MAX`` rule by
  accident. The names are deliberately not the group names, so a test reading
  the wrong column fails rather than coincidentally matching.
* **A 1:N fan-out table** (``order_tags``) so the DEV-1735 containment claim is
  testable: ordering by a tag name must not multiply a sibling ``amount:sum``.
  Order 1 carries three tags; every other order carries one.
* **A NULL joined name** (region 4) so null-ordering has real data to sort.
* **Model builders** — :func:`dev1747_models` (SQLite-shaped, bare
  ``sql_table``) and :func:`dev1747_pg_models` (same graph, for dry-run SQL
  shape assertions), including the derived crossing column ``cust_region``
  whose ``Column.sql`` reaches through a join.
* :func:`make_sqlite_engine` — storage + engine wiring bound to the seeded file.
* ORDER BY shape helpers the four render sites all have to satisfy.
"""

from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# --------------------------------------------------------------------------- #
# The corpus
# --------------------------------------------------------------------------- #
#: Group A spans two regions; group B sits on one BETWEEN them alphabetically.
#: MIN(A)="Alpha" < MIN(B)="Bravo"  -> ASC by MIN  == [A, B]
#: MAX(A)="Zulu"  > MAX(B)="Bravo"  -> ASC by MAX  == [B, A]
#: The two orderings disagree, which is exactly what makes D10 observable.
REGION_A_LOW = "Alpha"
REGION_A_HIGH = "Zulu"
REGION_B_ONLY = "Bravo"

#: Sibling-measure totals. Distinct per group, and neither is a multiple of the
#: other, so a fan-out that doubled or tripled one would be unmistakable.
GROUP_A_AMOUNT = 11.0 + 13.0  # 24.0 — two orders
GROUP_B_AMOUNT = 17.0         # 17.0 — one order
GROUP_NULL_AMOUNT = 19.0      # the NULL-region group

#: Order 1 carries THREE tags; if its join were pulled into the host base,
#: GROUP_A_AMOUNT would read 11.0 * 3 + 13.0 = 46.0 instead of 24.0.
ORDER_1_TAG_COUNT = 3


def seed_dev1747_sqlite(db_path: str) -> None:
    """Create + seed the DEV-1747 SQLite corpus at ``db_path``."""
    con = sqlite3.connect(db_path)
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
        CREATE TABLE order_tags (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            name TEXT
        );
        """
    )
    con.executemany(
        "INSERT INTO regions VALUES (?,?,?)",
        [
            (1, REGION_A_LOW, 100.0),
            (2, REGION_A_HIGH, 200.0),
            (3, REGION_B_ONLY, 300.0),
            # Region 4's name is NULL — the null-ordering group.
            (4, None, 400.0),
        ],
    )
    con.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            (100, 1, "gold", 1000.0),
            (101, 2, "gold", 250.0),
            (102, 3, "silver", 75.0),
            (103, 4, "silver", 50.0),
            # Region Alpha's SECOND customer, with NO orders and the other
            # tier. It exists so a target-side filter can change a cross-model
            # aggregate WITHIN a group that survives the filter, rather than
            # only removing whole groups: ``customers.tier == 'gold'`` takes
            # Alpha's spend from 1040 to 1000 while Alpha stays in the result.
            # Without it, a re-rooted CTE that failed to apply its copy of the
            # filter would still produce the right number for every surviving
            # group, because the join-back picks the group the host kept.
            (104, 1, "silver", 40.0),
        ],
    )
    con.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            # Group A: two orders, two DIFFERENT regions (Alpha and Zulu).
            (1, 100, "A", "2024-01-15", 11.0),
            (2, 101, "A", "2024-02-15", 13.0),
            # Group B: one order, region Bravo.
            (3, 102, "B", "2024-01-20", 17.0),
            # Group NULL-region: one order whose region name is NULL.
            (4, 103, "N", "2024-02-20", 19.0),
        ],
    )
    con.executemany(
        "INSERT INTO order_tags VALUES (?,?,?)",
        [
            # Order 1 fans out 3:1 — the containment probe.
            (1, 1, "rush"),
            (2, 1, "gift"),
            (3, 1, "fragile"),
            (4, 2, "rush"),
            # Distinct per group so the tag sort key never ties across groups
            # (a tie makes the row order unstable and the assertion flaky).
            (5, 3, "sale"),
            (6, 4, "trial"),
        ],
    )
    con.commit()
    con.close()


def _regions_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="population", type=DataType.DOUBLE),
        ],
    )


def _customers_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _order_tags_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="order_tags", sql_table="order_tags", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="order_id", type=DataType.INT),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def _orders_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
            # The DEV-1735 "also in scope" shape: a LOCAL derived column whose
            # ``Column.sql`` reaches THROUGH a join. Ordering by it is rejected
            # today in both grouped and ungrouped queries even though the bare
            # ``customers.regions.name`` resolves ungrouped (DEV-1703 Phase 1).
            Column(
                name="cust_region", type=DataType.TEXT,
                sql="customers__regions.name",
            ),
            # A NON-crossing derived column — the control. Ordering by it must
            # keep working exactly as it does today.
            Column(name="amount_x2", type=DataType.DOUBLE, sql="amount * 2"),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="order_tags", join_pairs=[["id", "order_id"]]),
        ],
    )


def dev1747_models(*, data_source: str = "test") -> List[SlayerModel]:
    """``orders -> customers -> regions`` plus the 1:N ``order_tags``.

    Returned host-first; ``[0]`` is the host and the rest are ``extra_models``.
    """
    return [
        _orders_model(data_source=data_source),
        _customers_model(data_source=data_source),
        _regions_model(data_source=data_source),
        _order_tags_model(data_source=data_source),
    ]


def dev1747_bundle():
    """A ``ResolvedSourceBundle`` over the corpus models, host-first.

    Lets the plan-level modules call ``plan_query`` directly instead of going
    through the engine — §5.10's contract is that the PLAN carries the order
    scope/phase/nulls, so it has to be assertable without rendering.
    """
    from slayer.engine.source_bundle import ResolvedSourceBundle

    models = dev1747_models()
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=models[1:],
    )


async def make_sqlite_engine(base_dir: str, db_path: str) -> SlayerQueryEngine:
    """Storage + engine bound to the seeded SQLite file at ``db_path``."""
    storage = YAMLStorage(base_dir=base_dir)
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=db_path),
    )
    for model in dev1747_models():
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


# --------------------------------------------------------------------------- #
# ORDER BY shape helpers
# --------------------------------------------------------------------------- #
def outermost_select(sql: str, *, dialect: str = "postgres") -> exp.Select:
    """The OUTERMOST SELECT — the one ORDER BY and pagination land on.

    Searching the whole statement is unsound here: a window function's
    ``OVER (ORDER BY …)``, an inner CTE, or a ranked subquery all carry an
    ``Order`` node, so a global ``find`` would happily assert against a clause
    the user's ORDER BY never reaches.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    assert parsed is not None, f"SQL failed to parse:\n{sql}"
    if isinstance(parsed, exp.Select):
        return parsed
    select = parsed.find(exp.Select)
    assert select is not None, f"no SELECT found in SQL:\n{sql}"
    return select


def order_terms(sql: str, *, dialect: str = "postgres") -> List[str]:
    """Rendered ORDER BY terms of the OUTERMOST select, in emitted order.

    Empty list when the statement has no outer ORDER BY — distinguishable from
    a term list, so a silently-dropped sort key fails loudly instead of
    matching a substring somewhere else in the statement.
    """
    order = outermost_select(sql, dialect=dialect).args.get("order")
    if order is None:
        return []
    return [t.sql(dialect=dialect) for t in order.expressions]


def order_by_text(sql: str, *, dialect: str = "postgres") -> str:
    """The outermost ORDER BY as one comma-joined string ('' when absent)."""
    return ", ".join(order_terms(sql, dialect=dialect))


def aggregate_funcs_over(sql: str, column: str, *, dialect: str = "postgres") -> List[str]:
    """Names of aggregate functions applied to ``column`` anywhere in ``sql``.

    D10 asserts the order wrap is ``MIN`` on ASC and ``MAX`` on DESC; this
    reads the emitted function rather than grepping, so a ``MIN`` appearing in
    an unrelated measure cannot satisfy the assertion.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    found: List[str] = []
    for node in tree.find_all(exp.Min, exp.Max):
        target = node.this
        name = getattr(target, "name", None) or (
            target.sql(dialect=dialect) if target is not None else ""
        )
        if name == column or (target is not None and column in target.sql(dialect=dialect)):
            found.append(type(node).__name__.upper())
    return found


def with_node_of(sql: str, *, dialect: str = "postgres"):
    """The statement's WITH node, wherever it sits.

    The local transform chain emits ``SELECT … FROM (WITH … SELECT …) AS _outer``
    on every dialect except T-SQL, so the WITH is NOT on the top-level
    statement. Looking only at ``tree.args["with_"]`` would report "no CTEs"
    for exactly the shape this PR rewrites.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    top = tree.args.get("with_")
    if top is not None:
        return top
    return tree.find(exp.With)


def cte_body_names(sql: str, *, dialect: str = "postgres") -> List[str]:
    """Names of the statement's CTEs, in emitted order."""
    with_node = with_node_of(sql, dialect=dialect)
    if with_node is None:
        return []
    return [cte.alias_or_name for cte in with_node.expressions]


def base_from_join_aliases(sql: str, *, dialect: str = "postgres") -> set:
    """Aliases joined into the *host base* relation.

    The DEV-1735 containment claim is that a grouped joined sort key does NOT
    pull its join into the host base — it lives inside the isolated CTE. That
    is only checkable by looking at the base's own JOIN list, not the
    statement's.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    with_node = tree.args.get("with_")
    aliases: set = set()
    candidates: List[exp.Expression] = []
    if with_node is not None:
        for cte in with_node.expressions:
            if cte.alias_or_name in ("_base", "base"):
                candidates.append(cte.this)
    if not candidates:
        candidates.append(tree)
    for candidate in candidates:
        for join in candidate.find_all(exp.Join):
            target = join.this
            if isinstance(target, exp.Table):
                aliases.add(target.alias_or_name)
    return aliases


def cte_map(sql: str, *, dialect: str = "postgres") -> Dict[str, exp.Expression]:
    """``{cte_name: cte_body}`` for every CTE in the statement.

    Finds WITH clauses wherever they sit, including the one the local transform
    chain nests inside a derived table, so a caller can reach an isolated CTE
    without knowing which render shape produced it.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    out: Dict[str, exp.Expression] = {}
    for with_node in tree.find_all(exp.With):
        for cte in with_node.expressions:
            out[cte.alias_or_name] = cte.this
    return out


def isolated_cte_bodies(
    sql: str, *, dialect: str = "postgres",
) -> Dict[str, exp.Expression]:
    """The CTEs the isolation machinery mints — everything but ``_base``.

    The DEV-1735 containment claim is two-sided: the crossed join must be ABSENT
    from the host base AND PRESENT in the isolated CTE. Asserting only the first
    half would also pass if the join vanished entirely and the sort key silently
    resolved to nothing.
    """
    return {
        name: body for name, body in cte_map(sql, dialect=dialect).items()
        if name not in ("_base", "base")
    }


def relation_names(node: exp.Expression) -> Set[str]:
    """Every table name AND alias ``node`` reads from.

    Both, because a joined relation appears under its alias
    (``regions AS customers__regions``) while a CTE reference appears under its
    own name — and a caller asserting containment should not have to know which
    of the two it is looking at.
    """
    names: Set[str] = set()
    for table in node.find_all(exp.Table):
        names.add(table.name)
        names.add(table.alias_or_name)
    names.discard("")
    return names


def is_null_safe_eq(predicate: exp.Expression) -> bool:
    """True when ``predicate`` compares two values NULL-safely.

    Accepts all three spellings ``SqlDialect.build_null_safe_eq`` emits —
    sqlglot's ``NullSafeEQ`` (``IS NOT DISTINCT FROM`` / MySQL ``<=>``), SQLite's
    bare ``IS`` between two non-NULL operands, and the expanded
    ``(a = b OR (a IS NULL AND b IS NULL))`` — and nothing else. A substring test
    for ``" IS "`` would also match ``IS NULL`` in an unrelated WHERE clause,
    which is how a plain ``=`` join-back could pass a null-safety assertion.
    """
    node = predicate.unnest()
    if isinstance(node, exp.NullSafeEQ):
        return True
    if isinstance(node, exp.Is):
        return not isinstance(node.expression, exp.Null)
    if isinstance(node, exp.Or):
        arms = [node.this.unnest(), node.expression.unnest()]
        has_eq = any(isinstance(arm, exp.EQ) for arm in arms)
        has_both_null = any(
            isinstance(arm, exp.And)
            and all(
                isinstance(side.unnest(), exp.Is)
                and isinstance(side.unnest().expression, exp.Null)
                for side in (arm.this, arm.expression)
            )
            for arm in arms
        )
        return has_eq and has_both_null
    return False


def all_conjuncts_null_safe(predicate: exp.Expression) -> bool:
    """Every top-level ``AND`` conjunct of ``predicate`` is null-safe.

    A multi-member grain joins back on an ``AND`` chain; one plain ``=`` among
    null-safe siblings still drops the NULL group, so the check has to be per
    conjunct rather than "contains a null-safe comparison".
    """
    parts: List[exp.Expression] = []

    def _split(node: exp.Expression) -> None:
        node = node.unnest()
        if isinstance(node, exp.And):
            _split(node.this)
            _split(node.expression)
        else:
            parts.append(node)

    _split(predicate)
    return bool(parts) and all(is_null_safe_eq(part) for part in parts)


def grain_join_back_predicates(
    sql: str, *, dialect: str = "postgres",
) -> List[exp.Expression]:
    """``ON`` predicates of the joins that attach an isolated CTE.

    Only those: the model joins inside a CTE are plain equalities by design, so
    including them would make a null-safety assertion fail for the wrong reason.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    minted = set(isolated_cte_bodies(sql, dialect=dialect))
    out: List[exp.Expression] = []
    for join in tree.find_all(exp.Join):
        target = join.this
        if not isinstance(target, exp.Table):
            continue
        if target.name not in minted and target.alias_or_name not in minted:
            continue
        on = join.args.get("on")
        if on is not None:
            out.append(on)
    return out


def aggregate_calls_in(
    node: exp.Expression, *, dialect: str = "postgres",
) -> List[Tuple[str, str]]:
    """``(FUNC, rendered-argument)`` for every aggregate call under ``node``.

    Lets a test say "the sort key is wrapped in MIN" about the SORT KEY rather
    than about the statement — ``"MIN(" in sql`` is satisfied by a MIN over any
    column at all, including a sibling measure.

    Identifier quoting is stripped from the argument so one assertion reads the
    same across dialects (``"a"."b"``, ```a`.`b```, ``[a].[b]``).
    """
    out: List[Tuple[str, str]] = []
    for call in node.find_all(exp.Min, exp.Max, exp.Sum, exp.Count, exp.Avg):
        arg = call.this
        rendered = arg.sql(dialect=dialect) if arg is not None else ""
        for quote in ('"', "`", "[", "]"):
            rendered = rendered.replace(quote, "")
        out.append((type(call).__name__.upper(), rendered))
    return out


def response_column_values(rows: List[dict], key: str) -> List[Optional[object]]:
    """``rows[i][key]`` for every row, preserving result order.

    Raises rather than defaulting on a missing key so a renamed result key
    surfaces as a clear failure instead of a list of ``None``.
    """
    out: List[Optional[object]] = []
    for i, row in enumerate(rows):
        assert key in row, f"row {i} has no key {key!r}; keys are {sorted(row)}"
        out.append(row[key])
    return out


#: Region Alpha's cross-model spend. The UNFILTERED total spans both of its
#: customers; the gold-only total is customer 100 alone. The gap is what makes
#: "did the re-rooted CTE apply its copy of the filter?" observable inside a
#: group the filter does not remove.
ALPHA_SPEND_ALL = 1000.0 + 40.0
ALPHA_SPEND_GOLD = 1000.0
