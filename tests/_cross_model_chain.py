"""Shared fixtures + SQL-shape helpers for the cross-model CTE test modules.

Underscore-prefixed (like ``tests/_engine_helpers.py``) so pytest skips it during
collection while ``from tests._cross_model_chain import ...`` still works.

Two groups live here:

* **SQL-shape helpers** — ``_norm``, ``_extract_cte_body``,
  ``_split_at_ranked_subquery``, ``_joinback_on_predicate``. Every cross-model
  test module needs to slice a ``_cm_*`` CTE body out of the emitted statement,
  split it at the ranked subquery, or read the grain join-back's ``ON``
  predicate. These were copied per-module until DEV-1728; the copies are now one
  definition (they were byte-identical, so the duplication bought nothing).
* **The canonical join chain** — ``orders_x → customers_v2 → regions →
  countries``, the fixture the DEV-1708 / DEV-1728 cross-model CTE tests render
  against. ``_customers_v2`` carries the derived-column matrix those suites
  exercise: target-local (``ltv_x2``, ``ltv_third``), one-hop crossing
  (``deep_pop``, ``deep_pop_x2``, ``deep_weight``), two-hop crossing
  (``deep_gdp``), and a crossing derived TIME dim (``region_opened_eff``). Each
  builder takes ``extra_columns`` so a module can add a shape it alone needs
  without growing the shared chain.
"""

from __future__ import annotations

import re as _re

import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery

from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# SQL-shape helpers
# --------------------------------------------------------------------------- #
def _norm(s: str) -> str:
    """Collapse all runs of whitespace to single spaces (pretty-printed SQL →
    one line) so shape assertions are newline-insensitive."""
    return " ".join(s.split())


def _extract_cte_body(sql: str, cte_name_pattern: str) -> str:
    """Extract one CTE body by matching ``<cte_name> AS (`` and walking balanced
    parentheses to its closing ``)``.

    Robust against nested subqueries inside the CTE body (e.g. the ranked
    ``FROM (SELECT ... ROW_NUMBER() …) AS …`` that first/last isolated CTEs
    contain). ``cte_name_pattern`` is a regex matched against the CTE name —
    typical use: ``r"_cm_\\w*loss_payment_amt\\w*"``. Raises ``AssertionError``
    if no matching CTE is found.
    """
    name_match = _re.search(rf"({cte_name_pattern})\s+AS\s*\(", sql)
    assert name_match, f"No CTE matching {cte_name_pattern!r} in:\n{sql}"
    # Position just after the opening paren of ``<name> AS (``.
    body_start = sql.index("(", name_match.start()) + 1
    depth = 1
    i = body_start
    while i < len(sql) and depth > 0:
        ch = sql[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return sql[body_start:i]
        i += 1
    raise AssertionError(
        f"Unbalanced parens — no closing ) for CTE {name_match.group(1)!r}:\n{sql}"
    )


def _split_at_ranked_subquery(norm: str) -> "tuple[str, str]":
    """Split a normalized CTE body into ``(outer, inner)`` at the ranked
    subquery's ``FROM (``. Asserts the marker exists so a shape change (no
    ranked subquery) fails loudly instead of silently slicing at ``-1``."""
    at = norm.find("FROM (")
    assert at != -1, f"no ranked subquery (FROM () in:\n{norm}"
    return norm[:at], norm[at:]


def _joinback_on_predicate(sql: str, *, dialect: str = "postgres") -> str:
    """Return the rendered ON predicate of the combined SELECT's
    ``LEFT JOIN _cm_* ON ...`` grain join-back (the null-safe target)."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    for join in tree.find_all(sqlglot.exp.Join):
        target = join.this
        name = getattr(target, "alias_or_name", "") or ""
        if name.startswith("_cm_") or name.startswith("_fm_"):
            on = join.args.get("on")
            if on is not None:
                return on.sql(dialect=dialect)
    raise AssertionError(f"no LEFT JOIN _cm_*/_fm_* ON predicate in:\n{sql}")


# --------------------------------------------------------------------------- #
# Model builders — chain: orders_x → customers_v2 → regions → countries.
# Postgres dialect for SQL-shape assertions (alias mangling is identity there).
# --------------------------------------------------------------------------- #
def _countries() -> SlayerModel:
    return SlayerModel(
        name="countries", sql_table="countries", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="gdp", sql="gdp", type=DataType.DOUBLE),
        ],
    )


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="population", sql="population", type=DataType.DOUBLE),
            Column(name="weight", sql="weight", type=DataType.DOUBLE),
            Column(name="country_id", sql="country_id", type=DataType.DOUBLE),
            Column(name="opened_at", sql="opened_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="countries", join_pairs=[["country_id", "id"]])],
    )


def _customers_v2(*, extra_columns=None) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        Column(name="lifetime_value", sql="lifetime_value", type=DataType.DOUBLE),
        Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP),
        Column(name="status", sql="status", type=DataType.TEXT),
        # Target-LOCAL derived dims (expand to customers_v2-only columns):
        Column(name="ltv_x2", sql="lifetime_value * 2", type=DataType.DOUBLE),
        Column(name="ltv_third", sql="lifetime_value / 3.0", type=DataType.INT),
        # Derived, one-hop crossing (customers_v2 → regions):
        Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
        Column(name="deep_pop_x2", sql="regions.population * 2", type=DataType.DOUBLE),
        Column(name="deep_weight", sql="regions.weight", type=DataType.DOUBLE),
        # Derived, TWO-hop crossing (customers_v2 → regions → countries):
        Column(name="deep_gdp", sql="regions__countries.gdp", type=DataType.DOUBLE),
        # Derived TIME dim whose sql crosses a further join (DEV-1701):
        Column(name="region_opened_eff",
               sql="coalesce(regions.opened_at, signup_at)",
               type=DataType.TIMESTAMP),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerModel(
        name="customers_v2", sql_table="customers", data_source="test",
        columns=cols,
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        default_time_dimension="signup_at",
    )


def _orders_x(*, extra_columns=None) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
        Column(name="status", sql="status", type=DataType.TEXT),
        Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerModel(
        name="orders_x", sql_table="orders", data_source="test",
        columns=cols,
        joins=[ModelJoin(target_model="customers_v2", join_pairs=[["customer_id", "id"]])],
        default_time_dimension="created_at",
    )


async def _gen(
    query: SlayerQuery,
    *,
    orders_extra=None,
    customers_extra=None,
    dialect: str = "postgres",
) -> str:
    """Render ``query`` against the orders_x chain and return the SQL."""
    return await _engine_generate(
        query=query,
        model=_orders_x(extra_columns=orders_extra),
        dialect=dialect,
        extra_models=[
            _customers_v2(extra_columns=customers_extra),
            _regions(),
            _countries(),
        ],
    )


__all__ = [
    "_norm",
    "_extract_cte_body",
    "_split_at_ranked_subquery",
    "_joinback_on_predicate",
    "_countries",
    "_regions",
    "_customers_v2",
    "_orders_x",
    "_gen",
]
