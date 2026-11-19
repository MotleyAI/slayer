"""Shared fixtures + SQL-shape helpers for the cross-model CTE test modules.

Underscore-prefixed (like ``tests/_engine_helpers.py``) so pytest skips it during
collection while ``from tests._cross_model_chain import ...`` still works.

Two groups live here:

* **SQL-shape helpers** — ``_split_at_ranked_subquery`` and
  ``_joinback_on_predicate``, for splitting a ``_cm_*`` CTE body at the ranked
  subquery and reading the grain join-back's ``ON`` predicate. These were copied
  per-module until DEV-1728; the copies are now one definition (they were
  byte-identical, so the duplication bought nothing). ``_norm`` /
  ``_extract_cte_body`` are re-exported from ``tests/_engine_helpers.py``, which
  DEV-1732 made the single home for the SQL-shape helpers that are not
  cross-model-specific — same one-definition rule, one module up.
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

import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery

from tests._engine_helpers import _engine_generate, _extract_cte_body, _norm


# --------------------------------------------------------------------------- #
# SQL-shape helpers
# --------------------------------------------------------------------------- #
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
        if name.startswith("_cm_"):
            on = join.args.get("on")
            if on is not None:
                return on.sql(dialect=dialect)
    raise AssertionError(f"no LEFT JOIN _cm_* ON predicate in:\n{sql}")


# --------------------------------------------------------------------------- #
# Model builders — chain: orders_x → customers_v2 → regions → countries.
# Postgres dialect for SQL-shape assertions (alias mangling is identity there).
# --------------------------------------------------------------------------- #
def _countries() -> SlayerDataset:
    return SlayerDataset(
        name="countries", sql_table="countries", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="gdp", sql="gdp", datatype=DataType.FLOAT),
        ],
    )


def _regions(*, extra_columns=None) -> SlayerDataset:
    cols = [
        DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="name", sql="name", datatype=DataType.STRING),
        DatasetField(name="population", sql="population", datatype=DataType.FLOAT),
        DatasetField(name="weight", sql="weight", datatype=DataType.FLOAT),
        DatasetField(name="country_id", sql="country_id", datatype=DataType.FLOAT),
        DatasetField(name="opened_at", sql="opened_at", datatype=DataType.DATETIME),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerDataset(
        name="regions", sql_table="regions", data_source="test",
        fields=cols,
        relationships=[DatasetRelationship(to="countries", from_columns=["country_id"], to_columns=["id"])],
    )


def _customers_v2(*, extra_columns=None) -> SlayerDataset:
    cols = [
        DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="region_id", sql="region_id", datatype=DataType.FLOAT),
        DatasetField(name="lifetime_value", sql="lifetime_value", datatype=DataType.FLOAT),
        DatasetField(name="signup_at", sql="signup_at", datatype=DataType.DATETIME),
        DatasetField(name="status", sql="status", datatype=DataType.STRING),
        # Target-LOCAL derived dims (expand to customers_v2-only columns):
        DatasetField(name="ltv_x2", sql="lifetime_value * 2", datatype=DataType.FLOAT),
        DatasetField(name="ltv_third", sql="lifetime_value / 3.0", datatype=DataType.INTEGER),
        # Derived, one-hop crossing (customers_v2 → regions):
        DatasetField(name="deep_pop", sql="regions.population", datatype=DataType.FLOAT),
        DatasetField(name="deep_pop_x2", sql="regions.population * 2", datatype=DataType.FLOAT),
        DatasetField(name="deep_weight", sql="regions.weight", datatype=DataType.FLOAT),
        # Derived, TWO-hop crossing (customers_v2 → regions → countries):
        DatasetField(name="deep_gdp", sql="regions.countries.gdp", datatype=DataType.FLOAT),
        # Derived TIME dim whose sql crosses a further join (DEV-1701):
        DatasetField(name="region_opened_eff",
               sql="coalesce(regions.opened_at, signup_at)",
               datatype=DataType.DATETIME),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerDataset(
        name="customers_v2", sql_table="customers", data_source="test",
        fields=cols,
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
        default_time_dimension="signup_at",
    )


def _orders_x(*, extra_columns=None) -> SlayerDataset:
    cols = [
        DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
        DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
        DatasetField(name="status", sql="status", datatype=DataType.STRING),
        DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerDataset(
        name="orders_x", sql_table="orders", data_source="test",
        fields=cols,
        relationships=[DatasetRelationship(to="customers_v2", from_columns=["customer_id"], to_columns=["id"])],
        default_time_dimension="created_at",
    )


async def _gen(
    query: SlayerQuery,
    *,
    orders_extra=None,
    customers_extra=None,
    regions_extra=None,
    dialect: str = "postgres",
) -> str:
    """Render ``query`` against the orders_x chain and return the SQL."""
    return await _engine_generate(
        query=query,
        dataset=_orders_x(extra_columns=orders_extra),
        dialect=dialect,
        extra_datasets=[
            _customers_v2(extra_columns=customers_extra),
            _regions(extra_columns=regions_extra),
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
