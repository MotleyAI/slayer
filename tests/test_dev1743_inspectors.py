"""DEV-1743 (category 10 / WP5): save-time inspectors and the ``__`` flip.

Two save-time inspectors resolve table aliases by splitting on ``__`` today:

* ``slayer/engine/schema_drift.py`` — ``_walk_alias_to_target_model`` (line ~667),
  reached from the pure cascade entry point ``compute_datasource_drops``.
* ``slayer/engine/column_dependency.py`` — ``_resolve_target_for_ref`` (line ~69),
  reached from ``_column_dependencies``.

After the dotted-canonical flip both must resolve BOTH forms through the shared
resolver: a DOTTED chain (``customers.regions.name`` = walk host→customers→regions)
AND a ``__``-named EXACT join target (``customer__region`` = one direct join),
without conflating the two.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column

from slayer.engine.column_dependency import _column_dependencies
from slayer.engine.schema_drift import (
    EditModelDelete,
    RemoveSpec,
    compute_datasource_drops,
)

from tests._dev1743_fixtures import DS, chain_models, dunder_target_models


# ---------------------------------------------------------------------------
# 1. schema-drift cascade over a ``__``-named DIRECT join target
# ---------------------------------------------------------------------------


def test_schema_drift_resolves_dunder_direct_join_target() -> None:
    """FAIL-FIRST: build ``orders`` with a DIRECT join to a model literally named
    ``customer__region`` and a derived column ``sql="customer__region.label"``.

    Today ``dunder_target_models()`` raises at construction (the ``__`` ban), so
    the model can't even be built. After the flip the cascade resolver must treat
    ``customer__region`` as an EXACT direct join target — NOT split it into
    ``customer`` → ``region`` — so that dropping ``customer__region.label``
    cascade-drops the dependent ``orders.cr_label`` column.

    Uses the pure entry point ``compute_datasource_drops`` (no live DB), which
    exercises ``_walk_alias_to_target_model`` via the derived-column cascade.
    """
    models = dunder_target_models()  # raises TODAY — the fail-first signal
    orders = next(m for m in models if m.name == "orders")
    orders.columns.append(
        Column(name="cr_label", sql="customer__region.label", type=DataType.TEXT)
    )

    # Seed the base diff: drop the ``label`` column on ``customer__region``.
    sql_table_diffs = {
        "customer__region": (
            EditModelDelete(
                model_name="customer__region",
                data_source=DS,
                remove=RemoveSpec(columns=["label"]),
            ),
            {"label"},
        )
    }

    result = compute_datasource_drops(
        models=models, sql_table_diffs=sql_table_diffs, sql_diffs={}
    )

    orders_edit = next((e for e in result if e.model_name == "orders"), None)
    assert isinstance(orders_edit, EditModelDelete), (
        "derived column referencing the __-named direct target did not cascade "
        "(the alias resolver mis-split customer__region instead of exact-matching)"
    )
    assert "cr_label" in orders_edit.remove.columns, orders_edit.remove.columns


# ---------------------------------------------------------------------------
# 2. column-dependency over a DOTTED chain (dotted-canonical)
# ---------------------------------------------------------------------------


def test_column_dependency_reports_dotted_chain_dependency() -> None:
    """FAIL-FIRST (dotted-canonical): a Mode-A ``Column.sql`` chain written with
    DOTS (``customers.regions.label``) must resolve ``orders → customers →
    regions`` and report the ``(regions, label)`` dependency.

    Today sqlglot reads the 3-part dotted ref as catalog-qualified
    (``db=customers``); ``_resolve_single_column`` skips any db/catalog-qualified
    node, so the chain dependency is never reported. Under the flip, dots are the
    canonical join-path delimiter and this dependency must surface.

    ``regions.label`` is a DERIVED column (``upper(name)``) because
    ``_column_dependencies`` only tracks derived→derived edges (base columns
    can't participate in a derived-column cycle).
    """
    models = chain_models()  # orders → customers → regions (no ``__`` — builds today)
    by_name = {m.name: m for m in models}

    regions = by_name["regions"]
    regions.columns.append(
        Column(name="label", sql="upper(name)", type=DataType.TEXT)
    )
    orders = by_name["orders"]
    orders.columns.append(
        Column(name="region_label", sql="customers.regions.label", type=DataType.TEXT)
    )

    deps = _column_dependencies(
        column=orders.get_column("region_label"), host=orders, reachable=by_name
    )
    assert ("regions", "label") in deps, (
        f"dotted chain customers.regions.label did not resolve to (regions, label); "
        f"got {deps}"
    )
