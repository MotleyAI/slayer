"""Category 10 / WP5: save-time inspectors and the ``__`` flip.

Two save-time inspectors resolve table aliases by splitting on ``__`` today:

* ``slayer/live_schema/schema_drift.py`` — ``_walk_alias_to_target_model`` (line ~667),
  reached from the pure cascade entry point ``compute_datasource_drops``.
* ``slayer/engine/column_dependency.py`` — ``_resolve_target_for_ref`` (line ~69),
  reached from ``_column_dependencies``.

After the dotted-canonical flip both must resolve BOTH forms through the shared
resolver: a DOTTED chain (``customers.regions.name`` = walk host→customers→regions)
AND a ``__``-named EXACT relationship target (``customer__region`` = one direct
relationship),
without conflating the two.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import DatasetField

from slayer.engine.field_dependency import _field_dependencies
from slayer.live_schema.schema_drift import (
    EditDatasetDelete,
    RemoveSpec,
    compute_datasource_drops,
)

from tests._dev1743_fixtures import DS, chain_datasets, dunder_target_datasets


# ---------------------------------------------------------------------------
# 1. schema-drift cascade over a ``__``-named DIRECT relationship target
# ---------------------------------------------------------------------------


def test_schema_drift_resolves_dunder_direct_join_target() -> None:
    """FAIL-FIRST: build ``orders`` with a DIRECT relationship to a dataset literally
    named ``customer__region`` and a derived field ``sql="customer__region.label"``.

    Today ``dunder_target_datasets()`` raises at construction (the ``__`` ban), so
    the dataset can't even be built. After the flip the cascade resolver must treat
    ``customer__region`` as an EXACT direct relationship target — NOT split it into
    ``customer`` → ``region`` — so that dropping ``customer__region.label``
    cascade-drops the dependent ``orders.cr_label`` field.

    Uses the pure entry point ``compute_datasource_drops`` (no live DB), which
    exercises ``_walk_alias_to_target_dataset`` via the derived-field cascade.
    """
    models = dunder_target_datasets()  # raises TODAY — the fail-first signal
    orders = next(m for m in models if m.name == "orders")
    orders.fields.append(
        DatasetField(name="cr_label", sql="customer__region.label", datatype=DataType.STRING)
    )

    # Seed the base diff: drop the ``label`` field on ``customer__region``.
    sql_table_diffs = {
        "customer__region": (
            EditDatasetDelete(
                dataset_name="customer__region",
                data_source=DS,
                remove=RemoveSpec(fields=["label"]),
            ),
            {"label"},
        )
    }

    result = compute_datasource_drops(
        datasets=models, sql_table_diffs=sql_table_diffs, sql_diffs={}
    )

    orders_edit = next((e for e in result if e.dataset_name == "orders"), None)
    assert isinstance(orders_edit, EditDatasetDelete), (
        "derived field referencing the __-named direct target did not cascade "
        "(the alias resolver mis-split customer__region instead of exact-matching)"
    )
    assert "cr_label" in orders_edit.remove.fields, orders_edit.remove.fields


# ---------------------------------------------------------------------------
# 2. field-dependency over a DOTTED chain (dotted-canonical)
# ---------------------------------------------------------------------------


def test_column_dependency_reports_dotted_chain_dependency() -> None:
    """FAIL-FIRST (dotted-canonical): a Mode-A ``DatasetField.sql`` chain written with
    DOTS (``customers.regions.label``) must resolve ``orders → customers →
    regions`` and report the ``(regions, label)`` dependency.

    Today sqlglot reads the 3-part dotted ref as catalog-qualified
    (``db=customers``); ``_resolve_single_column`` skips any db/catalog-qualified
    node, so the chain dependency is never reported. Under the flip, dots are the
    canonical relationship-path delimiter and this dependency must surface.

    ``regions.label`` is a DERIVED field (``upper(name)``) because
    ``_field_dependencies`` only tracks derived→derived edges (base fields
    can't participate in a derived-field cycle).
    """
    models = chain_datasets()  # orders → customers → regions (no ``__`` — builds today)
    by_name = {m.name: m for m in models}

    regions = by_name["regions"]
    regions.fields.append(
        DatasetField(name="label", sql="upper(name)", datatype=DataType.STRING)
    )
    orders = by_name["orders"]
    orders.fields.append(
        DatasetField(name="region_label", sql="customers.regions.label", datatype=DataType.STRING)
    )

    deps = _field_dependencies(
        field=orders.get_field("region_label"), host=orders, reachable=by_name
    )
    assert ("regions", "label") in deps, (
        f"dotted chain customers.regions.label did not resolve to (regions, label); "
        f"got {deps}"
    )
