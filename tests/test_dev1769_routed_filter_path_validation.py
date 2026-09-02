"""DEV-1769 — validate multi-hop routed-filter paths in the cross-model
target-scope renderer.

When a filter is routed into a cross-model CTE, its ``ValueKey`` column leaves
are re-rooted to the CTE-local scope. The two column-leaf kinds used to validate
their host-rooted path ASYMMETRICALLY:

* ``ColumnKey`` — rejects an intermediate-hop path (``path[-1] != target``).
* ``ColumnSqlKey`` — checked only ``model == target``; the PATH was not
  validated at all, so any path was silently stripped and re-rooted.

DEV-1769's verdict (option (b)): the silently-accepted shape — ``model ==
target`` but ``path[-1] != target`` — is UNREACHABLE for keys produced by the
current binder and passed unchanged through the live routing pipeline. The
binder builds every non-empty-path ``ColumnSqlKey`` with ``model == path[-1]``
(the terminal hop), and every routed-filter call site passes
``target_relation == target_model.name``. So the shape can only arise from a
hand-built / deserialized / rewritten / future-inconsistent key. The fix makes
both renderers fail closed on it, SYMMETRICALLY with ``ColumnKey``.

Two layers of coverage:

* **E2E (existing behaviour, pinned)** — the reachable shapes the planner
  actually routes: reachable filters route INTO the CTE, and a filter
  unreachable from the aggregate's root (intermediate-hop / other-model) is
  DROPPED from the producer with a warning (DEV-1836 filter inheritance
  superseded the pre-existing hard rejection at the E2E boundary).
* **Direct-call layer RETIRED (DEV-1838 2.5)** — the renderer-level
  ``_reroot_routed_leaf`` seam died with the routed-filter machinery; filters
  now re-root at plan time through producer inheritance, and the E2E layer
  above is the surviving coverage.

The aggregate is ``customers_v2.regions.population:sum`` throughout, so the CTE
target is ``regions`` (a TWO-hop path). That lets a filter path end AT the
target (``customers_v2.regions.*``) or at the intermediate hop
(``customers_v2.*``), which single-hop fixtures cannot express.
"""

from __future__ import annotations

import warnings


from slayer.core.enums import DataType
from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.models import Column, ModelMeasure
from slayer.core.query import SlayerQuery

from tests._cross_model_chain import (
    _extract_cte_body,
    _gen,
    _norm,
)
from tests._engine_helpers import _assert_valid_sql

# A derived column ON regions, so a filter ``customers_v2.regions.pop_x2`` binds
# to a ``ColumnSqlKey`` whose path ENDS at the target ``regions``.
_REGIONS_EXTRA = [Column(name="pop_x2", sql="population * 2", type=DataType.DOUBLE)]

# The aggregate whose source is the two-hop path → CTE target is ``regions``.
_TWO_HOP_AGG = ModelMeasure(formula="customers_v2.regions.population:sum")

# =========================================================================== #
# Layer 1 — E2E: the reachable shapes the planner actually routes.
# =========================================================================== #
class TestRoutedFilterPathE2E:
    """The planner routes these filters into the ``regions``-rooted CTE. The two
    ACCEPT cases prove reachable filters land in the CTE WHERE; the two REJECT
    cases prove the pre-existing rejections still fire (and are what the new
    ColumnSqlKey guard is made symmetric with)."""

    async def test_plain_column_path_ending_at_target_renders(self) -> None:
        """A plain-column filter whose path ENDS at the target (``customers_v2.
        regions.name``) re-roots to the local ``regions`` alias and lands in the
        CTE WHERE."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.regions.name = 'x'"],
        )
        sql = await _gen(query, regions_extra=_REGIONS_EXTRA)
        _assert_valid_sql(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "regions.name = 'x'" in cm_body, cm_body

    async def test_derived_column_path_ending_at_target_renders(self) -> None:
        """A DERIVED-column filter whose path ends at the target
        (``customers_v2.regions.pop_x2``, owned by ``regions``) expands its
        ``Column.sql`` rooted at the local ``regions`` alias — the multi-hop
        ColumnSqlKey ACCEPT case the guard must not reject."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.regions.pop_x2 > 5"],
        )
        sql = await _gen(query, regions_extra=_REGIONS_EXTRA)
        _assert_valid_sql(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "CAST(regions.population * 2 AS DOUBLE PRECISION) > 5" in cm_body, cm_body

    async def test_intermediate_hop_plain_column_filter_dropped(self) -> None:
        """DEV-1836: a plain-column filter on the INTERMEDIATE hop
        (``customers_v2.status`` — unreachable from the ``regions`` aggregate
        root) is dropped from the producer with a warning, superseding the
        pre-existing intermediate-hop rejection."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.status = 'x'"],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sql = await _gen(query, regions_extra=_REGIONS_EXTRA)
        _assert_valid_sql(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "status" not in cm_body, cm_body
        assert any(
            issubclass(w.category, UnreachableFilterDroppedWarning)
            and "customers_v2.status" in str(w.message)
            for w in caught
        ), [str(w.message) for w in caught]

    async def test_derived_column_owned_by_other_model_filter_dropped(self) -> None:
        """DEV-1836: a derived-column filter owned by the intermediate model
        (``customers_v2.ltv_x2`` — unreachable from the ``regions`` root) is
        dropped from the producer with a warning, superseding the pre-existing
        model-ownership rejection."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.ltv_x2 > 5"],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sql = await _gen(query, regions_extra=_REGIONS_EXTRA)
        _assert_valid_sql(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "ltv_x2" not in cm_body, cm_body
        assert any(
            issubclass(w.category, UnreachableFilterDroppedWarning)
            and "customers_v2.ltv_x2" in str(w.message)
            for w in caught
        ), [str(w.message) for w in caught]


# =========================================================================== #
# Layer 2 — direct call: the binder-unreachable inconsistent key (the new guard).
# =========================================================================== #
