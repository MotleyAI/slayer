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
  actually routes, proving both that reachable filters route INTO the CTE and
  that the pre-existing intermediate-hop / other-model rejections still fire
  with their exact messages. These pass before and after the fix.
* **Direct-call (the new guard)** — the binder-unreachable inconsistent key,
  which only a direct call to ``_reroot_routed_leaf`` (live) or
  ``_render_filter_value_key_in_target_scope`` (legacy escape hatch) can
  construct. These are red before the guard lands.

The aggregate is ``customers_v2.regions.population:sum`` throughout, so the CTE
target is ``regions`` (a TWO-hop path). That lets a filter path end AT the
target (``customers_v2.regions.*``) or at the intermediate hop
(``customers_v2.*``), which single-hop fixtures cannot express.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import ColumnKey, ColumnSqlKey
from slayer.core.models import Column, ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.generator import SQLGenerator

from tests._cross_model_chain import (
    _countries,
    _customers_v2,
    _extract_cte_body,
    _gen,
    _norm,
    _orders_x,
    _regions,
)
from tests._engine_helpers import _assert_valid_sql

# A derived column ON regions, so a filter ``customers_v2.regions.pop_x2`` binds
# to a ``ColumnSqlKey`` whose path ENDS at the target ``regions``.
_REGIONS_EXTRA = [Column(name="pop_x2", sql="population * 2", type=DataType.DOUBLE)]

# The aggregate whose source is the two-hop path → CTE target is ``regions``.
_TWO_HOP_AGG = ModelMeasure(formula="customers_v2.regions.population:sum")

# Fragments that uniquely identify each rejection branch.
_MSG_COLUMNKEY_INTERMEDIATE = r"intermediate hop"          # ColumnKey branch
_MSG_OTHER_MODEL = r"owned by"                             # existing model check
_MSG_NEW_COLUMNSQLKEY = (                                  # DEV-1769, ticket-specific
    r"DEV-1769: .*derived column 'pop_x2' via an intermediate-hop path"
)


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
        assert "regions.population * 2 > 5" in cm_body, cm_body

    async def test_intermediate_hop_plain_column_filter_raises(self) -> None:
        """A plain-column filter on the INTERMEDIATE hop (``customers_v2.status``
        — path ends at ``customers_v2``, not the ``regions`` target) routes into
        the CTE and hits the pre-existing ColumnKey intermediate-hop raise."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.status = 'x'"],
        )
        with pytest.raises(NotImplementedError, match=_MSG_COLUMNKEY_INTERMEDIATE):
            await _gen(query, regions_extra=_REGIONS_EXTRA)

    async def test_derived_column_owned_by_other_model_filter_raises(self) -> None:
        """A DERIVED-column filter owned by the intermediate model
        (``customers_v2.ltv_x2`` — owned by ``customers_v2``, not the ``regions``
        target) routes into the CTE and hits the pre-existing ColumnSqlKey
        model-ownership raise. The model check fires BEFORE the new path guard,
        so this shape keeps its exact message (ordering the direct tests pin)."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.ltv_x2 > 5"],
        )
        with pytest.raises(NotImplementedError, match=_MSG_OTHER_MODEL):
            await _gen(query, regions_extra=_REGIONS_EXTRA)


# =========================================================================== #
# Layer 2 — direct call: the binder-unreachable inconsistent key (the new guard).
# =========================================================================== #
def _regions_target():
    """The ``regions`` model used as the CTE target for direct renderer calls."""
    return _regions(extra_columns=_REGIONS_EXTRA)


def _direct_bundle() -> ResolvedSourceBundle:
    """A bundle whose ``regions`` carries ``pop_x2`` so the legacy renderer's
    accept path can expand it."""
    return ResolvedSourceBundle(
        source_model=_orders_x(),
        referenced_models=[
            _customers_v2(),
            _regions(extra_columns=_REGIONS_EXTRA),
            _countries(),
        ],
    )


# Hand-built keys against target ``regions``. Only the malformed one must raise.
_VALID_MULTI_HOP = ColumnSqlKey(
    path=("customers_v2", "regions"), model="regions", column_name="pop_x2",
)
_VALID_EMPTY_PATH = ColumnSqlKey(path=(), model="regions", column_name="pop_x2")
# Multi-hop, terminal hop is NOT the target — the shape the new guard catches.
_MALFORMED_MULTI_HOP = ColumnSqlKey(
    path=("orders_x", "customers_v2"), model="regions", column_name="pop_x2",
)
# Owned by another model — keeps failing on the EXISTING model check.
_OTHER_MODEL = ColumnSqlKey(
    path=("customers_v2",), model="customers_v2", column_name="ltv_x2",
)


class TestRerootRoutedLeafDirect:
    """The LIVE seam ``_reroot_routed_leaf`` — the shape only a direct call can
    build (binder never emits ``model != path[-1]``)."""

    def _gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="postgres")

    def test_malformed_multi_hop_path_raises_symmetrically(self) -> None:
        """model == target but path ends at an intermediate hop → the new
        DEV-1769 raise (symmetric with ColumnKey), distinct from the
        model-ownership message."""
        with pytest.raises(NotImplementedError, match=_MSG_NEW_COLUMNSQLKEY):
            self._gen()._reroot_routed_leaf(
                _MALFORMED_MULTI_HOP,
                target_relation="regions", target_model=_regions_target(),
            )

    def test_valid_multi_hop_path_ending_at_target_accepted(self) -> None:
        """path ENDS at the target → accepted, stripped to the local scope."""
        out = self._gen()._reroot_routed_leaf(
            _VALID_MULTI_HOP,
            target_relation="regions", target_model=_regions_target(),
        )
        assert isinstance(out, ColumnSqlKey)
        assert out.path == ()
        assert out.model == "regions"
        assert out.column_name == "pop_x2"

    def test_empty_path_accepted_unchanged(self) -> None:
        """An empty path is already local — returned unchanged."""
        out = self._gen()._reroot_routed_leaf(
            _VALID_EMPTY_PATH,
            target_relation="regions", target_model=_regions_target(),
        )
        assert out is _VALID_EMPTY_PATH

    def test_other_model_still_raises_on_model_check_first(self) -> None:
        """A key owned by another model raises the EXISTING ownership message —
        the model check must fire before the new path guard (Codex F7)."""
        with pytest.raises(NotImplementedError, match=_MSG_OTHER_MODEL):
            self._gen()._reroot_routed_leaf(
                _OTHER_MODEL,
                target_relation="regions", target_model=_regions_target(),
            )

    def test_plain_columnkey_intermediate_hop_still_raises(self) -> None:
        """The ColumnKey side is unchanged — its intermediate-hop raise is what
        the ColumnSqlKey guard is made symmetric with."""
        with pytest.raises(NotImplementedError, match=_MSG_COLUMNKEY_INTERMEDIATE):
            self._gen()._reroot_routed_leaf(
                ColumnKey(path=("customers_v2",), leaf="status"),
                target_relation="regions", target_model=_regions_target(),
            )


class TestLegacyTargetScopeRendererDirect:
    """The LEGACY escape hatch ``_render_filter_value_key_in_target_scope`` —
    production-dead except the first/last path (PR 6 deletes it), so tested by
    direct call. Unlike the live seam it EXPANDS immediately rather than
    returning a re-rooted key, so the accept case needs a real bundle."""

    def _gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="postgres")

    def _render(self, value_key):
        return self._gen()._render_filter_value_key_in_target_scope(
            value_key=value_key,
            target_relation="regions",
            target_model=_regions_target(),
            planned_query=None,
            bundle=_direct_bundle(),
        )

    def test_malformed_multi_hop_path_raises_symmetrically(self) -> None:
        with pytest.raises(NotImplementedError, match=_MSG_NEW_COLUMNSQLKEY):
            self._render(_MALFORMED_MULTI_HOP)

    def test_valid_multi_hop_path_ending_at_target_expands(self) -> None:
        """path ends at target → expands the derived ``Column.sql`` rooted at
        the local ``regions`` alias (the accept case the guard must not reject).
        Asserting the ``regions`` qualifier — not just the formula — pins that
        expansion roots at ``target_relation``, which is the behaviour under
        protection."""
        out = self._render(_VALID_MULTI_HOP)
        assert out is not None
        assert "regions.population * 2" in _norm(out.sql(dialect="postgres")), out.sql()

    def test_empty_path_expands(self) -> None:
        out = self._render(_VALID_EMPTY_PATH)
        assert out is not None
        assert "regions.population * 2" in _norm(out.sql(dialect="postgres")), out.sql()

    def test_other_model_still_raises_on_model_check_first(self) -> None:
        with pytest.raises(NotImplementedError, match=_MSG_OTHER_MODEL):
            self._render(_OTHER_MODEL)
