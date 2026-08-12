"""DEV-1783 — PR #286 review G1 production-correctness fixes.

Items 3 and 5, which have no natural home in an existing test module:

* item 3 — ``ntile``'s ``n`` must route through ``_normalise_periods`` in the
  generator, rejecting bool / non-integral Decimal rather than truncating
  (``NTILE(2)`` for ``Decimal("2.7")``) or emitting ``NTILE(True)``. The binder
  already gates this at bind time (``_ensure_positive_integer``); this pins the
  generator's own defense-in-depth, so the test drives the render method with a
  hand-built ``TransformKey`` the binder would have rejected.
* item 5 — a HOST-ROOTED route must leave ``where_ids`` empty: the sub-plan
  carries these predicates as its own filters, so listing them as forward-CTE
  ``where_filter_ids`` (an instruction to the host base to SKIP them) is wrong.

Items 1/2/6/7 live in their domain modules (reachability / pagination /
isolation-classifier / date-range-warning). Item 4 is REJECTED as invalid — its
"narrow the clear" fix would regress
``test_dev1747_reroot_filter_routing.py::TestRerootedAggregateRefFilter``,
turning a deliberate ``NotImplementedError`` into a silent wrong answer.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from slayer.core.keys import ColumnKey, Phase, TransformKey
from slayer.engine.binding import BoundFilter
from slayer.engine.cross_model_planner import (
    HostFilterRouting,
    _route_host_rooted_filters,
)
from slayer.engine.planned import ValueSlot
from slayer.sql.generator import SQLGenerator


# --------------------------------------------------------------------------- #
# Item 3 — ntile n normalisation in the generator
# --------------------------------------------------------------------------- #
def _render_ntile(n) -> str:
    """Render an ``ntile(revenue:sum, n=<n>)`` transform through the generator's
    window-transform path, with a minimally-materialised input slot."""
    gen = SQLGenerator(dialect="postgres")
    input_key = ColumnKey(path=(), leaf="revenue")
    nt_key = TransformKey(op="ntile", input=input_key, kwargs=(("n", n),))
    in_slot = ValueSlot(
        id="s_in", key=input_key, declared_name="revenue", phase=Phase.ROW,
    )
    nt_slot = ValueSlot(
        id="s_nt", key=nt_key, declared_name="bucket", phase=Phase.POST,
    )
    return gen._render_window_transform_sql(
        slot=nt_slot,
        slots_by_id={"s_in": in_slot, "s_nt": nt_slot},
        slot_id_by_key={input_key: "s_in", nt_key: "s_nt"},
        available_alias_by_slot_id={"s_in": "revenue"},
        planned_query=SimpleNamespace(projection=[]),
    ).sql(dialect="postgres")


class TestNtileNIsNormalised:

    def test_non_integral_decimal_is_rejected_not_truncated(self) -> None:
        """``Decimal("2.7")`` used to truncate to ``NTILE(2)`` silently."""
        n = Decimal("2.7")
        with pytest.raises(ValueError):
            _render_ntile(n)

    def test_bool_is_rejected(self) -> None:
        """``True`` is an ``int`` subclass, so ``isinstance(n, int)`` used to
        wave it through to ``NTILE(...)``."""
        nt_key = TransformKey(op="ntile", input=ColumnKey(path=(), leaf="x"),
                              kwargs=(("n", True),))
        # Guard: only meaningful if the model kept the bool as a bool.
        if dict(nt_key.kwargs).get("n") is not True:
            pytest.skip("bool coerced away by the model; covered by the Decimal case")
        with pytest.raises(ValueError):
            _render_ntile(True)

    def test_a_valid_integer_still_renders(self) -> None:
        sql = _render_ntile(4)
        assert "NTILE(4)" in sql.upper().replace(" ", ""), sql


# --------------------------------------------------------------------------- #
# Item 5 — host-rooted routes leave where_ids empty
# --------------------------------------------------------------------------- #
class TestHostRootedRoutesLeaveWhereIdsEmpty:

    @staticmethod
    def _row_routing(fid: str) -> HostFilterRouting:
        return HostFilterRouting(
            filter_id=fid,
            phase=Phase.ROW,
            text="status = 'paid'",
            bound=BoundFilter(
                value_key=ColumnKey(path=(), leaf="status"), phase=Phase.ROW,
            ),
        )

    def test_where_ids_is_empty_but_applied_is_populated(self) -> None:
        routes = _route_host_rooted_filters(host_filters=[self._row_routing("f1")])
        assert routes.applied == ["f1"], routes
        # ``where_ids`` is an instruction to the host base to SKIP the filter as
        # forward-CTE-delegated. A host-rooted CTE has no forward CTE, so it must
        # stay empty; the sub-plan already carries the predicate.
        assert routes.where_ids == [], routes
        assert routes.having_ids == [], routes
