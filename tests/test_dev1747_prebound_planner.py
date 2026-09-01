"""DEV-1747 §5.4 — the pre-bound planner seam, and zero text round-trips.

Rerooting once built its nested plan by SERIALIZING typed keys back to formula
text and re-parsing the whole thing (measures, filters) from strings — a P-E
violation that laundered structural identity through text and re-derived it.
§5.4 replaces it with ``PreboundQuery`` — the typed product of ``plan_query``'s
bind block — handed straight back to ``plan_query(prebound=…)``, which then
skips binding entirely.

Two properties are asserted:

* **Equivalence** — a prebound plan is structurally identical to the plan the
  text path produces, across a battery of shapes. Without this the seam is
  just a second planner.
* **No round-trips** — the parser is never invoked inside the reroot subtree.
  The spy is scoped to the subplan-builder boundary rather than being a global
  counter, because the HOST query legitimately parses.

The carrier guard is the third leg: ``plan_query`` reads ``query.*`` in several
places AFTER the bind block (``distinct_dimension_values``, ``source_model``,
``dimensions``, ``time_dimensions``, ``limit``, ``offset``). A prebound call
that forgets one would silently inherit a default, so the reroot passes a
STRICT carrier that raises on any attribute the seam has not approved.

Refs: DEV-1747 (D1), DEV-1742 §5.4 / P-D / P-E.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.keys import Phase

from slayer.core.enums import TimeGranularity
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.stage_planner import plan_query
from tests._dev1747_fixtures import dev1747_bundle
from slayer.core.keys import AggregateKey, ColumnKey, reroot_value_key
from slayer.engine import cross_model_planner
from slayer.engine import stage_planner
from slayer.engine.binding import BoundFilter
from slayer.engine.prebound import PreboundQuery
from slayer.engine.stage_planner import StrictQueryCarrier
from slayer.engine.stage_planner import bind_query_inputs

_SHAPES = {
    "plain": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[{"formula": "amount:sum", "name": "rev"}],
    ),
    "cross_model": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum", "name": "rev"},
            {"formula": "customers.spend:sum", "name": "cs"},
        ],
    ),
    "rerooted": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="name", model="customers.regions")],
        measures=[
            {"formula": "amount:sum", "name": "rev"},
            {"formula": "customers.spend:sum", "name": "cs"},
        ],
    ),
    "filtered": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[{"formula": "amount:sum", "name": "rev"}],
        filters=["customers.tier == 'gold'"],
    ),
    "time_dimension": SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2024-01-01", "2024-12-31"],
        )],
        measures=[{"formula": "amount:sum", "name": "rev"}],
    ),
    "ordered_paginated": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[{"formula": "amount:sum", "name": "rev"}],
        order=[OrderItem(column=ColumnRef(name="rev"), direction="desc")],
        limit=5,
        offset=2,
    ),
    "raw_rows": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        distinct_dimension_values=False,
        limit=3,
    ),
    # The HOST-rooted nested plan (``cte_root_model == "orders"``): a derived
    # column whose ``Column.sql`` crosses a join. A different set of helpers
    # builds this one, so the equivalence battery and the P-J sentinels both
    # need it alongside the target-rooted "rerooted" shape.
    "host_rooted": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[{"formula": "amount:sum", "name": "rev"}],
        order=[OrderItem(column=ColumnRef(name="cust_region"), direction="asc")],
    ),
}


def _plan(query: SlayerQuery):
    return plan_query(query=query, bundle=dev1747_bundle())


# ---------------------------------------------------------------------------
# Group 1 — the seam exists and is faithful
# ---------------------------------------------------------------------------
class TestPreboundEquivalence:
    @pytest.mark.parametrize("shape", sorted(_SHAPES))
    def test_prebound_plan_matches_the_text_plan(self, shape: str) -> None:
        """Extract the bind product from a normal plan, feed it back through
        ``prebound=``, and require an identical plan. Any divergence means the
        seam is a second planner rather than the same one."""

        query = _SHAPES[shape]
        expected = _plan(query)
        prebound = bind_query_inputs(query=query, bundle=dev1747_bundle())
        actual = plan_query(
            query=query, bundle=dev1747_bundle(), prebound=prebound,
        )
        assert actual.model_dump() == expected.model_dump()

    def test_prebound_is_optional(self) -> None:
        """Every existing caller passes no ``prebound`` and must be unaffected."""
        assert _plan(_SHAPES["plain"]).order == []

    def test_bind_query_inputs_carries_the_post_bind_scalars(self) -> None:
        """The fields ``plan_query`` reads off ``query`` AFTER binding. A
        missing one silently inherits a default — which is exactly the class of
        bug the strict carrier below is meant to make impossible."""

        prebound = bind_query_inputs(
            query=_SHAPES["ordered_paginated"], bundle=dev1747_bundle(),
        )
        assert prebound.limit == 5
        assert prebound.offset == 2
        assert prebound.n_dims == 1
        assert prebound.n_time_dimensions == 0

    def test_prebound_carries_the_resolved_main_time_key(self) -> None:
        """``_resolve_main_time_dimension`` takes the whole ``query``; the
        prebound path must supply the resolved key instead of re-deriving it
        from a text carrier."""

        prebound = bind_query_inputs(
            query=_SHAPES["time_dimension"], bundle=dev1747_bundle(),
        )
        assert prebound.main_time_key is not None


# ---------------------------------------------------------------------------
# Group 2 — no formula-text round-trips in the reroot path
# ---------------------------------------------------------------------------
class TestNoTextRoundTrip:
    def _reroot_query(self) -> SlayerQuery:
        return _SHAPES["rerooted"]

    def test_reroot_does_not_parse_inside_the_subplan_boundary(
        self, monkeypatch,
    ) -> None:
        """Scoped spy: the HOST query parses legitimately, so a global counter
        would be meaningless. The sentinel raises only once the reroot has
        begun building its nested plan."""

        # DEV-1836: rerooting is now the target-rooted producer synthesis
        # (``_synthesize_cross_model_producer``), which builds its nested plan
        # from a typed ``PreboundQuery`` and never re-parses formula text.
        real_builder_calls: list[int] = []
        original = stage_planner._synthesize_cross_model_producer

        def _wrapped(**kwargs):
            real_builder_calls.append(1)

            def _boom(*_a, **_kw):
                raise AssertionError(
                    "the reroot path parsed formula text — §5.4 requires the "
                    "nested PlannedQuery to be built from typed keys"
                )

            # Patch the names as BOUND in each module (both do
            # ``from … import parse_expr``), so patching the defining module
            # would miss them entirely and the test would pass vacuously.
            #
            # A SCOPED context, not ``monkeypatch.undo()``: undo is global to
            # the fixture and would also tear down the outer
            # ``_synthesize_cross_model_producer`` spy, disarming the sentinels
            # for any second or nested reroot call.
            with monkeypatch.context() as inner:
                for module in (cross_model_planner, stage_planner):
                    for symbol in (
                        "parse_expr", "parse_filter_expr",
                        "bind_expr", "bind_filter", "bind_time_dimension",
                    ):
                        if hasattr(module, symbol):
                            inner.setattr(module, symbol, _boom)
                return original(**kwargs)

        monkeypatch.setattr(
            stage_planner, "_synthesize_cross_model_producer", _wrapped,
        )
        _plan(self._reroot_query())
        assert real_builder_calls, "the reroot path never ran — test is vacuous"


    def test_nested_plan_measure_is_a_typed_key_not_a_formula(self) -> None:
        """The observable end state: the producer plan's aggregate slot carries
        the RE-ROOTED typed key, byte-identical to what ``reroot_value_key``
        produces — not something re-derived from a string."""

        plan = _plan(self._reroot_query())
        attach = next(
            a for a in plan.regroup_attach_plans
            if a.producer_root_model == "customers"
        )
        sub_agg = next(
            s for s in attach.producer_plan.aggregate_slots
            if isinstance(s.key, AggregateKey)
        )
        expected = reroot_value_key(
            AggregateKey(
                source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
            ),
            target_path=("customers",),
        )
        assert sub_agg.key == expected


# ---------------------------------------------------------------------------
# Group 3 — the strict carrier guard
# ---------------------------------------------------------------------------
class TestStrictCarrier:
    def test_unapproved_attribute_access_raises(self) -> None:
        """The guard Codex asked for: if ``plan_query`` grows a new post-bind
        ``query.*`` read and the seam does not carry it, the reroot must FAIL
        rather than silently plan against a default."""

        carrier = StrictQueryCarrier(source_model="orders")
        with pytest.raises(AttributeError):
            _ = carrier.some_field_the_seam_never_approved

    def test_approved_attributes_pass_through(self) -> None:

        carrier = StrictQueryCarrier(source_model="orders")
        assert carrier.source_model == "orders"

    def test_reroot_actually_constructs_the_strict_carrier(
        self, monkeypatch,
    ) -> None:
        """Wiring check — the guard only guards if the reroot actually uses it.

        Recorded at runtime: the name appearing in the module source proves
        nothing about the live path (an import, a comment, or a branch that is
        never taken all satisfy a grep).
        """

        built: list = []

        class _Recording(StrictQueryCarrier):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                built.append(self)

        # DEV-1836: the carrier is constructed inside the producer synthesis in
        # ``stage_planner`` (the ``plan_query(query=StrictQueryCarrier(...))``
        # call), so the spy patches the name as bound there.
        monkeypatch.setattr(stage_planner, "StrictQueryCarrier", _Recording)
        _plan(_SHAPES["rerooted"])
        assert built, (
            "the reroot built no StrictQueryCarrier — it is still handing the "
            "nested planner a full SlayerQuery, so an unapproved post-bind "
            "``query.*`` read would silently inherit a default"
        )

    def test_the_nested_planner_receives_the_carrier_not_a_text_query(
        self, monkeypatch,
    ) -> None:
        """The seam's boundary condition. A nested ``plan_query`` given a real
        ``SlayerQuery`` would re-bind formula text no matter how the keys were
        built upstream. DEV-1836: the producer is planned via a nested
        ``plan_query(query=StrictQueryCarrier(...))`` — spy the module-global
        ``plan_query`` so only the NESTED calls (not the import-bound top call
        in ``_plan``) are recorded."""

        seen: list = []
        original = stage_planner.plan_query

        def _recording(*, query, **kwargs):
            seen.append(query)
            return original(query=query, **kwargs)

        monkeypatch.setattr(stage_planner, "plan_query", _recording)
        _plan(_SHAPES["rerooted"])
        assert any(isinstance(q, StrictQueryCarrier) for q in seen), (
            f"the nested planner was never handed the typed carrier: {seen!r}"
        )
        assert not any(isinstance(q, SlayerQuery) for q in seen), (
            f"the nested planner was handed a SlayerQuery ({seen!r}); §5.4 "
            f"requires the typed pre-bound carrier"
        )


class TestPreboundQueryInvariants:
    """The seam's job is to make a malformed hand-off impossible, so its own
    shape has to be checked rather than trusted (Codex).

    Every count on ``PreboundQuery`` is a LIST-SLICE bound and the filter texts
    are read positionally with ``zip``. Both failure modes are silent: a
    negative count slices from the other end, and a short text list truncates
    the routing loop. Neither raises on its own.
    """

    @staticmethod
    def _filter(text: str = "status == 'A'"):
        return BoundFilter(
            value_key=ColumnKey(path=(), leaf="status"),
            phase=Phase.ROW,
            referenced_keys=(ColumnKey(path=(), leaf="status"),),
        )

    def test_filter_texts_must_be_parallel(self) -> None:

        filters = [self._filter(), self._filter()]
        with pytest.raises(ValidationError) as exc:
            PreboundQuery(
                bound_filters=filters, bound_filter_texts=["only one"],
            )
        assert "parallel" in str(exc.value)

    def test_parallel_lists_are_accepted(self) -> None:
        """The control — the guard must not reject the valid shape."""

        pq = PreboundQuery(
            bound_filters=[self._filter()], bound_filter_texts=["status == 'A'"],
        )
        assert len(pq.bound_filter_texts) == len(pq.bound_filters)

    def test_n_date_range_cannot_exceed_the_filters_it_slices(self) -> None:

        filters = [self._filter()]
        with pytest.raises(ValidationError):
            PreboundQuery(
                bound_filters=filters, bound_filter_texts=[None], n_date_range=2,
            )

    def test_the_grain_prefix_cannot_exceed_the_measures(self) -> None:
        """``n_dims`` + ``n_time_dimensions`` is a PREFIX length into
        ``declared_measures``. Slicing past the end returns a shorter list
        rather than raising, so an over-count silently plans fewer dimensions
        than were declared — and misclassifies the measures it does reach on
        the way (CodeRabbit)."""

        with pytest.raises(ValidationError) as exc:
            PreboundQuery(declared_measures=[], n_dims=1, n_time_dimensions=1)
        assert "grain members" in str(exc.value)

    @pytest.mark.parametrize(
        "field", ["n_date_range", "n_dims", "n_time_dimensions"],
    )
    def test_slice_bounds_cannot_be_negative(self, field: str) -> None:
        """``bound_filters[:-1]`` is not an empty slice — it keeps everything
        but the last element. A negative count would therefore drop a filter
        or a dimension and report nothing."""

        with pytest.raises(ValidationError):
            PreboundQuery(**{field: -1})
