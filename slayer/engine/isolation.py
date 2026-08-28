"""One plan-time decision about how an aggregate is isolated (P-C).

An aggregate that needs its own rows — because it crosses inputs, orders its
own rows (first/last), or carries its own frame (windowed) — is compiled as a
plan-shaped CTE rooted where its rows live and joined back on the query grain.
The host base contains only purely-local aggregates, so host cardinality never
changes.

*Whether* an aggregate needs that, and *where* its CTE is rooted, used to be
decided by three predicates inlined in the planner's aggregate loop. They ran in
a fixed order and each knew about the others by omission — the windowed skip
existed because a windowed measure would otherwise trip the crossing-input
trigger, and the crossing-input trigger excluded path-bearing sources because
the target-rooted branch had already claimed them. Reading any one of them meant
reading all three.

They are one function here, returning one value. Nothing about the decision
changed: the same inputs produce the same kind, which is what
``tests/test_dev1746_isolation_classifier.py`` pins.

This is also where cardinality-aware inlining will land (DEV-1688).
:func:`may_inline_crossing_inputs` is the seam — hardcoded ``False``, so every
crossing aggregate isolates, which is today's behaviour. When a future change
lets a provably 1:1 crossing input stay inline, it changes there and every
isolation kind sees it at once, rather than in the four places that used to
decide independently. The render-time counterpart is ``ScopeFrame.may_inline``,
which guards the projection boundary for individual values; this one guards
whole aggregates.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, List, Sequence, Set, Tuple

from slayer.core.keys import AggregateKey
from slayer.engine.aggregate_input_paths import compute_aggregate_input_join_paths

if TYPE_CHECKING:  # pragma: no cover — typing only, keeps the import leaf clean
    from slayer.engine.planned import ValueSlot
    from slayer.engine.source_bundle import ResolvedSourceBundle

__all__ = [
    "IsolationKind",
    "classify_isolation",
    "may_inline_crossing_inputs",
]


class IsolationKind(str, Enum):
    """How one aggregate slot is compiled."""

    #: Purely local — renders inline in the host base SELECT.
    NONE = "none"
    #: Its own ``_wm_`` CTE: a host-rooted range join carrying its own frame.
    WINDOWED = "windowed"
    #: Its own ``_cm_`` CTE rooted at the TARGET the aggregate's source names.
    TARGET_ROOTED = "target_rooted"
    #: Its own ``_cm_`` CTE rooted at the HOST, because a LOCAL aggregate's
    #: inputs (a ``Column.filter``, the source's ``Column.sql``, an arg or a
    #: kwarg) cross a join and so need their own rows.
    HOST_ROOTED = "host_rooted"
    #: Its own ``_rk_`` CTE rooted at the HOST: a ``first`` / ``last`` whose
    #: rows live on the host but which needs its OWN ROW ORDERING.
    RANKED_HOST = "ranked_host"
    #: Its own ``_rk_`` CTE rooted at the TARGET the aggregate's source names.
    RANKED_TARGET = "ranked_target"

    @property
    def needs_own_cte(self) -> bool:
        return self is not IsolationKind.NONE

    @property
    def is_ranked(self) -> bool:
        """Whether this kind compiles to a ranked (``first``/``last``) CTE."""
        return self in (IsolationKind.RANKED_HOST, IsolationKind.RANKED_TARGET)


def may_inline_crossing_inputs(crossed_paths: Sequence[Tuple[str, ...]]) -> bool:  # NOSONAR(S1172) — crossed_paths is the documented DEV-1688 seam; the cardinality-aware decision reads it, hardcoded False until then.
    """Whether an aggregate whose inputs cross ``crossed_paths`` may stay in the
    host base instead of being isolated into its own CTE.

    Hardcoded ``False``: a crossing input is isolated, always. Inlining one is
    only safe when the crossed join is provably 1:N-free for this aggregate,
    which needs the cardinality metadata DEV-1688 is about. Until then this is
    the single place that answer is given, so the future change has one site
    rather than one per isolation kind.
    """
    return False


def classify_isolation(
    *,
    slot: "ValueSlot",
    windowed_slot_ids: Set[str],
    bundle: "ResolvedSourceBundle",
    disable_host_rooted_isolation: bool = False,
) -> IsolationKind:
    """How ``slot`` is compiled. The single trigger decision.

    ``disable_host_rooted_isolation`` is set when planning a nested sub-plan:
    that sub-plan contains the same crossing measure and would otherwise recurse
    forever, and inside a CTE the crossing input renders inline legally, because
    the CTE is the aggregate's own scope.
    """
    if slot.id in windowed_slot_ids:
        # A windowed measure always compiles to its own ``_wm_`` CTE, even when
        # its ``Column.filter`` crosses a join — that crossing is carried inside
        # the windowed CTE, not by a second isolation on top of it.
        return IsolationKind.WINDOWED

    key = slot.key
    if not isinstance(key, AggregateKey):
        return IsolationKind.NONE

    if key.agg in ("first", "last"):
        # Its own ROW ORDERING is one of the three things P-C says an aggregate
        # can need its own rows for, so a first/last isolates whatever its
        # inputs do — the trigger the pre-B9 classifier had no case for, which
        # is why one first/last used to wrap the whole host base in a ranking
        # instead.
        #
        # Deliberately ahead of the crossing-input branch: a crossing first/last
        # already isolated, and routing it to the ranked CTE rather than the
        # generic one is what lets the ranked scope apply its own predicate and
        # rank in one place. Deliberately ahead of ``disable_host_rooted_isolation``
        # too: that guard exists because a CROSSING aggregate re-appears in its
        # own nested sub-plan and would isolate forever. A ranked plan is
        # RENDERED, never re-planned, and inside a sub-plan it still needs its
        # own row ordering — suppressing it there would leave the sub-plan's
        # first/last with no ranking at all.
        if getattr(key.source, "path", ()):
            return IsolationKind.RANKED_TARGET
        return IsolationKind.RANKED_HOST

    if getattr(key.source, "path", ()):
        # The source names another model: the aggregate's rows live there —
        # UNLESS it is marked host-grain (DEV-1747 D2), which separates where a
        # value is READ from where it is GROUPED. A joined ORDER BY wrap reads
        # through the join but must be computed per HOST row-group, so its
        # crossing IS the path and it belongs on the host-rooted route. Sending
        # it to a target-rooted CTE would collapse it to a scalar CROSS JOIN:
        # every group gets the same value and the sort silently does nothing.
        if getattr(key, "grain", "target") == "host":
            # Same recursion guard as the crossing-input branch below — inside
            # the nested sub-plan the CTE is already this aggregate's own scope,
            # so it renders inline (base-pull) rather than isolating forever.
            if disable_host_rooted_isolation:
                return IsolationKind.NONE
            return IsolationKind.HOST_ROOTED
        # Target-rooted isolation is deliberately NOT suppressed by the guard:
        # inlining a joined SUM into the host base would multiply it by the
        # join's fan-out.
        return IsolationKind.TARGET_ROOTED

    if disable_host_rooted_isolation:
        return IsolationKind.NONE

    # DEV-1829 — a LOCAL partitioned aggregate is migrated to a combined regroup
    # attach (``RegroupAttachPlan(attach_phase="combined")``) and substituted
    # away before the aggregate loop, so none should reach the classifier on the
    # non-suppressed path (a cross-model partitioned aggregate returns
    # TARGET_ROOTED above; a producer sub-plan returns NONE via the guard above).
    # Fail closed: a discovery gap would otherwise silently drop the isolation.
    if key.partition_keys is not None:
        # Explicit raise, not ``assert`` — this guards a silent wrong-grain
        # result, so it must survive ``python -O`` (Codex).
        raise AssertionError(
            f"A local partitioned aggregate {key!r} reached classify_isolation "
            f"without being desugared into a combined regroup producer (DEV-1829)."
        )

    crossed = _crossing_input_paths(key=key, bundle=bundle)
    if not crossed:
        return IsolationKind.NONE
    if may_inline_crossing_inputs(crossed):
        return IsolationKind.NONE
    return IsolationKind.HOST_ROOTED


def _crossing_input_paths(
    *, key: AggregateKey, bundle: "ResolvedSourceBundle",
) -> List[Tuple[str, ...]]:
    """Join paths a LOCAL aggregate's inputs cross.

    A ``Column.filter`` carries its crossings as typed
    ``referenced_join_paths`` from binder time; everything else (the source's
    ``Column.sql``, positional args including an explicit first/last time arg,
    and kwargs — column refs, user template fragments, and non-overridden
    model-default aggregation params) is computed structurally.

    Both sources are UNIONED (DEV-1783): an aggregate whose column filter AND
    whose source/args/kwargs each cross a different join must report both, or
    the isolation decision under-counts and a fan-out-multiplying input inlines.
    Order-stable (filter paths first) and de-duplicated.
    """
    out: List[Tuple[str, ...]] = []
    if key.column_filter_key is not None:
        for p in key.column_filter_key.referenced_join_paths:
            if p not in out:
                out.append(p)
    source_model = getattr(bundle, "source_model", None)
    for p in compute_aggregate_input_join_paths(
        key=key,
        anchor_model=source_model,
        anchor_relation=source_model.name if source_model is not None else "",
        bundle=bundle,
    ):
        if p not in out:
            out.append(p)
    return out
