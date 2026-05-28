"""Public error types raised by the SLayer core/engine/storage layers.

Kept in ``slayer.core`` so callers can catch them without importing engine or
storage internals. Each class is defined with a stable name and signature; the
message format is decided by the layer that raises it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Tuple

if TYPE_CHECKING:
    from slayer.engine.schema_drift import ToDeleteEntry  # noqa: F401


class SlayerError(Exception):
    """Base class for SLayer-specific errors.

    Catching ``SlayerError`` distinguishes our intentional failure modes from
    unexpected ``Exception`` paths (driver errors, IO errors, etc.).
    """


class AmbiguousModelError(SlayerError):
    """Raised when a bare model name resolves to ≥2 datasources and the
    datasource priority list does not pick a unique winner.

    The constructor stores the ambiguous name and the list of candidate
    datasources. The default message is intentionally surface-neutral: it
    states the fact and a generic remediation, but does not reference any
    Python-, REST-, MCP-, or CLI-specific invocation form. Each surface
    appends its own concrete remediation when it catches the error
    (``data_source=...`` query param for REST, the
    ``set_datasource_priority`` MCP tool, the ``slayer datasources
    priority`` CLI subcommand, etc.).
    """

    def __init__(self, name: str, candidates: List[str]) -> None:
        self.name = name
        self.candidates = list(candidates)
        super().__init__(
            f"Model '{name}' exists in multiple datasources: "
            f"{sorted(self.candidates)}. Specify a data_source or set a "
            f"datasource priority to disambiguate."
        )


class EntityResolutionError(SlayerError):
    """Raised when an entity reference cannot be resolved to a canonical
    ``<datasource>.<model>[.<leaf>]`` form (DEV-1357).

    Wraps the spec's resolution-failure cases: unknown segment, ambiguous
    bare column matching multiple models in the priority-winner
    datasource, ``*:count`` invoked outside a query context, and similar.
    Distinct from ``AmbiguousModelError`` (which fires for the model leg
    of bare-name resolution and is reused by the resolver verbatim).
    """


class MemoryNotFoundError(SlayerError):
    """Raised when a memory id does not exist in storage (DEV-1357 /
    DEV-1428).

    Memory ids are non-empty strings (auto-allocated int-shaped, or
    user-supplied like ``"kb.policy.42"``). The unified
    ``forget_memory`` MCP tool / REST endpoint / CLI subcommand surface
    this error when the requested id is unknown.
    """

    def __init__(self, memory_id: str) -> None:
        self.memory_id = str(memory_id)
        # Back-compat alias for callers that still use ``.identifier``.
        self.identifier = self.memory_id
        super().__init__(f"No memory with id '{self.memory_id}'.")


class SchemaDriftError(SlayerError):
    """Raised by ``SlayerQueryEngine.execute()`` when a query fails and the
    failure was attributed to schema drift via ``validate_models``.

    Carries the touched model names, the structured ``to_delete`` payload
    (filtered to those models), and the original DBAPI exception (set as
    ``__cause__`` for tracebacks).
    """

    def __init__(
        self,
        models: List[str],
        to_delete: List[Any],
        original: BaseException,
    ) -> None:
        self.models = list(models)
        self.to_delete = list(to_delete)
        super().__init__(
            f"Schema drift detected on models {sorted(self.models)}. "
            f"Run validate_models to inspect the {len(self.to_delete)} "
            f"pending delete(s)."
        )
        self.__cause__ = original


class ColumnCycleError(SlayerError, ValueError):
    """Raised when a derived ``Column.sql`` chain contains a cycle (DEV-1410).

    Carries the cycle as an ordered list of ``(model_name, column_name)``
    tuples reflecting the recursion order in which the cycle was discovered.

    Multi-inherits ``ValueError`` so existing call sites that catch
    ``ValueError`` (or use ``pytest.raises(ValueError)`` for the legacy
    compile-time cycle raise) continue to work unchanged.
    """

    def __init__(self, cycle: List[Tuple[str, str]]) -> None:
        self.cycle: List[Tuple[str, str]] = list(cycle)
        chain = " → ".join(f"{m}.{c}" for m, c in self.cycle)
        super().__init__(f"Circular column reference detected: {chain}")


# ---------------------------------------------------------------------------
# DEV-1450 stage-5 errors — typed, stable str() format.
#
# All error classes below build their message via ``_format_error_message``
# so ``str(error)`` follows the documented snapshot-friendly shape::
#
#     <ErrorName>: <one-line summary>
#       at <location>
#       scope: <short scope summary>
#       suggestion: <did-you-mean>
#
# Each indented line is optional. The first line ALWAYS starts with the
# class name so log greps and snapshot tests bind to a stable prefix.
# ---------------------------------------------------------------------------


def _format_error_message(
    *,
    cls_name: str,
    summary: str,
    location: str | None = None,
    scope: str | None = None,
    suggestion: str | None = None,
    extras: List[Tuple[str, str]] | None = None,
) -> str:
    """Build the stable error-message string used by stage-5 error classes.

    ``extras`` lets a class add bespoke key/value rows after the summary
    while keeping the leading ``ClassName:`` token intact.
    """
    lines = [f"{cls_name}: {summary}"]
    if location:
        lines.append(f"  at {location}")
    if scope:
        lines.append(f"  scope: {scope}")
    for k, v in (extras or []):
        lines.append(f"  {k}: {v}")
    if suggestion:
        lines.append(f"  suggestion: {suggestion}")
    return "\n".join(lines)


class UnknownReferenceError(SlayerError, ValueError):
    """A bare or dotted reference cannot be resolved in the current scope.

    Multi-inherits ``ValueError`` (like :class:`ColumnCycleError`) so the
    pre-existing call sites and tests that catch ``ValueError`` for a failed
    reference / model resolution keep working after the DEV-1450 cutover
    replaced the legacy ``ValueError`` resolution paths with this typed error.
    """

    def __init__(
        self,
        name: str,
        scope_kind: str,
        scope_summary: str,
        suggestion: str | None = None,
    ) -> None:
        self.name = name
        self.scope_kind = scope_kind
        self.scope_summary = scope_summary
        self.suggestion = suggestion
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Cannot resolve reference {name!r}.",
            scope=f"{scope_kind}: {scope_summary}",
            suggestion=suggestion,
        ))


class AmbiguousReferenceError(SlayerError, ValueError):
    """A reference matches multiple candidates in scope and can't pick one.

    Multi-inherits ``ValueError`` for back-compat (see
    :class:`UnknownReferenceError`).
    """

    def __init__(self, name: str, candidates: List[str]) -> None:
        self.name = name
        self.candidates = sorted(candidates)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Reference {name!r} has multiple candidates.",
            extras=[("candidates", repr(self.candidates))],
        ))


class IllegalScopeReferenceError(SlayerError, ValueError):
    """A reference is syntactically rejected by the current scope kind.

    Examples: ``__`` in a Mode-B ``ModelScope`` ref (use the dotted form);
    a dotted ref against a ``StageSchema`` (downstream stages see a flat
    namespace, no join syntax).

    Multi-inherits ``ValueError`` for back-compat (see
    :class:`UnknownReferenceError`).
    """

    def __init__(self, name: str, scope_kind: str, reason: str) -> None:
        self.name = name
        self.scope_kind = scope_kind
        self.reason = reason
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Reference {name!r} is not legal in this scope.",
            scope=scope_kind,
            extras=[("reason", reason)],
        ))


class IllegalWindowInFilterError(SlayerError, ValueError):
    """A filter contains a raw ``OVER(...)`` window expression, or refers
    to a ``Column.sql`` whose body contains a window function (DEV-1369 /
    DEV-1336 — predicate promotion was removed). Use a rank-family
    transform instead.

    Multi-inherits ``ValueError`` (like :class:`UnknownReferenceError`) so
    the pre-existing call sites and tests that catch ``ValueError`` for the
    legacy windowed-filter rejection keep working after the cutover.
    """

    def __init__(
        self,
        filter_expr: str,
        source: str,
        suggestion: str = "use a rank-family transform (e.g. `rank(<measure>) <= N`).",
    ) -> None:
        self.filter_expr = filter_expr
        self.source = source
        self.suggestion = suggestion
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary="Window expressions are not allowed in filters.",
            extras=[
                ("expr", repr(filter_expr)),
                ("source", source),
            ],
            suggestion=suggestion,
        ))


class AggregationNotAllowedError(SlayerError, ValueError):
    """An aggregation cannot apply to a column.

    Covers type-bucket violations (``sum`` on TEXT), primary-key
    restrictions (only ``count`` / ``count_distinct``), and explicit
    ``Column.allowed_aggregations`` whitelist violations.

    Subclasses ``ValueError`` (like the other resolution-time errors in
    this module) so callers wrapping the engine in ``except ValueError``
    keep catching aggregation-gating failures — the legacy enrichment
    pipeline raised a bare ``ValueError`` here.
    """

    def __init__(self, column: str, agg: str, reason: str) -> None:
        self.column = column
        self.agg = agg
        self.reason = reason
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Aggregation {agg!r} is not allowed on column {column!r}.",
            extras=[("reason", reason)],
        ))


class UnknownFunctionError(SlayerError, ValueError):
    """A function call in Mode B is not in the ``SCALAR_FUNCTIONS`` allowlist,
    the transform registry, or the model's aggregation set (C12).

    Subclasses ``ValueError`` (like the other binding-time errors here) so
    the REST ``ValueError -> 400`` mapping and ``except ValueError`` callers
    keep catching it — the legacy enrichment pipeline raised a bare
    ``ValueError`` for this case.
    """

    _DEFAULT_SUGGESTION = "move the call to a derived Column.sql (Mode A)."

    def __init__(
        self,
        name: str,
        location: str,
        suggestion: str | None = None,
    ) -> None:
        self.name = name
        self.location = location
        self.suggestion = suggestion or self._DEFAULT_SUGGESTION
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Function {name!r} is not allowed in Mode B.",
            location=location,
            suggestion=self.suggestion,
        ))


class MeasureRecursionLimitError(SlayerError, ValueError):
    """Named-measure expansion exceeded the configurable depth limit
    (default 32; ``SLAYER_MEASURE_EXPANSION_DEPTH``).

    ValueError-derived for REST/caller parity with the other binding-time
    errors (the legacy pipeline raised a bare ``ValueError``).
    """

    def __init__(self, chain: List[str], limit: int = 32) -> None:
        self.chain = list(chain)
        self.limit = limit
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Named-measure expansion exceeded depth (limit={limit}).",
            extras=[("chain", " → ".join(self.chain))],
        ))


class MeasureCycleError(SlayerError, ValueError):
    """Named-measure expansion encountered a cycle.

    ValueError-derived for REST/caller parity with the other binding-time
    errors (the legacy pipeline raised a bare ``ValueError``).
    """

    def __init__(self, chain: List[str]) -> None:
        self.chain = list(chain)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary="Cyclic reference in named-measure expansion.",
            extras=[("chain", " → ".join(self.chain))],
        ))


class DuplicateMeasureNameError(SlayerError, ValueError):
    """Two measures in the same query declare the same explicit ``name``
    (DEV-1443).

    ValueError-derived for REST/caller parity with the other binding-time
    errors (the legacy pipeline raised a bare ``ValueError``).
    """

    def __init__(self, name: str, occurrences: List[str]) -> None:
        self.name = name
        self.occurrences = list(occurrences)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Measure name {name!r} is declared more than once.",
            extras=[("occurrences", repr(self.occurrences))],
        ))


class MeasureNameCollidesWithColumnError(SlayerError, ValueError):
    """A declared measure ``name`` matches a source column on the model
    (DEV-1443) — the alias-form filter would silently bind to the source
    column instead of the aggregate.

    ValueError-derived for REST/caller parity with the other binding-time
    errors (the legacy pipeline raised a bare ``ValueError``).
    """

    def __init__(self, name: str, model: str) -> None:
        self.name = name
        self.model = model
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=(
                f"Declared measure name {name!r} matches a source column on "
                f"model {model!r}."
            ),
        ))


class CanonicalAliasShadowsColumnError(SlayerError, ValueError):
    """A formula's canonical alias (e.g., ``amount_sum`` for ``amount:sum``)
    shadows a source column on the same model (DEV-1443).

    ValueError-derived for REST/caller parity with the other binding-time
    errors (the legacy pipeline raised a bare ``ValueError``).
    """

    def __init__(self, formula: str, canonical: str, model: str) -> None:
        self.formula = formula
        self.canonical = canonical
        self.model = model
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=(
                f"Canonical alias {canonical!r} for formula {formula!r} "
                f"shadows a source column on model {model!r}."
            ),
        ))


class UnreachableFilterDroppedWarning(UserWarning):
    """A host filter referenced slots that aren't reachable from a
    cross-model CTE's root, so the filter was dropped from the CTE.
    The host query still applies the filter to its own rows; this is a
    visibility/debug warning, not an error.
    """

    def __init__(self, filter_text: str, reason: str) -> None:
        self.filter_text = filter_text
        self.reason = reason
        super().__init__(
            f"Filter {filter_text!r} dropped from cross-model CTE "
            f"(unreachable from CTE root): {reason}"
        )
