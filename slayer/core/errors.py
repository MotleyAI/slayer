"""Public error types raised by the SLayer core/engine/storage layers — kept in
``slayer.core`` so callers catch them without importing engine/storage internals."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Tuple

if TYPE_CHECKING:
    from slayer.engine.schema_drift import ToDeleteEntry  # noqa: F401


class SlayerError(Exception):
    """Base for SLayer-specific errors — catch to isolate intentional failures from driver/IO errors."""


class AmbiguousModelError(SlayerError):
    """A bare model name resolves to ≥2 datasources with no priority winner; the surface-neutral message lets each surface append its own remediation."""

    def __init__(self, name: str, candidates: list[str]) -> None:
        self.name = name
        self.candidates = list(candidates)
        super().__init__(
            f"Model '{name}' exists in multiple datasources: "
            f"{sorted(self.candidates)}. Specify a data_source or set a "
            f"datasource priority to disambiguate."
        )


class EntityResolutionError(SlayerError):
    """An entity ref can't resolve to canonical ``<datasource>.<model>[.<leaf>]`` form; distinct from ``AmbiguousModelError`` (the model leg of bare-name resolution)."""


class MemoryNotFoundError(SlayerError):
    """A memory id does not exist in storage."""

    def __init__(self, memory_id: str) -> None:
        self.memory_id = str(memory_id)
        # Back-compat alias for callers that still use ``.identifier``.
        self.identifier = self.memory_id
        super().__init__(f"No memory with id '{self.memory_id}'.")


class SchemaDriftError(SlayerError):
    """A query failed and ``validate_models`` blamed schema drift; carries touched models, the filtered ``to_delete`` payload, and the DBAPI cause (as ``__cause__``)."""

    def __init__(
        self,
        models: list[str],
        to_delete: list[Any],
        original: BaseException,
    ) -> None:
        self.models = list(models)
        self.to_delete = list(to_delete)
        super().__init__(
            f"Schema drift detected on models {sorted(self.models)}. "
            f"Run validate_models to inspect the {len(self.to_delete)} "
            f"pending delete(s). Original error: {original}"
        )
        self.__cause__ = original


class ColumnCycleError(SlayerError, ValueError):
    """A derived ``Column.sql`` chain contains a cycle; carries it as ordered ``(model, column)`` tuples."""

    def __init__(self, cycle: list[tuple[str, str]]) -> None:
        self.cycle: list[tuple[str, str]] = list(cycle)
        chain = " → ".join(f"{m}.{c}" for m, c in self.cycle)
        super().__init__(f"Circular column reference detected: {chain}")


# Stage-5 errors below build their message via ``_format_error_message`` for a
# stable ``str()``: ``ClassName: summary`` first line (grep/snapshot-stable),
# then optional ``at`` / ``scope`` / ``suggestion`` rows. Those subclassing
# ``ValueError`` do so for back-compat with ``except ValueError`` call sites.


def _format_error_message(
    *,
    cls_name: str,
    summary: str,
    location: str | None = None,
    scope: str | None = None,
    suggestion: str | None = None,
    extras: List[Tuple[str, str]] | None = None,
) -> str:
    """Build the stable stage-5 error message; ``extras`` adds bespoke key/value rows."""
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
    """A bare or dotted reference cannot be resolved in the current scope."""

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


class ModeASqlParseError(SlayerError, ValueError):
    """A free-SQL (Mode-A) fragment could not be parsed — now a loud failure (it used to fail soft, silently dropping joins); carries the fragment and its ``location``."""

    def __init__(
        self,
        fragment: str,
        location: str,
        reason: str | None = None,
    ) -> None:
        self.fragment = fragment
        self.location = location
        self.reason = reason
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Cannot parse SQL fragment {fragment!r}.",
            scope=location if reason is None else f"{location}: {reason}",
            suggestion=(
                "Mode-A surfaces take raw SQL for the target dialect. Check "
                "the fragment for balanced parentheses and quotes, and that "
                "any '{variable}' placeholders were supplied."
            ),
        ))


class AmbiguousReferenceError(SlayerError, ValueError):
    """A reference matches multiple candidates in scope."""

    def __init__(self, name: str, candidates: List[str]) -> None:
        self.name = name
        self.candidates = sorted(candidates)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Reference {name!r} has multiple candidates.",
            extras=[("candidates", repr(self.candidates))],
        ))


class IllegalScopeReferenceError(SlayerError, ValueError):
    """A reference is syntactically rejected by the scope kind (``__`` in a Mode-B ``ModelScope`` ref; a dotted ref against a flat ``StageSchema``)."""

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
    """A filter contains a raw ``OVER(...)`` window (directly or via a ``Column.sql``); use a rank-family transform instead."""

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
    """An aggregation can't apply to a column: type-bucket (``sum`` on TEXT), primary-key, or ``allowed_aggregations`` whitelist violation."""

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
    """A Mode-B function call is not in the ``SCALAR_FUNCTIONS`` allowlist, transform registry, or model aggregation set (REST maps it to 400)."""

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
    """Named-measure expansion exceeded the depth limit (default 32; ``SLAYER_MEASURE_EXPANSION_DEPTH``)."""

    def __init__(self, chain: List[str], limit: int = 32) -> None:
        self.chain = list(chain)
        self.limit = limit
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Named-measure expansion exceeded depth (limit={limit}).",
            extras=[("chain", " → ".join(self.chain))],
        ))


class MeasureCycleError(SlayerError, ValueError):
    """Named-measure expansion encountered a cycle."""

    def __init__(self, chain: List[str]) -> None:
        self.chain = list(chain)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary="Cyclic reference in named-measure expansion.",
            extras=[("chain", " → ".join(self.chain))],
        ))


class DuplicateMeasureNameError(SlayerError, ValueError):
    """Two measures in one query declare the same explicit ``name``."""

    def __init__(self, name: str, occurrences: List[str]) -> None:
        self.name = name
        self.occurrences = list(occurrences)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Measure name {name!r} is declared more than once.",
            extras=[("occurrences", repr(self.occurrences))],
        ))


class MeasureNameCollidesWithColumnError(SlayerError, ValueError):
    """A declared measure ``name`` matches a source column, so the alias-form filter would bind to the column, not the aggregate."""

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
    """A formula's canonical alias (``amount_sum`` for ``amount:sum``) shadows a source column on the same model."""

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
    """A host filter referenced slots unreachable from a cross-model CTE's root, so it was dropped from the CTE (still applied to host rows). Visibility warning, not an error."""

    def __init__(self, filter_text: str, reason: str) -> None:
        self.filter_text = filter_text
        self.reason = reason
        super().__init__(
            f"Filter {filter_text!r} dropped from cross-model CTE "
            f"(unreachable from CTE root): {reason}"
        )


class BroadcastGrainWarning(UserWarning):
    """A cross-model aggregate's implicit grain lost a dimension (not attributable from its root) to broadcasting; result grain unchanged. Visibility warning, not an error."""

    def __init__(self, measure: str, reason: str) -> None:
        self.measure = measure
        self.reason = reason
        super().__init__(
            f"Metric {measure!r} broadcast across an unattributable "
            f"dimension: {reason}"
        )


class RenderContextMissingFacilityError(SlayerError, ValueError):
    """A ValueKey render needed a facility its render context lacked; fails closed rather than degrading quietly (silent fallbacks drifted the old renderer copies)."""

    def __init__(
        self,
        key_kind: str,
        facility: str,
        detail: str | None = None,
    ) -> None:
        self.key_kind = key_kind
        self.facility = facility
        self.detail = detail
        suffix = f" ({detail})" if detail else ""
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=(
                f"Rendering a {key_kind} requires the {facility!r} "
                f"render-context facility, which was not supplied{suffix}."
            ),
        ))


class IdCollisionError(SlayerError, ValueError):
    """YAML storage saving an entity whose id differs from an existing one only by case (they collide as filenames on case-insensitive filesystems)."""

    _LABELS = {
        "model": "Model name",
        "datasource": "Datasource name",
        "memory": "Memory id",
    }

    def __init__(
        self,
        *,
        kind: str,
        new_id: str,
        existing_id: str,
        data_source: str | None = None,
    ) -> None:
        self.kind = kind
        self.new_id = new_id
        self.existing_id = existing_id
        self.data_source = data_source
        label = self._LABELS.get(kind, "Id")
        scope = f" in datasource '{data_source}'" if data_source else ""
        super().__init__(
            f"{label} '{new_id}' conflicts with existing '{existing_id}'"
            f"{scope} (differs only by case). Rename or delete one."
        )


class IdentifierCollisionError(SlayerError, ValueError):
    """Two distinct SLayer-generated names collapse onto one identifier after dialect length-fitting — raised loudly rather than corrupting a result set."""

    def __init__(
        self,
        *,
        first: str,
        second: str,
        emitted: str,
        dialect: str,
        limit: int | None,
        namespace: str = "identifier",
    ) -> None:
        self.first = first
        self.second = second
        self.emitted = emitted
        self.dialect = dialect
        self.limit = limit
        self.namespace = namespace
        super().__init__(
            f"{namespace} collision on dialect '{dialect}' "
            f"(max_identifier_bytes={limit}): {first!r} and {second!r} both "
            f"emit as {emitted!r}. Rename one of the underlying models or "
            f"columns to break the tie."
        )


class ForcedFilterError(SlayerError):
    """The session policy's ruleset can't be safely applied to a query; carries the offending ``table``/``column`` (either may be ``None``)."""

    def __init__(
        self,
        message: str,
        *,
        table: str | None = None,
        column: str | None = None,
    ) -> None:
        self.table = table
        self.column = column
        super().__init__(message)


class DistinctDimensionValuesError(SlayerError, ValueError):
    """``distinct_dimension_values=False`` (raw rows, no top-level ``GROUP BY``) conflicts with any aggregation or a query with no projected columns."""


class UnresolvableOrderColumnError(SlayerError, ValueError):
    """An ``order`` item references a column not bindable to the query's FROM scope (usually an unprojected joined column whose join was never resolved); rejected at compile time rather than emitting failing SQL."""

    def __init__(self, *, column: str, qualifier: str) -> None:
        self.column = column
        self.qualifier = qualifier
        super().__init__(
            f"ORDER BY column '{qualifier}.{column}' cannot be resolved: it is not a "
            f"projected field, a base column, or a column on a join that is in scope. "
            f"Project it (add to dimensions/measures), reference it in a filter, or "
            f"order by a projected field instead."
        )


class UnresolvableDimensionJoinError(SlayerError, ValueError):
    """A dimension dotted path that is not a valid direct-join chain and can't be uniquely
    routed to its target. ``__str__`` is computed from the fields so a ``suggested_path`` set after construction shows."""

    def __init__(
        self,
        *,
        reference: str,
        root_model: str,
        reason: str | None = None,
        available_joins: "list[str] | None" = None,
        suggested_path: str | None = None,
    ) -> None:
        self.reference = reference
        self.root_model = root_model
        self.reason = reason
        self.available_joins = available_joins
        self.suggested_path = suggested_path
        super().__init__()

    def __str__(self) -> str:
        msg = (
            f"Cannot resolve dimension '{self.reference}': not a valid join path "
            f"from '{self.root_model}'."
        )
        if self.reason:
            msg += f" {self.reason}"
        if self.available_joins is not None:
            msg += f" Available joins from '{self.root_model}': {sorted(self.available_joins)}."
        if self.suggested_path:
            msg += f" Did you mean '{self.suggested_path}'?"
        return msg


class LegacyDunderAliasError(SlayerError, ValueError):
    """A ``__``-delimited Mode-A join qualifier that is no longer accepted (``.`` is now the only chain separator) — the legacy split-alias spelling."""

    def __init__(self, *, alias: str, dotted: str, model: str) -> None:
        self.alias = alias
        self.dotted = dotted
        self.model = model
        super().__init__()

    def __str__(self) -> str:
        return (
            f"No joined model named '{self.alias}' on '{self.model}'. The "
            f"'__'-delimited split-alias form is no longer accepted; write the "
            f"join path with dots instead: '{self.dotted}'."
        )
