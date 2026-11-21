"""Public error types raised by the SLayer core/engine/storage layers — kept in
``slayer.core`` so callers catch them without importing engine/storage internals."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, List, Sequence, Tuple

from typing_extensions import deprecated

from slayer.core.enums import TimeGranularity

if TYPE_CHECKING:
    from slayer.core.relationship_walker import OrientedRelationship  # noqa: F401
    from slayer.live_schema.schema_drift import ToDeleteEntry


class SlayerError(ValueError):
    """Base for SLayer-specific errors — catch to isolate intentional failures from
    driver/IO errors. A ``ValueError`` subclass so every intentional
    slayer failure is caught by ``except ValueError`` / REST-400 call sites and by
    tests that pin a mode/typing refusal as a ``ValueError`` — the per-error
    ``(SlayerError, ValueError)`` classes remain valid (redundant but harmless)."""


class AmbiguousDatasetError(SlayerError):
    """A bare dataset name resolves to ≥2 datasources with no priority winner; the surface-neutral message lets each surface append its own remediation."""

    def __init__(self, name: str, candidates: list[str]) -> None:
        self.name = name
        self.candidates = list(candidates)
        super().__init__(
            f"Dataset '{name}' exists in multiple datasources: "
            f"{sorted(self.candidates)}. Specify a data_source or set a "
            f"datasource priority to disambiguate."
        )


class AmbiguousRelationshipPathError(SlayerError):
    """A hop between two datasets is spanned by two or more edges and no edge
    name disambiguates it, so traversal fails closed in both directions rather
    than silently picking the first stored edge. ``candidates`` holds the
    competing oriented edges; the message names each edge's declaring dataset,
    name (if any), key pairs, and cardinality, plus the remediation."""

    def __init__(
        self,
        *,
        source_dataset: str,
        target_dataset: str,
        candidates: "Sequence[OrientedRelationship]",
        token: str | None = None,
        context: str | None = None,
    ) -> None:
        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.candidates = list(candidates)
        self.token = token
        lines = [
            (f"{context}: " if context else "")
            + f"Ambiguous relationship hop {source_dataset!r} → {target_dataset!r}: "
            f"{len(self.candidates)} edges connect these datasets and no edge "
            f"name resolves it."
        ]
        for c in self.candidates:
            pairs = ", ".join(f"{a}={b}" for a, b in c.column_pairs)
            named = f" name={c.name!r}" if c.name else ""
            lines.append(
                f"  - declared on {c.declaring_dataset!r}{named}: "
                f"pairs [{pairs}], cardinality {c.cardinality}"
            )
        lines.append(
            "Give one edge a `name` and use it as the path segment to "
            "disambiguate."
        )
        super().__init__("\n".join(lines))


class QueryExtensionError(SlayerError):
    """A query extension that cannot apply: a relationship conflicting with the relationship graph, or an INNER relationship the population cannot reach."""


class EntityResolutionError(SlayerError):
    """An entity ref can't resolve to canonical ``<datasource>.<dataset>[.<leaf>]`` form; distinct from ``AmbiguousDatasetError`` (the dataset leg of bare-name resolution)."""


class DatasetNotFoundError(SlayerError):
    """A query names a dataset that does not exist in storage; ``context`` names where it was named."""

    def __init__(self, name: str, *, context: str | None = None) -> None:
        self.name = name
        super().__init__(f"{context}: dataset '{name}' not found" if context else f"Dataset '{name}' not found")


class MemoryNotFoundError(SlayerError):
    """A memory id does not exist in storage."""

    def __init__(self, memory_id: str) -> None:
        self.memory_id = str(memory_id)
        super().__init__(f"No memory with id '{self.memory_id}'.")


class SchemaDriftError(SlayerError):
    """A query failed and schema drift in the datasets it read explains it; carries the drifted datasets, their
    ``to_delete`` entries, the report-only ``findings`` and the DBAPI cause (as ``__cause__``)."""

    def __init__(
        self,
        datasets: list[str],
        to_delete: "Sequence[ToDeleteEntry]",
        original: BaseException,
        findings: list[Any] | None = None,
    ) -> None:
        self.datasets = list(datasets)
        self.to_delete = list(to_delete)
        self.findings = list(findings or [])
        notes = "".join(f" {getattr(f, 'summary', f)}" for f in self.findings)
        super().__init__(
            f"Schema drift detected on datasets {sorted(self.datasets)}. "
            f"Run validate_datasets to inspect the {len(self.to_delete)} "
            f"pending delete(s).{notes} Original error: {original}"
        )
        self.__cause__ = original


class FieldSqlStatementError(SlayerError):
    """A field's native SQL is a statement, or several, not one expression."""

    def __init__(self, *, field: str, sql: str) -> None:
        self.field = field
        super().__init__(
            f"Field {field!r}: its SQL must be one SQL expression, not a statement or several ({sql!r}); a "
            f"parenthesised scalar subquery is an expression."
        )


class FieldExpressionError(SlayerError):
    """A field's or metric's ``expression`` has no form it can run, or two entries for one dialect."""

    def __init__(self, *, owner: str, reason: str, suggestion: str | None = None) -> None:
        self.owner = owner
        super().__init__(f"{owner}: {reason}" + (f" {suggestion}" if suggestion else ""))


class DatasetFieldCycleError(SlayerError, ValueError):
    """A derived ``DatasetField.sql`` chain contains a cycle; carries it as ordered ``(dataset, field)`` tuples."""

    def __init__(self, cycle: list[tuple[str, str]]) -> None:
        self.cycle: list[tuple[str, str]] = list(cycle)
        chain = " → ".join(f"{m}.{c}" for m, c in self.cycle)
        super().__init__(f"Circular field reference detected: {chain}")


class DatasetFilterCycleError(SlayerError, ValueError):
    """A dataset's filters read its own dataset; carries the ordered dataset ``cycle``."""

    def __init__(self, cycle: Sequence[str]) -> None:
        self.cycle: list[str] = list(cycle)
        super().__init__(
            f"Circular dataset filters: {' → '.join(self.cycle)}. A dataset's filters read the "
            f"filtered rows of every dataset they reach through a relationship, so these datasets' filtered rows depend on "
            f"themselves; drop the filter that closes the cycle."
        )


class DerivedFieldFanningError(SlayerError, ValueError):
    """A derived ``DatasetField.sql``/``DatasetField.filter`` reference provably crosses a fanning hop from its declaring dataset — a set per row, not a field; carries ``field``/``dataset``/``hop``/``reference``."""

    def __init__(self, *, field: str, dataset: str, hop: str, reference: str) -> None:
        self.field = field
        self.dataset = dataset
        self.hop = hop
        self.reference = reference
        super().__init__(
            f"Derived field {field!r} on dataset {dataset!r} references {reference!r}, "
            f"crossing a fanning relationship hop to {hop!r}: it is a set per row, not a "
            f"field of {dataset!r}. Aggregate the target field (<aggregation>({reference})) "
            f"or filter by it; if the hop is really to-one, declare its cardinality "
            f"(many_to_one/one_to_one) or a covering unique key."
        )


class CircularRelationshipPathError(SlayerError, ValueError):
    """A relationship path revisits a dataset already on it (``a → b → a``) — a definite
    malformation, like ambiguity, which the shared walker raises. ``None`` from the
    walker now means only an unknown/unloaded hop. Carries the ``reference`` (full
    user spelling), ``root_model``, the ``revisited`` dataset, the revisiting ``hop``
    token and the ``via`` dataset it leaves; an optional declaring ``field``."""

    def __init__(
        self, *, reference: str, root_dataset: str, revisited: str,
        hop: str, via: str, field: str | None = None,
    ) -> None:
        self.reference = reference
        self.root_dataset = root_dataset
        self.revisited = revisited
        self.hop = hop
        self.via = via
        self.field = field
        super().__init__(self._message())

    def _message(self) -> str:
        msg = (
            f"Circular relationship path detected resolving {self.reference!r} from "
            f"{self.root_dataset!r}: hop {self.hop!r} revisits dataset {self.revisited!r}."
        )
        if self.field is not None:
            msg += f" (in the definition of field {self.field!r})"
        return msg


class DerivedFieldCircularError(CircularRelationshipPathError):
    """A derived ``DatasetField.sql``/``DatasetField.filter`` whose path revisits a dataset on it —
    not a function of its declaring dataset's row; ``dataset`` aliases ``root_model``."""

    def __init__(
        self, *, field: str, reference: str, root_dataset: str,
        revisited: str, hop: str, via: str,
    ) -> None:
        super().__init__(
            reference=reference, root_dataset=root_dataset, revisited=revisited,
            hop=hop, via=via, field=field,
        )

    @property
    def dataset(self) -> str:
        return self.root_dataset

    def _message(self) -> str:
        return (
            f"Derived field {self.field!r} on dataset {self.dataset!r} references "
            f"{self.reference!r}, which revisits dataset {self.revisited!r} (hop "
            f"{self.hop!r} from {self.via!r}): a relationship path never revisits a dataset on "
            f"it, so this is not a field of {self.dataset!r}. Reference the field on "
            f"{self.revisited!r} directly if you mean this row's value, or declare the "
            f"aggregate on {self.via!r}, which reaches {self.revisited!r} forward."
        )


class DerivedFieldAmbiguousError(AmbiguousRelationshipPathError):
    """A derived ``DatasetField.sql``/``DatasetField.filter`` reference crossing a hop that two or more edges span."""

    def __init__(
        self, *, field: str, dataset: str, reference: str, error: AmbiguousRelationshipPathError,
    ) -> None:
        self.field = field
        self.dataset = dataset
        self.reference = reference
        super().__init__(
            source_dataset=error.source_dataset, target_dataset=error.target_dataset,
            candidates=error.candidates, token=error.token,
            context=f"Derived field {field!r} on dataset {dataset!r} references {reference!r}",
        )


class MissingDriverError(SlayerError, ImportError):
    """A datasource's driver module or SQLAlchemy dialect plugin is not installed; ``hint`` says what to install."""

    def __init__(
        self, *, datasource_name: str, ds_type: str | None, missing: str, hint: str, detail: str,
    ) -> None:
        self.datasource_name = datasource_name
        self.ds_type = ds_type
        self.missing = missing
        self.hint = hint
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=(
                f"Datasource {datasource_name!r} (type {ds_type!r}) needs {missing!r}, "
                f"which could not be loaded: {detail}"
            ),
            suggestion=hint,
        ))
        self.name = missing


class UnsupportedDriverError(SlayerError):
    """A datasource's URL names a driver SLayer cannot serve the type through; ``hint`` names one it can."""

    def __init__(self, *, datasource_name: str, ds_type: str | None, driver: str, reason: str, hint: str) -> None:
        self.datasource_name = datasource_name
        self.ds_type = ds_type
        self.driver = driver
        self.hint = hint
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Datasource {datasource_name!r} (type {ds_type!r}) cannot use driver {driver!r}: {reason}",
            suggestion=hint,
        ))


class ReadOnlyConnectionError(SlayerError):
    """A datasource's connection settings would stop SLayer connecting to it read-only."""

    def __init__(self, *, datasource_name: str, reason: str, hint: str) -> None:
        self.datasource_name = datasource_name
        self.hint = hint
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Datasource {datasource_name!r} cannot be connected to read-only: {reason}",
            suggestion=hint,
        ))


class ExplainUnsupportedError(SlayerError):
    """An EXPLAIN its dialect, or the client serving it, cannot run."""


class DatasetSqlValidationError(SlayerError, ValueError):
    """Raw-``sql`` dataset source rejected by its reachable datasource at save time.

    ``ValueError`` subclass so REST 400 / ``except ValueError`` callers catch it;
    names the datasource *type* only, never ``repr(ds)`` (no secret leak)."""

    def __init__(
        self,
        *,
        dataset_name: str,
        data_source: str,
        ds_type: str | None,
        reason: str,
    ) -> None:
        self.dataset_name = dataset_name
        self.data_source = data_source
        self.ds_type = ds_type
        self.reason = reason
        super().__init__(
            f"Dataset {dataset_name!r} has invalid SQL for datasource "
            f"{data_source!r} (type {ds_type!r}): {reason}"
        )


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


class QueryTypeError(SlayerError):
    """Base of the checker's query type errors; only its concrete families are raised."""

    def __init__(
        self,
        *,
        summary: str,
        location: str | None = None,
        scope: str | None = None,
        suggestion: str | None = None,
        extras: List[Tuple[str, str]] | None = None,
    ) -> None:
        if type(self) is QueryTypeError:
            raise TypeError("QueryTypeError is abstract; raise a concrete family class.")
        self.summary = summary
        self.location = location
        self.scope = scope
        self.suggestion = suggestion
        super().__init__(_format_error_message(
            cls_name=type(self).__name__, summary=summary, location=location,
            scope=scope, suggestion=suggestion, extras=extras,
        ))

    def __reduce__(self):
        return _rebuild_query_type_error, (type(self), self.args, self.__dict__)


def _rebuild_query_type_error(cls: "type[SlayerError]", args: tuple, state: dict) -> SlayerError:
    """Unpickle without re-running the keyword-only constructor."""
    exc = cls.__new__(cls)
    Exception.__init__(exc, *args)
    exc.__dict__.update(state)
    return exc


class TimeAxisError(QueryTypeError):
    """A time-ordered or windowed computation lacks a usable time axis."""


class TimeDimensionFieldError(TimeAxisError):
    """A time dimension's field is non-temporal, or re-buckets to a non-nesting granularity."""


class WindowDurationError(QueryTypeError):
    """A malformed ``window=`` duration."""


class PartitionKeyError(QueryTypeError):
    """A ``partition_by=`` key that does not resolve or is not attributable."""


class UnsafeRelationshipInputError(QueryTypeError):
    """An aggregate input read across an unproven or fanning relationship hop."""


class UnanalyzableDependencyError(QueryTypeError):
    """A dependency whose definition cannot be analysed for relationship hops."""


class TransformInputError(QueryTypeError):
    """A transform operand the transform cannot consume."""


class TransformArgumentError(QueryTypeError):
    """A missing, unknown or malformed transform argument."""


class ComputedDimensionError(QueryTypeError):
    """A malformed computed (expression) dimension."""


class AssociationError(QueryTypeError):
    """Distinct-entity association that cannot be applied."""


class ParameterGrainError(QueryTypeError):
    """An aggregation parameter not determined by its operand grain."""


class ReaggregationError(QueryTypeError):
    """A re-aggregation the outer aggregation cannot express."""


class NameCollisionError(QueryTypeError):
    """Two names in one query that collide."""


class DimensionTypeError(QueryTypeError):
    """A field whose type cannot be grouped on."""


class DatasetFilterError(QueryTypeError):
    """A dataset filter referencing something only a query filter may."""


class PositionTypingError(QueryTypeError):
    """A filter conjunct / order target is valid as neither field nor metric."""


class DateOperandTypeError(QueryTypeError):
    """A date function operand that is not DATE/TIMESTAMP, or a ``date_add`` count that is not numeric."""


class TimeLiteralError(QueryTypeError):
    """A string compared with a temporal operand that is not a time point, or a sub-day point against a DATE operand."""


class DistinctDimensionValuesError(QueryTypeError):
    """``distinct_dimension_values=False`` (raw rows) conflicts with an aggregation or an empty projection."""


class UnknownGranularityError(QueryTypeError):
    """A granularity name that is neither built-in nor defined on the query's datasource."""


class TimeSpineError(QueryTypeError):
    """A query shape the time spine cannot take: no lower bound, spine rows counted, the spine field
    outside a bucketed time dimension or a bound, or a stored dataset shadowing ``time_spine``."""


class UnrelatedDatasetError(QueryTypeError):
    """A row-level read of a dataset no relationship path reaches, outside an aggregation or a one-sided filter."""


class CastTypeError(QueryTypeError):
    """A CAST target type the query's dialect cannot express."""


class AggregationFilterError(QueryTypeError):
    """A ``FILTER (WHERE …)`` condition that is not a row-level predicate of its aggregation."""


class ScalarArgumentError(QueryTypeError):
    """A scalar function argument outside its contract: a pattern, path, flag or index the library does not
    take, or a literal outside the function's domain."""


class ScalarOperandTypeError(QueryTypeError):
    """A scalar function or operator over an operand of a type it does not read (a number function over TEXT,
    a JSON read of a non-JSON value, a TEXT literal that is not a number beside a number)."""


class ListTypeError(QueryTypeError):
    """A list where a scalar is required (or the reverse), a malformed list literal, or a list construct on a
    database without native lists."""


class ServerVersionError(QueryTypeError):
    """A function the connected server's version does not provide."""


class UnsupportedServerFunctionError(QueryTypeError):
    """A library function the connected database cannot yet run with its portable meaning."""


class UnreachableDefinitionError(SlayerError, ValueError):
    """A stored ``DatasetField.sql`` / ``DatasetField.filter`` / dataset filter naming a dataset no relationship path reaches from its dataset."""

    def __init__(self, *, dataset: str, reference: str, unreached: str, definition: str | None = None) -> None:
        self.dataset = dataset
        self.reference = reference
        self.unreached = unreached
        self.definition = definition
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"{definition or 'A definition'} on dataset {dataset!r} reads {reference!r}, but no relationship path "
            f"reaches {unreached!r} from {dataset!r}.",
            suggestion=f"Declare a relationship between them, or read {unreached!r} in a query through aggregates.",
        ))

    def labelled(self, definition: str) -> "UnreachableDefinitionError":
        """This error naming ``definition``, unless it already names one."""
        if self.definition is not None:
            return self
        return UnreachableDefinitionError(
            dataset=self.dataset, reference=self.reference, unreached=self.unreached, definition=definition,
        )

    def __reduce__(self):
        return _rebuild_query_type_error, (type(self), self.args, self.__dict__)


class GranularityDefinitionError(SlayerError):
    """A datasource custom-granularity definition breaking a save-time rule."""

    def __init__(self, *, name: str, rule: str) -> None:
        self.name = name
        self.rule = rule
        super().__init__(f"Granularity {name!r}: {rule}.")


class ReservedDatasetNameError(SlayerError):
    """A user dataset named after a built-in dataset."""

    def __init__(self, *, name: str) -> None:
        self.name = name
        super().__init__(
            f"Dataset name {name!r} is reserved for the built-in time spine dataset 'time_spine' (dataset names match in "
            f"any case); choose another name."
        )


class UnrecognizedStrictValueError(SlayerError):
    """A query's deprecated ``strict`` flag holds a value that is neither true nor false."""

    def __init__(self, value: Any) -> None:
        self.value = value
        super().__init__(
            f"Unrecognized strict value {value!r}; strict is deprecated, use "
            f"to_many_handling='error' (one of broadcast|associate|error) instead."
        )

    def __reduce__(self) -> tuple[type, tuple[Any]]:
        return type(self), (self.value,)


class SpellingConflictError(SlayerError):
    """One object spells a concept both the pre-2.0 way (``old``) and the Ossie way (``new``)."""

    def __init__(self, *, old: str, new: str) -> None:
        self.old = old
        self.new = new
        super().__init__(f"Both `{new}` and its pre-2.0 spelling `{old}` are given; keep only `{new}`.")

    def __reduce__(self) -> tuple[Any, ...]:
        return _rebuild_spelling_conflict, (self.old, self.new)


def _rebuild_spelling_conflict(old: str, new: str) -> SpellingConflictError:
    return SpellingConflictError(old=old, new=new)


class UnknownKeyError(SlayerError):
    """An input object carries ``key`` (at ``path``, in ``dataset`` when known), which SLayer does not read; ``keys``
    says what the object takes."""

    def __init__(self, *, key: str, path: str, dataset: str | None = None, keys: str | None = None) -> None:
        self.key = key
        self.path = path
        self.dataset = dataset
        self.keys = keys
        at = f" at {path!r}" if path != key else ""
        where = f" of dataset {dataset!r}" if dataset is not None else ""
        takes = f"; {keys}" if keys else ""
        super().__init__(f"Unknown key {key!r}{at}{where}{takes}.")

    def __reduce__(self) -> tuple[Any, ...]:
        return _rebuild_unknown_key, (self.key, self.path, self.dataset, self.keys)


def _rebuild_unknown_key(key: str, path: str, dataset: str | None, keys: str | None) -> UnknownKeyError:
    return UnknownKeyError(key=key, path=path, dataset=dataset, keys=keys)


class FieldTextError(SlayerError):
    """A query entry's ``field`` is an object where it takes a reference or expression as text."""

    def __init__(self, *, where: str, value: Any) -> None:
        self.where = where
        self.value = value
        super().__init__(f"The {where} `field` takes a reference or an expression as text, got {value!r}.")

    def __reduce__(self) -> tuple[Any, ...]:
        return _rebuild_field_text, (self.where, self.value)


def _rebuild_field_text(where: str, value: Any) -> FieldTextError:
    return FieldTextError(where=where, value=value)


class DefaultTimeDimensionTypeError(SlayerError):
    """A dataset's declared ``default_time_dimension`` names a field of a non-temporal type."""

    def __init__(self, *, dataset: str, field: str, field_type: str) -> None:
        self.dataset = dataset
        self.field = field
        super().__init__(
            f"Dataset {dataset!r}: default_time_dimension {field!r} has type {field_type}; "
            f"it must name a date or time field."
        )


class DuplicateMetricNameError(NameCollisionError):
    """Two metrics in one query declare the same explicit ``name``."""

    def __init__(self, name: str, occurrences: List[str]) -> None:
        self.name = name
        self.occurrences = list(occurrences)
        super().__init__(
            summary="The metric name is declared more than once.",
            location=f"metric {name!r}",
            extras=[("occurrences", repr(self.occurrences))],
        )


class MetricNameCollidesWithFieldError(NameCollisionError):
    """A declared metric ``name`` matches a source field, so the alias-form filter would bind to the field."""

    def __init__(self, name: str, dataset: str, suggestion: str | None = None) -> None:
        self.name = name
        self.dataset = dataset
        super().__init__(
            summary=f"The declared metric name matches a source field on dataset {dataset!r}.",
            location=f"metric {name!r}",
            suggestion=suggestion,
        )


class CanonicalAliasShadowsFieldError(NameCollisionError):
    """An expression's canonical alias (``amount_sum`` for ``sum(amount)``) shadows a source field."""

    def __init__(self, expression: str, canonical: str, dataset: str) -> None:
        self.expression = expression
        self.canonical = canonical
        self.dataset = dataset
        super().__init__(
            summary=f"The canonical alias {canonical!r} shadows a source field on dataset {dataset!r}.",
            location=f"metric {expression!r}",
        )


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


class UnknownMetricError(UnknownReferenceError):
    """A bare query name that is neither an alias of the query nor a metric."""


class UnqualifiedRowCountError(SlayerError, ValueError):
    """``count(*)`` in a query that names no population: a row count names its dataset."""

    def __init__(self, *, suggestion: str | None = None) -> None:
        self.suggestion = suggestion
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary="'count(*)' names no dataset in a query without a population.",
            suggestion=suggestion or "Write count(<dataset>.*), or name the query's population.",
        ))


class RefinementConflictError(SlayerError):
    """A saved-query refinement gives an entry of the saved final stage a different value."""

    def __init__(self, *, field: str, key: str, saved: str, refined: str, suggestion: str) -> None:
        self.field = field
        self.key = key
        self.saved = saved
        self.refined = refined
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"The refinement redefines {field} entry {key!r} of the saved query.",
            extras=[("saved", saved), ("refinement", refined)],
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


class NotReadOnlyQueryError(SlayerError):
    """SQL that must be a read-only query is not one."""

    def __init__(self, *, sql: str) -> None:
        self.sql = sql
        super().__init__(f"Not a read-only query: {sql!r}.")


class AmbiguousReferenceError(SlayerError, ValueError):
    """A reference matches multiple candidates in scope."""

    def __init__(self, name: str, candidates: List[str], suggestion: str | None = None) -> None:
        self.name = name
        self.candidates = sorted(candidates)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Reference {name!r} has multiple candidates.",
            extras=[("candidates", repr(self.candidates))],
            suggestion=suggestion,
        ))


class AmbiguousAggregationError(SlayerError):
    """A call matches several of its dataset's aggregations case-insensitively and none exactly."""

    def __init__(self, *, dataset: str, call: str, candidates: List[str]) -> None:
        self.dataset = dataset
        self.call = call
        self.candidates = sorted(candidates)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Aggregation call {call!r} is ambiguous on dataset {dataset!r}: it matches "
            f"{', '.join(self.candidates)} case-insensitively.",
            suggestion="Spell the call exactly as one of them.",
        ))


class IllegalScopeReferenceError(SlayerError, ValueError):
    """A reference is syntactically rejected by the scope kind (``__`` in a Mode-B ``DatasetScope`` ref; a dotted ref against a flat ``StageSchema``)."""

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
    """A filter contains a raw ``OVER(...)`` window (directly or via a ``DatasetField.sql``); use a rank-family transform instead."""

    def __init__(
        self,
        filter_expr: str,
        source: str,
        suggestion: str = "use a rank-family transform (e.g. `rank(<metric>, direction='desc') <= N`).",
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
    """An aggregation can't apply to a field: type-bucket (``sum`` on TEXT), primary-key, or ``allowed_aggregations`` whitelist violation."""

    def __init__(self, field: str, agg: str, reason: str) -> None:
        self.field = field
        self.agg = agg
        self.reason = reason
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Aggregation {agg!r} is not allowed on field {field!r}.",
            extras=[("reason", reason)],
        ))


class AggregationArgumentError(SlayerError, ValueError):
    """An aggregation argument, or declared parameter, that the rendered aggregation never reads."""


class UnanalyzableAggregationParameterError(SlayerError):
    """An aggregation parameter's SQL text does not parse, or holds an aggregate,
    window function or subquery, so it cannot bind to a value."""

    def __init__(
        self, *, dataset: str | None, aggregation: str, parameter: str, text: str, reason: str,
    ) -> None:
        self.dataset = dataset
        self.aggregation = aggregation
        self.parameter = parameter
        self.text = text
        where = f" on dataset '{dataset}'" if dataset else ""
        super().__init__(
            f"Aggregation '{aggregation}'{where}: parameter '{parameter}' = `{text}` "
            f"cannot be used ({reason}); a parameter must be a field, a literal, or a "
            f"row-level expression over fields."
        )


class UnknownFunctionError(SlayerError, ValueError):
    """A Mode-B function call is not in the ``SCALAR_FUNCTIONS`` allowlist, transform registry, or dataset aggregation set (REST maps it to 400)."""

    _DEFAULT_SUGGESTION = "move the call to a derived DatasetField.sql (Mode A)."

    def __init__(
        self,
        name: str,
        location: str | None,
        suggestion: str | None = None,
        summary: str | None = None,
    ) -> None:
        self.name = name
        self.location = location
        self.suggestion = suggestion or self._DEFAULT_SUGGESTION
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=summary or f"Function {name!r} is not allowed in Mode B.",
            location=location,
            suggestion=self.suggestion,
        ))


class MetricRecursionLimitError(SlayerError, ValueError):
    """Named-metric expansion exceeded the depth limit (default 32; ``SLAYER_METRIC_EXPANSION_DEPTH``)."""

    def __init__(self, chain: List[str], limit: int = 32) -> None:
        self.chain = list(chain)
        self.limit = limit
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Named-metric expansion exceeded depth (limit={limit}).",
            extras=[("chain", " → ".join(self.chain))],
        ))


class MetricCycleError(SlayerError, ValueError):
    """Named-metric expansion encountered a cycle."""

    def __init__(self, chain: List[str]) -> None:
        self.chain = list(chain)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary="Cyclic reference in named-metric expansion.",
            extras=[("chain", " → ".join(self.chain))],
        ))


class VariableError(SlayerError):
    """A ``{name}`` placeholder whose name is invalid or whose variable is undefined."""


class UnresolvedPlaceholderError(SlayerError):
    """A ``{name}`` placeholder reached binding unsubstituted."""

    def __init__(self, *, name: str, expression: str) -> None:
        self.name = name
        self.expression = expression
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"{{{name}}} looks like a variable placeholder, but no value was substituted into it.",
            location=repr(expression),
            suggestion=(
                f"supply {name!r} through `variables` (a saved metric reached through a relationship "
                "is not substituted); write `{{` / `}}` for literal braces."
            ),
        ))


class BroadcastGrainWarning(UserWarning):
    """A cross-dataset aggregate's implicit grain lost a dimension (not attributable from its root) to broadcasting; result grain unchanged. Visibility warning, not an error."""

    def __init__(self, metric: str, reason: str) -> None:
        super().__init__(metric, reason)  # args mirror params so cls(*w.args) reconstructs across pytest-xdist
        self.metric = metric
        self.reason = reason

    def __str__(self) -> str:
        return (
            f"Metric {self.metric!r} broadcast across an unattributable "
            f"dimension: {self.reason}"
        )


class AssociatedGrainWarning(UserWarning):
    """An aggregate resolved by distinct-entity association over unattributable dimension(s); cells' populations may overlap and are not additive. Visibility warning, not an error."""

    def __init__(self, metric: str, dimensions: str) -> None:
        super().__init__(metric, dimensions)  # args mirror params so cls(*w.args) reconstructs across pytest-xdist
        self.metric = metric
        self.dimensions = dimensions

    def __str__(self) -> str:
        return (
            f"Metric {self.metric!r} associated over unattributable dimension(s) "
            f"{self.dimensions}; cell populations may overlap and are not additive."
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
        "dataset": "Dataset name",
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
            f"emit as {emitted!r}. Rename one of the underlying datasets or "
            f"fields to break the tie."
        )


class IdentifierLengthError(SlayerError, ValueError):
    """An emitted identifier exceeds the dialect's byte limit after fitting.

    The always-on emission backstop: a non-exempt over-limit token means
    unaccounted provenance, so generation fails rather than hand the database a
    silently-truncatable name (sql principle 9)."""

    def __init__(
        self,
        *,
        tokens: Sequence[str],
        dialect: str,
        limit: int,
    ) -> None:
        self.tokens = sorted(tokens)
        self.dialect = dialect
        self.limit = limit
        joined = ", ".join(repr(t) for t in self.tokens)
        super().__init__(
            f"emitted identifier(s) exceed max_identifier_bytes={limit} on "
            f"dialect '{dialect}' and are neither fitted nor user-authored: "
            f"{joined}. This is a fitting gap — report it rather than let the "
            f"database truncate silently."
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


class GranularityCallError(SlayerError, ValueError):
    """A time-bucket query entry is malformed or unresolvable: a ``date_trunc`` of the wrong shape, a granularity called like a function, a ``time_dimensions`` string that is not ``date_trunc('<unit>', expr)``, a same-field+granularity metadata conflict, or an order key with no matching projected time dimension."""

    @classmethod
    def wrong_shape(cls, entry: str) -> "GranularityCallError":
        """A time bucket written in any shape other than ``date_trunc('<unit>', expr)``."""
        return cls(
            f"Time bucket {entry!r} must be written date_trunc('<unit>', expr) with one row-level expr, e.g. "
            f"date_trunc('month', created_at); units: {', '.join(g.value for g in TimeGranularity)} or a "
            f"datasource granularity."
        )

    @classmethod
    def no_call_form(cls, unit: str, *, operand: str) -> "GranularityCallError":
        """A granularity name called like a function."""
        return cls(f"{unit}(…) is not a function: write the time bucket as date_trunc('{unit}', {operand}).")


class ReservedFunctionNameError(SlayerError, ValueError):
    """A call of, or a new aggregation named, a reserved function name (``agg``)."""

    def __init__(self, *, name: str, where: str) -> None:
        self.name = name
        super().__init__(
            f"{where}: {name!r} is reserved — AGG (`agg` in any case) names metric evaluation, so it is never a "
            f"function or an aggregation name. A field or metric may still be named agg."
        )


class CaseCollisionError(SlayerError, ValueError):
    """A save would introduce names that differ only in case into one namespace."""

    def __init__(self, *, names: List[str], namespace: str) -> None:
        self.names = list(names)
        super().__init__(
            f"{' and '.join(repr(n) for n in names)} in {namespace} differ only in case; names match in any case, "
            f"so they would collide. Rename one of them."
        )


class CallArgumentError(SlayerError, ValueError):
    """A call's arguments in an order or repetition no signature reads."""


class ColonSpellingError(SlayerError, ValueError):
    """A removed colon spelling (``x:agg``, ``col:gran``); ``twin`` is its functional spelling."""

    def __init__(self, *, span: str, twin: str) -> None:
        self.span = span
        self.twin = twin
        super().__init__(f"The colon spelling {span!r} was removed in SLayer 2.0; write {twin} instead.")


class UnresolvableOrderFieldError(SlayerError, ValueError):
    """An ``order`` item references a field not bindable to the query's FROM scope (usually an unprojected joined field whose relationship was never resolved); rejected at compile time rather than emitting failing SQL."""

    def __init__(self, *, field: str, qualifier: str) -> None:
        self.field = field
        self.qualifier = qualifier
        super().__init__(
            f"ORDER BY field '{qualifier}.{field}' cannot be resolved: it is not a "
            f"projected field, a base field, or a field on a joined dataset that is in scope. "
            f"Project it (add to dimensions/metrics), reference it in a filter, or "
            f"order by a projected field instead."
        )


class UnresolvableDimensionRelationshipError(SlayerError, ValueError):
    """A dimension dotted path that is not a valid direct-relationship chain and can't be uniquely
    routed to its target. ``__str__`` is computed from the fields so a ``suggested_path`` set after construction shows."""

    def __init__(
        self,
        *,
        reference: str,
        root_dataset: str,
        reason: str | None = None,
        available_relationships: "list[str] | None" = None,
        suggested_path: str | None = None,
    ) -> None:
        self.reference = reference
        self.root_dataset = root_dataset
        self.reason = reason
        self.available_relationships = available_relationships
        self.suggested_path = suggested_path
        super().__init__()

    def __str__(self) -> str:
        msg = (
            f"Cannot resolve dimension '{self.reference}': not a valid relationship path "
            f"from '{self.root_dataset}'."
        )
        if self.reason:
            msg += f" {self.reason}"
        if self.available_relationships is not None:
            msg += f" Available relationships from '{self.root_dataset}': {sorted(self.available_relationships)}."
        if self.suggested_path:
            msg += f" Did you mean '{self.suggested_path}'?"
        return msg


class PopulationErrorReason(str, Enum):
    """Why dimension-determined population inference failed closed."""

    TIE = "tie"
    NO_VIABLE_CANDIDATE = "no_viable_candidate"
    EMPTY_DETERMINATION = "empty_determination"
    AMBIGUOUS_PATH = "ambiguous_path"
    AMBIGUOUS_DATASOURCE = "ambiguous_datasource"
    NO_DATASOURCE = "no_datasource"
    SIBLING_STAGE = "sibling_stage"


class PopulationInferenceError(SlayerError, ValueError):
    """A rootless query's population could not be uniquely inferred.

    Carries the ``reason`` kind plus the ``candidates`` / ``datasources`` that
    left it under-determined; ``str()`` is stable-prefixed for snapshots."""

    _SUMMARIES = {
        PopulationErrorReason.TIE: (
            "Multiple minimal populations determine every queried dimension; "
            "name one explicitly as `population`."
        ),
        PopulationErrorReason.NO_VIABLE_CANDIDATE: (
            "No single dataset determines every queried dimension along "
            "provably to-one relationship paths; name a `population` explicitly."
        ),
        PopulationErrorReason.EMPTY_DETERMINATION: (
            "There is nothing to infer a population from: the query has no "
            "dimensions and no field-typed filters. Add a dimension or name a "
            "`population`."
        ),
        PopulationErrorReason.AMBIGUOUS_PATH: (
            "A queried dimension's relationship path is ambiguous from every otherwise "
            "viable population; name a `population` explicitly."
        ),
        PopulationErrorReason.AMBIGUOUS_DATASOURCE: (
            "The referenced datasets exist together in more than one datasource; "
            "pass a `data_source` to disambiguate."
        ),
        PopulationErrorReason.NO_DATASOURCE: (
            "No single datasource holds every referenced dataset."
        ),
        PopulationErrorReason.SIBLING_STAGE: (
            "A rootless stage's dimensions anchor at a sibling stage; name that "
            "sibling as the stage's `population`."
        ),
    }

    def __init__(
        self,
        reason: PopulationErrorReason,
        *,
        candidates: Sequence[str] | None = None,
        datasources: Sequence[str] | None = None,
        detail: str | None = None,
    ) -> None:
        self.reason = reason
        self.candidates = list(candidates or [])
        self.datasources = list(datasources or [])
        extras: List[Tuple[str, str]] = []
        if self.candidates:
            extras.append(("candidates", repr(sorted(self.candidates))))
        if self.datasources:
            extras.append(("datasources", repr(sorted(self.datasources))))
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=self._SUMMARIES[reason],
            scope=detail,
            extras=extras,
        ))


class MaterialisationStageError(SlayerError, ValueError):
    """A ``PlannedQuery`` violates the materialisation-stage invariant: a value is unstaged, or references a value staged later than itself (it would render before its inputs are materialised). Raised at plan time before any SQL is generated."""

    def __init__(self, summary: str) -> None:
        super().__init__(_format_error_message(
            cls_name=type(self).__name__, summary=summary,
        ))


class RelationshipKeyError(SlayerError, ValueError):
    """A relationship key that does not name a declared base field on its side."""

    def __init__(self, *, summary: str, suggestion: str | None = None) -> None:
        super().__init__(_format_error_message(
            cls_name=type(self).__name__, summary=summary, suggestion=suggestion,
        ))


class LegacyDunderAliasError(SlayerError, ValueError):
    """A ``__``-delimited Mode-A relationship qualifier that is no longer accepted (``.`` is now the only chain separator) — the legacy split-alias spelling."""

    def __init__(self, *, alias: str, dotted: str, dataset: str) -> None:
        self.alias = alias
        self.dotted = dotted
        self.dataset = dataset
        super().__init__()

    def __str__(self) -> str:
        return (
            f"No joined dataset named '{self.alias}' on '{self.dataset}'. The "
            f"'__'-delimited split-alias form is no longer accepted; write the "
            f"relationship path with dots instead: '{self.dotted}'."
        )


class UnconvertibleStoredQueryError(SlayerError):
    """A stored query holding a removed form (extension fields, an inline dataset) that cannot be converted."""

    def __init__(self, construct: str) -> None:
        self.construct = construct
        super().__init__(
            f"Stored query cannot be converted: {construct}. Rewrite the per-query field as an expression where it "
            f"is used, or the inline dataset as a stored dataset, and save the document again."
        )


class UnqualifiableReferenceError(SlayerError):
    """A stored query's reference whose pre-2.0 reading has no qualified spelling with the same meaning."""

    def __init__(self, *, reference: str, reason: str) -> None:
        self.reference = reference
        self.reason = reason
        super().__init__(
            f"Stored query cannot be upgraded to qualified references: {reference!r} {reason}. Spell it as a "
            f"reference starting at a dataset and save the document again."
        )


class StoredDocumentLoadError(SlayerError, ValueError):
    """One stored dataset or memory could not be loaded; the original failure is ``__cause__``."""

    def __init__(self, *, kind: str, name: str, data_source: str | None, cause: BaseException) -> None:
        self.kind = kind
        self.name = name
        self.data_source = data_source
        self.cause_text = str(cause)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__, summary=f"{self.document}: {cause}",
        ))

    @property
    def document(self) -> str:
        """``<data_source>.<name>`` for a dataset, ``memory:<id>`` for a memory."""
        return f"{self.data_source}.{self.name}" if self.data_source is not None else f"memory:{self.name}"

    def __reduce__(self):
        return _rebuild_query_type_error, (type(self), self.args, self.__dict__)


class HiddenContentConflictError(SlayerError):
    """A write conflicts with stored parts the caller cannot access; names only the caller's own document."""

    def __init__(self, *, kind: str, name: str) -> None:
        self.kind = kind
        self.name = name
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"The change to {kind} {name!r} conflicts with parts of it you cannot access.",
            suggestion="Ask a caller with full access to make this change.",
        ))


class AccessTagsEditError(SlayerError):
    """A caller without bypass tried to set or change a dataset's ``access_tags``."""

    def __init__(self, *, dataset: str) -> None:
        self.dataset = dataset
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Dataset {dataset!r}: only a caller with full access may set or change access_tags.",
        ))


class UnknownRelationshipTargetError(SlayerError):
    """A saved relationship names a dataset the caller cannot see."""

    def __init__(self, *, dataset: str, target: str) -> None:
        self.dataset = dataset
        self.target = target
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Dataset {dataset!r}: relationship target {target!r} is not a known dataset.",
        ))


class RelationshipTargetCaseError(SlayerError, ValueError):
    """A stored relationship target that reaches a dataset only by case. ``saving`` is the dataset whose save the
    peer's relationship blocks; without it, ``dataset``'s own relationship is refused."""

    def __init__(
        self, *, dataset: str, relationship: str | None, target: str, declared: str, saving: bool = False,
    ) -> None:
        self.dataset = dataset
        self.relationship = relationship
        self.target = target
        self.declared = declared
        edge = f"relationship {relationship!r}" if relationship else f"the unnamed relationship to {target!r}"
        if saving:
            summary = (f"Dataset {declared!r} differs only in case from the target {target!r} of {edge} on dataset "
                       f"{dataset!r}; a stored relationship target names its dataset exactly.")
            suggestion = f"Respell that relationship's target to {declared!r} first, then save {declared!r}."
        else:
            summary = (f"Dataset {dataset!r}: {edge} targets {target!r}, which matches dataset {declared!r} only by case; "
                       f"a stored relationship target names its dataset exactly.")
            suggestion = f"Write the target as {declared!r}."
        super().__init__(_format_error_message(cls_name=type(self).__name__, summary=summary, suggestion=suggestion))


class QueryBackedCycleError(SlayerError, ValueError):
    """A query-backed dataset that references itself, directly or transitively."""

    def __init__(self, *, path: Sequence[str]) -> None:
        self.path = list(path)
        super().__init__(_format_error_message(
            cls_name=type(self).__name__,
            summary=f"Query-backed datasets reference each other in a cycle: {' -> '.join(self.path)}.",
            suggestion="Break the cycle so no query-backed dataset reads itself.",
        ))

_RENAMED_CLASSES = {
    "AmbiguousJoinPathError": "AmbiguousRelationshipPathError",
    "AmbiguousModelError": "AmbiguousDatasetError",
    "CanonicalAliasShadowsColumnError": "CanonicalAliasShadowsFieldError",
    "CircularJoinPathError": "CircularRelationshipPathError",
    "ColumnCycleError": "DatasetFieldCycleError",
    "ColumnSqlStatementError": "FieldSqlStatementError",
    "DerivedColumnAmbiguousError": "DerivedFieldAmbiguousError",
    "DerivedColumnCircularError": "DerivedFieldCircularError",
    "DerivedColumnFanningError": "DerivedFieldFanningError",
    "DuplicateMeasureNameError": "DuplicateMetricNameError",
    "JoinKeyError": "RelationshipKeyError",
    "JoinTargetCaseError": "RelationshipTargetCaseError",
    "MeasureCycleError": "MetricCycleError",
    "MeasureNameCollidesWithColumnError": "MetricNameCollidesWithFieldError",
    "MeasureRecursionLimitError": "MetricRecursionLimitError",
    "ModelFilterCycleError": "DatasetFilterCycleError",
    "ModelFilterError": "DatasetFilterError",
    "ModelNotFoundError": "DatasetNotFoundError",
    "ModelSqlValidationError": "DatasetSqlValidationError",
    "ReservedModelNameError": "ReservedDatasetNameError",
    "TimeDimensionColumnError": "TimeDimensionFieldError",
    "UnknownJoinTargetError": "UnknownRelationshipTargetError",
    "UnresolvableDimensionJoinError": "UnresolvableDimensionRelationshipError",
    "UnresolvableOrderColumnError": "UnresolvableOrderFieldError",
    "UnsafeJoinInputError": "UnsafeRelationshipInputError",
}

if TYPE_CHECKING:
    @deprecated("AmbiguousJoinPathError is deprecated; use AmbiguousRelationshipPathError", category=None)
    class AmbiguousJoinPathError(AmbiguousRelationshipPathError): ...

    @deprecated("AmbiguousModelError is deprecated; use AmbiguousDatasetError", category=None)
    class AmbiguousModelError(AmbiguousDatasetError): ...

    @deprecated("CanonicalAliasShadowsColumnError is deprecated; use CanonicalAliasShadowsFieldError", category=None)
    class CanonicalAliasShadowsColumnError(CanonicalAliasShadowsFieldError): ...

    @deprecated("CircularJoinPathError is deprecated; use CircularRelationshipPathError", category=None)
    class CircularJoinPathError(CircularRelationshipPathError): ...

    @deprecated("ColumnCycleError is deprecated; use DatasetFieldCycleError", category=None)
    class ColumnCycleError(DatasetFieldCycleError): ...

    @deprecated("ColumnSqlStatementError is deprecated; use FieldSqlStatementError", category=None)
    class ColumnSqlStatementError(FieldSqlStatementError): ...

    @deprecated("DerivedColumnAmbiguousError is deprecated; use DerivedFieldAmbiguousError", category=None)
    class DerivedColumnAmbiguousError(DerivedFieldAmbiguousError): ...

    @deprecated("DerivedColumnCircularError is deprecated; use DerivedFieldCircularError", category=None)
    class DerivedColumnCircularError(DerivedFieldCircularError): ...

    @deprecated("DerivedColumnFanningError is deprecated; use DerivedFieldFanningError", category=None)
    class DerivedColumnFanningError(DerivedFieldFanningError): ...

    @deprecated("DuplicateMeasureNameError is deprecated; use DuplicateMetricNameError", category=None)
    class DuplicateMeasureNameError(DuplicateMetricNameError): ...

    @deprecated("JoinKeyError is deprecated; use RelationshipKeyError", category=None)
    class JoinKeyError(RelationshipKeyError): ...

    @deprecated("JoinTargetCaseError is deprecated; use RelationshipTargetCaseError", category=None)
    class JoinTargetCaseError(RelationshipTargetCaseError): ...

    @deprecated("MeasureCycleError is deprecated; use MetricCycleError", category=None)
    class MeasureCycleError(MetricCycleError): ...

    @deprecated("MeasureNameCollidesWithColumnError is deprecated; use MetricNameCollidesWithFieldError", category=None)
    class MeasureNameCollidesWithColumnError(MetricNameCollidesWithFieldError): ...

    @deprecated("MeasureRecursionLimitError is deprecated; use MetricRecursionLimitError", category=None)
    class MeasureRecursionLimitError(MetricRecursionLimitError): ...

    @deprecated("ModelFilterCycleError is deprecated; use DatasetFilterCycleError", category=None)
    class ModelFilterCycleError(DatasetFilterCycleError): ...

    @deprecated("ModelFilterError is deprecated; use DatasetFilterError", category=None)
    class ModelFilterError(DatasetFilterError): ...

    @deprecated("ModelNotFoundError is deprecated; use DatasetNotFoundError", category=None)
    class ModelNotFoundError(DatasetNotFoundError): ...

    @deprecated("ModelSqlValidationError is deprecated; use DatasetSqlValidationError", category=None)
    class ModelSqlValidationError(DatasetSqlValidationError): ...

    @deprecated("ReservedModelNameError is deprecated; use ReservedDatasetNameError", category=None)
    class ReservedModelNameError(ReservedDatasetNameError): ...

    @deprecated("TimeDimensionColumnError is deprecated; use TimeDimensionFieldError", category=None)
    class TimeDimensionColumnError(TimeDimensionFieldError): ...

    @deprecated("UnknownJoinTargetError is deprecated; use UnknownRelationshipTargetError", category=None)
    class UnknownJoinTargetError(UnknownRelationshipTargetError): ...

    @deprecated(
        "UnresolvableDimensionJoinError is deprecated; use UnresolvableDimensionRelationshipError", category=None,
    )
    class UnresolvableDimensionJoinError(UnresolvableDimensionRelationshipError): ...

    @deprecated("UnresolvableOrderColumnError is deprecated; use UnresolvableOrderFieldError", category=None)
    class UnresolvableOrderColumnError(UnresolvableOrderFieldError): ...

    @deprecated("UnsafeJoinInputError is deprecated; use UnsafeRelationshipInputError", category=None)
    class UnsafeJoinInputError(UnsafeRelationshipInputError): ...


from slayer.core import vocabulary  # noqa: E402  # ALLOW(import-not-top): circular — vocabulary imports errors


def __getattr__(name: str) -> Any:
    return vocabulary.renamed_name(name, namespace=globals(), renames=_RENAMED_CLASSES)
