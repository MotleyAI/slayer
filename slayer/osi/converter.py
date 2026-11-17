"""Convert parsed OSI documents into SLayer datasets.

Each OSI dataset becomes one ``SlayerDataset`` over its live-introspected source.
A current-spec dataset exposes exactly its declared fields; a legacy dataset
overlays its fields on every introspected column. Relationships become
``DatasetRelationship`` entries within their own document; metrics become
``DatasetMetric`` expressions — a dataset-scoped metric on its dataset, a
model-scoped one on the dataset that reaches every dataset it references.
Constructs that cannot be expressed exactly are reported in the
``ConversionResult``, never silently lost.
"""

from __future__ import annotations

import itertools
import logging
from collections.abc import Callable
from typing import Any, Literal, Optional, TypeVar

import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import AmbiguousRelationshipPathError, UnsupportedServerFunctionError
from slayer.core.syntax import parse_expr
from slayer.core.relationship_walker import resolve_hop
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset, relationship_key_error
from slayer.core.names import name_taken
from slayer.core.query import SlayerQuery
from slayer.core.refs import IDENTIFIER_RE as _IDENTIFIER_RE
from slayer.engine.binding import bind_dataset_expression
from slayer.sql.field_expansion import root_scope_field_ids, resolve_ref_target
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type
from slayer.sql.render.parse import fold_source_identifiers
from slayer.live_schema.ingestion import introspect_table_to_dataset
from slayer.live_schema.schema_scope import live_name, requested_schema_ref
from slayer.core.relationship_search import blocking_pairs, shortest_route, shortest_routes
from slayer.importers.column_types import LiveDatasource, LiveSource
from slayer.importers.aggregates import (
    MaterializedField, Unconvertible, contains_args, convert_expression, length_arg, outermost_aggregates,
)
from slayer.importers.physical import PhysicalIndex, live_exact_name, root_physical_columns, unreadable_fields
from slayer.ingest_report import ConversionResult, ConversionWarning
from slayer.database.client import ResultColumnType, SqlClient
from slayer.osi.dialects import Selection, is_native, read, reader_for, render, select_expression
from slayer.osi.models import (
    OSSIE_DATATYPES,
    TEMPORAL_DATATYPES,
    OSIAIContext,
    OSIDataset,
    OSIDocument,
    OSIField,
    OSIMetric,
    OSINonportableExtension,
    OSIRelationship,
    ai_context_to_dict,
    render_description,
)
from slayer.osi import slayer_extension as ext
from slayer.osi.source import ParsedSource, parse_source, resolve_datasource

logger = logging.getLogger(__name__)

_TIME_DIMENSION_TYPES = (DataType.DATE, DataType.TIMESTAMP)


def _typed_field(*, name: str, data_type: DataType, sql: str | None = None, element: DataType | None = None,
                  hidden: bool = False, db_type: str | None = None) -> DatasetField:
    """A field of a probed type; a list's elements are ``element``, else UNKNOWN (a probe cannot see them)."""
    if data_type is not DataType.ARRAY:
        return DatasetField(name=name, sql=sql, datatype=data_type, hidden=hidden, db_type=db_type)
    return DatasetField(name=name, sql=sql, datatype=data_type, element_type=element or DataType.UNKNOWN, hidden=hidden,
                  db_type=db_type)

OwnerOf = Callable[[Optional[str], str], Optional[str]]
Severity = Literal["unconverted", "dropped", "info"]


class OsiConversionError(Exception):
    """Raised when an OSI import set cannot be converted (e.g. duplicate names)."""


def _repeated_names(names: list[str]) -> list[list[str]]:
    """Groups of ``names`` equal up to case that occur more than once, each in declaration order."""
    groups: dict[str, list[str]] = {}
    for name in names:
        groups.setdefault(name.casefold(), []).append(name)
    return [g for g in groups.values() if len(g) > 1]


# Unsafe in a dataset name, which becomes a YAML storage filename. ``__`` is allowed.
_UNSAFE_DATASET_NAME_CHARS = (".", ":", "/", "\\", "\x00")


def _legal_dataset_name(name: str) -> bool:
    if not name or name.strip() != name or not name.strip():
        return False
    return not any(ch in name for ch in _UNSAFE_DATASET_NAME_CHARS)


def _legal_field_name(name: str) -> bool:
    return "." not in name and ":" not in name


def _legal_metric_name(name: str) -> bool:
    return bool(_IDENTIFIER_RE.match(name))


def _bare_field(tree: exp.Expr) -> exp.Identifier | None:
    """The identifier if ``tree`` is one unqualified (possibly quoted) column, else ``None``."""
    if (isinstance(tree, exp.Column) and not tree.table and isinstance(tree.this, exp.Identifier)
            and not tree.args.get("db") and not tree.args.get("catalog")):
        return tree.this
    return None


def _missing_expr_fields(tree: exp.Expr, known: Callable[[exp.Identifier], bool], self_name: str) -> list[str]:
    """Unqualified and self-qualified column names in ``tree`` no source column has.

    Other qualifiers are cross-dataset refs, checked after relationships exist.
    """
    missing = []
    for col in tree.find_all(exp.Column):
        if not isinstance(col.this, exp.Identifier) or (col.table and col.table != self_name) or known(col.this):
            continue
        missing.append(f"{col.table}.{col.name}" if col.table else col.name)
    return missing


def _is_time_role(field: OSIField) -> bool:
    if field.dimension is not None and field.dimension.is_time is not None:
        return field.dimension.is_time
    return field.datatype in TEMPORAL_DATATYPES


def _first_time_dimension(dataset: SlayerDataset) -> str | None:
    return next(
        (c.name for c in dataset.fields
         if (c.meta or {}).get("osi_is_time") and c.datatype in _TIME_DIMENSION_TYPES),
        None,
    )


def _is_source_qualifier(ident: exp.Identifier) -> bool:
    """Whether ``ident`` is the Ossie ``source`` qualifier, which matches case-insensitively quoted or not."""
    return ident.name.casefold() == "source"


def _match_name(ident: exp.Identifier, names: list[str]) -> str:
    """The declared name ``ident`` refers to: quoted exactly, unquoted case-insensitively."""
    if ident.quoted:
        if ident.name in names:
            return ident.name
        raise LookupError(f"{ident.name!r} is not declared")
    hits = [n for n in names if n.lower() == ident.name.lower()]
    if len(hits) > 1:
        raise LookupError(f"{ident.name!r} is ambiguous between {hits}")
    if not hits:
        raise LookupError(f"{ident.name!r} is not declared")
    return hits[0]


def _ref_owner(col: exp.Column, *, qual: list[exp.Identifier], home: str | None, doc_datasets: list[str]) -> str:
    """The dataset whose field ``col`` (qualified by ``qual``) reads; raises ``LookupError`` on a scope violation."""
    if home is not None:
        if qual:
            raise LookupError(f"qualified reference {col.sql()!r}; dataset-scoped metrics "
                              f"reference their own dataset's fields unqualified")
        return home
    if not qual:
        raise LookupError(f"unqualified reference {col.name!r}; model-scoped metrics "
                          f"reference fields as dataset.field")
    return _match_name(qual[0], doc_datasets)


def _list_call(node: exp.Expr, *, dialect: SqlDialect, is_list: Callable[[exp.Expr], bool]) -> exp.Expr:
    """``node`` as SLayer's own list SQL when it is an array function, else ``node``."""
    if isinstance(node, exp.ArrayContains):
        return dialect.build_list_call("contains", contains_args(node, is_list=is_list))
    if (isinstance(node, exp.EQ) and isinstance(node.expression, exp.Any)
            and not isinstance(node.expression.this, (exp.Subquery, exp.Select))):
        return dialect.build_list_call("contains", (node.expression.this, node.this))
    if (values := length_arg(node)) is not None:
        return dialect.build_list_call("length", (values,))
    return node


def _bottom_up(tree: exp.Expr, fn: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """A copy of ``tree`` with ``fn`` applied to every node, children before their parent."""
    tree = tree.copy()
    for node in reversed(list(tree.walk())):
        new = fn(node)
        if new is node:
            continue
        if node is tree:
            tree = new
        else:
            node.replace(new)
    return tree


def _foreign(extensions: Optional[list[OSINonportableExtension]]) -> list[dict[str, Any]]:
    return [{"vendor_name": e.vendor_name, "data": e.data} for e in extensions or [] if not ext.is_slayer(e)]


def _build_meta(
    ctx: Optional[OSIAIContext],
    extensions: Optional[list[OSINonportableExtension]],
    extra: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    meta: dict[str, Any] = dict(extra or {})
    ctx_dict = ai_context_to_dict(ctx)
    if ctx_dict:
        meta["osi_ai_context"] = ctx_dict
    foreign = _foreign(extensions)
    if foreign:
        meta["osi_nonportable_extensions"] = foreign
    return meta or None


_T = TypeVar("_T")
_E = TypeVar("_E", OSIDocument, OSIDataset, OSIField, OSIMetric, OSIRelationship)


class _ExtensionCheck:
    """A document with every extension entry checked: legacy strings decoded, non-objects dropped and reported."""

    def __init__(self, doc: OSIDocument) -> None:
        self.notes: list[ConversionWarning] = []
        self._legacy = False
        datasets = [self._dataset(ds) for ds in doc.datasets]
        self.document = self._entity(doc, label=f"document {doc.name!r}", update={
            "datasets": datasets,
            "relationships": self._each(doc.relationships, lambda r: self._entity(
                r, label=f"relationship {r.name!r}")),
            "metrics": self._each(doc.metrics, lambda m: self._entity(
                m, label=f"metric {m.name!r}", metric_name=m.name)),
        })
        if self._legacy:
            self.notes.append(ConversionWarning(
                message=f"Document {doc.name!r} carries non-portable extensions under the legacy key "
                        f"custom_extensions; they were read as nonportable_extensions.",
                category=_EXTENSION, severity="info", suggestion=_REEXPORT_CURRENT))

    @staticmethod
    def _each(items: Optional[list[_T]], fn: Callable[[_T], _T]) -> Optional[list[_T]]:
        return None if items is None else [fn(x) for x in items]

    def _dataset(self, ds: OSIDataset) -> OSIDataset:
        of = f"of dataset {ds.name!r}"
        return self._entity(ds, label=f"dataset {ds.name!r}", dataset_name=ds.name, update={
            "fields": self._each(ds.fields, lambda f: self._entity(
                f, label=f"field {f.name!r} {of}", dataset_name=ds.name)),
            "metrics": self._each(ds.metrics, lambda m: self._entity(
                m, label=f"metric {m.name!r} {of}", dataset_name=ds.name, metric_name=m.name)),
        })

    def _entity(
            self, entity: _E, *, label: str, dataset_name: str | None = None, metric_name: str | None = None,
            update: Optional[dict[str, Any]] = None) -> _E:
        self._legacy = self._legacy or entity.legacy_key
        kept: list[OSINonportableExtension] = []
        for entry in entity.nonportable_extensions or []:
            data = ext.decoded_legacy(entry.data) if entry.legacy else entry.data
            if ext.checked(vendor_name=entry.vendor_name, data=data) is None:
                self.notes.append(ConversionWarning(
                    dataset_name=dataset_name, metric_name=metric_name, category=_EXTENSION, severity="dropped",
                    message=f"The {entry.vendor_name!r} non-portable extension on the {label} is dropped: it "
                            f"needs a non-blank vendor_name and data that is a JSON object (dates, timestamps, "
                            f"non-finite numbers and non-string keys are not JSON).",
                    suggestion="Fix the entry's data in the document."))
                continue
            kept.append(OSINonportableExtension(vendor_name=entry.vendor_name, data=data))
        return entity.model_copy(update={**(update or {}), "nonportable_extensions": kept or None})


_EXTENSION = "extension"
_REEXPORT_CURRENT = "Re-export the document from SLayer to write the current key."
_EDITED ="The Ossie content was edited after export, so it wins; re-export from SLayer to restore the rest."
_REEXPORT = "Re-export the document from SLayer."
_IMPORT_INPUTS = "Import every dataset it reads."


class OsiConversionResult(ConversionResult):
    """The converted datasets, plus query-backed restorations to save through the engine after them."""
    query_backed: list[SlayerDataset] = Field(default_factory=list)


class _PendingMetric(BaseModel):
    """A metric restored from a ``SLAYER`` payload, added once everything it references exists."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataset: str
    metric: DatasetMetric
    label: str
    #: The metric to convert from its SQL instead, when the restored expression does not validate.
    osi_metric: Optional[OSIMetric] = None
    doc: Optional[OSIDocument] = None
    home: Optional[str] = None


class _MetadataUnavailable(Exception):
    """A dataset's live metadata was not collected, or could not be read."""


class OsiToSlayerConverter:
    """Convert OSI documents into SLayer datasets: ``collect_metadata`` reads the database, ``convert`` is pure."""

    def __init__(
        self,
        documents: list[OSIDocument],
        data_source: str,
        *,
        dialect: str | None = None,
        target_dialect: str | None = None,
    ) -> None:
        checks = [_ExtensionCheck(doc) for doc in documents]
        self.documents = [c.document for c in checks]
        self._extension_notes = [n for c in checks for n in c.notes]
        self.data_source = data_source
        self.dialect = dialect
        self.target_dialect = target_dialect
        # sqlglot name of the datasource dialect every stored raw SQL is rendered in.
        self._dialect: SqlDialect = dialect_for_ds_type(target_dialect)
        self._target = self._dialect.sqlglot_name
        # Every dataset's name: a column a dataset name qualifies is a logical reference.
        self._dataset_names = {ds.name for doc in documents for ds in doc.datasets}
        # Live metadata from ``collect_metadata``: per dataset its introspected dataset (or the failure); the
        # source queries' column types and the derived expressions' probes.
        self._tables: dict[str, SlayerDataset | str] = {}
        self._datasource = LiveDatasource(offline_reason=None, data_source=data_source, db_type=target_dialect,
                                          dialect=self._target)
        self._reset()

    def _reset(self) -> None:
        self._models: dict[str, SlayerDataset] = {}
        # Introspected source columns per dataset, for field validation.
        self._physical: dict[str, dict[str, DatasetField]] = {}
        self._live: dict[str, LiveSource | None] = {}
        self._warnings: list[ConversionWarning] = list(self._extension_notes)
        self._unconverted: list[ConversionWarning] = []
        # (dataset, field) -> the introspected DatasetField a legacy derived overlay replaced,
        # so an invalidated overlay reverts to the physical column.
        self._shadowed: dict[tuple[str, str], DatasetField] = {}
        # Decoded SLAYER payloads (with whether their fingerprint matches), applied after the plain import.
        self._dataset_payloads: dict[str, tuple[ext.DatasetPayload, bool]] = {}
        self._field_payloads: dict[tuple[str, str], tuple[ext.FieldPayload, bool]] = {}
        self._pending: list[_PendingMetric] = []
        self._query_backed: list[SlayerDataset] = []

    # ---- report helpers ----

    def _warn(self, message: str, *, dataset_name: str | None = None,
              metric_name: str | None = None, category: str = "general",
              severity: Severity = "dropped", suggestion: str | None = None) -> None:
        self._warnings.append(ConversionWarning(
            dataset_name=dataset_name, metric_name=metric_name, message=message,
            category=category, severity=severity, suggestion=suggestion,
        ))

    def _unconv(self, message: str, *, metric_name: str, category: str = "metric",
                suggestion: str | None = None) -> None:
        self._unconverted.append(ConversionWarning(
            metric_name=metric_name, message=message, category=category,
            severity="unconverted", suggestion=suggestion,
        ))

    def _note_selection(self, selection: Selection, *, label: str, dataset_name: str | None = None,
                        metric_name: str | None = None) -> None:
        for note in selection.notes:
            self._warn(f"{label}: {note}.", dataset_name=dataset_name, metric_name=metric_name,
                       category="dialect", severity="info")

    # ---- top-level ----

    async def collect_metadata(self, *, client: SqlClient) -> None:
        """Read every dataset's source and every type probe conversion needs through ``client``."""
        if self.target_dialect is None:
            self._dialect = dialect_for_ds_type(client.datasource.type)
            self._target = self._dialect.sqlglot_name
            self._datasource.dialect = self._target
        for doc in self.documents:
            for ds in doc.datasets:
                await self._collect_table(client=client, ds=ds)
        await self._datasource.collect(client=client, convert=self._convert)

    def _read_source(self, ds: OSIDataset) -> ParsedSource | None:
        """``ds``'s source read with the datasource's identifier folding; ``None`` when it does not parse."""
        parsed = parse_source(ds.source, dialect=self._dialect)
        folded = fold_source_identifiers(parsed.query or ds.source.strip(), dialect=self._dialect,
                                         table=not parsed.is_query)
        if folded.unreadable:
            return None
        if parsed.is_query:
            return parsed.model_copy(update={"query": folded.text})
        return parse_source(folded.text, dialect=self._dialect)

    async def _collect_table(self, *, client: SqlClient, ds: OSIDataset) -> None:
        try:
            parsed = self._read_source(ds)
        except Exception:  # noqa: BLE001 — conversion reports an unparseable source itself
            return
        if parsed is None or parsed.table is None:
            return
        schema = parsed.schema_name and live_name(parsed.schema_name, quoted=parsed.schema_quoted)
        try:
            self._tables[ds.name] = await introspect_table_to_dataset(
                client=client, table_name=live_name(parsed.table, quoted=parsed.table_quoted),
                ref=requested_schema_ref(schema, client=client), data_source=self.data_source, dataset_name=ds.name,
            )
        except Exception as exc:  # noqa: BLE001 — per-dataset isolation
            self._tables[ds.name] = str(exc)

    def convert(self) -> OsiConversionResult:
        """Convert from the collected metadata (pure: the database is read by ``collect_metadata``)."""
        return self._convert()

    def _convert(self) -> OsiConversionResult:
        self._reset()
        self._check_duplicate_dataset_names()
        self._check_dataset_namespaces()

        for doc in self.documents:
            for ds in doc.datasets:
                self._build_dataset(ds=ds, doc=doc)
        for doc in self.documents:
            for rel in doc.relationships or []:
                self._build_relationship(rel=rel, doc=doc)

        self._validate_cross_dataset_field_refs()
        self._type_cross_dataset_fields()
        self._restore_dataset_attributes()

        for doc in self.documents:
            self._build_metrics_for(doc)
        self._restore_metrics()
        self._restore_fields()
        self._restore_sources()

        return OsiConversionResult(
            datasets=list(self._models.values()),
            unconverted_metrics=self._unconverted,
            warnings=self._warnings,
            query_backed=self._query_backed,
        )

    def _doc_datasets(self, doc: OSIDocument) -> list[str]:
        return [d.name for d in doc.datasets if d.name in self._models]

    def _build_metrics_for(self, doc: OSIDocument) -> None:
        doc_datasets = self._doc_datasets(doc)
        for ds in doc.datasets:
            if ds.name in self._models:
                for metric in ds.metrics or []:
                    self._metric(metric=metric, doc=doc, home=ds.name, doc_datasets=doc_datasets)
        for metric in doc.metrics or []:
            self._metric(metric=metric, doc=doc, home=None, doc_datasets=doc_datasets)

    def _metric(self, metric: OSIMetric, *, doc: OSIDocument, home: str | None, doc_datasets: list[str]) -> None:
        """Restore ``metric``'s SLayer metric when its payload matches, else convert its SQL."""
        found = self._payload(metric.nonportable_extensions, kind="metric", entity=metric.name,
                              label=f"metric {metric.name!r}", dataset_name=home,
                              ossie=ext.expression_digest(metric.expression))
        if found is not None and found[1]:
            payload = found[0]
            assert isinstance(payload, ext.MetricPayload)
            built = _build_meta(ctx=metric.ai_context, extensions=metric.nonportable_extensions,
                                extra={"osi_datatype": metric.datatype} if metric.datatype else None)
            slayer_metric = payload.metric.model_copy(update={"meta": {**(built or {}), **(payload.metric.meta or {})}
                                                         or None})
            self._pending.append(_PendingMetric(dataset=payload.dataset, metric=slayer_metric, label=f"Metric {metric.name!r}",
                                                 osi_metric=metric, doc=doc, home=home))
            return
        if found is not None:
            self._warn(f"Metric {metric.name!r}: {_EDITED}", dataset_name=home, metric_name=metric.name,
                       category=_EXTENSION, severity="info", suggestion=_EDITED)
        slayer_metric = self._build_metric(metric=metric, doc=doc, home=home, doc_datasets=doc_datasets)
        if found is not None and slayer_metric is not None:
            self._apply_metric_attributes(slayer_metric, payload=found[0])

    @staticmethod
    def _apply_metric_attributes(metric: DatasetMetric, *, payload: ext.Payload) -> None:
        assert isinstance(payload, ext.MetricPayload)
        metric.label = payload.metric.label
        metric.datatype = payload.metric.datatype
        metric.meta = {**(metric.meta or {}), **(payload.metric.meta or {})} or None

    def _check_duplicate_dataset_names(self) -> None:
        seen: set[str] = set()
        dupes: set[str] = set()
        for doc in self.documents:
            for ds in doc.datasets:
                if ds.name in seen:
                    dupes.add(ds.name)
                seen.add(ds.name)
        if dupes:
            raise OsiConversionError(
                f"Duplicate dataset names across the OSI import set: "
                f"{sorted(dupes)}. Dataset names map to SLayer dataset names and "
                f"must be unique within a datasource."
            )

    def _check_dataset_namespaces(self) -> None:
        """A current-spec dataset's field and dataset-metric names are unique in any case (Ossie: unquoted names fold)."""
        clashes = [
            f"{ds.name!r}: {', '.join(repr(n) for n in group)}"
            for doc in self.documents if not doc.legacy
            for ds in doc.datasets
            for group in _repeated_names([*(f.name for f in ds.fields or []), *(m.name for m in ds.metrics or [])])
        ]
        if clashes:
            raise OsiConversionError(
                "Within a dataset, field and dataset-metric names must be unique, and unquoted names match in any "
                "case; repeated in " + "; ".join(clashes) + "."
            )

    # ---- Ossie datasets -> SLayer datasets ----

    def _build_dataset(self, ds: OSIDataset, doc: OSIDocument) -> None:
        if not _legal_dataset_name(ds.name):
            self._warn(
                f"Dataset name {ds.name!r} is not a safe SLayer dataset name "
                f"(empty, surrounding whitespace, or a forbidden character such "
                f"as '.', ':', '/', '\\\\', NUL); skipping.",
                dataset_name=ds.name, category="illegal_name",
            )
            return

        parsed = self._read_source(ds)
        if parsed is None:
            self._warn(f"Dataset {ds.name!r} dropped: its source {ds.source!r} does not parse, so its identifiers "
                       f"could not be read with the source's meaning.", dataset_name=ds.name, category="expression")
            return
        resolve_datasource(parsed.database, self.data_source)  # stubbed routing

        try:
            if parsed.query is not None:
                base = self._build_sql_mode_dataset(ds, parsed.query)
            else:
                base = self._introspected_table(ds)
        except Exception as exc:  # noqa: BLE001 — per-dataset isolation
            self._warn(
                f"Failed to introspect dataset {ds.name!r} (source {ds.source!r}): "
                f"{exc}; skipping.",
                dataset_name=ds.name, category="introspection",
            )
            return

        self._physical[ds.name] = {c.name: c for c in base.fields}
        if doc.legacy:
            self._overlay_fields(base, ds)
        else:
            self._declare_fields(base, ds)
        self._drop_unreadable_fields(base)
        self._apply_dataset_metadata(base, ds, doc)
        self._models[ds.name] = base
        found = self._payload(ds.nonportable_extensions, kind="dataset", entity=ds.name, label=f"dataset {ds.name!r}",
                              dataset_name=ds.name, ossie=ext.source_digest(ds.source, reader=self._target))
        if found is not None:
            payload, matched = found
            assert isinstance(payload, ext.DatasetPayload)
            self._dataset_payloads[ds.name] = (payload, matched)

    def _introspected_table(self, ds: OSIDataset) -> SlayerDataset:
        introspected = self._tables.get(ds.name, "no table metadata was collected (collect_metadata)")
        if isinstance(introspected, str):
            raise _MetadataUnavailable(introspected)
        return introspected.model_copy(deep=True)

    def _build_sql_mode_dataset(self, ds: OSIDataset, query: str) -> SlayerDataset:
        # The query's output columns, as the datasource's probe reported them.
        live = self._datasource.source(sql=query)
        if isinstance(live, str):
            raise _MetadataUnavailable(live)
        fields = [_typed_field(name=name, data_type=typed[0], db_type=live.result_types[name].lossy_db_type)
                   for name, typed in live.columns.items()]
        self._live[ds.name] = live
        return SlayerDataset(name=ds.name, sql=query, data_source=self.data_source,
                           fields=fields)

    def _declare_fields(self, dataset: SlayerDataset, ds: OSIDataset) -> None:
        """Current spec: the dataset's fields are exactly the declared fields."""
        if not ds.fields:
            self._warn(f"Dataset {ds.name!r} declares no fields; its SLayer dataset exposes none.",
                       dataset_name=ds.name, category="missing_column", severity="info")
        fields: list[DatasetField] = []
        for field in ds.fields or []:
            col = self._field_field(field=field, ds=ds, dataset=dataset)
            if col is not None:
                fields.append(col)
        dataset.fields = fields
        self._apply_primary_key(dataset, ds)
        dataset.default_time_dimension = _first_time_dimension(dataset)

    def _drop_unreadable_fields(self, dataset: SlayerDataset) -> None:
        """Drop each field reading a source column another field's name holds (a legacy overlay reverts)."""
        dropped = unreadable_fields(dataset.fields, dialect=self._target, live=self._physical[dataset.name])
        for name, problem in dropped.items():
            col = next(c for c in dataset.fields if c.name == name)
            original = self._shadowed.pop((dataset.name, name), None)
            if original is not None:
                dataset.fields[dataset.fields.index(col)] = original
            else:
                dataset.fields.remove(col)
            self._warn(f"Field {name!r} on {dataset.name!r} dropped: {problem}"
                       f"{'; the source column is kept' if original is not None else ''}.",
                       dataset_name=dataset.name, category="masked_column")
        if dataset.default_time_dimension in dropped and dataset.get_field(dataset.default_time_dimension) is None:
            dataset.default_time_dimension = _first_time_dimension(dataset)

    def _apply_primary_key(self, dataset: SlayerDataset, ds: OSIDataset) -> None:
        """A valid declared key replaces the inherited flags; an empty or unresolved one clears them."""
        if ds.primary_key is None:
            return  # renamed introspected key fields keep their flag
        declared = {c.name for c in dataset.fields}
        missing = [k for k in ds.primary_key if k not in declared]
        if not ds.primary_key:
            self._warn(f"Dataset {ds.name!r} declares an empty primary_key; ignoring it.",
                       dataset_name=ds.name, category="primary_key")
        elif missing:
            self._warn(f"Dataset {ds.name!r} primary_key names {missing}, which are not declared "
                       f"fields; not applied.", dataset_name=ds.name, category="primary_key")
        key = set(ds.primary_key) if ds.primary_key and not missing else set()
        for col in dataset.fields:
            col.primary_key = col.name in key

    def _overlay_fields(self, dataset: SlayerDataset, ds: OSIDataset) -> None:
        """Legacy: overlay each field on the introspected fields."""
        by_name = {c.name: c for c in dataset.fields}
        for field in ds.fields or []:
            col = self._field_field(field=field, ds=ds, dataset=dataset)
            if col is None:
                continue
            existing = by_name.get(col.name)
            col.primary_key = existing.primary_key if existing is not None else False
            if existing is None:
                dataset.fields.append(col)
            else:
                if col.sql is not None:
                    self._shadowed.setdefault((dataset.name, col.name), existing)
                dataset.fields[dataset.fields.index(existing)] = col
            by_name[col.name] = col

        if ds.primary_key is not None:
            self._apply_legacy_primary_key(dataset, ds)
        if not dataset.default_time_dimension:
            dataset.default_time_dimension = _first_time_dimension(dataset)

    def _apply_legacy_primary_key(self, dataset: SlayerDataset, ds: OSIDataset) -> None:
        """A valid key replaces the introspected one; an empty or unresolved one keeps it."""
        assert ds.primary_key is not None
        present = {c.name for c in dataset.fields}
        missing_pk = [k for k in ds.primary_key if k not in present]
        if not ds.primary_key or missing_pk:
            self._warn(
                f"Dataset {ds.name!r} primary_key is empty or references unknown "
                f"field(s) {missing_pk}; keeping the introspected primary key.",
                dataset_name=ds.name, category="primary_key",
            )
            return
        osi_pk = set(ds.primary_key)
        for col in dataset.fields:
            col.primary_key = col.name in osi_pk

    def _field_sql(self, field: OSIField, ds: OSIDataset) -> tuple[str, exp.Expr] | None:
        """The field's selected expression rendered for the target, with its parse tree."""
        label = f"Field {field.name!r} on {ds.name!r}"
        selection = select_expression(field.expression, override=self.dialect, target=self._target)
        self._note_selection(selection, label=label, dataset_name=ds.name)
        if selection.candidate is None:
            self._warn(f"{label} {selection.reason}; skipping.", dataset_name=ds.name, category="dialect")
            return None
        folded = fold_source_identifiers(selection.candidate.text, dialect=self._dialect,
                                         read=reader_for(selection.candidate.dialect),
                                         logical_qualifiers=self._dataset_names - {ds.name},
                                         kept_qualifiers={ds.name})
        candidate = selection.candidate.model_copy(update={"text": folded.text})
        try:
            tree = read(candidate)
        except SqlglotError:
            self._warn(f"{label} has an unparseable expression {candidate.text!r}; skipping.",
                       dataset_name=ds.name, category="expression")
            return None
        try:
            rendered = tree if is_native(candidate, target=self._target) else self._list_calls(tree, ds=ds)
        except (Unconvertible, UnsupportedServerFunctionError) as exc:
            self._warn(f"{label}: {exc}; skipping.", dataset_name=ds.name, category="expression")
            return None
        return render(candidate, rendered, target=self._target), tree

    def _list_calls(self, tree: exp.Expr, *, ds: OSIDataset) -> exp.Expr:
        """``tree`` with array functions as SLayer's own list SQL for the target."""
        physical = self._physical[ds.name]

        def is_list(node: exp.Expr) -> bool:
            col = physical.get(node.name) if isinstance(node, exp.Column) and not node.table else None
            return col is not None and col.datatype is DataType.ARRAY

        return _bottom_up(tree, lambda node: _list_call(node, dialect=self._dialect, is_list=is_list))

    def _field_field(self, field: OSIField, ds: OSIDataset, dataset: SlayerDataset) -> DatasetField | None:
        """The DatasetField one field maps to, or ``None`` on a reported clean-fail."""
        if not _legal_field_name(field.name):
            self._warn(
                f"Field name {field.name!r} on dataset {ds.name!r} contains "
                f"'.'/':'; skipping the field.",
                dataset_name=ds.name, category="illegal_name",
            )
            return None
        selected = self._field_sql(field, ds)
        if selected is None:
            return None
        sql, tree = selected
        if (label := self._duplicated_label(ds.name, tree)) is not None:
            self._warn(f"Field {field.name!r} on {ds.name!r} reads source column {label!r}, which the source query "
                       f"outputs more than once; skipping.", dataset_name=ds.name, category="ambiguous_column")
            return None
        declared = OSSIE_DATATYPES.get(field.datatype or "")
        bare = _bare_field(tree)
        typed = (self._bare_field_field(field=field, ds=ds, bare=bare, sql=sql) if bare is not None
                 else self._derived_field_field(field=field, ds=ds, dataset=dataset, sql=sql, tree=tree,
                                                 declared=declared))
        if typed is None:
            return None
        col, db_type = typed
        if db_type is not None and declared is not None and db_type != declared:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} declares datatype {field.datatype} but the "
                f"database reports {db_type.value}; using {db_type.value}.",
                dataset_name=ds.name, category="datatype", severity="info",
            )
        self._apply_field_metadata(col=col, field=field)
        found = self._payload(field.nonportable_extensions, kind="field", entity=field.name,
                              label=f"field {field.name!r} of {ds.name!r}", dataset_name=ds.name,
                              ossie=ext.expression_digest(field.expression))
        if found is not None:
            payload, matched = found
            assert isinstance(payload, ext.FieldPayload)
            self._field_payloads[(ds.name, field.name)] = (payload, matched)
        return col

    def _live_names(self, ds_name: str) -> list[tuple[str, DatasetField]]:
        """Each source column of ``ds_name`` with its live name in SQLAlchemy's spelling."""
        columns = [c for c in self._physical[ds_name].values() if c.is_base]
        return [(live_name(c.physical_name, quoted=c.physical_quoted), c) for c in columns]

    def _source_fields(self, ds_name: str, ident: exp.Identifier) -> list[DatasetField]:
        """The source columns of ``ds_name`` that ``ident`` names, matched with the datasource dialect's rules."""
        live = self._live_names(ds_name)
        exact = {live_exact_name(n, dialect=self._dialect): c for n, c in live}
        index = PhysicalIndex([], dialect=self._target, live=[n for n, _ in live])
        return [exact[n] for n in index.live_names(ident)]

    def _duplicated_label(self, ds_name: str, tree: exp.Expr) -> str | None:
        """A root-scope source column in ``tree`` that ``ds_name``'s source query outputs more than once."""
        live = self._live.get(ds_name)
        if live is None:
            return None
        return next((c.name for c in root_physical_columns(tree) if live.duplicated(c.this)), None)

    def _bare_field_field(self, *, field: OSIField, ds: OSIDataset, bare: exp.Identifier,
                           sql: str) -> tuple[DatasetField, DataType | None] | None:
        found = self._source_fields(ds.name, bare)
        source = found[0] if len(found) == 1 else None
        if source is None:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} references column {bare!r} "
                f"which is not present in the source; skipping.",
                dataset_name=ds.name, category="missing_column",
            )
            return None
        # A copy keeps the source's type, db_type, key flag and DB comment.
        if source.physical_quoted:
            kept = source.sql
        else:
            kept = None if bare.name == field.name else sql
        col = source.model_copy(deep=True, update={"name": field.name, "sql": kept})
        return col, source.datatype

    def _derived_field_field(self, *, field: OSIField, ds: OSIDataset, dataset: SlayerDataset, sql: str,
                              tree: exp.Expr, declared: DataType | None) -> tuple[DatasetField, DataType | None] | None:
        physical = self._physical[ds.name]
        missing = _missing_expr_fields(tree, known=lambda ident: bool(self._source_fields(ds.name, ident)),
                                        self_name=ds.name)
        if missing:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} references unknown "
                f"field(s) {missing}; skipping.",
                dataset_name=ds.name, category="missing_column",
            )
            return None
        probed = self._probe_expression_type(dataset=dataset, expr=sql)
        if probed is not None:
            slayer_field = _typed_field(name=field.name, sql=sql, data_type=probed.type, db_type=probed.lossy_db_type)
            return slayer_field, probed.type
        shadowed = physical.get(field.name)
        fallback = declared or (shadowed.datatype if shadowed is not None else None) or DataType.DOUBLE
        element = shadowed.element_type if shadowed is not None else None
        return _typed_field(name=field.name, sql=sql, data_type=fallback, element=element), None

    @staticmethod
    def _apply_field_metadata(*, col: DatasetField, field: OSIField) -> None:
        col.label = field.label or col.label
        col.description = render_description(explicit=field.description, ctx=field.ai_context) \
            or col.description
        extra: dict[str, Any] = {}
        if field.datatype:
            extra["osi_datatype"] = field.datatype
        explicit = field.dimension.is_time if field.dimension is not None else None
        if explicit is not None or _is_time_role(field):
            extra["osi_is_time"] = _is_time_role(field)
        meta = _build_meta(ctx=field.ai_context, extensions=field.nonportable_extensions, extra=extra)
        if meta:
            col.meta = {**(col.meta or {}), **meta}

    def _probe_expression_type(self, dataset: SlayerDataset, expr: str) -> ResultColumnType | None:
        """A derived expression's type from a zero-row probe of the dataset's source, or ``None``."""
        if dataset.name not in self._live:
            found = self._datasource.source(sql_table=dataset.sql_table, sql=dataset.sql) \
                if dataset.sql_table or dataset.sql else None
            self._live[dataset.name] = found if isinstance(found, LiveSource) else None
        live = self._live[dataset.name]
        typed = live.probe(expr) if live is not None else None
        if live is None or typed is None:
            return None
        return live.probed(expr) or ResultColumnType(type=typed[0])

    def _apply_dataset_metadata(self, dataset: SlayerDataset, ds: OSIDataset, doc: OSIDocument) -> None:
        dataset.description = render_description(explicit=ds.description, ctx=ds.ai_context) \
            or dataset.description
        extra: dict[str, Any] = {}
        unique_keys = self._unique_keys(dataset, ds)
        if unique_keys:
            extra["osi_unique_keys"] = unique_keys
        single = {key[0] for key in unique_keys if len(key) == 1}
        for col in dataset.fields:
            col.unique = col.unique or col.name in single
        doc_ctx = ai_context_to_dict(doc.ai_context)
        doc_ext = _foreign(doc.nonportable_extensions)
        if doc_ctx or doc.description or doc_ext:
            extra["osi_semantic_model"] = {
                "name": doc.name,
                "description": doc.description,
                "ai_context": doc_ctx,
                **({"nonportable_extensions": doc_ext} if doc_ext else {}),
            }
        meta = _build_meta(ctx=ds.ai_context, extensions=ds.nonportable_extensions, extra=extra)
        if meta:
            dataset.meta = {**(dataset.meta or {}), **meta}

    def _unique_keys(self, dataset: SlayerDataset, ds: OSIDataset) -> list[list[str]]:
        """Non-empty entries naming only the dataset's fields (declared fields; legacy: plus source columns)."""
        names = {c.name for c in dataset.fields}
        kept: list[list[str]] = []
        for key in ds.unique_keys or []:
            missing = [k for k in key if k not in names]
            if not key or missing:
                self._warn(
                    f"Dataset {ds.name!r} unique_keys entry {key} is empty or names "
                    f"{missing}, which are not fields of the dataset; ignoring it.",
                    dataset_name=ds.name, category="unique_keys",
                )
                continue
            kept.append(key)
        return kept

    # ---- Ossie relationships -> SLayer relationships ----

    def _build_relationship(self, rel: OSIRelationship, doc: OSIDocument) -> None:
        src = rel.from_dataset
        doc_datasets = self._doc_datasets(doc)
        if src not in doc_datasets:
            self._warn(
                f"Relationship {rel.name!r} references source dataset {src!r}, "
                f"which is not imported from its document; skipping.",
                category="relationship",
            )
            return
        if rel.to not in doc_datasets:
            self._warn(
                f"Relationship {rel.name!r} targets dataset {rel.to!r}, which is not "
                f"imported from its document; skipping.",
                dataset_name=src, category="relationship",
            )
            return
        if not rel.from_columns or len(rel.from_columns) != len(rel.to_columns):
            self._warn(
                f"Relationship {rel.name!r} has empty or mismatched key lengths "
                f"({len(rel.from_columns)} vs {len(rel.to_columns)}); skipping.",
                dataset_name=src, category="relationship",
            )
            return

        missing = self._missing_relationship_fields(rel)
        if missing:
            self._warn(
                f"Relationship {rel.name!r} keys on {missing}, which are not plain "
                f"base fields of their datasets; skipping.",
                dataset_name=src, category="relationship",
            )
            return

        found = self._payload(rel.nonportable_extensions, kind="relationship", entity=rel.name,
                              label=f"relationship {rel.name!r}", dataset_name=src,
                              ossie=ext.relationship_digest(from_dataset=src, to=rel.to, from_columns=rel.from_columns,
                                                            to_columns=rel.to_columns))
        if found is not None and not found[1]:
            self._warn(f"Relationship {rel.name!r}: {_EDITED}", dataset_name=src, category=_EXTENSION,
                       severity="info", suggestion=_EDITED)
        if found is not None and found[1] and self._restore_declarations(rel, payload=found[0]):
            return

        # DatasetRelationship keys only on ``to``, so a second relationship to one target is unreachable.
        if any(j.to == rel.to for j in self._models[src].relationships):
            self._warn(
                f"Relationship {rel.name!r} is a second relationship from {src!r} to "
                f"{rel.to!r}; SLayer cannot disambiguate multiple relationships to one "
                f"dataset (no relationship aliases). Keeping the first; skipping this one.",
                dataset_name=src, category="relationship",
            )
            return

        pairs = [[f, t] for f, t in zip(rel.from_columns, rel.to_columns)]
        self._models[src].relationships.append(DatasetRelationship(
            to=rel.to,
            join_pairs=pairs,
            # OSI relationships are direction-implied: from = many, to = one.
            cardinality=RelationshipCardinality.MANY_TO_ONE,
            description=render_description(explicit=None, ctx=rel.ai_context),
            meta=_build_meta(ctx=rel.ai_context, extensions=rel.nonportable_extensions),
        ))

    def _validate_cross_dataset_field_refs(self) -> None:
        """Drop fields whose cross-dataset refs don't resolve through the relationship graph.

        Fixed-point: a dropped field can invalidate another field or a relationship.
        """
        while True:
            if not (self._drop_invalid_cross_dataset_fields()
                    or self._drop_stale_relationships()):
                break

    def _drop_invalid_cross_dataset_fields(self) -> bool:
        dropped = False
        for dataset in self._models.values():
            for col, bad in self._invalid_cross_dataset_fields(dataset):
                self._revert_or_drop_field(dataset=dataset, col=col, bad=bad)
                dropped = True
        return dropped

    def _invalid_cross_dataset_fields(
        self, dataset: SlayerDataset
    ) -> list[tuple[DatasetField, list[str]]]:
        result = []
        for col in dataset.fields:
            if col.sql:
                bad = self._unresolvable_cross_dataset_refs(dataset=dataset, sql=col.sql)
                if bad:
                    result.append((col, bad))
        return result

    def _revert_or_drop_field(
        self, dataset: SlayerDataset, col: DatasetField, bad: list[str]
    ) -> None:
        original = self._shadowed.pop((dataset.name, col.name), None)
        if original is not None:
            dataset.fields[dataset.fields.index(col)] = original
            self._warn(
                f"Derived overlay for {col.name!r} on {dataset.name!r} references "
                f"unresolvable cross-dataset field(s) {bad}; reverting to the "
                f"physical column.",
                dataset_name=dataset.name, category="missing_column",
            )
        else:
            dataset.fields.remove(col)
            if dataset.default_time_dimension == col.name:
                dataset.default_time_dimension = _first_time_dimension(dataset)
            self._warn(
                f"Field {col.name!r} on {dataset.name!r} references unresolvable "
                f"cross-dataset field(s) {bad}; dropping.",
                dataset_name=dataset.name, category="missing_column",
            )

    def _drop_stale_relationships(self) -> bool:
        """Drop relationships whose key fields were removed by field validation."""
        dropped = False
        for dataset in self._models.values():
            stale = [j for j in dataset.relationships if self._relationship_fields_missing(dataset=dataset, relationship=j)]
            for relationship in stale:
                dataset.relationships.remove(relationship)
                dropped = True
                self._warn(
                    f"Relationship from {dataset.name!r} to {relationship.to!r} "
                    f"references a field removed during validation; dropping.",
                    dataset_name=dataset.name, category="relationship",
                )
        return dropped

    def _parse_target(self, sql: str) -> exp.Expr | None:
        try:
            return sqlglot.parse_one(sql, read=self._target)
        except SqlglotError:
            return None

    def _type_cross_dataset_fields(self) -> None:
        """Type a derived field that is exactly one cross-dataset ref from the field its chain of such refs ends at."""
        for dataset in self._models.values():
            for col in dataset.fields:
                end = self._ref_chain_end(dataset=dataset, col=col)
                if end is not None:
                    col.datatype, col.element_type = end.datatype, end.element_type

    def _ref_chain_end(self, *, dataset: SlayerDataset, col: DatasetField) -> DatasetField | None:
        """The field a chain of single cross-dataset refs from ``col`` ends at; ``None`` without a ref or on a cycle."""
        seen = {id(col)}
        end: DatasetField | None = None
        while col.sql and (hop := self._single_cross_dataset_ref(dataset=dataset, sql=col.sql)) is not None:
            dataset, col = hop
            if id(col) in seen:
                return None
            seen.add(id(col))
            end = col
        return end

    def _single_cross_dataset_ref(self, dataset: SlayerDataset, sql: str) -> tuple[SlayerDataset, DatasetField] | None:
        """The dataset and field ``sql`` names when it is exactly one cross-dataset ref."""
        tree = self._parse_target(sql)
        if not isinstance(tree, exp.Column):
            return None
        # The full qualifier chain: a multi-hop ref keeps earlier hops in .db/.catalog.
        parts = [p.name for p in tree.parts]
        quals = parts[:-1]
        if not quals or quals == [dataset.name]:
            return None
        target = self._walk_join_alias(host=dataset, alias=".".join(quals))
        if target is None:
            return None
        field = next((c for c in target.fields if c.name == parts[-1]), None)
        return None if field is None else (target, field)

    def _relationship_fields_missing(self, dataset: SlayerDataset, relationship: DatasetRelationship) -> bool:
        target = self._models.get(relationship.to)
        for src_col, tgt_col in relationship.join_pairs:
            if not self._dataset_has_field(dataset_name=dataset.name, field=src_col):
                return True
            if target is None or not any(c.name == tgt_col for c in target.fields):
                return True
        return False

    def _is_physical_multihop_ref(self, *, dataset: SlayerDataset, quals: list[str]) -> bool:
        """True for a multi-hop qualifier whose first hop is not a relationship hop (a physical
        ``schema.table.column`` ref). An ambiguous hop IS a relationship hop."""
        if len(quals) < 2:
            return False
        try:
            return resolve_hop(
                current=dataset, token=quals[0], datasets_by_name=self._models
            ) is None
        except AmbiguousRelationshipPathError:
            return False

    def _unresolvable_cross_dataset_refs(self, dataset: SlayerDataset, sql: str) -> list[str]:
        """Root-scope cross-dataset field refs in ``sql`` that don't resolve to a joined
        dataset + existing field; physical multi-part refs are skipped."""
        tree = self._parse_target(sql)
        if not isinstance(tree, Expression):
            return []
        root_ids = root_scope_field_ids(parsed=tree)
        bad = []
        for col in tree.find_all(exp.Column):
            if id(col) not in root_ids:
                continue  # nested-scope alias (CTE / sub-query) — not a relationship ref
            parts = [p.name for p in col.parts]
            quals, leaf = parts[:-1], parts[-1]
            if not quals or quals == [dataset.name]:
                continue  # self / unqualified — validated at field time
            if self._is_physical_multihop_ref(dataset=dataset, quals=quals):
                continue
            target = self._walk_join_alias(host=dataset, alias=".".join(quals))
            if target is None or not any(c.name == leaf for c in target.fields):
                bad.append(".".join(parts))
        return bad

    def _walk_join_alias(self, host: SlayerDataset, alias: str) -> SlayerDataset | None:
        """Resolve a dotted relationship qualifier to its terminal joined dataset, or None (never ``__``-split)."""
        return resolve_ref_target(
            qualifiers=tuple(alias.split(".")),
            source_dataset=host,
            datasets_by_name=self._models,
        )

    def _dataset_has_field(self, dataset_name: str, field: str) -> bool:
        dataset = self._models.get(dataset_name)
        return dataset is not None and name_taken(field, (c.name for c in dataset.fields))

    def _dataset_has_name(self, dataset_name: str, name: str) -> bool:
        """Whether ``name`` is taken by a field or metric (one shared namespace)."""
        dataset = self._models.get(dataset_name)
        if dataset is None:
            return False
        return name_taken(name, [*(c.name for c in dataset.fields), *(m.name for m in dataset.metrics if m.name)])

    def _missing_relationship_fields(self, rel: OSIRelationship) -> list[str]:
        """Qualified relationship key names that are not plain base fields of their dataset."""
        sides = ((rel.from_dataset, rel.from_columns), (rel.to, rel.to_columns))
        return [
            f"{name}.{c}" for name, cols in sides for c in cols
            if relationship_key_error(dataset=rel.from_dataset, target=rel.to, key=c, side=name,
                              fields=self._models[name].fields) is not None
        ]

    # ---- Ossie metrics -> SLayer metrics ----

    def _metric_tree(self, metric: OSIMetric) -> exp.Expr | None:
        """The metric's selected expression, parsed; ``None`` when reported."""
        selection = select_expression(metric.expression, override=self.dialect, target=self._target)
        self._note_selection(selection, label=f"Metric {metric.name!r}", metric_name=metric.name)
        if selection.candidate is None:
            self._unconv(f"Metric {metric.name!r} {selection.reason}.",
                         metric_name=metric.name, category="dialect")
            return None
        try:
            return read(selection.candidate)
        except SqlglotError as exc:
            self._unconv(f"Metric {metric.name!r}: could not parse expression: {exc}",
                         metric_name=metric.name)
            return None

    def _canonicalize_refs(self, tree: exp.Expr, *, doc_datasets: list[str], home: str | None,
                           helpers: dict[str, DatasetField], metric: str) -> exp.Expr:
        """``tree`` with current-spec metric refs rewritten to declared names, and ``source.column`` refs to the
        field reading it (a new hidden one added to ``helpers``); raises ``LookupError`` on a scope violation."""
        def canonical(col: exp.Expr) -> exp.Expr:
            if not isinstance(col, exp.Column):
                return col
            idents = [p for p in col.parts if isinstance(p, exp.Identifier)]
            if len(idents) != len(col.parts) or len(idents) > 2:
                raise LookupError(f"reference {col.sql()!r} is not a field reference")
            *qual, leaf = idents
            if home is not None and qual and _is_source_qualifier(qual[0]):
                return self._source_field_ref(leaf, home=home, helpers=helpers, metric=metric)
            owner = _ref_owner(col, qual=qual, home=home, doc_datasets=doc_datasets)
            declared = [c.name for c in self._models[owner].fields if not c.hidden]
            return exp.column(_match_name(leaf, declared), table=owner if home is None else None)
        return tree.transform(canonical, copy=False)

    def _source_field_ref(self, leaf: exp.Identifier, *, home: str, helpers: dict[str, DatasetField],
                           metric: str) -> exp.Column:
        """A ref to the field of ``home`` reading the source column ``leaf`` unchanged; raises ``LookupError``."""
        source = self._source_field(leaf, home=home)
        if source.name not in helpers:
            reader = self._source_reader(source, leaf=leaf, home=home)
            if reader is not None:
                return exp.column(reader.name)
            taken = {h.name for h in helpers.values()}
            name = next(n for n in (f"_{metric}_{i}" for i in itertools.count())
                        if n not in taken and not self._dataset_has_name(home, n))
            sql = source.sql if source.physical_quoted else exp.to_identifier(source.physical_name).sql(self._target)
            helpers[source.name] = _typed_field(name=name, sql=sql, data_type=source.datatype,
                                                 element=source.element_type, hidden=True, db_type=source.db_type)
        return exp.column(helpers[source.name].name)

    def _source_field(self, leaf: exp.Identifier, *, home: str) -> DatasetField:
        """The one source column of ``home`` that ``leaf`` names, as written or (unquoted) folded by the database."""
        if any(c.name.casefold() == "source" for c in self._models[home].fields if not c.hidden):
            raise LookupError(f"the field 'source' of {home!r} takes precedence over the source qualifier")
        found = self._source_fields(home, leaf)
        if not leaf.quoted:
            folded = exp.to_identifier(self._dialect.unquoted_identifier(leaf.name), quoted=True)
            found += [c for c in self._source_fields(home, folded) if all(c is not f for f in found)]
        if len(found) != 1:
            names = sorted(c.name for c in found)
            raise LookupError(f"source column {leaf.name!r} is ambiguous between {names}" if found
                              else f"source column {leaf.name!r} is not a column of {home!r}'s source")
        live = self._live.get(home)
        if live is not None and live.duplicated(leaf):
            raise LookupError(f"source column {leaf.name!r} is output more than once by {home!r}'s source query")
        return found[0]

    def _source_reader(self, source: DatasetField, *, leaf: exp.Identifier, home: str) -> DatasetField | None:
        """The dataset field reading ``source`` unchanged, or ``None``; raises when a field's name masks it."""
        live_names = self._live_names(home)
        exact = exp.to_identifier(next(live_exact_name(n, dialect=self._dialect) for n, c in live_names if c is source),
                                  quoted=True)
        index = PhysicalIndex(self._models[home].fields, dialect=self._target, live=[n for n, _ in live_names])
        if (problem := index.problem(exact)) is not None:
            raise LookupError(f"source column {leaf.name!r} cannot be read: {problem}")
        return index.reader(exact)

    def _build_metric(self, metric: OSIMetric, *, doc: OSIDocument, home: str | None,
                       doc_datasets: list[str]) -> DatasetMetric | None:
        if not _legal_metric_name(metric.name):
            self._unconv(
                f"Metric name {metric.name!r} is not a valid SLayer identifier.",
                metric_name=metric.name,
            )
            return None

        tree = self._metric_tree(metric)
        if tree is None:
            return None
        helpers: dict[str, DatasetField] = {}
        if not doc.legacy:
            try:
                tree = self._canonicalize_refs(tree, doc_datasets=doc_datasets, home=home, helpers=helpers,
                                               metric=metric.name)
            except LookupError as exc:
                self._unconv(f"Metric {metric.name!r}: {exc}.", metric_name=metric.name)
                return None
        if home is None:
            return self._convert_metric(metric, tree=tree, home=home, doc_datasets=doc_datasets)
        # Helpers stay only when the metric converts.
        fields = self._models[home].fields
        fields.extend(helpers.values())
        slayer_metric = self._convert_metric(metric, tree=tree, home=home, doc_datasets=doc_datasets)
        if slayer_metric is None:
            added = {id(h) for h in helpers.values()}
            fields[:] = [c for c in fields if id(c) not in added]
        return slayer_metric

    def _convert_metric(self, metric: OSIMetric, *, tree: exp.Expr, home: str | None,
                         doc_datasets: list[str]) -> DatasetMetric | None:
        """The metric ``tree`` converts to on its anchor, or ``None`` when reported."""
        scope = [home] if home is not None else doc_datasets
        owner_of = self._make_owner_of(scope)
        anchor = home or self._select_anchor(metric_name=metric.name, tree=tree,
                                             doc_datasets=doc_datasets, owner_of=owner_of)
        if anchor is None:
            return None  # already reported

        # Metrics and fields share one namespace on the anchor dataset.
        anchor_dataset = self._models[anchor]
        if name_taken(metric.name, (m.name for m in anchor_dataset.metrics if m.name)):
            self._unconv(
                f"Metric {metric.name!r} duplicates an existing metric on "
                f"dataset {anchor!r}.",
                metric_name=metric.name, category="duplicate_measure",
            )
            return None
        if self._dataset_has_field(dataset_name=anchor, field=metric.name):
            self._unconv(
                f"Metric {metric.name!r} collides with a field name on dataset "
                f"{anchor!r}.",
                metric_name=metric.name, category="name_collision",
            )
            return None

        result = convert_expression(
            tree, entity_name=metric.name, owner_of=owner_of, ref_of=self._make_ref_of(anchor=anchor),
            sql_dialect=self._dialect,
            name_taken=self._dataset_has_name, target=self._target, is_list=self._make_is_list(owner_of),
        )
        if not result.ok or result.expression is None:
            self._unconv(
                f"Metric {metric.name!r}: {result.reason}",
                metric_name=metric.name,
            )
            return None

        # Build the metric before materializing fields, so a rejected metric leaves no orphans.
        try:
            parse_expr(result.expression)
            slayer_metric = DatasetMetric(
                expression=result.expression,
                name=metric.name,
                description=render_description(explicit=metric.description, ctx=metric.ai_context),
                meta=_build_meta(ctx=metric.ai_context, extensions=metric.nonportable_extensions,
                                 extra={"osi_datatype": metric.datatype} if metric.datatype else None),
            )
        except Exception as exc:  # noqa: BLE001 — any parse/validation error -> report
            self._unconv(
                f"Metric {metric.name!r}: emitted expression {result.expression!r} is "
                f"not a valid SLayer metric ({exc}).",
                metric_name=metric.name,
            )
            return None

        self._materialize_fields(result.materialized)
        anchor_dataset.metrics.append(slayer_metric)
        for w in result.warnings:
            self._warn(f"Metric {metric.name!r}: {w}", metric_name=metric.name,
                       category="dialect", severity="info")
        return slayer_metric

    def _select_anchor(self, metric_name: str, tree: exp.Expr, doc_datasets: list[str],
                       owner_of: OwnerOf) -> str | None:
        def owners_in(node: exp.Expr) -> set[str]:
            return {o for col in node.find_all(exp.Column) if (o := owner_of(col.table or None, col.name))}

        owners = owners_in(tree)
        if owners:
            # One aggregate reading several datasets must reach them all over to-one hops.
            spans = set().union(*(s for agg in outermost_aggregates(tree) if len(s := owners_in(agg)) > 1))
            anchor = self._min_hops_root(candidates=doc_datasets, mentioned=owners, to_one=spans)
            if anchor is None:
                pairs = {
                    p.describe(): None
                    for c in doc_datasets for m in sorted(owners)
                    for p in blocking_pairs(root=self._models[c], target=m, datasets_by_name=self._models)
                }
                self._unconv(
                    f"Metric {metric_name!r} references datasets {sorted(owners)} "
                    f"with no single dataset reaching all of them via relationships."
                    + "".join(f" {d}." for d in pairs),
                    metric_name=metric_name, category="no_join_path",
                    suggestion="Add the required relationship(s), or split the "
                               "metric across a multi-stage query.",
                )
            return anchor
        # No field refs (e.g. COUNT(*)): the unique fact table, else an orphan of ambiguous grain.
        anchor = self._fact_root(doc_datasets)
        if anchor is None:
            self._unconv(
                f"Metric {metric_name!r} has no field references and the "
                f"semantic model has no unique fact table to attribute it to; "
                f"its grain is ambiguous.",
                metric_name=metric_name, category="orphan_metric",
                suggestion="Aggregate over an explicit field, or ensure the "
                           "semantic model has a single fact dataset.",
            )
        return anchor

    def _fact_root(self, doc_datasets: list[str]) -> str | None:
        """The unique dataset that is never a relationship target, or ``None``."""
        targets = {j.to for name in doc_datasets for j in self._models[name].relationships}
        non_targets = [n for n in doc_datasets if n not in targets]
        return non_targets[0] if len(non_targets) == 1 else None

    def _make_owner_of(self, scope: list[str]) -> OwnerOf:
        def owner_of(qualifier: Optional[str], field: str) -> Optional[str]:
            if qualifier is not None:
                return qualifier if qualifier in scope and self._dataset_has_field(qualifier, field) else None
            # Unqualified: exactly one owner, else unresolved (never bound by dataset order).
            matches = [name for name in scope if self._dataset_has_field(name, field)]
            return matches[0] if len(matches) == 1 else None
        return owner_of

    def _make_is_list(self, owner_of: OwnerOf) -> Callable[[Optional[str], str], bool]:
        def is_list(qualifier: Optional[str], field: str) -> bool:
            owner = owner_of(qualifier, field)
            col = self._models[owner].get_field(field) if owner is not None else None
            return col is not None and col.datatype is DataType.ARRAY
        return is_list

    def _min_hops_root(self, *, candidates: list[str], mentioned: set[str], to_one: set[str]) -> str | None:
        """Among ``candidates`` routing to every ``mentioned`` dataset (``to_one`` ones without
        fanning): prefer no fanning hop, then the fewest total hops, a mentioned candidate, the smallest name."""
        keyed = []
        for c in candidates:
            routed = shortest_routes(root=self._models[c], datasets_by_name=self._models)
            if all(m in routed for m in mentioned) and not any(routed[m].fanning for m in to_one):
                fans = any(routed[m].fanning for m in mentioned)
                keyed.append((fans, sum(len(routed[m].edges) for m in mentioned), 0 if c in mentioned else 1, c))
        return min(keyed)[3] if keyed else None

    def _make_ref_of(self, anchor: str) -> Callable[[str, str], Optional[str]]:
        def ref_of(dataset: str, field: str) -> Optional[str]:
            route = shortest_route(root=self._models[anchor], target=dataset, datasets_by_name=self._models)
            if route is None:
                return None
            return ".".join([*route.tokens, field])
        return ref_of

    def _physical_sql(self, dataset: SlayerDataset, sql: str) -> str:
        """``sql`` with each field reference replaced by the field's own SQL, for type probing."""
        tree = self._parse_target(sql)
        if tree is None:
            return sql
        by_name = {c.name: c for c in dataset.fields}

        def inline(ref: exp.Expr) -> exp.Expr:
            col = by_name.get(ref.name) if isinstance(ref, exp.Column) and not ref.table else None
            definition = self._parse_target(col.sql) if col is not None and col.sql else None
            return exp.Paren(this=definition) if definition is not None else ref
        return tree.transform(inline, copy=False).sql(dialect=self._target)

    def _materialize_fields(self, materialized: list[MaterializedField]) -> None:
        for mc in materialized:
            dataset = self._models.get(mc.owning_dataset)
            if dataset is None or any(c.name == mc.name for c in dataset.fields):
                continue
            probed = self._probe_expression_type(dataset=dataset, expr=self._physical_sql(dataset, mc.sql))
            dataset.fields.append(_typed_field(
                name=mc.name, sql=mc.sql, hidden=True,
                data_type=probed.type if probed is not None else DataType.DOUBLE,
                db_type=probed.lossy_db_type if probed is not None else None,
            ))

    # ---- SLAYER extension restore ----

    def _payload(self, extensions: Optional[list[OSINonportableExtension]], *, kind: str, entity: str, label: str,
                 dataset_name: str | None, ossie: Any) -> tuple[ext.Payload, bool] | None:
        """The entity's decoded ``SLAYER`` payload and whether its fingerprint matches; ``None`` (reported) otherwise."""
        entries = [e for e in extensions or [] if ext.is_slayer(e)]
        if not entries:
            return None
        if len(entries) > 1:
            self._warn(f"The {label} carries {len(entries)} SLAYER extensions; ignoring them.",
                       dataset_name=dataset_name, category=_EXTENSION, suggestion="Keep exactly one SLAYER extension.")
            return None
        try:
            payload = ext.decode(entries[0].data)
        except ValueError as exc:
            self._warn(f"The SLAYER extension on the {label} is malformed or of an unknown version; ignoring it "
                       f"({exc}).", dataset_name=dataset_name, category=_EXTENSION,
                       suggestion=_REEXPORT)
            return None
        if payload.kind != kind or payload.entity != entity:
            self._warn(f"The SLAYER extension on the {label} describes {payload.kind} {payload.entity!r}; "
                       f"ignoring it.", dataset_name=dataset_name, category=_EXTENSION,
                       suggestion=_REEXPORT)
            return None
        return payload, ext.matches(payload, ossie=ossie)

    def _restore_declarations(self, rel: OSIRelationship, *, payload: ext.Payload) -> bool:
        """Add the SLayer relationships ``rel`` stands for, when every endpoint and key validates."""
        assert isinstance(payload, ext.RelationshipPayload)
        built = _build_meta(ctx=rel.ai_context, extensions=rel.nonportable_extensions)
        relationships: list[tuple[SlayerDataset, DatasetRelationship]] = []
        for declaration in payload.declarations:
            dataset, relationship = self._models.get(declaration.dataset), declaration.relationship
            target = self._models.get(relationship.to)
            problem = None
            if dataset is None or target is None:
                problem = f"joins {declaration.dataset!r} to {relationship.to!r}, which are not both imported"
            elif any(j.to == relationship.to and j.name == relationship.name for j in dataset.relationships):
                problem = f"duplicates a relationship from {dataset.name!r} to {relationship.to!r}"
            else:
                errors = [relationship_key_error(dataset=dataset.name, target=target.name, key=a, side=dataset.name,
                                         fields=dataset.fields) or
                          relationship_key_error(dataset=dataset.name, target=target.name, key=b, side=target.name,
                                         fields=target.fields)
                          for a, b in relationship.join_pairs]
                problem = next((str(e).splitlines()[0] for e in errors if e is not None), None)
            if problem is not None:
                self._warn(f"Relationship {rel.name!r}: its SLayer relationship {problem}; imported as a plain "
                           f"many-to-one relationship.", dataset_name=rel.from_dataset, category=_EXTENSION,
                           suggestion="Import every dataset the relationship connects.")
                return False
            assert dataset is not None
            relationships.append((dataset, relationship.model_copy(update={"meta": {**(built or {}), **(relationship.meta or {})} or None})))
        for dataset, relationship in relationships:
            dataset.relationships.append(relationship)
        return True

    def _restore_dataset_attributes(self) -> None:
        """Dataset attributes Ossie has no place for, and the SLayer-only fields, from each dataset payload."""
        for name, (payload, _) in self._dataset_payloads.items():
            dataset = self._models.get(name)
            if dataset is None:
                continue
            dataset.hidden = payload.hidden
            dataset.access_tags = list(payload.access_tags)
            dataset.aggregations = list(payload.aggregations)
            dataset.meta = {**(dataset.meta or {}), **(payload.meta or {})} or None
            if payload.default_time_dimension is None or dataset.get_field(payload.default_time_dimension):
                dataset.default_time_dimension = payload.default_time_dimension
            for field in payload.fields:
                self._restore_field(dataset, field)

    def _restore_field(self, dataset: SlayerDataset, field: DatasetField) -> None:
        bad = [] if not field.sql else self._unresolvable_cross_dataset_refs(dataset=dataset, sql=field.sql)
        if self._dataset_has_name(dataset.name, field.name) or bad:
            reason = f"reads {bad}, which do not resolve" if bad else "collides with an imported name"
            self._warn(f"SLayer-only field {field.name!r} on {dataset.name!r} {reason}; skipping it.",
                       dataset_name=dataset.name, category=_EXTENSION, suggestion=_IMPORT_INPUTS)
            return
        dataset.fields.append(field)

    def _restore_metrics(self) -> None:
        """Add restored and SLayer-only metrics once everything they reference exists; a remaining one falls back."""
        for name, (payload, _) in self._dataset_payloads.items():
            for metric in payload.metrics:
                self._pending.append(_PendingMetric(dataset=name, metric=metric,
                                                     label=f"SLayer-only metric {metric.name or metric.expression!r}"))
        pending, errors = list(self._pending), {}
        while pending:
            remaining = []
            for item in pending:
                error = self._add_metric(item)
                if error is not None:
                    errors[id(item)] = error
                    remaining.append(item)
            if len(remaining) == len(pending):
                break
            pending = remaining
        for item in pending:
            self._fall_back(item, error=errors[id(item)])

    def _add_metric(self, item: _PendingMetric) -> str | None:
        dataset = self._models.get(item.dataset)
        if dataset is None:
            return f"its dataset {item.dataset!r} is not imported"
        if item.metric.name and self._dataset_has_name(item.dataset, item.metric.name):
            return f"its name collides on {item.dataset!r}"
        dataset.metrics.append(item.metric)
        try:
            bind_dataset_expression(item.metric.expression, dataset=dataset, datasets=list(self._models.values()),
                               dialect=self._target)
        except Exception as exc:  # noqa: BLE001 — any binding failure keeps it pending
            dataset.metrics.remove(item.metric)
            return str(exc).splitlines()[0]
        return None

    def _fall_back(self, item: _PendingMetric, *, error: str) -> None:
        if item.osi_metric is None or item.doc is None:
            self._warn(f"{item.label} on {item.dataset!r} does not validate ({error}); skipping it.",
                       dataset_name=item.dataset, metric_name=item.metric.name, category=_EXTENSION,
                       suggestion=_IMPORT_INPUTS)
            return
        self._warn(f"{item.label}: its SLayer metric does not validate ({error}); imported from its SQL.",
                   dataset_name=item.dataset, metric_name=item.osi_metric.name, category=_EXTENSION, severity="info",
                   suggestion=_IMPORT_INPUTS)
        self._build_metric(metric=item.osi_metric, doc=item.doc, home=item.home, doc_datasets=self._doc_datasets(item.doc))

    def _restore_fields(self) -> None:
        """Field attributes; the value/filter split only where the field's fingerprint matches."""
        for (name, field), (payload, matched) in self._field_payloads.items():
            dataset = self._models.get(name)
            slayer_field = dataset.get_field(field) if dataset is not None else None
            if dataset is None or slayer_field is None:
                continue
            content = payload.field
            slayer_field.hidden, slayer_field.format = content.hidden, content.format
            slayer_field.meta = {**(slayer_field.meta or {}), **(content.meta or {})} or None
            if not matched:
                self._warn(f"Field {field!r} of {name!r}: {_EDITED}", dataset_name=name, category=_EXTENSION,
                           severity="info", suggestion=_EDITED)
                continue
            parts = [p for p in (content.sql, content.filter) if p]
            if any(self._unresolvable_cross_dataset_refs(dataset=dataset, sql=p) for p in parts):
                self._warn(f"Field {field!r} of {name!r}: its SLayer definition does not resolve; kept its "
                           f"expression.", dataset_name=name, category=_EXTENSION, severity="info",
                           suggestion=_IMPORT_INPUTS)
                continue
            slayer_field.sql, slayer_field.filter = content.sql, content.filter
            slayer_field.datatype = content.datatype or slayer_field.datatype
            # Type-dependent, so restored only with the type.
            slayer_field.granularity, slayer_field.allowed_aggregations = content.granularity, content.allowed_aggregations

    def _restore_sources(self) -> None:
        """Table/SQL with row filters in place; saved queries handed back for the engine to validate."""
        for name, (payload, matched) in self._dataset_payloads.items():
            dataset = self._models.get(name)
            if dataset is None:
                continue
            if not matched:
                self._warn(f"Dataset {name!r}: {_EDITED}", dataset_name=name, category=_EXTENSION, severity="info",
                           suggestion=_EDITED)
                continue
            source = payload.source
            if source.source_queries is None:
                dataset.sql_table, dataset.sql, dataset.filters = source.sql_table, source.sql, list(source.filters)
                continue
            try:
                restored = dataset.model_copy(update={
                    "sql": None, "sql_table": None, "filters": [], "fields": [],
                    "source_queries": [SlayerQuery.model_validate(q) for q in source.source_queries],
                    "query_variables": dict(source.query_variables),
                })
            except ValueError as exc:
                self._warn(f"Dataset {name!r}: its saved queries do not load ({exc}); imported as a sql-mode "
                           f"dataset over its source.", dataset_name=name, category=_EXTENSION, severity="info",
                           suggestion=_REEXPORT)
                continue
            self._query_backed.append(restored)
