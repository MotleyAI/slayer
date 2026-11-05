"""Convert parsed OSI documents into SLayer models.

Each OSI dataset becomes one ``SlayerModel`` over its live-introspected source.
A current-spec model exposes exactly its declared fields; a legacy model
overlays its fields on every introspected column. Relationships become
``ModelJoin`` entries within their own document; metrics become
``ModelMeasure`` formulas — a dataset-scoped metric on its dataset, a
model-scoped one on the model that reaches every dataset it references.
Constructs that cannot be expressed exactly are reported in the
``ConversionResult``, never silently lost.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Optional

import sqlalchemy as sa
import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.errors import SqlglotError

from slayer.core.deprecation import warn_deprecated
from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.join_walker import resolve_hop
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel, join_key_error
from slayer.core.query import SlayerQuery
from slayer.core.refs import IDENTIFIER_RE as _IDENTIFIER_RE
from slayer.engine.binding import bind_model_formula
from slayer.engine.syntax import parse_expr
from slayer.sql.column_expansion import root_scope_column_ids, resolve_ref_target
from slayer.sql.dialects import dialect_for_ds_type
from slayer.engine.ingestion import introspect_table_to_model
from slayer.engine.query_engine import run_over_caller_engine
from slayer.core.join_search import blocking_pairs, shortest_route, shortest_routes
from slayer.ingest_report import ConversionResult, ConversionWarning
from slayer.sql.client import ResultColumnType, SqlClient
from slayer.osi.dialects import Selection, read, render, select_expression
from slayer.osi.expression import MaterializedColumn, convert_expression, outermost_aggregates
from slayer.osi.models import (
    OSSIE_DATATYPES,
    TEMPORAL_DATATYPES,
    OSIAIContext,
    OSICustomExtension,
    OSIDataset,
    OSIDocument,
    OSIField,
    OSIMetric,
    OSIRelationship,
    ai_context_to_dict,
    render_description,
)
from slayer.osi import slayer_extension as ext
from slayer.osi.source import parse_source, resolve_datasource

logger = logging.getLogger(__name__)

# Conversion passes ``collect_metadata`` runs to discover the type probes conversion asks for.
_MAX_PROBE_PASSES = 8

# Target dialects lacking a GROUP BY percentile/median aggregate.
_NO_PERCENTILE_DIALECTS = frozenset({"mysql", "tsql"})

_TIME_DIMENSION_TYPES = (DataType.DATE, DataType.TIMESTAMP)


def _typed_column(*, name: str, data_type: DataType, sql: str | None = None, element: DataType | None = None,
                  hidden: bool = False, db_type: str | None = None) -> Column:
    """A column of a probed type; a list's elements are ``element``, else UNKNOWN (a probe cannot see them)."""
    if data_type is not DataType.ARRAY:
        return Column(name=name, sql=sql, type=data_type, db_type=db_type, hidden=hidden)
    return Column(name=name, sql=sql, type=data_type, element_type=element or DataType.UNKNOWN, db_type=db_type,
                  hidden=hidden)


OwnerOf = Callable[[Optional[str], str], Optional[str]]


class OsiConversionError(Exception):
    """Raised when an OSI import set cannot be converted (e.g. duplicate names)."""


# Unsafe in a model name, which becomes a YAML storage filename. ``__`` is allowed.
_UNSAFE_MODEL_NAME_CHARS = (".", ":", "/", "\\", "\x00")


def _legal_model_name(name: str) -> bool:
    if not name or name.strip() != name or not name.strip():
        return False
    return not any(ch in name for ch in _UNSAFE_MODEL_NAME_CHARS)


def _legal_column_name(name: str) -> bool:
    return "." not in name and ":" not in name


def _legal_measure_name(name: str) -> bool:
    return bool(_IDENTIFIER_RE.match(name))


def _bare_column(tree: exp.Expr) -> str | None:
    """The column name if ``tree`` is one unqualified (possibly quoted) column, else ``None``."""
    if (isinstance(tree, exp.Column) and not tree.table
            and not tree.args.get("db") and not tree.args.get("catalog")):
        return tree.name
    return None


def _missing_expr_columns(tree: exp.Expr, available: set[str], self_name: str) -> list[str]:
    """Unqualified and self-qualified column names in ``tree`` absent from ``available``.

    Other qualifiers are cross-model refs, checked after joins exist.
    """
    missing = []
    for col in tree.find_all(exp.Column):
        if not col.table:
            if col.name not in available:
                missing.append(col.name)
        elif col.table == self_name and col.name not in available:
            missing.append(f"{col.table}.{col.name}")
    return missing


def _is_time_role(field: OSIField) -> bool:
    if field.dimension is not None and field.dimension.is_time is not None:
        return field.dimension.is_time
    return field.datatype in TEMPORAL_DATATYPES


def _first_time_dimension(model: SlayerModel) -> str | None:
    return next(
        (c.name for c in model.columns
         if (c.meta or {}).get("osi_is_time") and c.type in _TIME_DIMENSION_TYPES),
        None,
    )


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


def _build_meta(
    ctx: Optional[OSIAIContext],
    custom_extensions: Optional[list[OSICustomExtension]],
    extra: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    meta: dict[str, Any] = dict(extra or {})
    ctx_dict = ai_context_to_dict(ctx)
    if ctx_dict:
        meta["osi_ai_context"] = ctx_dict
    foreign = [e.model_dump() for e in custom_extensions or [] if not ext.is_slayer(e)]
    if foreign:
        meta["osi_custom_extensions"] = foreign
    return meta or None


_EXTENSION = "extension"
_EDITED = "The Ossie content was edited after export, so it wins; re-export from SLayer to restore the rest."


class OsiConversionResult(ConversionResult):
    """The converted models, plus query-backed restorations to save through the engine after them."""
    query_backed: list[SlayerModel] = Field(default_factory=list)


class _PendingMeasure(BaseModel):
    """A measure restored from a ``SLAYER`` payload, added once everything it references exists."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    model: str
    measure: ModelMeasure
    label: str
    #: The metric to convert from its SQL instead, when the restored formula does not validate.
    metric: Optional[OSIMetric] = None
    doc: Optional[OSIDocument] = None
    home: Optional[str] = None


class _MetadataUnavailable(Exception):
    """A dataset's live metadata was not collected, or could not be read."""


class OsiToSlayerConverter:
    """Convert OSI documents into SLayer models: ``collect_metadata`` reads the database, ``convert`` is pure."""

    def __init__(
        self,
        documents: list[OSIDocument],
        data_source: str,
        sa_engine: sa.Engine | None = None,
        *,
        dialect: str | None = None,
        target_dialect: str | None = None,
    ) -> None:
        """Passing ``sa_engine`` is deprecated: ``convert()`` then collects its own metadata through it."""
        if sa_engine is not None:
            warn_deprecated(
                "OsiToSlayerConverter(sa_engine=...) is deprecated; await "
                "converter.collect_metadata(client=engine.client_for(datasource)) before convert(). "
                "sa_engine works until SLayer 2.0.",
                removal="2.0",
            )
        self.documents = documents
        self.data_source = data_source
        self.sa_engine = sa_engine
        self._collected = False
        self.dialect = dialect
        self.target_dialect = target_dialect
        # sqlglot name of the datasource dialect every stored raw SQL is rendered in.
        self._target = dialect_for_ds_type(target_dialect).sqlglot_name
        # Live metadata from ``collect_metadata``: per dataset its introspected model (or the
        # failure), per source query its column types, per probe statement its types (None: failed).
        self._tables: dict[str, SlayerModel | str] = {}
        self._query_types: dict[str, dict[str, ResultColumnType] | str] = {}
        self._probes: dict[str, dict[str, ResultColumnType] | None] = {}
        self._probe_misses: set[str] = set()
        self._reset()

    def _reset(self) -> None:
        self._models: dict[str, SlayerModel] = {}
        # Introspected source columns per model, for field validation.
        self._physical: dict[str, dict[str, Column]] = {}
        self._warnings: list[ConversionWarning] = []
        self._unconverted: list[ConversionWarning] = []
        # (model, column) -> the introspected Column a legacy derived overlay replaced,
        # so an invalidated overlay reverts to the physical column.
        self._shadowed: dict[tuple[str, str], Column] = {}
        # Decoded SLAYER payloads (with whether their fingerprint matches), applied after the plain import.
        self._dataset_payloads: dict[str, tuple[ext.DatasetPayload, bool]] = {}
        self._field_payloads: dict[tuple[str, str], tuple[ext.FieldPayload, bool]] = {}
        self._pending: list[_PendingMeasure] = []
        self._query_backed: list[SlayerModel] = []

    # ---- report helpers ----

    def _warn(self, message: str, *, model_name: str | None = None,
              metric_name: str | None = None, category: str = "general",
              severity: str = "dropped", suggestion: str | None = None) -> None:
        self._warnings.append(ConversionWarning(
            model_name=model_name, metric_name=metric_name, message=message,
            category=category, severity=severity, suggestion=suggestion,
        ))

    def _unconv(self, message: str, *, metric_name: str, category: str = "metric",
                suggestion: str | None = None) -> None:
        self._unconverted.append(ConversionWarning(
            metric_name=metric_name, message=message, category=category,
            severity="unconverted", suggestion=suggestion,
        ))

    def _note_selection(self, selection: Selection, *, label: str, model_name: str | None = None,
                        metric_name: str | None = None) -> None:
        for note in selection.notes:
            self._warn(f"{label}: {note}.", model_name=model_name, metric_name=metric_name,
                       category="dialect", severity="info")

    # ---- top-level ----

    async def collect_metadata(self, *, client: SqlClient) -> None:
        """Read every dataset's source and every type probe conversion needs through ``client``."""
        self._collected = False
        if self.target_dialect is None:
            self._target = dialect_for_ds_type(client.datasource.type).sqlglot_name
        for doc in self.documents:
            for ds in doc.datasets:
                await self._collect_source(client=client, ds=ds)
        for _ in range(_MAX_PROBE_PASSES):
            self._convert()
            misses = self._probe_misses - self._probes.keys()
            if not misses:
                break
            for probe in sorted(misses):
                try:
                    self._probes[probe] = await client.get_result_column_types(probe)
                except Exception:  # noqa: BLE001 — probe is best-effort; conversion falls back
                    self._probes[probe] = None
        self._collected = True

    async def _collect_source(self, *, client: SqlClient, ds: OSIDataset) -> None:
        try:
            parsed = parse_source(ds.source)
        except Exception:  # noqa: BLE001 — conversion reports an unparseable source itself
            return
        try:
            if parsed.query is not None:
                self._query_types[parsed.query] = await client.get_result_column_types(parsed.query)
            elif parsed.table is not None:
                self._tables[ds.name] = await introspect_table_to_model(
                    client=client, table_name=parsed.table, schema=parsed.schema_name,
                    data_source=self.data_source, model_name=ds.name,
                )
        except Exception as exc:  # noqa: BLE001 — per-dataset isolation
            if parsed.query is not None:
                self._query_types[parsed.query] = str(exc)
            else:
                self._tables[ds.name] = str(exc)

    def convert(self) -> OsiConversionResult:
        """Convert from the collected metadata (a deprecated ``sa_engine`` converter collects it first)."""
        if not self._collected and self.sa_engine is not None:
            run_over_caller_engine(
                self.sa_engine, data_source=self.data_source, work=lambda client: self.collect_metadata(client=client),
            )
        return self._convert()

    def _convert(self) -> OsiConversionResult:
        self._reset()
        self._check_duplicate_dataset_names()

        for doc in self.documents:
            for ds in doc.datasets:
                self._build_model(ds=ds, doc=doc)
        for doc in self.documents:
            for rel in doc.relationships or []:
                self._build_join(rel=rel, doc=doc)

        self._validate_cross_model_field_refs()
        self._type_cross_model_columns()
        self._restore_dataset_attributes()

        for doc in self.documents:
            self._build_measures_for(doc)
        self._restore_measures()
        self._restore_fields()
        self._restore_sources()

        return OsiConversionResult(
            models=list(self._models.values()),
            unconverted_metrics=self._unconverted,
            warnings=self._warnings,
            query_backed=self._query_backed,
        )

    def _doc_models(self, doc: OSIDocument) -> list[str]:
        return [d.name for d in doc.datasets if d.name in self._models]

    def _build_measures_for(self, doc: OSIDocument) -> None:
        doc_models = self._doc_models(doc)
        for ds in doc.datasets:
            if ds.name in self._models:
                for metric in ds.metrics or []:
                    self._metric(metric=metric, doc=doc, home=ds.name, doc_models=doc_models)
        for metric in doc.metrics or []:
            self._metric(metric=metric, doc=doc, home=None, doc_models=doc_models)

    def _metric(self, metric: OSIMetric, *, doc: OSIDocument, home: str | None, doc_models: list[str]) -> None:
        """Restore ``metric``'s SLayer measure when its payload matches, else convert its SQL."""
        found = self._payload(metric.custom_extensions, kind="metric", entity=metric.name,
                              label=f"metric {metric.name!r}", model_name=home,
                              ossie=ext.expression_digest(metric.expression))
        if found is not None and found[1]:
            payload = found[0]
            assert isinstance(payload, ext.MetricPayload)
            built = _build_meta(ctx=metric.ai_context, custom_extensions=metric.custom_extensions,
                                extra={"osi_datatype": metric.datatype} if metric.datatype else None)
            measure = payload.measure.model_copy(update={"meta": {**(built or {}), **(payload.measure.meta or {})}
                                                         or None})
            self._pending.append(_PendingMeasure(model=payload.model, measure=measure, label=f"Metric {metric.name!r}",
                                                 metric=metric, doc=doc, home=home))
            return
        if found is not None:
            self._warn(f"Metric {metric.name!r}: {_EDITED}", model_name=home, metric_name=metric.name,
                       category=_EXTENSION, severity="info", suggestion=_EDITED)
        self._build_measure(metric=metric, doc=doc, home=home, doc_models=doc_models)
        if found is not None:
            self._apply_measure_attributes(metric.name, payload=found[0])

    def _apply_measure_attributes(self, name: str, *, payload: ext.Payload) -> None:
        assert isinstance(payload, ext.MetricPayload)
        for model in self._models.values():
            for measure in model.measures:
                if measure.name == name:
                    measure.label = payload.measure.label
                    measure.type = payload.measure.type
                    measure.meta = {**(measure.meta or {}), **(payload.measure.meta or {})} or None
                    return

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
                f"{sorted(dupes)}. Dataset names map to SLayer model names and "
                f"must be unique within a datasource."
            )

    # ---- datasets -> models ----

    def _build_model(self, ds: OSIDataset, doc: OSIDocument) -> None:
        if not _legal_model_name(ds.name):
            self._warn(
                f"Dataset name {ds.name!r} is not a safe SLayer model name "
                f"(empty, surrounding whitespace, or a forbidden character such "
                f"as '.', ':', '/', '\\\\', NUL); skipping.",
                model_name=ds.name, category="illegal_name",
            )
            return

        parsed = parse_source(ds.source)
        resolve_datasource(parsed.database, self.data_source)  # stubbed routing

        try:
            if parsed.is_query:
                base = self._build_sql_mode_model(ds, parsed.query)
            else:
                base = self._introspected_table(ds)
        except Exception as exc:  # noqa: BLE001 — per-dataset isolation
            self._warn(
                f"Failed to introspect dataset {ds.name!r} (source {ds.source!r}): "
                f"{exc}; skipping.",
                model_name=ds.name, category="introspection",
            )
            return

        self._physical[ds.name] = {c.name: c for c in base.columns}
        if doc.legacy:
            self._overlay_fields(base, ds)
        else:
            self._declare_fields(base, ds)
        self._apply_dataset_metadata(base, ds, doc)
        self._models[ds.name] = base
        found = self._payload(ds.custom_extensions, kind="dataset", entity=ds.name, label=f"dataset {ds.name!r}",
                              model_name=ds.name, ossie=ext.source_digest(ds.source, reader=self._target))
        if found is not None:
            payload, matched = found
            assert isinstance(payload, ext.DatasetPayload)
            self._dataset_payloads[ds.name] = (payload, matched)

    def _introspected_table(self, ds: OSIDataset) -> SlayerModel:
        introspected = self._tables.get(ds.name, "no table metadata was collected (collect_metadata)")
        if isinstance(introspected, str):
            raise _MetadataUnavailable(introspected)
        return introspected.model_copy(deep=True)

    def _build_sql_mode_model(self, ds: OSIDataset, query: str) -> SlayerModel:
        # The query's output columns, as the datasource's probe reported them.
        types = self._query_types.get(query, "no query metadata was collected (collect_metadata)")
        if isinstance(types, str):
            raise _MetadataUnavailable(types)
        columns = [_typed_column(name=name, data_type=t.type, db_type=t.lossy_db_type) for name, t in types.items()]
        return SlayerModel(name=ds.name, sql=query, data_source=self.data_source,
                           columns=columns)

    def _declare_fields(self, model: SlayerModel, ds: OSIDataset) -> None:
        """Current spec: the model's columns are exactly the declared fields."""
        if not ds.fields:
            self._warn(f"Dataset {ds.name!r} declares no fields; its model exposes no columns.",
                       model_name=ds.name, category="missing_column", severity="info")
        columns: list[Column] = []
        for field in ds.fields or []:
            col = self._field_column(field=field, ds=ds, model=model)
            if col is None:
                continue
            if any(c.name == col.name for c in columns):
                self._warn(f"Field {field.name!r} on {ds.name!r} is declared twice; keeping the first.",
                           model_name=ds.name, category="duplicate_column")
                continue
            columns.append(col)
        model.columns = columns
        self._apply_primary_key(model, ds)
        model.default_time_dimension = _first_time_dimension(model)

    def _apply_primary_key(self, model: SlayerModel, ds: OSIDataset) -> None:
        """A valid declared key replaces the inherited flags; an empty or unresolved one clears them."""
        if ds.primary_key is None:
            return  # renamed introspected key columns keep their flag
        declared = {c.name for c in model.columns}
        missing = [k for k in ds.primary_key if k not in declared]
        if not ds.primary_key:
            self._warn(f"Dataset {ds.name!r} declares an empty primary_key; ignoring it.",
                       model_name=ds.name, category="primary_key")
        elif missing:
            self._warn(f"Dataset {ds.name!r} primary_key names {missing}, which are not declared "
                       f"fields; not applied.", model_name=ds.name, category="primary_key")
        key = set(ds.primary_key) if ds.primary_key and not missing else set()
        for col in model.columns:
            col.primary_key = col.name in key

    def _overlay_fields(self, model: SlayerModel, ds: OSIDataset) -> None:
        """Legacy: overlay each field on the introspected columns."""
        by_name = {c.name: c for c in model.columns}
        for field in ds.fields or []:
            col = self._field_column(field=field, ds=ds, model=model)
            if col is None:
                continue
            existing = by_name.get(col.name)
            col.primary_key = existing.primary_key if existing is not None else False
            if existing is None:
                model.columns.append(col)
            else:
                if col.sql is not None:
                    self._shadowed.setdefault((model.name, col.name), existing)
                model.columns[model.columns.index(existing)] = col
            by_name[col.name] = col

        if ds.primary_key is not None:
            self._apply_legacy_primary_key(model, ds)
        if not model.default_time_dimension:
            model.default_time_dimension = _first_time_dimension(model)

    def _apply_legacy_primary_key(self, model: SlayerModel, ds: OSIDataset) -> None:
        """A valid key replaces the introspected one; an empty or unresolved one keeps it."""
        assert ds.primary_key is not None
        present = {c.name for c in model.columns}
        missing_pk = [k for k in ds.primary_key if k not in present]
        if not ds.primary_key or missing_pk:
            self._warn(
                f"Dataset {ds.name!r} primary_key is empty or references unknown "
                f"column(s) {missing_pk}; keeping the introspected primary key.",
                model_name=ds.name, category="primary_key",
            )
            return
        osi_pk = set(ds.primary_key)
        for col in model.columns:
            col.primary_key = col.name in osi_pk

    def _field_sql(self, field: OSIField, ds: OSIDataset) -> tuple[str, exp.Expr] | None:
        """The field's selected expression rendered for the target, with its parse tree."""
        label = f"Field {field.name!r} on {ds.name!r}"
        selection = select_expression(field.expression, override=self.dialect, target=self._target)
        self._note_selection(selection, label=label, model_name=ds.name)
        if selection.candidate is None:
            self._warn(f"{label} {selection.reason}; skipping.", model_name=ds.name, category="dialect")
            return None
        try:
            tree = read(selection.candidate)
        except SqlglotError:
            self._warn(f"{label} has an unparseable expression {selection.candidate.text!r}; skipping.",
                       model_name=ds.name, category="expression")
            return None
        return render(selection.candidate, tree, target=self._target), tree

    def _field_column(self, field: OSIField, ds: OSIDataset, model: SlayerModel) -> Column | None:
        """The Column one field maps to, or ``None`` on a reported clean-fail."""
        if not _legal_column_name(field.name):
            self._warn(
                f"Field name {field.name!r} on dataset {ds.name!r} contains "
                f"'.'/':'; skipping the field.",
                model_name=ds.name, category="illegal_name",
            )
            return None
        selected = self._field_sql(field, ds)
        if selected is None:
            return None
        sql, tree = selected
        declared = OSSIE_DATATYPES.get(field.datatype or "")
        bare = _bare_column(tree)
        typed = (self._bare_field_column(field=field, ds=ds, bare=bare, sql=sql) if bare is not None
                 else self._derived_field_column(field=field, ds=ds, model=model, sql=sql, tree=tree,
                                                 declared=declared))
        if typed is None:
            return None
        col, db_type = typed
        if db_type is not None and declared is not None and db_type != declared:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} declares datatype {field.datatype} but the "
                f"database reports {db_type.value}; using {db_type.value}.",
                model_name=ds.name, category="datatype", severity="info",
            )
        self._apply_field_metadata(col=col, field=field)
        found = self._payload(field.custom_extensions, kind="field", entity=field.name,
                              label=f"field {field.name!r} of {ds.name!r}", model_name=ds.name,
                              ossie=ext.expression_digest(field.expression))
        if found is not None:
            payload, matched = found
            assert isinstance(payload, ext.FieldPayload)
            self._field_payloads[(ds.name, field.name)] = (payload, matched)
        return col

    def _bare_field_column(self, *, field: OSIField, ds: OSIDataset, bare: str,
                           sql: str) -> tuple[Column, DataType | None] | None:
        source = self._physical[ds.name].get(bare)
        if source is None:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} references column {bare!r} "
                f"which is not present in the source; skipping.",
                model_name=ds.name, category="missing_column",
            )
            return None
        # A copy keeps the source's type, db_type, key flag and DB comment.
        col = source.model_copy(deep=True, update={"name": field.name, "sql": None if bare == field.name else sql})
        return col, source.type

    def _derived_field_column(self, *, field: OSIField, ds: OSIDataset, model: SlayerModel, sql: str,
                              tree: exp.Expr, declared: DataType | None) -> tuple[Column, DataType | None] | None:
        physical = self._physical[ds.name]
        missing = _missing_expr_columns(tree, available=set(physical), self_name=ds.name)
        if missing:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} references unknown "
                f"column(s) {missing}; skipping.",
                model_name=ds.name, category="missing_column",
            )
            return None
        probed = self._probe_expression_type(model=model, expr=sql)
        if probed is not None:
            return _typed_column(
                name=field.name, sql=sql, data_type=probed.type, db_type=probed.lossy_db_type,
            ), probed.type
        shadowed = physical.get(field.name)
        fallback = declared or (shadowed.type if shadowed is not None else None) or DataType.DOUBLE
        element = shadowed.element_type if shadowed is not None else None
        return _typed_column(name=field.name, sql=sql, data_type=fallback, element=element), None

    @staticmethod
    def _apply_field_metadata(*, col: Column, field: OSIField) -> None:
        col.label = field.label or col.label
        col.description = render_description(explicit=field.description, ctx=field.ai_context) \
            or col.description
        extra: dict[str, Any] = {}
        if field.datatype:
            extra["osi_datatype"] = field.datatype
        explicit = field.dimension.is_time if field.dimension is not None else None
        if explicit is not None or _is_time_role(field):
            extra["osi_is_time"] = _is_time_role(field)
        meta = _build_meta(ctx=field.ai_context, custom_extensions=field.custom_extensions, extra=extra)
        if meta:
            col.meta = {**(col.meta or {}), **meta}

    def _probe_expression_type(self, model: SlayerModel, expr: str) -> ResultColumnType | None:
        """A derived expression's type from a LIMIT-0 probe of the model's source, or ``None``."""
        if model.sql_table:
            from_clause = model.sql_table
        elif model.sql:
            from_clause = f"({model.sql}) AS _osi_sub"
        else:
            return None
        probe = f"SELECT ({expr}) AS _osi_probe FROM {from_clause}"
        if probe not in self._probes:
            self._probe_misses.add(probe)
            return None
        types = self._probes[probe]
        return next(iter(types.values()), None) if types else None

    def _apply_dataset_metadata(self, model: SlayerModel, ds: OSIDataset, doc: OSIDocument) -> None:
        model.description = render_description(explicit=ds.description, ctx=ds.ai_context) \
            or model.description
        extra: dict[str, Any] = {}
        unique_keys = self._unique_keys(model, ds)
        if unique_keys:
            extra["osi_unique_keys"] = unique_keys
        doc_ctx = ai_context_to_dict(doc.ai_context)
        doc_ext = [e.model_dump() for e in doc.custom_extensions or []]
        if doc_ctx or doc.description or doc_ext:
            extra["osi_semantic_model"] = {
                "name": doc.name,
                "description": doc.description,
                "ai_context": doc_ctx,
                **({"custom_extensions": doc_ext} if doc_ext else {}),
            }
        meta = _build_meta(ctx=ds.ai_context, custom_extensions=ds.custom_extensions, extra=extra)
        if meta:
            model.meta = {**(model.meta or {}), **meta}

    def _unique_keys(self, model: SlayerModel, ds: OSIDataset) -> list[list[str]]:
        """Non-empty entries naming only the model's columns (declared fields; legacy: plus source columns)."""
        names = {c.name for c in model.columns}
        kept: list[list[str]] = []
        for key in ds.unique_keys or []:
            missing = [k for k in key if k not in names]
            if not key or missing:
                self._warn(
                    f"Dataset {ds.name!r} unique_keys entry {key} is empty or names "
                    f"{missing}, which are not columns of the model; ignoring it.",
                    model_name=ds.name, category="unique_keys",
                )
                continue
            kept.append(key)
        return kept

    # ---- relationships -> joins ----

    def _build_join(self, rel: OSIRelationship, doc: OSIDocument) -> None:
        src = rel.from_dataset
        doc_models = self._doc_models(doc)
        if src not in doc_models:
            self._warn(
                f"Relationship {rel.name!r} references source dataset {src!r}, "
                f"which is not imported from its document; skipping.",
                category="relationship",
            )
            return
        if rel.to not in doc_models:
            self._warn(
                f"Relationship {rel.name!r} targets dataset {rel.to!r}, which is not "
                f"imported from its document; skipping.",
                model_name=src, category="relationship",
            )
            return
        if not rel.from_columns or len(rel.from_columns) != len(rel.to_columns):
            self._warn(
                f"Relationship {rel.name!r} has empty or mismatched key lengths "
                f"({len(rel.from_columns)} vs {len(rel.to_columns)}); skipping.",
                model_name=src, category="relationship",
            )
            return

        missing = self._missing_join_columns(rel)
        if missing:
            self._warn(
                f"Relationship {rel.name!r} keys on {missing}, which are not plain "
                f"base fields of their datasets; skipping.",
                model_name=src, category="relationship",
            )
            return

        found = self._payload(rel.custom_extensions, kind="relationship", entity=rel.name,
                              label=f"relationship {rel.name!r}", model_name=src,
                              ossie=ext.relationship_digest(from_dataset=src, to=rel.to, from_columns=rel.from_columns,
                                                            to_columns=rel.to_columns))
        if found is not None and not found[1]:
            self._warn(f"Relationship {rel.name!r}: {_EDITED}", model_name=src, category=_EXTENSION,
                       severity="info", suggestion=_EDITED)
        if found is not None and found[1] and self._restore_declarations(rel, payload=found[0]):
            return

        # ModelJoin keys only on target_model, so a second join to one target is unreachable.
        if any(j.target_model == rel.to for j in self._models[src].joins):
            self._warn(
                f"Relationship {rel.name!r} is a second join from {src!r} to "
                f"{rel.to!r}; SLayer cannot disambiguate multiple joins to one "
                f"model (no join aliases). Keeping the first; skipping this one.",
                model_name=src, category="relationship",
            )
            return

        pairs = [[f, t] for f, t in zip(rel.from_columns, rel.to_columns)]
        self._models[src].joins.append(ModelJoin(
            target_model=rel.to,
            join_pairs=pairs,
            # OSI relationships are direction-implied: from = many, to = one.
            cardinality=JoinCardinality.MANY_TO_ONE,
            description=render_description(explicit=None, ctx=rel.ai_context),
            meta=_build_meta(ctx=rel.ai_context, custom_extensions=rel.custom_extensions),
        ))

    def _validate_cross_model_field_refs(self) -> None:
        """Drop columns whose cross-model refs don't resolve through the join graph.

        Fixed-point: a dropped column can invalidate another column or a join.
        """
        while True:
            if not (self._drop_invalid_cross_model_columns()
                    or self._drop_stale_joins()):
                break

    def _drop_invalid_cross_model_columns(self) -> bool:
        dropped = False
        for model in self._models.values():
            for col, bad in self._invalid_cross_model_columns(model):
                self._revert_or_drop_column(model=model, col=col, bad=bad)
                dropped = True
        return dropped

    def _invalid_cross_model_columns(
        self, model: SlayerModel
    ) -> list[tuple[Column, list[str]]]:
        result = []
        for col in model.columns:
            if col.sql:
                bad = self._unresolvable_cross_model_refs(model=model, sql=col.sql)
                if bad:
                    result.append((col, bad))
        return result

    def _revert_or_drop_column(
        self, model: SlayerModel, col: Column, bad: list[str]
    ) -> None:
        original = self._shadowed.pop((model.name, col.name), None)
        if original is not None:
            model.columns[model.columns.index(col)] = original
            self._warn(
                f"Derived overlay for {col.name!r} on {model.name!r} references "
                f"unresolvable cross-model column(s) {bad}; reverting to the "
                f"physical column.",
                model_name=model.name, category="missing_column",
            )
        else:
            model.columns.remove(col)
            if model.default_time_dimension == col.name:
                model.default_time_dimension = _first_time_dimension(model)
            self._warn(
                f"Column {col.name!r} on {model.name!r} references unresolvable "
                f"cross-model column(s) {bad}; dropping.",
                model_name=model.name, category="missing_column",
            )

    def _drop_stale_joins(self) -> bool:
        """Drop joins whose key columns were removed by column validation."""
        dropped = False
        for model in self._models.values():
            stale = [j for j in model.joins if self._join_columns_missing(model=model, join=j)]
            for join in stale:
                model.joins.remove(join)
                dropped = True
                self._warn(
                    f"Join from {model.name!r} to {join.target_model!r} "
                    f"references a column removed during validation; dropping.",
                    model_name=model.name, category="relationship",
                )
        return dropped

    def _parse_target(self, sql: str) -> exp.Expr | None:
        try:
            return sqlglot.parse_one(sql, read=self._target)
        except SqlglotError:
            return None

    def _type_cross_model_columns(self) -> None:
        """Type a derived column that is exactly one cross-model ref from the column its chain of such refs ends at."""
        for model in self._models.values():
            for col in model.columns:
                end = self._ref_chain_end(model=model, col=col)
                if end is not None:
                    col.type, col.element_type = end.type, end.element_type

    def _ref_chain_end(self, *, model: SlayerModel, col: Column) -> Column | None:
        """The column a chain of single cross-model refs from ``col`` ends at; ``None`` without a ref or on a cycle."""
        seen = {id(col)}
        end: Column | None = None
        while col.sql and (hop := self._single_cross_model_ref(model=model, sql=col.sql)) is not None:
            model, col = hop
            if id(col) in seen:
                return None
            seen.add(id(col))
            end = col
        return end

    def _single_cross_model_ref(self, model: SlayerModel, sql: str) -> tuple[SlayerModel, Column] | None:
        """The model and column ``sql`` names when it is exactly one cross-model ref."""
        tree = self._parse_target(sql)
        if not isinstance(tree, exp.Column):
            return None
        # The full qualifier chain: a multi-hop ref keeps earlier hops in .db/.catalog.
        parts = [p.name for p in tree.parts]
        quals = parts[:-1]
        if not quals or quals == [model.name]:
            return None
        target = self._walk_join_alias(host=model, alias=".".join(quals))
        if target is None:
            return None
        column = next((c for c in target.columns if c.name == parts[-1]), None)
        return None if column is None else (target, column)

    def _join_columns_missing(self, model: SlayerModel, join: ModelJoin) -> bool:
        target = self._models.get(join.target_model)
        for src_col, tgt_col in join.join_pairs:
            if not self._model_has_column(model_name=model.name, column=src_col):
                return True
            if target is None or not any(c.name == tgt_col for c in target.columns):
                return True
        return False

    def _is_physical_multihop_ref(self, *, model: SlayerModel, quals: list[str]) -> bool:
        """True for a multi-hop qualifier whose first hop is not a join hop (a physical
        ``schema.table.column`` ref). An ambiguous hop IS a join hop."""
        if len(quals) < 2:
            return False
        try:
            return resolve_hop(
                current=model, token=quals[0], models_by_name=self._models
            ) is None
        except AmbiguousJoinPathError:
            return False

    def _unresolvable_cross_model_refs(self, model: SlayerModel, sql: str) -> list[str]:
        """Root-scope cross-model column refs in ``sql`` that don't resolve to a joined
        model + existing column; physical multi-part refs are skipped."""
        tree = self._parse_target(sql)
        if tree is None:
            return []
        root_ids = root_scope_column_ids(parsed=tree)
        bad = []
        for col in tree.find_all(exp.Column):
            if id(col) not in root_ids:
                continue  # nested-scope alias (CTE / sub-query) — not a join ref
            parts = [p.name for p in col.parts]
            quals, leaf = parts[:-1], parts[-1]
            if not quals or quals == [model.name]:
                continue  # self / unqualified — validated at field time
            if self._is_physical_multihop_ref(model=model, quals=quals):
                continue
            target = self._walk_join_alias(host=model, alias=".".join(quals))
            if target is None or not any(c.name == leaf for c in target.columns):
                bad.append(".".join(parts))
        return bad

    def _walk_join_alias(self, host: SlayerModel, alias: str) -> SlayerModel | None:
        """Resolve a dotted join qualifier to its terminal joined model, or None (never ``__``-split)."""
        return resolve_ref_target(
            qualifiers=tuple(alias.split(".")),
            source_model=host,
            models_by_name=self._models,
        )

    def _model_has_column(self, model_name: str, column: str) -> bool:
        model = self._models.get(model_name)
        return model is not None and any(c.name == column for c in model.columns)

    def _model_has_name(self, model_name: str, name: str) -> bool:
        """Whether ``name`` is taken by a column or measure (one shared namespace)."""
        model = self._models.get(model_name)
        if model is None:
            return False
        return (any(c.name == name for c in model.columns)
                or any(m.name == name for m in model.measures))

    def _missing_join_columns(self, rel: OSIRelationship) -> list[str]:
        """Qualified relationship key names that are not plain base columns of their model."""
        sides = ((rel.from_dataset, rel.from_columns), (rel.to, rel.to_columns))
        return [
            f"{name}.{c}" for name, cols in sides for c in cols
            if join_key_error(model=rel.from_dataset, target=rel.to, key=c, side=name,
                              columns=self._models[name].columns) is not None
        ]

    # ---- metrics -> measures ----

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

    def _canonicalize_refs(self, tree: exp.Expr, *, doc_models: list[str], home: str | None) -> exp.Expr:
        """``tree`` with current-spec metric refs rewritten to declared names; raises ``LookupError`` on a scope violation."""
        def canonical(col: exp.Expr) -> exp.Expr:
            if not isinstance(col, exp.Column):
                return col
            idents = [p for p in col.parts if isinstance(p, exp.Identifier)]
            if len(idents) != len(col.parts) or len(idents) > 2:
                raise LookupError(f"reference {col.sql()!r} is not a field reference")
            *qual, leaf = idents
            if home is None:
                if not qual:
                    raise LookupError(f"unqualified reference {leaf.name!r}; model-scoped metrics "
                                      f"reference fields as dataset.field")
                owner = _match_name(qual[0], doc_models)
            elif qual:
                raise LookupError(f"qualified reference {col.sql()!r}; dataset-scoped metrics "
                                  f"reference their own dataset's fields unqualified")
            else:
                owner = home
            declared = [c.name for c in self._models[owner].columns if not c.hidden]
            return exp.column(_match_name(leaf, declared), table=owner if home is None else None)
        return tree.transform(canonical, copy=False)

    def _build_measure(self, metric: OSIMetric, *, doc: OSIDocument, home: str | None,
                       doc_models: list[str]) -> None:
        if not _legal_measure_name(metric.name):
            self._unconv(
                f"Metric name {metric.name!r} is not a valid SLayer identifier.",
                metric_name=metric.name,
            )
            return

        tree = self._metric_tree(metric)
        if tree is None:
            return
        if not doc.legacy:
            try:
                tree = self._canonicalize_refs(tree, doc_models=doc_models, home=home)
            except LookupError as exc:
                self._unconv(f"Metric {metric.name!r}: {exc}.", metric_name=metric.name)
                return

        scope = [home] if home is not None else doc_models
        owner_of = self._make_owner_of(scope)
        anchor = home or self._select_anchor(metric_name=metric.name, tree=tree,
                                             doc_models=doc_models, owner_of=owner_of)
        if anchor is None:
            return  # already reported

        # Measures and columns share one namespace on the anchor model.
        anchor_model = self._models[anchor]
        if any(m.name == metric.name for m in anchor_model.measures):
            self._unconv(
                f"Metric {metric.name!r} duplicates an existing measure on "
                f"model {anchor!r}.",
                metric_name=metric.name, category="duplicate_measure",
            )
            return
        if self._model_has_column(model_name=anchor, column=metric.name):
            self._unconv(
                f"Metric {metric.name!r} collides with a column name on model "
                f"{anchor!r}.",
                metric_name=metric.name, category="name_collision",
            )
            return

        result = convert_expression(
            tree, entity_name=metric.name, owner_of=owner_of, ref_of=self._make_ref_of(anchor=anchor),
            percentile_unsupported=self._target in _NO_PERCENTILE_DIALECTS,
            name_taken=self._model_has_name, target=self._target,
        )
        if not result.ok or result.formula is None:
            self._unconv(
                f"Metric {metric.name!r}: {result.reason}",
                metric_name=metric.name,
            )
            return

        # Build the measure before materializing columns, so a rejected metric leaves no orphans.
        try:
            parse_expr(result.formula)
            measure = ModelMeasure(
                formula=result.formula,
                name=metric.name,
                description=render_description(explicit=metric.description, ctx=metric.ai_context),
                meta=_build_meta(ctx=metric.ai_context, custom_extensions=metric.custom_extensions,
                                 extra={"osi_datatype": metric.datatype} if metric.datatype else None),
            )
        except Exception as exc:  # noqa: BLE001 — any parse/validation error -> report
            self._unconv(
                f"Metric {metric.name!r}: emitted formula {result.formula!r} is "
                f"not a valid SLayer measure ({exc}).",
                metric_name=metric.name,
            )
            return

        self._materialize_columns(result.materialized)
        anchor_model.measures.append(measure)
        for w in result.warnings:
            self._warn(f"Metric {metric.name!r}: {w}", metric_name=metric.name,
                       category="dialect", severity="info")

    def _select_anchor(self, metric_name: str, tree: exp.Expr, doc_models: list[str],
                       owner_of: OwnerOf) -> str | None:
        def owners_in(node: exp.Expr) -> set[str]:
            return {o for col in node.find_all(exp.Column) if (o := owner_of(col.table or None, col.name))}

        owners = owners_in(tree)
        if owners:
            # One aggregate reading several datasets must reach them all over to-one hops.
            spans = set().union(*(s for agg in outermost_aggregates(tree) if len(s := owners_in(agg)) > 1))
            anchor = self._min_hops_root(candidates=doc_models, mentioned=owners, to_one=spans)
            if anchor is None:
                pairs = {
                    p.describe(): None
                    for c in doc_models for m in sorted(owners)
                    for p in blocking_pairs(root=self._models[c], target=m, models_by_name=self._models)
                }
                self._unconv(
                    f"Metric {metric_name!r} references models {sorted(owners)} "
                    f"with no single model reaching all of them via joins."
                    + "".join(f" {d}." for d in pairs),
                    metric_name=metric_name, category="no_join_path",
                    suggestion="Add the required relationship(s), or split the "
                               "metric across a multi-stage query.",
                )
            return anchor
        # No column refs (e.g. COUNT(*)): the unique fact table, else an orphan of ambiguous grain.
        anchor = self._fact_root(doc_models)
        if anchor is None:
            self._unconv(
                f"Metric {metric_name!r} has no column references and the "
                f"semantic model has no unique fact table to attribute it to; "
                f"its grain is ambiguous.",
                metric_name=metric_name, category="orphan_metric",
                suggestion="Aggregate over an explicit column, or ensure the "
                           "semantic model has a single fact dataset.",
            )
        return anchor

    def _fact_root(self, doc_models: list[str]) -> str | None:
        """The unique dataset that is never a join target, or ``None``."""
        targets = {j.target_model for name in doc_models for j in self._models[name].joins}
        non_targets = [n for n in doc_models if n not in targets]
        return non_targets[0] if len(non_targets) == 1 else None

    def _make_owner_of(self, scope: list[str]) -> OwnerOf:
        def owner_of(qualifier: Optional[str], column: str) -> Optional[str]:
            if qualifier is not None:
                return qualifier if qualifier in scope and self._model_has_column(qualifier, column) else None
            # Unqualified: exactly one owner, else unresolved (never bound by dataset order).
            matches = [name for name in scope if self._model_has_column(name, column)]
            return matches[0] if len(matches) == 1 else None
        return owner_of

    def _min_hops_root(self, *, candidates: list[str], mentioned: set[str], to_one: set[str]) -> str | None:
        """Among ``candidates`` routing to every ``mentioned`` model (``to_one`` ones without
        fanning): prefer no fanning hop, then the fewest total hops, a mentioned candidate, the smallest name."""
        keyed = []
        for c in candidates:
            routed = shortest_routes(root=self._models[c], models_by_name=self._models)
            if all(m in routed for m in mentioned) and not any(routed[m].fanning for m in to_one):
                fans = any(routed[m].fanning for m in mentioned)
                keyed.append((fans, sum(len(routed[m].edges) for m in mentioned), 0 if c in mentioned else 1, c))
        return min(keyed)[3] if keyed else None

    def _make_ref_of(self, anchor: str) -> Callable[[str, str], Optional[str]]:
        def ref_of(model: str, column: str) -> Optional[str]:
            route = shortest_route(root=self._models[anchor], target=model, models_by_name=self._models)
            if route is None:
                return None
            return ".".join([*route.tokens, column])
        return ref_of

    def _physical_sql(self, model: SlayerModel, sql: str) -> str:
        """``sql`` with each field reference replaced by the field's own SQL, for type probing."""
        tree = self._parse_target(sql)
        if tree is None:
            return sql
        by_name = {c.name: c for c in model.columns}

        def inline(ref: exp.Expr) -> exp.Expr:
            col = by_name.get(ref.name) if isinstance(ref, exp.Column) and not ref.table else None
            definition = self._parse_target(col.sql) if col is not None and col.sql else None
            return exp.Paren(this=definition) if definition is not None else ref
        return tree.transform(inline, copy=False).sql(dialect=self._target)

    def _materialize_columns(self, materialized: list[MaterializedColumn]) -> None:
        for mc in materialized:
            model = self._models.get(mc.owning_model)
            if model is None or any(c.name == mc.name for c in model.columns):
                continue
            probed = self._probe_expression_type(model=model, expr=self._physical_sql(model, mc.sql))
            model.columns.append(_typed_column(
                name=mc.name, sql=mc.sql, hidden=True,
                data_type=probed.type if probed is not None else DataType.DOUBLE,
                db_type=probed.lossy_db_type if probed is not None else None,
            ))

    # ---- SLAYER extension restore ----

    def _payload(self, extensions: Optional[list[OSICustomExtension]], *, kind: str, entity: str, label: str,
                 model_name: str | None, ossie: Any) -> tuple[ext.Payload, bool] | None:
        """The entity's decoded ``SLAYER`` payload and whether its fingerprint matches; ``None`` (reported) otherwise."""
        entries = [e for e in extensions or [] if ext.is_slayer(e)]
        if not entries:
            return None
        if len(entries) > 1:
            self._warn(f"The {label} carries {len(entries)} SLAYER extensions; ignoring them.",
                       model_name=model_name, category=_EXTENSION, suggestion="Keep exactly one SLAYER extension.")
            return None
        try:
            payload = ext.decode(entries[0].data)
        except ValueError as exc:
            self._warn(f"The SLAYER extension on the {label} is malformed or of an unknown version; ignoring it "
                       f"({exc}).", model_name=model_name, category=_EXTENSION,
                       suggestion="Re-export the document from SLayer.")
            return None
        if payload.kind != kind or payload.entity != entity:
            self._warn(f"The SLAYER extension on the {label} describes {payload.kind} {payload.entity!r}; "
                       f"ignoring it.", model_name=model_name, category=_EXTENSION,
                       suggestion="Re-export the document from SLayer.")
            return None
        return payload, ext.matches(payload, ossie=ossie)

    def _restore_declarations(self, rel: OSIRelationship, *, payload: ext.Payload) -> bool:
        """Add the SLayer joins ``rel`` stands for, when every endpoint and key validates."""
        assert isinstance(payload, ext.RelationshipPayload)
        built = _build_meta(ctx=rel.ai_context, custom_extensions=rel.custom_extensions)
        joins: list[tuple[SlayerModel, ModelJoin]] = []
        for declaration in payload.declarations:
            model, join = self._models.get(declaration.model), declaration.join
            target = self._models.get(join.target_model)
            problem = None
            if model is None or target is None:
                problem = f"joins {declaration.model!r} to {join.target_model!r}, which are not both imported"
            elif any(j.target_model == join.target_model and j.name == join.name for j in model.joins):
                problem = f"duplicates a join from {model.name!r} to {join.target_model!r}"
            else:
                errors = [join_key_error(model=model.name, target=target.name, key=a, side=model.name,
                                         columns=model.columns) or
                          join_key_error(model=model.name, target=target.name, key=b, side=target.name,
                                         columns=target.columns)
                          for a, b in join.join_pairs]
                problem = next((str(e).splitlines()[0] for e in errors if e is not None), None)
            if problem is not None:
                self._warn(f"Relationship {rel.name!r}: its SLayer join {problem}; imported as a plain "
                           f"many-to-one join.", model_name=rel.from_dataset, category=_EXTENSION,
                           suggestion="Import every model the join connects.")
                return False
            assert model is not None
            joins.append((model, join.model_copy(update={"meta": {**(built or {}), **(join.meta or {})} or None})))
        for model, join in joins:
            model.joins.append(join)
        return True

    def _restore_dataset_attributes(self) -> None:
        """Model attributes Ossie has no place for, and the SLayer-only columns, from each dataset payload."""
        for name, (payload, _) in self._dataset_payloads.items():
            model = self._models.get(name)
            if model is None:
                continue
            model.hidden = payload.hidden
            model.access_tags = list(payload.access_tags)
            model.aggregations = list(payload.aggregations)
            model.meta = {**(model.meta or {}), **(payload.meta or {})} or None
            if payload.default_time_dimension is None or model.get_column(payload.default_time_dimension):
                model.default_time_dimension = payload.default_time_dimension
            for column in payload.columns:
                self._restore_column(model, column)

    def _restore_column(self, model: SlayerModel, column: Column) -> None:
        bad = [] if not column.sql else self._unresolvable_cross_model_refs(model=model, sql=column.sql)
        if self._model_has_name(model.name, column.name) or bad:
            reason = f"reads {bad}, which do not resolve" if bad else "collides with an imported name"
            self._warn(f"SLayer-only column {column.name!r} on {model.name!r} {reason}; skipping it.",
                       model_name=model.name, category=_EXTENSION, suggestion="Import every model it reads.")
            return
        model.columns.append(column)

    def _restore_measures(self) -> None:
        """Add restored and SLayer-only measures once everything they reference exists; a remaining one falls back."""
        for name, (payload, _) in self._dataset_payloads.items():
            for measure in payload.measures:
                self._pending.append(_PendingMeasure(model=name, measure=measure,
                                                     label=f"SLayer-only measure {measure.name or measure.formula!r}"))
        pending, errors = list(self._pending), {}
        progress = True
        while pending and progress:
            progress = False
            for item in list(pending):
                error = self._add_measure(item)
                if error is None:
                    pending.remove(item)
                    progress = True
                else:
                    errors[id(item)] = error
        for item in pending:
            self._fall_back(item, error=errors[id(item)])

    def _add_measure(self, item: _PendingMeasure) -> str | None:
        model = self._models.get(item.model)
        if model is None:
            return f"its model {item.model!r} is not imported"
        if item.measure.name and self._model_has_name(item.model, item.measure.name):
            return f"its name collides on {item.model!r}"
        model.measures.append(item.measure)
        try:
            bind_model_formula(item.measure.formula, model=model, models=list(self._models.values()),
                               dialect=self._target)
        except Exception as exc:  # noqa: BLE001 — any binding failure keeps it pending
            model.measures.remove(item.measure)
            return str(exc).splitlines()[0]
        return None

    def _fall_back(self, item: _PendingMeasure, *, error: str) -> None:
        if item.metric is None or item.doc is None:
            self._warn(f"{item.label} on {item.model!r} does not validate ({error}); skipping it.",
                       model_name=item.model, metric_name=item.measure.name, category=_EXTENSION,
                       suggestion="Import every model it reads.")
            return
        self._warn(f"{item.label}: its SLayer measure does not validate ({error}); imported from its SQL.",
                   model_name=item.model, metric_name=item.metric.name, category=_EXTENSION, severity="info",
                   suggestion="Import every model it reads.")
        self._build_measure(metric=item.metric, doc=item.doc, home=item.home, doc_models=self._doc_models(item.doc))

    def _restore_fields(self) -> None:
        """Column attributes; the value/filter split only where the field's fingerprint matches."""
        for (name, field), (payload, matched) in self._field_payloads.items():
            model = self._models.get(name)
            column = model.get_column(field) if model is not None else None
            if model is None or column is None:
                continue
            content = payload.column
            column.unique, column.hidden = content.unique, content.hidden
            column.format, column.granularity = content.format, content.granularity
            column.allowed_aggregations = content.allowed_aggregations
            column.meta = {**(column.meta or {}), **(content.meta or {})} or None
            if not matched:
                self._warn(f"Field {field!r} of {name!r}: {_EDITED}", model_name=name, category=_EXTENSION,
                           severity="info", suggestion=_EDITED)
                continue
            parts = [p for p in (content.sql, content.filter) if p]
            if any(self._unresolvable_cross_model_refs(model=model, sql=p) for p in parts):
                self._warn(f"Field {field!r} of {name!r}: its SLayer definition does not resolve; kept its "
                           f"expression.", model_name=name, category=_EXTENSION, severity="info",
                           suggestion="Import every model it reads.")
                continue
            column.sql, column.filter = content.sql, content.filter
            column.type = content.type or column.type

    def _restore_sources(self) -> None:
        """Table/SQL with row filters in place; saved queries handed back for the engine to validate."""
        for name, (payload, matched) in self._dataset_payloads.items():
            model = self._models.get(name)
            if model is None:
                continue
            if not matched:
                self._warn(f"Dataset {name!r}: {_EDITED}", model_name=name, category=_EXTENSION, severity="info",
                           suggestion=_EDITED)
                continue
            source = payload.source
            if source.source_queries is None:
                model.sql_table, model.sql, model.filters = source.sql_table, source.sql, list(source.filters)
                continue
            try:
                restored = model.model_copy(update={
                    "sql": None, "sql_table": None, "filters": [], "columns": [],
                    "source_queries": [SlayerQuery.model_validate(q) for q in source.source_queries],
                    "query_variables": dict(source.query_variables),
                })
            except ValueError as exc:
                self._warn(f"Dataset {name!r}: its saved queries do not load ({exc}); imported as a sql-mode "
                           f"model over its source.", model_name=name, category=_EXTENSION, severity="info",
                           suggestion="Re-export the document from SLayer.")
                continue
            self._query_backed.append(restored)
