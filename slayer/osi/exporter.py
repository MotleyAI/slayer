"""Export one datasource's SLayer datasets as a current-spec Ossie document.

Everything Ossie defines with SLayer's meaning goes out as Ossie; SLayer-only
content travels in one ``SLAYER`` non-portable extension per entity; everything else
is reported (entity, reason, workaround), never silently dropped.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Literal, cast

import warnings

import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.document_text import edit_text, parse_document_text, without_vendor
from slayer.core.deprecation import outside_stacklevel
from slayer.core.errors import (
    AmbiguousReferenceError, OssieDocumentInvalidError, SlayerError, StoredDocumentLoadError, UnreadableDocumentError,
)
from slayer.core.granularity import CustomGranularity
from slayer.core.warnings import SlayerUnloadableDocumentWarning, UnloadableDocumentWarning
from slayer.core.names import match_name
from slayer.core.field_expression import OSSIE_SQL, ExpressionObject, dialect_entries, portable_text, token
from slayer.core.enums import RANKED_AGGREGATIONS, RelationshipCardinality
from slayer.core.relationship_walker import (
    aggregation_owner, dataset_field_type, terminal_dataset, unique_key_sets, walk,
)
from slayer.core.time_points import parse_temporal_value
from slayer.core.keys import (
    MIRRORED_TIME_POINT_OP,
    AggregateKey,
    ArithmeticKey,
    FieldKey,
    FieldSqlKey,
    LiteralKey,
    SqlFragmentKey,
    StarKey,
    TimePointCmpKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    aggregation_source_type,
    walk_value_keys,
)
from slayer.core.models import (
    SOURCE_READER_PREFIX,
    WINDOW_PARAM,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
    is_key_set_unique,
    aggregation_definition,
    relationship_key_error,
    rendered_expression,
)
from slayer.core.syntax import DottedRef, Ref, reference_spans, walk_parsed_refs
from slayer.engine.binding import bind_dataset_expression
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ingest_report import ConversionReport, ConversionWarning
from slayer.core import slayer_extension as ext
from slayer.sql.ossie_dialects import PORTABLE_READER, expression_digest, ossie_token, source_digest
from slayer.osi.models import OSSIE_DATATYPES, TEMPORAL_DATATYPES, explicit_description
from slayer.osi.yaml_writer import dump_yaml
from slayer.core.ossie_expression import OSIExpression
from slayer.core.ossie_steps import ossie_from
from slayer.core.ossie_validity import portable_problem
from slayer.core.portable_sql import portable_sql
from slayer.sql.field_expansion import expand_field_definition_parts_sync, expand_derived_refs_sync
from slayer.sql.root_scope import root_scope_field_ids
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.ossie import OssieDialect
from slayer.sql.render.aggregates import AggregateSpec, render_aggregate
from slayer.sql.render.value_expr import AliasFacilities, CompositeFacilities, RenderContext, render_value_key

OSSIE_VERSION = "0.3.0.dev0"
_SELF = "__slayer_self"
# Aggregations with no Ossie metric form: ranked picks and list collection.
_SLAYER_ONLY_AGGREGATIONS = frozenset(RANKED_AGGREGATIONS) | {"collect"}
_EXCLUDED_FIELD_FIELDS = {"sampled", "sampled_values", "distinct_count"}

_RESTORE = "Re-import the document with `slayer import-osi` to restore it; other Ossie tools do not see it."
_WITH_EXTENSIONS = "Export with SLAYER extensions (drop --no-extensions) to carry it."
_LOSSY = "Export with SLAYER extensions (drop --no-extensions) for an exact SLayer round trip."


class OsiExportError(Exception):
    """An export request names an unknown datasource or dataset, or nothing is exportable."""


class OsiExportResult(ConversionReport):
    """The exported Ossie document, its text and its report."""
    document: dict[str, Any] = Field(default_factory=dict)
    text: str = ""


class _Refused(Exception):
    def __init__(self, reason: str, suggestion: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.suggestion = suggestion


class _Names:
    """Names unique under case-insensitive matching."""

    def __init__(self) -> None:
        self._taken: set[str] = set()

    def allocate(self, base: str) -> str:
        name, n = base, 2
        while name.lower() in self._taken:
            name, n = f"{base}_{n}", n + 1
        self._taken.add(name.lower())
        return name


class _Dataset(BaseModel):
    """One exported dataset: its graph dataset (a query-backed one's rendered form), source and fields."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataset: SlayerDataset
    graph: SlayerDataset
    source: str
    structure: ext.SourceStructure
    osi_fields: dict[str, dict[str, Any]] = {}
    fields: dict[str, DatasetField] = {}
    slayer_fields: list[DatasetField] = []
    slayer_metrics: list[DatasetMetric] = []
    primary_key: list[str] = []
    unique_keys: list[list[str]] = []
    metrics: list[dict[str, Any]] = []


class _Metric(BaseModel):
    """A metric classified as an Ossie metric."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    home: str
    metric: DatasetMetric
    scope: Literal["dataset", "model"]
    dataset: str
    tree: exp.Expr
    portable: bool
    name: str = ""


#: ``(from_dataset, to, from_columns, to_columns)``.
_Shape = tuple[str, str, list[str], list[str]]


class _Relationship(BaseModel):
    """One FK-direction relationship and the SLayer relationship it stands for."""

    from_dataset: str
    to: str
    from_columns: list[str]
    to_columns: list[str]
    declaration: ext.RelationshipDeclaration
    name: str = ""


def _slayer_meta(meta: dict[str, Any] | None) -> dict[str, Any] | None:
    """``meta`` without the importer's ``osi_*`` keys, which the Ossie content regenerates."""
    kept = {k: v for k, v in (meta or {}).items() if not k.startswith("osi_")}
    return kept or None


def _strip_self(tree: exp.Expr) -> exp.Expr:
    for column in tree.find_all(exp.Column):
        if column.table == _SELF:
            column.set("table", None)
    return tree


def _datatype(field: DatasetField) -> str | None:
    """The field's datatype as Ossie writes it; a list has none."""
    return field.datatype.value if field.datatype is not None and field.datatype.value in OSSIE_DATATYPES else None


def _time_role(field: DatasetField) -> bool:
    recorded = (field.meta or {}).get("osi_is_time")
    if isinstance(recorded, bool):
        return recorded
    return field.datatype is not None and field.datatype.is_temporal


_AGGREGATE_MARK = "slayer_rendered_aggregate"
#: Stands in for an ``AGG(d.m)`` operand while a model-scoped metric re-renders.
_SENTINEL = 7_314_159_265_358


def _ossie_division(tree: Expression) -> Expression:
    """Drop the ``NULLIF(d, 0)`` the composer wraps each divisor in (Ossie ``/`` reads back as SLayer ``/``);
    a rendered aggregate's own template is kept."""
    for div in list(tree.find_all(exp.Div)):  # NOSONAR(S7504) — snapshot: replace() mutates the tree mid-walk
        node: exp.Expr | None = div
        while node is not None and not node.meta.get(_AGGREGATE_MARK):
            node = node.parent
        divisor = div.expression
        if node is None and isinstance(divisor, exp.Nullif) and divisor.expression == exp.Literal.number(0):
            divisor.replace(divisor.this)
    return tree


def _instant_comparisons(key: ValueKey) -> ValueKey:
    """``key`` with each comparison against an instant (a bare date is its midnight) as the plain SQL comparison."""
    key = cast(ValueKey, key.map_children(_instant_comparisons))
    if not isinstance(key, TimePointCmpKey) or key.reads_whole_day or parse_temporal_value(key.point.strip()) is None:
        return key
    literal = LiteralKey(value=key.point.strip())
    if key.literal_on_left:
        return ArithmeticKey(op=_SQL_OP[MIRRORED_TIME_POINT_OP[key.op]], operands=(literal, key.operand))
    return ArithmeticKey(op=_SQL_OP[key.op], operands=(key.operand, literal))


_SQL_OP = {"=": "==", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}


def _refuse_slayer_only(agg: AggregateKey) -> None:
    if agg.agg in _SLAYER_ONLY_AGGREGATIONS:
        raise _Refused(f"uses {agg.agg}, which has no Ossie metric form", _RESTORE)
    windowed = (agg.partition_keys is not None or agg.locus != "target" or bool(agg.args)
                or any(k == WINDOW_PARAM for k, _ in agg.kwargs))
    if windowed:
        raise _Refused("is a windowed or partitioned aggregate", _RESTORE)


def _calls(tree: exp.Expr) -> set[str]:
    """The lower-cased names of the functions ``tree`` calls."""
    return {(f.name if isinstance(f, exp.Anonymous) else f.sql_name()).lower() for f in tree.find_all(exp.Func)}


def _outermost_aggregates(key: ValueKey) -> Iterator[AggregateKey]:
    if isinstance(key, AggregateKey):
        yield key
        return
    for child in key.children():
        yield from _outermost_aggregates(child)


def _row_leaves(value: Any) -> Iterator[FieldKey | FieldSqlKey | StarKey]:
    if isinstance(value, (FieldKey, FieldSqlKey, StarKey)):
        yield value
    elif isinstance(value, SqlFragmentKey):
        yield from value.refs
    elif isinstance(value, (AggregateKey, TransformKey)):
        raise _Refused("re-aggregates another aggregate", _RESTORE)
    elif hasattr(value, "children"):
        for child in value.children():
            yield from _row_leaves(child)


class _Exporter:
    def __init__(self, *, config: DatasourceConfig, datasets: dict[str, SlayerDataset], selected: list[str],
                 virtual: dict[str, SlayerDataset | Exception], extensions: bool,
                 resolved: dict[str, SlayerDataset | Exception] | None = None,
                 documents: list[dict[str, Any]] | None = None) -> None:
        self.config = config
        #: The stored documents, each merged verbatim; their datasets are fixed.
        self.documents = documents or []
        self.fixed: dict[str, dict[str, Any]] = {
            d["name"]: d for doc in self.documents for d in doc["datasets"] if d["name"] in datasets}
        self.datasets = datasets
        self.selected = selected
        self.extensions = extensions
        self.dialect: SqlDialect = dialect_for_ds_type(config.type)
        self.target = self.dialect.sqlglot_name
        self.token = ossie_token(config.type)
        self.neutral = OssieDialect()
        self.granularities = config.granularity_definitions
        self.virtual = virtual
        self.graph: dict[str, SlayerDataset] = {
            name: r if isinstance(r := (resolved or {}).get(name), SlayerDataset) else d for name, d in datasets.items()}
        for name, rendered in virtual.items():
            if isinstance(rendered, SlayerDataset):
                stored = datasets[name]
                self.graph[name] = rendered.model_copy(update={
                    "metrics": stored.metrics, "relationships": stored.relationships, "aggregations": stored.aggregations,
                })
        self.osi_datasets: dict[str, _Dataset] = {}
        self.relationships: list[_Relationship] = []
        self.model_metrics: list[dict[str, Any]] = []
        #: Exported dataset-scoped metrics by (home dataset, SLayer name), case-folded: ``(dataset, exported name)``.
        self.exported: dict[tuple[str, str], tuple[str, str]] = {}
        self.entries: list[ConversionWarning] = []

    # ---- report ----

    def _report(self, message: str, *, severity: Literal["dropped", "info"], category: str, suggestion: str,
                dataset: str | None = None, metric: str | None = None) -> None:
        self.entries.append(ConversionWarning(dataset_name=dataset, metric_name=metric, message=message,
                                              category=category, severity=severity, suggestion=suggestion))

    def _slayer_only(self, message: str, *, dataset: str, metric: str | None = None) -> None:
        if self.extensions:
            self._report(f"{message}; it travels only in the SLAYER extension.", severity="info",
                         category="extension_only", suggestion=_RESTORE, dataset=dataset, metric=metric)
        else:
            self._report(f"{message}; not exported.", severity="dropped", category="slayer_only",
                         suggestion=_WITH_EXTENSIONS, dataset=dataset, metric=metric)

    def _lossy(self, message: str, *, dataset: str, metric: str | None = None) -> None:
        if not self.extensions:
            self._report(message, severity="info", category="lossy", suggestion=_LOSSY, dataset=dataset, metric=metric)

    def _missing_datasets(self, needed: set[str]) -> list[str]:
        return sorted(n for n in needed if n not in self.osi_datasets)

    def _needs_suggestion(self, missing: list[str]) -> str:
        outside = [m for m in missing if m in self.datasets and m not in self.selected]
        if outside:
            return f"Add {', '.join(repr(m) for m in outside)} to --datasets."
        return f"Make {', '.join(repr(m) for m in missing)} exportable (see its own report entry)."

    # ---- top level ----

    def run(self) -> OsiExportResult:
        for name, entity in self.fixed.items():
            self._fixed_dataset(name, entity)
        for name in self.selected:
            if name not in self.fixed:
                self._dataset(name)
        if not self.osi_datasets:
            reasons = "; ".join(e.message for e in self.entries) or "it has no datasets"
            raise OsiExportError(f"Nothing to export from datasource {self.config.name!r}: {reasons}")
        own = [n for n in self.osi_datasets if n not in self.fixed]
        for name in own:
            self._fields(name)
        for name in own:
            self._keys(name)
        for name in own:
            for relationship in self.datasets[name].relationships:
                self._relationship(name, relationship)
        metrics = [m for name in own for m in self._metrics(name)]
        self._name_entities(metrics)
        self.exported = {(m.home.casefold(), (m.metric.name or "").casefold()): (m.dataset, m.name)
                         for m in metrics if m.scope == "dataset" and m.name}
        for metric in metrics:
            self._place(metric)
        return OsiExportResult(document=self._document(), warnings=self.entries)

    # ---- datasets and sources ----

    def _fixed_dataset(self, name: str, entity: dict[str, Any]) -> None:
        """A stored document's dataset, exported verbatim; registered so standalone content can reference it."""
        dataset = self.datasets[name]
        self.osi_datasets[name] = _Dataset(
            dataset=dataset, graph=self.graph[name], source=entity["source"], structure=ext.SourceStructure(),
            osi_fields={f["name"]: f for f in entity.get("fields") or []}, fields={f.name: f for f in dataset.fields},
            primary_key=list(entity.get("primary_key") or []),
            unique_keys=[list(k) for k in entity.get("unique_keys") or []],
        )

    def _dataset(self, name: str) -> None:
        dataset = self.datasets[name]
        if dataset.access_tags and not self.extensions:
            self._report(f"Dataset {name!r} has access tags; re-importing it without them would make it public, "
                         f"so it is not exported.", severity="dropped", category="access",
                         suggestion="Export with SLAYER extensions so its access tags travel with it.", dataset=name)
            return
        try:
            source, structure = self._source(name, dataset)
        except _Refused as refused:
            self._report(f"Dataset {name!r} {refused.reason}; not exported.", severity="dropped", category="dataset",
                         suggestion=refused.suggestion, dataset=name)
            return
        self.osi_datasets[name] = _Dataset(dataset=dataset, graph=self.graph[name], source=source, structure=structure)

    def _source(self, name: str, dataset: SlayerDataset) -> tuple[str, ext.SourceStructure]:
        if dataset.source_queries:
            rendered = self.virtual.get(name)
            if not isinstance(rendered, SlayerDataset):
                raise _Refused(f"is query-backed and its query does not render ({rendered})",
                               "Fix the saved query so the dataset runs.")
            assert rendered.sql is not None
            return rendered.sql, ext.SourceStructure(
                source_queries=[q.model_dump(mode="json", exclude_none=True) for q in dataset.source_queries],
                query_variables=dict(dataset.query_variables),
            )
        structure = ext.SourceStructure(sql_table=dataset.sql_table, sql=dataset.sql, filters=list(dataset.filters) or None)
        base = dataset.sql_table or dataset.sql
        if base is None:
            raise _Refused("has no source", "Give the dataset a table or SQL.")
        if not dataset.filters:
            return base, structure
        conditions = [exp.Paren(this=self._own_filter(dataset, f)) for f in dataset.filters]
        relation = (exp.to_table(dataset.sql_table, dialect=self.target) if dataset.sql_table
                    else exp.Subquery(this=sqlglot.parse_one(base, read=self.target),
                                      alias=exp.TableAlias(this=exp.to_identifier("_src"))))
        query = exp.select("*").from_(relation).where(exp.and_(*conditions))
        return query.sql(dialect=self.target), structure

    def _own_filter(self, dataset: SlayerDataset, predicate: str) -> exp.Expr:
        crossed: set[tuple[str, ...]] = set()
        try:
            expanded = expand_derived_refs_sync(sql=predicate, dataset=dataset, alias_path=_SELF,
                                                datasets_by_name=self.graph, dialect=self.target,
                                                crossed_paths=crossed)
            tree = sqlglot.parse_one(expanded or predicate, read=self.target)
        except Exception as exc:  # noqa: BLE001 — any expansion failure refuses the dataset
            raise _Refused(f"has a row filter {predicate!r} that does not expand ({exc})",
                           "Fix the filter.") from exc
        if crossed or any(c.table and c.table != _SELF for c in tree.find_all(exp.Column)):
            raise _Refused(f"has the row filter {predicate!r}, which reads a joined dataset",
                           "Express the restriction in the dataset's SQL, or apply it as a query filter.")
        return _strip_self(tree)

    # ---- fields ----

    def _fields(self, name: str) -> None:
        ds = self.osi_datasets[name]
        stored = {c.name: c for c in ds.dataset.fields}
        for field in ds.graph.fields:
            if field.name.startswith(SOURCE_READER_PREFIX):
                continue
            ds.fields[field.name] = stored.get(field.name, field)
            try:
                tree, crossed = self._expand(ds.graph, field)
            except Exception as exc:  # noqa: BLE001 — an unexpandable field is SLayer-only
                self._carry_field(ds, field, reason=f"does not expand to its source ({exc})", needs=set())
                continue
            if crossed:
                needs = {t.name for p in crossed
                         if (t := terminal_dataset(root=ds.graph, path=p, datasets_by_name=self.graph)) is not None}
                self._carry_field(ds, field, reason="reads a joined dataset", needs=needs)
                continue
            ds.osi_fields[field.name] = self._field(ds, field, tree)

    def _carry_field(self, ds: _Dataset, field: DatasetField, *, reason: str, needs: set[str]) -> None:
        name = ds.dataset.name
        missing = self._missing_datasets(needs)
        if missing:
            self._report(f"Field {field.name!r} on {name!r} reads {missing}, which are not exported; "
                         f"not exported.", severity="dropped", category="subset",
                         suggestion=self._needs_suggestion(missing), dataset=name)
            return
        self._slayer_only(f"Field {field.name!r} on {name!r} {reason}", dataset=name)
        if self.extensions:
            ds.slayer_fields.append(field.model_copy(deep=True))

    def _expand(self, dataset: SlayerDataset, field: DatasetField) -> tuple[exp.Expr, set[tuple[str, ...]]]:
        crossed: set[tuple[str, ...]] = set()
        value, predicate = expand_field_definition_parts_sync(
            field=field, dataset=dataset, alias_path=_SELF, datasets_by_name=self.graph, dialect=self.target,
            crossed_paths=crossed,
        )
        tree = _strip_self(sqlglot.parse_one(value, read=self.target))
        if predicate:
            mask = _strip_self(sqlglot.parse_one(predicate, read=self.target))
            tree = exp.Case(ifs=[exp.If(this=mask, true=tree)])
        return tree, crossed

    def _expression(self, ds: _Dataset, field: DatasetField, tree: exp.Expr) -> dict[str, Any]:
        """Each stored form as itself: portable ``sql`` with field paths inlined while every part is portable, each
        dialect entry verbatim; the datasource's own entry is ``tree`` (the native expansion) unless verbatim matches,
        and the only form of a filtered field."""
        stored = ds.fields[field.name]
        native = tree.sql(dialect=self.target)
        if stored.filter is not None:
            return {"dialects": [{"dialect": self.token, "expression": native}]}
        own = dialect_entries(stored.expression, self.token)
        verbatim = own[0].expression if own else stored.sql
        if verbatim is not None:
            try:
                if _strip_self(sqlglot.parse_one(verbatim, read=self.target)) == tree:
                    native = verbatim
            except SqlglotError:
                pass
        portable = self._portable(ds.dataset, stored, seen=frozenset())
        others = [{"dialect": e.dialect, "expression": e.expression} for e in (
            stored.expression.dialects or [] if isinstance(stored.expression, ExpressionObject) else [])
            if token(e.dialect) not in (self.token, OSSIE_SQL)]
        portable = _valid_portable(portable)
        out: dict[str, Any] = {"sql": portable} if portable is not None else {}
        if portable is None or own or stored.sql is not None:
            others.append({"dialect": self.token, "expression": native})
        if others:
            out["dialects"] = others
        return out

    def _portable(self, dataset: SlayerDataset, field: DatasetField, *, seen: frozenset[str]) -> str | None:
        """``field``'s portable form with each path to a field of ``dataset`` replaced by that field's portable form;
        ``None`` when a part is not portable (a filter, a dialect-only or unconverted field, another dataset)."""
        if field.filter is not None or field.sql is not None or field.name in seen:
            return None
        text = portable_text(field.expression) if field.expression is not None else None
        if text is None:
            return None if field.expression is not None else exp.to_identifier(field.name).sql(PORTABLE_READER)
        try:
            tree = sqlglot.parse_one(text, read=PORTABLE_READER)
        except SqlglotError:
            return None
        if not isinstance(tree, Expression):
            return None
        root = root_scope_field_ids(parsed=tree)
        paths = [col for col in tree.find_all(exp.Column) if id(col) in root and col.table]
        for col in paths:
            inner = self._portable_path(dataset, col, seen=seen | {field.name})
            if inner is None:
                return None
            col.replace(exp.Paren(this=sqlglot.parse_one(inner, read=PORTABLE_READER)))
        return tree.sql(dialect=PORTABLE_READER)

    def _portable_path(self, dataset: SlayerDataset, col: exp.Column, *, seen: frozenset[str]) -> str | None:
        """The portable form of the field of ``dataset`` that path ``col`` names (each part exactly when quoted, else in
        any case), else ``None``."""
        table = col.args.get("table")
        if not isinstance(table, exp.Identifier) or col.args.get("db") or not isinstance(col.this, exp.Identifier):
            return None
        try:
            own = match_name(name=table.name, declared=[dataset.name], quoted=table.quoted) is not None
            ref = dataset.get_field(col.name, quoted=col.this.quoted) if own else None
        except AmbiguousReferenceError:
            return None
        return self._portable(dataset, ref, seen=seen) if ref is not None else None

    def _field(self, ds: _Dataset, field: DatasetField, tree: exp.Expr) -> dict[str, Any]:
        stored = ds.fields[field.name]
        meta = stored.meta or {}
        osi_field: dict[str, Any] = {
            "name": field.name,
            "expression": self._expression(ds, field, tree),
        }
        datatype = _datatype(field)
        if _time_role(stored) != (datatype in TEMPORAL_DATATYPES):
            osi_field["dimension"] = {"is_time": _time_role(stored)}
        if stored.label:
            osi_field["label"] = stored.label
        description = explicit_description(stored.description, meta.get("osi_ai_context"))
        if description:
            osi_field["description"] = description
        if datatype:
            osi_field["datatype"] = datatype
        if meta.get("osi_ai_context"):
            osi_field["ai_context"] = meta["osi_ai_context"]
        payload = ext.FieldPayload(entity=field.name, field=ext.FieldContent(
            expression=field.expression, filter=field.filter, datatype=stored.datatype, hidden=stored.hidden,
            format=stored.format, granularity=stored.granularity,
            allowed_aggregations=stored.allowed_aggregations, meta=_slayer_meta(stored.meta),
        ))
        self._extensions(osi_field, meta, payload=payload, dataset=ds.dataset.name,
                         ossie=expression_digest(OSIExpression.model_validate(osi_field["expression"])))
        return osi_field

    def _extensions(self, entity: dict[str, Any], meta: dict[str, Any], *, payload: Any, dataset: str,
                    ossie: Any) -> None:
        """Re-emit valid foreign extensions recorded in ``meta``, then this entity's ``SLAYER`` payload."""
        out = self._foreign(meta.get("osi_nonportable_extensions"), owner=repr(entity["name"]), dataset=dataset)
        if self.extensions:
            out.append(ext.encode(payload, ossie=ossie))
        if out:
            entity["nonportable_extensions"] = out

    def _foreign(self, entries: Any, *, owner: str, dataset: str | None) -> list[dict[str, Any]]:
        """The valid foreign entries of a recorded list; the malformed ones are reported."""
        out: list[dict[str, Any]] = []
        if entries is None:
            entries = []
        for raw in entries if isinstance(entries, list) else [entries]:
            if isinstance(raw, dict) and ext.is_slayer(raw):
                continue
            entry = (ext.checked(vendor_name=raw["vendor_name"], data=raw["data"])
                     if isinstance(raw, dict) and set(raw) == {"vendor_name", "data"} else None)
            if entry is None:
                self._report(f"Non-portable extension {raw!r} on {owner} is malformed (it needs exactly a "
                             f"non-empty vendor_name and a JSON object data); not exported.", severity="dropped",
                             category="extension", suggestion="Fix or remove it in the entity's metadata.",
                             dataset=dataset)
                continue
            out.append(entry)
        return out

    # ---- keys ----

    def _keys(self, name: str) -> None:
        ds = self.osi_datasets[name]
        fields = ds.graph.fields
        plain = {n for n in ds.osi_fields if ds.fields[n].filter is None}
        primary = [c.name for c in fields if c.primary_key]
        if primary and self._key_ok(name, primary, plain, kind="primary key"):
            ds.primary_key = primary
        raw = [[c.name] for c in fields if c.unique]
        raw += [list(k) for k in (ds.dataset.meta or {}).get("osi_unique_keys") or [] if isinstance(k, list)]
        seen: set[tuple[str, ...]] = set()
        for key in raw:
            if tuple(key) in seen or not key:
                continue
            seen.add(tuple(key))
            if self._key_ok(name, key, plain, kind="unique key"):
                ds.unique_keys.append(key)

    def _key_ok(self, name: str, key: list[str], plain: set[str], *, kind: str) -> bool:
        bad = [k for k in key if k not in plain]
        if bad:
            self._report(f"The {kind} {key} of {name!r} names {bad}, which are not plain, unfiltered fields; "
                         f"not exported.", severity="dropped", category="key",
                         suggestion="Key on plain, unfiltered fields of the dataset.", dataset=name)
        return not bad

    def _is_key(self, name: str, fields: list[str]) -> bool:
        """``fields`` hold a unique key of the dataset: one routing proves, or one the document exports."""
        ds = self.osi_datasets[name]
        return is_key_set_unique(key_fields=fields, unique_key_sets=[*unique_key_sets(ds.graph), *ds.unique_keys])

    # ---- relationships ----

    def _relationship(self, name: str, relationship: DatasetRelationship) -> None:
        target = relationship.to
        label = f"Relationship {relationship.name or target!r} from {name!r} to {target!r}"
        if target not in self.osi_datasets:
            missing = [target]
            self._report(f"{label}: {target!r} is not exported; not exported.", severity="dropped",
                         category="subset", suggestion=self._needs_suggestion(missing), dataset=name)
            return
        if relationship.cardinality is RelationshipCardinality.MANY_TO_MANY:
            self._report(f"{label} is many-to-many, which Ossie relationships cannot express; not exported.",
                         severity="dropped", category="relationship", dataset=name,
                         suggestion="Model the bridge as its own dataset with many-to-one relationships to both sides.")
            return
        sources = [p[0] for p in relationship.column_pairs]
        targets = [p[1] for p in relationship.column_pairs]
        bad = self._bad_keys(name, target, sources) + self._bad_keys(target, name, targets)
        if bad:
            self._report(f"{label} keys on {bad}; not exported.", severity="dropped", category="relationship",
                         suggestion="Key the relationship on unfiltered base fields exported as fields.", dataset=name)
            return
        forward = (name, target, sources, targets)
        shape = self._relationship_shape(relationship, forward=forward)
        if shape is None:
            self._report(f"{label} has no cardinality and its keys are a primary or unique key of neither "
                         f"side; not exported.", severity="dropped", category="relationship", dataset=name,
                         suggestion="Set the relationship's cardinality.")
            return
        self.relationships.append(_Relationship(
            from_dataset=shape[0], to=shape[1], from_columns=shape[2], to_columns=shape[3],
            declaration=ext.RelationshipDeclaration(dataset=name, relationship=relationship)))
        plain = (shape == forward and relationship.cardinality is RelationshipCardinality.MANY_TO_ONE
                 and relationship.join_type.value == "left" and relationship.name is None and not relationship.trusted)
        if not plain:
            self._lossy(f"{label} ({relationship.cardinality.value if relationship.cardinality else 'no cardinality'}, "
                        f"{relationship.join_type.value}{', named' if relationship.name else ''}"
                        f"{', trusted' if relationship.trusted else ''}) imports back as a plain untrusted "
                        f"many-to-one left join from {shape[0]!r}.", dataset=name)

    def _relationship_shape(self, relationship: DatasetRelationship, *, forward: _Shape) -> _Shape | None:
        """``forward`` or its reverse, whichever points from the many side; ``None`` when undeterminable."""
        name, target, sources, targets = forward
        many = ossie_from(declaring=name, to=target, cardinality=relationship.cardinality, declaring_keys=sources,
                          to_keys=targets, keyed=lambda ds, keys: self._is_key(ds, list(keys)),
                          document_from=relationship.document_from)
        if many is None:
            return None
        return forward if many == name else (target, name, targets, sources)

    def _bad_keys(self, side: str, other: str, keys: list[str]) -> list[str]:
        ds = self.osi_datasets[side]
        return [f"{side}.{k}" for k in keys
                if k not in ds.osi_fields
                or relationship_key_error(dataset=side, target=other, key=k, side=side, fields=ds.graph.fields) is not None]

    # ---- metrics ----

    def _metrics(self, name: str) -> list[_Metric]:
        ds = self.osi_datasets[name]
        out: list[_Metric] = []
        for slayer_metric in ds.graph.metrics:
            label = f"Metric {slayer_metric.name or slayer_metric.expression!r} on {name!r}"
            try:
                key = bind_dataset_expression(slayer_metric.text, dataset=ds.graph, datasets=list(self.graph.values()),
                                              dialect=self.target, granularities=self.granularities)
            except Exception as exc:  # noqa: BLE001 — an unbindable metric travels verbatim
                self._carry_metric(ds, slayer_metric, label=f"{label} does not bind ({exc})", needs=set())
                continue
            needs = self._read_datasets(ds.graph, key)
            try:
                metric = self._classify(ds, slayer_metric, key)
            except _Refused as refused:
                self._carry_metric(ds, slayer_metric, label=f"{label} {refused.reason}", needs=needs)
                continue
            missing = self._missing_datasets(needs)
            if missing:
                self._report(f"{label} reads {missing}, which are not exported; not exported.",
                             severity="dropped", category="subset", suggestion=self._needs_suggestion(missing),
                             dataset=name, metric=slayer_metric.name)
                continue
            out.append(metric)
        return out

    def _carry_metric(self, ds: _Dataset, metric: DatasetMetric, *, label: str, needs: set[str]) -> None:
        name = ds.dataset.name
        missing = self._missing_datasets(needs)
        if missing:
            self._report(f"{label}; it reads {missing}, which are not exported; not exported.",
                         severity="dropped", category="subset", suggestion=self._needs_suggestion(missing),
                         dataset=name, metric=metric.name)
            return
        self._slayer_only(label, dataset=name, metric=metric.name)
        if self.extensions:
            ds.slayer_metrics.append(metric.model_copy(deep=True))

    def _read_datasets(self, home: SlayerDataset, key: ValueKey) -> set[str]:
        out: set[str] = set()
        for leaf in walk_value_keys(key):
            if isinstance(leaf, (FieldKey, FieldSqlKey, StarKey)):
                owner = terminal_dataset(root=home, path=leaf.path, datasets_by_name=self.graph)
                if owner is not None:
                    out.add(owner.name)
        return out

    def _classify(self, ds: _Dataset, metric: DatasetMetric, key: ValueKey) -> _Metric:
        if any(isinstance(k, TransformKey) for k in walk_value_keys(key)):
            raise _Refused("is a transform, which Ossie metrics exclude", _RESTORE)
        aggregates = list(_outermost_aggregates(key))
        if not aggregates:
            raise _Refused("has no aggregate", _RESTORE)
        readers: list[str] = []
        for agg in aggregates:
            readers.append(self._aggregate_reader(ds, agg))
        portable = not any(a.agg == "count_distinct_approx" for a in aggregates) and not any(
            isinstance(k, TimeTruncKey) and isinstance(k.granularity, CustomGranularity) for k in walk_value_keys(key))
        scoped = len(set(readers)) == 1 and readers[0] not in self.fixed
        try:
            tree = self._render(ds.graph, _instant_comparisons(key), scoped=scoped, portable=portable)
        except Exception as exc:  # noqa: BLE001 — anything the shared renderer refuses stays SLayer-only
            raise _Refused(f"does not render as standard SQL ({exc})", _RESTORE) from exc
        if not portable:
            self._check_dialect_entry(self.graph[readers[0]] if scoped else ds.graph, tree)
        return _Metric(home=ds.dataset.name, metric=metric, scope="dataset" if scoped else "model",
                       dataset=readers[0], tree=tree, portable=portable)

    def _check_dialect_entry(self, receiver: SlayerDataset, tree: exp.Expr) -> None:
        """Refuse a dialect entry SLayer cannot read back on ``receiver``, or that calls SLayer's own registered
        functions."""
        registered = sorted(_calls(tree) & self.dialect.slayer_registered_functions)
        if registered:
            raise _Refused(f"renders on {self.token} only through SLayer's own functions {registered}", _RESTORE)
        try:
            bind_dataset_expression(tree.sql(dialect=self.target), dataset=receiver, datasets=list(self.graph.values()),
                                    dialect=self.target, granularities=self.granularities)
        except Exception as exc:  # noqa: BLE001 — any failure means load could not read the entry
            raise _Refused(f"renders on {self.token} only as SQL SLayer cannot read back "
                           f"({str(exc).splitlines()[0]})", _RESTORE) from exc

    def _aggregate_reader(self, ds: _Dataset, agg: AggregateKey) -> str:
        """The one dataset ``agg`` reads, or refused."""
        _refuse_slayer_only(agg)
        leaves = list(_row_leaves(agg.source)) + [leaf for _, v in agg.kwargs for leaf in _row_leaves(v)]
        paths = {leaf.path for leaf in leaves} or {()}
        if len(paths) != 1:
            raise _Refused("has an aggregate reading several datasets", _RESTORE)
        (path,) = paths
        owner = terminal_dataset(root=ds.graph, path=path, datasets_by_name=self.graph)
        if owner is None or not self._unique_route(ds.graph, path):
            raise _Refused("reads a dataset it reaches by more than one relationship route", _RESTORE)
        fields = self.osi_datasets[owner.name].osi_fields if owner.name in self.osi_datasets else {}
        for leaf in leaves:
            leaf_name = leaf.leaf if isinstance(leaf, FieldKey) else getattr(leaf, "field_name", None)
            if leaf_name is not None and leaf_name not in fields and owner.name in self.osi_datasets:
                raise _Refused(f"reads the SLayer-only field {leaf_name!r}", _RESTORE)
        return owner.name

    def _unique_route(self, home: SlayerDataset, path: tuple[str, ...]) -> bool:
        try:
            chain = walk(root=home, path=path, datasets_by_name=self.graph)
        except SlayerError:
            return False
        if chain is None:
            return False
        for hop in chain:
            src, dst = self.graph[hop.source_dataset], self.graph[hop.to]
            edges = [j for j in src.relationships if j.to == dst.name]
            edges += [j for j in dst.relationships if j.to == src.name]
            if len(edges) != 1:
                return False
        return True

    # ---- metric rendering ----

    def _ident(self, name: str, names: list[str]) -> exp.Identifier:
        ambiguous = sum(n.lower() == name.lower() for n in names) > 1
        ident = exp.to_identifier(name)
        if ambiguous:
            ident.set("quoted", True)
        return ident

    def _ref(self, home: SlayerDataset, leaf: FieldKey | FieldSqlKey, *, scoped: bool) -> exp.Column:
        owner = terminal_dataset(root=home, path=leaf.path, datasets_by_name=self.graph)
        assert owner is not None
        name = leaf.leaf if isinstance(leaf, FieldKey) else leaf.field_name
        column = exp.Column(this=self._ident(name, list(self.osi_datasets[owner.name].osi_fields)))
        if not scoped:
            column.set("table", self._ident(owner.name, list(self.osi_datasets)))
        return column

    def _render(self, home: SlayerDataset, key: ValueKey, *, scoped: bool, portable: bool) -> exp.Expr:
        dialect = self.neutral if portable else self.dialect
        field_type = dataset_field_type(dataset=home, datasets_by_name=self.graph)

        def row(value: ValueKey) -> Expression:
            leaves = [k for k in walk_value_keys(value) if isinstance(k, (FieldKey, FieldSqlKey))]
            slots = {leaf: f"s{i}" for i, leaf in enumerate(leaves)}
            aliases = AliasFacilities(slotted_kinds=(FieldKey, FieldSqlKey), slot_id_by_key=slots,
                                      value_by_slot_id={s: self._ref(home, k, scoped=scoped) for k, s in slots.items()})
            return _ossie_division(render_value_key(key=value, ctx=RenderContext(
                dialect=dialect, aliases=aliases, field_type=field_type)))

        def param(value: Any) -> Expression:
            if isinstance(value, (FieldKey, FieldSqlKey)):
                return row(value)
            return render_value_key(key=LiteralKey(value=value), ctx=RenderContext(dialect=dialect))

        def aggregate(agg: AggregateKey) -> Expression:
            owner = aggregation_owner(root=home, source=agg.source, datasets_by_name=self.graph)
            definition = aggregation_definition(owner=owner, agg=agg.agg)
            rendered = render_aggregate(AggregateSpec(
                aggregation=agg.agg,
                value=None if isinstance(agg.source, StarKey) else row(agg.source),
                bindings={name: param(value) for name, value in agg.kwargs},
                input_type=aggregation_source_type(agg.source, field_type=field_type),
                expression=rendered_expression(agg=agg.agg, definition=definition),
            ), dialect=dialect)
            rendered.meta[_AGGREGATE_MARK] = True
            return rendered

        return _ossie_division(render_value_key(key=key, ctx=RenderContext(
            dialect=dialect, composites=CompositeFacilities(agg_builder=aggregate), field_type=field_type)))

    # ---- naming and placement ----

    def _name_entities(self, metrics: list[_Metric]) -> None:
        scopes = self._field_scopes()
        dataset_scoped = [m for m in metrics if m.scope == "dataset"]
        for metric in (m for m in dataset_scoped if m.dataset == m.home):
            metric.name = scopes[metric.home].allocate(metric.metric.name or "measure")
        for metric in (m for m in dataset_scoped if m.dataset != m.home):
            metric.name = scopes[metric.dataset].allocate(f"{metric.home}_{metric.metric.name or 'measure'}")
        self._name_document_entities([m for m in metrics if m.scope == "model"])

    def _field_scopes(self) -> dict[str, _Names]:
        scopes: dict[str, _Names] = {}
        for name, ds in self.osi_datasets.items():
            scopes[name] = _Names()
            for field in ds.osi_fields:
                scopes[name].allocate(field)
        return scopes

    def _name_document_entities(self, dataset_metrics: list[_Metric]) -> None:
        """Explicitly named relationships first, then model metrics, then the remaining relationships."""
        document = _Names()
        for doc in self.documents:
            for taken in [*(doc.get("relationships") or []), *(doc.get("metrics") or [])]:
                document.allocate(taken["name"])
        for rel in self.relationships:
            explicit = rel.declaration.relationship.name
            if explicit:
                rel.name = document.allocate(explicit)
        for metric in dataset_metrics:
            metric.name = document.allocate(metric.metric.name or "measure")
        for rel in self.relationships:
            if not rel.name:
                rel.name = document.allocate(f"{rel.from_dataset}_{rel.to}")

    def _referenced(self, home: SlayerDataset, ref: Ref | DottedRef) -> tuple[str, str] | None:
        """The exported dataset-scoped metric ``ref`` (a metric reference of ``home``) names: ``(dataset, name)``."""
        parts: tuple[str, ...] = (ref.name,) if isinstance(ref, Ref) else ref.parts
        path, leaf = parts[:-1], parts[-1]
        owner = terminal_dataset(root=home, path=path, datasets_by_name=self.graph) if path else home
        target = owner.get_metric(leaf) if owner is not None else None
        if target is None or target.name is None:
            return None
        return self.exported.get((owner.name.casefold(), target.name.casefold()))  # type: ignore[union-attr]

    def _with_agg(self, metric: _Metric) -> exp.Expr | None:
        """A model-scoped metric with each reference to an exported dataset-scoped metric written ``AGG(d.m)``
        (Ossie's only way to refer to one); ``None`` when it has none or does not re-render."""
        home = self.graph[metric.home]
        text = metric.metric.text
        try:
            parsed, spans = reference_spans(text)
        except ValueError:
            return None
        found = [(spans[id(r)], hit) for r in walk_parsed_refs(parsed)
                 if isinstance(r, (Ref, DottedRef)) and id(r) in spans and (hit := self._referenced(home, r))]
        if not found or str(_SENTINEL) in text:
            return None
        aggs: dict[str, tuple[str, str]] = {}
        for k, ((start, end), hit) in enumerate(sorted(found, key=lambda f: f[0][0], reverse=True)):
            aggs[str(_SENTINEL + k)] = hit
            text = f"{text[:start]}({_SENTINEL + k}){text[end:]}"
        try:
            key = bind_dataset_expression(text, dataset=home, datasets=list(self.graph.values()), dialect=self.target,
                                          granularities=self.granularities)
            tree = self._render(home, _instant_comparisons(key), scoped=False, portable=metric.portable)
        except Exception:  # noqa: BLE001 — the expanded form stands
            return None
        for literal in tree.find_all(exp.Literal):
            if (hit := aggs.get(literal.this)) is not None:
                dataset, name = hit
                literal.replace(exp.Anonymous(this="AGG", expressions=[exp.Column(
                    this=self._ident(name, list(self.osi_datasets[dataset].osi_fields)),
                    table=self._ident(dataset, list(self.osi_datasets)))]))
        return tree

    def _place(self, metric: _Metric) -> None:
        slayer_metric = metric.metric
        meta = slayer_metric.meta or {}
        tree = (self._with_agg(metric) if metric.scope == "model" else None) or metric.tree
        portable = _valid_portable(tree.sql(dialect=PORTABLE_READER)) if metric.portable else None
        expression: dict[str, Any] = {"sql": portable} if portable is not None else {
            "dialects": [{"dialect": self.token, "expression": tree.sql(dialect=self.target)}]}
        entity: dict[str, Any] = {"name": metric.name, "expression": expression}
        description = explicit_description(slayer_metric.description, meta.get("osi_ai_context"))
        if description:
            entity["description"] = description
        if slayer_metric.datatype is not None and slayer_metric.datatype.value in OSSIE_DATATYPES:
            entity["datatype"] = slayer_metric.datatype.value
        if meta.get("osi_ai_context"):
            entity["ai_context"] = meta["osi_ai_context"]
        payload = ext.MetricPayload(entity=metric.name, dataset=metric.home, metric=slayer_metric.model_copy(
            update={"meta": _slayer_meta(slayer_metric.meta)}))
        self._extensions(entity, meta, payload=payload, dataset=metric.home,
                         ossie=expression_digest(OSIExpression.model_validate(expression)))
        if metric.scope == "dataset":
            self.osi_datasets[metric.dataset].metrics.append(entity)
            relocated = metric.dataset != metric.home
        else:
            self.model_metrics.append(entity)
            relocated = metric.home not in self._read_osi_datasets(metric)
        if relocated:
            self._lossy(f"Metric {slayer_metric.name!r} on {metric.home!r} is exported as metric {metric.name!r} "
                        f"{'on ' + repr(metric.dataset) if metric.scope == 'dataset' else 'of the model'} and "
                        f"imports back there.", dataset=metric.home, metric=slayer_metric.name)

    def _read_osi_datasets(self, metric: _Metric) -> set[str]:
        return {c.table for c in metric.tree.find_all(exp.Column) if c.table}

    # ---- document ----

    def _shared_semantic_model(self) -> dict[str, Any] | None:
        shared = [(ds.dataset.meta or {}).get("osi_semantic_model") for ds in self.osi_datasets.values()]
        first = shared[0]
        if isinstance(first, dict) and first.get("name") and all(s == first for s in shared):
            return first
        return None

    def _document_header(self, semantic: dict[str, Any] | None) -> dict[str, Any]:
        """Version, name, description and AI context: the shared semantic model's, else the datasource's."""
        document: dict[str, Any] = {"version": OSSIE_VERSION}
        if semantic is None:
            document["name"] = self.config.name
            if self.config.description:
                document["description"] = self.config.description
            return document
        document["name"] = semantic["name"]
        if semantic.get("description"):
            document["description"] = semantic["description"]
        if semantic.get("ai_context"):
            document["ai_context"] = semantic["ai_context"]
        return document

    def _document(self) -> dict[str, Any]:
        semantic = self._shared_semantic_model()
        document = self._document_header(semantic)
        document["datasets"] = [self._verbatim(self.fixed[name]) if name in self.fixed else self._dataset_entity(ds)
                                for name, ds in sorted(self.osi_datasets.items())]
        relationships = sorted(self.relationships,
                               key=lambda r: (r.from_dataset, r.to, r.from_columns, r.to_columns))
        merged_rels = [self._verbatim(r) for doc in self.documents for r in doc.get("relationships") or []]
        merged_rels += [self._relationship_entity(r) for r in relationships]
        if merged_rels:
            document["relationships"] = merged_rels
        merged_metrics = [self._verbatim(m) for doc in self.documents for m in doc.get("metrics") or []]
        merged_metrics += self.model_metrics
        if merged_metrics:
            document["metrics"] = merged_metrics
        foreign = self._foreign((semantic or {}).get("nonportable_extensions"), owner="the document", dataset=None)
        if self.documents and self.extensions:
            foreign.append(ext.encode(ext.DocumentPayload(entity=document["name"], documents=[
                self._boundary(doc) for doc in self.documents]), ossie=document["name"]))
        if foreign:
            document["nonportable_extensions"] = foreign
        return document

    def _verbatim(self, entity: dict[str, Any]) -> dict[str, Any]:
        """A stored document's entity as written; without ``SLAYER`` entries when extensions are off."""
        return entity if self.extensions else without_vendor(entity, ext.VENDOR)

    @staticmethod
    def _boundary(doc: dict[str, Any]) -> ext.DocumentBoundary:
        return ext.DocumentBoundary(
            name=doc["name"], description=doc.get("description"), ai_context=doc.get("ai_context"),
            datasets=[d["name"] for d in doc["datasets"]],
            relationships=[r["name"] for r in doc.get("relationships") or []],
            metrics=[m["name"] for m in doc.get("metrics") or []],
            nonportable_extensions=list(doc.get("nonportable_extensions") or []),
        )

    def _dataset_entity(self, ds: _Dataset) -> dict[str, Any]:
        dataset = ds.dataset
        meta = dataset.meta or {}
        entity: dict[str, Any] = {"name": dataset.name, "source": ds.source}
        if ds.primary_key:
            entity["primary_key"] = ds.primary_key
        if ds.unique_keys:
            entity["unique_keys"] = ds.unique_keys
        description = explicit_description(dataset.description, meta.get("osi_ai_context"))
        if description:
            entity["description"] = description
        if meta.get("osi_ai_context"):
            entity["ai_context"] = meta["osi_ai_context"]
        if ds.osi_fields:
            entity["fields"] = list(ds.osi_fields.values())
        if ds.metrics:
            entity["metrics"] = ds.metrics
        payload = ext.DatasetPayload(
            entity=dataset.name, source=ds.structure, hidden=dataset.hidden, access_tags=list(dataset.access_tags),
            default_time_dimension=dataset.default_time_dimension, meta=_slayer_meta(dataset.meta),
            aggregations=list(dataset.aggregations),
            fields=[DatasetField.model_validate(c.model_dump(exclude=_EXCLUDED_FIELD_FIELDS))
                     for c in ds.slayer_fields],
            metrics=ds.slayer_metrics,
            enforced_keys=[list(k) for k in dataset.enforced_keys],
        )
        self._extensions(entity, meta, payload=payload, dataset=dataset.name,
                         ossie=source_digest(ds.source, reader=self.target))
        return entity

    def _relationship_entity(self, rel: _Relationship) -> dict[str, Any]:
        entity: dict[str, Any] = {"name": rel.name, "from": rel.from_dataset, "to": rel.to,
                                  "from_columns": rel.from_columns, "to_columns": rel.to_columns}
        meta = rel.declaration.relationship.meta or {}
        if meta.get("osi_ai_context"):
            entity["ai_context"] = meta["osi_ai_context"]
        declared = rel.declaration.relationship
        declaration = rel.declaration.model_copy(
            update={"relationship": declared.model_copy(update={"meta": _slayer_meta(declared.meta)})})
        self._extensions(entity, meta, payload=ext.RelationshipPayload(entity=rel.name, declarations=[declaration]),
                         dataset=rel.from_dataset, ossie=ext.relationship_digest(
                             from_dataset=rel.from_dataset, to=rel.to, from_columns=rel.from_columns,
                             to_columns=rel.to_columns))
        return entity


async def _stored_datasets(*, storage: Any, datasource: str) -> dict[str, SlayerDataset]:
    stored: dict[str, SlayerDataset] = {}
    for name in sorted(await storage.list_datasets(data_source=datasource)):
        dataset = await storage.get_dataset(name, data_source=datasource)
        if dataset is not None:
            stored[name] = dataset
    return stored


async def _expanded_query_backed(
    *, engine: SlayerQueryEngine, stored: dict[str, SlayerDataset], selected: list[str],
) -> dict[str, SlayerDataset | Exception]:
    """Each selected query-backed dataset's virtual expansion, or the exception expanding it raised."""
    virtual: dict[str, SlayerDataset | Exception] = {}
    for name in selected:
        if stored[name].source_queries:
            try:
                virtual[name] = await engine.expand_query_backed_dataset(stored[name])
            except Exception as exc:  # noqa: BLE001 — reported on the dataset
                virtual[name] = exc
    return virtual


async def _stored_documents(*, storage: Any, datasource: str, loaded: set[str],
                            skipped: list[StoredDocumentLoadError]) -> list[tuple[str, str, Any]]:
    """The name, text and data of each stored semantic-model document named in ``loaded`` that still reads; one that
    no longer reads goes to ``skipped``."""
    documents: list[tuple[str, str, Any]] = []
    for _, name in await storage.list_semantic_models(datasource):
        if name not in loaded:
            continue
        try:
            text = await storage.get_semantic_model_text(name, data_source=datasource)
            if text is not None:
                documents.append((name, text, parse_document_text(text)))
        except (UnreadableDocumentError, OssieDocumentInvalidError) as exc:  # edited since it loaded
            skipped.append(StoredDocumentLoadError(kind="semantic_model", name=name, data_source=datasource, cause=exc))
    return documents


def _valid_portable(text: str | None) -> str | None:
    """``text`` in the canonical portable-SQL writer's spelling; ``None`` when it is still not valid portable SQL."""
    written = portable_sql(text) if text is not None else None
    return written if written is not None and portable_problem(written) is None else None


def _left_out(skipped: list[StoredDocumentLoadError]) -> list[ConversionWarning]:
    """A report entry, and an ``unloadable_document`` warning, for each document the export leaves out."""
    out: list[ConversionWarning] = []
    for error in skipped:
        payload = UnloadableDocumentWarning(document_kind="semantic_model", data_source=error.data_source, name=error.name,
                                            cause=error.cause_text)
        warnings.warn(SlayerUnloadableDocumentWarning(payload), stacklevel=outside_stacklevel())
        out.append(ConversionWarning(message=payload.human_message(), category="unloadable_document", severity="dropped"))
    return out


async def export_osi(*, engine: SlayerQueryEngine, datasource: str, datasets: list[str] | None = None,
                     extensions: bool = True) -> OsiExportResult:
    """Export ``datasource``'s datasets (or the ``datasets`` subset) as an Ossie document and its report."""
    storage = engine.storage
    config = await storage.get_datasource(datasource)
    if config is None:
        raise OsiExportError(f"Datasource {datasource!r} not found.")
    stored = await _stored_datasets(storage=storage, datasource=datasource)
    skipped = [e for e in (await storage.load_datasets(data_source=datasource))[1] if e.kind == "semantic_model"]
    documents = await _stored_documents(storage=storage, datasource=datasource, skipped=skipped,
                                        loaded={d.semantic_model for d in stored.values() if d.semantic_model})
    readable = {name for name, _, _ in documents}
    stored = {n: d for n, d in stored.items() if not d.semantic_model or d.semantic_model in readable}
    left_out = _left_out(skipped)
    if not stored and skipped:
        raise skipped[0]  # nothing else to export: the document's own load error
    unknown = [m for m in datasets or [] if m not in stored]
    if unknown:
        raise OsiExportError(f"Unknown dataset(s) in datasource {datasource!r}: {', '.join(unknown)}.")
    selected = sorted(set(datasets)) if datasets is not None else sorted(stored)
    virtual = await _expanded_query_backed(engine=engine, stored=stored, selected=selected)
    resolved = await engine.resolved_datasets(list(stored.values()), datasource=config)
    parsed = [data for _, _, data in documents]
    members = {d["name"] for doc in parsed for d in doc["datasets"]}
    if datasets is None and len(documents) == 1 and members == set(stored):
        text = documents[0][1] if extensions else edit_text(documents[0][1],
                                                            lambda data: without_vendor(data, ext.VENDOR))
        return OsiExportResult(document=parse_document_text(text), text=text, warnings=left_out)
    result = _Exporter(config=config, datasets=stored, selected=selected, virtual=virtual, extensions=extensions,
                       resolved=resolved, documents=parsed).run()
    result.warnings = [*left_out, *result.warnings]
    result.text = dump_yaml(result.document)
    return result
