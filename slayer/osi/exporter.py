"""Export one datasource's SLayer models as a current-spec Ossie document.

Everything Ossie defines with SLayer's meaning goes out as Ossie; SLayer-only
content travels in one ``SLAYER`` custom extension per entity; everything else
is reported (entity, reason, workaround), never silently dropped.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any, Literal

import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.errors import ErrorLevel, SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.errors import SlayerError
from slayer.core.enums import RANKED_AGGREGATIONS, DataType, JoinCardinality
from slayer.core.join_walker import aggregation_owner, model_column_type, terminal_model, walk
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    LiteralKey,
    SqlFragmentKey,
    StarKey,
    TransformKey,
    ValueKey,
    aggregation_source_type,
    walk_value_keys,
)
from slayer.core.models import (
    WINDOW_PARAM,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
    aggregation_definition,
    join_key_error,
    rendered_formula,
)
from slayer.engine.binding import bind_model_formula
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ingest_report import ConversionReport, ConversionWarning
from slayer.osi import slayer_extension as ext
from slayer.osi.dialects import PORTABLE_READER, ossie_token
from slayer.osi.models import OSSIE_DATATYPES, TEMPORAL_DATATYPES, OSIExpression, explicit_description
from slayer.sql.column_expansion import expand_column_definition_parts_sync, expand_derived_refs_sync
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.render.aggregates import AggregateSpec, render_aggregate
from slayer.sql.render.value_expr import AliasFacilities, CompositeFacilities, RenderContext, render_value_key

OSSIE_VERSION = "0.3.0.dev0"
_SELF = "__slayer_self"
_EXACT_NUMERIC = re.compile(r"^\s*(DECIMAL|NUMERIC|NUMBER|DEC|MONEY|SMALLMONEY|BIGNUMERIC|BIGDECIMAL)\b", re.I)
_WITH_TZ = re.compile(r"WITH\s+(LOCAL\s+)?TIME\s+ZONE|TIMESTAMPTZ|TIMESTAMP_TZ|TIMESTAMP_LTZ|DATETIMEOFFSET", re.I)
_WITHOUT_TZ = re.compile(r"WITHOUT\s+TIME\s+ZONE", re.I)
# Aggregations with no Ossie metric form: ranked picks and list collection.
_SLAYER_ONLY_AGGREGATIONS = frozenset(RANKED_AGGREGATIONS) | {"collect"}
_EXCLUDED_COLUMN_FIELDS = {"sampled", "sampled_values", "distinct_count"}

_RESTORE = "Re-import the document with `slayer import-osi` to restore it; other Ossie tools do not see it."
_WITH_EXTENSIONS = "Export with SLAYER extensions (drop --no-extensions) to carry it."
_LOSSY = "Export with SLAYER extensions (drop --no-extensions) for an exact SLayer round trip."


class OsiExportError(Exception):
    """An export request names an unknown datasource or model, or nothing is exportable."""


class OsiExportResult(ConversionReport):
    """The exported Ossie document and its report."""
    document: dict[str, Any] = Field(default_factory=dict)


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
    """One exported model: its graph model (a query-backed one's rendered form), source and fields."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    model: SlayerModel
    graph: SlayerModel
    source: str
    structure: ext.SourceStructure
    fields: dict[str, dict[str, Any]] = {}
    columns: dict[str, Column] = {}
    slayer_columns: list[Column] = []
    slayer_measures: list[ModelMeasure] = []
    primary_key: list[str] = []
    unique_keys: list[list[str]] = []
    metrics: list[dict[str, Any]] = []


class _Metric(BaseModel):
    """A measure classified as an Ossie metric."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    home: str
    measure: ModelMeasure
    scope: Literal["dataset", "model"]
    dataset: str
    tree: exp.Expr
    portable: bool
    name: str = ""


#: ``(from_dataset, to, from_columns, to_columns)``.
_Shape = tuple[str, str, list[str], list[str]]


class _Relationship(BaseModel):
    """One FK-direction relationship and the SLayer joins it stands for."""

    from_dataset: str
    to: str
    from_columns: list[str]
    to_columns: list[str]
    declarations: list[ext.JoinDeclaration] = []
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


def _datatype(column: Column) -> str | None:
    recorded = (column.meta or {}).get("osi_datatype")
    if isinstance(recorded, str) and OSSIE_DATATYPES.get(recorded) is column.type:
        return recorded
    db_type = column.db_type or ""
    if column.type is DataType.DOUBLE:
        return "Decimal" if _EXACT_NUMERIC.match(db_type) else "Float"
    if column.type is DataType.TIMESTAMP:
        return "DateTimeTz" if _WITH_TZ.search(db_type) and not _WITHOUT_TZ.search(db_type) else "DateTime"
    return {DataType.TEXT: "String", DataType.INT: "Integer", DataType.BOOLEAN: "Boolean",
            DataType.DATE: "Date"}.get(column.type)


def _time_role(column: Column) -> bool:
    recorded = (column.meta or {}).get("osi_is_time")
    if isinstance(recorded, bool):
        return recorded
    return column.type in (DataType.DATE, DataType.TIMESTAMP)


_AGGREGATE_MARK = "slayer_rendered_aggregate"


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


def _refuse_slayer_only(agg: AggregateKey) -> None:
    if agg.agg in _SLAYER_ONLY_AGGREGATIONS:
        raise _Refused(f"uses {agg.agg}, which has no Ossie metric form", _RESTORE)
    windowed = (agg.partition_keys is not None or agg.locus != "target" or bool(agg.args)
                or any(k == WINDOW_PARAM for k, _ in agg.kwargs))
    if windowed:
        raise _Refused("is a windowed or partitioned aggregate", _RESTORE)


def _outermost_aggregates(key: ValueKey) -> Iterator[AggregateKey]:
    if isinstance(key, AggregateKey):
        yield key
        return
    for child in key.children():
        yield from _outermost_aggregates(child)


def _row_leaves(value: Any) -> Iterator[ColumnKey | ColumnSqlKey | StarKey]:
    if isinstance(value, (ColumnKey, ColumnSqlKey, StarKey)):
        yield value
    elif isinstance(value, SqlFragmentKey):
        yield from value.refs
    elif isinstance(value, (AggregateKey, TransformKey)):
        raise _Refused("re-aggregates another aggregate", _RESTORE)
    elif hasattr(value, "children"):
        for child in value.children():
            yield from _row_leaves(child)


class _Exporter:
    def __init__(self, *, config: DatasourceConfig, models: dict[str, SlayerModel], selected: list[str],
                 virtual: dict[str, SlayerModel | Exception], extensions: bool) -> None:
        self.config = config
        self.models = models
        self.selected = selected
        self.extensions = extensions
        self.dialect: SqlDialect = dialect_for_ds_type(config.type)
        self.target = self.dialect.sqlglot_name
        self.token = ossie_token(config.type)
        self.neutral = SqlDialect()
        self.virtual = virtual
        self.graph: dict[str, SlayerModel] = dict(models)
        for name, rendered in virtual.items():
            if isinstance(rendered, SlayerModel):
                stored = models[name]
                self.graph[name] = rendered.model_copy(update={
                    "measures": stored.measures, "joins": stored.joins, "aggregations": stored.aggregations,
                })
        self.datasets: dict[str, _Dataset] = {}
        self.relationships: list[_Relationship] = []
        self.model_metrics: list[dict[str, Any]] = []
        self.entries: list[ConversionWarning] = []

    # ---- report ----

    def _report(self, message: str, *, severity: Literal["dropped", "info"], category: str, suggestion: str,
                model: str | None = None, metric: str | None = None) -> None:
        self.entries.append(ConversionWarning(model_name=model, metric_name=metric, message=message,
                                              category=category, severity=severity, suggestion=suggestion))

    def _slayer_only(self, message: str, *, model: str, metric: str | None = None) -> None:
        if self.extensions:
            self._report(f"{message}; it travels only in the SLAYER extension.", severity="info",
                         category="extension_only", suggestion=_RESTORE, model=model, metric=metric)
        else:
            self._report(f"{message}; not exported.", severity="dropped", category="slayer_only",
                         suggestion=_WITH_EXTENSIONS, model=model, metric=metric)

    def _lossy(self, message: str, *, model: str, metric: str | None = None) -> None:
        if not self.extensions:
            self._report(message, severity="info", category="lossy", suggestion=_LOSSY, model=model, metric=metric)

    def _missing_models(self, needed: set[str]) -> list[str]:
        return sorted(n for n in needed if n not in self.datasets)

    def _needs_suggestion(self, missing: list[str]) -> str:
        outside = [m for m in missing if m in self.models and m not in self.selected]
        if outside:
            return f"Add {', '.join(repr(m) for m in outside)} to --models."
        return f"Make {', '.join(repr(m) for m in missing)} exportable (see its own report entry)."

    # ---- top level ----

    def run(self) -> OsiExportResult:
        for name in self.selected:
            self._dataset(name)
        if not self.datasets:
            reasons = "; ".join(e.message for e in self.entries) or "it has no models"
            raise OsiExportError(f"Nothing to export from datasource {self.config.name!r}: {reasons}")
        for name in self.datasets:
            self._columns(name)
        for name in self.datasets:
            self._keys(name)
        for name in self.datasets:
            for join in self.models[name].joins:
                self._relationship(name, join)
        metrics = [m for name in self.datasets for m in self._measures(name)]
        self._name_entities(metrics)
        for metric in metrics:
            self._place(metric)
        return OsiExportResult(document=self._document(), warnings=self.entries)

    # ---- datasets and sources ----

    def _dataset(self, name: str) -> None:
        model = self.models[name]
        if model.access_tags and not self.extensions:
            self._report(f"Model {name!r} has access tags; re-importing it without them would make it public, "
                         f"so it is not exported.", severity="dropped", category="access",
                         suggestion="Export with SLAYER extensions so its access tags travel with it.", model=name)
            return
        try:
            source, structure = self._source(name, model)
        except _Refused as refused:
            self._report(f"Model {name!r} {refused.reason}; not exported.", severity="dropped", category="dataset",
                         suggestion=refused.suggestion, model=name)
            return
        self.datasets[name] = _Dataset(model=model, graph=self.graph[name], source=source, structure=structure)

    def _source(self, name: str, model: SlayerModel) -> tuple[str, ext.SourceStructure]:
        if model.source_queries:
            rendered = self.virtual.get(name)
            if not isinstance(rendered, SlayerModel):
                raise _Refused(f"is query-backed and its query does not render ({rendered})",
                               "Fix the saved query so the model runs.")
            assert rendered.sql is not None
            return rendered.sql, ext.SourceStructure(
                source_queries=[q.model_dump(mode="json", exclude_none=True) for q in model.source_queries],
                query_variables=dict(model.query_variables),
            )
        structure = ext.SourceStructure(sql_table=model.sql_table, sql=model.sql, filters=list(model.filters))
        base = model.sql_table or model.sql
        if base is None:
            raise _Refused("has no source", "Give the model a table or SQL.")
        if not model.filters:
            return base, structure
        conditions = [exp.Paren(this=self._own_filter(model, f)) for f in model.filters]
        relation = (exp.to_table(model.sql_table, dialect=self.target) if model.sql_table
                    else exp.Subquery(this=sqlglot.parse_one(base, read=self.target),
                                      alias=exp.TableAlias(this=exp.to_identifier("_src"))))
        query = exp.select("*").from_(relation).where(exp.and_(*conditions))
        return query.sql(dialect=self.target), structure

    def _own_filter(self, model: SlayerModel, predicate: str) -> exp.Expr:
        crossed: set[tuple[str, ...]] = set()
        try:
            expanded = expand_derived_refs_sync(sql=predicate, model=model, alias_path=_SELF,
                                                models_by_name=self.graph, dialect=self.target,
                                                crossed_paths=crossed)
            tree = sqlglot.parse_one(expanded or predicate, read=self.target)
        except Exception as exc:  # noqa: BLE001 — any expansion failure refuses the model
            raise _Refused(f"has a row filter {predicate!r} that does not expand ({exc})",
                           "Fix the filter.") from exc
        if crossed or any(c.table and c.table != _SELF for c in tree.find_all(exp.Column)):
            raise _Refused(f"has the row filter {predicate!r}, which reads a joined model",
                           "Express the restriction in the model's SQL, or apply it as a query filter.")
        return _strip_self(tree)

    # ---- fields ----

    def _columns(self, name: str) -> None:
        ds = self.datasets[name]
        stored = {c.name: c for c in ds.model.columns}
        for column in ds.graph.columns:
            ds.columns[column.name] = stored.get(column.name, column)
            try:
                tree, crossed = self._expand(ds.graph, column)
            except Exception as exc:  # noqa: BLE001 — an unexpandable column is SLayer-only
                self._carry_column(ds, column, reason=f"does not expand to its source ({exc})", needs=set())
                continue
            if crossed:
                needs = {t.name for p in crossed
                         if (t := terminal_model(root=ds.graph, path=p, models_by_name=self.graph)) is not None}
                self._carry_column(ds, column, reason="reads a joined model", needs=needs)
                continue
            ds.fields[column.name] = self._field(ds, column, tree)

    def _carry_column(self, ds: _Dataset, column: Column, *, reason: str, needs: set[str]) -> None:
        name = ds.model.name
        missing = self._missing_models(needs)
        if missing:
            self._report(f"Column {column.name!r} on {name!r} reads {missing}, which are not exported; "
                         f"not exported.", severity="dropped", category="subset",
                         suggestion=self._needs_suggestion(missing), model=name)
            return
        self._slayer_only(f"Column {column.name!r} on {name!r} {reason}", model=name)
        if self.extensions:
            ds.slayer_columns.append(column.model_copy(deep=True))

    def _expand(self, model: SlayerModel, column: Column) -> tuple[exp.Expr, set[tuple[str, ...]]]:
        crossed: set[tuple[str, ...]] = set()
        value, predicate = expand_column_definition_parts_sync(
            column=column, model=model, alias_path=_SELF, models_by_name=self.graph, dialect=self.target,
            crossed_paths=crossed,
        )
        tree = _strip_self(sqlglot.parse_one(value, read=self.target))
        if predicate:
            mask = _strip_self(sqlglot.parse_one(predicate, read=self.target))
            tree = exp.Case(ifs=[exp.If(this=mask, true=tree)])
        return tree, crossed

    def _expression(self, tree: exp.Expr, *, verbatim: str | None) -> dict[str, Any]:
        """Portable ``sql`` when it reads back exactly in the datasource dialect, else one native entry."""
        native = tree.sql(dialect=self.target)
        if verbatim is not None:
            try:
                if sqlglot.parse_one(verbatim, read=self.target) == tree:
                    native = verbatim
            except SqlglotError:
                pass
        try:
            portable = tree.sql(dialect=PORTABLE_READER, unsupported_level=ErrorLevel.RAISE)
            back = sqlglot.parse_one(portable, read=PORTABLE_READER).sql(
                dialect=self.target, unsupported_level=ErrorLevel.RAISE)
            exact = back == sqlglot.parse_one(native, read=self.target).sql(
                dialect=self.target, unsupported_level=ErrorLevel.RAISE)
        except SqlglotError:
            exact = False
        if exact:
            return {"sql": portable}
        return {"dialects": [{"dialect": self.token, "expression": native}]}

    def _field(self, ds: _Dataset, column: Column, tree: exp.Expr) -> dict[str, Any]:
        stored = ds.columns[column.name]
        meta = stored.meta or {}
        field: dict[str, Any] = {
            "name": column.name,
            "expression": self._expression(tree, verbatim=column.sql if not column.filter else None),
        }
        datatype = _datatype(column)
        if _time_role(stored) != (datatype in TEMPORAL_DATATYPES):
            field["dimension"] = {"is_time": _time_role(stored)}
        if stored.label:
            field["label"] = stored.label
        description = explicit_description(stored.description, meta.get("osi_ai_context"))
        if description:
            field["description"] = description
        if datatype:
            field["datatype"] = datatype
        if meta.get("osi_ai_context"):
            field["ai_context"] = meta["osi_ai_context"]
        payload = ext.FieldPayload(entity=column.name, column=ext.ColumnContent(
            sql=stored.sql, filter=stored.filter, type=stored.type, hidden=stored.hidden,
            format=stored.format, granularity=stored.granularity,
            allowed_aggregations=stored.allowed_aggregations, meta=_slayer_meta(stored.meta),
        ))
        self._extensions(field, meta, payload=payload, model=ds.model.name,
                         ossie=ext.expression_digest(OSIExpression.model_validate(field["expression"])))
        return field

    def _extensions(self, entity: dict[str, Any], meta: dict[str, Any], *, payload: Any, model: str,
                    ossie: Any) -> None:
        """Re-emit valid foreign extensions recorded in ``meta``, then this entity's ``SLAYER`` payload."""
        out: list[dict[str, Any]] = []
        for raw in meta.get("osi_custom_extensions") or []:
            if isinstance(raw, dict) and ext.is_slayer(raw):
                continue
            valid = (isinstance(raw, dict) and set(raw) == {"vendor_name", "data"}
                     and isinstance(raw["vendor_name"], str) and raw["vendor_name"].strip()
                     and isinstance(raw["data"], str))
            if not valid:
                self._report(f"Custom extension {raw!r} on {entity['name']!r} is malformed (it needs exactly a "
                             f"non-empty vendor_name and a string data); not exported.", severity="dropped",
                             category="extension", suggestion="Fix or remove it in the entity's metadata.",
                             model=model)
                continue
            out.append({"vendor_name": raw["vendor_name"], "data": raw["data"]})
        if self.extensions:
            out.append(ext.encode(payload, ossie=ossie).model_dump())
        if out:
            entity["custom_extensions"] = out

    # ---- keys ----

    def _keys(self, name: str) -> None:
        ds = self.datasets[name]
        columns = ds.graph.columns
        plain = {n for n in ds.fields if ds.columns[n].filter is None}
        primary = [c.name for c in columns if c.primary_key]
        if primary and self._key_ok(name, primary, plain, kind="primary key"):
            ds.primary_key = primary
        raw = [[c.name] for c in columns if c.unique]
        raw += [list(k) for k in (ds.model.meta or {}).get("osi_unique_keys") or [] if isinstance(k, list)]
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
                         suggestion="Key on plain, unfiltered columns of the model.", model=name)
        return not bad

    def _is_key(self, name: str, columns: list[str]) -> bool:
        ds = self.datasets[name]
        return any(set(columns) == set(k) for k in [ds.primary_key, *ds.unique_keys] if k)

    # ---- relationships ----

    def _relationship(self, name: str, join: ModelJoin) -> None:
        target = join.target_model
        label = f"Join {join.name or target!r} from {name!r} to {target!r}"
        if target not in self.datasets:
            missing = [target]
            self._report(f"{label}: {target!r} is not exported; not exported.", severity="dropped",
                         category="subset", suggestion=self._needs_suggestion(missing), model=name)
            return
        if join.cardinality is JoinCardinality.MANY_TO_MANY:
            self._report(f"{label} is many-to-many, which Ossie relationships cannot express; not exported.",
                         severity="dropped", category="relationship", model=name,
                         suggestion="Model the bridge as its own model with many-to-one joins to both sides.")
            return
        sources = [p[0] for p in join.join_pairs]
        targets = [p[1] for p in join.join_pairs]
        bad = self._bad_keys(name, target, sources) + self._bad_keys(target, name, targets)
        if bad:
            self._report(f"{label} keys on {bad}; not exported.", severity="dropped", category="relationship",
                         suggestion="Key the join on unfiltered base columns exported as fields.", model=name)
            return
        forward = (name, target, sources, targets)
        shape = self._relationship_shape(join, forward=forward)
        if shape is None:
            self._report(f"{label} has no cardinality and its keys are a primary or unique key of neither "
                         f"side; not exported.", severity="dropped", category="relationship", model=name,
                         suggestion="Set the join's cardinality.")
            return
        declaration = ext.JoinDeclaration(model=name, join=join)
        rel = next((r for r in self.relationships
                    if (r.from_dataset, r.to, r.from_columns, r.to_columns) == shape), None)
        if rel is None:
            rel = _Relationship(from_dataset=shape[0], to=shape[1], from_columns=shape[2], to_columns=shape[3])
            self.relationships.append(rel)
        rel.declarations.append(declaration)
        plain = (shape == forward and join.cardinality is JoinCardinality.MANY_TO_ONE
                 and join.join_type.value == "left" and join.name is None)
        if not plain:
            self._lossy(f"{label} ({join.cardinality.value if join.cardinality else 'no cardinality'}, "
                        f"{join.join_type.value}{', named' if join.name else ''}) imports back as a plain "
                        f"many-to-one left join from {shape[0]!r}.", model=name)

    def _relationship_shape(self, join: ModelJoin, *, forward: _Shape) -> _Shape | None:
        """``forward`` or its reverse, whichever points from the many side; ``None`` when undeterminable."""
        name, target, sources, targets = forward
        backward = (target, name, targets, sources)
        if join.cardinality in (JoinCardinality.MANY_TO_ONE, JoinCardinality.ONE_TO_ONE):
            return forward
        if join.cardinality is JoinCardinality.ONE_TO_MANY:
            return backward
        if self._is_key(target, targets):
            return forward
        if self._is_key(name, sources):
            return backward
        return None

    def _bad_keys(self, side: str, other: str, keys: list[str]) -> list[str]:
        ds = self.datasets[side]
        return [f"{side}.{k}" for k in keys
                if k not in ds.fields
                or join_key_error(model=side, target=other, key=k, side=side, columns=ds.graph.columns) is not None]

    # ---- measures ----

    def _measures(self, name: str) -> list[_Metric]:
        ds = self.datasets[name]
        out: list[_Metric] = []
        for measure in ds.graph.measures:
            label = f"Measure {measure.name or measure.formula!r} on {name!r}"
            try:
                key = bind_model_formula(measure.formula, model=ds.graph, models=list(self.graph.values()),
                                         dialect=self.target)
            except Exception as exc:  # noqa: BLE001 — an unbindable measure travels verbatim
                self._carry_measure(ds, measure, label=f"{label} does not bind ({exc})", needs=set())
                continue
            needs = self._read_models(ds.graph, key)
            try:
                metric = self._classify(ds, measure, key)
            except _Refused as refused:
                self._carry_measure(ds, measure, label=f"{label} {refused.reason}", needs=needs)
                continue
            missing = self._missing_models(needs)
            if missing:
                self._report(f"{label} reads {missing}, which are not exported; not exported.",
                             severity="dropped", category="subset", suggestion=self._needs_suggestion(missing),
                             model=name, metric=measure.name)
                continue
            out.append(metric)
        return out

    def _carry_measure(self, ds: _Dataset, measure: ModelMeasure, *, label: str, needs: set[str]) -> None:
        name = ds.model.name
        missing = self._missing_models(needs)
        if missing:
            self._report(f"{label}; it reads {missing}, which are not exported; not exported.",
                         severity="dropped", category="subset", suggestion=self._needs_suggestion(missing),
                         model=name, metric=measure.name)
            return
        self._slayer_only(label, model=name, metric=measure.name)
        if self.extensions:
            ds.slayer_measures.append(measure.model_copy(deep=True))

    def _read_models(self, home: SlayerModel, key: ValueKey) -> set[str]:
        out: set[str] = set()
        for leaf in walk_value_keys(key):
            if isinstance(leaf, (ColumnKey, ColumnSqlKey, StarKey)):
                owner = terminal_model(root=home, path=leaf.path, models_by_name=self.graph)
                if owner is not None:
                    out.add(owner.name)
        return out

    def _classify(self, ds: _Dataset, measure: ModelMeasure, key: ValueKey) -> _Metric:
        if any(isinstance(k, TransformKey) for k in walk_value_keys(key)):
            raise _Refused("is a transform, which Ossie metrics exclude", _RESTORE)
        aggregates = list(_outermost_aggregates(key))
        if not aggregates:
            raise _Refused("has no aggregate", _RESTORE)
        readers: list[str] = []
        for agg in aggregates:
            readers.append(self._aggregate_reader(ds, agg))
        portable = not any(a.agg == "count_distinct_approx" for a in aggregates)
        scoped = len(set(readers)) == 1
        try:
            tree = self._render(ds.graph, key, scoped=scoped, portable=portable)
        except Exception as exc:  # noqa: BLE001 — anything the shared renderer refuses stays SLayer-only
            raise _Refused(f"does not render as standard SQL ({exc})", _RESTORE) from exc
        return _Metric(home=ds.model.name, measure=measure, scope="dataset" if scoped else "model",
                       dataset=readers[0], tree=tree, portable=portable)

    def _aggregate_reader(self, ds: _Dataset, agg: AggregateKey) -> str:
        """The one model ``agg`` reads, or refused."""
        _refuse_slayer_only(agg)
        leaves = list(_row_leaves(agg.source)) + [leaf for _, v in agg.kwargs for leaf in _row_leaves(v)]
        paths = {leaf.path for leaf in leaves} or {()}
        if len(paths) != 1:
            raise _Refused("has an aggregate reading several models", _RESTORE)
        (path,) = paths
        owner = terminal_model(root=ds.graph, path=path, models_by_name=self.graph)
        if owner is None or not self._unique_route(ds.graph, path):
            raise _Refused("reads a model it reaches by more than one join route", _RESTORE)
        fields = self.datasets[owner.name].fields if owner.name in self.datasets else {}
        for leaf in leaves:
            leaf_name = leaf.leaf if isinstance(leaf, ColumnKey) else getattr(leaf, "column_name", None)
            if leaf_name is not None and leaf_name not in fields and owner.name in self.datasets:
                raise _Refused(f"reads the SLayer-only column {leaf_name!r}", _RESTORE)
        return owner.name

    def _unique_route(self, home: SlayerModel, path: tuple[str, ...]) -> bool:
        try:
            chain = walk(root=home, path=path, models_by_name=self.graph)
        except SlayerError:
            return False
        if chain is None:
            return False
        for hop in chain:
            src, dst = self.graph[hop.source_model], self.graph[hop.target_model]
            edges = [j for j in src.joins if j.target_model == dst.name]
            edges += [j for j in dst.joins if j.target_model == src.name]
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

    def _ref(self, home: SlayerModel, leaf: ColumnKey | ColumnSqlKey, *, scoped: bool) -> exp.Column:
        owner = terminal_model(root=home, path=leaf.path, models_by_name=self.graph)
        assert owner is not None
        name = leaf.leaf if isinstance(leaf, ColumnKey) else leaf.column_name
        column = exp.Column(this=self._ident(name, list(self.datasets[owner.name].fields)))
        if not scoped:
            column.set("table", self._ident(owner.name, list(self.datasets)))
        return column

    def _render(self, home: SlayerModel, key: ValueKey, *, scoped: bool, portable: bool) -> exp.Expr:
        dialect = self.neutral if portable else self.dialect
        column_type = model_column_type(model=home, models_by_name=self.graph)

        def row(value: ValueKey) -> Expression:
            leaves = [k for k in walk_value_keys(value) if isinstance(k, (ColumnKey, ColumnSqlKey))]
            slots = {leaf: f"s{i}" for i, leaf in enumerate(leaves)}
            aliases = AliasFacilities(slot_id_by_key=slots,
                                      value_by_slot_id={s: self._ref(home, k, scoped=scoped) for k, s in slots.items()})
            return _ossie_division(render_value_key(key=value, ctx=RenderContext(
                dialect=dialect, aliases=aliases, column_type=column_type)))

        def param(value: Any) -> Expression:
            if isinstance(value, (ColumnKey, ColumnSqlKey)):
                return row(value)
            return render_value_key(key=LiteralKey(value=value), ctx=RenderContext(dialect=dialect))

        def aggregate(agg: AggregateKey) -> Expression:
            owner = aggregation_owner(root=home, source=agg.source, models_by_name=self.graph)
            definition = aggregation_definition(owner=owner, agg=agg.agg)
            rendered = render_aggregate(AggregateSpec(
                aggregation=agg.agg,
                value=None if isinstance(agg.source, StarKey) else row(agg.source),
                bindings={name: param(value) for name, value in agg.kwargs},
                input_type=aggregation_source_type(agg.source, column_type=column_type),
                formula=rendered_formula(agg=agg.agg, definition=definition),
            ), dialect=dialect)
            rendered.meta[_AGGREGATE_MARK] = True
            return rendered

        return _ossie_division(render_value_key(key=key, ctx=RenderContext(
            dialect=dialect, composites=CompositeFacilities(agg_builder=aggregate), column_type=column_type)))

    # ---- naming and placement ----

    def _name_entities(self, metrics: list[_Metric]) -> None:
        scopes = self._field_scopes()
        dataset_scoped = [m for m in metrics if m.scope == "dataset"]
        for metric in (m for m in dataset_scoped if m.dataset == m.home):
            metric.name = scopes[metric.home].allocate(metric.measure.name or "measure")
        for metric in (m for m in dataset_scoped if m.dataset != m.home):
            metric.name = scopes[metric.dataset].allocate(f"{metric.home}_{metric.measure.name or 'measure'}")
        self._name_document_entities([m for m in metrics if m.scope == "model"])

    def _field_scopes(self) -> dict[str, _Names]:
        scopes: dict[str, _Names] = {}
        for name, ds in self.datasets.items():
            scopes[name] = _Names()
            for field in ds.fields:
                scopes[name].allocate(field)
        return scopes

    def _name_document_entities(self, model_metrics: list[_Metric]) -> None:
        """Explicitly named relationships first, then model metrics, then the remaining relationships."""
        document = _Names()
        for rel in self.relationships:
            explicit = next((d.join.name for d in rel.declarations if d.join.name), None)
            if explicit:
                rel.name = document.allocate(explicit)
        for metric in model_metrics:
            metric.name = document.allocate(metric.measure.name or "measure")
        for rel in self.relationships:
            if not rel.name:
                rel.name = document.allocate(f"{rel.from_dataset}_{rel.to}")

    def _place(self, metric: _Metric) -> None:
        measure = metric.measure
        meta = measure.meta or {}
        text = metric.tree.sql(dialect=PORTABLE_READER if metric.portable else self.target)
        expression: dict[str, Any] = (
            {"sql": text} if metric.portable else {"dialects": [{"dialect": self.token, "expression": text}]})
        entity: dict[str, Any] = {"name": metric.name, "expression": expression}
        description = explicit_description(measure.description, meta.get("osi_ai_context"))
        if description:
            entity["description"] = description
        if isinstance(meta.get("osi_datatype"), str):
            entity["datatype"] = meta["osi_datatype"]
        if meta.get("osi_ai_context"):
            entity["ai_context"] = meta["osi_ai_context"]
        payload = ext.MetricPayload(entity=metric.name, model=metric.home, measure=measure.model_copy(
            update={"meta": _slayer_meta(measure.meta)}))
        self._extensions(entity, meta, payload=payload, model=metric.home,
                         ossie=ext.expression_digest(OSIExpression.model_validate(expression)))
        if metric.scope == "dataset":
            self.datasets[metric.dataset].metrics.append(entity)
            relocated = metric.dataset != metric.home
        else:
            self.model_metrics.append(entity)
            relocated = metric.home not in self._read_datasets(metric)
        if relocated:
            self._lossy(f"Measure {measure.name!r} on {metric.home!r} is exported as metric {metric.name!r} "
                        f"{'on ' + repr(metric.dataset) if metric.scope == 'dataset' else 'of the model'} and "
                        f"imports back there.", model=metric.home, metric=measure.name)

    def _read_datasets(self, metric: _Metric) -> set[str]:
        return {c.table for c in metric.tree.find_all(exp.Column) if c.table}

    # ---- document ----

    def _shared_semantic_model(self) -> dict[str, Any] | None:
        shared = [(ds.model.meta or {}).get("osi_semantic_model") for ds in self.datasets.values()]
        first = shared[0]
        if isinstance(first, dict) and first.get("name") and all(s == first for s in shared):
            return first
        return None

    def _document(self) -> dict[str, Any]:
        semantic = self._shared_semantic_model()
        document: dict[str, Any] = {"version": OSSIE_VERSION}
        if semantic is not None:
            document["name"] = semantic["name"]
            if semantic.get("description"):
                document["description"] = semantic["description"]
            if semantic.get("ai_context"):
                document["ai_context"] = semantic["ai_context"]
        else:
            document["name"] = self.config.name
            if self.config.description:
                document["description"] = self.config.description
        document["datasets"] = [self._dataset_entity(ds) for _, ds in sorted(self.datasets.items())]
        relationships = sorted(self.relationships,
                               key=lambda r: (r.from_dataset, r.to, r.from_columns, r.to_columns))
        if relationships:
            document["relationships"] = [self._relationship_entity(r) for r in relationships]
        if self.model_metrics:
            document["metrics"] = self.model_metrics
        foreign = [e for e in (semantic or {}).get("custom_extensions") or [] if not ext.is_slayer(e)]
        if foreign:
            document["custom_extensions"] = foreign
        return document

    def _dataset_entity(self, ds: _Dataset) -> dict[str, Any]:
        model = ds.model
        meta = model.meta or {}
        entity: dict[str, Any] = {"name": model.name, "source": ds.source}
        if ds.primary_key:
            entity["primary_key"] = ds.primary_key
        if ds.unique_keys:
            entity["unique_keys"] = ds.unique_keys
        description = explicit_description(model.description, meta.get("osi_ai_context"))
        if description:
            entity["description"] = description
        if meta.get("osi_ai_context"):
            entity["ai_context"] = meta["osi_ai_context"]
        if ds.fields:
            entity["fields"] = list(ds.fields.values())
        if ds.metrics:
            entity["metrics"] = ds.metrics
        payload = ext.DatasetPayload(
            entity=model.name, source=ds.structure, hidden=model.hidden, access_tags=list(model.access_tags),
            default_time_dimension=model.default_time_dimension, meta=_slayer_meta(model.meta),
            aggregations=list(model.aggregations),
            columns=[Column.model_validate(c.model_dump(exclude=_EXCLUDED_COLUMN_FIELDS))
                     for c in ds.slayer_columns],
            measures=ds.slayer_measures,
        )
        self._extensions(entity, meta, payload=payload, model=model.name,
                         ossie=ext.source_digest(ds.source, reader=self.target))
        return entity

    def _relationship_entity(self, rel: _Relationship) -> dict[str, Any]:
        entity: dict[str, Any] = {"name": rel.name, "from": rel.from_dataset, "to": rel.to,
                                  "from_columns": rel.from_columns, "to_columns": rel.to_columns}
        meta = rel.declarations[0].join.meta or {}
        if meta.get("osi_ai_context"):
            entity["ai_context"] = meta["osi_ai_context"]
        declarations = [d.model_copy(update={"join": d.join.model_copy(update={"meta": _slayer_meta(d.join.meta)})})
                        for d in rel.declarations]
        self._extensions(entity, meta, payload=ext.RelationshipPayload(entity=rel.name, declarations=declarations),
                         model=rel.from_dataset, ossie=ext.relationship_digest(
                             from_dataset=rel.from_dataset, to=rel.to, from_columns=rel.from_columns,
                             to_columns=rel.to_columns))
        return entity


async def export_osi(*, engine: SlayerQueryEngine, datasource: str, models: list[str] | None = None,
                     extensions: bool = True) -> OsiExportResult:
    """Export ``datasource``'s models (or the ``models`` subset) as an Ossie document and its report."""
    storage = engine.storage
    config = await storage.get_datasource(datasource)
    if config is None:
        raise OsiExportError(f"Datasource {datasource!r} not found.")
    stored: dict[str, SlayerModel] = {}
    for name in sorted(await storage.list_models(data_source=datasource)):
        model = await storage.get_model(name, data_source=datasource)
        if model is not None:
            stored[name] = model
    unknown = [m for m in models or [] if m not in stored]
    if unknown:
        raise OsiExportError(f"Unknown model(s) in datasource {datasource!r}: {', '.join(unknown)}.")
    selected = sorted(set(models)) if models is not None else sorted(stored)
    virtual: dict[str, SlayerModel | Exception] = {}
    for name in selected:
        if stored[name].source_queries:
            try:
                virtual[name] = await engine.expand_query_backed_model(stored[name])
            except Exception as exc:  # noqa: BLE001 — reported on the model
                virtual[name] = exc
    return _Exporter(config=config, models=stored, selected=selected, virtual=virtual,
                     extensions=extensions).run()
