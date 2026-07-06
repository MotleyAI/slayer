"""Convert parsed OSI documents into SLayer models (DEV-1643).

Each OSI dataset becomes one ``SlayerModel``: its physical table is introspected
live (real column types + PK) and OSI semantic metadata (labels, descriptions,
is_time, ai_context, primary keys) is overlaid on top. OSI relationships become
``ModelJoin`` entries; OSI metrics become ``ModelMeasure`` formulas, anchored on
the model that reaches every dataset the metric references (via the shared
``recommend_root_model`` selection core). Constructs that cannot be expressed
exactly are clean-failed to a ``ConversionResult`` report, never silently lost.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import sqlalchemy as sa
import sqlglot
import sqlglot.expressions as exp

from slayer.core.enums import DataType
from slayer.core.formula import parse_formula
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.refs import IDENTIFIER_RE as _IDENTIFIER_RE
from slayer.engine.column_expansion import _root_scope_column_ids
from slayer.engine.ingestion import introspect_table_to_model
from slayer.engine.join_graph import JoinGraph, min_hops_root
from slayer.ingest_report import ConversionResult, ConversionWarning
from slayer.sql.client import _get_column_types_sync
from slayer.osi.expression import SQL_DIALECTS, convert_expression
from slayer.osi.models import (
    OSIAIContext,
    OSICustomExtension,
    OSIDataset,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIMetric,
    OSIRelationship,
    OSISemanticModel,
    ai_context_to_dict,
)
from slayer.osi.source import parse_source, resolve_datasource

logger = logging.getLogger(__name__)

# Dialects lacking a GROUP BY percentile/median aggregate (mirrors the dbt
# converter's caveat set).
_NO_PERCENTILE_DIALECTS = frozenset({"mysql", "tsql", "mssql", "sqlserver"})


class OsiConversionError(Exception):
    """Raised when an OSI import set cannot be converted (e.g. duplicate names)."""


def _legal_model_name(name: str) -> bool:
    return "__" not in name and "." not in name and ":" not in name


def _legal_column_name(name: str) -> bool:
    return "." not in name and ":" not in name


def _legal_measure_name(name: str) -> bool:
    return bool(_IDENTIFIER_RE.match(name))


def _is_bare_identifier(sql: str) -> bool:
    return bool(_IDENTIFIER_RE.match(sql.strip()))


def _missing_expr_columns(
    sql: str, available: set[str], self_name: str
) -> list[str] | None:
    """Unqualified and self-qualified column names in ``sql`` absent from
    ``available``. Returns ``None`` when ``sql`` cannot be parsed.

    Self-qualified references (``<self_name>.col``) are validated too; genuinely
    cross-model references (a different qualifier) are left to query-time join
    resolution, matching how ``Column.sql`` expansion treats join aliases.
    """
    try:
        tree = sqlglot.parse_one(sql)
    except sqlglot.errors.ParseError:
        return None
    missing = []
    for col in tree.find_all(exp.Column):
        if not col.table:
            if col.name not in available:
                missing.append(col.name)
        elif col.table == self_name and col.name not in available:
            missing.append(f"{col.table}.{col.name}")
    return missing


def _render_description(explicit: Optional[str], ctx: Optional[OSIAIContext]) -> Optional[str]:
    """Description = explicit OSI description (lead) + ai_context instructions +
    synonyms."""
    parts: list[str] = []
    if explicit:
        parts.append(explicit)
    ctx_dict = ai_context_to_dict(ctx)
    if ctx_dict:
        instructions = ctx_dict.get("instructions")
        if instructions and instructions != explicit:
            parts.append(instructions)
        synonyms = ctx_dict.get("synonyms")
        if synonyms:
            parts.append("Synonyms: " + ", ".join(synonyms))
    return "\n".join(parts) or None


def _build_meta(
    ctx: Optional[OSIAIContext],
    custom_extensions: Optional[list[OSICustomExtension]],
    extra: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    meta: dict[str, Any] = dict(extra or {})
    ctx_dict = ai_context_to_dict(ctx)
    if ctx_dict:
        meta["osi_ai_context"] = ctx_dict
    if custom_extensions:
        meta["osi_custom_extensions"] = [e.model_dump() for e in custom_extensions]
    return meta or None


class OsiToSlayerConverter:
    """Convert OSI documents into SLayer models."""

    def __init__(
        self,
        documents: list[OSIDocument],
        data_source: str,
        sa_engine: sa.Engine,
        *,
        dialect: str = "ANSI_SQL",
        target_dialect: str | None = None,
    ) -> None:
        self.documents = documents
        self.data_source = data_source
        self.sa_engine = sa_engine
        self.dialect = dialect
        self.target_dialect = target_dialect
        self._models: dict[str, SlayerModel] = {}
        self._warnings: list[ConversionWarning] = []
        self._unconverted: list[ConversionWarning] = []

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

    # ---- top-level ----

    def convert(self) -> ConversionResult:
        inspector = sa.inspect(self.sa_engine)
        semantic_models = [sm for doc in self.documents for sm in doc.semantic_model]
        self._check_duplicate_dataset_names(semantic_models)

        for sm in semantic_models:
            for ds in sm.datasets:
                self._build_model(ds, sm, inspector)
        for sm in semantic_models:
            for rel in sm.relationships or []:
                self._build_join(rel)

        self._validate_cross_model_field_refs()

        graph = JoinGraph.build_from_models(list(self._models.values()))
        for sm in semantic_models:
            self._build_measures_for(sm, graph)

        return ConversionResult(
            models=list(self._models.values()),
            unconverted_metrics=self._unconverted,
            warnings=self._warnings,
        )

    def _build_measures_for(self, sm: OSISemanticModel, graph: JoinGraph) -> None:
        sm_model_names = [d.name for d in sm.datasets if d.name in self._models]
        for metric in sm.metrics or []:
            self._build_measure(metric, sm_model_names, graph)

    def _check_duplicate_dataset_names(self, sms: list[OSISemanticModel]) -> None:
        seen: set[str] = set()
        dupes: set[str] = set()
        for sm in sms:
            for ds in sm.datasets:
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

    def _build_model(self, ds: OSIDataset, sm: OSISemanticModel,
                     inspector: sa.engine.Inspector) -> None:
        if not _legal_model_name(ds.name):
            self._warn(
                f"Dataset name {ds.name!r} contains characters SLayer forbids in "
                f"model names ('__', '.', ':'); skipping.",
                model_name=ds.name, category="illegal_name",
            )
            return

        parsed = parse_source(ds.source)
        resolve_datasource(parsed.database, self.data_source)  # stubbed routing

        try:
            if parsed.is_query:
                base = self._build_sql_mode_model(ds, parsed.query)
            else:
                base = introspect_table_to_model(
                    sa_engine=self.sa_engine, inspector=inspector,
                    table_name=parsed.table, schema=parsed.schema_name,
                    data_source=self.data_source, model_name=ds.name,
                )
        except Exception as exc:  # noqa: BLE001 — per-dataset isolation
            self._warn(
                f"Failed to introspect dataset {ds.name!r} (source {ds.source!r}): "
                f"{exc}; skipping.",
                model_name=ds.name, category="introspection",
            )
            return

        self._overlay_fields(base, ds)
        self._apply_dataset_metadata(base, ds, sm)
        self._models[ds.name] = base

    def _build_sql_mode_model(self, ds: OSIDataset, query: str) -> SlayerModel:
        # Query source: introspect the query's output columns live (LIMIT-0 /
        # cursor-metadata probe), exactly as table sources are introspected.
        # ``target_dialect`` is the datasource type, used for the dialect-correct
        # probe (LIMIT 0 vs SELECT TOP vs SQLite's LIMIT-1 fallback).
        types = _get_column_types_sync(
            query, connection_string="", db_type=self.target_dialect,
            engine=self.sa_engine,
        )
        columns = [Column(name=name, type=category) for name, category in types.items()]
        return SlayerModel(name=ds.name, sql=query, data_source=self.data_source,
                           columns=columns)

    def _overlay_fields(self, model: SlayerModel, ds: OSIDataset) -> None:
        by_name = {c.name: c for c in model.columns}
        introspected = set(by_name)
        first_time_dim: str | None = None

        for field in ds.fields or []:
            time_col = self._overlay_one_field(field, model, by_name, introspected, ds)
            if time_col and first_time_dim is None:
                first_time_dim = time_col

        # OSI primary_key is authoritative.
        for pk in ds.primary_key or []:
            if pk in by_name:
                by_name[pk].primary_key = True

        if first_time_dim and not model.default_time_dimension:
            model.default_time_dimension = first_time_dim

    def _overlay_one_field(self, field: OSIField, model: SlayerModel,
                           by_name: dict[str, Column], introspected: set[str],
                           ds: OSIDataset) -> str | None:
        """Overlay one OSI field onto the model. Returns the column name if the
        field is a time dimension, else None (clean-fails are reported)."""
        if not _legal_column_name(field.name):
            self._warn(
                f"Field name {field.name!r} on dataset {ds.name!r} contains "
                f"'.'/':'; skipping the field.",
                model_name=ds.name, category="illegal_name",
            )
            return None

        sql = self._resolve_expression(field.expression)
        if sql is None:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} has no SQL-dialect "
                f"expression; skipping.",
                model_name=ds.name, category="dialect",
            )
            return None

        col = self._resolve_field_column(field, sql, by_name, introspected, ds)
        if col is None:
            return None

        col.label = field.label or col.label
        col.description = _render_description(field.description, field.ai_context) \
            or col.description
        meta = _build_meta(field.ai_context, field.custom_extensions)
        if meta:
            col.meta = {**(col.meta or {}), **meta}

        is_time = bool(field.dimension and field.dimension.is_time)
        if is_time and col.type not in (DataType.DATE, DataType.TIMESTAMP):
            col.type = DataType.TIMESTAMP

        # Bare overlays return the existing column object (no-op here); an
        # aliased/derived field that shadows an existing column REPLACES it so
        # its expression isn't silently dropped by an append-only path.
        existing = by_name.get(col.name)
        if existing is None:
            model.columns.append(col)
            by_name[col.name] = col
        elif existing is not col:
            model.columns[model.columns.index(existing)] = col
            by_name[col.name] = col
        return col.name if is_time else None

    def _resolve_field_column(self, field: OSIField, sql: str,
                              by_name: dict[str, Column], introspected: set[str],
                              ds: OSIDataset) -> Column | None:
        """Return the Column (existing or new) this field maps to, or None on a
        clean-fail (already reported)."""
        if _is_bare_identifier(sql):
            if sql == field.name and field.name in by_name:
                return by_name[field.name]          # overlay existing column
            if sql in introspected:
                # aliased/renamed reference to a real column -> derived column
                return Column(name=field.name, sql=sql, type=by_name[sql].type)
            self._warn(
                f"Field {field.name!r} on {ds.name!r} references column {sql!r} "
                f"which is not present in the table; skipping.",
                model_name=ds.name, category="missing_column",
            )
            return None
        # derived expression. Validate its column references exist on the table
        # (consistent with the bare-field / metric / relationship checks); a
        # collision with an existing column is handled by the replace-or-append
        # logic in _overlay_one_field.
        missing = _missing_expr_columns(sql, introspected, ds.name)
        if missing is None:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} has an unparseable "
                f"expression {sql!r}; skipping.",
                model_name=ds.name, category="expression",
            )
            return None
        if missing:
            self._warn(
                f"Field {field.name!r} on {ds.name!r} references unknown "
                f"column(s) {missing}; skipping.",
                model_name=ds.name, category="missing_column",
            )
            return None
        is_time = bool(field.dimension and field.dimension.is_time)
        return Column(
            name=field.name, sql=sql,
            type=DataType.TIMESTAMP if is_time else DataType.DOUBLE,
        )

    def _apply_dataset_metadata(self, model: SlayerModel, ds: OSIDataset,
                                sm: OSISemanticModel) -> None:
        model.description = _render_description(ds.description, ds.ai_context) \
            or model.description
        extra: dict[str, Any] = {}
        if ds.unique_keys:
            extra["osi_unique_keys"] = ds.unique_keys
        sm_ctx = ai_context_to_dict(sm.ai_context)
        if sm_ctx or sm.description:
            extra["osi_semantic_model"] = {
                "name": sm.name,
                "description": sm.description,
                "ai_context": sm_ctx,
            }
        meta = _build_meta(ds.ai_context, ds.custom_extensions, extra=extra)
        if meta:
            model.meta = {**(model.meta or {}), **meta}

    # ---- relationships -> joins ----

    def _build_join(self, rel: OSIRelationship) -> None:
        src = rel.from_dataset
        if src not in self._models:
            self._warn(
                f"Relationship {rel.name!r} references unknown source dataset "
                f"{src!r}; skipping.",
                category="relationship",
            )
            return
        if rel.to not in self._models:
            self._warn(
                f"Relationship {rel.name!r} targets unknown dataset {rel.to!r}; "
                f"skipping.",
                model_name=src, category="relationship",
            )
            return
        if len(rel.from_columns) != len(rel.to_columns):
            self._warn(
                f"Relationship {rel.name!r} has mismatched key lengths "
                f"({len(rel.from_columns)} vs {len(rel.to_columns)}); skipping.",
                model_name=src, category="relationship",
            )
            return

        missing = self._missing_join_columns(rel)
        if missing:
            self._warn(
                f"Relationship {rel.name!r} references join columns not present "
                f"on their models: {missing}; skipping.",
                model_name=src, category="relationship",
            )
            return

        # SLayer's ModelJoin keys only on target_model and runtime join-walking
        # picks the first match, so a second relationship to the same target
        # (e.g. a distinct role) would be unreachable and could bind refs to the
        # wrong join. Keep the first; report and skip duplicates.
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
            description=_render_description(None, rel.ai_context),
            meta=_build_meta(rel.ai_context, rel.custom_extensions),
        ))

    def _validate_cross_model_field_refs(self) -> None:
        """Post-join pass: derived columns may reference joined models via
        ``<alias>.<col>`` / ``<a>__<b>.<col>``. Resolve each such ref through the
        join graph and drop (with a report) any column whose cross-model ref
        names a model with no join path or a nonexistent target column — so a
        typo clean-fails at import instead of erroring at query time.
        """
        # Fixed-point: dropping a column can invalidate another column that
        # referenced it, so re-run until a pass drops nothing.
        changed = True
        while changed:
            changed = False
            for model in list(self._models.values()):
                for col in list(model.columns):
                    if not col.sql:
                        continue
                    bad = self._unresolvable_cross_model_refs(model, col.sql)
                    if bad:
                        model.columns.remove(col)
                        changed = True
                        self._warn(
                            f"Column {col.name!r} on {model.name!r} references "
                            f"unresolvable cross-model column(s) {bad}; dropping.",
                            model_name=model.name, category="missing_column",
                        )

    def _unresolvable_cross_model_refs(self, model: SlayerModel, sql: str) -> list[str]:
        """Cross-model (non-self, qualified) column refs in ``sql`` that don't
        resolve to a joined model + existing column. Mirrors runtime scope rules:
        only root-scope refs are checked (nested subquery/CTE aliases are left
        alone), and catalog/db-qualified physical refs are skipped. Self /
        unqualified refs were validated at field-overlay time."""
        try:
            tree = sqlglot.parse_one(sql)
        except sqlglot.errors.ParseError:
            return []
        root_ids = _root_scope_column_ids(parsed=tree)
        bad = []
        for col in tree.find_all(exp.Column):
            if id(col) not in root_ids:
                continue  # nested-scope alias (CTE / sub-query) — not a join ref
            if col.args.get("db") or col.args.get("catalog"):
                continue  # catalog/db-qualified physical ref — outside SLayer's contract
            if not col.table or col.table == model.name:
                continue
            target = self._walk_join_alias(model, col.table)
            if target is None or not any(c.name == col.name for c in target.columns):
                bad.append(f"{col.table}.{col.name}")
        return bad

    def _walk_join_alias(self, host: SlayerModel, alias: str) -> SlayerModel | None:
        """Resolve a ``__``-delimited join alias (e.g. ``customers__regions``)
        to the terminal joined model by walking ``host``'s join chain, or None
        if any hop is not a declared join."""
        current = host
        for hop in (alias.split("__") if "__" in alias else [alias]):
            join = next((j for j in current.joins if j.target_model == hop), None)
            if join is None:
                return None
            current = self._models.get(hop)
            if current is None:
                return None
        return current

    def _model_has_column(self, model_name: str, column: str) -> bool:
        model = self._models.get(model_name)
        return model is not None and any(c.name == column for c in model.columns)

    def _model_has_name(self, model_name: str, name: str) -> bool:
        """Whether ``name`` is taken by a column OR measure on the model —
        SLayer requires the two to share one namespace, so a materialized
        hidden-column name must avoid both."""
        model = self._models.get(model_name)
        if model is None:
            return False
        return (any(c.name == name for c in model.columns)
                or any(m.name == name for m in model.measures))

    def _missing_join_columns(self, rel: OSIRelationship) -> list[str]:
        """Qualified names of relationship join columns absent from their
        model, so a typo clean-fails instead of emitting a broken join."""
        missing = [f"{rel.from_dataset}.{c}" for c in rel.from_columns
                   if not self._model_has_column(rel.from_dataset, c)]
        missing += [f"{rel.to}.{c}" for c in rel.to_columns
                    if not self._model_has_column(rel.to, c)]
        return missing

    # ---- metrics -> measures ----

    def _build_measure(self, metric: OSIMetric, sm_model_names: list[str],
                       graph: JoinGraph) -> None:
        if not _legal_measure_name(metric.name):
            self._unconv(
                f"Metric name {metric.name!r} is not a valid SLayer identifier.",
                metric_name=metric.name,
            )
            return

        expr = self._resolve_expression(metric.expression)
        if expr is None:
            self._unconv(
                f"Metric {metric.name!r} has no SQL-dialect expression.",
                metric_name=metric.name, category="dialect",
            )
            return

        owner_of = self._make_owner_of(sm_model_names)
        anchor = self._select_anchor(metric.name, expr, sm_model_names, owner_of, graph)
        if anchor is None:
            return  # already reported

        # Enforce SLayer's namespace invariants before the post-construction
        # append (which bypasses SlayerModel's validators): a measure name must
        # be unique and must not collide with a column on the same model.
        anchor_model = self._models[anchor]
        if any(m.name == metric.name for m in anchor_model.measures):
            self._unconv(
                f"Metric {metric.name!r} duplicates an existing measure on "
                f"model {anchor!r}.",
                metric_name=metric.name, category="duplicate_measure",
            )
            return
        if self._model_has_column(anchor, metric.name):
            self._unconv(
                f"Metric {metric.name!r} collides with a column name on model "
                f"{anchor!r}.",
                metric_name=metric.name, category="name_collision",
            )
            return

        ref_of = self._make_ref_of(graph, anchor)
        percentile_unsupported = (
            self.target_dialect is not None
            and self.target_dialect.lower() in _NO_PERCENTILE_DIALECTS
        )
        result = convert_expression(
            expr, entity_name=metric.name, owner_of=owner_of, ref_of=ref_of,
            percentile_unsupported=percentile_unsupported,
            name_taken=self._model_has_name,
        )
        if not result.ok:
            self._unconv(
                f"Metric {metric.name!r}: {result.reason}",
                metric_name=metric.name,
            )
            return

        try:
            parse_formula(result.formula)
        except Exception as exc:  # noqa: BLE001 — reject un-parseable emission
            self._unconv(
                f"Metric {metric.name!r}: emitted formula {result.formula!r} is "
                f"not a valid SLayer formula ({exc}).",
                metric_name=metric.name,
            )
            return

        # Build the measure first so a construction failure (e.g. a metric named
        # after a reserved transform like ``cumsum``) clean-fails instead of
        # crashing the import — and before materializing columns, so a rejected
        # metric leaves no orphan hidden columns.
        try:
            measure = ModelMeasure(
                formula=result.formula,
                name=metric.name,
                description=_render_description(metric.description, metric.ai_context),
                meta=_build_meta(metric.ai_context, metric.custom_extensions),
            )
        except Exception as exc:  # noqa: BLE001 — any validation error -> report
            self._unconv(
                f"Metric {metric.name!r} cannot be expressed as a SLayer measure "
                f"({exc}).",
                metric_name=metric.name,
            )
            return

        self._materialize_columns(result.materialized)
        self._models[anchor].measures.append(measure)
        for w in result.warnings:
            self._warn(f"Metric {metric.name!r}: {w}", metric_name=metric.name,
                       category="dialect", severity="info")

    def _select_anchor(self, metric_name: str, expr: str, sm_model_names: list[str],
                       owner_of, graph: JoinGraph) -> str | None:
        owners = self._referenced_owners(expr, owner_of)
        if owners:
            anchor = min_hops_root(graph, sm_model_names, owners)
            if anchor is None:
                self._unconv(
                    f"Metric {metric_name!r} references models {sorted(owners)} "
                    f"with no single model reaching all of them via joins.",
                    metric_name=metric_name, category="no_join_path",
                    suggestion="Add the required relationship(s), or split the "
                               "metric across a multi-stage query.",
                )
            return anchor
        # No column references (e.g. COUNT(*)). Attribute it to the semantic
        # model's unique fact table (the one dataset that is never a join
        # target). When there is no unique fact table, the metric is an orphan
        # with no determinable grain -> error rather than guess.
        anchor = self._fact_root(sm_model_names)
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

    def _referenced_owners(self, expr: str, owner_of) -> set[str]:
        try:
            tree = sqlglot.parse_one(expr)
        except sqlglot.errors.ParseError:
            return set()
        owners: set[str] = set()
        for col in tree.find_all(exp.Column):
            owner = owner_of(col.table or None, col.name)
            if owner:
                owners.add(owner)
        return owners

    def _fact_root(self, sm_model_names: list[str]) -> str | None:
        """The unique dataset that is never a join target (the fact table), or
        ``None`` when there is no unique fact table (0 or >1 candidates)."""
        if not sm_model_names:
            return None
        targets: set[str] = set()
        for name in sm_model_names:
            for j in self._models[name].joins:
                targets.add(j.target_model)
        non_targets = [n for n in sm_model_names if n not in targets]
        return non_targets[0] if len(non_targets) == 1 else None

    def _make_owner_of(self, sm_model_names: list[str]):
        def has_column(model_name: str, column: str) -> bool:
            model = self._models.get(model_name)
            return model is not None and any(c.name == column for c in model.columns)

        def owner_of(qualifier: Optional[str], column: str) -> Optional[str]:
            if qualifier is not None:
                # Verify the column actually exists on the qualified model —
                # otherwise a metric like SUM(orders.no_such_col) would import
                # as a measure that fails at query time.
                return qualifier if has_column(qualifier, column) else None
            # Unqualified: resolve only when exactly one dataset owns the column.
            # Ambiguity (the same column name on multiple datasets) returns None
            # so the metric clean-fails instead of binding by dataset order.
            matches = [name for name in sm_model_names if has_column(name, column)]
            return matches[0] if len(matches) == 1 else None
        return owner_of

    def _make_ref_of(self, graph: JoinGraph, anchor: str):
        def ref_of(model: str, column: str) -> Optional[str]:
            path = graph.shortest_path(anchor, model)
            if path is None:
                return None
            return ".".join([*path, column])
        return ref_of

    def _materialize_columns(self, materialized) -> None:
        for mc in materialized:
            model = self._models.get(mc.owning_model)
            if model is None:
                continue
            if any(c.name == mc.name for c in model.columns):
                continue
            model.columns.append(Column(
                name=mc.name, sql=mc.sql, type=DataType.DOUBLE, hidden=True,
            ))

    # ---- dialect selection ----

    def _resolve_expression(self, osi_expr: OSIExpression) -> str | None:
        """Pick the expression for the requested dialect, else fall back among
        SQL-compatible dialects. Non-SQL-only expressions return None."""
        by_dialect = {de.dialect.value: de.expression for de in osi_expr.dialects}
        if self.dialect in by_dialect:
            return by_dialect[self.dialect]
        for name, expression in by_dialect.items():
            if name in SQL_DIALECTS:
                return expression
        return None
