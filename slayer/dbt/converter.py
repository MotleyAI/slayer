"""Convert a parsed DbtProject into SLayer models.

Orchestrates the full pipeline: entity resolution, dimension/measure
conversion, measure consolidation (one ``Column`` per unique expr +
one ``ModelMeasure`` per dbt measure), and folding of metric definitions
(simple-with-filter / derived / ratio / cumulative) into ``ModelMeasure``
entries on the source semantic model.

The converter never emits ``SlayerQuery`` definitions: every dbt artefact
that produces a query-shaped result is expressed as a named formula on a
model. Constructs that cannot be expressed exactly (conversion metrics,
windowed / grain-to-date cumulatives, semi-additive measures, …) are *failed
cleanly*: routed to the structured ``ConversionResult`` report with a precise
reason + workaround, and the raw construct is stashed into the owning entity's
``meta`` so the dropped semantics are retained, never silently lost. (DEV-1595.)
"""

import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Literal, Optional, Tuple

import sqlalchemy as sa
from pydantic import BaseModel, Field

from slayer.core.enums import DataType, JoinType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.formula import parse_formula
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.refs import IDENTIFIER_RE as _IDENTIFIER_RE
from slayer.dbt.entities import EntityRegistry
from slayer.dbt.filters import _DIMENSION_RE, convert_dbt_filter
from slayer.dbt.models import (
    DbtConfig,
    DbtDimension,
    DbtMeasure,
    DbtMetric,
    DbtMetricInput,
    DbtMetricTimeWindow,
    DbtMetricTypeParams,
    DbtProject,
    DbtRegularModel,
    DbtSemanticModel,
)
from slayer.dbt.sql_resolver import resolve_refs
from slayer.engine.ingestion import introspect_table_to_model

logger = logging.getLogger(__name__)

# Map dbt aggregation names to SLayer aggregation names
_AGG_MAP: Dict[str, str] = {
    "sum": "sum",
    "average": "avg",
    "avg": "avg",
    "count": "count",
    "count_distinct": "count_distinct",
    # DEV-1595: SLayer-added dialect-aware approximate-distinct. Not in
    # MetricFlow's AggregationType enum — mapped defensively for non-canonical
    # / legacy inputs (e.g. dbt-to-cube's countDistinctApprox).
    "count_distinct_approx": "count_distinct_approx",
    "min": "min",
    "max": "max",
    "median": "median",
    "percentile": "percentile",
    "sum_boolean": "sum",
}

_FLOAT_FORMAT = NumberFormat(type=NumberFormatType.FLOAT)

# Standard SLayer time granularities accepted in offset_window shifts. Custom /
# non-standard granularities (e.g. "fortnight") are routed to a clean-fail.
_STANDARD_GRAINS: frozenset = frozenset({
    "second", "minute", "hour", "day", "week", "month", "quarter", "year",
})

# Dialects with no GROUP-BY percentile / median aggregate. A percentile/median
# measure imports fine but fails at query time on these, so the converter emits
# an info-level caveat when ``target_dialect`` is one of them.
_NO_PERCENTILE_DIALECTS: frozenset = frozenset({"mysql", "tsql", "mssql", "sqlserver"})

# Shared workaround text for an unreachable cross-model filter (used by the
# simple / ratio / derived push-down paths).
_JOIN_REACHABILITY_SUGGESTION = "Add the required join, or filter on a local dimension."


class DbtConversionError(Exception):
    """Raised when a dbt project cannot be converted to SLayer shape.

    The message includes the offending semantic-model name and the
    colliding identifiers so the user can fix the dbt definitions.
    """


class ConversionWarning(BaseModel):
    """A structured entry in the conversion report (DEV-1595).

    ``category`` groups entries in ``render_report``; ``severity`` is one of
    ``"unconverted"`` (tried to convert, couldn't), ``"dropped"`` (intentional
    clean-fail of an inexpressible construct), or ``"info"`` (a caveat — the
    construct imports but has a runtime limitation). ``suggestion`` carries the
    documented workaround.
    """
    model_name: Optional[str] = None
    metric_name: Optional[str] = None
    message: str
    category: str = "general"
    severity: Literal["unconverted", "dropped", "info"] = "unconverted"
    suggestion: Optional[str] = None


class ConversionResult(BaseModel):
    """Result of converting a DbtProject to SLayer representations."""
    models: List[SlayerModel] = Field(default_factory=list)
    unconverted_metrics: List[ConversionWarning] = Field(default_factory=list)
    warnings: List[ConversionWarning] = Field(default_factory=list)

    def _all_entries(self) -> List[ConversionWarning]:
        return list(self.unconverted_metrics) + list(self.warnings)

    def render_report(self) -> str:
        """Render the conversion report grouped by category (DEV-1595).

        Each category becomes a heading with a count; each entry lists its
        entity, severity, reason, and (when present) the documented workaround.
        """
        entries = self._all_entries()
        if not entries:
            return "No conversion issues."
        by_cat: Dict[str, List[ConversionWarning]] = defaultdict(list)
        for e in entries:
            by_cat[e.category or "general"].append(e)
        lines: List[str] = []
        for cat in sorted(by_cat):
            items = by_cat[cat]
            lines.append(f"## {cat} ({len(items)})")
            for e in items:
                entity = e.metric_name or e.model_name or "general"
                lines.append(f"  - [{e.severity}] {entity}: {e.message}")
                if e.suggestion:
                    lines.append(f"      workaround: {e.suggestion}")
            lines.append("")
        return "\n".join(lines).rstrip()

    def tally(self) -> Tuple[int, int]:
        """``(unconverted, dropped)`` counts by severity for the CLI summary."""
        entries = self._all_entries()
        unconverted = sum(1 for e in entries if e.severity == "unconverted")
        dropped = sum(1 for e in entries if e.severity == "dropped")
        return unconverted, dropped


def _map_agg(dbt_agg: str) -> str:
    """Map a dbt aggregation name to a SLayer aggregation name."""
    mapped = _AGG_MAP.get(dbt_agg.lower())
    if mapped is None:
        logger.warning("Unknown dbt aggregation '%s', passing through as-is", dbt_agg)
        return dbt_agg.lower()
    return mapped


def _is_simple_identifier(s: str) -> bool:
    """A bare SQL column reference (no operators, calls, or dots)."""
    return bool(_IDENTIFIER_RE.match(s))


def _meta_of(config: Optional[DbtConfig]) -> Optional[Dict[str, Any]]:
    """Extract ``config.meta`` (or ``None``)."""
    if config is not None and config.meta:
        return dict(config.meta)
    return None


def _convert_dimension(dim: DbtDimension) -> Column:
    """Convert a dbt dimension to a SLayer column."""
    if dim.type == "time":
        data_type = DataType.TIMESTAMP
    else:
        data_type = DataType.TEXT

    sql = dim.expr if dim.expr and dim.expr != dim.name else None

    return Column(
        name=dim.name,
        sql=sql,
        type=data_type,
        description=dim.description,
        label=dim.label,
        meta=_meta_of(dim.config),
    )


class DbtToSlayerConverter:
    """Convert a DbtProject into SLayer models."""

    def __init__(
        self,
        project: DbtProject,
        data_source: str,
        sa_engine: Optional[sa.Engine] = None,
        include_hidden_models: bool = False,
        target_dialect: Optional[str] = None,
    ) -> None:
        self.project = project
        self.data_source = data_source
        self.sa_engine = sa_engine
        self.include_hidden_models = include_hidden_models
        # DEV-1595: when set to a dialect that lacks percentile/median
        # (mysql / tsql), the converter emits info caveats for those measures.
        self.target_dialect = target_dialect
        self.entity_registry = EntityRegistry()
        self._warnings: List[ConversionWarning] = []
        self._unconverted: List[ConversionWarning] = []
        # {model_name: SlayerModel} for metric resolution
        self._models_by_name: Dict[str, SlayerModel] = {}
        # {model_name: DbtSemanticModel} for looking up entities
        self._dbt_models_by_name: Dict[str, DbtSemanticModel] = {}
        # Filtered-column dedup: {(model, column_expr, normalized_filter): col_name}
        self._filtered_columns: Dict[Tuple[str, str, Optional[str]], str] = {}
        # {regular_model_name: raw_code} — used to inline SQL into semantic
        # models whose underlying dbt model is a query rather than a table.
        self._regular_models_sql: Dict[str, str] = {
            rm.name: rm.raw_code
            for rm in project.regular_models
            if rm.raw_code
        }

    def convert(self) -> ConversionResult:
        """Full conversion pipeline."""
        self.entity_registry.build(self.project.semantic_models)

        for sm in self.project.semantic_models:
            self._dbt_models_by_name[sm.name] = sm

        models: List[SlayerModel] = []
        for sm in self.project.semantic_models:
            model = self._convert_semantic_model(sm)
            models.append(model)
            self._models_by_name[model.name] = model

        for metric in self.project.metrics:
            self._convert_metric(metric)

        self._prune_dangling_measures()
        self._mirror_inner_joins()

        if self.include_hidden_models and self.project.regular_models:
            models.extend(self._convert_regular_models(existing_names={m.name for m in models}))

        return ConversionResult(
            models=models,
            unconverted_metrics=self._unconverted,
            warnings=self._warnings,
        )

    def _mirror_inner_joins(self) -> None:
        """Ensure inner joins are symmetric: if A→B is inner, B→A should be too."""
        for model in list(self._models_by_name.values()):
            for join in model.joins:
                if join.join_type != JoinType.INNER:
                    continue
                target = self._models_by_name.get(join.target_model)
                if target is None:
                    continue
                reverse_pairs = [[tgt, src] for src, tgt in join.join_pairs]
                already_exists = any(
                    j.target_model == model.name and j.join_pairs == reverse_pairs
                    for j in target.joins
                )
                if not already_exists:
                    target.joins.append(ModelJoin(
                        target_model=model.name,
                        join_pairs=reverse_pairs,
                        join_type=JoinType.INNER,
                    ))

    def _prune_dangling_measures(self) -> None:
        """Drop+report any ``ModelMeasure`` whose formula references a name that
        does not resolve on its model (DEV-1595 robust validation pass).

        A derived / ratio metric whose input metric was itself clean-failed
        (measure-less, time-spine gap-fill, unreachable filter, filtered-leaf
        clean-fail, or a transitively-dropped dependency) leaves a bare formula
        reference to a measure that was never materialized. Rather than predict
        every such case inline at conversion time, this final pass validates the
        emitted formulas against the actual model and removes the ones that
        can't resolve — running to a fixpoint so a measure depending on a
        just-dropped one is dropped too.
        """
        for model in self._models_by_name.values():
            self._prune_model_measures(model)

    def _prune_model_measures(self, model: SlayerModel) -> None:
        agg_names = frozenset(a.name for a in model.aggregations)
        changed = True
        while changed:
            changed = False
            named = {m.name: m.formula for m in model.measures if m.name}
            survivors: List[ModelMeasure] = []
            for m in model.measures:
                others = {k: v for k, v in named.items() if k != m.name}
                try:
                    parse_formula(m.formula, extra_agg_names=agg_names, named_measures=others)
                    survivors.append(m)
                except (ValueError, RecursionError) as exc:
                    self._unconverted.append(ConversionWarning(
                        model_name=model.name,
                        metric_name=m.name,
                        category="dangling_reference",
                        severity="dropped",
                        message=(
                            f"Measure '{m.name}' references a name that does not resolve "
                            f"on model '{model.name}' and was dropped: {exc}"
                        ),
                        suggestion="Ensure every referenced metric/measure converts successfully.",
                    ))
                    changed = True
            if changed:
                model.measures = survivors

    def _convert_regular_models(self, existing_names: set) -> List[SlayerModel]:
        """Convert orphan dbt models (not wrapped by semantic_models) to hidden SLayer models."""
        if self.sa_engine is None:
            self._warnings.append(ConversionWarning(
                category="hidden_models",
                severity="info",
                message=(
                    "include_hidden_models=True but no SQLAlchemy engine was provided; "
                    "skipping regular-model import."
                ),
            ))
            return []

        engine = self.sa_engine
        inspector = sa.inspect(engine)
        results: List[SlayerModel] = []
        for rm in self.project.regular_models:
            if rm.name in existing_names:
                continue
            converted = self._convert_regular_model(rm=rm, sa_engine=engine, inspector=inspector)
            if converted is not None:
                results.append(converted)
                existing_names.add(converted.name)
        return results

    def _convert_regular_model(
        self,
        rm: DbtRegularModel,
        sa_engine: sa.Engine,
        inspector: sa.engine.Inspector,
    ) -> Optional[SlayerModel]:
        """Introspect a regular dbt model and wrap it as a hidden SlayerModel."""
        table_name = rm.alias or rm.name
        try:
            model = introspect_table_to_model(
                sa_engine=sa_engine,
                inspector=inspector,
                table_name=table_name,
                schema=rm.schema_name,
                data_source=self.data_source,
                model_name=rm.name,
            )
        except Exception as exc:
            self._warnings.append(ConversionWarning(
                model_name=rm.name,
                category="hidden_models",
                severity="info",
                message=(
                    f"Skipped hidden import of dbt model '{rm.name}' "
                    f"(table '{table_name}'): {type(exc).__name__}: {exc}"
                ),
            ))
            return None

        model.hidden = True
        if rm.description:
            model.description = rm.description

        col_descriptions = {c.name: c.description for c in rm.columns if c.description}
        if col_descriptions:
            for c in model.columns:
                desc = col_descriptions.get(c.name)
                if desc and not c.description:
                    c.description = desc

        return model

    def _convert_semantic_model(self, sm: DbtSemanticModel) -> SlayerModel:
        """Convert a single dbt semantic model to a SlayerModel.

        Hard-fails (DbtConversionError) when the same name appears as both a
        dimension and a measure on this semantic model — ambiguous, since v2
        SLayer columns and measures share a namespace per model.
        """
        # Q-G: hard-fail on dim/measure name collisions before doing any work.
        dim_names = {d.name for d in sm.dimensions}
        measure_names = {m.name for m in sm.measures}
        collisions = sorted(dim_names & measure_names)
        if collisions:
            raise DbtConversionError(
                f"Semantic model '{sm.name}': dimension and measure share name(s) "
                f"{collisions}. SLayer columns and measures occupy a single "
                f"namespace per model — rename one side in the dbt project."
            )

        ref_name = sm.model or sm.name

        sql_source: Optional[str] = None
        sql_table: Optional[str] = None
        if ref_name in self._regular_models_sql:
            resolved, warnings = resolve_refs(
                self._regular_models_sql[ref_name],
                self._regular_models_sql,
            )
            sql_source = resolved
            for message in warnings:
                self._warnings.append(ConversionWarning(
                    model_name=sm.name,
                    category="sql_inline",
                    severity="info",
                    message=message,
                ))
        else:
            sql_table = ref_name

        default_time_dim = None
        if sm.defaults and sm.defaults.agg_time_dimension:
            default_time_dim = sm.defaults.agg_time_dimension

        # DEV-1595: accumulate model-level meta (config.meta + label + any
        # clean-fail raw stashes added during measure conversion).
        model_meta: Dict[str, Any] = {}
        cfg_meta = _meta_of(sm.config)
        if cfg_meta:
            model_meta.update(cfg_meta)
        if sm.label:
            model_meta.setdefault("label", sm.label)

        cols: List[Column] = [_convert_dimension(d) for d in sm.dimensions]

        # Add primary key column for primary/unique entities.
        entity_col_names = {c.name for c in cols}
        for entity in sm.entities:
            if entity.type in ("primary", "unique"):
                col_name = entity.expr or entity.name
                if col_name not in entity_col_names:
                    cols.append(Column(
                        name=col_name,
                        type=DataType.DOUBLE,
                        primary_key=True,
                        description=entity.description,
                        label=entity.label,
                        meta=self._entity_meta(entity),
                    ))
                    entity_col_names.add(col_name)
                else:
                    for c in cols:
                        if c.name == col_name:
                            c.primary_key = True

        if sm.primary_entity:
            pe_name = sm.primary_entity
            pe_expr = pe_name
            for e in sm.entities:
                if e.name == pe_name:
                    pe_expr = e.expr or e.name
                    break
            if pe_expr not in entity_col_names:
                cols.append(Column(
                    name=pe_expr,
                    type=DataType.DOUBLE,
                    primary_key=True,
                ))
                entity_col_names.add(pe_expr)

        measure_cols, measures = self._convert_measures(
            dbt_measures=sm.measures,
            sm_name=sm.name,
            existing_column_names={c.name for c in cols},
            model_meta=model_meta,
        )
        cols.extend(measure_cols)

        joins = self.entity_registry.resolve_joins_for_model(sm)

        return SlayerModel(
            name=sm.name,
            sql_table=sql_table,
            sql=sql_source,
            data_source=self.data_source,
            description=sm.description,
            default_time_dimension=default_time_dim,
            columns=cols,
            measures=measures,
            joins=joins,
            meta=model_meta or None,
        )

    @staticmethod
    def _entity_meta(entity) -> Optional[Dict[str, Any]]:
        """Build a PK-column meta blob from an entity's config.meta + role."""
        meta = _meta_of(getattr(entity, "config", None)) or {}
        role = getattr(entity, "role", None)
        if role:
            meta.setdefault("role", role)
        return meta or None

    # ── Measure conversion ────────────────────────────────────────────

    def _convert_measures(
        self,
        dbt_measures: List[DbtMeasure],
        *,
        sm_name: str,
        existing_column_names: set,
        model_meta: Dict[str, Any],
    ) -> Tuple[List[Column], List[ModelMeasure]]:
        """Convert dbt measures into a (Columns, ModelMeasures) pair.

        Each unique measure expression yields a single ``Column``; each dbt
        measure yields one ``ModelMeasure`` whose formula is ``<col>:<agg>``.
        Special handling (DEV-1595):

        * ``sum_boolean`` → a dedicated ``CASE WHEN (<expr>) THEN 1 ELSE 0 END``
          ``INT`` column aggregated with ``:sum`` (cross-DB safe; null bool → 0).
        * ``percentile`` → ``:percentile(p=<value>)``; clean-fails when the
          value is absent or discrete/approximate flags are set.
        * ``non_additive_dimension`` (semi-additive) → clean-fail.
        """
        measure_names = {m.name for m in dbt_measures}
        columns: List[Column] = []
        measures: List[ModelMeasure] = []
        used_column_names = set(existing_column_names)

        def _alloc(base: str) -> str:
            col_name = base
            while col_name in measure_names or col_name in used_column_names:
                col_name = f"{col_name}_col"
            used_column_names.add(col_name)
            return col_name

        groups: Dict[str, List[DbtMeasure]] = defaultdict(list)
        for m in dbt_measures:
            if m.non_additive_dimension is not None:
                self._fail_measure(
                    m, sm_name, model_meta,
                    category="non_additive_dimension", severity="dropped",
                    message=(
                        f"Measure '{m.name}' uses a non_additive_dimension "
                        f"(semi-additive aggregation), which is not exactly expressible."
                    ),
                    suggestion=(
                        "Express as balance:last(<time_col>) / first(...) "
                        "or a multi-stage query."
                    ),
                    raw={"non_additive_dimension": m.non_additive_dimension.model_dump()},
                )
                continue
            if m.agg.lower() == "sum_boolean":
                expr = m.expr or m.name
                col_name = _alloc(f"{m.name}_col")
                columns.append(Column(
                    name=col_name,
                    sql=f"CASE WHEN ({expr}) THEN 1 ELSE 0 END",
                    type=DataType.INT,
                    meta=_meta_of(m.config),
                ))
                self._emit_model_measure(measures, m, f"{col_name}:sum", sm_name)
                continue
            groups[m.expr or m.name].append(m)

        for expr_key, group in groups.items():
            if _is_simple_identifier(expr_key):
                base_name = expr_key
            else:
                base_name = f"{group[0].name}_col"
            col_name = _alloc(base_name)
            sql = expr_key if expr_key != col_name else None
            columns.append(Column(
                name=col_name,
                sql=sql,
                type=DataType.DOUBLE,
                format=_FLOAT_FORMAT,
            ))
            for m in group:
                formula = self._measure_formula(m, col_name, sm_name, model_meta)
                if formula is None:
                    continue
                self._emit_model_measure(measures, m, formula, sm_name)

        return columns, measures

    def _measure_formula(
        self, m: DbtMeasure, col_name: str, sm_name: str, model_meta: Dict[str, Any]
    ) -> Optional[str]:
        """Build the ``<col>:<agg>`` formula for a dbt measure, or ``None`` if
        it clean-fails (percentile without a value / discrete-approx flags)."""
        mapped = _map_agg(m.agg)
        if mapped == "percentile":
            ap = m.agg_params
            if ap is None or ap.percentile is None:
                self._fail_measure(
                    m, sm_name, model_meta,
                    category="percentile", severity="dropped",
                    message=f"Measure '{m.name}' is a percentile aggregation with no percentile value.",
                    suggestion="Set agg_params.percentile (continuous, in [0, 1]).",
                    raw={"agg": m.agg, "agg_params": ap.model_dump() if ap else None},
                )
                return None
            if ap.use_discrete_percentile or ap.use_approximate_percentile:
                self._fail_measure(
                    m, sm_name, model_meta,
                    category="percentile", severity="dropped",
                    message=(
                        f"Measure '{m.name}' uses discrete/approximate percentile; "
                        f"only continuous-exact PERCENTILE_CONT is supported."
                    ),
                    suggestion="Remove use_discrete_percentile / use_approximate_percentile.",
                    raw={"agg_params": ap.model_dump()},
                )
                return None
            self._maybe_dialect_caveat(m.name, "percentile")
            return f"{col_name}:percentile(p={ap.percentile})"
        if mapped == "median":
            self._maybe_dialect_caveat(m.name, "median")
        return f"{col_name}:{mapped}"

    def _emit_model_measure(
        self, measures: List[ModelMeasure], m: DbtMeasure, formula: str, sm_name: str
    ) -> None:
        """Append a ``ModelMeasure`` for a dbt measure, routing transform-name
        collisions to the report instead of raising."""
        try:
            measures.append(ModelMeasure(
                name=m.name,
                formula=formula,
                label=m.label,
                description=m.description,
                meta=_meta_of(m.config),
            ))
        except ValueError as exc:
            self._unconverted.append(ConversionWarning(
                model_name=sm_name,
                metric_name=m.name,
                category="measure",
                severity="unconverted",
                message=(
                    f"dbt measure '{m.name}' could not be converted to a "
                    f"ModelMeasure: {exc}"
                ),
            ))

    def _maybe_dialect_caveat(self, name: str, agg: str) -> None:
        """Emit an info caveat when the target dialect lacks ``agg``."""
        if self.target_dialect and self.target_dialect.lower() in _NO_PERCENTILE_DIALECTS:
            self._warnings.append(ConversionWarning(
                metric_name=name,
                category="percentile",
                severity="info",
                message=(
                    f"'{name}' uses {agg}, which is not supported on dialect "
                    f"'{self.target_dialect}'; the measure imports but will fail "
                    f"at query time."
                ),
                suggestion="Query on a dialect with native percentile/median support.",
            ))

    # ── Clean-fail helpers ─────────────────────────────────────────────

    def _fail_measure(
        self,
        m: DbtMeasure,
        sm_name: str,
        model_meta: Dict[str, Any],
        *,
        category: str,
        severity: Literal["unconverted", "dropped", "info"],
        message: str,
        suggestion: Optional[str] = None,
        raw: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Route a measure-level clean-fail to the report + stash raw into meta."""
        self._unconverted.append(ConversionWarning(
            model_name=sm_name,
            metric_name=m.name,
            category=category,
            severity=severity,
            message=message,
            suggestion=suggestion,
        ))
        if raw is not None:
            self._stash_meta(model_meta, m.name, category, raw)

    def _fail_metric(
        self,
        metric: DbtMetric,
        *,
        category: str,
        severity: Literal["unconverted", "dropped", "info"],
        message: str,
        suggestion: Optional[str] = None,
        raw: Optional[Dict[str, Any]] = None,
        model_name: Optional[str] = None,
    ) -> None:
        """Route a metric-level clean-fail to the report + best-effort meta stash."""
        self._unconverted.append(ConversionWarning(
            model_name=model_name,
            metric_name=metric.name,
            category=category,
            severity=severity,
            message=message,
            suggestion=suggestion,
        ))
        if raw is not None:
            src = self._find_metric_source_model(metric)
            slayer_model = self._models_by_name.get(src) if src else None
            if slayer_model is not None:
                if slayer_model.meta is None:
                    slayer_model.meta = {}
                self._stash_meta(slayer_model.meta, metric.name, category, raw)

    @staticmethod
    def _stash_meta(meta_dict: Dict[str, Any], name: str, category: str, raw: Any) -> None:
        """Append a raw dropped construct to ``meta['dbt_unconverted']``."""
        bucket = meta_dict.setdefault("dbt_unconverted", [])
        bucket.append({"name": name, "category": category, "raw": raw})

    # ── Metric conversion ─────────────────────────────────────────────

    def _convert_metric(self, metric: DbtMetric) -> None:
        """Route a dbt metric to the appropriate handler.

        All handlers fold their output into a ``ModelMeasure`` on the source
        semantic model (or route to the report on failure). No ``SlayerQuery``
        is produced.
        """
        metric_type = metric.type.lower()

        if metric_type == "simple":
            self._convert_simple_metric(metric)
        elif metric_type == "derived":
            self._convert_derived_metric(metric)
        elif metric_type == "ratio":
            self._convert_ratio_metric(metric)
        elif metric_type == "cumulative":
            self._convert_cumulative_metric(metric)
        elif metric_type == "conversion":
            self._fail_metric(
                metric,
                category="conversion_metric",
                severity="dropped",
                message=f"Conversion metric '{metric.name}' (funnel) is not supported in SLayer.",
                suggestion="Express the funnel as a multi-stage query.",
                raw={"type": metric.type},
            )
        else:
            self._fail_metric(
                metric,
                category="unknown_metric_type",
                severity="unconverted",
                message=f"Unknown metric type '{metric.type}' for metric '{metric.name}'.",
            )

    def _add_model_measure(
        self,
        *,
        slayer_model: SlayerModel,
        metric: DbtMetric,
        formula: str,
    ) -> None:
        """Append a ``ModelMeasure`` to ``slayer_model``.

        Routes transform-name collisions to ``unconverted_metrics`` instead of
        raising. Skips with a warning if the name collides with an existing
        column or measure on the model.
        """
        if self._metric_name_collides(metric.name, slayer_model):
            return
        try:
            slayer_model.measures.append(ModelMeasure(
                name=metric.name,
                formula=formula,
                label=metric.label,
                description=metric.description,
                meta=_meta_of(metric.config),
            ))
        except ValueError as exc:
            self._unconverted.append(ConversionWarning(
                model_name=slayer_model.name,
                metric_name=metric.name,
                category="metric",
                severity="unconverted",
                message=(
                    f"Metric '{metric.name}' could not be converted to a "
                    f"ModelMeasure: {exc}"
                ),
            ))

    def _metric_name_collides(self, name: str, slayer_model: SlayerModel) -> bool:
        """Whether ``name`` collides with a column/measure on ``slayer_model``.
        Emits a collision warning when it does."""
        existing_names = {c.name for c in slayer_model.columns}
        existing_names.update(m.name for m in slayer_model.measures if m.name is not None)
        if name in existing_names:
            self._warnings.append(ConversionWarning(
                model_name=slayer_model.name,
                metric_name=name,
                category="collision",
                severity="info",
                message=(
                    f"Metric '{name}' collides with an existing column or "
                    f"measure on model '{slayer_model.name}'. Skipped."
                ),
            ))
            return True
        return False

    def _simple_metric_unsupported(
        self, metric: DbtMetric, tp: Optional[DbtMetricTypeParams]
    ) -> bool:
        """Route the unsupported simple-metric shapes (measure-less aggregation
        via ``metric_aggregation_params``, time-spine gap filling) to the
        report; return ``True`` when one fired."""
        if tp is None:
            return False
        if tp.metric_aggregation_params is not None:
            self._fail_metric(
                metric,
                category="measure_less_metric",
                severity="dropped",
                message=(
                    f"Simple metric '{metric.name}' uses metric_aggregation_params "
                    f"(a measure-less aggregation), an unsupported shape."
                ),
                suggestion="Define an explicit measure on the semantic model.",
                raw={"metric_aggregation_params": tp.metric_aggregation_params.model_dump()},
            )
            return True
        mref = tp.measure
        if mref and (mref.join_to_timespine or mref.fill_nulls_with is not None):
            self._fail_metric(
                metric,
                category="timespine_gap_fill",
                severity="dropped",
                message=(
                    f"Metric '{metric.name}' uses join_to_timespine / fill_nulls_with; "
                    f"SLayer has no time-spine gap filling."
                ),
                suggestion="Remove join_to_timespine / fill_nulls_with.",
                raw={"join_to_timespine": mref.join_to_timespine,
                     "fill_nulls_with": mref.fill_nulls_with},
            )
            return True
        return False

    def _convert_simple_metric(self, metric: DbtMetric) -> None:
        """A simple metric is a (filtered) re-aggregation of a single measure.

        Without a filter: nothing to do — the underlying measure is already
        addressable as a ModelMeasure. With a filter: push it down into a leaf
        Column carrying the CASE-WHEN ``filter`` and reference it from a
        ModelMeasure.
        """
        tp = metric.type_params

        if self._simple_metric_unsupported(metric, tp):
            return

        measure_name = tp.measure_name if tp else None
        if not measure_name:
            self._fail_metric(
                metric,
                category="simple_metric",
                severity="unconverted",
                message=f"Simple metric '{metric.name}' has no measure reference.",
            )
            return

        mref = tp.measure if tp else None
        raw_filter = self._combine_filters(metric.filter, mref.filter if mref else None)
        if not raw_filter:
            return  # unfiltered simple metric — the measure is already addressable.

        source_sm = self._find_measure_model(measure_name)
        if source_sm is None:
            self._fail_metric(
                metric,
                category="simple_metric",
                severity="unconverted",
                message=f"Cannot find measure '{measure_name}' in any semantic model.",
            )
            return

        ok, reason = self._filter_reachable(raw_filter, source_sm)
        if not ok:
            self._fail_metric(
                metric,
                category="cross_model_filter",
                severity="dropped",
                message=f"Metric '{metric.name}': {reason}.",
                suggestion=_JOIN_REACHABILITY_SUGGESTION,
            )
            return

        dbt_measure = next((m for m in source_sm.measures if m.name == measure_name), None)
        if dbt_measure is None:
            return

        slayer_model = self._models_by_name.get(source_sm.name)
        if slayer_model is None:
            return

        if self._metric_name_collides(metric.name, slayer_model):
            return

        leaf_ref = self._filtered_leaf_ref(
            metric=metric,
            slayer_model=slayer_model,
            source_sm=source_sm,
            dbt_measure=dbt_measure,
            raw_filter=raw_filter,
        )
        if leaf_ref is None:
            return  # clean-failed (e.g. filtered percentile without a value)
        self._add_model_measure(slayer_model=slayer_model, metric=metric, formula=leaf_ref)

    def _convert_derived_metric(self, metric: DbtMetric) -> None:
        """A derived metric expresses a formula over other metrics/measures.

        Input references are substituted in the ``expr``; an ``offset_window``
        on a single-aggregate input is lowered to a ``time_shift`` call
        (DEV-1595). Inexpressible shapes (offset_to_grain, offset on a
        multi-aggregate input, custom granularity, metric-level filter on a
        derived expr) clean-fail.
        """
        tp = metric.type_params
        if not tp:
            return

        expr = tp.expr
        if not expr:
            self._fail_metric(
                metric, category="derived_metric", severity="unconverted",
                message=f"Derived metric '{metric.name}' has no expr.",
            )
            return

        if metric.filter:
            self._fail_metric(
                metric, category="filter_pushdown", severity="dropped",
                message=(
                    f"Derived metric '{metric.name}' has a metric-level filter; "
                    f"leaf push-down across a derived expression is not supported."
                ),
                suggestion="Push the filter into the input metrics instead.",
            )
            return

        formula, clean_failed = self._substitute_derived_inputs(metric, tp, expr)
        if clean_failed:
            return

        source_model_name = self._find_metric_source_model(metric)
        if source_model_name is None:
            self._fail_metric(
                metric, category="derived_metric", severity="unconverted",
                message=f"Could not determine source model for derived metric '{metric.name}'.",
            )
            return

        slayer_model = self._models_by_name.get(source_model_name)
        if slayer_model is None:
            self._fail_metric(
                metric, category="derived_metric", severity="unconverted",
                message=(
                    f"Source model '{source_model_name}' for derived metric "
                    f"'{metric.name}' was not converted."
                ),
            )
            return

        self._add_model_measure(slayer_model=slayer_model, metric=metric, formula=formula)

    def _substitute_derived_inputs(
        self, metric: DbtMetric, tp: DbtMetricTypeParams, expr: str
    ) -> Tuple[str, bool]:
        """Substitute each derived input ref in ``expr`` with its resolved form.

        Returns ``(formula, clean_failed)``; ``clean_failed=True`` means an
        input routed a report entry and the metric should be abandoned.
        """
        formula = expr
        for m_input in tp.metrics or []:
            ref_name = m_input.alias or m_input.name
            replacement, clean_failed = self._derived_input_replacement(metric, m_input)
            if clean_failed:
                return formula, True
            if replacement and replacement != ref_name:
                formula = re.sub(
                    rf"\b{re.escape(ref_name)}\b",
                    replacement.replace("\\", r"\\"),
                    formula,
                )
        return formula, False

    def _offset_window_ref(self, metric: DbtMetric, m_input: DbtMetricInput) -> Optional[str]:
        """Lower an ``offset_window`` input to ``time_shift(<input>, -N, '<gran>')``.

        Only when the input resolves to a single aggregate (measure / simple
        metric) and the granularity is standard; otherwise clean-fail.
        """
        window = DbtMetricTimeWindow.parse(m_input.offset_window)
        if window is None:
            self._fail_metric(
                metric, category="offset_window", severity="dropped",
                message=(
                    f"Derived metric '{metric.name}': could not parse offset_window "
                    f"'{m_input.offset_window}'."
                ),
            )
            return None

        gran = window.granularity.lower()
        if gran not in _STANDARD_GRAINS:
            self._fail_metric(
                metric, category="custom_granularity", severity="dropped",
                message=(
                    f"Derived metric '{metric.name}': offset_window uses non-standard "
                    f"granularity '{window.granularity}'."
                ),
                suggestion="Use a standard granularity (day/week/month/quarter/year).",
            )
            return None

        leaf = self._resolve_input_to_leaf(m_input.name)
        if leaf is None:
            self._fail_metric(
                metric, category="offset_window", severity="dropped",
                message=(
                    f"Derived metric '{metric.name}': offset_window on input "
                    f"'{m_input.name}', which is not a single aggregate "
                    f"(measure / simple metric); not exactly expressible."
                ),
                suggestion="Offset only single-aggregate inputs.",
            )
            return None

        resolved_input = self._resolve_metric_to_name(m_input.name)
        if not resolved_input:
            _, dbt_measure = leaf
            resolved_input = dbt_measure.name
        return f"time_shift({resolved_input}, -{window.count}, '{gran}')"

    def _derived_input_replacement(
        self, metric: DbtMetric, m_input: DbtMetricInput
    ) -> Tuple[Optional[str], bool]:
        """Resolve one derived-metric input to its formula replacement.

        Returns ``(replacement, clean_failed)``. ``clean_failed=True`` means a
        report entry was emitted and the whole metric should be abandoned;
        ``replacement is None`` with ``clean_failed=False`` means no
        substitution is needed (the input ref already matches its name).
        Handles ``offset_to_grain`` (clean-fail), ``offset_window`` →
        ``time_shift``, per-input ``filter`` → leaf push-down, and the
        unsupported offset+filter combination (clean-fail).
        """
        ref_name = m_input.alias or m_input.name
        if m_input.offset_to_grain is not None:
            self._fail_metric(
                metric, category="offset_to_grain", severity="dropped",
                message=(
                    f"Derived metric '{metric.name}': input '{ref_name}' uses "
                    f"offset_to_grain; SLayer has no truncate-to-grain shift."
                ),
                suggestion="Use cumsum(...) and put the grain dimension in the query.",
            )
            return None, True

        has_offset = m_input.offset_window is not None
        has_filter = bool(m_input.filter)
        if has_offset and has_filter:
            self._fail_metric(
                metric, category="filter_pushdown", severity="dropped",
                message=(
                    f"Derived metric '{metric.name}': input '{ref_name}' combines "
                    f"offset_window with a per-input filter; not exactly expressible."
                ),
                suggestion="Split the offset and the filter into separate inputs.",
            )
            return None, True
        if has_offset:
            rep = self._offset_window_ref(metric, m_input)
            return rep, rep is None
        if has_filter:
            rep = self._derived_filtered_input_ref(metric, m_input)
            return rep, rep is None
        # A plain reference to an input metric that was itself clean-failed (and
        # so never materialized) leaves a dangling bare name in the formula; the
        # post-conversion _prune_dangling_measures pass catches and reports it.
        return self._resolve_metric_to_name(m_input.name), False

    def _derived_filtered_input_ref(
        self, metric: DbtMetric, m_input: DbtMetricInput
    ) -> Optional[str]:
        """Push a derived input's per-input filter into its single-aggregate
        leaf, returning the filtered colon-form ref (or ``None`` on clean-fail).
        """
        leaf = self._resolve_input_to_leaf_filtered(m_input.name)
        if leaf is None:
            self._fail_metric(
                metric, category="filter_pushdown", severity="dropped",
                message=(
                    f"Derived metric '{metric.name}': input '{m_input.name}' carries a "
                    f"filter but is not a single aggregate; not exactly expressible."
                ),
                suggestion="Filter a simple-aggregate input, or use a multi-stage model.",
            )
            return None
        source_sm, dbt_measure, chain_filter = leaf
        # Intersect the input's filter with any filter the referenced simple
        # metric already carries, so the referenced metric's filter isn't lost.
        raw_filter = self._combine_filters(chain_filter, m_input.filter)
        ok, reason = self._filter_reachable(raw_filter, source_sm)
        if not ok:
            self._fail_metric(
                metric, category="cross_model_filter", severity="dropped",
                message=f"Derived metric '{metric.name}': {reason}.",
                suggestion=_JOIN_REACHABILITY_SUGGESTION,
            )
            return None
        slayer_model = self._models_by_name.get(source_sm.name)
        if slayer_model is None:
            return None
        return self._filtered_leaf_ref(
            metric=metric,
            slayer_model=slayer_model,
            source_sm=source_sm,
            dbt_measure=dbt_measure,
            raw_filter=raw_filter,
        )

    def _convert_ratio_metric(self, metric: DbtMetric) -> None:
        """A ratio metric is numerator / denominator over two measures/metrics.

        The denominator is NULL-guarded (``nullif(den, 0)``). Metric-level and
        per-input filters push down independently into each leaf (DEV-1595).
        """
        tp = metric.type_params
        if not tp:
            return

        num = tp.numerator
        den = tp.denominator
        if not num or not den:
            self._fail_metric(
                metric, category="ratio_metric", severity="unconverted",
                message=f"Ratio metric '{metric.name}' missing numerator or denominator.",
            )
            return

        source_model_name = self._find_metric_source_model(metric)
        if source_model_name is None:
            self._fail_metric(
                metric, category="ratio_metric", severity="unconverted",
                message=f"Could not determine source model for ratio metric '{metric.name}'.",
            )
            return

        slayer_model = self._models_by_name.get(source_model_name)
        if slayer_model is None:
            self._fail_metric(
                metric, category="ratio_metric", severity="unconverted",
                message=(
                    f"Source model '{source_model_name}' for ratio metric "
                    f"'{metric.name}' was not converted."
                ),
            )
            return

        num_ref = self._ratio_side_ref(metric, num, slayer_model)
        if num_ref is None:
            return
        den_ref = self._ratio_side_ref(metric, den, slayer_model)
        if den_ref is None:
            return

        self._add_model_measure(
            slayer_model=slayer_model,
            metric=metric,
            formula=f"{num_ref} / nullif({den_ref}, 0)",
        )

    def _ratio_side_ref(
        self, metric: DbtMetric, side: DbtMetricInput, slayer_model: SlayerModel
    ) -> Optional[str]:
        """Resolve one ratio side to a formula reference, pushing down the
        combined (metric-level + per-input) filter into a leaf when present."""
        raw_filter = self._combine_filters(metric.filter, side.filter)
        if not raw_filter:
            # A ratio side referencing a clean-failed (non-materialized) metric
            # leaves a dangling formula name that _prune_dangling_measures drops.
            return self._resolve_metric_to_name(side.name) or side.name

        leaf = self._resolve_input_to_leaf_filtered(side.name)
        if leaf is None:
            self._fail_metric(
                metric, category="filter_pushdown", severity="dropped",
                message=(
                    f"Ratio metric '{metric.name}': input '{side.name}' is a "
                    f"ratio/derived (multi-aggregate) metric carrying a filter; "
                    f"filtering after metric calculation is not exactly expressible."
                ),
                suggestion="Restructure as a multi-stage source_queries model.",
            )
            return None

        source_sm, dbt_measure, chain_filter = leaf
        # Intersect with any filter the referenced simple metric already carries
        # so it isn't silently dropped.
        raw_filter = self._combine_filters(chain_filter, raw_filter)
        ok, reason = self._filter_reachable(raw_filter, source_sm)
        if not ok:
            self._fail_metric(
                metric, category="cross_model_filter", severity="dropped",
                message=f"Ratio metric '{metric.name}': {reason}.",
                suggestion=_JOIN_REACHABILITY_SUGGESTION,
            )
            return None

        return self._filtered_leaf_ref(
            metric=metric,
            slayer_model=slayer_model,
            source_sm=source_sm,
            dbt_measure=dbt_measure,
            raw_filter=raw_filter,
        )

    def _convert_cumulative_metric(self, metric: DbtMetric) -> None:
        """A cumulative metric is a running total of one underlying measure.

        Only the *unbounded* form (``period_agg=first``, no window, no
        grain_to_date) maps to ``cumsum(measure)``. Windowed / grain-to-date /
        non-default-period-agg variants are query-grain-dependent and clean-fail.
        """
        tp = metric.type_params
        if not tp:
            self._fail_metric(
                metric, category="cumulative_metric", severity="unconverted",
                message=f"Cumulative metric '{metric.name}' has no type_params.",
            )
            return

        ctp = tp.cumulative_type_params
        window = tp.window or (ctp.window if ctp else None)
        grain_to_date = tp.grain_to_date or (ctp.grain_to_date if ctp else None)
        period_agg = ctp.period_agg if ctp else None
        measure_name = tp.measure_name or (ctp.measure if ctp else None)

        if self._cumulative_clean_fail(
            metric, window=window, grain_to_date=grain_to_date, period_agg=period_agg
        ):
            return

        if not measure_name:
            self._fail_metric(
                metric, category="cumulative_metric", severity="unconverted",
                message=f"Cumulative metric '{metric.name}' has no measure reference.",
            )
            return

        measure_ref = self._resolve_measure_to_name(measure_name)
        if not measure_ref:
            self._fail_metric(
                metric, category="cumulative_metric", severity="unconverted",
                message=(
                    f"Cumulative metric '{metric.name}' references unknown measure "
                    f"'{measure_name}'."
                ),
            )
            return

        slayer_model = self._cumulative_source_model(metric, measure_name)
        if slayer_model is None:
            return

        self._add_model_measure(
            slayer_model=slayer_model,
            metric=metric,
            formula=f"cumsum({measure_ref})",
        )

    def _cumulative_source_model(
        self, metric: DbtMetric, measure_name: str
    ) -> Optional[SlayerModel]:
        """Resolve the SlayerModel a cumulative metric folds into, routing the
        not-found / not-converted cases to the report (returns ``None``)."""
        source_model_name = self._find_metric_source_model(metric)
        if source_model_name is None:
            # measure lives on exactly one model — fall back to that.
            sm = self._find_measure_model(measure_name)
            source_model_name = sm.name if sm else None
        if source_model_name is None:
            self._fail_metric(
                metric, category="cumulative_metric", severity="unconverted",
                message=f"Could not determine source model for cumulative metric '{metric.name}'.",
            )
            return None
        slayer_model = self._models_by_name.get(source_model_name)
        if slayer_model is None:
            self._fail_metric(
                metric, category="cumulative_metric", severity="unconverted",
                message=(
                    f"Source model '{source_model_name}' for cumulative metric "
                    f"'{metric.name}' was not converted."
                ),
            )
            return None
        return slayer_model

    def _cumulative_clean_fail(
        self,
        metric: DbtMetric,
        *,
        window: Optional[DbtMetricTimeWindow],
        grain_to_date: Optional[str],
        period_agg: Optional[str],
    ) -> bool:
        """Route the query-grain-dependent cumulative variants to the report.

        Returns ``True`` (and emits a report entry) for a rolling window,
        grain-to-date reset, or non-default ``period_agg``; ``False`` when the
        cumulative is the unbounded form SLayer maps to ``cumsum(measure)``.
        """
        if window is not None:
            self._fail_metric(
                metric, category="windowed_cumulative", severity="dropped",
                message=(
                    f"Cumulative metric '{metric.name}' has a rolling window; a windowed "
                    f"running total with period re-aggregation is not exactly expressible."
                ),
                suggestion="Use cumsum(measure) for an unbounded running total.",
                raw={"window": window.model_dump()},
            )
            return True
        if grain_to_date is not None:
            self._fail_metric(
                metric, category="grain_to_date_cumulative", severity="dropped",
                message=(
                    f"Cumulative metric '{metric.name}' uses grain_to_date; reset-at-grain "
                    f"can't bake into a saved measure (it is query-grain-dependent)."
                ),
                suggestion="Use cumsum(measure) and put the grain dimension in the query.",
                raw={"grain_to_date": grain_to_date},
            )
            return True
        if period_agg is not None and period_agg.lower() != "first":
            self._fail_metric(
                metric, category="period_agg", severity="dropped",
                message=(
                    f"Cumulative metric '{metric.name}' uses period_agg='{period_agg}'; "
                    f"only the default (first) running total is exactly expressible."
                ),
                suggestion="Use the default period_agg (first) for cumsum(measure).",
                raw={"period_agg": period_agg},
            )
            return True
        return False

    # ── Filter push-down helpers ───────────────────────────────────────

    @staticmethod
    def _combine_filters(a: Optional[str], b: Optional[str]) -> Optional[str]:
        """AND-join two raw dbt-Jinja filter strings (each parenthesised)."""
        parts = [p for p in (a, b) if p]
        if not parts:
            return None
        if len(parts) == 1:
            return parts[0]
        return " AND ".join(f"({p})" for p in parts)

    def _convert_filter(self, raw: str, source_sm: DbtSemanticModel) -> str:
        """Convert a raw dbt-Jinja filter to a SLayer filter string."""
        model_entities = {e.name: e.type for e in source_sm.entities}
        sm_by_name = {sm.name: sm for sm in self.project.semantic_models}
        return convert_dbt_filter(
            filter_str=raw,
            source_model_name=source_sm.name,
            entity_registry=self.entity_registry,
            model_entity_names=model_entities,
            all_semantic_models=sm_by_name,
        )

    def _filter_reachable(
        self, raw: str, source_sm: DbtSemanticModel
    ) -> Tuple[bool, Optional[str]]:
        """Whether every ``Dimension('entity__dim')`` in ``raw`` resolves to a
        filter SLayer can actually emit from ``source_sm``.

        ``convert_dbt_filter`` lowers ``Dimension('entity__dim')`` to a
        **one-hop** ``<entity_owner_model>.<dim>`` reference, which only
        resolves when that model is **directly** joined to the source model —
        i.e. the entity is declared on the source model itself. A multi-hop
        filter (e.g. ``orders → customers → regions``) would need the full
        ``customers__regions.dim`` join path, which the dbt filter converter
        cannot produce, so it is clean-failed rather than emitted as a broken
        one-hop path. (Full multi-hop cross-model filter support is tracked
        separately — DEV-1445.)

        A foreign entity declared on the source but with **no joinable owner
        model** is also clean-failed: ``convert_dbt_filter`` would fall back to
        a bare ``dim`` name that doesn't exist on the source table. (Whether a
        reachable model actually has the column isn't verified here — undeclared
        dimensions are legitimately bare table columns, and column existence is
        a query-time schema-drift concern, consistent with the rest of the
        converter.)
        """
        entity_types = {e.name: e.type for e in source_sm.entities}
        for mm in _DIMENSION_RE.finditer(raw):
            ok, reason = self._entity_filter_reachable(
                mm.group(1), mm.group(2), source_sm, entity_types
            )
            if not ok:
                return False, reason
        return True, None

    def _entity_filter_reachable(
        self,
        entity_name: str,
        dim_name: str,
        source_sm: DbtSemanticModel,
        entity_types: Dict[str, str],
    ) -> Tuple[bool, Optional[str]]:
        """Reachability decision for a single ``entity__dim`` filter token."""
        if entity_name == source_sm.name:
            return True, None  # the source table's own column
        etype = entity_types.get(entity_name)
        if etype in ("primary", "unique") or entity_name == source_sm.primary_entity:
            return True, None  # local primary/unique → bare dim on the source table
        if etype == "foreign":
            owners = sorted({
                m for m, _expr in self.entity_registry._primaries.get(entity_name, [])
                if m != source_sm.name
            })
            if not owners:
                return False, (
                    f"filter dimension '{entity_name}__{dim_name}' references entity "
                    f"'{entity_name}', which has no joinable owner model"
                )
            if len(owners) > 1:
                # ``convert_dbt_filter`` qualifies the filter to a single owner
                # (the lexicographically first), so a multi-owner entity would
                # be lowered to a possibly-wrong model. Clean-fail rather than
                # emit an ambiguously-qualified filter.
                return False, (
                    f"filter dimension '{entity_name}__{dim_name}' references entity "
                    f"'{entity_name}', which is owned by multiple models {owners}; "
                    f"the filter cannot be unambiguously qualified to one join"
                )
            return True, None  # one-hop join → <owner>.<dim>
        return False, (
            f"filter dimension '{entity_name}__{dim_name}' is not reachable from "
            f"model '{source_sm.name}' via a direct join (multi-hop cross-model "
            f"filters are not exactly expressible)"
        )

    def _filtered_leaf_ref(
        self,
        *,
        metric: DbtMetric,
        slayer_model: SlayerModel,
        source_sm: DbtSemanticModel,
        dbt_measure: DbtMeasure,
        raw_filter: str,
    ) -> Optional[str]:
        """Get-or-create the filtered leaf Column for ``(model, expr, filter)``
        and return the colon-form formula referencing it (or ``None`` on a
        clean-fail that has already been routed to the report).

        Dedup key is ``(model, column_expr, normalized_filter)`` — the
        aggregation lives on the formula, so multiple aggregations over the
        same filtered column share one Column. Special measure forms are
        preserved exactly: ``sum_boolean`` builds the CASE-WHEN INT column and
        aggregates with ``:sum``; ``percentile`` keeps its ``p=`` argument
        (and clean-fails on a missing value / discrete / approximate flags).
        """
        leaf = self._filtered_leaf_spec(metric, dbt_measure)
        if leaf is None:
            return None  # clean-failed inside _filtered_leaf_spec
        column_expr, col_type, col_format, agg_call = leaf

        slayer_filter = self._convert_filter(raw_filter, source_sm)
        key = (slayer_model.name, column_expr, slayer_filter)
        col_name = self._filtered_columns.get(key)
        if col_name is None:
            col_name = self._alloc_column_name(slayer_model, f"{dbt_measure.name}_filtered")
            slayer_model.columns.append(Column(
                name=col_name,
                sql=column_expr,
                type=col_type,
                format=col_format,
                filter=slayer_filter,
            ))
            self._filtered_columns[key] = col_name
        return f"{col_name}:{agg_call}"

    def _filtered_leaf_spec(
        self, metric: DbtMetric, dbt_measure: DbtMeasure
    ) -> Optional[Tuple[str, DataType, Optional[NumberFormat], str]]:
        """Compute ``(column_expr, type, format, agg_call)`` for a filtered
        leaf, preserving special measure semantics; ``None`` on clean-fail."""
        if dbt_measure.non_additive_dimension is not None:
            # A semi-additive measure can't be lowered to a plain filtered
            # aggregate — that would drop the non-additive semantics. This is the
            # choke point for both the simple-metric-filter path and the
            # derived/ratio push-down path.
            self._fail_metric(
                metric, category="non_additive_dimension", severity="dropped",
                message=(
                    f"Filtered metric '{metric.name}' wraps a non-additive "
                    f"(semi-additive) measure '{dbt_measure.name}', which is not "
                    f"exactly expressible as a filtered aggregate."
                ),
                suggestion=(
                    "Express as balance:last(<time_col>) / first(...) or a "
                    "multi-stage query."
                ),
                raw={"non_additive_dimension": dbt_measure.non_additive_dimension.model_dump()},
            )
            return None
        agg = dbt_measure.agg.lower()
        if agg == "sum_boolean":
            expr = dbt_measure.expr or dbt_measure.name
            return f"CASE WHEN ({expr}) THEN 1 ELSE 0 END", DataType.INT, None, "sum"

        column_expr = (
            dbt_measure.expr
            if (dbt_measure.expr and dbt_measure.expr != dbt_measure.name)
            else dbt_measure.name
        )
        mapped = _map_agg(dbt_measure.agg)
        if mapped == "percentile":
            ap = dbt_measure.agg_params
            if ap is None or ap.percentile is None or ap.use_discrete_percentile or ap.use_approximate_percentile:
                self._fail_metric(
                    metric, category="percentile", severity="dropped",
                    message=(
                        f"Filtered metric '{metric.name}' wraps a percentile measure "
                        f"with no usable continuous percentile value."
                    ),
                    suggestion="Set agg_params.percentile (continuous, in [0, 1]).",
                )
                return None
            return column_expr, DataType.DOUBLE, _FLOAT_FORMAT, f"percentile(p={ap.percentile})"
        return column_expr, DataType.DOUBLE, _FLOAT_FORMAT, mapped

    @staticmethod
    def _alloc_column_name(slayer_model: SlayerModel, base: str) -> str:
        used = {c.name for c in slayer_model.columns}
        used |= {m.name for m in slayer_model.measures if m.name}
        name = base
        while name in used:
            name = f"{name}_col"
        return name

    # ── Resolution helpers ────────────────────────────────────────────

    def _find_measure_model(self, measure_name: str) -> Optional[DbtSemanticModel]:
        """Find which dbt semantic model contains a given measure."""
        for sm in self.project.semantic_models:
            for m in sm.measures:
                if m.name == measure_name:
                    return sm
        return None

    def _resolve_input_to_leaf(
        self, name: str
    ) -> Optional[Tuple[DbtSemanticModel, DbtMeasure]]:
        """Resolve a ratio/derived input to its single-aggregate leaf measure,
        or ``None`` when it's a multi-aggregate (ratio/derived/cumulative) metric."""
        res = self._resolve_input_to_leaf_filtered(name)
        return (res[0], res[1]) if res else None

    def _resolve_input_to_leaf_filtered(
        self, name: str
    ) -> Optional[Tuple[DbtSemanticModel, DbtMeasure, Optional[str]]]:
        """Like :meth:`_resolve_input_to_leaf`, but also accumulates the raw
        filter(s) encountered along the resolution chain.

        When an input names a *filtered* simple metric (its own ``filter`` or a
        ``measure.filter``), that filter must be intersected with any additional
        per-input / metric-level filter during push-down — otherwise the
        referenced metric's filter is silently dropped, widening results.
        Returns ``(source_model, leaf_measure, accumulated_raw_filter)`` or
        ``None`` for a multi-aggregate input.
        """
        sm = self._find_measure_model(name)
        if sm is not None:
            dbt_measure = next((m for m in sm.measures if m.name == name), None)
            if dbt_measure is not None:
                return sm, dbt_measure, None
        for mtc in self.project.metrics:
            if mtc.name != name:
                continue
            if (
                mtc.type
                and mtc.type.lower() == "simple"
                and mtc.type_params
                and mtc.type_params.measure_name
            ):
                tp = mtc.type_params
                mref = tp.measure
                # Unsupported simple-metric shapes (measure-less aggregation,
                # time-spine gap fill) are clean-failed elsewhere; they must NOT
                # be resurrected as plain pushable aggregates here.
                if tp.metric_aggregation_params is not None:
                    return None
                if mref and (mref.join_to_timespine or mref.fill_nulls_with is not None):
                    return None
                inner = self._resolve_input_to_leaf_filtered(tp.measure_name)
                if inner is None:
                    return None
                inner_sm, inner_measure, inner_filter = inner
                own_filter = self._combine_filters(
                    mtc.filter, mref.filter if mref else None
                )
                return inner_sm, inner_measure, self._combine_filters(own_filter, inner_filter)
            return None  # ratio / derived / cumulative / conversion → multi-aggregate
        return None

    def _find_metric_source_model(self, metric: DbtMetric) -> Optional[str]:
        """Determine the source model for a metric.

        Walks ``measure``, ``metrics``, ``numerator``/``denominator``, and the
        nested cumulative measure, and returns the unique source model name —
        or ``None`` when inputs span multiple models.
        """
        if metric.type_params is None:
            return None
        sources = self._collect_metric_sources_from_params(metric.type_params)
        return next(iter(sources)) if len(sources) == 1 else None

    def _collect_metric_sources(self, metric_name: str, _seen: Optional[set] = None) -> set:
        """Collect every distinct semantic-model name a metric ultimately resolves to."""
        seen = _seen if _seen is not None else set()
        if metric_name in seen:
            return set()
        seen = seen | {metric_name}

        for m in self.project.metrics:
            if m.name != metric_name:
                continue
            if m.type_params is None:
                return set()
            return self._collect_metric_sources_from_params(m.type_params, seen=seen)

        sm = self._find_measure_model(metric_name)
        return {sm.name} if sm else set()

    def _collect_metric_sources_from_params(
        self, type_params: DbtMetricTypeParams, *, seen: Optional[set] = None
    ) -> set:
        """Shared shape-walker used by both entry points above."""
        sources: set = set()
        if type_params.measure_name:
            sm = self._find_measure_model(type_params.measure_name)
            if sm:
                sources.add(sm.name)
        ctp = type_params.cumulative_type_params
        if ctp and ctp.measure:
            sm = self._find_measure_model(ctp.measure)
            if sm:
                sources.add(sm.name)
        if type_params.metrics:
            for m_input in type_params.metrics:
                sources |= self._collect_metric_sources(m_input.name, _seen=seen)
        for side in (type_params.numerator, type_params.denominator):
            if side is None:
                continue
            sources |= self._collect_metric_sources(side.name, _seen=seen)
        return sources

    def _resolve_metric_to_name(self, metric_name: str) -> Optional[str]:
        """Resolve a metric name to a formula reference (bare ModelMeasure name).

        For an *unfiltered* simple metric — which the converter does not
        materialize — resolves to the backing dbt measure name instead. Falls
        back to ``_resolve_measure_to_name`` when ``metric_name`` is a measure.
        """
        for m in self.project.metrics:
            if m.name != metric_name:
                continue
            if (
                m.type
                and m.type.lower() == "simple"
                and not m.filter
                and m.type_params is not None
                and m.type_params.measure_name
                and not (
                    m.type_params.measure
                    and (
                        m.type_params.measure.join_to_timespine
                        or m.type_params.measure.fill_nulls_with is not None
                    )
                )
            ):
                return self._resolve_measure_to_name(m.type_params.measure_name)
            return metric_name
        return self._resolve_measure_to_name(metric_name)

    def _resolve_measure_to_name(self, measure_name: str) -> Optional[str]:
        """Resolve a dbt measure name to a formula reference (bare name)."""
        sm = self._find_measure_model(measure_name)
        if sm is None:
            return None
        slayer_model = self._models_by_name.get(sm.name)
        if slayer_model is None:
            return None
        for m in slayer_model.measures:
            if m.name == measure_name:
                return measure_name
        return None
