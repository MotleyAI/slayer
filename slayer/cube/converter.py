"""Convert a parsed CubeProject into SLayer models.

Mirrors ``slayer/dbt/converter.py``. For each cube → one table-owning model;
for each view → one facade model. Everything that can't map cleanly is recorded
on the ``CubeConversionReport``. See DEV-1608.
"""

import logging
import re

import sqlglot
from pydantic import BaseModel

from slayer.core.enums import DataType, JoinType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.formula import ALL_TRANSFORMS, parse_formula
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.cube.extends import flatten_cube_extends, flatten_view_extends
from slayer.cube.models import CubeCube, CubeDimension, CubeMeasure, CubeView
from slayer.cube.refs import is_bare_identifier, parse_join_on, translate_cube_refs
from slayer.cube.report import (
    CubeConversionIssue,
    CubeConversionReport,
    CubeConversionResult,
    CubeIssueCategory,
)

logger = logging.getLogger(__name__)

_AGG_TYPES = {"sum", "avg", "min", "max", "count", "count_distinct", "count_distinct_approx"}
_CALC_TYPES = {"number", "string", "time", "boolean"}
_DEFERRED_MEASURE_TYPES = {"number_agg"}
_DIM_TYPE_MAP = {
    "string": DataType.TEXT, "number": DataType.DOUBLE,
    "boolean": DataType.BOOLEAN, "time": DataType.TIMESTAMP,
}
_FORMAT_MAP = {
    "percent": NumberFormatType.PERCENT, "currency": NumberFormatType.CURRENCY,
    "number": NumberFormatType.FLOAT,
}
_DURATION_UNITS = {"day": "d", "month": "m", "week": "w", "year": "y",
                   "hour": "h", "minute": "min", "second": "s"}
_CUBE_INFRA_FIELDS = ("refresh_key", "calendar", "hierarchies", "access_policy", "sql_alias")


class _MeasureInfo(BaseModel):
    """How a converted cube measure can be re-aggregated by a view facade."""
    kind: str  # "agg" | "calc" | "star_count"
    underlying_col: str | None = None
    agg: str | None = None


class _Names:
    """Shared column+measure namespace allocator."""

    def __init__(self) -> None:
        self.used: set[str] = set()

    def take(self, base: str, suffix: str = "_col") -> str:
        name = base
        while name in self.used:
            name = name + suffix
        self.used.add(name)
        return name

    def reserve(self, name: str) -> None:
        self.used.add(name)


def _map_format(fmt, report: CubeConversionReport, *, cube: str, member: str) -> NumberFormat | None:
    if fmt is None:
        return None
    ftype = fmt.get("type") if isinstance(fmt, dict) else fmt
    nf_type = _FORMAT_MAP.get(ftype) if isinstance(ftype, str) else None
    if nf_type is None:
        report.add(CubeConversionIssue(
            category=CubeIssueCategory.UNSUPPORTED_FORMAT, severity="info",
            cube=cube, member=member, message=f"Unsupported format '{fmt}'; dropped.",
        ))
        return None
    kwargs = {"type": nf_type}
    if nf_type == NumberFormatType.CURRENCY and isinstance(fmt, dict) and fmt.get("currency_symbol"):
        kwargs["symbol"] = fmt["currency_symbol"]  # symbol ONLY for currency (Codex #8)
    try:
        return NumberFormat(**kwargs)
    except Exception:  # noqa: BLE001
        report.add(CubeConversionIssue(
            category=CubeIssueCategory.UNSUPPORTED_FORMAT, severity="info",
            cube=cube, member=member, message=f"Invalid format '{fmt}'; dropped.",
        ))
        return None


def _window_from_rolling(rolling, report, *, cube, member) -> str | None:
    trailing = rolling.get("trailing")
    if rolling.get("leading") or rolling.get("offset") or trailing in (None, "unbounded"):
        report.add(CubeConversionIssue(
            category=CubeIssueCategory.UNSUPPORTED_ROLLING_WINDOW, severity="warning",
            cube=cube, member=member,
            message="Only finite trailing rolling_window maps; fell back to plain aggregation.",
        ))
        return None
    m = re.match(r"(\d+)\s+(\w+)", str(trailing))
    unit = _DURATION_UNITS.get(m.group(2).rstrip("s")) if m else None
    if not m or unit is None:
        report.add(CubeConversionIssue(
            category=CubeIssueCategory.UNSUPPORTED_ROLLING_WINDOW, severity="warning",
            cube=cube, member=member, message=f"Unparseable rolling_window '{trailing}'.",
        ))
        return None
    return f"{m.group(1)}{unit}"


class CubeToSlayerConverter:
    """Convert a CubeProject into SLayer models + a structured report."""

    def __init__(self, project, data_source: str, parse_issues=None) -> None:
        self.project = project
        self.data_source = data_source
        self.parse_issues = parse_issues or []
        self._cubes: dict[str, CubeCube] = {}
        self._models: dict[str, SlayerModel] = {}
        # model name → {measure name → _MeasureInfo}
        self._measure_info: dict[str, dict[str, _MeasureInfo]] = {}

    # ── pipeline ───────────────────────────────────────────────────────────

    def convert(self) -> CubeConversionResult:
        report = CubeConversionReport(issues=list(self.parse_issues))

        cubes, cube_issues = flatten_cube_extends(self.project.cubes)
        report.issues.extend(cube_issues)
        views, view_issues = flatten_view_extends(self.project.views)
        report.issues.extend(view_issues)

        self._cubes = {c.name: c for c in cubes}
        models: list[SlayerModel] = []
        for cube in cubes:
            model = self._convert_cube(cube, report)
            if model is not None:
                models.append(model)
                self._models[model.name] = model

        for view in views:
            model = self._convert_view(view, report)
            if model is not None:
                models.append(model)
                self._models[model.name] = model

        report.model_count = len(models)
        report.hidden_count = sum(1 for m in models if m.hidden)
        report.view_count = sum(
            1 for m in models if (m.meta or {}).get("cube_kind") == "view")
        return CubeConversionResult(models=models, report=report)

    # ── cube → model ───────────────────────────────────────────────────────

    def _convert_cube(self, cube: CubeCube, report: CubeConversionReport) -> SlayerModel | None:
        source = self._cube_source(cube, report)
        if source is None:
            return None

        meta: dict = dict(cube.meta or {})
        if cube.title:
            meta["cube_title"] = cube.title
        unmapped: dict = {}
        if cube.data_source:
            unmapped["data_source"] = cube.data_source
        if cube.pre_aggregations:
            unmapped["pre_aggregations"] = cube.pre_aggregations
        for field in _CUBE_INFRA_FIELDS:
            val = getattr(cube, field, None)
            if val is not None:
                unmapped[field] = val
        for key in list(unmapped):
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.UNMAPPED_INFRA, severity="warning",
                cube=cube.name, message=f"'{key}' has no SLayer equivalent; stashed in meta.",
                raw=str(unmapped[key]),
            ))

        names = _Names()
        columns: list[Column] = []
        measures: list[ModelMeasure] = []
        info: dict[str, _MeasureInfo] = {}

        for dim in cube.dimensions:
            self._convert_dimension(cube, dim, columns, names, unmapped, report)
        dedup: dict[tuple, str] = {}
        for meas in cube.measures:
            self._convert_measure(cube, meas, columns, measures, names, dedup, info, report)
        for seg in cube.segments:
            self._convert_segment(cube, seg, columns, names, report)

        joins = self._convert_joins(cube, report)
        columns, measures = self._validate_offline(cube.name, columns, measures, report)
        self._dedisambiguate_namespace(columns, measures, report, cube=cube.name)

        if unmapped:
            meta["cube_unmapped"] = unmapped

        try:
            model = SlayerModel(
                name=cube.name, data_source=self.data_source,
                hidden=not cube.public, description=cube.description,
                meta=meta or None, columns=columns, measures=measures, joins=joins,
                **source,
            )
        except Exception as exc:  # noqa: BLE001 — illegal name etc. → report, don't crash
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="error",
                cube=cube.name, message=f"Could not build model: {exc}",
            ))
            return None
        self._measure_info[cube.name] = info
        return model

    def _cube_source(self, cube: CubeCube, report) -> dict | None:
        if cube.sql_table:
            return {"sql_table": cube.sql_table}
        if cube.sql:
            translated = translate_cube_refs(cube.sql, mode="sql", cube=cube.name)
            try:
                sqlglot.parse_one(translated)
            except Exception:  # noqa: BLE001
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_SQL, severity="error",
                    cube=cube.name, message="Cube 'sql' could not be translated; cube dropped.",
                ))
                return None
            return {"sql": translated}
        report.add(CubeConversionIssue(
            category=CubeIssueCategory.NO_SOURCE, severity="error",
            cube=cube.name, message="Cube has no sql_table/sql source; dropped.",
        ))
        return None

    # ── dimensions ─────────────────────────────────────────────────────────

    def _convert_dimension(self, cube, dim: CubeDimension, columns, names, unmapped, report) -> None:
        if dim.type == "switch":
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.DEFERRED_STAGE2, severity="warning",
                cube=cube.name, member=dim.name,
                message="`switch` dimension is a Tesseract feature (Stage 2); skipped.",
            ))
            return
        if dim.type == "geo":
            unmapped.setdefault("geo", []).append(
                {"name": dim.name, "latitude": dim.latitude, "longitude": dim.longitude})
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.GEO_UNMAPPED, severity="warning",
                cube=cube.name, member=dim.name, message="geo dimension has no SLayer type; stashed."))
            return
        if dim.sub_query:
            unmapped.setdefault("sub_query", []).append(dim.name)
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.SUBQUERY_UNMAPPED, severity="warning",
                cube=cube.name, member=dim.name, message="sub_query dimension has no SLayer equivalent."))
            return
        if dim.granularities:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.GRANULARITY_UNMAPPED, severity="info",
                cube=cube.name, member=dim.name,
                message="Custom granularities are query-time in SLayer; base column kept."))

        if dim.case:
            sql = self._build_case_sql(cube, dim.case)
        elif dim.sql:
            translated = translate_cube_refs(dim.sql, mode="sql", cube=cube.name)
            sql = None if translated == dim.name else translated
        else:
            sql = None

        name = names.take(dim.name)
        columns.append(Column(
            name=name, sql=sql, type=_DIM_TYPE_MAP.get(dim.type, DataType.TEXT),
            primary_key=dim.primary_key, label=dim.title, description=dim.description,
            meta=dim.meta,
            format=_map_format(dim.format, report, cube=cube.name, member=dim.name),
        ))

    def _build_case_sql(self, cube, case: dict) -> str:
        parts = ["CASE"]
        for when in case.get("when", []):
            cond = translate_cube_refs(when.get("sql", ""), mode="sql", cube=cube.name)
            parts.append(f"WHEN {cond} THEN '{when.get('label', '')}'")
        if case.get("else"):
            parts.append(f"ELSE '{case['else'].get('label', '')}'")
        parts.append("END")
        return " ".join(parts)

    # ── measures ───────────────────────────────────────────────────────────

    def _convert_measure(self, cube, meas: CubeMeasure, columns, measures, names, dedup, info, report) -> None:
        if meas.type in _DEFERRED_MEASURE_TYPES or meas.case is not None:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.DEFERRED_STAGE2, severity="warning",
                cube=cube.name, member=meas.name,
                message=f"Measure type/shape '{meas.type}' is a Tesseract feature (Stage 2); skipped."))
            return
        if meas.drill_members:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.UNMAPPED_INFRA, severity="info",
                cube=cube.name, member=meas.name, message="drill_members has no SLayer equivalent."))
        if meas.multi_stage or meas.time_shift or meas.grain or meas.filter is not None:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.DEFERRED_STAGE2, severity="info",
                cube=cube.name, member=meas.name,
                message="multi_stage/time_shift/grain/filter on measure deferred to Stage 2; "
                        "emitted as plain aggregation if possible."))

        if meas.name in ALL_TRANSFORMS:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                cube=cube.name, member=meas.name,
                message=f"Measure name '{meas.name}' shadows a SLayer transform; skipped."))
            return

        # Reserve the measure's name BEFORE creating its underlying column, so a
        # bare-identifier column yields (`rate` → `rate_col`) and the measure keeps
        # the Cube name (dbt-importer idiom). Collision with a dimension column
        # falls through to the `_measure` suffix.
        final_name = names.take(meas.name, suffix="_measure")
        if meas.type in _CALC_TYPES and meas.sql:
            self._convert_calc_measure(cube, meas, measures, final_name, report)
            return
        self._convert_agg_measure(cube, meas, columns, measures, names, dedup, info, report, final_name)

    def _convert_agg_measure(self, cube, meas, columns, measures, names, dedup, info, report, final_name) -> None:
        agg = "count_distinct" if meas.type == "count_distinct_approx" else meas.type
        if meas.type == "count_distinct_approx":
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.LOSSY_MAPPING, severity="info",
                cube=cube.name, member=meas.name,
                message="count_distinct_approx → exact count_distinct (SLayer has no approx)."))

        window = _window_from_rolling(meas.rolling_window, report, cube=cube.name, member=meas.name) \
            if meas.rolling_window else None

        if meas.type == "count" and not meas.sql:
            formula = "*:count" + (f"(window='{window}')" if window else "")
            self._emit_measure(measures, names, final_name, formula, meas, report, cube.name)
            info[meas.name] = _MeasureInfo(kind="star_count")
            return

        translated = translate_cube_refs(meas.sql, mode="sql", cube=cube.name)
        filter_pred = self._measure_filter(cube, meas)
        col_name = self._get_or_create_column(
            meas, translated, filter_pred, columns, names, dedup, report, cube)
        formula = f"{col_name}:{agg}" + (f"(window='{window}')" if window else "")
        self._emit_measure(measures, names, final_name, formula, meas, report, cube.name)
        info[meas.name] = _MeasureInfo(kind="agg", underlying_col=col_name, agg=agg)

    def _convert_calc_measure(self, cube, meas, measures, final_name, report) -> None:
        formula = translate_cube_refs(meas.sql, mode="dsl", cube=cube.name)
        self._emit_measure(measures, None, final_name, formula, meas, report, cube.name,
                           result_type=_DIM_TYPE_MAP.get(meas.type))

    def _measure_filter(self, cube, meas) -> str | None:
        if not meas.filters:
            return None
        preds = [translate_cube_refs(f.sql, mode="sql", cube=cube.name) for f in meas.filters]
        return " AND ".join(preds) if preds else None

    def _get_or_create_column(self, meas, translated_sql, filter_pred, columns, names, dedup, report, cube) -> str:
        key = (translated_sql, filter_pred)
        if key in dedup:
            return dedup[key]
        base = translated_sql if is_bare_identifier(translated_sql) else f"{meas.name}_col"
        col_name = names.take(base)
        columns.append(Column(
            name=col_name,
            sql=None if col_name == translated_sql else translated_sql,
            type=DataType.DOUBLE, filter=filter_pred,
            format=_map_format(meas.format, report, cube=cube.name, member=meas.name),
        ))
        dedup[key] = col_name
        return col_name

    def _emit_measure(self, measures, names, final_name, formula, meas, report, cube_name, *, result_type=None) -> None:
        try:
            measures.append(ModelMeasure(
                name=final_name, formula=formula, label=meas.title,
                description=meas.description, type=result_type, meta=meas.meta))
        except Exception as exc:  # noqa: BLE001
            if names is not None:
                names.used.discard(final_name)
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                cube=cube_name, member=meas.name,
                message=f"Measure '{meas.name}' could not be built: {exc}"))

    # ── segments ───────────────────────────────────────────────────────────

    def _convert_segment(self, cube, seg, columns, names, report) -> None:
        name = names.take(seg.name, suffix="_seg")
        columns.append(Column(
            name=name, sql=translate_cube_refs(seg.sql, mode="sql", cube=cube.name),
            type=DataType.BOOLEAN, label=seg.title, description=seg.description, meta=seg.meta))
        report.add(CubeConversionIssue(
            category=CubeIssueCategory.SEGMENT_AS_COLUMN, severity="info",
            cube=cube.name, member=seg.name,
            message=f"Segment '{seg.name}' mapped to a boolean column."))

    # ── joins ──────────────────────────────────────────────────────────────

    def _convert_joins(self, cube, report) -> list[ModelJoin]:
        joins: list[ModelJoin] = []
        for cj in cube.joins:
            pairs = parse_join_on(cj.sql, source_cube=cube.name, target_cube=cj.name)
            resolved = self._resolve_join_pairs(cube, cj, pairs) if pairs else None
            if not resolved:
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.UNSUPPORTED_JOIN, severity="warning",
                    cube=cube.name, member=cj.name,
                    message=f"Join ON '{cj.sql}' is not an equality of physical columns; dropped."))
                continue
            joins.append(ModelJoin(target_model=cj.name, join_pairs=resolved, join_type=JoinType.LEFT))
        return joins

    def _resolve_join_pairs(self, cube, cj, pairs) -> list[list[str]] | None:
        target = self._cubes.get(cj.name)
        out: list[list[str]] = []
        for src_member, tgt_member in pairs:
            src = self._physical_col(cube, src_member)
            tgt = self._physical_col(target, tgt_member) if target else tgt_member
            if src is None or tgt is None:
                return None
            out.append([src, tgt])
        return out

    def _physical_col(self, cube, member: str) -> str | None:
        if cube is None:
            return member
        dim = next((d for d in cube.dimensions if d.name == member), None)
        if dim is None or dim.sql is None:
            return member
        translated = translate_cube_refs(dim.sql, mode="sql", cube=cube.name)
        return translated.strip() if is_bare_identifier(translated) else None

    # ── offline validation + namespace safety ──────────────────────────────

    def _validate_offline(self, cube_name, columns, measures, report):
        good_cols = []
        dropped: set[str] = set()
        for col in columns:
            if col.sql is None:
                good_cols.append(col)
                continue
            try:
                sqlglot.parse_one(col.sql)
                good_cols.append(col)
            except Exception:  # noqa: BLE001
                dropped.add(col.name)
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_SQL, severity="warning",
                    cube=cube_name, member=col.name,
                    message=f"Column sql does not parse; dropped: {col.sql!r}"))
        col_names = {c.name for c in good_cols}
        known = col_names | {m.name for m in measures if m.name}
        good_measures = []
        for m in measures:
            ref_col = m.formula.split(":")[0].strip()
            if ref_col in dropped:
                continue
            if not self._formula_parses(m.formula, known):
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                    cube=cube_name, member=m.name,
                    message=f"Measure formula does not parse; dropped: {m.formula!r}"))
                continue
            good_measures.append(m)
        return good_cols, good_measures

    def _formula_parses(self, formula: str, known_names: set[str]) -> bool:
        nm = {n: "*:count" for n in known_names}
        try:
            parse_formula(formula, named_measures=nm or None)
            return True
        except Exception:  # noqa: BLE001
            return False

    def _dedisambiguate_namespace(self, columns, measures, report, *, cube) -> None:
        # The allocator already guarantees uniqueness; this is a defensive check.
        col_names = {c.name for c in columns}
        for m in measures:
            if m.name in col_names:  # pragma: no cover — allocator prevents this
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_MEASURE, severity="info",
                    cube=cube, member=m.name, message="measure/column name overlap auto-resolved."))

    # ── views → facade models ──────────────────────────────────────────────

    def _convert_view(self, view: CubeView, report) -> SlayerModel | None:
        if not view.cubes:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.AMBIGUOUS_VIEW_ROOT, severity="warning",
                view=view.name, message="View has no cubes; skipped."))
            return None
        root_cube_name = view.cubes[0].join_path.split(".")[0]
        root_cube = self._cubes.get(root_cube_name)
        root_model = self._models.get(root_cube_name)
        if root_cube is None or root_model is None:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.AMBIGUOUS_VIEW_ROOT, severity="warning",
                view=view.name,
                message=f"View root cube '{root_cube_name}' was not emitted; view dropped."))
            return None

        source = {"sql_table": root_model.sql_table} if root_model.sql_table else {"sql": root_model.sql}
        meta = {"cube_kind": "view"}
        unmapped: dict = {}
        if view.folders:
            unmapped["folders"] = view.folders
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.FOLDERS_UNMAPPED, severity="info",
                view=view.name, message="Folders have no SLayer hierarchy; stashed in meta."))

        names = _Names()
        columns: list[Column] = []
        measures: list[ModelMeasure] = []
        join_targets: set[str] = set()

        for ref in view.cubes:
            self._convert_view_ref(view, ref, root_cube_name, root_model,
                                   columns, measures, names, join_targets, report)

        joins = [j for j in root_model.joins if j.target_model in join_targets]
        filters = self._view_default_filters(view, root_cube_name, report)
        if unmapped:
            meta["cube_unmapped"] = unmapped

        try:
            return SlayerModel(
                name=view.name, data_source=self.data_source,
                hidden=not view.public, description=view.description, meta=meta,
                columns=columns, measures=measures, joins=joins, filters=filters,
                **source,
            )
        except Exception as exc:  # noqa: BLE001
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="error",
                view=view.name, message=f"Could not build facade model: {exc}"))
            return None

    def _convert_view_ref(self, view, ref, root_cube_name, root_model,
                          columns, measures, names, join_targets, report) -> None:
        path = ref.join_path.split(".")
        cube_name = path[-1]
        cube = self._cubes.get(cube_name)
        cube_model = self._models.get(cube_name)
        is_root = (len(path) == 1 and cube_name == root_cube_name)

        if cube is None or cube_model is None:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.DISCONNECTED_VIEW, severity="warning",
                view=view.name, member=ref.join_path,
                message=f"View member cube '{cube_name}' not available; skipped."))
            return

        if not is_root:
            join = next((j for j in root_cube.joins if j.name == cube_name), None) \
                if (root_cube := self._cubes.get(root_cube_name)) else None
            if join is None or not any(j.target_model == cube_name for j in root_model.joins):
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.DISCONNECTED_VIEW, severity="warning",
                    view=view.name, member=ref.join_path,
                    message=f"'{cube_name}' is not joined to root '{root_cube_name}'; skipped."))
                return
            if join.relationship in ("one_to_many", "has_many"):
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.VIEW_FANOUT_RISK, severity="warning",
                    view=view.name, member=ref.join_path,
                    message=f"Join to '{cube_name}' is {join.relationship}; root measures may fan out."))
            join_targets.add(cube_name)

        prefix = f"{ref.alias or cube_name}_" if ref.prefix else ""
        dim_names, meas_names = self._selected_members(cube, ref)

        for dname in dim_names:
            self._facade_dimension(cube, cube_model, dname, prefix, is_root,
                                   columns, names)
        for mname in meas_names:
            self._facade_measure(view, cube_name, cube_model, mname, prefix, is_root,
                                 columns, measures, names, report)

    def _selected_members(self, cube, ref) -> tuple[list[str], list[str]]:
        dims = [d.name for d in cube.dimensions if d.type not in ("geo", "switch") and not d.sub_query]
        meas = [m.name for m in cube.measures]
        exclude = set(ref.excludes or [])
        if ref.includes in (None, "*"):
            chosen = [n for n in dims + meas if n not in exclude]
        else:
            chosen = [n for n in ref.includes if n not in exclude]
        chosen_set = set(chosen)
        return ([d for d in dims if d in chosen_set], [m for m in meas if m in chosen_set])

    def _facade_dimension(self, cube, cube_model, dname, prefix, is_root, columns, names) -> None:
        col = cube_model.get_column(dname)
        if col is None:
            return
        if is_root:
            sql = col.sql if col.sql else col.name
        else:
            sql = f"{cube.name}.{col.name}"
        exported = names.take(f"{prefix}{dname}")
        columns.append(Column(
            name=exported, sql=sql, type=col.type, label=col.label,
            description=col.description, format=col.format))

    def _facade_measure(self, view, cube_name, cube_model, mname, prefix, is_root,
                        columns, measures, names, report) -> None:
        info = self._measure_info.get(cube_name, {}).get(mname)
        if info is None:
            return
        exported = names.take(f"{prefix}{mname}", suffix="_measure")
        if info.kind == "star_count":
            formula = "*:count" if is_root else f"{cube_name}.*:count"
        elif info.kind == "agg":
            base = f"{info.underlying_col}:{info.agg}"
            if is_root:
                src_col = cube_model.get_column(info.underlying_col)
                if src_col is not None and not any(c.name == info.underlying_col for c in columns):
                    columns.append(src_col.model_copy())  # carry the underlying column onto the facade
                formula = base
            else:
                formula = f"{cube_name}.{info.underlying_col}:{info.agg}"
        else:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_MEASURE, severity="info",
                view=view.name, member=mname,
                message=f"Calculated measure '{mname}' re-export not supported in Stage 1."))
            return
        try:
            measures.append(ModelMeasure(name=exported, formula=formula))
        except Exception as exc:  # noqa: BLE001
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                view=view.name, member=mname, message=f"Facade measure failed: {exc}"))

    def _view_default_filters(self, view, root_cube_name, report) -> list[str]:
        filters: list[str] = []
        for df in view.default_filters or []:
            member = df.get("member", "")
            op = df.get("operator")
            values = df.get("values") or []
            col = self._resolve_view_member(member, root_cube_name)
            if op == "equals" and len(values) == 1:
                filters.append(f"{col} = '{values[0]}'")
            elif op in ("equals", "in") and values:
                vlist = ", ".join(f"'{v}'" for v in values)
                filters.append(f"{col} IN ({vlist})")
            else:
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.UNSUPPORTED_DEFAULT_FILTER, severity="info",
                    view=view.name, member=member,
                    message=f"default_filter operator '{op}' not mapped; dropped."))
        return filters

    def _resolve_view_member(self, member: str, root_cube_name: str) -> str:
        parts = member.split(".")
        if parts and parts[0] == root_cube_name:
            parts = parts[1:]
        return ".".join(parts) if parts else member
