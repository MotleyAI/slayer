"""Convert a parsed CubeProject into SLayer datasets.

Mirrors ``slayer/dbt/converter.py``. For each cube → one table-owning dataset;
for each view → one facade dataset. Everything that can't map cleanly is recorded
on the ``CubeConversionReport``.
"""

import logging
import re
from collections import defaultdict
from typing import Any

import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.errors import SqlglotError

from slayer.core.enums import (
    DataType,
    RelationshipCardinality,
    JoinType,
    invert_cardinality,
)
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import render_probe_text
from slayer.core.syntax import metric_dependency_problems
from slayer.core.refs import IDENTIFIER_RE, functional_agg_text
from slayer.cube.extends import flatten_cube_extends, flatten_view_extends
from slayer.cube.fold import fold_cube
from slayer.cube.filter_params import (
    apply_filter_params,
    parse_string_filter_params,
)
from slayer.cube.models import (
    CubeCube,
    CubeDimension,
    CubeFilterParamRef,
    CubeMeasure,
    CubeView,
    CubeViewCubeRef,
)
from slayer.cube.refs import CubeSql, ForeignRef, JoinSide, classify_cube_refs, parse_join_keys, translate_cube_refs
from slayer.cube.report import (
    CubeConversionIssue,
    CubeConversionReport,
    CubeConversionResult,
    CubeIssueCategory,
)
from slayer.importers.aggregates import OperandPolicy, Unconvertible, convert_aggregates
from slayer.importers.column_types import (
    LiveSource,
    Typed,
    LiveDatasource,
    contradiction_note,
    contradicts,
    implied_type,
    list_refusal,
    offline_note,
    refused_over_list,
    untyped_message,
)
from slayer.importers.metrics import metric_problems, unreachable_fields
from slayer.importers.physical import (
    PhysicalIndex, UndecidableIdentifierCaseError, physical_identifier, root_physical_columns, unreadable_fields,
)
from slayer.database.client import SqlClient
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type

logger = logging.getLogger(__name__)

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


def _windowed(agg: str, window: str | None) -> str:
    """An aggregation suffix, with a ``window=`` argument when rolling."""
    return f"{agg}(window='{window}')" if window else agg


def _sql_str_literal(value) -> str:
    """Render a value as a single-quoted SQL string literal, escaping quotes."""
    return "'" + str(value).replace("'", "''") + "'"


def _case_when_predicates(dim) -> list[dict]:
    """Return the ``when`` predicate dicts of a CASE-WHEN dimension (each carries
    a ``sql``), or ``[]`` for a non-case dimension. These are Mode-A surfaces too,
    so they can host FILTER_PARAMS."""
    if not dim.case:
        return []
    return [w for w in dim.case.get("when", []) if isinstance(w, dict)]


class _MeasureInfo(BaseModel):
    """How a converted cube measure can be re-aggregated by a view facade."""
    kind: str  # "agg" | "calc" | "star_count"
    underlying_col: str | None = None
    suffix: str | None = None  # the source measure's aggregation suffix, window included
    emitted_name: str | None = None  # the emitted DatasetMetric name (for pruning)


class _Names:
    """Shared field+metric namespace allocator."""

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


#: Cube ``relationship`` → SLayer ``RelationshipCardinality`` (closed table).
_RELATIONSHIP_CARDINALITY = {
    "many_to_one": RelationshipCardinality.MANY_TO_ONE,
    "belongs_to": RelationshipCardinality.MANY_TO_ONE,
    "one_to_many": RelationshipCardinality.ONE_TO_MANY,
    "has_many": RelationshipCardinality.ONE_TO_MANY,
    "one_to_one": RelationshipCardinality.ONE_TO_ONE,
    "has_one": RelationshipCardinality.ONE_TO_ONE,
}


_TO_ONE = (RelationshipCardinality.MANY_TO_ONE, RelationshipCardinality.ONE_TO_ONE)


def _is_exact_inverse(a: DatasetRelationship, b: DatasetRelationship) -> bool:
    """Swapped pair set, same join type, cardinalities inversion-consistent (or one unset)."""
    if a.join_type != b.join_type:
        return False
    if {(s, t) for s, t in a.join_pairs} != {(t, s) for s, t in b.join_pairs}:
        return False
    if a.cardinality is None or b.cardinality is None:
        return True
    return invert_cardinality(a.cardinality) == b.cardinality


def _inverse_pair_loser(
    a: tuple[str, DatasetRelationship], b: tuple[str, DatasetRelationship]
) -> DatasetRelationship:
    """The edge to drop: keep the to-one side, else the cardinality-carrying
    side, else the lexicographically smaller ``(dataset, target)`` declaration."""
    for keep, drop in ((a, b), (b, a)):
        if keep[1].cardinality in _TO_ONE:
            return drop[1]
    for keep, drop in ((a, b), (b, a)):
        if keep[1].cardinality is not None:
            return drop[1]
    return max(a, b, key=lambda x: (x[0], x[1].to))[1]


def _drop_inverse_half(
    *, dataset: SlayerDataset, relationship: DatasetRelationship, peer: SlayerDataset
) -> None:
    """Remove the losing half when ``peer`` declares the exact inverse of ``relationship``."""
    back = next(
        (b for b in peer.relationships
         if b.to == dataset.name and _is_exact_inverse(relationship, b)),
        None,
    )
    if back is None:
        return
    loser = _inverse_pair_loser((dataset.name, relationship), (peer.name, back))
    if loser is relationship:
        dataset.relationships.remove(relationship)
    else:
        peer.relationships.remove(back)


def _map_relationship(relationship: str | None) -> RelationshipCardinality | None:
    """Map a Cube join ``relationship`` onto cardinality; unknown → None."""
    if relationship is None:
        return None
    return _RELATIONSHIP_CARDINALITY.get(relationship.strip().lower())


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
        return NumberFormat.model_validate(kwargs)
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


def _implied(aggs: list[str]) -> Typed | None:
    """The type a field's aggregations imply: numeric over a count's ``TEXT``."""
    implied = [t for a in aggs if (t := implied_type(a)) is not None]
    return next((t for t in implied if t[0] is not DataType.TEXT), implied[0] if implied else None)


class _CubeOperands(OperandPolicy):
    """Cube: a bare operand aggregates its field; any other operand, and a FILTER, go to a Mode-A helper field."""

    sql_residual = True

    def __init__(self, *, converter: "CubeToSlayerConverter", build: "_CubeBuild", metric: str) -> None:
        super().__init__()
        self.converter = converter
        self.build = build
        self.measure = metric
        self._measures = {m.name for m in build.cube.measures}

    def operand(self, operand: exp.Expr, *, mask: exp.Expr | None, agg: str) -> str:
        column = self._field(operand, mask=mask, agg=agg)
        self.build.sources[self.build.measure_names[self.measure]].append((column, agg))
        return column

    def _field(self, operand: exp.Expr, *, mask: exp.Expr | None, agg: str) -> str:
        if isinstance(operand, exp.Column) and mask is None:
            return self.converter._bare_source(self.build, operand, agg=agg)
        foreign = self.build.foreign
        other = sorted({c.table or foreign[c.name].cube for node in (operand, mask) if node is not None
                        for c in node.find_all(exp.Column) if c.table or c.name in foreign})
        if other:
            raise Unconvertible(f"an aggregate operand reads another cube ({', '.join(other)})")
        dialect = self.converter._dialect
        return self.converter._helper(self.build, metric=self.measure, sql=operand.sql(dialect=dialect),
                                      predicate=mask.sql(dialect=dialect) if mask is not None else None, agg=agg)

    def residual(self, node: exp.Column) -> str | None:
        member = self.build.markers.get(node.name) if not node.table else None
        return self.build.measure_names[member] if member in self._measures else None


class _CubeBuild(BaseModel):
    """One cube's fields and metrics while they are built; field SQL holds member markers until resolved."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    cube: CubeCube
    names: _Names
    emitted: dict[str, str]  # member reference -> emitted name (a dimension before a same-named measure)
    measure_names: dict[str, str]
    segment_names: dict[str, str]
    offline: str | None = None  # why types are not checked against the database
    fields: list[DatasetField] = Field(default_factory=list)
    metrics: list[DatasetMetric] = Field(default_factory=list)
    info: dict[str, _MeasureInfo] = Field(default_factory=dict)
    markers: dict[str, str] = Field(default_factory=dict)  # marker -> source member
    foreign: dict[str, ForeignRef] = Field(default_factory=dict)  # marker -> a joined cube's physical column
    foreign_sql: dict[str, str] = Field(default_factory=dict)  # marker -> `other.column`
    foreign_problems: dict[str, str] = Field(default_factory=dict)  # marker -> why it cannot be read
    unknown: dict[str, list[str]] = Field(default_factory=dict)  # field -> undeclared members it reads
    declared: dict[str, Typed] = Field(default_factory=dict)  # field -> Cube dimension type
    field_aggs: dict[str, list[str]] = Field(default_factory=lambda: defaultdict(list))
    sources: dict[str, list[tuple[str, str]]] = Field(default_factory=lambda: defaultdict(list))
    helpers: dict[tuple[str, str | None, Typed | None], str] = Field(default_factory=dict)
    fixed: set[str] = Field(default_factory=set)  # fields whose type no probe changes (segments, CASE labels)
    dimensions: set[str] = Field(default_factory=set)
    source: dict[str, str] = Field(default_factory=dict)
    meta: dict = Field(default_factory=dict)
    unmapped: dict = Field(default_factory=dict)
    fp_refs: list[CubeFilterParamRef] = Field(default_factory=list)
    fp_required: set[str] = Field(default_factory=set)


class CubeToSlayerConverter:
    """Convert a CubeProject into SLayer datasets + a structured report."""

    def __init__(
        self, project, data_source: str, parse_issues=None,
        *, honor_required_meta: bool = True, target_dialect: str | None = None, offline_reason: str | None = None,
    ) -> None:
        self.project = project
        self.data_source = data_source
        self.parse_issues = parse_issues or []
        # honor a member's truthy ``meta.required`` by emitting its
        # FILTER_PARAMS pushdown as a required (raise-on-missing) variable rather
        # than an optional block. ``--ignore-required-meta`` flips this off.
        self.honor_required_meta = honor_required_meta
        self.target_dialect = target_dialect
        self._live = LiveDatasource(offline_reason=offline_reason, data_source=data_source,
                                    db_type=target_dialect, dialect=None)
        self._use_dialect(dialect_for_ds_type(target_dialect) if target_dialect else None)
        self._offline_noted: set[str] = set()
        self._cubes: dict[str, CubeCube] = {}
        self._models: dict[str, SlayerDataset] = {}
        # dataset name → {metric name → _MeasureInfo}
        self._measure_info: dict[str, dict[str, _MeasureInfo]] = {}
        # cube → [(join target, relationship, key sides)], resolved once every cube is built
        self._pending_joins: dict[str, list[tuple[str, str | None, str, list[tuple[JoinSide, JoinSide]]]]] = {}
        # Per-cube FILTER_PARAMS state (set in _convert_cube, consumed by the
        # Mode-A translate helpers).
        self._active_refs: list[CubeFilterParamRef] = []
        self._active_required: set[str] = set()
        # Unquoted mixed-case identifiers no datasource dialect says how to read.
        self._undecidable: list[str] = []

    def _use_dialect(self, dialect: SqlDialect | None) -> None:
        self._sql_dialect = dialect
        self._dialect = self._live.dialect = dialect.sqlglot_name if dialect is not None else None

    async def collect_metadata(self, *, client: SqlClient) -> None:
        """Read every source and probe conversion asks for through ``client`` (in its datasource's dialect when none
        was given); ``convert`` then needs no database."""
        if self._sql_dialect is None:
            self._use_dialect(dialect_for_ds_type(client.datasource.type))
        await self._live.collect(client=client, convert=self.convert)

    # ── pipeline ───────────────────────────────────────────────────────────

    def convert(self) -> CubeConversionResult:
        """Convert from the collected metadata (pure: the database is read by ``collect_metadata``)."""
        self._offline_noted = set()
        self._models, self._measure_info, self._pending_joins = {}, {}, {}
        self._undecidable = []
        report = CubeConversionReport(issues=list(self.parse_issues))

        cubes, cube_issues = flatten_cube_extends(self.project.cubes)
        report.issues.extend(cube_issues)
        views, view_issues = flatten_view_extends(self.project.views)
        report.issues.extend(view_issues)

        self._cubes = {c.name: c for c in cubes}
        prepared = [p for cube in cubes if (p := self._prepare_cube(cube, report)) is not None]
        if self._undecidable:
            raise UndecidableIdentifierCaseError(self._undecidable)
        self._cubes.update((cube.name, cube) for cube, _ in prepared)
        builds = [b for cube, refs in prepared if (b := self._build_cube(cube, refs, report)) is not None]
        by_cube = {b.cube.name: b for b in builds}
        for build in builds:
            self._resolve_foreign(build, by_cube)
        datasets: list[SlayerDataset] = []
        for build in builds:
            model = self._finish_cube(build, report)
            if model is not None:
                datasets.append(model)
                self._models[model.name] = model
        self._resolve_relationships(datasets, report)
        self._drop_unreachable(datasets, report)
        for model in datasets:
            self._validate_metrics(model, report)

        for view in views:
            model = self._convert_view(view, report)
            if model is not None:
                datasets.append(model)
                self._models[model.name] = model

        # mutually-inverse declarations collapse to one edge (reverse
        # traversal is automatic); contradicting pairs import both edges. After
        # view conversion so facades still see the pre-dedup root joins.
        self._dedup_inverse_relationships(
            [m for m in datasets if (m.meta or {}).get("cube_kind") != "view"])

        report.dataset_count = len(datasets)
        report.hidden_count = sum(1 for m in datasets if m.hidden)
        report.view_count = sum(
            1 for m in datasets if (m.meta or {}).get("cube_kind") == "view")
        return CubeConversionResult(datasets=datasets, report=report)

    # ── cube → dataset ───────────────────────────────────────────────────────

    def _prepare_cube(self, cube: CubeCube, report: CubeConversionReport
                      ) -> tuple[CubeCube, list[CubeFilterParamRef]] | None:
        """The cube with FILTER_PARAMS extracted and its physical SQL folded; ``None`` when it is dropped."""
        # gather + validate FILTER_PARAMS refs BEFORE any translation,
        # so a bad ref drops the cube cleanly with no half-built variable entries.
        setup = self._setup_filter_params(cube, report)
        if setup is None:
            return None
        folded = fold_cube(*setup, dialect=self._sql_dialect)
        report.issues.extend(folded.issues)
        self._undecidable += folded.undecidable
        return (folded.cube, folded.refs) if folded.cube is not None else None

    def _build_cube(self, cube: CubeCube, refs: list[CubeFilterParamRef], report: CubeConversionReport
                    ) -> _CubeBuild | None:
        """The cube's fields and metrics, member markers and other cubes' physical columns still unresolved."""
        self._active_refs, self._active_required = refs, self._required_members(cube, refs)
        source = self._cube_source(cube, report)
        if source is None:
            return None

        meta, unmapped = self._build_meta_and_unmapped(cube, refs, report)
        dims, measure_names, segment_names = self._reserve_names(cube)
        build = _CubeBuild(cube=cube, names=_Names(), emitted={**segment_names, **measure_names, **dims},
                           measure_names=measure_names, segment_names=segment_names, source=source, meta=meta,
                           unmapped=unmapped, fp_refs=refs, fp_required=self._active_required)
        build.names.used.update([*dims.values(), *measure_names.values(), *segment_names.values()])

        for dim in cube.dimensions:
            self._convert_dimension(build, dim, build.unmapped, report)
        for seg in cube.segments:
            self._convert_segment(build, seg, report)
        for meas in cube.measures:
            self._convert_measure(build, meas, report)
        return build

    def _finish_cube(self, build: _CubeBuild, report: CubeConversionReport) -> SlayerDataset | None:
        """Drop, resolve and type ``build``'s fields and metrics into the cube's dataset."""
        cube, source, meta, unmapped = build.cube, build.source, build.meta, build.unmapped
        self._active_refs, self._active_required = build.fp_refs, build.fp_required
        live = self._live_source(source)
        if isinstance(live, str):
            build.offline, live = live, None
            self._note_offline(build, report)
        self._drop_unreadable(build, live=live, report=report)
        self._resolve_markers(build)
        self._type_fields(build, live=live, report=report)
        columns, measures = self._validate_offline(build, report)
        self._pending_joins[cube.name] = self._join_keys(cube, report)

        if unmapped:
            meta["cube_unmapped"] = unmapped

        try:
            model = SlayerDataset(
                name=cube.name, data_source=self.data_source,
                hidden=not cube.public, description=cube.description,
                meta=meta or None, fields=columns, metrics=measures, relationships=[],
                sql_table=source.get("sql_table"), sql=source.get("sql"),
            )
        # Illegal dataset name etc. → report, don't crash the run.
        except Exception as exc:  # noqa: BLE001
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="error",
                cube=cube.name, message=f"Could not build dataset: {exc}",
            ))
            return None
        surviving = {m.name for m in measures}
        self._measure_info[cube.name] = {k: v for k, v in build.info.items() if v.emitted_name in surviving}
        self._report_filter_param_variables(cube, build.fp_refs, report)
        return model

    @staticmethod
    def _reserve_names(cube: CubeCube) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
        """Each dimension's, measure's and segment's emitted name, fixed before any SQL is translated."""
        names = _Names()
        dims = {d.name: names.take(d.name) for d in cube.dimensions
                if d.type not in ("switch", "geo") and not d.sub_query}
        measures = {m.name: names.take(m.name, suffix="_measure") for m in cube.measures}
        segments = {s.name: names.take(s.name, suffix="_seg") for s in cube.segments}
        return dims, measures, segments

    def _live_source(self, source: dict) -> LiveSource | str:
        """The cube's live schema, or why its types are not checked against the database."""
        sql = source.get("sql")
        return self._live.source(sql_table=source.get("sql_table"), sql=render_probe_text(sql) if sql else None)

    def _note_offline(self, build: _CubeBuild, report) -> None:
        """Report, once per reason, that a type was inferred without the database."""
        if build.offline is not None and build.offline not in self._offline_noted:
            self._offline_noted.add(build.offline)
            report.add(CubeConversionIssue(category=CubeIssueCategory.TYPING, severity="info",
                                           message=offline_note(build.offline)))

    def _build_meta_and_unmapped(
        self, cube: CubeCube, refs, report: CubeConversionReport
    ) -> tuple[dict, dict]:
        """Assemble the dataset ``meta`` (cube meta + title + FILTER_PARAMS
        variables) and the ``unmapped`` infra bag, reporting each stashed field."""
        meta: dict = dict(cube.meta or {})
        cube_vars = self._build_cube_variables(cube, refs)
        if cube_vars:
            meta["cube_variables"] = cube_vars
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
        for key in unmapped:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.UNMAPPED_INFRA, severity="warning",
                cube=cube.name, message=f"'{key}' has no SLayer equivalent; stashed in meta.",
                raw=str(unmapped[key]),
            ))
        return meta, unmapped

    # ── FILTER_PARAMS ────────────────────────────────────────────

    def _setup_filter_params(
        self, cube: CubeCube, report: CubeConversionReport
    ) -> tuple[CubeCube, list[CubeFilterParamRef]] | None:
        """Prepare + validate FILTER_PARAMS for a cube, priming the per-cube
        translate state. Returns ``(working_cube, refs)`` or ``None`` (a
        validation error was reported and the cube must be dropped)."""
        cube, refs = self._prepare_filter_params(cube)
        fp_error = self._validate_filter_params(cube, refs)
        if fp_error is not None:
            report.add(fp_error)
            return None
        self._active_refs = refs
        self._active_required = self._required_members(cube, refs)
        return cube, refs

    def _prepare_filter_params(
        self, cube: CubeCube
    ) -> tuple[CubeCube, list[CubeFilterParamRef]]:
        """Return ``(working_cube, refs)``. JS cubes arrive with structured refs
        + sentinelised surfaces; YAML cubes carry raw ``{FILTER_PARAMS...}`` text
        which is scanned here (on a deep copy) into refs + sentinels."""
        if cube.filter_params:
            return cube, list(cube.filter_params)
        if not self._cube_mentions_filter_params(cube):
            return cube, []
        cube = cube.model_copy(deep=True)
        refs: list[CubeFilterParamRef] = []

        def scan(text: str) -> str:
            if "{FILTER_PARAMS." not in text:
                return text
            ext = parse_string_filter_params(
                text, host_cube=cube.name, start_index=len(refs))
            refs.extend(ext.refs)
            # Unsupported occurrences are left verbatim; _validate_filter_params
            # sees the still-present {FILTER_PARAMS...} text and drops the cube.
            return ext.text

        def scan_opt(text: str | None) -> str | None:
            return scan(text) if text else text

        cube.sql = scan_opt(cube.sql)
        for dim in cube.dimensions:
            dim.sql = scan_opt(dim.sql)
            for when in _case_when_predicates(dim):
                when["sql"] = scan_opt(when.get("sql"))
        for meas in cube.measures:
            meas.sql = scan_opt(meas.sql)
            for f in meas.filters:
                f.sql = scan(f.sql)
        for seg in cube.segments:
            seg.sql = scan(seg.sql)
        return cube, refs

    def _cube_mentions_filter_params(self, cube: CubeCube) -> bool:
        surfaces = [cube.sql]
        surfaces += [d.sql for d in cube.dimensions]
        surfaces += [w.get("sql") for d in cube.dimensions for w in _case_when_predicates(d)]
        surfaces += [m.sql for m in cube.measures]
        surfaces += [f.sql for m in cube.measures for f in m.filters]
        surfaces += [s.sql for s in cube.segments]
        return any(s and "{FILTER_PARAMS." in s for s in surfaces)

    def _validate_filter_params(
        self, cube: CubeCube, refs: list[CubeFilterParamRef]
    ) -> CubeConversionIssue | None:
        """Validate FILTER_PARAMS refs; return a drop-the-cube issue or None."""
        if self._cube_mentions_filter_params(cube):
            return CubeConversionIssue(
                category=CubeIssueCategory.FILTER_PARAMS_UNSUPPORTED, severity="error",
                cube=cube.name,
                message="Unsupported FILTER_PARAMS form (cross-cube, arrow-in-YAML, "
                        "or unparseable); cube dropped.")
        valid_members = {d.name for d in cube.dimensions} | {m.name for m in cube.measures}
        var_owner: dict[str, str] = {}
        for ref in refs:
            if ref.cube != cube.name:
                return CubeConversionIssue(
                    category=CubeIssueCategory.FILTER_PARAMS_UNSUPPORTED, severity="error",
                    cube=cube.name, member=ref.member,
                    message=f"Cross-cube FILTER_PARAMS reference "
                            f"'{ref.cube}.{ref.member}' (host cube is '{cube.name}'); "
                            f"cube dropped.")
            if ref.member not in valid_members:
                return CubeConversionIssue(
                    category=CubeIssueCategory.FILTER_PARAMS_UNSUPPORTED, severity="error",
                    cube=cube.name, member=ref.member,
                    message=f"FILTER_PARAMS references unknown member "
                            f"'{ref.member}'; cube dropped.")
            for var in ref.var_names:
                prior = var_owner.get(var)
                if prior is not None and prior != ref.member:
                    return CubeConversionIssue(
                        category=CubeIssueCategory.FILTER_PARAMS_UNSUPPORTED, severity="error",
                        cube=cube.name, member=ref.member,
                        message=f"FILTER_PARAMS variable '{var}' is generated by both "
                                f"member '{prior}' and '{ref.member}'; cube dropped.")
                var_owner[var] = ref.member
        return None

    def _required_members(self, cube: CubeCube, refs) -> set[str]:
        if not self.honor_required_meta:
            return set()
        return {r.member for r in refs if self._member_required(cube, r.member)}

    def _member_required(self, cube: CubeCube, member: str) -> bool:
        entity = next((d for d in cube.dimensions if d.name == member), None) \
            or next((m for m in cube.measures if m.name == member), None)
        return bool(entity and (entity.meta or {}).get("required"))

    def _member_description(self, cube: CubeCube, member: str) -> str | None:
        entity = next((d for d in cube.dimensions if d.name == member), None) \
            or next((m for m in cube.measures if m.name == member), None)
        return entity.description if entity is not None else None

    def _build_cube_variables(self, cube: CubeCube, refs) -> dict:
        """Stash one entry per emitted variable under ``meta.cube_variables``.

        ``list_valued`` is the front-end-NEUTRAL half of the contract, and the
        only field the engine reads: the string form emits the fixed template
        ``col IN ({var})``, whose parentheses the importer — not the caller —
        wrote, so the caller cannot supply the per-element quotes a scalar
        placeholder normally expects. Flagging it lets the engine coerce a bare
        scalar to a one-element list instead of rendering an unquoted identifier.
        ``kind`` stays Cube's own taxonomy, for the report/humans.
        """
        required = self._required_members(cube, refs)
        out: dict = {}
        for ref in refs:
            for var in ref.var_names:
                out[var] = {
                    "member": ref.member,
                    "required": ref.member in required,
                    "kind": ref.kind,
                    "list_valued": ref.kind == "string",
                    "description": self._member_description(cube, ref.member),
                }
        return out

    def _report_filter_param_variables(self, cube, refs, report) -> None:
        # Dedup by variable NAME, not by member: one member can generate several
        # variables across refs (e.g. a scalar arrow yielding `_from` and a range
        # arrow yielding `_from`+`_to`), and each logical variable must be
        # reported exactly once so the report matches meta.cube_variables.
        seen_vars: set[str] = set()
        required = self._active_required
        for ref in refs:
            new_vars = [v for v in ref.var_names if v not in seen_vars]
            if not new_vars:
                continue
            seen_vars.update(new_vars)
            req = "required" if ref.member in required else "optional"
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.FILTER_PARAMS_VARIABLE, severity="info",
                cube=cube.name, member=ref.member,
                message=f"FILTER_PARAMS member '{ref.member}' → {req} variable(s) "
                        f"{new_vars}."))

    def _resolve_fp(self, translated: str) -> str:
        """Replace FILTER_PARAMS sentinels in already-``translate_cube_refs``-ed
        text with their SLayer Mode-A form (block / bare). A no-op when the text
        carries no sentinels."""
        return apply_filter_params(
            translated, self._active_refs, required_members=self._active_required)

    def _cube_source(self, cube: CubeCube, report) -> dict | None:
        if cube.sql_table:
            return {"sql_table": cube.sql_table}
        if cube.sql:
            translated = self._resolve_fp(
                translate_cube_refs(cube.sql, mode="sql", cube=cube.name))
            try:
                sqlglot.parse_one(render_probe_text(translated))
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

    # ── member SQL ─────────────────────────────────────────────────────────

    def _refs(self, build: _CubeBuild, text: str) -> CubeSql:
        """``text``'s Cube refs classified, its markers recorded on ``build``."""
        found = classify_cube_refs(text, cube=build.cube.name, members=build.emitted,
                                   start=len(build.markers) + len(build.foreign))
        build.markers.update(found.members)
        build.foreign.update(found.foreign)
        return found

    def _mode_a(self, build: _CubeBuild, field: str, text: str) -> str:
        """Mode-A SQL (member markers kept, FILTER_PARAMS resolved), noting undeclared members ``field`` reads."""
        found = self._refs(build, text)
        if found.unknown:
            build.unknown.setdefault(field, []).extend(found.unknown)
        return self._resolve_fp(found.text)

    # ── dimensions ─────────────────────────────────────────────────────────

    def _convert_dimension(self, build: _CubeBuild, dim: CubeDimension, unmapped, report) -> None:
        cube = build.cube
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
                message="Custom granularities are query-time in SLayer; base field kept."))

        name = build.emitted[dim.name]
        if dim.case:
            sql: str | None = self._build_case_sql(build, name, dim.case)
            build.fixed.add(name)
        elif dim.sql:
            sql = self._mode_a(build, name, dim.sql)
            sql = None if sql == name else sql
        else:
            sql = None
        declared = _DIM_TYPE_MAP.get(dim.type, DataType.TEXT)
        build.dimensions.add(name)
        if "type" in dim.model_fields_set:
            build.declared[name] = (declared, None)
        build.fields.append(DatasetField(
            name=name, sql=sql, datatype=declared,
            primary_key=dim.primary_key, hidden=not dim.public,
            label=dim.title, description=dim.description, meta=dim.meta,
            format=_map_format(dim.format, report, cube=cube.name, member=dim.name),
        ))

    def _build_case_sql(self, build: _CubeBuild, field: str, case: dict) -> str:
        parts = ["CASE"]
        for when in case.get("when", []):
            cond = self._mode_a(build, field, when.get("sql", ""))
            parts.append(f"WHEN {cond} THEN {_sql_str_literal(when.get('label', ''))}")
        if case.get("else"):
            parts.append(f"ELSE {_sql_str_literal(case['else'].get('label', ''))}")
        parts.append("END")
        return " ".join(parts)

    # ── segments ───────────────────────────────────────────────────────────

    def _convert_segment(self, build: _CubeBuild, seg, report) -> None:
        name = build.segment_names[seg.name]
        build.fixed.add(name)
        build.fields.append(DatasetField(
            name=name, sql=self._mode_a(build, name, seg.sql),
            datatype=DataType.BOOLEAN, hidden=not seg.public,
            label=seg.title, description=seg.description, meta=seg.meta))
        report.add(CubeConversionIssue(
            category=CubeIssueCategory.SEGMENT_AS_COLUMN, severity="info",
            cube=build.cube.name, member=seg.name,
            message=f"Segment '{seg.name}' mapped to a boolean field."))

    # ── measures ───────────────────────────────────────────────────────────

    def _convert_measure(self, build: _CubeBuild, meas: CubeMeasure, report) -> None:
        cube = build.cube
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
        try:
            if meas.type in _CALC_TYPES and meas.sql:
                self._convert_calc_measure(build, meas, report)
            else:
                self._convert_agg_measure(build, meas, report)
        except Unconvertible as exc:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning", cube=cube.name, member=meas.name,
                message=f"Measure '{meas.name}' dropped: {exc}."))

    def _convert_agg_measure(self, build: _CubeBuild, meas: CubeMeasure, report) -> None:
        cube = build.cube
        agg = meas.type

        window = _window_from_rolling(meas.rolling_window, report, cube=cube.name, member=meas.name) \
            if meas.rolling_window else None
        suffix = _windowed(agg, window)
        name = build.measure_names[meas.name]

        if meas.type == "count" and not meas.sql:
            if self._emit_metric(build, name=name, expression=functional_agg_text(source="*", suffix=suffix),
                                  meas=meas, report=report):
                build.info[meas.name] = _MeasureInfo(kind="star_count", suffix=suffix, emitted_name=name)
            return

        operand = self._refs(build, meas.sql or meas.name)
        filters = [self._refs(build, f.sql) for f in meas.filters]
        if unknown := [*operand.unknown, *(m for f in filters for m in f.unknown)]:
            raise Unconvertible(f"it references undeclared member(s) {sorted(set(unknown))}")
        fmt = _map_format(meas.format, report, cube=cube.name, member=meas.name)
        try:
            parsed: exp.Expr | None = self._parse(operand.text)
        except Unconvertible:
            parsed = None  # kept as a helper field, dropped (and reported) with it
        if not filters and isinstance(parsed, exp.Column):
            col_name = self._bare_source(build, parsed, agg=agg, fmt=fmt)
        else:
            predicate = " AND ".join(self._resolve_fp(f.text) for f in filters) or None
            col_name = self._helper(build, metric=meas.name, sql=self._resolve_fp(operand.text), predicate=predicate,
                                    agg=agg, fmt=fmt)
        build.sources[name].append((col_name, agg))
        if self._emit_metric(build, name=name, expression=functional_agg_text(source=col_name, suffix=suffix),
                              meas=meas, report=report):
            build.info[meas.name] = _MeasureInfo(kind="agg", underlying_col=col_name, suffix=suffix,
                                                 emitted_name=name)

    def _convert_calc_measure(self, build: _CubeBuild, meas: CubeMeasure, report) -> None:
        name = build.measure_names[meas.name]
        found = self._refs(build, meas.sql or "")
        if found.unknown:
            raise Unconvertible(f"it references undeclared member(s) {sorted(set(found.unknown))}")
        if "\x00" in found.text:
            raise Unconvertible("FILTER_PARAMS in a calculated measure has no expression form")
        policy = _CubeOperands(converter=self, build=build, metric=meas.name)
        result = convert_aggregates(found.text, policy=policy, sql_dialect=self._sql_dialect, dialect=self._dialect)
        if not result.ok or result.expression is None:
            raise Unconvertible(f"its SQL {meas.sql!r} has no expression form ({result.reason})")
        if self._emit_metric(build, name=name, expression=result.expression, meas=meas, report=report,
                              result_type=_DIM_TYPE_MAP.get(meas.type)):
            build.info[meas.name] = _MeasureInfo(kind="calc", emitted_name=name)

    def _parse(self, text: str) -> exp.Expr:
        try:
            return sqlglot.parse_one(render_probe_text(self._resolve_fp(text)), read=self._dialect)
        except SqlglotError as exc:
            raise Unconvertible(f"its SQL does not parse ({exc})") from exc

    def _bare_source(self, build: _CubeBuild, node: exp.Column, *, agg: str, fmt: NumberFormat | None = None) -> str:
        """The field ``agg`` aggregates for one bare reference (a member, a joined cube's field, or physical)."""
        if not node.table and node.name in build.foreign:
            return node.name  # resolved once every cube is built
        member = build.markers.get(node.name) if not node.table else None
        if member is not None:
            col = build.emitted.get(member)
            if col is None or not any(c.name == col for c in build.fields):
                raise Unconvertible(f"it aggregates {member!r}, which is not a field of the cube")
            return col
        if node.table:
            return f"{node.table}.{node.name}"
        index = PhysicalIndex(build.fields, dialect=self._dialect)
        reader = index.reader(node.this)
        if reader is not None:
            build.field_aggs[reader.name].append(agg)
            return reader.name
        if (problem := index.problem(node.this)) is not None:
            raise Unconvertible(problem)
        name = self._add_physical(build, node.this, type=DataType.DOUBLE, format=fmt)
        build.field_aggs[name].append(agg)
        return name

    def _add_physical(self, build: _CubeBuild, physical: exp.Identifier, **field: Any) -> str:
        """A new base field of ``build`` reading ``physical``, named after it when the name is free."""
        name = build.names.take(physical.name) if physical.name not in build.names.used \
            else build.names.take(f"{physical.name}_col")
        sql = None if name == physical.name and not physical.quoted else physical.sql(dialect=self._dialect)
        build.fields.append(DatasetField(name=name, sql=sql, **field))
        return name

    def _resolve_foreign(self, build: _CubeBuild, builds: dict[str, _CubeBuild]) -> None:
        """Each ``{other}.x`` as ``other.<the field reading x>``, adding a hidden one to ``other`` when none does."""
        joined = {j.name for j in build.cube.joins}
        for marker, ref in build.foreign.items():
            target = builds.get(ref.cube) if ref.cube in joined else None
            if target is None:
                build.foreign_problems[marker] = f"it reads cube {ref.cube!r}, which {build.cube.name!r} does not join"
                continue
            column, problem = self._foreign_field(target, ref)
            if problem is not None:
                build.foreign_problems[marker] = problem
            else:
                build.foreign_sql[marker] = f"{ref.cube}.{column}"

    def _foreign_field(self, target: _CubeBuild, ref: ForeignRef) -> tuple[str | None, str | None]:
        """``(field of target reading ref's physical column, problem)``."""
        try:
            node = sqlglot.parse_one(ref.column, read=self._dialect)
        except SqlglotError:
            node = None
        if not isinstance(node, exp.Column) or not isinstance(node.this, exp.Identifier):
            return None, f"{ref.column!r} is a reserved word; quote it to read the physical column of {ref.cube!r}"
        index = PhysicalIndex(target.fields, dialect=self._dialect)
        if (reader := index.reader(node.this)) is not None:
            return reader.name, None
        if (problem := index.problem(node.this)) is not None:
            return None, f"in cube {ref.cube!r}, {problem}"
        name = self._add_physical(target, node.this, hidden=True)
        target.dimensions.add(name)
        return name, None

    def _helper(self, build: _CubeBuild, *, metric: str, sql: str, predicate: str | None, agg: str,
                fmt: NumberFormat | None = None) -> str:
        """A hidden Mode-A field holding ``sql`` over the rows ``predicate`` keeps, shared per (sql, filter, type)."""
        implied = implied_type(agg)
        key = (sql, predicate, implied)
        name = build.helpers.get(key)
        if name is None:
            name = build.helpers[key] = build.names.take(f"{metric}_col")
            build.fields.append(DatasetField(name=name, sql=sql, filter=predicate, format=fmt, hidden=True,
                                        datatype=implied[0] if implied else DataType.DOUBLE))
        build.field_aggs[name].append(agg)
        return name

    def _emit_metric(self, build: _CubeBuild, *, name: str, expression: str, meas, report, result_type=None) -> bool:
        try:
            build.metrics.append(DatasetMetric(
                name=name, expression=expression, label=meas.title,
                description=meas.description, datatype=result_type, meta=meas.meta))
            return True
        except Exception as exc:  # noqa: BLE001
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                cube=build.cube.name, member=meas.name,
                message=f"Measure '{meas.name}' could not be built: {exc}"))
            return False

    # ── meaning + typing ───────────────────────────────────────────────────

    def _drop_unreadable(self, build: _CubeBuild, *, live: LiveSource | None, report) -> None:
        """Drop fields reading a masked physical column or an undeclared member, then whatever reads a dropped one."""
        problems = unreadable_fields(build.fields, dialect=self._dialect,
                                      live=live.names if live is not None else (),
                                      logical=frozenset([*build.markers, *build.foreign]), prepare=render_probe_text)
        problems.update({c: f"it references undeclared member(s) {sorted(set(m))}" for c, m in build.unknown.items()})
        problems.update({c.name: p for c in build.fields for mk, p in build.foreign_problems.items()
                         if mk in f"{c.sql} {c.filter}"})
        self._drop_foreign_metrics(build, report)
        member_of = {v: k for k, v in build.emitted.items()}
        while True:
            dropped_members = {member_of.get(n, n) for n in problems}
            more = {c.name: f"it reads the dropped member {member!r}" for c in build.fields
                    if c.name not in problems
                    for member in [next((build.markers[mk] for mk in build.markers
                                         if mk in f"{c.sql} {c.filter}" and build.markers[mk] in dropped_members),
                                        None)]
                    if member is not None}
            if not more:
                break
            problems.update(more)
        for name, problem in problems.items():
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_SQL, severity="warning", cube=build.cube.name,
                member=member_of.get(name, name), message=f"Field {name!r} dropped: {problem}."))
        build.fields = [c for c in build.fields if c.name not in problems]

    def _drop_foreign_metrics(self, build: _CubeBuild, report) -> None:
        """Drop the metrics reading a joined cube's physical column that cannot be read."""
        kept: list[DatasetMetric] = []
        for m in build.metrics:
            problem = next((p for mk, p in build.foreign_problems.items() if mk in m.expression), None)
            if problem is None:
                kept.append(m)
                continue
            report.add(CubeConversionIssue(category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                                           cube=build.cube.name, member=m.name,
                                           message=f"Measure {m.name!r} dropped: {problem}."))
        build.metrics = kept

    def _resolve_markers(self, build: _CubeBuild) -> None:
        """Replace member markers with their members' emitted names, and joined cubes' physical columns."""
        found = CubeSql(text="", members=build.markers)

        def resolve(text: str | None) -> str | None:
            if not text:
                return text
            out = found.model_copy(update={"text": text}).resolved(build.emitted)
            for marker, sql in build.foreign_sql.items():
                out = out.replace(marker, sql)
            return out

        for col in build.fields:
            col.sql, col.filter = resolve(col.sql), resolve(col.filter)
        for m in build.metrics:
            m.expression = resolve(m.expression) or m.expression

    def _type_fields(self, build: _CubeBuild, *, live: LiveSource | None, report) -> None:
        """Type each field from the database, then the Cube dimension type, then its aggregations; drop the metrics
        SLayer cannot read over the result."""
        index = PhysicalIndex(build.fields, dialect=self._dialect)
        untyped: set[str] = set()
        for col in build.fields:
            if (col.name not in build.fixed and not self._type_field(build, index, col, live=live, report=report)
                    and col.name not in build.dimensions):
                untyped.add(col.name)
        build.metrics = self._readable_metrics(build, untyped, report)
        used = {col for m in build.metrics for col, _ in build.sources.get(m.name or "", [])}
        build.fields = [c for c in build.fields if c.name not in untyped or c.name in used]

    def _type_field(self, build: _CubeBuild, index: PhysicalIndex, col: DatasetField, *, live: LiveSource | None,
                     report) -> bool:
        """Type ``col``; ``False`` when nothing types it."""
        found = self._live_type(col, live=live)
        declared = build.declared.get(col.name) or self._dimension_type(build, index, col)
        if found is not None and declared is not None and contradicts(declared, found):
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.TYPING, severity="info", cube=build.cube.name,
                member=col.name, message=contradiction_note(col.name, declared=declared, live=found)))
        found = found or declared or _implied(build.field_aggs.get(col.name, []))
        if found is None:
            return False
        col.datatype, col.element_type = found
        if col.datatype not in (DataType.INT, DataType.DOUBLE):
            col.format = None
        return True

    def _readable_metrics(self, build: _CubeBuild, untyped: set[str], report) -> list[DatasetMetric]:
        """``build``'s metrics SLayer can read over their typed sources; the rest are reported."""
        kept: list[DatasetMetric] = []
        for m in build.metrics:
            problem = next((p for col_name, agg in build.sources.get(m.name or "", [])
                            if (p := self._source_problem(build, m.name or "", col_name, agg, untyped)) is not None),
                           None)
            if problem is None:
                kept.append(m)
                continue
            report.add(CubeConversionIssue(category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                                           cube=build.cube.name, member=m.name, message=problem))
        return kept

    def _live_type(self, col: DatasetField, *, live: LiveSource | None) -> Typed | None:
        if live is None:
            return None
        if col.is_base and col.filter is None:
            return live.field(physical_identifier(col, dialect=self._dialect))
        return live.probe(render_probe_text(col.sql)) if col.sql else None

    @staticmethod
    def _dimension_type(build: _CubeBuild, index: PhysicalIndex, col: DatasetField) -> Typed | None:
        """The declared type of a dimension reading the same physical column as ``col``."""
        if not col.is_base or col.filter is not None:
            return None
        reader = index.reader(exp.to_identifier(col.physical_name))
        return build.declared.get(reader.name) if reader is not None else None

    @staticmethod
    def _source_problem(build: _CubeBuild, metric: str, col_name: str, agg: str, untyped: set[str]) -> str | None:
        col = next((c for c in build.fields if c.name == col_name), None)
        if col is not None and col.datatype is DataType.ARRAY and col.name not in untyped and refused_over_list(agg):
            return list_refusal(metric, agg=agg, field=col_name)
        if col_name in untyped and implied_type(agg) is None:
            return untyped_message(metric, agg=agg, field=col_name)
        return None

    # ── joins ──────────────────────────────────────────────────────────────

    def _join_keys(self, cube: CubeCube, report) -> list[tuple[str, str | None, str, list[tuple[JoinSide, JoinSide]]]]:
        """Each join's key sides, parsed now and resolved once every cube is built."""
        out = []
        for cj in cube.joins:
            keys = parse_join_keys(cj.sql, source_cube=cube.name, target_cube=cj.name)
            if keys is None:
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.UNSUPPORTED_JOIN, severity="warning", cube=cube.name, member=cj.name,
                    message=f"Join ON '{cj.sql}' is not an equality of columns; dropped."))
                continue
            out.append((cj.name, cj.relationship, cj.sql, keys))
        return out

    def _resolve_relationships(self, datasets: list[SlayerDataset], report) -> None:
        """Map each join's key sides to base fields of both cubes, adding a hidden one for a free physical name."""
        by_name = {m.name: m for m in datasets}
        for model in datasets:
            for target_name, relationship, on_sql, keys in self._pending_joins.get(model.name, []):
                target = by_name.get(target_name)
                if target is None:
                    report.add(CubeConversionIssue(
                        category=CubeIssueCategory.UNSUPPORTED_JOIN, severity="warning",
                        cube=model.name, member=target_name,
                        message=f"Join target cube '{target_name}' is not available; dropped."))
                    continue
                pairs = [(self._relationship_key(model, src), self._relationship_key(target, tgt)) for src, tgt in keys]
                problem = next((p for pair in pairs for _, p in pair if p is not None), None)
                if problem is not None:
                    report.add(CubeConversionIssue(
                        category=CubeIssueCategory.UNSUPPORTED_JOIN, severity="warning",
                        cube=model.name, member=target_name,
                        message=f"Join ON '{on_sql}' dropped: {problem}."))
                    continue
                for (src, _), (tgt, _) in pairs:
                    self._declare_hidden_key(model, src)
                    self._declare_hidden_key(target, tgt)
                cardinality = _map_relationship(relationship)
                if cardinality is None:
                    report.add(CubeConversionIssue(
                        category=CubeIssueCategory.LOSSY_MAPPING, severity="warning",
                        cube=model.name, member=target_name,
                        message=(f"Unrecognized join relationship {relationship!r}; "
                                 f"cardinality left unset (metrics crossing it broadcast).")))
                model.relationships.append(DatasetRelationship(
                    to=target_name, join_pairs=[[s, t] for (s, _), (t, _) in pairs],
                    join_type=JoinType.LEFT, cardinality=cardinality))

    def _drop_unreachable(self, datasets: list[SlayerDataset], report) -> None:
        """Drop fields reading a path no join reaches, then those reading a dropped field."""
        dropped: dict[str, set[str]] = defaultdict(set)
        while found := {m.name: p for m in datasets if (p := unreachable_fields(
                m, datasets_by_name=self._models, dialect=self._dialect, dropped=frozenset(dropped[m.name]),
                prepare=render_probe_text))}:
            for model in datasets:
                for name, problem in found.get(model.name, {}).items():
                    report.add(CubeConversionIssue(category=CubeIssueCategory.COMPLEX_SQL, severity="warning",
                                                   cube=model.name, member=name,
                                                   message=f"Field {name!r} dropped: {problem}."))
                    dropped[model.name].add(name)
                model.fields = [c for c in model.fields if c.name not in dropped[model.name]]

    def _relationship_key(self, dataset: SlayerDataset, side: JoinSide) -> tuple[str, str | None]:
        """``(field name, problem)`` for one join key side of ``dataset``."""
        cube = self._cubes.get(dataset.name)
        if not side.physical and cube is not None and side.name in self._reserve_names(cube)[0]:
            col = dataset.get_field(self._reserve_names(cube)[0][side.name])
            if col is None or not col.is_base or col.filter is not None:
                return side.name, f"member {side.name!r} of {dataset.name!r} is not a base field"
            return col.name, None
        index = PhysicalIndex(dataset.fields, dialect=self._dialect)
        ident = exp.to_identifier(side.name)
        reader = index.reader(ident)
        if reader is not None:
            return reader.name, None
        if (problem := index.problem(ident)) is not None:
            return side.name, problem
        if dataset.get_metric(side.name) is not None or not IDENTIFIER_RE.match(side.name):
            return side.name, f"no field of {dataset.name!r} can read the physical column {side.name!r}"
        return side.name, None

    @staticmethod
    def _declare_hidden_key(dataset: SlayerDataset, name: str) -> None:
        if dataset.get_field(name) is None:
            dataset.fields.append(DatasetField(name=name, hidden=True))

    def _dedup_inverse_relationships(self, datasets: list[SlayerDataset]) -> None:
        """Collapse each mutually-inverse declaration pair to its to-one edge."""
        by_name = {m.name: m for m in datasets}
        for model in datasets:
            for join in list(model.relationships):  # NOSONAR(S7504) — materialised before in-place removal
                peer = by_name.get(join.to)
                if peer is None or peer.name <= model.name:
                    continue  # visit each unordered pair once
                _drop_inverse_half(dataset=model, relationship=join, peer=peer)

    # ── offline validation ─────────────────────────────────────────────────

    def _validate_offline(self, build: _CubeBuild, report) -> tuple[list[DatasetField], list[DatasetMetric]]:
        good_cols, dropped = self._validate_fields(build.cube.name, build.fields, report)
        return good_cols, self._drop_metrics_over(
            cube_name=build.cube.name, metrics=build.metrics, info=build.info, dropped=dropped, report=report)

    def _validate_fields(self, cube_name, fields, report) -> tuple[list[DatasetField], set[str]]:
        good_cols = []
        dropped: set[str] = set()
        for col in fields:
            if col.sql is None:
                good_cols.append(col)
                continue
            try:
                # A field sql may carry {? ?} blocks / {var} placeholders;
                # probe-render before the syntax-only parse.
                sqlglot.parse_one(render_probe_text(col.sql))
                good_cols.append(col)
            except Exception:  # noqa: BLE001
                dropped.add(col.name)
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_SQL, severity="warning",
                    cube=cube_name, member=col.name,
                    message=f"Field sql does not parse; dropped: {col.sql!r}"))
        return good_cols, dropped

    def _drop_metrics_over(self, *, cube_name, metrics: list[DatasetMetric], info, dropped, report) -> list[DatasetMetric]:
        by_emitted = {i.emitted_name: i for i in info.values() if i.emitted_name}
        good_measures = []
        for m in metrics:
            col = by_emitted[m.name].underlying_col if m.name in by_emitted else None
            if col in dropped:
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                    cube=cube_name, member=m.name,
                    message=f"Metric aggregates dropped field {col!r}; dropped: {m.expression!r}"))
            else:
                good_measures.append(m)
        return good_measures

    def _drop_unparseable_metrics(self, cube_name, metrics: list[DatasetMetric], report, *, dataset: SlayerDataset | None = None
                                   ) -> list[DatasetMetric]:
        """``metrics`` that resolve among their peers (and, given ``dataset``, its fields and relationship paths)."""
        if dataset is not None:
            problems = metric_problems(dataset.model_copy(update={"metrics": metrics}), datasets_by_name=self._models,
                                        dialect=self._sql_dialect)
        else:
            problems = metric_dependency_problems([(m.name, m.expression) for m in metrics])
        for m, problem in zip(metrics, problems):
            if problem is not None:
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                    cube=cube_name, member=m.name,
                    message=f"Metric expression does not resolve ({problem}); dropped: {m.expression!r}"))
        return [m for m, problem in zip(metrics, problems) if problem is None]

    def _validate_metrics(self, dataset: SlayerDataset, report) -> None:
        """Drop metrics that do not resolve among the dataset's metrics, fields and relationship paths."""
        dataset.metrics = self._drop_unparseable_metrics(dataset.name, dataset.metrics, report, dataset=dataset)
        surviving = {m.name for m in dataset.metrics}
        info = self._measure_info.get(dataset.name, {})
        self._measure_info[dataset.name] = {k: v for k, v in info.items() if v.emitted_name in surviving}

    # ── views → facade datasets ──────────────────────────────────────────────

    def _convert_view(self, view: CubeView, report) -> SlayerDataset | None:
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

        meta: dict[str, Any] = {"cube_kind": "view"}
        unmapped: dict = {}
        if view.folders:
            unmapped["folders"] = view.folders
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.FOLDERS_UNMAPPED, severity="info",
                view=view.name, message="Folders have no SLayer hierarchy; stashed in meta."))

        names = _Names()
        fields: list[DatasetField] = []
        metrics: list[DatasetMetric] = []
        relationship_targets: set[str] = set()

        for ref in view.cubes:
            self._convert_view_ref(view, ref, root_cube_name, root_model,
                                   fields, metrics, names, relationship_targets, report)

        joins = [
            j for j in root_model.relationships
            if j.to in relationship_targets and self._facade_join_keys(
                join=j, root_dataset=root_model, fields=fields, names=names,
                view=view, report=report)
        ]
        filters = self._view_default_filters(view, root_cube_name, report)
        if unmapped:
            meta["cube_unmapped"] = unmapped

        try:
            return SlayerDataset(
                name=view.name, data_source=self.data_source,
                hidden=not view.public, description=view.description, meta=meta,
                fields=fields, metrics=metrics, relationships=joins, filters=filters,
                sql_table=root_model.sql_table or None,
                sql=None if root_model.sql_table else root_model.sql,
            )
        except Exception as exc:  # noqa: BLE001
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="error",
                view=view.name, message=f"Could not build facade dataset: {exc}"))
            return None

    def _facade_join_keys(self, *, join: DatasetRelationship, root_dataset: SlayerDataset, fields, names, view, report) -> bool:
        """Declare a carried join's source keys on the facade (hidden); ``False`` drops the join."""
        for src, _ in join.join_pairs:
            col = next((c for c in fields if c.name == src), None)
            root_col = root_dataset.get_field(src)
            if col is None and root_col is not None and src not in names.used:
                names.reserve(src)
                fields.append(DatasetField(name=src, sql=root_col.sql, datatype=root_col.datatype,
                                      hidden=True))
            elif col is None or not col.is_base or col.filter is not None:
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.UNSUPPORTED_JOIN, severity="warning",
                    view=view.name, member=join.to,
                    message=f"Join key '{src}' is not a base field of the facade; join dropped."))
                return False
        return True

    def _convert_view_ref(self, view: CubeView, ref: CubeViewCubeRef, root_cube_name: str, root_dataset: SlayerDataset,
                          fields, metrics, names, relationship_targets, report) -> None:
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
            if join is None or not any(j.to == cube_name for j in root_dataset.relationships):
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.DISCONNECTED_VIEW, severity="warning",
                    view=view.name, member=ref.join_path,
                    message=f"'{cube_name}' is not joined to root '{root_cube_name}'; skipped."))
                return
            if join.relationship in ("one_to_many", "has_many"):
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.VIEW_FANOUT_RISK, severity="warning",
                    view=view.name, member=ref.join_path,
                    message=f"Join to '{cube_name}' is {join.relationship}; root metrics may fan out."))
            relationship_targets.add(cube_name)

        prefix = f"{ref.alias or cube_name}_" if ref.prefix else ""
        dim_names, meas_names = self._selected_members(cube, ref, view, report)

        for dname in dim_names:
            self._facade_dimension(cube, cube_model, dname, prefix, is_root,
                                   fields, names, view, report)
        for mname in meas_names:
            self._facade_metric(view, cube_name, cube_model, mname, prefix, is_root,
                                 fields, metrics, names, report)

    def _selected_members(self, cube: CubeCube, ref: CubeViewCubeRef, view, report) -> tuple[list[str], list[str]]:
        dims = [d.name for d in cube.dimensions if d.type not in ("geo", "switch") and not d.sub_query]
        meas = [m.name for m in cube.measures]
        exclude = set(ref.excludes or [])
        if ref.includes in (None, "*"):
            # `includes: "*"` must not re-export members the cube marked private.
            private = {d.name for d in cube.dimensions if not d.public} \
                | {m.name for m in cube.measures if not m.public}
            chosen = [n for n in dims + meas if n not in exclude and n not in private]
        else:
            chosen = self._include_names(ref.includes, exclude, view, report)
        chosen_set = set(chosen)
        return ([d for d in dims if d in chosen_set], [m for m in meas if m in chosen_set])

    def _include_names(self, includes, exclude, view, report) -> list[str]:
        """Extract member names from an ``includes`` list. Cube's per-member
        override object form (``{name, format, meta, …}``) is accepted so it
        doesn't crash the view; the overrides are reported as unsupported. An
        entry with no valid ``name`` (Cube requires one) — e.g. ``{}`` or
        ``{"alias": ...}`` — is reported as a parse error rather than silently
        dropped or reported with a confusing ``member=None``."""
        names: list[str] = []
        for entry in includes:
            name = entry.get("name") if isinstance(entry, dict) else entry
            if not isinstance(name, str) or not name:
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.PARSE_ERROR, severity="warning",
                    view=view.name,
                    message=f"View include entry {entry!r} has no valid 'name'; skipped."))
                continue
            if isinstance(entry, dict) and any(k != "name" for k in entry):
                report.add(CubeConversionIssue(
                    category=CubeIssueCategory.UNMAPPED_INFRA, severity="info",
                    view=view.name, member=name,
                    message=f"Per-member override on '{name}' is not applied (Stage 1)."))
            if name not in exclude:
                names.append(name)
        return names

    def _facade_dimension(self, cube: CubeCube, cube_model: SlayerDataset, dname, prefix, is_root, fields, names, view, report) -> None:
        col = cube_model.get_field(dname)
        if col is None:
            return
        if is_root:
            sql = col.sql if col.sql else col.name
            if not self._carry(cube_model=cube_model, sql=[col.sql], fields=fields, names=names,
                               view=view, member=dname, report=report):
                return
        else:
            sql = f"{cube.name}.{col.name}"
        exported = names.take(f"{prefix}{dname}")
        fields.append(DatasetField(
            name=exported, sql=sql, datatype=col.datatype, label=col.label,
            description=col.description, format=col.format))

    def _facade_metric(self, view, cube_name, cube_model, mname, prefix, is_root,
                        fields, metrics, names, report) -> None:
        info = self._measure_info.get(cube_name, {}).get(mname)
        if info is None:
            return
        exported = names.take(f"{prefix}{mname}", suffix="_measure")
        if is_root and info.kind == "agg" and info.underlying_col and not self._carry(
                cube_model=cube_model, sql=[info.underlying_col], fields=fields, names=names,
                view=view, member=mname, report=report):
            return
        formula = self._facade_metric_expression(info=info, cube_name=cube_name, is_root=is_root)
        if formula is None:
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_MEASURE, severity="info",
                view=view.name, member=mname,
                message=f"Calculated measure '{mname}' re-export not supported in Stage 1."))
            return
        try:
            metrics.append(DatasetMetric(name=exported, expression=formula))
        except Exception as exc:  # noqa: BLE001
            report.add(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_MEASURE, severity="warning",
                view=view.name, member=mname, message=f"Facade metric failed: {exc}"))

    @staticmethod
    def _facade_metric_expression(*, info, cube_name, is_root) -> str | None:
        """Functional facade expression for a base measure; ``None`` for a calc measure."""
        if not info.suffix:
            return None
        if info.kind == "star_count":
            return functional_agg_text(source="*" if is_root else f"{cube_name}.*", suffix=info.suffix)
        if info.kind != "agg" or not info.underlying_col:
            return None
        source = info.underlying_col if is_root else f"{cube_name}.{info.underlying_col}"
        return functional_agg_text(source=source, suffix=info.suffix)

    def _carry(self, *, cube_model: SlayerDataset, sql: list[str | None], fields: list[DatasetField], names: _Names,
               view, member: str, report) -> bool:
        """Copy onto the facade, hidden, every cube field ``sql`` reads (transitively); ``False`` when one's name
        is already taken there by a different field."""
        pending = list(sql)
        while pending:
            text = pending.pop()
            if not text:
                continue
            try:
                refs = root_physical_columns(sqlglot.parse_one(render_probe_text(text), read=self._dialect))
            except SqlglotError:
                continue
            for ref in refs:
                col = cube_model.get_field(ref.name)
                taken = next((c for c in fields if c.name == ref.name), None)
                if col is None or (taken is not None and (taken.sql, taken.filter) == (col.sql, col.filter)):
                    continue
                if taken is not None:
                    report.add(CubeConversionIssue(
                        category=CubeIssueCategory.COMPLEX_SQL, severity="warning", view=view.name, member=member,
                        message=f"'{member}' reads {cube_model.name}.{col.name}, whose name the view already "
                                f"uses for another member; skipped."))
                    return False
                names.reserve(col.name)
                fields.append(col.model_copy(update={"hidden": True}))
                pending += [col.sql, col.filter]
        return True

    def _view_default_filters(self, view, root_cube_name, report) -> list[str]:
        filters: list[str] = []
        for df in view.default_filters or []:
            member = df.get("member", "")
            op = df.get("operator")
            values = df.get("values") or []
            col = self._resolve_view_member(member, root_cube_name)
            if op == "equals" and len(values) == 1:
                filters.append(f"{col} = {_sql_str_literal(values[0])}")
            elif op in ("equals", "in") and values:
                vlist = ", ".join(_sql_str_literal(v) for v in values)
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
