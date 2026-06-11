"""Schema drift validation for SLayer.

Diffs persisted SlayerModels against live database schemas and emits a
minimal list of *deletes* (drop columns / measures / joins / filters / models)
needed to keep SQL generation valid against the live state. See DEV-1356.

The public surface is ``SlayerQueryEngine.validate_models()`` (in
``query_engine.py``); this module owns the diff/cascade engine, the live-
schema introspection helpers, and the Pydantic payload types they share.

Read-only — never writes to storage.
"""

from __future__ import annotations

import asyncio
import logging
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)

import sqlalchemy as sa
import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.formula import (
    AggregatedMeasureRef,
    ArithmeticField,
    MixedArithmeticField,
    TransformField,
    parse_filter,
    parse_formula,
)
from slayer.core.models import (
    Column,
    DatasourceConfig,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.engine.ingestion import (
    _safe_get_columns,
    _safe_get_pk_constraint,
    _sa_type_is_float,
    _sa_type_to_data_type,
)
from slayer.sql.client import SlayerSQLClient

logger = logging.getLogger(__name__)


# ===========================================================================
# Public payload types
# ===========================================================================


class DeleteReason(BaseModel):
    """Reason attached to a delete entry — surfaces in CLI / MCP / REST output."""

    target: str  # e.g. "column:status", "measure:aov", "join:customers", "model:orders"
    reason: str


class RemoveSpec(BaseModel):
    """Per-entity removal spec, mirroring the MCP ``edit_model`` ``remove=`` shape."""

    columns: List[str] = Field(default_factory=list)
    measures: List[str] = Field(default_factory=list)
    aggregations: List[str] = Field(default_factory=list)
    joins: List[str] = Field(default_factory=list)


class EditModelDelete(BaseModel):
    """Surgical removals on an existing model. Replays as ``edit_model``."""

    tool: Literal["edit_model"] = "edit_model"
    model_name: str
    data_source: str
    remove: RemoveSpec = Field(default_factory=RemoveSpec)
    remove_filters: List[str] = Field(default_factory=list)
    reasons: List[DeleteReason] = Field(default_factory=list)


class WholeModelDelete(BaseModel):
    """Whole-model removal. Replays as ``delete_model``."""

    tool: Literal["delete_model"] = "delete_model"
    model_name: str
    data_source: str
    reasons: List[DeleteReason] = Field(default_factory=list)


ToDeleteEntry = Annotated[
    Union[EditModelDelete, WholeModelDelete], Field(discriminator="tool")
]


class ModelAddition(BaseModel):
    """One model touched by an idempotent re-ingestion pass."""

    model_name: str
    data_source: str
    created: bool = False  # True if the model was new
    new_columns: List[str] = Field(default_factory=list)
    new_joins: List[str] = Field(default_factory=list)
    # DEV-1538: persisted INT columns whose type widened (to DOUBLE or TEXT)
    # because the SQLite affinity probe disagreed with the declared type.
    widened_columns: List[str] = Field(default_factory=list)


class IngestionError(BaseModel):
    """Per-model failure during idempotent ingestion."""

    model_name: str
    data_source: str
    error: str


class IdempotentIngestResult(BaseModel):
    """Combined return shape of the idempotent ``ingest_datasource`` pass."""

    additions: List[ModelAddition] = Field(default_factory=list)
    to_delete: List[ToDeleteEntry] = Field(default_factory=list)
    errors: List[IngestionError] = Field(default_factory=list)


class AppliedEntry(BaseModel):
    """A delete entry that ``apply_drift_deletes`` successfully applied."""

    tool: Literal["edit_model", "delete_model"]
    model_name: str
    data_source: str


class ApplyError(BaseModel):
    """Per-entry failure during ``apply_drift_deletes``."""

    tool: Literal["edit_model", "delete_model"]
    model_name: str
    data_source: str
    error: str


class ApplyDriftResult(BaseModel):
    """Combined return shape of ``apply_drift_deletes``."""

    applied: List[AppliedEntry] = Field(default_factory=list)
    errors: List[ApplyError] = Field(default_factory=list)
    residual: List[ToDeleteEntry] = Field(default_factory=list)


# ===========================================================================
# Internal live-schema input shapes
# ===========================================================================


class LiveTable(BaseModel):
    """One live table's columns/PK/FKs in SLayer's coarse type buckets.

    Internal — built by the SQLAlchemy introspection layer and consumed by
    the diff functions.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    columns: Dict[str, DataType] = Field(default_factory=dict)
    pk_columns: Set[str] = Field(default_factory=set)
    # Each entry: (local_column, ref_table, ref_column)
    fk_relationships: List[Tuple[str, str, str]] = Field(default_factory=list)


# ===========================================================================
# Type-bucket comparison
# ===========================================================================


def data_type_bucket(dt: DataType) -> str:
    """Return the coarse bucket used to compare persisted vs live types.

    DEV-1361: ``INT`` and ``DOUBLE`` are now distinct enum members but both
    bucket as ``"number"`` so drift detection does not false-positive when a
    persisted ``DOUBLE`` column is reported as ``INT`` by live introspection
    (the v5 refinement step reconciles these without raising drift). ``DATE``
    and ``TIMESTAMP`` collapse to ``"temporal"`` so a persisted DATE column
    does not flag as drift when the driver reports TIMESTAMP (or vice versa).
    """
    if dt in (DataType.INT, DataType.DOUBLE):
        return "number"
    if dt == DataType.TEXT:
        return "string"
    if dt == DataType.BOOLEAN:
        return "boolean"
    if dt in (DataType.DATE, DataType.TIMESTAMP):
        return "temporal"
    return str(dt)


def _is_bare_identifier(s: Optional[str]) -> bool:
    """``s`` is a bare SQL identifier (alphanumeric + underscore, no leading digit)."""
    if not s:
        return False
    s = s.strip()
    if not s or s[0].isdigit():
        return False
    return all(c.isalnum() or c == "_" for c in s)


def _column_is_base(col_sql: Optional[str]) -> bool:
    """A Column whose ``sql`` is None or a bare identifier is a "base"
    column — it claims a live column. Derived expressions
    (``amount * 2``, ``customers.region``, etc.) do not.
    """
    if col_sql is None:
        return True
    return _is_bare_identifier(col_sql)


# ===========================================================================
# Pure diff functions
# ===========================================================================


def _diff_sql_table_columns(
    *, model: SlayerModel, live_table: LiveTable
) -> Tuple[List[str], List[DeleteReason]]:
    """Per-column diff of a sql_table-mode model against live columns."""
    dropped: List[str] = []
    reasons: List[DeleteReason] = []
    for col in model.columns:
        # Only compare base columns directly. Derived columns are handled
        # by cascade.
        if not _column_is_base(col.sql):
            continue
        bare_name = (col.sql or col.name).strip()
        if bare_name not in live_table.columns:
            dropped.append(col.name)
            reasons.append(
                DeleteReason(
                    target=f"column:{col.name}",
                    reason=f"Live column {bare_name!r} not found",
                )
            )
            continue
        live_dt = live_table.columns[bare_name]
        if data_type_bucket(col.type) != data_type_bucket(live_dt):
            dropped.append(col.name)
            reasons.append(
                DeleteReason(
                    target=f"column:{col.name}",
                    reason=(
                        f"Type bucket mismatch: persisted={col.type}, "
                        f"live={live_dt}"
                    ),
                )
            )
    return dropped, reasons


def _diff_sql_table_joins(
    *,
    model: SlayerModel,
    live_table: LiveTable,
    available_models_in_ds: Set[str],
) -> Tuple[List[str], List[DeleteReason]]:
    """Per-join diff of a sql_table-mode model against live FK columns and
    in-datasource model availability."""
    dropped: List[str] = []
    reasons: List[DeleteReason] = []
    # ``join.join_pairs[*][0]`` is the semantic column name (``Column.name``).
    # Resolve to the physical column name via ``Column.sql`` before checking
    # against the live table — for a base column like
    # ``Column(name="customer_id", sql="customer_fk")``, ``live_table.columns``
    # contains ``customer_fk``, not ``customer_id``. Without this resolution
    # the membership check wrongly drops valid joins.
    base_sql_by_name = {
        c.name: (c.sql or c.name).strip()
        for c in model.columns
        if _column_is_base(c.sql)
    }
    for join in model.joins:
        local_cols = [pair[0] for pair in join.join_pairs]
        missing_locals = [
            lc for lc in local_cols
            if base_sql_by_name.get(lc, lc) not in live_table.columns
        ]
        if missing_locals:
            dropped.append(join.target_model)
            reasons.append(
                DeleteReason(
                    target=f"join:{join.target_model}",
                    reason=(
                        f"Local FK column(s) {missing_locals} missing from "
                        f"live table"
                    ),
                )
            )
            continue
        if join.target_model not in available_models_in_ds:
            dropped.append(join.target_model)
            reasons.append(
                DeleteReason(
                    target=f"join:{join.target_model}",
                    reason=(
                        f"Join target {join.target_model!r} not present in "
                        f"datasource {model.data_source!r}"
                    ),
                )
            )
    return dropped, reasons


def diff_sql_table_model(
    *,
    model: SlayerModel,
    live_table: Optional[LiveTable],
    available_models_in_ds: Set[str],
) -> Tuple[Optional[ToDeleteEntry], Set[str]]:
    """Diff a sql_table-mode model against live introspection.

    Returns ``(entry_or_None, dropped_column_names)``.

    * ``live_table is None`` → ``WholeModelDelete`` (live table missing).
    * Persisted base column missing from live → ``drop_column``.
    * Persisted base column's bucket ≠ live bucket → ``drop_column``.
    * Persisted join's local column missing from live → ``drop_join``.
    * Persisted join's target_model not in ``available_models_in_ds`` →
      ``drop_join``.

    Cascade walking is the caller's responsibility (see
    ``compute_datasource_drops``).
    """
    if live_table is None:
        return (
            WholeModelDelete(
                model_name=model.name,
                data_source=model.data_source,
                reasons=[
                    DeleteReason(
                        target=f"model:{model.name}",
                        reason=(
                            f"Live table {model.sql_table!r} not found in "
                            f"datasource {model.data_source!r}"
                        ),
                    )
                ],
            ),
            {c.name for c in model.columns},
        )

    dropped_cols, col_reasons = _diff_sql_table_columns(
        model=model, live_table=live_table
    )
    dropped_joins, join_reasons = _diff_sql_table_joins(
        model=model,
        live_table=live_table,
        available_models_in_ds=available_models_in_ds,
    )

    if not dropped_cols and not dropped_joins:
        return None, set()
    reasons = col_reasons + join_reasons

    return (
        EditModelDelete(
            model_name=model.name,
            data_source=model.data_source,
            remove=RemoveSpec(columns=dropped_cols, joins=dropped_joins),
            reasons=reasons,
        ),
        set(dropped_cols),
    )


def diff_sql_model(
    *,
    model: SlayerModel,
    live_columns: Optional[Dict[str, DataType]],
) -> Tuple[Optional[ToDeleteEntry], Set[str]]:
    """Diff a sql-mode model against trial-execute cursor metadata.

    ``live_columns is None`` ⇒ trial-execute failed ⇒ ``WholeModelDelete``.
    """
    if live_columns is None:
        return (
            WholeModelDelete(
                model_name=model.name,
                data_source=model.data_source,
                reasons=[
                    DeleteReason(
                        target=f"model:{model.name}",
                        reason=(
                            "Trial-execute on model.sql failed; the SQL no "
                            "longer parses or executes against the live "
                            "datasource"
                        ),
                    )
                ],
            ),
            {c.name for c in model.columns},
        )

    dropped_cols: List[str] = []
    reasons: List[DeleteReason] = []
    for col in model.columns:
        # Cursor exposes ALIAS names — match by col.name first, fall back to
        # col.sql for legacy cases where a Column's name differs from its
        # underlying SQL identifier.
        live_dt = live_columns.get(col.name)
        if live_dt is None and col.sql is not None:
            live_dt = live_columns.get(col.sql.strip())
        if live_dt is None:
            # Only flag base / aliased-base columns. A derived column whose
            # sql is a non-trivial expression is handled by cascade rules.
            if _column_is_base(col.sql) or col.sql == col.name:
                dropped_cols.append(col.name)
                reasons.append(
                    DeleteReason(
                        target=f"column:{col.name}",
                        reason=(
                            f"Cursor metadata for model.sql does not "
                            f"include {col.name!r}"
                        ),
                    )
                )
            continue
        if data_type_bucket(col.type) != data_type_bucket(live_dt):
            dropped_cols.append(col.name)
            reasons.append(
                DeleteReason(
                    target=f"column:{col.name}",
                    reason=(
                        f"Type bucket mismatch on cursor metadata: "
                        f"persisted={col.type}, live={live_dt}"
                    ),
                )
            )

    if not dropped_cols:
        return None, set()
    return (
        EditModelDelete(
            model_name=model.name,
            data_source=model.data_source,
            remove=RemoveSpec(columns=dropped_cols),
            reasons=reasons,
        ),
        set(dropped_cols),
    )


# ===========================================================================
# Reference extraction helpers (sqlglot AST + formula AST)
# ===========================================================================


def _extract_column_refs_from_sql(sql: str) -> List[Tuple[Optional[str], str]]:
    """Return all ``(table_alias, column_name)`` refs in a SQL expression.

    ``table_alias`` is ``None`` for bare identifiers, the raw alias string
    for qualified ones (e.g. ``customers.region`` → ``("customers",
    "region")``; ``customers__regions.name`` → ``("customers__regions",
    "name")``).
    """
    try:
        parsed = sqlglot.parse_one(sql)
    except Exception:
        return []
    refs: List[Tuple[Optional[str], str]] = []
    for col in parsed.find_all(exp.Column):
        if col.args.get("db") or col.args.get("catalog"):
            continue
        table_id = col.args.get("table")
        table_alias = table_id.name if table_id else None
        refs.append((table_alias, col.name))
    return refs


def _agg_ref_names(agg_refs: Dict[str, AggregatedMeasureRef]) -> Set[str]:
    """Names from a ``measure:agg`` placeholder map, excluding ``*``."""
    return {ref.measure_name for ref in agg_refs.values() if ref.measure_name != "*"}


def _bare_measure_names(
    measure_names: List[str],
    agg_refs: Dict[str, AggregatedMeasureRef],
    *,
    skip_placeholder_prefix: Optional[str] = None,
) -> Set[str]:
    """Filter raw ``measure_names`` to the ones that are not colon-syntax
    placeholders, optionally also stripping sub-transform placeholders.
    """
    out: Set[str] = set()
    for n in measure_names:
        if n in agg_refs:
            continue
        if skip_placeholder_prefix and n.startswith(skip_placeholder_prefix):
            continue
        out.add(n)
    return out


def _walk_field_spec_measure_refs(spec: Any) -> Set[str]:
    """Walk a ``FieldSpec`` (parse_formula output) and return the set of
    measure_name strings (which may be dotted: ``"customers.revenue"``).
    """
    if isinstance(spec, AggregatedMeasureRef):
        return _agg_ref_names({"_": spec})
    if isinstance(spec, ArithmeticField):
        return _agg_ref_names(spec.agg_refs) | _bare_measure_names(
            spec.measure_names, spec.agg_refs
        )
    if isinstance(spec, MixedArithmeticField):
        out = _agg_ref_names(spec.agg_refs) | _bare_measure_names(
            spec.measure_names, spec.agg_refs, skip_placeholder_prefix="_t"
        )
        for _, t in spec.sub_transforms:
            out.update(_walk_field_spec_measure_refs(t))
        return out
    if isinstance(spec, TransformField):
        return _walk_field_spec_measure_refs(spec.inner)
    return set()


def _measure_formula_refs(
    formula: str,
    *,
    named_measures: Optional[Dict[str, str]] = None,
) -> Set[str]:
    """Best-effort: parse ``formula`` and return the set of column / measure
    names it references. Returns the empty set on any parse failure.

    ``named_measures`` is the map ``{measure_name: formula_text}`` for the
    enclosing model — required for bare measure references like
    ``aov / *:count`` (where ``aov`` is itself a saved measure on the
    model) to parse cleanly.
    """
    try:
        spec = parse_formula(formula, named_measures=named_measures)
    except Exception:
        return set()
    return _walk_field_spec_measure_refs(spec)


def _filter_refs(filter_str: str) -> List[str]:
    """Best-effort: return list of column references in a SQL-mode filter.

    Used to scan ``Column.filter`` / ``SlayerModel.filters`` strings (Mode A
    SQL — DEV-1369). Returns ``[]`` on parse failure.
    """
    try:
        pf = parse_sql_predicate(filter_str)
    except Exception:
        return []
    return list(pf.columns)


def _filter_refs_dsl(filter_str: str) -> List[str]:
    """Best-effort: return list of column / measure references in a DSL filter.

    Used to scan ``SlayerQuery.filters`` strings (Mode B DSL — DEV-1369),
    which accept colon-syntax aggregations (``revenue:sum > 100``) and
    transform calls (``change(revenue:sum) > 0``). Returns ``[]`` on
    parse failure.

    ``parse_filter`` replaces colon syntax with canonical aliases in
    ``pf.columns`` (``revenue_sum``), so we recover the underlying base
    measure names from ``pf.agg_refs`` and strip the synthesized aliases
    from the raw columns. ``"*"`` (from ``*:count``) is excluded — it
    isn't a real column reference.
    """
    try:
        pf = parse_filter(filter_str)
    except Exception:
        return []
    measure_names = [ref.measure_name for ref in pf.agg_refs if ref.measure_name != "*"]
    canonical_aliases = set(pf.synthesized_aliases)
    raw_columns = [c for c in pf.columns if c not in canonical_aliases]
    return list(dict.fromkeys(measure_names + raw_columns))


def _walk_alias_to_target_model(
    *,
    source_model: SlayerModel,
    table_alias: str,
    models_by_name: Dict[str, SlayerModel],
) -> Optional[SlayerModel]:
    """Resolve a ``__``-delimited path alias starting from ``source_model``
    to the terminal joined model. Returns None if any hop fails.
    """
    if table_alias in (source_model.name, ""):
        return source_model
    parts = table_alias.split("__") if "__" in table_alias else [table_alias]
    current = source_model
    for hop in parts:
        join = next((j for j in current.joins if j.target_model == hop), None)
        if join is None:
            return None
        nxt = models_by_name.get(hop)
        if nxt is None:
            return None
        current = nxt
    return current


def _resolve_dotted_ref_to_model(
    *,
    source_model: SlayerModel,
    dotted_ref: str,
    models_by_name: Dict[str, SlayerModel],
) -> Tuple[Optional[SlayerModel], Optional[str]]:
    """Resolve a dotted measure/column ref like ``customers.region`` or
    ``customers.regions.name`` to ``(target_model, leaf_name)``.

    Same-model bare refs (no dot) return ``(source_model, ref)``.
    """
    if "." not in dotted_ref:
        return source_model, dotted_ref
    prefix, leaf = dotted_ref.rsplit(".", 1)
    target = _walk_alias_to_target_model(
        source_model=source_model,
        table_alias=prefix.replace(".", "__"),
        models_by_name=models_by_name,
    )
    return target, leaf


# ===========================================================================
# Cascade helpers
# ===========================================================================


def _pk_columns(model: SlayerModel) -> Set[str]:
    return {c.name for c in model.columns if c.primary_key}


def _ensure_edit_entry(
    *,
    edit_entries: Dict[str, EditModelDelete],
    model: SlayerModel,
) -> EditModelDelete:
    if model.name not in edit_entries:
        edit_entries[model.name] = EditModelDelete(
            model_name=model.name,
            data_source=model.data_source,
        )
    return edit_entries[model.name]


def _add_dropped_column(
    *,
    edit_entries: Dict[str, EditModelDelete],
    dropped_cols: Dict[str, Set[str]],
    model: SlayerModel,
    column_name: str,
    reason: str,
) -> bool:
    """Record a cascade-induced column drop. Returns True if newly added."""
    if column_name in dropped_cols.get(model.name, set()):
        return False
    entry = _ensure_edit_entry(edit_entries=edit_entries, model=model)
    if column_name not in entry.remove.columns:
        entry.remove.columns.append(column_name)
        entry.reasons.append(
            DeleteReason(target=f"column:{column_name}", reason=reason)
        )
    dropped_cols.setdefault(model.name, set()).add(column_name)
    return True


def _add_dropped_measure(
    *,
    edit_entries: Dict[str, EditModelDelete],
    dropped_measures: Dict[str, Set[str]],
    model: SlayerModel,
    measure_name: str,
    reason: str,
) -> bool:
    if measure_name in dropped_measures.get(model.name, set()):
        return False
    entry = _ensure_edit_entry(edit_entries=edit_entries, model=model)
    if measure_name not in entry.remove.measures:
        entry.remove.measures.append(measure_name)
        entry.reasons.append(
            DeleteReason(target=f"measure:{measure_name}", reason=reason)
        )
    dropped_measures.setdefault(model.name, set()).add(measure_name)
    return True


def _add_dropped_join(
    *,
    edit_entries: Dict[str, EditModelDelete],
    dropped_joins: Dict[str, Set[str]],
    model: SlayerModel,
    target_name: str,
    reason: str,
) -> bool:
    if target_name in dropped_joins.get(model.name, set()):
        return False
    entry = _ensure_edit_entry(edit_entries=edit_entries, model=model)
    if target_name not in entry.remove.joins:
        entry.remove.joins.append(target_name)
        entry.reasons.append(
            DeleteReason(target=f"join:{target_name}", reason=reason)
        )
    dropped_joins.setdefault(model.name, set()).add(target_name)
    return True


def _add_remove_filter(
    *,
    edit_entries: Dict[str, EditModelDelete],
    model: SlayerModel,
    filter_text: str,
    reason: str,
) -> bool:
    entry = _ensure_edit_entry(edit_entries=edit_entries, model=model)
    if filter_text in entry.remove_filters:
        return False
    entry.remove_filters.append(filter_text)
    entry.reasons.append(
        DeleteReason(target=f"filter:{filter_text}", reason=reason)
    )
    return True


# ===========================================================================
# Query-backed cascade
# ===========================================================================


def _resolve_stage_source_to_base(
    *,
    source_model: object,
    prior_stages_by_name: Dict[str, SlayerQuery],
) -> Optional[str]:
    """Walk a ``source_model`` reference (str / SlayerModel / ModelExtension /
    prior-stage-name) back to a real persisted base model name.

    ``ModelExtension`` carries a ``source_name: str`` field that names the
    underlying model — we follow it transparently so query-backed drift
    attribution doesn't silently skip extension-wrapped stages.
    """
    seen: Set[str] = set()
    current = source_model
    while True:
        if isinstance(current, str):
            if current in seen:
                return None  # cycle — should never happen, validated upstream
            seen.add(current)
            if current in prior_stages_by_name:
                current = prior_stages_by_name[current].source_model
                continue
            return current
        # ModelExtension wraps a base model — unwrap via source_name (str).
        source_name = getattr(current, "source_name", None)
        if isinstance(source_name, str):
            current = source_name
            continue
        if isinstance(current, SlayerModel):
            return current.name
        return None


class _StageGraph(BaseModel):
    """Resolved join-graph context for a query-backed stage.

    Carries the stage's resolved source name (``stage_source_name``), the
    extension-added join targets (``extension_targets``), and the set of
    every model name reachable from the source via the in-DS join graph.
    Used to attribute multi-hop dotted refs and to bound dropped-join
    checks to models the stage can actually reach.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    stage_source_name: Optional[str] = None
    extension_targets: Set[str] = Field(default_factory=set)
    reachable: Set[str] = Field(default_factory=set)
    models_by_name: Dict[str, SlayerModel] = Field(default_factory=dict)


def _build_stage_graph(
    *,
    stage: SlayerQuery,
    stage_source_name: Optional[str],
    models_by_name: Dict[str, SlayerModel],
) -> _StageGraph:
    """Build a ``_StageGraph`` for a single stage. ``stage_source_name`` is
    the resolved base model name (str), or ``None`` for inline / unresolved
    sources.
    """
    extension_targets = _stage_join_targets(stage)
    reachable: Set[str] = set()
    if stage_source_name:
        reachable.add(stage_source_name)
    reachable |= extension_targets
    frontier = list(reachable)
    visited: Set[str] = set()
    while frontier:
        name = frontier.pop()
        if name in visited:
            continue
        visited.add(name)
        m = models_by_name.get(name)
        if m is None:
            continue
        for j in m.joins:
            if j.target_model not in reachable:
                reachable.add(j.target_model)
                frontier.append(j.target_model)
    return _StageGraph(
        stage_source_name=stage_source_name,
        extension_targets=extension_targets,
        reachable=reachable,
        models_by_name=models_by_name,
    )


def _attribute_ref_to_base(
    *,
    ref: str,
    base_name: str,
    graph: _StageGraph,
) -> Optional[str]:
    """Walk ``ref`` through the stage's join graph and return the leaf
    column name when it resolves to ``base_name``, else ``None``.

    Bare refs (no dot) attribute to ``stage_source_name``. Single-dot and
    multi-hop dotted refs are walked through ``models_by_name`` —
    ``customers.regions.name`` from a stage rooted at ``orders`` resolves
    to ``regions.name`` if ``orders → customers → regions`` exists.
    """
    if "." not in ref:
        return ref if graph.stage_source_name == base_name else None
    parts = ref.split(".")
    leaf = parts[-1]
    path = parts[:-1]
    current = graph.stage_source_name
    if current is None:
        return None
    # Root-qualified refs like ``orders.amount`` from a stage rooted at
    # ``orders``: ``orders`` is not in its own join set, so the regular
    # walk below would miss this case. Treat path == [stage_source_name]
    # as a same-model ref.
    if path == [graph.stage_source_name]:
        return leaf if graph.stage_source_name == base_name else None
    for hop in path:
        m = graph.models_by_name.get(current)
        join_targets = {j.target_model for j in (m.joins if m is not None else [])}
        if current == graph.stage_source_name:
            join_targets |= graph.extension_targets
        if hop not in join_targets:
            return None
        current = hop
    return leaf if current == base_name else None


def _measure_refs_on_base(
    stage: SlayerQuery, base_name: str, graph: _StageGraph
) -> Set[str]:
    out: Set[str] = set()
    for m in stage.measures or []:
        formula = getattr(m, "formula", None)
        if not formula:
            continue
        for ref in _measure_formula_refs(formula):
            attributed = _attribute_ref_to_base(
                ref=ref, base_name=base_name, graph=graph
            )
            if attributed is not None:
                out.add(attributed)
    return out


def _dimension_refs_on_base(
    stage: SlayerQuery, base_name: str, graph: _StageGraph
) -> Set[str]:
    out: Set[str] = set()
    for d in stage.dimensions or []:
        full = getattr(d, "full_name", None) or str(d)
        attributed = _attribute_ref_to_base(
            ref=full, base_name=base_name, graph=graph
        )
        if attributed is not None:
            out.add(attributed)
    return out


def _time_dimension_refs_on_base(
    stage: SlayerQuery, base_name: str, graph: _StageGraph
) -> Set[str]:
    out: Set[str] = set()
    for td in stage.time_dimensions or []:
        attributed = _attribute_ref_to_base(
            ref=td.dimension.full_name, base_name=base_name, graph=graph
        )
        if attributed is not None:
            out.add(attributed)
    return out


def _filter_refs_on_base(
    stage: SlayerQuery, base_name: str, graph: _StageGraph
) -> Set[str]:
    out: Set[str] = set()
    # ``SlayerQuery.filters`` are Mode B (DSL) — go through the DSL parser
    # so colon-syntax aggregations and transforms surface their underlying
    # measure names. ``_filter_refs`` (SQL-mode) would drop them silently.
    for f in stage.filters or []:
        for col in _filter_refs_dsl(f):
            attributed = _attribute_ref_to_base(
                ref=col, base_name=base_name, graph=graph
            )
            if attributed is not None:
                out.add(attributed)
    return out


def _stage_referenced_columns_for_base(
    *,
    stage: SlayerQuery,
    base_name: str,
    graph: Optional[_StageGraph] = None,
) -> Set[str]:
    """Return the set of column names referenced *on* ``base_name`` by a
    single source_queries stage. Walks the stage's join graph (passed via
    ``graph``) so multi-hop dotted refs and ModelExtension-added joins are
    handled. Falls back to a graph with no models (string-prefix match
    only) when ``graph`` is omitted, preserving legacy callers.
    """
    if graph is None:
        stage_source_name = (
            stage.source_model if isinstance(stage.source_model, str) else None
        )
        graph = _StageGraph(
            stage_source_name=stage_source_name,
            extension_targets=_stage_join_targets(stage),
            reachable={stage_source_name} if stage_source_name else set(),
            models_by_name={},
        )
    return (
        _measure_refs_on_base(stage, base_name, graph)
        | _dimension_refs_on_base(stage, base_name, graph)
        | _time_dimension_refs_on_base(stage, base_name, graph)
        | _filter_refs_on_base(stage, base_name, graph)
    )


def _stage_join_targets(stage: SlayerQuery) -> Set[str]:
    """Return the set of join target_model names referenced by a stage.

    ``SlayerQuery`` itself has no ``joins`` field; joins on a stage live
    on its ``source_model`` when that's a ``ModelExtension``. Read off
    ``stage.source_model.joins`` via ``getattr`` with defaults so plain
    stages (str source_model, SlayerModel source_model) return the empty
    set without raising.
    """
    source = getattr(stage, "source_model", None)
    joins = getattr(source, "joins", None) or []
    out: Set[str] = set()
    for j in joins:
        target = getattr(j, "target_model", None)
        if isinstance(target, str):
            out.add(target)
    return out


def _check_stage_against_base(
    *,
    stage: SlayerQuery,
    base_name: str,
    graph: _StageGraph,
    dropped_cols: Dict[str, Set[str]],
    pk_per_model: Dict[str, Set[str]],
) -> Set[str]:
    """Return the set of dropped column names on ``base_name`` that this
    stage references (resolved through the stage's join graph).

    PK columns are excluded — rule 7. Returns the empty set when no hits.
    """
    cascadable = dropped_cols.get(base_name, set()) - pk_per_model.get(
        base_name, set()
    )
    if not cascadable:
        return set()
    return _stage_referenced_columns_for_base(
        stage=stage, base_name=base_name, graph=graph
    ) & cascadable


def _stage_uses_dropped_join(
    *,
    stage: SlayerQuery,
    base_name: str,
    graph: _StageGraph,
    dropped_joins: Dict[str, Set[str]],
) -> Optional[str]:
    """If ``stage`` references any column under a join target that's been
    dropped on ``base_name``, return the conflicting target; else None.

    Bounded to ``graph.reachable`` so a dropped ``invoices → customers``
    join doesn't whole-drop a stage rooted at ``orders`` that uses
    ``customers.name`` via its own ``orders → customers`` link.
    """
    if base_name not in graph.reachable:
        return None
    targets = dropped_joins.get(base_name, set())
    if not targets:
        return None
    for target in targets:
        # ``_stage_referenced_columns_for_base(stage, base_name=target)``
        # returns the column names the stage references *on* ``target``
        # (resolved through the join graph). Non-empty ⇒ the dropped join
        # means those references no longer resolve.
        if _stage_referenced_columns_for_base(
            stage=stage, base_name=target, graph=graph
        ):
            return target
    return None


def _check_stage_for_whole_drop(
    *,
    stage: SlayerQuery,
    base_name: str,
    qb_name: str,
    graph: _StageGraph,
    whole_dropped_models: Set[str],
    dropped_cols: Dict[str, Set[str]],
    dropped_joins: Dict[str, Set[str]],
    pk_per_model: Dict[str, Set[str]],
    candidate_base_names: Set[str],
) -> Optional[DeleteReason]:
    """Decide whether a single stage of a query-backed model triggers the
    whole-drop. Returns a ``DeleteReason`` on the first matching trigger,
    or ``None`` when the stage has no fatal references.
    """
    if base_name in whole_dropped_models:
        return DeleteReason(
            target=f"model:{qb_name}",
            reason=(
                f"source_queries stage references base model "
                f"{base_name!r} which is being whole-dropped"
            ),
        )
    for join_target in _stage_join_targets(stage):
        if join_target in whole_dropped_models:
            return DeleteReason(
                target=f"model:{qb_name}",
                reason=(
                    f"source_queries stage joins to {join_target!r} "
                    f"which is being whole-dropped"
                ),
            )
    for candidate in candidate_base_names:
        broken_target = _stage_uses_dropped_join(
            stage=stage,
            base_name=candidate,
            graph=graph,
            dropped_joins=dropped_joins,
        )
        if broken_target is not None:
            return DeleteReason(
                target=f"model:{qb_name}",
                reason=(
                    f"source_queries stage references {broken_target!r} "
                    f"via {candidate!r} but that join has been dropped"
                ),
            )
        hits = _check_stage_against_base(
            stage=stage,
            base_name=candidate,
            graph=graph,
            dropped_cols=dropped_cols,
            pk_per_model=pk_per_model,
        )
        if hits:
            return DeleteReason(
                target=f"model:{qb_name}",
                reason=(
                    f"source_queries stage references columns on "
                    f"{candidate!r} that have been dropped: {sorted(hits)}"
                ),
            )
    return None


def _query_backed_should_whole_drop(
    *,
    qb_model: SlayerModel,
    dropped_cols: Dict[str, Set[str]],
    dropped_joins: Dict[str, Set[str]],
    whole_dropped_models: Set[str],
    pk_per_model: Dict[str, Set[str]],
    candidate_base_names: Optional[Set[str]] = None,
    models_by_name: Optional[Dict[str, SlayerModel]] = None,
) -> Optional[DeleteReason]:
    """Return a non-None DeleteReason when this query-backed model should be
    whole-dropped due to cascading from base-model drift, else None.

    ``candidate_base_names`` is the set of every model name in the same DS;
    each stage's references are checked against every candidate.
    ``models_by_name`` carries the join graph so multi-hop refs and
    extension-added joins resolve correctly. Both default to empty,
    preserving the legacy contract for any callers that haven't been
    migrated yet.
    """
    if not qb_model.source_queries:
        return None
    stages = list(qb_model.source_queries)
    models_by_name = models_by_name or {}

    for i, stage in enumerate(stages):
        prior_by_name: Dict[str, SlayerQuery] = {}
        for s in stages[:i]:
            s_name = getattr(s, "name", None)
            if s_name:
                prior_by_name[s_name] = s
        base_name = _resolve_stage_source_to_base(
            source_model=stage.source_model,
            prior_stages_by_name=prior_by_name,
        )
        if base_name is None:
            continue
        graph = _build_stage_graph(
            stage=stage,
            stage_source_name=base_name,
            models_by_name=models_by_name,
        )
        reason = _check_stage_for_whole_drop(
            stage=stage,
            base_name=base_name,
            qb_name=qb_model.name,
            graph=graph,
            whole_dropped_models=whole_dropped_models,
            dropped_cols=dropped_cols,
            dropped_joins=dropped_joins,
            pk_per_model=pk_per_model,
            candidate_base_names=candidate_base_names or {base_name},
        )
        if reason is not None:
            return reason
    return None


# ===========================================================================
# Cascade walker + collapse
# ===========================================================================


class _CascadeState:
    """Mutable state threaded through the per-rule cascade helpers.

    Plain class (not Pydantic) so dict/set fields preserve reference
    identity — the cascade rules mutate them in-place and the orchestrator
    in ``compute_datasource_drops`` has to see those mutations.
    """

    __slots__ = (
        "models_by_name",
        "edit_entries",
        "whole_entries",
        "dropped_cols",
        "dropped_measures",
        "dropped_joins",
        "pk_per_model",
    )

    def __init__(
        self,
        *,
        models_by_name: Dict[str, SlayerModel],
        edit_entries: Dict[str, EditModelDelete],
        whole_entries: Dict[str, WholeModelDelete],
        dropped_cols: Dict[str, Set[str]],
        dropped_measures: Dict[str, Set[str]],
        dropped_joins: Dict[str, Set[str]],
        pk_per_model: Dict[str, Set[str]],
    ) -> None:
        self.models_by_name = models_by_name
        self.edit_entries = edit_entries
        self.whole_entries = whole_entries
        self.dropped_cols = dropped_cols
        self.dropped_measures = dropped_measures
        self.dropped_joins = dropped_joins
        self.pk_per_model = pk_per_model

    def cascadable(self, name: str) -> Set[str]:
        """Cascadable column drops on ``name`` (excludes PKs — rule 7)."""
        return self.dropped_cols.get(name, set()) - self.pk_per_model.get(name, set())


def _column_ref_targets_dropped(
    *,
    table_alias: Optional[str],
    ref_col: str,
    model: SlayerModel,
    state: _CascadeState,
) -> Tuple[bool, Optional[SlayerModel]]:
    """Decide if a single ``(table_alias, ref_col)`` reference resolves to a
    dropped column. Returns ``(is_dropped, resolved_target_model)``.
    """
    if table_alias is None or table_alias == model.name:
        return ref_col in state.cascadable(model.name), model
    target = _walk_alias_to_target_model(
        source_model=model,
        table_alias=table_alias,
        models_by_name=state.models_by_name,
    )
    if target is None or target.data_source != model.data_source:
        return False, None
    return ref_col in state.cascadable(target.name), target


def _first_dropped_sql_column_ref(
    *, col: Column, model: SlayerModel, state: _CascadeState
) -> Optional[Tuple[SlayerModel, str]]:
    """Return ``(target_model, ref_col)`` for the first reference in
    ``col.sql`` that resolves to a dropped column, or ``None`` when
    nothing in the column's SQL references a dropped target.
    """
    if col.sql is None or _is_bare_identifier(col.sql):
        return None
    for table_alias, ref_col in _extract_column_refs_from_sql(col.sql):
        is_dropped, target = _column_ref_targets_dropped(
            table_alias=table_alias,
            ref_col=ref_col,
            model=model,
            state=state,
        )
        if is_dropped and target is not None:
            return target, ref_col
    return None


def _cascade_derived_columns(
    *, model: SlayerModel, state: _CascadeState
) -> bool:
    """Rules 1 + 5: derived ``Column.sql`` referencing dropped columns
    (same model or via the join graph)."""
    changed = False
    dropped_set = state.dropped_cols.get(model.name, set())
    for col in model.columns:
        if col.name in dropped_set:
            continue
        hit = _first_dropped_sql_column_ref(col=col, model=model, state=state)
        if hit is None:
            continue
        target, ref_col = hit
        ref_label = ref_col if target is model else f"{target.name}.{ref_col!r}"
        if _add_dropped_column(
            edit_entries=state.edit_entries,
            dropped_cols=state.dropped_cols,
            model=model,
            column_name=col.name,
            reason=f"Derived sql {col.sql!r} references dropped column {ref_label}",
        ):
            changed = True
    return changed


def _measure_drop_cause(
    *, ref: str, model: SlayerModel, state: _CascadeState
) -> Optional[str]:
    """If the measure ref resolves to a dropped column or measure, return a
    reason string; otherwise None.
    """
    tgt_model, leaf = _resolve_dotted_ref_to_model(
        source_model=model,
        dotted_ref=ref,
        models_by_name=state.models_by_name,
    )
    if tgt_model is None or tgt_model.data_source != model.data_source:
        return None
    if leaf in state.cascadable(tgt_model.name):
        return f"references dropped column {tgt_model.name}.{leaf!r}"
    if leaf in state.dropped_measures.get(tgt_model.name, set()):
        return f"references dropped measure {tgt_model.name}.{leaf!r}"
    return None


def _first_dropped_cause(
    *,
    refs: Set[str],
    model: SlayerModel,
    state: _CascadeState,
) -> Optional[str]:
    """Return the cause string for the first ref that resolves to a dropped
    column or measure, or ``None`` when nothing in ``refs`` is dropped.
    """
    for ref in refs:
        cause = _measure_drop_cause(ref=ref, model=model, state=state)
        if cause is not None:
            return cause
    return None


def _cascade_measures(*, model: SlayerModel, state: _CascadeState) -> bool:
    """Rule 2: ``ModelMeasure.formula`` referencing a dropped column or
    dropped measure."""
    changed = False
    named_measures = {m.name: m.formula for m in model.measures if m.name}
    dropped_set = state.dropped_measures.get(model.name, set())
    for measure in model.measures:
        if measure.name is None or measure.name in dropped_set:
            continue
        refs = _measure_formula_refs(
            measure.formula, named_measures=named_measures
        )
        cause = _first_dropped_cause(refs=refs, model=model, state=state)
        if cause is None:
            continue
        if _add_dropped_measure(
            edit_entries=state.edit_entries,
            dropped_measures=state.dropped_measures,
            model=model,
            measure_name=measure.name,
            reason=f"Formula {measure.formula!r} {cause}",
        ):
            changed = True
    return changed


def _cascade_joins(*, model: SlayerModel, state: _CascadeState) -> bool:
    """Rule 3a + 3b: local FK column dropped on this model, or foreign
    column dropped on the join target."""
    changed = False
    for join in model.joins:
        if join.target_model in state.dropped_joins.get(model.name, set()):
            continue
        local_missing = [
            pair[0] for pair in join.join_pairs
            if pair[0] in state.cascadable(model.name)
        ]
        if local_missing:
            changed = _add_dropped_join(
                edit_entries=state.edit_entries,
                dropped_joins=state.dropped_joins,
                model=model,
                target_name=join.target_model,
                reason=f"Local FK column(s) {local_missing} dropped from this model",
            ) or changed
            continue
        tgt = state.models_by_name.get(join.target_model)
        if tgt is None or tgt.data_source != model.data_source:
            continue
        # Check raw dropped_cols, not cascadable (rule 7 PK exclusion):
        # a target PK drop still invalidates the join itself even though
        # downstream cascades stop at the column level.
        foreign_missing = [
            pair[1] for pair in join.join_pairs
            if pair[1] in state.dropped_cols.get(tgt.name, set())
        ]
        if not foreign_missing:
            continue
        if _add_dropped_join(
            edit_entries=state.edit_entries,
            dropped_joins=state.dropped_joins,
            model=model,
            target_name=join.target_model,
            reason=(
                f"Foreign column(s) {foreign_missing} dropped on target "
                f"model {join.target_model!r}"
            ),
        ):
            changed = True
    return changed


def _cascade_filters(*, model: SlayerModel, state: _CascadeState) -> bool:
    """Rule 4: model-level filter strings referencing dropped columns."""
    changed = False
    for filter_str in model.filters:
        entry = state.edit_entries.get(model.name)
        if entry is not None and filter_str in entry.remove_filters:
            continue
        for col_ref in _filter_refs(filter_str):
            tgt_model, leaf = _resolve_dotted_ref_to_model(
                source_model=model,
                dotted_ref=col_ref,
                models_by_name=state.models_by_name,
            )
            if (
                tgt_model is None
                or tgt_model.data_source != model.data_source
                or leaf not in state.cascadable(tgt_model.name)
            ):
                continue
            if _add_remove_filter(
                edit_entries=state.edit_entries,
                model=model,
                filter_text=filter_str,
                reason=f"Filter references dropped column {tgt_model.name}.{leaf!r}",
            ):
                changed = True
            break
    return changed


def _cascade_query_backed(
    *, models: List[SlayerModel], state: _CascadeState
) -> bool:
    """Rule 6: query-backed model whose source_queries chain transitively
    references dropped state — whole-drop."""
    changed = False
    whole_dropped_names = set(state.whole_entries.keys())
    # Every model name in the DS is a candidate base for cross-model dotted
    # refs inside the query-backed stages.
    candidate_base_names = set(state.models_by_name.keys())
    for model in models:
        if model.name in state.whole_entries or not model.source_queries:
            continue
        reason = _query_backed_should_whole_drop(
            qb_model=model,
            dropped_cols=state.dropped_cols,
            dropped_joins=state.dropped_joins,
            whole_dropped_models=whole_dropped_names,
            pk_per_model=state.pk_per_model,
            candidate_base_names=candidate_base_names,
            models_by_name=state.models_by_name,
        )
        if reason is None:
            continue
        state.whole_entries[model.name] = WholeModelDelete(
            model_name=model.name,
            data_source=model.data_source,
            reasons=[reason],
        )
        # Treat all of this model's columns as dropped so further rounds
        # propagate transitively.
        state.dropped_cols[model.name] = {c.name for c in model.columns}
        changed = True
    return changed


def _cascade_one_pass(
    *,
    models: List[SlayerModel],
    models_by_name: Dict[str, SlayerModel],
    edit_entries: Dict[str, EditModelDelete],
    whole_entries: Dict[str, WholeModelDelete],
    dropped_cols: Dict[str, Set[str]],
    dropped_measures: Dict[str, Set[str]],
    dropped_joins: Dict[str, Set[str]],
    pk_per_model: Dict[str, Set[str]],
) -> bool:
    """Run a single cascade pass; return True if anything new was added.

    Each cascade rule is delegated to a focused helper. The big
    function-level switch lives there; this loop only orchestrates.
    """
    state = _CascadeState(
        models_by_name=models_by_name,
        edit_entries=edit_entries,
        whole_entries=whole_entries,
        dropped_cols=dropped_cols,
        dropped_measures=dropped_measures,
        dropped_joins=dropped_joins,
        pk_per_model=pk_per_model,
    )

    changed = False
    for model in models:
        if model.name in whole_entries:
            continue
        if _cascade_derived_columns(model=model, state=state):
            changed = True
        if _cascade_measures(model=model, state=state):
            changed = True
        if _cascade_joins(model=model, state=state):
            changed = True
        if _cascade_filters(model=model, state=state):
            changed = True

    if _cascade_query_backed(models=models, state=state):
        changed = True

    return changed


def _seed_one_diff_entry(
    *,
    model_name: str,
    entry: Optional[ToDeleteEntry],
    cols: Set[str],
    edit_entries: Dict[str, EditModelDelete],
    whole_entries: Dict[str, WholeModelDelete],
    dropped_cols: Dict[str, Set[str]],
    dropped_measures: Dict[str, Set[str]],
    dropped_joins: Dict[str, Set[str]],
) -> None:
    """Apply one ``(entry, dropped_columns)`` diff result to the cascade
    state dicts."""
    if isinstance(entry, WholeModelDelete):
        whole_entries[model_name] = entry
    elif isinstance(entry, EditModelDelete):
        edit_entries[model_name] = entry
        if entry.remove.joins:
            dropped_joins.setdefault(model_name, set()).update(
                entry.remove.joins
            )
        if entry.remove.measures:
            dropped_measures.setdefault(model_name, set()).update(
                entry.remove.measures
            )
    if cols:
        dropped_cols.setdefault(model_name, set()).update(cols)


def _seed_state_from_diffs(
    *,
    diffs_iterables: Tuple[
        Dict[str, Tuple[Optional[ToDeleteEntry], Set[str]]], ...
    ],
    edit_entries: Dict[str, EditModelDelete],
    whole_entries: Dict[str, WholeModelDelete],
    dropped_cols: Dict[str, Set[str]],
    dropped_measures: Dict[str, Set[str]],
    dropped_joins: Dict[str, Set[str]],
) -> None:
    """Populate the cascade state dicts from the base per-model diffs."""
    for diffs in diffs_iterables:
        for model_name, (entry, cols) in diffs.items():
            _seed_one_diff_entry(
                model_name=model_name,
                entry=entry,
                cols=cols,
                edit_entries=edit_entries,
                whole_entries=whole_entries,
                dropped_cols=dropped_cols,
                dropped_measures=dropped_measures,
                dropped_joins=dropped_joins,
            )


def _collapse_entries(
    *,
    edit_entries: Dict[str, EditModelDelete],
    whole_entries: Dict[str, WholeModelDelete],
) -> List[ToDeleteEntry]:
    """Apply the collapse rule (whole-drop preempts edit on the same model)
    and return the final, name-sorted list of delete entries.
    """
    final: List[ToDeleteEntry] = []
    for name in sorted(set(edit_entries.keys()) | set(whole_entries.keys())):
        if name in whole_entries:
            final.append(whole_entries[name])
        else:
            final.append(edit_entries[name])
    return final


def compute_datasource_drops(
    *,
    models: List[SlayerModel],
    sql_table_diffs: Dict[str, Tuple[Optional[ToDeleteEntry], Set[str]]],
    sql_diffs: Dict[str, Tuple[Optional[ToDeleteEntry], Set[str]]],
) -> List[ToDeleteEntry]:
    """Combine per-model base diffs with cascade walking and the collapse rule.

    Pure: takes pre-computed diffs as input and returns the final flat
    list. Caller is responsible for restricting ``models`` to a single
    datasource — cascade walking does not cross datasource boundaries.
    """
    edit_entries: Dict[str, EditModelDelete] = {}
    whole_entries: Dict[str, WholeModelDelete] = {}
    dropped_cols: Dict[str, Set[str]] = {}
    dropped_measures: Dict[str, Set[str]] = {}
    dropped_joins: Dict[str, Set[str]] = {}

    _seed_state_from_diffs(
        diffs_iterables=(sql_table_diffs, sql_diffs),
        edit_entries=edit_entries,
        whole_entries=whole_entries,
        dropped_cols=dropped_cols,
        dropped_measures=dropped_measures,
        dropped_joins=dropped_joins,
    )

    models_by_name = {m.name: m for m in models}
    pk_per_model = {m.name: _pk_columns(m) for m in models}

    # Iterate to fixed point — safety bound, DAGs converge in <10 passes.
    for _ in range(100):
        if not _cascade_one_pass(
            models=models,
            models_by_name=models_by_name,
            edit_entries=edit_entries,
            whole_entries=whole_entries,
            dropped_cols=dropped_cols,
            dropped_measures=dropped_measures,
            dropped_joins=dropped_joins,
            pk_per_model=pk_per_model,
        ):
            break

    final = _collapse_entries(
        edit_entries=edit_entries, whole_entries=whole_entries
    )
    return final


# ===========================================================================
# Live introspection
# ===========================================================================


def _live_schema_for_datasource(
    *,
    datasource: DatasourceConfig,
    schema: Optional[str] = None,
) -> Dict[str, LiveTable]:
    """Return ``{table_name: LiveTable}`` for every live table in the DS,
    using SQLAlchemy ``Inspector`` and the same fallback path as
    auto-ingestion (``slayer/engine/ingestion.py``).
    """
    from slayer.sql import engine_factory
    sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
    try:
        inspector = sa.inspect(sa_engine)
        table_names = list(inspector.get_table_names(schema=schema))
        out: Dict[str, LiveTable] = {}
        for table_name in table_names:
            try:
                out[table_name] = _introspect_one_table(
                    inspector=inspector,
                    sa_engine=sa_engine,
                    table_name=table_name,
                    schema=schema,
                )
            except Exception as exc:
                logger.warning(
                    "validate_models: failed to introspect %r in datasource "
                    "%r: %s",
                    table_name,
                    datasource.name,
                    exc,
                )
        return out
    finally:
        # Same rationale as ``ingest_datasource``: this is a one-shot
        # admin path. Disposing releases the underlying connection so
        # external direct file access (e.g. ``duckdb.connect(file)``)
        # in the same process isn't blocked.
        sa_engine.dispose()


def _introspect_one_table(
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    schema: Optional[str],
) -> LiveTable:
    """Build a ``LiveTable`` for one table via the existing safe-introspection
    path used by ``slayer/engine/ingestion.py``.
    """
    cols_meta = _safe_get_columns(inspector, sa_engine, table_name, schema)
    pk = _safe_get_pk_constraint(inspector, sa_engine, table_name, schema)
    pk_columns = set(pk.get("constrained_columns", []) or [])

    columns: Dict[str, DataType] = {}
    for col in cols_meta:
        col_type = col["type"]
        if isinstance(col_type, DataType):
            columns[col["name"]] = col_type
        else:
            columns[col["name"]] = _sa_type_to_data_type(col_type)
            # is_float not relevant for bucket comparison; both INT and FLOAT
            # collapse to NUMBER.
            _ = _sa_type_is_float(col_type)

    fks: List[Tuple[str, str, str]] = []
    try:
        for fk in inspector.get_foreign_keys(table_name, schema=schema):
            constrained = fk.get("constrained_columns") or []
            referred_table = fk.get("referred_table")
            referred = fk.get("referred_columns") or []
            for src, tgt in zip(constrained, referred):
                if referred_table:
                    fks.append((src, referred_table, tgt))
    except Exception:
        # Some dialects (ClickHouse, BigQuery) don't surface FK metadata
        # via Inspector. Skip silently — joins are still validated by name.
        # Snowflake DOES expose declarative FK constraints; see
        # docs/configuration/datasources.md.
        pass

    return LiveTable(columns=columns, pk_columns=pk_columns, fk_relationships=fks)


# Map cursor type-category strings (as returned by SlayerSQLClient.get_column_types)
# to DataType buckets.
_CURSOR_CATEGORY_TO_DATATYPE = {
    "number": DataType.DOUBLE,
    "string": DataType.TEXT,
    "boolean": DataType.BOOLEAN,
    "time": DataType.TIMESTAMP,
}


async def _live_columns_for_sql_model(
    *,
    model: SlayerModel,
    client: SlayerSQLClient,
) -> Optional[Dict[str, DataType]]:
    """Trial-execute ``model.sql`` with a 0-row guard and return cursor types.

    Returns ``None`` when the trial-execute itself fails — callers map that
    to ``WholeModelDelete``.
    """
    if not model.sql:
        return None
    # Strip trailing whitespace and a single statement terminator before
    # wrapping — a persisted ``SELECT 1;`` is valid at top level but
    # invalid inside ``SELECT * FROM (...) AS _sd_validate``. Without the
    # strip, that bogus syntax error would be attributed to drift and
    # produce a false WholeModelDelete.
    inner_sql = model.sql.rstrip()
    if inner_sql.endswith(";"):
        inner_sql = inner_sql[:-1].rstrip()
    try:
        trial_sql = f"SELECT * FROM ({inner_sql}) AS _sd_validate WHERE 1=0"
        cats = await client.get_column_types(trial_sql)
    except Exception as exc:
        logger.info(
            "validate_models: trial-execute on %r failed: %s",
            model.name,
            exc,
        )
        return None
    return {
        name: _CURSOR_CATEGORY_TO_DATATYPE.get(cat, DataType.TEXT)
        for name, cat in cats.items()
    }


# ===========================================================================
# Datasource-level orchestrator
# ===========================================================================


def _resolve_live_table(
    *, sql_table: str, live_tables: Dict[str, LiveTable]
) -> Optional[LiveTable]:
    """Look up a model's ``sql_table`` in the live introspection map,
    falling back to the bare name when the persisted value is schema-
    qualified (``schema.table``).
    """
    live = live_tables.get(sql_table)
    if live is None and "." in sql_table:
        live = live_tables.get(sql_table.split(".", 1)[1])
    return live


def _is_validate_models_base_column(col: Column) -> bool:
    """Same base-column predicate as the storage refinement: ``col.sql``
    is None or a single bare identifier."""
    if col.type is not DataType.INT:
        return False
    if col.sql is None:
        return True
    s = col.sql.strip()
    if not s or s[0].isdigit():
        return False
    return all(c.isalnum() or c == "_" for c in s)


def _probe_validate_models_column(
    *, conn, model: SlayerModel, col: Column, table_name: str,
    schema_name: Optional[str],
) -> Optional[DataType]:
    """Run the affinity probe for one column in a validate_models pass."""
    from slayer.sql.sqlite_introspect import probe_sqlite_integer_column
    try:
        return probe_sqlite_integer_column(
            conn=conn,
            table=table_name,
            column=col.sql or col.name,
            schema=schema_name,
        )
    except Exception as exc:
        logger.warning(
            "validate_models probe raised for %s.%s; ignoring: %s",
            model.name, col.name, exc,
        )
        return None


def _drift_reason_for_probe(
    *, model: SlayerModel, col: Column, verdict: DataType,
) -> DeleteReason:
    return DeleteReason(
        target=f"column:{col.name}",
        reason=(
            f"SQLite affinity probe widened {model.name}.{col.name} "
            f"from INT to {verdict.value}; re-run `slayer ingest` "
            f"to recreate with the correct type."
        ),
    )


def _sqlite_probe_int_drift_for_model(
    *,
    model: SlayerModel,
    sa_engine,
    default_schema: Optional[str] = None,
) -> List[Tuple[str, DataType, DeleteReason]]:
    """DEV-1538: probe-driven type-drift detection on SQLite.

    For every persisted base column with ``Column.type == DataType.INT``,
    open a connection from ``sa_engine`` and run
    :func:`probe_sqlite_integer_column` against the live storage. When the
    probe disagrees (verdict DOUBLE or TEXT), return a list of
    ``(column_name, verdict, DeleteReason)`` tuples so the caller can
    merge them into the model's diff state BEFORE the cascade fixed-point
    walk fires.

    ``default_schema`` (typically ``datasource.schema_name``) is used as
    the SQLite schema when ``model.sql_table`` is an unqualified table
    name. Without this, attached SQLite schemas would silently fall back
    to ``main`` and drift would be skipped or attributed to the wrong DB.

    Probe failures (``None`` verdict — explicit failure or saturated
    sample) silently skip; the helper's own WARNING covers them.
    """
    if model.sql_table is None:
        return []

    if "." in model.sql_table:
        schema_name, _, table_name = model.sql_table.partition(".")
        schema_name = schema_name or None
    else:
        schema_name, table_name = default_schema or None, model.sql_table

    drifts: List[Tuple[str, DataType, DeleteReason]] = []
    with sa_engine.connect() as conn:
        for col in model.columns:
            if not _is_validate_models_base_column(col):
                continue
            verdict = _probe_validate_models_column(
                conn=conn, model=model, col=col,
                table_name=table_name, schema_name=schema_name,
            )
            if verdict is None or verdict is DataType.INT:
                continue
            drifts.append(
                (col.name, verdict, _drift_reason_for_probe(
                    model=model, col=col, verdict=verdict,
                ))
            )
    return drifts


async def _sqlite_probe_drifts_for_models(
    *,
    datasource: DatasourceConfig,
    sql_table_models: List[SlayerModel],
) -> Dict[str, List[Tuple[str, DataType, DeleteReason]]]:
    """Run the SQLite probe for every model in one synchronous worker so
    the engine + connection lifecycle is shared across the validate pass."""
    if not sql_table_models:
        return {}
    if (datasource.type or "").lower() != "sqlite":
        return {m.name: [] for m in sql_table_models}

    def _run() -> Dict[str, List[Tuple[str, DataType, DeleteReason]]]:
        from slayer.sql import engine_factory
        sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
        try:
            out: Dict[str, List[Tuple[str, DataType, DeleteReason]]] = {}
            default_schema = datasource.schema_name or None
            for m in sql_table_models:
                out[m.name] = _sqlite_probe_int_drift_for_model(
                    model=m,
                    sa_engine=sa_engine,
                    default_schema=default_schema,
                )
            return out
        finally:
            # Cached engine — do not dispose; engine_factory owns lifecycle.
            pass

    return await asyncio.to_thread(_run)


def _merge_probe_drifts_into_diff(
    *,
    model: SlayerModel,
    base_diff: Tuple[Optional[ToDeleteEntry], Set[str]],
    probe_drifts: List[Tuple[str, DataType, DeleteReason]],
) -> Tuple[Optional[ToDeleteEntry], Set[str]]:
    """Merge ``probe_drifts`` from
    :func:`_sqlite_probe_int_drift_for_model` into a model's
    ``(entry, dropped_columns)`` diff so the cascade fixed-point walk in
    :func:`compute_datasource_drops` treats them as regular column drops.

    Skipped when ``base_diff`` is already a :class:`WholeModelDelete`
    (the whole model is going anyway) or when there are no drifts.
    """
    if not probe_drifts:
        return base_diff
    base_entry, dropped = base_diff
    if isinstance(base_entry, WholeModelDelete):
        return base_diff

    drift_cols = [name for name, _, _ in probe_drifts]
    drift_reasons = [reason for _, _, reason in probe_drifts]

    if base_entry is None:
        merged_entry = EditModelDelete(
            model_name=model.name,
            data_source=model.data_source,
            remove=RemoveSpec(columns=list(drift_cols)),
            reasons=list(drift_reasons),
        )
    else:
        # base_entry is an EditModelDelete; append probe columns + reasons
        # without dropping anything that was already there.
        merged_columns = list(base_entry.remove.columns)
        for c in drift_cols:
            if c not in merged_columns:
                merged_columns.append(c)
        merged_entry = base_entry.model_copy(
            update={
                "remove": base_entry.remove.model_copy(
                    update={"columns": merged_columns}
                ),
                "reasons": list(base_entry.reasons) + list(drift_reasons),
            }
        )

    merged_dropped = set(dropped) | set(drift_cols)
    return merged_entry, merged_dropped


def _diff_one_sql_table_model(
    *,
    model: SlayerModel,
    live_tables: Dict[str, "LiveTable"],
    available_in_ds: Set[str],
    probe_drifts: List[Tuple[str, DataType, DeleteReason]],
) -> Tuple[Optional[ToDeleteEntry], Set[str]]:
    """Per-model body of :func:`_collect_sql_table_diffs` — resolves the
    live table, runs ``diff_sql_table_model``, and merges any DEV-1538
    SQLite probe drifts so the cascade walk treats them as regular drops."""
    live = _resolve_live_table(
        sql_table=model.sql_table or "", live_tables=live_tables,
    )
    base = diff_sql_table_model(
        model=model,
        live_table=live,
        available_models_in_ds=available_in_ds,
    )
    return _merge_probe_drifts_into_diff(
        model=model, base_diff=base, probe_drifts=probe_drifts,
    )


async def _collect_sql_table_diffs(
    *,
    datasource: DatasourceConfig,
    sql_table_models: List[SlayerModel],
    available_in_ds: Set[str],
) -> Dict[str, Tuple[Optional[ToDeleteEntry], Set[str]]]:
    """Run live SQLAlchemy introspection (off the event loop) and diff each
    sql_table-mode model against it. On SQLite, additionally run the
    DEV-1538 affinity probe per persisted INT base column and merge any
    drift entries into each model's diff so the cascade fixed-point walk
    sees them as regular column drops.
    """
    if not sql_table_models:
        return {}
    # Honour the datasource's configured schema_name so non-default-schema
    # datasources diff against the right table set; otherwise SQLAlchemy
    # introspects the default and produces false WholeModelDeletes.
    live_tables = await asyncio.to_thread(
        _live_schema_for_datasource,
        datasource=datasource,
        schema=datasource.schema_name or None,
    )
    probe_drifts_by_model = await _sqlite_probe_drifts_for_models(
        datasource=datasource,
        sql_table_models=sql_table_models,
    )
    return {
        m.name: _diff_one_sql_table_model(
            model=m,
            live_tables=live_tables,
            available_in_ds=available_in_ds,
            probe_drifts=probe_drifts_by_model.get(m.name, []),
        )
        for m in sql_table_models
    }


async def _collect_sql_diffs(
    *,
    datasource: DatasourceConfig,
    sql_models: List[SlayerModel],
    sql_clients: Optional[Dict[Tuple[str, str], SlayerSQLClient]],
) -> Dict[str, Tuple[Optional[ToDeleteEntry], Set[str]]]:
    """Trial-execute each sql-mode model concurrently and produce its diff."""
    out: Dict[str, Tuple[Optional[ToDeleteEntry], Set[str]]] = {}
    if not sql_models:
        return out
    # DEV-1551: SlayerQueryEngine._sql_clients is tuple-keyed
    # (connection_string, runtime_fingerprint) so Snowflake datasources
    # sharing a connection_name but differing in warehouse/role get
    # distinct clients. Mirror that key shape here via the shared
    # ``_sql_client_cache_key`` helper.
    from slayer.engine.query_engine import _sql_client_cache_key  # noqa: PLC0415
    client = (sql_clients or {}).get(_sql_client_cache_key(datasource))
    if client is None:
        client = SlayerSQLClient(datasource=datasource)

    async def _diff_one(model: SlayerModel) -> None:
        live_cols = await _live_columns_for_sql_model(model=model, client=client)
        out[model.name] = diff_sql_model(model=model, live_columns=live_cols)

    await asyncio.gather(*(_diff_one(m) for m in sql_models))
    return out


async def validate_datasource(
    *,
    datasource: DatasourceConfig,
    models: List[SlayerModel],
    sql_clients: Optional[Dict[Tuple[str, str], SlayerSQLClient]] = None,
) -> List[ToDeleteEntry]:
    """Validate every persisted model in ``models`` (all in the same DS)
    against the live schema of ``datasource``. Read-only.
    """
    if not models:
        return []

    available_in_ds = {m.name for m in models}
    sql_table_diffs = await _collect_sql_table_diffs(
        datasource=datasource,
        sql_table_models=[m for m in models if m.sql_table],
        available_in_ds=available_in_ds,
    )
    sql_diffs = await _collect_sql_diffs(
        datasource=datasource,
        sql_models=[m for m in models if m.sql],
        sql_clients=sql_clients,
    )
    return compute_datasource_drops(
        models=models,
        sql_table_diffs=sql_table_diffs,
        sql_diffs=sql_diffs,
    )
