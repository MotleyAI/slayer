"""Schema drift: diff persisted models against live schemas, emit minimal deletes. Read-only."""

from __future__ import annotations

import asyncio
import logging
from typing import (
    Annotated,
    Any,
    Literal,
    Optional,
    Set,
    Union,
)

import sqlalchemy as sa
import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from slayer.core.enums import DataType
from slayer.core.formula import parse_filter
from slayer.core.models import (
    Column,
    DatasourceConfig,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.core.refs import IDENTIFIER_RE
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.engine.introspect_utils import _safe_get_columns
from slayer.engine.schema_scope import (
    SchemaEnumerationError,
    SchemaRef,
    default_schema_ref,
    engine_qualifies_tokens,
    is_system_schema,
    resolve_ingest_scope,
    split_sql_table,
)
from slayer.engine import ingestion
from slayer.engine.ingestion import (
    _safe_get_pk_constraint,
    _sa_type_is_float,
    _sa_type_to_data_type,
)
from slayer.engine.column_expansion import resolve_ref_target
from slayer.engine.syntax import (
    AggCall,
    DottedRef,
    Ref,
    StarSource,
    parse_expr,
    walk_parsed_refs,
)
from slayer.sql import engine_factory, sqlite_introspect
from slayer.sql.client import SlayerSQLClient, build_sql_model_trial_query
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.engine_factory import EngineCacheKey, _sql_client_cache_key

logger = logging.getLogger(__name__)


# Public payload types


class DeleteReason(BaseModel):
    """Reason attached to a delete entry — surfaces in CLI / MCP / REST output."""

    target: str  # e.g. "column:status", "measure:aov", "join:customers", "model:orders"
    reason: str


class RemoveSpec(BaseModel):
    """Per-entity removal spec, mirroring the MCP ``edit_model`` ``remove=`` shape."""

    columns: list[str] = Field(default_factory=list)
    measures: list[str] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)
    joins: list[str] = Field(default_factory=list)


class EditModelDelete(BaseModel):
    """Surgical removals on an existing model. Replays as ``edit_model``."""

    tool: Literal["edit_model"] = "edit_model"
    model_name: str
    data_source: str
    remove: RemoveSpec = Field(default_factory=RemoveSpec)
    remove_filters: list[str] = Field(default_factory=list)
    reasons: list[DeleteReason] = Field(default_factory=list)


class WholeModelDelete(BaseModel):
    """Whole-model removal. Replays as ``delete_model``."""

    tool: Literal["delete_model"] = "delete_model"
    model_name: str
    data_source: str
    reasons: list[DeleteReason] = Field(default_factory=list)
    # "invalid_sql": model's own SQL fails though its tables/columns exist.
    cause: Literal["schema_drift", "invalid_sql"] = "schema_drift"


ToDeleteEntry = Annotated[
    EditModelDelete | WholeModelDelete, Field(discriminator="tool")
]


class ModelAddition(BaseModel):
    """One model touched by an idempotent re-ingestion pass."""

    model_name: str
    data_source: str
    created: bool = False  # True if the model was new
    new_columns: list[str] = Field(default_factory=list)
    new_joins: list[str] = Field(default_factory=list)
    # INT columns widened to DOUBLE/TEXT by the SQLite affinity probe.
    widened_columns: list[str] = Field(default_factory=list)
    # Output-only label; durable record is SlayerModel.source_kind.
    source_kind: str | None = None
    # e.g. "view → table" when a re-ingest found the live object changed kind.
    kind_change: str | None = None
    # Columns whose empty description was filled from a DB comment.
    described_columns: list[str] = Field(default_factory=list)
    model_described: bool = False
    # Self-heal that added a missing schema qualifier ("reports → openfda_rest.reports").
    sql_table_change: str | None = None


class IngestionError(BaseModel):
    """Per-model failure during idempotent ingestion."""

    model_name: str
    data_source: str
    error: str


class IdempotentIngestResult(BaseModel):
    """Combined return shape of the idempotent ``ingest_datasource`` pass."""

    additions: list[ModelAddition] = Field(default_factory=list)
    to_delete: list[ToDeleteEntry] = Field(default_factory=list)
    errors: list[IngestionError] = Field(default_factory=list)
    # Live objects that couldn't be modelled at all; entries are IngestableObject.
    skipped: list[Any] = Field(default_factory=list)
    # Every live object discovered this pass; entries are IngestableObject.
    objects: list[Any] = Field(default_factory=list)
    # ELT/migration bookkeeping modelled hidden; entries are InternalTable.
    hidden_internals: list[Any] = Field(default_factory=list)
    # True when the datasource description was filled from the BigQuery dataset.
    datasource_described: bool = False
    # Message naming schemas discovered but not ingested this pass, or None.
    schema_hint: str | None = None
    # Requested schemas dropped from scope with a reason; entries are SkippedSchema.
    skipped_schemas: list[Any] = Field(default_factory=list)


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

    applied: list[AppliedEntry] = Field(default_factory=list)
    errors: list[ApplyError] = Field(default_factory=list)
    residual: list[ToDeleteEntry] = Field(default_factory=list)


# Internal live-schema input shapes


class LiveTable(BaseModel):
    """One live table's columns/PK/FKs in SLayer's coarse type buckets."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    columns: dict[str, DataType] = Field(default_factory=dict)
    pk_columns: set[str] = Field(default_factory=set)
    # Each entry: (local_column, ref_table, ref_column)
    fk_relationships: list[tuple[str, str, str]] = Field(default_factory=list)


# Type-bucket comparison


def data_type_bucket(dt: DataType) -> str:
    """Coarse bucket for comparing types; INT/DOUBLE → "number", DATE/TIMESTAMP → "temporal" so driver variance isn't drift."""
    if dt in (DataType.INT, DataType.DOUBLE):
        return "number"
    if dt == DataType.TEXT:
        return "string"
    if dt == DataType.BOOLEAN:
        return "boolean"
    if dt in (DataType.DATE, DataType.TIMESTAMP):
        return "temporal"
    return str(dt)


def _type_buckets_conflict(*, persisted: DataType, live: DataType) -> bool:
    """True when persisted vs live types are incompatible; opacity is one-way (live-opaque + operable-persisted conflicts, persisted-opaque never does)."""
    if live.is_opaque:
        return not persisted.is_opaque
    if persisted.is_opaque:
        return False
    return data_type_bucket(persisted) != data_type_bucket(live)


def _is_bare_identifier(s: str | None) -> bool:
    if not s:
        return False
    return IDENTIFIER_RE.match(s.strip()) is not None


def _column_is_base(col_sql: str | None) -> bool:
    """A base column claims a live column: Column.sql is None or a bare identifier."""
    if col_sql is None:
        return True
    return _is_bare_identifier(col_sql)


# Pure diff functions


def _diff_sql_table_columns(
    *, model: SlayerModel, live_table: LiveTable
) -> tuple[list[str], list[DeleteReason]]:
    """Per-column diff of a sql_table-mode model against live columns."""
    dropped: list[str] = []
    reasons: list[DeleteReason] = []
    for col in model.columns:
        # Base columns only; derived handled by cascade.
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
        if _type_buckets_conflict(persisted=col.type, live=live_dt):
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
    available_models_in_ds: set[str],
) -> tuple[list[str], list[DeleteReason]]:
    """Per-join diff of a sql_table-mode model against live FK columns and model availability."""
    dropped: list[str] = []
    reasons: list[DeleteReason] = []
    # join_pairs[*][0] is the semantic Column.name; resolve to the physical
    # Column.sql before the live-table check, else valid joins are dropped.
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
    live_table: LiveTable | None,
    available_models_in_ds: set[str],
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Diff a sql_table-mode model against live introspection → ``(entry_or_None, dropped_column_names)``; ``live_table is None`` → WholeModelDelete."""
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


def _sql_trial_failure_delete(
    *, model: SlayerModel, invalid_sql: bool
) -> tuple[WholeModelDelete, set[str]]:
    """``WholeModelDelete`` for a sql-mode model whose trial-execute failed."""
    if invalid_sql:
        reason = (
            "Trial-execute on model.sql failed although its source "
            "tables and referenced columns exist; the SQL itself does "
            "not execute against this datasource"
        )
    else:
        reason = (
            "Trial-execute on model.sql failed; the SQL no longer "
            "parses or executes against the live datasource"
        )
    return (
        WholeModelDelete(
            model_name=model.name,
            data_source=model.data_source,
            cause="invalid_sql" if invalid_sql else "schema_drift",
            reasons=[
                DeleteReason(target=f"model:{model.name}", reason=reason)
            ],
        ),
        {c.name for c in model.columns},
    )


def _diff_sql_model_columns(
    *, model: SlayerModel, live_columns: dict[str, DataType]
) -> tuple[list[str], list[DeleteReason]]:
    """Model columns missing from (or type-conflicting with) cursor metadata."""
    dropped_cols: list[str] = []
    reasons: list[DeleteReason] = []
    for col in model.columns:
        # Cursor exposes alias names: match col.name, fall back to col.sql.
        live_dt = live_columns.get(col.name)
        if live_dt is None and col.sql is not None:
            live_dt = live_columns.get(col.sql.strip())
        if live_dt is None:
            # Base/aliased-base only; derived expressions handled by cascade.
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
        if _type_buckets_conflict(persisted=col.type, live=live_dt):
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
    return dropped_cols, reasons


def diff_sql_model(
    *,
    model: SlayerModel,
    live_columns: dict[str, DataType] | None,
    invalid_sql: bool = False,
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Diff a sql-mode model against cursor metadata; ``live_columns is None`` ⇒ WholeModelDelete, ``invalid_sql=True`` ⇒ the SQL itself is broken, not drift."""
    if live_columns is None:
        return _sql_trial_failure_delete(model=model, invalid_sql=invalid_sql)
    dropped_cols, reasons = _diff_sql_model_columns(
        model=model, live_columns=live_columns
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


# Reference extraction helpers (sqlglot AST + formula AST)


def _extract_column_refs_from_sql(sql: str) -> list[tuple[str | None, str]]:
    """All ``(qualifier, column_name)`` refs in a SQL expression; ``qualifier`` is None for bare identifiers, else the full dotted join-path (``__`` never split)."""
    try:
        parsed = sqlglot.parse_one(sql)
    except Exception:
        return []
    refs: list[tuple[str | None, str]] = []
    for col in parsed.find_all(exp.Column):
        parts = [p.name for p in col.parts]
        qualifier = ".".join(parts[:-1]) if len(parts) > 1 else None
        refs.append((qualifier, parts[-1]))
    return refs


def _parsed_ref_name(node: Union[Ref, DottedRef, AggCall]) -> Optional[str]:
    """Textual name of a reference-bearing parse node; AggCall collapses to its source name, ``*:count`` (StarSource) yields None."""
    if isinstance(node, AggCall):
        source = node.source
        if not isinstance(source, (Ref, DottedRef)):
            # Star, or a DEV-1826 expression source (the caller walks its
            # operand refs itself).
            return None
        node = source
    if isinstance(node, Ref):
        return node.name
    return ".".join(node.parts)


def _measure_formula_refs(formula: str) -> Set[str]:
    """Column/measure names in a Mode-B formula (dotted for cross-model);
    textual only. Both aggregation spellings parse natively (DEV-1826), and an
    unknown functional name defers as an ``AggCall`` candidate, so custom
    aggregations — joined-model ones included — need no registry walk."""
    try:
        parsed = parse_expr(formula)
    except Exception:
        return set()
    out: Set[str] = set()
    for node in walk_parsed_refs(parsed):
        if isinstance(node, AggCall) and not isinstance(
            node.source, (Ref, DottedRef, StarSource)
        ):
            # DEV-1826 expression source: attribute each operand ref.
            for inner in walk_parsed_refs(node.source):
                inner_name = _parsed_ref_name(inner)
                if inner_name is not None:
                    out.add(inner_name)
            continue
        name = _parsed_ref_name(node)
        if name is not None:
            out.add(name)
    return out


def _filter_refs(filter_str: str) -> list[str]:
    """Column references in a SQL-mode (Mode A) filter; ``[]`` on parse failure."""
    try:
        pf = parse_sql_predicate(filter_str)
    except Exception:
        return []
    return list(pf.columns)


def _filter_refs_dsl(filter_str: str) -> list[str]:
    """Column/measure references in a DSL (Mode B) filter; recovers base measures from ``agg_refs`` and strips synthesized colon aliases (``*`` excluded)."""
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
    models_by_name: dict[str, SlayerModel],
) -> SlayerModel | None:
    """Resolve a Mode-A qualifier to its terminal joined model; exact-matched then walked as a chain of exact hops (never ``__``-split), None if any hop fails."""
    if table_alias in (source_model.name, ""):
        return source_model
    return resolve_ref_target(
        qualifiers=tuple(table_alias.split(".")),
        source_model=source_model,
        resolve_model=models_by_name.get,
    )


def _resolve_dotted_ref_to_model(
    *,
    source_model: SlayerModel,
    dotted_ref: str,
    models_by_name: dict[str, SlayerModel],
) -> tuple[SlayerModel | None, str | None]:
    """Resolve a dotted ref (``customers.regions.name``) to ``(target_model, leaf)``; bare refs return ``(source_model, ref)``."""
    if "." not in dotted_ref:
        return source_model, dotted_ref
    prefix, leaf = dotted_ref.rsplit(".", 1)
    target = _walk_alias_to_target_model(
        source_model=source_model,
        table_alias=prefix,
        models_by_name=models_by_name,
    )
    return target, leaf


# Cascade helpers


def _pk_columns(model: SlayerModel) -> set[str]:
    return {c.name for c in model.columns if c.primary_key}


def _ensure_edit_entry(
    *,
    edit_entries: dict[str, EditModelDelete],
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
    edit_entries: dict[str, EditModelDelete],
    dropped_cols: dict[str, set[str]],
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
    edit_entries: dict[str, EditModelDelete],
    dropped_measures: dict[str, set[str]],
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
    edit_entries: dict[str, EditModelDelete],
    dropped_joins: dict[str, set[str]],
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
    edit_entries: dict[str, EditModelDelete],
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


# Query-backed cascade


def _resolve_stage_source_to_base(
    *,
    source_model: object,
    prior_stages_by_name: dict[str, SlayerQuery],
) -> str | None:
    """Walk a ``source_model`` ref (str / SlayerModel / ModelExtension / prior-stage name) back to a real persisted base model name."""
    seen: set[str] = set()
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
    """Resolved join-graph context for a query-backed stage: source name, extension-added join targets, and every model reachable via the in-DS graph."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    stage_source_name: str | None = None
    extension_targets: set[str] = Field(default_factory=set)
    reachable: set[str] = Field(default_factory=set)
    models_by_name: dict[str, SlayerModel] = Field(default_factory=dict)


def _build_stage_graph(
    *,
    stage: SlayerQuery,
    stage_source_name: str | None,
    models_by_name: dict[str, SlayerModel],
) -> _StageGraph:
    """Build a ``_StageGraph`` for one stage; ``stage_source_name`` None for inline sources."""
    extension_targets = _stage_join_targets(stage)
    reachable: set[str] = set()
    if stage_source_name:
        reachable.add(stage_source_name)
    reachable |= extension_targets
    frontier = list(reachable)
    visited: set[str] = set()
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
) -> str | None:
    """Walk ``ref`` through the stage's join graph; the leaf column name when it resolves to ``base_name``, else None (bare refs attribute to the stage source)."""
    if "." not in ref:
        return ref if graph.stage_source_name == base_name else None
    parts = ref.split(".")
    leaf = parts[-1]
    path = parts[:-1]
    current = graph.stage_source_name
    if current is None:
        return None
    # Root-qualified ref (``orders.amount`` from a stage rooted at orders):
    # ``orders`` isn't in its own join set, so treat path==[source] as same-model.
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
) -> set[str]:
    out: set[str] = set()
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
) -> set[str]:
    out: set[str] = set()
    for td in stage.time_dimensions or []:
        attributed = _attribute_ref_to_base(
            ref=td.dimension.full_name, base_name=base_name, graph=graph
        )
        if attributed is not None:
            out.add(attributed)
    return out


def _filter_refs_on_base(
    stage: SlayerQuery, base_name: str, graph: _StageGraph
) -> set[str]:
    out: set[str] = set()
    # Mode-B (DSL) filters: use the DSL parser so colon aggs/transforms surface.
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
    graph: _StageGraph | None = None,
) -> set[str]:
    """Column names referenced *on* ``base_name`` by one stage; walks the join graph for multi-hop/ModelExtension refs, ``graph=None`` falls back to string-prefix match."""
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


def _stage_join_targets(stage: SlayerQuery) -> set[str]:
    """Join target_model names referenced by a stage (they live on a ``ModelExtension`` source_model; plain stages return the empty set)."""
    source = getattr(stage, "source_model", None)
    joins = getattr(source, "joins", None) or []
    out: set[str] = set()
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
    dropped_cols: dict[str, set[str]],
    pk_per_model: dict[str, set[str]],
) -> set[str]:
    """Dropped column names on ``base_name`` this stage references (PKs excluded — rule 7)."""
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
    dropped_joins: dict[str, set[str]],
) -> str | None:
    """The conflicting target if ``stage`` references a column under a join dropped on ``base_name``, else None; bounded to ``graph.reachable``."""
    if base_name not in graph.reachable:
        return None
    targets = dropped_joins.get(base_name, set())
    if not targets:
        return None
    for target in targets:
        # Column names the stage references on ``target`` (via the join graph);
        # non-empty ⇒ the dropped join means those refs no longer resolve.
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
    whole_dropped_models: set[str],
    dropped_cols: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
    pk_per_model: dict[str, set[str]],
    candidate_base_names: set[str],
) -> DeleteReason | None:
    """First whole-drop trigger for one stage of a query-backed model, or None."""
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
    dropped_cols: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
    whole_dropped_models: set[str],
    pk_per_model: dict[str, set[str]],
    candidate_base_names: set[str] | None = None,
    models_by_name: dict[str, SlayerModel] | None = None,
) -> DeleteReason | None:
    """DeleteReason when a query-backed model should be whole-dropped by cascade from base drift, else None; ``candidate_base_names``/``models_by_name`` default empty for legacy callers."""
    if not qb_model.source_queries:
        return None
    stages = list(qb_model.source_queries)
    models_by_name = models_by_name or {}

    for i, stage in enumerate(stages):
        prior_by_name: dict[str, SlayerQuery] = {}
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


# Cascade walker + collapse


class _CascadeState:
    """Mutable state threaded through the cascade helpers; a plain class (not Pydantic) so dict/set fields keep reference identity for in-place mutation."""

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
        models_by_name: dict[str, SlayerModel],
        edit_entries: dict[str, EditModelDelete],
        whole_entries: dict[str, WholeModelDelete],
        dropped_cols: dict[str, set[str]],
        dropped_measures: dict[str, set[str]],
        dropped_joins: dict[str, set[str]],
        pk_per_model: dict[str, set[str]],
    ) -> None:
        self.models_by_name = models_by_name
        self.edit_entries = edit_entries
        self.whole_entries = whole_entries
        self.dropped_cols = dropped_cols
        self.dropped_measures = dropped_measures
        self.dropped_joins = dropped_joins
        self.pk_per_model = pk_per_model

    def cascadable(self, name: str) -> set[str]:
        """Cascadable column drops on ``name`` (excludes PKs — rule 7)."""
        return self.dropped_cols.get(name, set()) - self.pk_per_model.get(name, set())


def _column_ref_targets_dropped(
    *,
    table_alias: str | None,
    ref_col: str,
    model: SlayerModel,
    state: _CascadeState,
) -> tuple[bool, SlayerModel | None]:
    """Whether one ``(table_alias, ref_col)`` ref resolves to a dropped column → ``(is_dropped, resolved_target_model)``."""
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
) -> tuple[SlayerModel, str] | None:
    """``(target_model, ref_col)`` for the first ref in ``col.sql`` resolving to a dropped column, else None."""
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
    """Rules 1 + 5: derived ``Column.sql`` referencing dropped columns (same model or via joins)."""
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
) -> str | None:
    """Reason string if the measure ref resolves to a dropped column/measure, else None."""
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
    refs: set[str],
    model: SlayerModel,
    state: _CascadeState,
) -> str | None:
    """Cause string for the first ref resolving to a dropped column/measure, else None."""
    for ref in refs:
        cause = _measure_drop_cause(ref=ref, model=model, state=state)
        if cause is not None:
            return cause
    return None


def _cascade_measures(*, model: SlayerModel, state: _CascadeState) -> bool:
    """Rule 2: ``ModelMeasure.formula`` referencing a dropped column or measure."""
    changed = False
    dropped_set = state.dropped_measures.get(model.name, set())
    for measure in model.measures:
        if measure.name is None or measure.name in dropped_set:
            continue
        refs = _measure_formula_refs(measure.formula)
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
    """Rules 3a + 3b: local FK column dropped here, or foreign column dropped on the target."""
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
        # Raw dropped_cols, not cascadable: a target PK drop still invalidates the join.
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
    *, models: list[SlayerModel], state: _CascadeState
) -> bool:
    """Rule 6: query-backed model whose source_queries transitively reference dropped state — whole-drop."""
    changed = False
    whole_dropped_names = set(state.whole_entries.keys())
    # Every model in the DS is a candidate base for cross-model dotted refs.
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
        # Treat all columns as dropped so further rounds propagate transitively.
        state.dropped_cols[model.name] = {c.name for c in model.columns}
        changed = True
    return changed


def _cascade_one_pass(
    *,
    models: list[SlayerModel],
    models_by_name: dict[str, SlayerModel],
    edit_entries: dict[str, EditModelDelete],
    whole_entries: dict[str, WholeModelDelete],
    dropped_cols: dict[str, set[str]],
    dropped_measures: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
    pk_per_model: dict[str, set[str]],
) -> bool:
    """Run one cascade pass; True if anything new was added."""
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
    entry: ToDeleteEntry | None,
    cols: set[str],
    edit_entries: dict[str, EditModelDelete],
    whole_entries: dict[str, WholeModelDelete],
    dropped_cols: dict[str, set[str]],
    dropped_measures: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
) -> None:
    """Apply one ``(entry, dropped_columns)`` diff to the cascade state dicts."""
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
    diffs_iterables: tuple[
        dict[str, tuple[ToDeleteEntry | None, set[str]]], ...
    ],
    edit_entries: dict[str, EditModelDelete],
    whole_entries: dict[str, WholeModelDelete],
    dropped_cols: dict[str, set[str]],
    dropped_measures: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
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
    edit_entries: dict[str, EditModelDelete],
    whole_entries: dict[str, WholeModelDelete],
) -> list[ToDeleteEntry]:
    """Collapse rule: whole-drop preempts edit on the same model; return name-sorted entries."""
    final: list[ToDeleteEntry] = []
    for name in sorted(set(edit_entries.keys()) | set(whole_entries.keys())):
        if name in whole_entries:
            final.append(whole_entries[name])
        else:
            final.append(edit_entries[name])
    return final


def compute_datasource_drops(
    *,
    models: list[SlayerModel],
    sql_table_diffs: dict[str, tuple[ToDeleteEntry | None, set[str]]],
    sql_diffs: dict[str, tuple[ToDeleteEntry | None, set[str]]],
) -> list[ToDeleteEntry]:
    """Combine per-model base diffs with cascade walking and the collapse rule; pure, caller must restrict ``models`` to one datasource (cascade doesn't cross datasources)."""
    edit_entries: dict[str, EditModelDelete] = {}
    whole_entries: dict[str, WholeModelDelete] = {}
    dropped_cols: dict[str, set[str]] = {}
    dropped_measures: dict[str, set[str]] = {}
    dropped_joins: dict[str, set[str]] = {}

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


# Live introspection


class IntrospectionUnavailable(Exception):
    """Every table failed to introspect — callers must not read this as "all dropped"."""


def _live_schema_refs(
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    datasource: DatasourceConfig,
    schema: str | None,
    fallback_schema_tokens: set[str] | None = None,
) -> list[SchemaRef]:
    """Schemas to introspect: explicit ``schema`` scopes to one, else every own-catalog schema (catalog attached via ``resolve_ingest_scope``); on enumeration failure ``fallback_schema_tokens`` is scoped, None stays fail-closed."""
    try:
        refs = resolve_ingest_scope(
            inspector=inspector,
            sa_engine=sa_engine,
            requested=[schema] if schema is not None else None,
            all_schemas=schema is None,
            datasource_schema=datasource.schema_name,
        ).schemas
    except SchemaEnumerationError as exc:
        if fallback_schema_tokens is None:
            raise IntrospectionUnavailable(
                f"validate_models: could not list schemas in datasource "
                f"{datasource.name!r}; skipping drift verdict to avoid false "
                f"deletions"
            ) from exc
        refs = []
    if refs or fallback_schema_tokens is None:
        return refs
    # Enumeration failed/empty: introspect exactly the models' schemas + the default.
    requested = set(fallback_schema_tokens)
    if datasource.schema_name:
        requested.add(datasource.schema_name)
    resolved = resolve_ingest_scope(
        inspector=inspector,
        sa_engine=sa_engine,
        requested=sorted(requested) or None,
        all_schemas=False,
        datasource_schema=datasource.schema_name,
    ).schemas
    # Add the connection default (bare models), deduped by token, preferring the default-marked ref.
    default_ref = default_schema_ref(inspector, sa_engine)
    candidates = list(resolved)
    default_token = default_ref.token or default_ref.name or ""
    if not is_system_schema(default_token, qualifies=engine_qualifies_tokens(sa_engine)):
        candidates.append(default_ref)  # skip a default that is itself a system schema (C2)
    by_token: dict[str | None, SchemaRef] = {}
    for ref in candidates:
        prev = by_token.get(ref.token)
        if prev is None or (ref.is_default and not prev.is_default):
            by_token[ref.token] = ref
    return list(by_token.values())


def _add_live_object(
    out: dict[str, LiveTable],
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    ref: SchemaRef,
    obj_name: str,
    single: bool,
    datasource: DatasourceConfig,
) -> None:
    """Introspect one object and key it into ``out`` (best-effort); the default/single-schema scope is keyed both bare and qualified so legacy and explicit models resolve."""
    try:
        live = _introspect_one_table(
            inspector=inspector, sa_engine=sa_engine, table_name=obj_name, ref=ref,
        )
    except Exception as exc:  # noqa: BLE001 — one object's introspection failed
        logger.warning(
            "validate_models: failed to introspect %r in datasource %r: %s",
            obj_name, datasource.name, exc,
        )
        return
    out[ref.qualify(obj_name)] = live
    if ref.is_default or single:
        out.setdefault(obj_name, live)
        if ref.name:
            out.setdefault(f"{ref.name}.{obj_name}", live)


def _collect_live_tables(
    refs: list[SchemaRef],
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    datasource: DatasourceConfig,
) -> tuple[dict[str, LiveTable], int]:
    """Introspect every object across ``refs`` into a ``{key: LiveTable}`` map (persisted-``sql_table`` keys, default also bare); returns ``(map, object_count)``."""
    single = len(refs) == 1
    out: dict[str, LiveTable] = {}
    object_count = 0
    failed: list[str] = []
    for ref in refs:
        try:
            # Module-attr call keeps test patches on ``ingestion`` effective.
            objs = ingestion.list_ingestable_objects(
                inspector=inspector, ref=ref, include_views=True
            )
        except Exception as exc:  # noqa: BLE001 — one schema's listing failed
            logger.warning(
                "validate_models: failed to list schema %r in datasource "
                "%r: %s", ref.token, datasource.name, exc,
            )
            failed.append(str(ref.token))
            continue
        for obj in objs:
            object_count += 1
            _add_live_object(
                out=out, inspector=inspector, sa_engine=sa_engine, ref=ref,
                obj_name=obj.name, single=single, datasource=datasource,
            )
    if failed:
        # Fail closed: a partial map would report every model in the unlisted
        # schema as a WholeModelDelete — data loss under --force-clean.
        raise IntrospectionUnavailable(
            f"validate_models: could not list schema(s) {failed} in datasource "
            f"{datasource.name!r}; skipping drift verdict to avoid false deletions"
        )
    return out, object_count


def _live_schema_for_datasource(
    *,
    datasource: DatasourceConfig,
    schema: str | None = None,
    fallback_schema_tokens: set[str] | None = None,
) -> dict[str, LiveTable]:
    """``{object_name: LiveTable}`` for every live table AND view in the DS; views are included so a model over a view isn't falsely reported as a WholeModelDelete."""
    sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
    try:
        inspector = sa.inspect(sa_engine)
        refs = _live_schema_refs(
            inspector=inspector, sa_engine=sa_engine,
            datasource=datasource, schema=schema,
            fallback_schema_tokens=fallback_schema_tokens,
        )
        out, object_count = _collect_live_tables(
            refs, inspector=inspector, sa_engine=sa_engine, datasource=datasource,
        )
        if object_count and not out:
            raise IntrospectionUnavailable(
                f"failed to introspect every table in datasource "
                f"{datasource.name!r} ({object_count} table(s))"
            )
        return out
    finally:
        # One-shot admin path: dispose to release the connection (unblocks direct
        # file access); quiet so it can't mask an in-flight introspection error.
        ingestion._dispose_quietly(sa_engine)


def _introspect_one_table(
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    ref: SchemaRef | None,
) -> LiveTable:
    """Build a ``LiveTable`` for one table via ingestion's safe-introspection path; ``ref`` keeps column/PK fallbacks catalog-scoped on DuckDB."""
    schema_token = ref.token if ref else None
    cols_meta = _safe_get_columns(inspector, sa_engine, table_name, ref)
    pk = _safe_get_pk_constraint(inspector, sa_engine, table_name, ref)
    pk_columns = set(pk.get("constrained_columns", []) or [])

    columns: dict[str, DataType] = {}
    for col in cols_meta:
        col_type = col["type"]
        if isinstance(col_type, DataType):
            columns[col["name"]] = col_type
        else:
            columns[col["name"]] = _sa_type_to_data_type(col_type)
            # is_float irrelevant for bucket comparison (INT/FLOAT → NUMBER).
            _ = _sa_type_is_float(col_type)

    fks: list[tuple[str, str, str]] = []
    try:
        for fk in inspector.get_foreign_keys(table_name, schema=schema_token):
            constrained = fk.get("constrained_columns") or []
            referred_table = fk.get("referred_table")
            referred = fk.get("referred_columns") or []
            for src, tgt in zip(constrained, referred):
                if referred_table:
                    fks.append((src, referred_table, tgt))
    except Exception:
        # Some dialects (ClickHouse, BigQuery) don't surface FK metadata; joins
        # are still validated by name.
        pass

    return LiveTable(columns=columns, pk_columns=pk_columns, fk_relationships=fks)


# Cursor type-category strings → DataType buckets.
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
) -> dict[str, DataType] | None:
    """Trial-execute ``model.sql`` with a 0-row guard; return cursor types, or None on failure."""
    if not model.sql:
        return None
    # Trailing ``;`` stripped before wrapping, else its syntax error looks like drift.
    try:
        cats = await client.get_column_types(build_sql_model_trial_query(model.sql))
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


def _has_star_projection(parsed: exp.Expression) -> bool:
    """Whether any SELECT in the statement projects ``*`` or ``t.*``."""
    for select in parsed.find_all(exp.Select):
        for proj in select.expressions:
            if isinstance(proj, exp.Star):
                return True
            if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
                return True
    return False


def _scope_table_sources(*, scope: Scope, dialect: str) -> dict[str, str]:
    """Alias/name → rendered bare table for one scope's physical tables (CTE/derived excluded)."""
    out: dict[str, str] = {}
    for name, source in scope.sources.items():
        if isinstance(source, exp.Table) and source.name:
            bare = source.copy()
            bare.set("alias", None)
            out[name] = bare.sql(dialect=dialect)
    return out


def _add_using_join_columns(
    *,
    scope: Scope,
    local_tables: dict[str, str],
    refs: dict[str, set[str]],
    dialect: str,
) -> bool:
    """Attribute ``JOIN .. USING (k)`` keys to the join's operand tables (USING idents aren't Columns, so the walk misses them); True when an operand is a CTE/derived table that can't be verified."""
    joins = scope.expression.args.get("joins") or []
    if not any(join.args.get("using") for join in joins):
        return False
    args = scope.expression.args
    from_expr = args.get("from_") or args.get("from")  # key renamed in sqlglot 30
    operands = [from_expr.this] if from_expr else []
    cannot_verify = False
    for join in joins:
        operands.append(join.this)
        using = join.args.get("using") or []
        if not using:
            continue
        physical = [
            rendered
            for op in operands
            if isinstance(op, exp.Table)
            and (rendered := local_tables.get(op.alias_or_name)) is not None
        ]
        if len(physical) < len(operands):
            cannot_verify = True
        rendered_keys = [
            exp.Column(this=ident.copy()).sql(dialect=dialect)
            for ident in using
            if isinstance(ident, exp.Identifier)
        ]
        for table in physical:
            refs[table].update(rendered_keys)
    return cannot_verify


def _attribute_qualified_ref(
    *,
    qualifier: str,
    rendered: str,
    scope: Scope,
    tables_by_scope: dict[int, dict[str, str]],
    refs: dict[str, set[str]],
) -> bool:
    """Attribute ``qual.col`` in the nearest enclosing scope defining the qualifier; True ⇒ derived/CTE/unknown qualifier (not attributable)."""
    s = scope
    while s is not None:
        target = (tables_by_scope.get(id(s)) or {}).get(qualifier)
        if target is not None:
            refs[target].add(rendered)
            return False
        if qualifier in s.sources:  # CTE / derived-table alias
            return True
        s = s.parent
    return True


def _attribute_bare_ref(
    *,
    rendered: str,
    scope: Scope,
    tables_by_scope: dict[int, dict[str, str]],
    refs: dict[str, set[str]],
) -> bool | None:
    """Attribute a bare column in the nearest enclosing scope with sources; True ⇒ skippable derived ref, None ⇒ ambiguous over several sources."""
    s = scope
    while s is not None and not s.sources:
        s = s.parent
    if s is None:
        return True
    if len(s.sources) > 1:
        return None
    local = tables_by_scope.get(id(s)) or {}
    if not local:
        return True  # the single source is a CTE / derived table
    refs[next(iter(local.values()))].add(rendered)
    return False


def _attribute_scope_columns(
    *,
    scope: Scope,
    tables_by_scope: dict[int, dict[str, str]],
    refs: dict[str, set[str]],
    seen: set[int],
    dialect: str,
) -> bool | None:
    """Attribute every yet-unseen column ref of one scope; ``seen`` (node ids) processes each ref once at its innermost scope, None when a bare column can't be attributed."""
    derived = False
    for col in scope.columns:
        if id(col) in seen or not isinstance(col.this, exp.Identifier):
            continue
        seen.add(id(col))
        rendered = exp.Column(this=col.this.copy()).sql(dialect=dialect)
        if col.table:
            verdict = _attribute_qualified_ref(
                qualifier=col.table, rendered=rendered, scope=scope,
                tables_by_scope=tables_by_scope, refs=refs,
            )
        else:
            verdict = _attribute_bare_ref(
                rendered=rendered, scope=scope,
                tables_by_scope=tables_by_scope, refs=refs,
            )
        if verdict is None:
            return None
        derived = derived or verdict
    return derived


def _sql_model_source_refs(*, sql: str, dialect: str) -> "dict[str, set[str]] | None":
    """Map each source table of a sql-mode model to the columns it references (incl. JOIN USING keys); None when unparseable, a bare column isn't attributable, or a CTE/derived ref coexists with ``*``."""
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
        scopes = traverse_scope(parsed)
    except Exception:
        return None
    if not scopes:
        return None
    refs: dict[str, set[str]] = {}
    tables_by_scope: dict[int, dict[str, str]] = {}
    has_derived_ref = False
    for scope in scopes:
        local = _scope_table_sources(scope=scope, dialect=dialect)
        tables_by_scope[id(scope)] = local
        for rendered in local.values():
            refs.setdefault(rendered, set())
        if _add_using_join_columns(
            scope=scope, local_tables=local, refs=refs, dialect=dialect
        ):
            has_derived_ref = True
    seen: set[int] = set()
    for scope in scopes:  # innermost first — a bare column binds innermost
        derived = _attribute_scope_columns(
            scope=scope, tables_by_scope=tables_by_scope, refs=refs,
            seen=seen, dialect=dialect,
        )
        if derived is None:
            return None
        has_derived_ref = has_derived_ref or derived
    if has_derived_ref and _has_star_projection(parsed):
        # A ``*`` may hide the physical column behind the derived ref.
        return None
    return refs


async def _source_tables_resolve(
    *, model: SlayerModel, client: SlayerSQLClient
) -> bool:
    """Whether every table AND column in ``model.sql`` accepts a 0-row probe; True ⇒ the SQL itself is broken (not drift), any probe failure keeps the drift classification."""
    dialect = dialect_for_ds_type(client.datasource.type).sqlglot_name
    refs = _sql_model_source_refs(sql=model.sql or "", dialect=dialect)
    if refs is None:
        return False
    for table, columns in refs.items():
        select_list = ", ".join(sorted(columns)) or "*"
        try:
            await client.get_column_types(
                f"SELECT {select_list} FROM {table} AS _sd_probe WHERE 1=0"
            )
        except Exception:
            return False
    return True


# Datasource-level orchestrator


def _strip_ident_quotes(ident: str) -> str:
    """Strip surrounding double-quotes from an identifier and unescape ``""`` → ``"``."""
    ident = ident.strip()
    if len(ident) >= 2 and ident[0] == '"' == ident[-1]:
        return ident[1:-1].replace('""', '"')
    return ident


def _resolve_live_table(
    *, sql_table: str, live_tables: dict[str, LiveTable]
) -> LiveTable | None:
    """Look up a model's ``sql_table`` in the live map (full / last-two / unquoted variants); a qualified name is NEVER stripped to bare, which would mask a dropped non-default twin."""
    parts = sql_table.split(".")
    # Unquote per segment, not the whole string (only the object is quoted).
    unq = [_strip_ident_quotes(p) for p in parts]
    candidates = [sql_table, ".".join(unq)]
    if len(parts) >= 2:
        candidates.append(".".join(parts[-2:]))
        candidates.append(".".join(unq[-2:]))
    for name in candidates:
        live = live_tables.get(name)
        if live is not None:
            return live
    return None


def _is_validate_models_base_column(col: Column) -> bool:
    """Same base-column predicate as storage refinement: ``col.sql`` None or a bare identifier."""
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
    schema_name: str | None,
) -> DataType | None:
    """Run the affinity probe for one column in a validate_models pass."""
    try:
        # Module-attr call keeps test patches on ``sqlite_introspect`` effective.
        return sqlite_introspect.probe_sqlite_integer_column(
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
    default_schema: str | None = None,
) -> list[tuple[str, DataType, DeleteReason]]:
    """Probe-driven INT type-drift on SQLite: ``(column, verdict, DeleteReason)`` for each INT base column the probe widens to DOUBLE/TEXT; ``default_schema`` for unqualified tables, failures skip."""
    if model.sql_table is None:
        return []

    schema_name, table_name = split_sql_table(model.sql_table)
    if schema_name is None:
        schema_name = default_schema or None

    drifts: list[tuple[str, DataType, DeleteReason]] = []
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
    sql_table_models: list[SlayerModel],
) -> dict[str, list[tuple[str, DataType, DeleteReason]]]:
    """Run the SQLite probe for every model in one worker (shared engine/connection)."""
    if not sql_table_models:
        return {}
    if (datasource.type or "").lower() != "sqlite":
        return {m.name: [] for m in sql_table_models}

    def _run() -> dict[str, list[tuple[str, DataType, DeleteReason]]]:
        sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
        try:
            out: dict[str, list[tuple[str, DataType, DeleteReason]]] = {}
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
    base_diff: tuple[ToDeleteEntry | None, set[str]],
    probe_drifts: list[tuple[str, DataType, DeleteReason]],
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Merge probe drifts into a model's ``(entry, dropped_columns)`` diff as regular column drops; skipped for a WholeModelDelete or no drifts."""
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
        # EditModelDelete: append probe columns + reasons, keeping existing.
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
    live_tables: dict[str, "LiveTable"],
    available_in_ds: set[str],
    probe_drifts: list[tuple[str, DataType, DeleteReason]],
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Per-model body of ``_collect_sql_table_diffs``: resolve live table, diff, merge probe drifts."""
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
    sql_table_models: list[SlayerModel],
    available_in_ds: set[str],
) -> dict[str, tuple[ToDeleteEntry | None, set[str]]]:
    """Introspect live (off the event loop) and diff each sql_table-mode model; on SQLite also run the affinity probe and merge drift into each diff."""
    if not sql_table_models:
        return {}
    # Cover every own-catalog schema (a model may be qualified to another own
    # schema); fall back to the models' own schemas if enumeration is unavailable.
    fallback_tokens = {
        tok for m in sql_table_models
        if (tok := split_sql_table(m.sql_table or "")[0]) is not None
    }
    try:
        live_tables = await asyncio.to_thread(
            _live_schema_for_datasource,
            datasource=datasource,
            schema=None,
            fallback_schema_tokens=fallback_tokens,
        )
    except IntrospectionUnavailable as exc:
        # Unknown live schema: don't report deletions (a transient error would
        # hand --force-clean a whole tenant).
        logger.warning("validate_models: skipping drift verdict: %s", exc)
        return {}
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
    sql_models: list[SlayerModel],
    sql_clients: dict[EngineCacheKey, SlayerSQLClient] | None,
) -> dict[str, tuple[ToDeleteEntry | None, set[str]]]:
    """Trial-execute each sql-mode model concurrently and produce its diff."""
    out: dict[str, tuple[ToDeleteEntry | None, set[str]]] = {}
    if not sql_models:
        return out
    # Tuple-keyed like SlayerQueryEngine._sql_clients so Snowflake datasources
    # differing in warehouse/role get distinct clients.
    key = _sql_client_cache_key(datasource)
    client = (sql_clients or {}).get(key)
    if client is None:
        client = SlayerSQLClient(datasource=datasource)
        # Cache the client back so its asyncpg pool is reachable by
        # SlayerQueryEngine.aclose(). None sql_clients (direct/test) is unchanged.
        if sql_clients is not None:
            sql_clients[key] = client

    async def _diff_one(model: SlayerModel) -> None:
        live_cols = await _live_columns_for_sql_model(model=model, client=client)
        invalid_sql = live_cols is None and await _source_tables_resolve(
            model=model, client=client
        )
        out[model.name] = diff_sql_model(
            model=model, live_columns=live_cols, invalid_sql=invalid_sql
        )

    await asyncio.gather(*(_diff_one(m) for m in sql_models))
    return out


async def validate_datasource(
    *,
    datasource: DatasourceConfig,
    models: list[SlayerModel],
    sql_clients: dict[EngineCacheKey, SlayerSQLClient] | None = None,
) -> list[ToDeleteEntry]:
    """Validate every persisted model in ``models`` (all one DS) against the live schema. Read-only."""
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
