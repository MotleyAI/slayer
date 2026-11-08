"""Schema drift: diff persisted models against live schemas, emit minimal deletes. Read-only."""

from __future__ import annotations

import asyncio
import functools
import logging
import re
import time
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Coroutine
from contextlib import asynccontextmanager
from typing import (
    Annotated,
    Any,
    Literal,
    Set,
)

import sqlglot
from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from slayer.async_utils import marked_coroutine_function
from slayer.core.deprecation import warn_deprecated
from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    SlayerModel,
    is_base_column_sql,
    physical_column_sql,
)
from slayer.core.query import ModelExtension, SlayerQuery, SourceSpec
from slayer.core.refs import IDENTIFIER_RE
from slayer.core.warnings import BrokenDependentWarning
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.engine.introspect_utils import reflect_columns
from slayer.engine.schema_scope import (
    UNREACHABLE,
    SchemaEnumerationError,
    SchemaRef,
    default_schema_ref,
    is_system_schema,
    live_key,
    qualifies_tokens,
    resolve_ingest_scope,
    split_sql_table,
    sql_table_parts,
)
from slayer.engine import ingestion, query_engine
from slayer.core.errors import AmbiguousJoinPathError, CircularJoinPathError, StoredDocumentLoadError
from slayer.core.join_edges import JoinEdgeRef, edge_reference, is_addressable
from slayer.core.join_search import reachable
from slayer.core.join_walker import OrientedJoin, resolve_hop, walk
from slayer.sql.column_expansion import resolve_ref_target
from slayer.engine.dimension_routing import short_form_route_or_none
from slayer.core.syntax import (
    AggCall,
    DottedRef,
    ParsedExpr,
    Ref,
    StarSource,
    parse_expr,
    walk_parsed_refs,
)
from slayer.sql import sqlite_introspect
from slayer.sql.client import SqlClient, adapt_client, render_type_probe
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type
from slayer.sql.dialects.reflection import ResultColumnType
from slayer.sql.engine_factory import ClientCacheKey, client_cache_key
from slayer.sql.naming import render_table_name
from slayer.ir.source_bundle import added_joins

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
    # Edge references of the dropped joins that have one; ``join_edges`` addresses each exactly.
    joins: list[str] = Field(default_factory=list)
    join_edges: list[JoinEdgeRef] = Field(default_factory=list)

    def add_join(self, *, model: SlayerModel, join: ModelJoin) -> None:
        if is_addressable(model=model, join=join):
            self.joins.append(edge_reference(model=model, join=join))
        self.join_edges.append(JoinEdgeRef.of(join))


#: Model name → exact references of its joins dropped so far.
_DroppedJoins = dict[str, list[JoinEdgeRef]]


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
    # "invalid_sql": model's own SQL fails though its tables/columns exist;
    # "unloadable": the stored document itself cannot be loaded.
    cause: Literal["schema_drift", "invalid_sql", "unloadable"] = "schema_drift"


ToDeleteEntry = Annotated[
    EditModelDelete | WholeModelDelete, Field(discriminator="tool")
]


RemedyKind = Literal["reingest", "set_type"]

#: Remedy texts serialized before ``remedy_kind`` existed.
_LEGACY_REMEDIES: dict[RemedyKind, re.Pattern[str]] = {
    "reingest": re.compile(r"re-ingest datasource '.*' \(`slayer ingest`\) to re-type it"),
    "set_type": re.compile(r"set the column's type to [A-Z_]+"),
}


class RetypeFinding(BaseModel):
    """Report-only: a column whose live column exists with a conflicting type."""

    finding: Literal["retype"] = "retype"
    model_name: str
    data_source: str
    column: str
    stored_type: DataType
    live_type: DataType
    live_db_type: str | None = None
    remedy_kind: RemedyKind

    @model_validator(mode="before")
    @classmethod
    def _infer_legacy_remedy_kind(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "remedy_kind" in data:
            return data
        remedy = data.get("remedy")
        kind = next((k for k, pattern in _LEGACY_REMEDIES.items()
                     if isinstance(remedy, str) and pattern.fullmatch(remedy)), None)
        if kind is None:
            raise ValueError(f"cannot infer remedy_kind from remedy {remedy!r}")
        return {**data, "remedy_kind": kind}

    @computed_field
    @property
    def remedy(self) -> str:
        if self.remedy_kind == "reingest":
            return f"re-ingest datasource '{self.data_source}' (`slayer ingest`) to re-type it"
        return f"set the column's type to {self.live_type.value}"

    @computed_field
    @property
    def summary(self) -> str:
        live = f"{self.live_db_type} ({self.live_type.value})" if self.live_db_type else self.live_type.value
        return (f"Column '{self.model_name}.{self.column}' is stored as {self.stored_type.value} but its live "
                f"column is {live}; {self.remedy}.")

    @classmethod
    def of(
        cls, *, model: SlayerModel, column: Column, live_type: DataType, live_db_type: str | None,
        remedy_kind: RemedyKind,
    ) -> "RetypeFinding":
        return cls(
            model_name=model.name, data_source=model.data_source, column=column.name, stored_type=column.type,
            live_type=live_type, live_db_type=live_db_type, remedy_kind=remedy_kind,
        )


class NarrowedAccessFinding(BaseModel):
    """Report-only: the model reads tagged models (``narrowed_by``) whose tags do not cover its own."""

    finding: Literal["narrowed_access"] = "narrowed_access"
    model_name: str
    data_source: str
    narrowed_by: list[str]


#: Validation entries that report rather than delete.
REPORT_ONLY_FINDINGS = (NarrowedAccessFinding, RetypeFinding)


def unloadable_model_delete(*, error: StoredDocumentLoadError) -> WholeModelDelete:
    """The validation report's entry for a stored model that cannot be loaded."""
    return WholeModelDelete(
        model_name=error.name, data_source=error.data_source or "", cause="unloadable",
        reasons=[DeleteReason(target=f"model:{error.name}", reason=error.cause_text)],
    )


class ModelAddition(BaseModel):
    """One model touched by an idempotent re-ingestion pass."""

    model_name: str
    data_source: str
    created: bool = False  # True if the model was new
    new_columns: list[str] = Field(default_factory=list)
    # Added joins by edge reference.
    new_joins: list[str] = Field(default_factory=list)
    # Edge names this pass generated on the model's joins.
    named_joins: list[str] = Field(default_factory=list)
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
    # Stored definitions a list re-type this pass breaks.
    broken_dependents: list[BrokenDependentWarning] = Field(default_factory=list)


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
    """One live table's columns in SLayer's coarse type buckets."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    columns: dict[str, DataType] = Field(default_factory=dict)
    # Each live column's database type name, where the database reports one.
    db_types: dict[str, str] = Field(default_factory=dict)
    # Listed but its metadata read failed: present, yet no drift evidence.
    readable: bool = True


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


def _type_buckets_conflict(*, persisted: DataType, live: DataType, temporal_text: bool) -> bool:
    """True when persisted vs live types are incompatible; opacity is one-way (live-opaque + operable-persisted conflicts, persisted-opaque never does); a non-list over a live list is a re-type finding, never a conflict; ``temporal_text``: a DATE/TIMESTAMP over live TEXT is the dialect's own date storage."""
    if live is DataType.ARRAY and persisted is not DataType.ARRAY:
        return False
    if temporal_text and live is DataType.TEXT and persisted in (DataType.DATE, DataType.TIMESTAMP):
        return False
    if live.is_opaque:
        return not persisted.is_opaque
    if persisted.is_opaque:
        return False
    return data_type_bucket(persisted) != data_type_bucket(live)


def _retyped(*, persisted: DataType, live: DataType, temporal_text: bool) -> bool:
    return (live is DataType.ARRAY and persisted is not DataType.ARRAY) or _type_buckets_conflict(
        persisted=persisted, live=live, temporal_text=temporal_text)


def retype_findings(
    *, model: SlayerModel, live_table: LiveTable | None, temporal_text: bool = False,
) -> list[RetypeFinding]:
    """One finding per stored base column whose live column's type conflicts; a live list re-ingests."""
    if live_table is None:
        return []
    out: list[RetypeFinding] = []
    for col in model.columns:
        if not _column_is_base(col.sql):
            continue
        name = physical_column_sql(sql=col.sql, name=col.name)
        live = live_table.columns.get(name)
        if live is not None and _retyped(persisted=col.type, live=live, temporal_text=temporal_text):
            out.append(RetypeFinding.of(
                model=model, column=col, live_type=live, live_db_type=live_table.db_types.get(name),
                remedy_kind="reingest" if live is DataType.ARRAY else "set_type",
            ))
    return out


def sql_model_retype_findings(
    *, model: SlayerModel, live_columns: dict[str, ResultColumnType] | None, temporal_text: bool = False,
) -> list[RetypeFinding]:
    """One finding per column of a sql-mode model whose result column's type conflicts."""
    out: list[RetypeFinding] = []
    for col in model.columns:
        live = (live_columns or {}).get(col.name)
        if live is None and col.sql is not None:
            live = (live_columns or {}).get(col.sql.strip())
        if live is not None and _retyped(persisted=col.type, live=live.type, temporal_text=temporal_text):
            out.append(RetypeFinding.of(
                model=model, column=col, live_type=live.type, live_db_type=live.db_type,
                remedy_kind="reingest" if live.type is DataType.ARRAY else "set_type",
            ))
    return out


def _is_bare_identifier(s: str | None) -> bool:
    if not s:
        return False
    return IDENTIFIER_RE.match(s.strip()) is not None


def _column_is_base(col_sql: str | None) -> bool:
    """A base column claims a live column (see ``is_base_column_sql``)."""
    return is_base_column_sql(col_sql)


# Pure diff functions


def _diff_sql_table_columns(
    *, model: SlayerModel, live_table: LiveTable,
) -> tuple[list[str], list[DeleteReason]]:
    """Per-column diff of a sql_table-mode model against live columns."""
    dropped: list[str] = []
    reasons: list[DeleteReason] = []
    for col in model.columns:
        # Base columns only; derived handled by cascade.
        if not _column_is_base(col.sql):
            continue
        bare_name = physical_column_sql(sql=col.sql, name=col.name)
        if bare_name not in live_table.columns:
            dropped.append(col.name)
            reasons.append(
                DeleteReason(
                    target=f"column:{col.name}",
                    reason=f"Live column {bare_name!r} not found",
                )
            )
    return dropped, reasons


def _diff_sql_table_joins(
    *,
    model: SlayerModel,
    live_table: LiveTable,
    available_models_in_ds: set[str],
) -> tuple[list[ModelJoin], list[DeleteReason]]:
    """Per-join diff of a sql_table-mode model against live FK columns and model availability."""
    dropped: list[ModelJoin] = []
    reasons: list[DeleteReason] = []
    for join in model.joins:
        local_cols = [pair[0] for pair in join.join_pairs]
        missing_locals = [
            lc for lc in local_cols
            if (col := model.get_column(lc)) is None
            or physical_column_sql(sql=col.sql, name=col.name) not in live_table.columns
        ]
        if missing_locals:
            dropped.append(join)
            reasons.append(
                DeleteReason(
                    target=f"join:{edge_reference(model=model, join=join)}",
                    reason=(
                        f"Local FK column(s) {missing_locals} missing from "
                        f"live table"
                    ),
                )
            )
            continue
        if join.target_model not in available_models_in_ds:
            dropped.append(join)
            reasons.append(
                DeleteReason(
                    target=f"join:{edge_reference(model=model, join=join)}",
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

    dropped_cols, col_reasons = _diff_sql_table_columns(model=model, live_table=live_table)
    dropped_joins, join_reasons = _diff_sql_table_joins(
        model=model,
        live_table=live_table,
        available_models_in_ds=available_models_in_ds,
    )

    if not dropped_cols and not dropped_joins:
        return None, set()
    reasons = col_reasons + join_reasons
    remove = RemoveSpec(columns=dropped_cols)
    for join in dropped_joins:
        remove.add_join(model=model, join=join)

    return (
        EditModelDelete(
            model_name=model.name,
            data_source=model.data_source,
            remove=remove,
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
    *, model: SlayerModel, live_columns: Collection[str],
) -> tuple[list[str], list[DeleteReason]]:
    """Model columns missing from cursor metadata."""
    dropped_cols: list[str] = []
    reasons: list[DeleteReason] = []
    for col in model.columns:
        # Cursor exposes alias names: match col.name, fall back to col.sql.
        if col.name not in live_columns and (col.sql is None or col.sql.strip() not in live_columns):
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
    return dropped_cols, reasons


def diff_sql_model(
    *,
    model: SlayerModel,
    live_columns: Collection[str] | None,
    invalid_sql: bool = False,
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Diff a sql-mode model against cursor metadata's column names; ``live_columns is None`` ⇒ WholeModelDelete,
    ``invalid_sql=True`` ⇒ the SQL itself is broken, not drift."""
    if live_columns is None:
        return _sql_trial_failure_delete(model=model, invalid_sql=invalid_sql)
    dropped_cols, reasons = _diff_sql_model_columns(model=model, live_columns=live_columns)
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


def _parsed_ref_name(node: Ref | DottedRef | AggCall) -> str | None:
    """Textual name of a reference-bearing parse node; AggCall collapses to its source name, ``count(*)`` (StarSource) yields None."""
    if isinstance(node, AggCall):
        source = node.source
        if not isinstance(source, (Ref, DottedRef)):
            # Star, or an expression source (the caller walks its
            # operand refs itself).
            return None
        node = source
    if isinstance(node, Ref):
        return node.name
    return ".".join(node.parts)


def _walk_ref_names(parsed: ParsedExpr):
    """Yield the name of each reference in a parsed Mode-B tree; an ``AggCall``
    collapses to its source name, an expression source attributes each
    operand ref, ``*`` sources yield nothing."""
    for node in walk_parsed_refs(parsed):
        if isinstance(node, AggCall) and not isinstance(
            node.source, (Ref, DottedRef, StarSource)
        ):
            for inner in walk_parsed_refs(node.source):
                inner_name = _parsed_ref_name(inner)
                if inner_name is not None:
                    yield inner_name
            continue
        name = _parsed_ref_name(node)
        if name is not None:
            yield name


def _measure_formula_refs(formula: str) -> Set[str]:
    """Column/measure names in a Mode-B formula (dotted for cross-model);
    textual only. Both aggregation spellings parse natively, and an
    unknown functional name defers as an ``AggCall`` candidate, so custom
    aggregations — joined-model ones included — need no registry walk."""
    try:
        parsed = parse_expr(formula)
    except Exception:
        return set()
    return set(_walk_ref_names(parsed))


def _filter_refs(filter_str: str) -> list[str]:
    """Column references in a SQL-mode (Mode A) filter; ``[]`` on parse failure."""
    try:
        pf = parse_sql_predicate(filter_str)
    except Exception:
        return []
    return list(pf.columns)


def _filter_refs_dsl(filter_str: str) -> list[str]:
    """Column/measure references in a DSL (Mode B) filter, in expression order
    (deduplicated); ``[]`` on parse failure."""
    try:
        parsed = parse_expr(filter_str)
    except Exception:
        return []
    return list(dict.fromkeys(_walk_ref_names(parsed)))


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
        models_by_name=models_by_name,
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
    dropped_joins: _DroppedJoins,
    model: SlayerModel,
    join: ModelJoin,
    reason: str,
) -> bool:
    ref = JoinEdgeRef.of(join)
    if ref in dropped_joins.get(model.name, []):
        return False
    entry = _ensure_edit_entry(edit_entries=edit_entries, model=model)
    if ref not in entry.remove.join_edges:
        entry.remove.add_join(model=model, join=join)
        entry.reasons.append(
            DeleteReason(target=f"join:{edge_reference(model=model, join=join)}", reason=reason)
        )
    dropped_joins.setdefault(model.name, []).append(ref)
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
    source_model: SourceSpec | None,
    prior_stages_by_name: dict[str, SlayerQuery],
) -> str | None:
    """Walk a ``source_model`` spec (or prior-stage name) back to a real persisted base model name."""
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
        if isinstance(current, ModelExtension):
            current = current.source_name
            continue
        if isinstance(current, SlayerModel):
            return current.name
        return None


class _StageGraph(BaseModel):
    """Resolved join-graph context for a query-backed stage: source name, its extension-augmented
    model, extension-added joins, and every model reachable via the in-DS graph."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    stage_source_name: str | None = None
    # The stage source as the runtime binds it: the stored model plus the stage's extension joins.
    source: SlayerModel | None = None
    extension_joins: list[ModelJoin] = Field(default_factory=list)
    reachable: set[str] = Field(default_factory=set)
    models_by_name: dict[str, SlayerModel] = Field(default_factory=dict)

    @property
    def extension_targets(self) -> set[str]:
        return {j.target_model for j in self.extension_joins}


def _build_stage_graph(
    *,
    stage: SlayerQuery,
    stage_source_name: str | None,
    models_by_name: dict[str, SlayerModel],
) -> _StageGraph:
    """Build a ``_StageGraph`` for one stage; ``stage_source_name`` None for inline sources."""
    extension_joins = _stage_source_joins(stage)
    extension_targets = {j.target_model for j in extension_joins}
    base = models_by_name.get(stage_source_name) if stage_source_name else None
    if base is not None:
        extension_joins = added_joins(base=base, joins=extension_joins)
    source = base.model_copy(update={"joins": [*base.joins, *extension_joins]}) if base is not None else None
    models = {**models_by_name, source.name: source} if source is not None else models_by_name
    roots = [source] if source is not None else [models[t] for t in sorted(extension_targets) if t in models]
    found: set[str] = {stage_source_name} if stage_source_name else set()
    found |= extension_targets
    for root in roots:
        found |= reachable(root=root, models_by_name=models)
    return _StageGraph(
        stage_source_name=stage_source_name,
        source=source,
        extension_joins=list(extension_joins),
        reachable=found,
        models_by_name=models,
    )


def _hop_path(*, ref: str, graph: _StageGraph) -> list[str]:
    """``ref``'s hop tokens, a leading self-prefix stripped as the binder does."""
    path = ref.split(".")[:-1]
    return path[1:] if path and path[0] == graph.stage_source_name else path


def _attribute_ref_to_base(
    *,
    ref: str,
    base_name: str,
    graph: _StageGraph,
) -> str | None:
    """Walk ``ref`` through the stage's join graph; the leaf column name when it resolves to ``base_name``, else None (bare refs attribute to the stage source)."""
    if graph.stage_source_name is None:
        return None
    leaf = ref.rsplit(".", 1)[-1]
    path = _hop_path(ref=ref, graph=graph)
    if not path:
        return leaf if graph.stage_source_name == base_name else None
    walked = _walk_stage_chain(path=path, graph=graph)
    terminal = walked[0] if walked is not None else None
    if terminal is None and len(path) == 1:
        # Short-form auto-routing: a len==1 prefix with no direct join
        # attributes to its uniquely-routed terminal (mirroring full paths).
        terminal = _route_short_form_terminal(target=path[0], graph=graph)
    if terminal is None:
        return None
    return leaf if terminal == base_name else None


def _short_form_route(*, target: str, graph: _StageGraph) -> list[str] | None:
    """The hop route to a short-form target uniquely routable from the stage
    source over the datasource-scoped join graph (ambiguous / unreachable → None).
    Routing triggers only when the first hop resolves to no edge; a parallel pair
    directly off the source is a fail-closed ambiguous hop, not a
    route, so it attributes to nothing — mirroring the binder."""
    root = graph.source
    if root is None:
        return None
    try:
        if resolve_hop(
            current=root, token=target, models_by_name=graph.models_by_name,
        ) is not None:
            return None
    except AmbiguousJoinPathError:
        return None
    return short_form_route_or_none(
        root=root, target_model=target, models_by_name=graph.models_by_name,
    )


def _route_short_form_terminal(*, target: str, graph: _StageGraph) -> str | None:
    return target if _short_form_route(target=target, graph=graph) is not None else None


def _walk_stage_chain(*, path: list[str], graph: _StageGraph) -> tuple[str, list[OrientedJoin]] | None:
    """Terminal model of ``path`` spelled from the extension-augmented stage source and the stored
    edges it walks (extension joins excluded), or ``None``."""
    if graph.source is None:
        return None
    try:
        chain = walk(root=graph.source, path=path, models_by_name=graph.models_by_name)
    except (AmbiguousJoinPathError, CircularJoinPathError):
        return None
    if chain is None:
        return None
    extension = [JoinEdgeRef.of(j) for j in graph.extension_joins]
    stored = [e for e in chain if not (e.declaring_model == graph.source.name
                                       and any(r.matches_hop(e) for r in extension))]
    return (chain[-1].target_model if chain else graph.source.name), stored


def _ref_stored_edges(*, ref: str, graph: _StageGraph) -> list[OrientedJoin]:
    """The stored edges ``ref``'s path walks, a short-form ref along its route."""
    path = _hop_path(ref=ref, graph=graph)
    if not path:
        return []
    walked = _walk_stage_chain(path=path, graph=graph)
    if walked is None and len(path) == 1:
        route = _short_form_route(target=path[0], graph=graph)
        walked = _walk_stage_chain(path=route, graph=graph) if route else None
    return walked[1] if walked is not None else []


def _stage_refs(stage: SlayerQuery) -> list[str]:
    """Every column reference of one stage: measures, dimensions, time dimensions, filters."""
    refs = [
        ref for m in stage.measures or [] if (formula := getattr(m, "formula", None))
        for ref in _measure_formula_refs(formula)
    ]
    refs += [getattr(d, "full_name", None) or str(d) for d in stage.dimensions or []]
    refs += [ref for td in stage.time_dimensions or [] for ref in _measure_formula_refs(td.dimension)]
    # Mode-B (DSL) filters: use the DSL parser so aggregations/transforms surface.
    refs += [col for f in stage.filters or [] for col in _filter_refs_dsl(f)]
    return refs


def _stage_referenced_columns_for_base(
    *,
    stage: SlayerQuery,
    base_name: str,
    graph: _StageGraph,
) -> set[str]:
    """Column names referenced *on* ``base_name`` by one stage, walked through the stage's join graph."""
    attributed = (_attribute_ref_to_base(ref=ref, base_name=base_name, graph=graph) for ref in _stage_refs(stage))
    return {leaf for leaf in attributed if leaf is not None}


def _stage_source_joins(stage: SlayerQuery) -> list[ModelJoin]:
    """Joins declared on a stage's inline source (extension or inline model); none for a bare name."""
    source = stage.source_model
    if isinstance(source, ModelExtension):
        return source.joins or []
    if isinstance(source, SlayerModel):
        return source.joins
    return []


def _stage_join_targets(stage: SlayerQuery) -> set[str]:
    """Join target_model names referenced by a stage's inline source."""
    return {j.target_model for j in _stage_source_joins(stage)}


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
    dropped_joins: _DroppedJoins,
) -> str | None:
    """The reference of a join dropped on ``base_name`` that a ``stage`` ref walks, else None."""
    dropped = dropped_joins.get(base_name, [])
    if not dropped or base_name not in graph.reachable:
        return None
    for ref in _stage_refs(stage):
        for edge in _ref_stored_edges(ref=ref, graph=graph):
            hit = next((d for d in dropped if edge.declaring_model == base_name and d.matches_hop(edge)), None)
            if hit is not None:
                return hit.name or hit.target_model
    return None


def _check_stage_for_whole_drop(
    *,
    stage: SlayerQuery,
    base_name: str,
    qb_name: str,
    graph: _StageGraph,
    whole_dropped_models: set[str],
    dropped_cols: dict[str, set[str]],
    dropped_joins: _DroppedJoins,
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
    dropped_joins: _DroppedJoins,
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
        dropped_joins: _DroppedJoins,
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
    if col.sql is None or is_base_column_sql(col.sql):
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
        if JoinEdgeRef.of(join) in state.dropped_joins.get(model.name, []):
            continue
        # Raw dropped_cols: a dropped local PK key invalidates the join too.
        local_missing = [
            pair[0] for pair in join.join_pairs
            if pair[0] in state.dropped_cols.get(model.name, set())
        ]
        if local_missing:
            changed = _add_dropped_join(
                edit_entries=state.edit_entries,
                dropped_joins=state.dropped_joins,
                model=model,
                join=join,
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
            join=join,
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
    dropped_joins: _DroppedJoins,
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


def _removed_join_refs(*, model: SlayerModel | None, remove: RemoveSpec) -> list[JoinEdgeRef]:
    """Exact references of every join ``remove`` drops; a reference-only entry resolves on ``model``."""
    refs = list(remove.join_edges)
    for ref in remove.joins:
        joins = [] if model is None else (
            [j for j in model.joins if j.name == ref] or [j for j in model.joins if j.target_model == ref]
        )
        refs.extend(r for r in map(JoinEdgeRef.of, joins) if r not in refs)
    return refs


def _seed_one_diff_entry(
    *,
    model_name: str,
    model: SlayerModel | None,
    entry: ToDeleteEntry | None,
    cols: set[str],
    edit_entries: dict[str, EditModelDelete],
    whole_entries: dict[str, WholeModelDelete],
    dropped_cols: dict[str, set[str]],
    dropped_measures: dict[str, set[str]],
    dropped_joins: _DroppedJoins,
) -> None:
    """Apply one ``(entry, dropped_columns)`` diff to the cascade state dicts."""
    if isinstance(entry, WholeModelDelete):
        whole_entries[model_name] = entry
    elif isinstance(entry, EditModelDelete):
        edit_entries[model_name] = entry
        refs = _removed_join_refs(model=model, remove=entry.remove)
        if refs:
            dropped_joins.setdefault(model_name, []).extend(refs)
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
    models_by_name: dict[str, SlayerModel],
    edit_entries: dict[str, EditModelDelete],
    whole_entries: dict[str, WholeModelDelete],
    dropped_cols: dict[str, set[str]],
    dropped_measures: dict[str, set[str]],
    dropped_joins: _DroppedJoins,
) -> None:
    """Populate the cascade state dicts from the base per-model diffs."""
    for diffs in diffs_iterables:
        for model_name, (entry, cols) in diffs.items():
            _seed_one_diff_entry(
                model_name=model_name,
                model=models_by_name.get(model_name),
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
    dropped_joins: _DroppedJoins = {}
    models_by_name = {m.name: m for m in models}

    _seed_state_from_diffs(
        diffs_iterables=(sql_table_diffs, sql_diffs),
        models_by_name=models_by_name,
        edit_entries=edit_entries,
        whole_entries=whole_entries,
        dropped_cols=dropped_cols,
        dropped_measures=dropped_measures,
        dropped_joins=dropped_joins,
    )

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


def column_dependents(*, models: list[SlayerModel], model: SlayerModel, column: str) -> list[ToDeleteEntry]:
    """What deleting ``model.column`` would cascade to among ``models`` (one datasource); the column itself
    included."""
    seed = EditModelDelete(model_name=model.name, data_source=model.data_source, remove=RemoveSpec(columns=[column]))
    return compute_datasource_drops(models=models, sql_table_diffs={model.name: (seed, {column})}, sql_diffs={})


def reads_retyped_column(
    *, models: list[SlayerModel], finding: RetypeFinding, read_columns: Set[tuple[str, str]],
) -> bool:
    """Whether ``read_columns`` reads ``finding``'s column, directly or through a column depending on it."""
    owner = next((m for m in models if m.name == finding.model_name), None)
    if owner is None:
        return (finding.model_name, finding.column) in read_columns
    for entry in column_dependents(models=models, model=owner, column=finding.column):
        if isinstance(entry, WholeModelDelete):
            if any(m == entry.model_name for m, _ in read_columns):
                return True
        elif any((entry.model_name, c) in read_columns for c in entry.remove.columns):
            return True
    return False


# Live introspection


class IntrospectionUnavailable(Exception):
    """Every table failed to introspect — callers must not read this as "all dropped"."""


class DatasourceUnreachable(Exception):
    """A snapshot's datasource recently failed to connect; no verdict until it expires."""


async def _live_schema_refs(
    *,
    client: SqlClient,
    datasource: DatasourceConfig,
    schema: str | None,
    fallback_schema_tokens: set[str] | None = None,
) -> list[SchemaRef]:
    """Schemas to introspect: explicit ``schema`` scopes to one, else every own-catalog schema (catalog attached via ``resolve_ingest_scope``); on enumeration failure ``fallback_schema_tokens`` is scoped, None stays fail-closed."""
    try:
        refs = (await resolve_ingest_scope(
            client=client,
            requested=[schema] if schema is not None else None,
            all_schemas=schema is None,
            datasource_schema=datasource.schema_name,
        )).schemas
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
    resolved = (await resolve_ingest_scope(
        client=client,
        requested=sorted(requested) or None,
        all_schemas=False,
        datasource_schema=datasource.schema_name,
    )).schemas
    # Add the connection default (bare models), deduped by token, preferring the default-marked ref.
    default_ref = await default_schema_ref(client)
    candidates = list(resolved)
    default_token = default_ref.token or default_ref.name or ""
    if not is_system_schema(default_token, qualifies=qualifies_tokens(datasource)):
        candidates.append(default_ref)  # skip a default that is itself a system schema (C2)
    by_token: dict[str | None, SchemaRef] = {}
    for ref in candidates:
        prev = by_token.get(ref.token)
        if prev is None or (ref.is_default and not prev.is_default):
            by_token[ref.token] = ref
    return list(by_token.values())


_Listings = list[tuple[SchemaRef, list[str]]]
_Lookup = Callable[[SchemaRef, str], Awaitable["LiveTable"]]


async def _introspect_live_object(
    *,
    client: SqlClient,
    ref: SchemaRef,
    obj_name: str,
    datasource: DatasourceConfig,
) -> LiveTable:
    """One listed object's ``LiveTable``; unreadable (no verdict) when reading it failed, not-found included."""
    try:
        return await _introspect_one_table(client=client, table_name=obj_name, ref=ref)
    except Exception as exc:  # noqa: BLE001 — one object's introspection failed
        logger.warning(
            "validate_models: failed to introspect %r in datasource %r: %s",
            obj_name, datasource.name, exc,
        )
        return LiveTable(readable=False)


def _live_object_keys(*, ref: SchemaRef, obj_name: str, single: bool, dialect: SqlDialect) -> list[str]:
    """The live-map keys of one object; the default/single-schema scope is keyed both bare and qualified so legacy and explicit models resolve."""
    keys = [ref.qualify(obj_name, dialect=dialect)]
    if ref.is_default or single:
        keys.append(render_table_name(parts=[obj_name], dialect=dialect))
        if schema := ref.live_name:
            keys.append(render_table_name(parts=[schema, obj_name], dialect=dialect))
    return keys


def _key_live_object(
    out: dict[str, LiveTable], *, ref: SchemaRef, obj_name: str, single: bool, live: LiveTable, dialect: SqlDialect,
) -> None:
    first, *rest = _live_object_keys(ref=ref, obj_name=obj_name, single=single, dialect=dialect)
    out[first] = live
    for key in rest:
        out.setdefault(key, live)


async def _list_live_objects(
    refs: list[SchemaRef], *, client: SqlClient, datasource: DatasourceConfig,
) -> _Listings:
    """Every table and view name per schema in ``refs``; deletion verdicts rest on these listings alone."""
    listings: _Listings = []
    failed: list[str] = []
    for ref in refs:
        try:
            # Module-attr call keeps test patches on ``ingestion`` effective.
            objs = await ingestion.list_ingestable_objects(client=client, ref=ref, include_views=True)
        except UNREACHABLE:
            raise
        except Exception as exc:  # noqa: BLE001 — one schema's listing failed
            logger.warning(
                "validate_models: failed to list schema %r in datasource "
                "%r: %s", ref.token, datasource.name, exc,
            )
            failed.append(str(ref.token))
            continue
        listings.append((ref, [obj.live_name for obj in objs]))
    if failed:
        # Fail closed: a partial map would report every model in the unlisted
        # schema as a WholeModelDelete — data loss under --force-clean.
        raise IntrospectionUnavailable(
            f"validate_models: could not list schema(s) {failed} in datasource "
            f"{datasource.name!r}; skipping drift verdict to avoid false deletions"
        )
    return listings


async def _collect_live_tables(
    listings: _Listings,
    *,
    lookup: _Lookup,
    datasource: DatasourceConfig,
    wanted: set[str] | None = None,
) -> dict[str, LiveTable]:
    """``{key: LiveTable}`` over the listed objects, only those with a key in ``wanted`` when given."""
    single = len(listings) == 1
    dialect = dialect_for_ds_type(datasource.type)
    out: dict[str, LiveTable] = {}
    found = 0
    for ref, names in listings:
        for name in names:
            if wanted is not None and wanted.isdisjoint(
                _live_object_keys(ref=ref, obj_name=name, single=single, dialect=dialect)
            ):
                continue
            found += 1
            _key_live_object(out, ref=ref, obj_name=name, single=single, live=await lookup(ref, name),
                             dialect=dialect)
    # Only a whole-datasource pass can tell that every table failed.
    if wanted is None and found and not any(t.readable for t in out.values()):
        raise IntrospectionUnavailable(
            f"failed to introspect every table in datasource "
            f"{datasource.name!r} ({found} table(s))"
        )
    return out


async def _live_schema_for_datasource(
    *,
    client: SqlClient,
    datasource: DatasourceConfig,
    schema: str | None = None,
    fallback_schema_tokens: set[str] | None = None,
) -> dict[str, LiveTable]:
    """``{object_name: LiveTable}`` for every live table AND view in the DS; views are included so a model over a view isn't falsely reported as a WholeModelDelete."""
    refs = await _live_schema_refs(
        client=client, datasource=datasource, schema=schema, fallback_schema_tokens=fallback_schema_tokens,
    )
    listings = await _list_live_objects(refs, client=client, datasource=datasource)

    async def lookup(ref: SchemaRef, obj_name: str) -> LiveTable:
        return await _introspect_live_object(client=client, ref=ref, obj_name=obj_name, datasource=datasource)

    return await _collect_live_tables(listings, lookup=lookup, datasource=datasource)


_SNAPSHOT_TTL_S = 60.0
_SNAPSHOT_CAP = 256


class _Catalog(BaseModel):
    """Schema refs and the latest listing pass over them."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    refs: list[SchemaRef]
    # Enumeration may fall back to the models' own schemas, so refs cover only these.
    refs_for: frozenset[str]
    listings: _Listings | None = None
    # ``sql_table``s the listing was taken to resolve: only their absence is trusted.
    listed_for: set[str] = Field(default_factory=set)


class _SqlTrial(BaseModel):
    live_columns: dict[str, ResultColumnType] | None
    invalid_sql: bool

    @classmethod
    def of(cls, types: "ingestion.SqlResultTypes | None", *, invalid_sql: bool) -> "_SqlTrial":
        if types is None:
            return cls(live_columns=None, invalid_sql=invalid_sql)
        return cls(live_columns=types.result_column_types(), invalid_sql=invalid_sql)


class DriftSnapshot(BaseModel):
    """One datasource's reusable live-schema facts for query-time attribution."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    created_at: float
    unavailable: bool = False
    unreachable: str | None = None
    catalog: _Catalog | None = None
    tables: dict[tuple[SchemaRef, tuple[str, bool]], LiveTable] = Field(default_factory=dict)
    sql_trials: dict[str, _SqlTrial] = Field(default_factory=dict)
    lock: asyncio.Lock = Field(default_factory=asyncio.Lock)

    def raise_if_unreachable(self) -> None:
        if self.unreachable is not None:
            raise DatasourceUnreachable(self.unreachable)

    async def live_tables(
        self, *, client: SqlClient, datasource: DatasourceConfig, sql_tables: set[str],
        fallback_schema_tokens: set[str],
    ) -> dict[str, LiveTable]:
        """Keyed ``LiveTable``s of the objects ``sql_tables`` name, gathering only what is missing."""
        if self.unavailable:
            raise IntrospectionUnavailable(
                f"live schema of datasource {datasource.name!r} recently unavailable"
            )
        dialect = dialect_for_ds_type(datasource.type)
        wanted = {c for t in sql_tables for c in _sql_table_candidates(t, dialect=dialect)}

        async def lookup(ref: SchemaRef, obj_name: str) -> LiveTable:
            return await self._table(client=client, datasource=datasource, ref=ref, obj_name=obj_name)

        async def collect(cat: _Catalog) -> dict[str, LiveTable]:
            assert cat.listings is not None
            return await _collect_live_tables(cat.listings, datasource=datasource, wanted=wanted, lookup=lookup)

        try:
            cat = await self._catalog(
                client=client, datasource=datasource, fallback_schema_tokens=fallback_schema_tokens,
            )
            listed = cat.listings is None
            if listed:
                await self._list(cat=cat, client=client, datasource=datasource)
            live = await collect(cat)
            unresolved = {
                t for t in sql_tables if _resolve_live_table(sql_table=t, live_tables=live, dialect=dialect) is None
            }
            if not listed and not unresolved <= cat.listed_for:
                await self._list(cat=cat, client=client, datasource=datasource)
                listed = True
                live = await collect(cat)
            if listed:
                cat.listed_for |= sql_tables
            return live
        except IntrospectionUnavailable:
            self.unavailable = True
            raise
        except Exception as exc:
            self.unreachable = f"live schema of datasource {datasource.name!r} failed: {exc}"
            raise

    async def _catalog(
        self, *, client: SqlClient, datasource: DatasourceConfig, fallback_schema_tokens: set[str],
    ) -> _Catalog:
        cat = self.catalog
        if cat is None or not fallback_schema_tokens <= cat.refs_for:
            tokens = fallback_schema_tokens | (cat.refs_for if cat is not None else frozenset())
            refs = await _live_schema_refs(
                client=client, datasource=datasource, schema=None, fallback_schema_tokens=set(tokens),
            )
            if cat is None or refs != cat.refs:
                cat = self.catalog = _Catalog(refs=refs, refs_for=frozenset(tokens))
            else:
                cat.refs_for = frozenset(tokens)
        return cat

    @staticmethod
    async def _list(*, cat: _Catalog, client: SqlClient, datasource: DatasourceConfig) -> None:
        cat.listings = await _list_live_objects(cat.refs, client=client, datasource=datasource)

    async def _table(
        self, *, client: SqlClient, datasource: DatasourceConfig, ref: SchemaRef, obj_name: str,
    ) -> LiveTable:
        key = (ref, live_key(obj_name))
        live = self.tables.get(key)
        if live is None:
            live = self.tables[key] = await _introspect_live_object(
                client=client, ref=ref, obj_name=obj_name, datasource=datasource,
            )
        return live


class LiveSnapshotCache:
    """Per-datasource ``DriftSnapshot``s: expire 60 s after creation, LRU-capped, single-flight."""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._entries: OrderedDict[ClientCacheKey, DriftSnapshot] = OrderedDict()

    def __len__(self) -> int:
        return len(self._entries)

    def clear(self) -> None:
        self._entries.clear()

    def _expired(self, snapshot: DriftSnapshot) -> bool:
        return self._clock() - snapshot.created_at >= _SNAPSHOT_TTL_S

    def _entry(self, key: ClientCacheKey, *, stale: DriftSnapshot | None = None) -> DriftSnapshot:
        """``key``'s snapshot (a fresh one in place of ``stale``); held snapshots are never dropped, keeping single-flight."""
        idle = [k for k, s in self._entries.items() if k != key and not s.lock.locked()]
        for expired in [k for k in idle if self._expired(self._entries[k])]:
            del self._entries[expired]
        snapshot = self._entries.get(key)
        if snapshot is None or snapshot is stale or (
            self._expired(snapshot) and not snapshot.lock.locked()
        ):
            snapshot = self._entries[key] = DriftSnapshot(created_at=self._clock())
        self._entries.move_to_end(key)
        # Least recently used first.
        idle = [k for k in idle if k in self._entries]
        for evicted in idle[: max(0, len(self._entries) - _SNAPSHOT_CAP)]:
            del self._entries[evicted]
        return snapshot

    @asynccontextmanager
    async def acquire(self, datasource: DatasourceConfig) -> AsyncGenerator[DriftSnapshot, None]:
        """The datasource's snapshot, held exclusively for the block."""
        key = client_cache_key(datasource)
        snapshot = self._entry(key)
        while True:
            async with snapshot.lock:
                if not self._expired(snapshot):
                    yield snapshot
                    return
            # Expired while waiting: every waiter moves to one fresh snapshot.
            snapshot = self._entry(key, stale=snapshot)


async def _introspect_one_table(*, client: SqlClient, table_name: str, ref: SchemaRef | None) -> LiveTable:
    """One table's live columns via the reflection door; ``ref`` keeps the read catalog-scoped."""
    cols = await reflect_columns(client=client, table_name=table_name, ref=ref)
    return LiveTable(
        columns={c.name: c.type for c in cols},
        db_types={c.name: c.type_name for c in cols if c.type_name},
    )


async def _live_columns_for_sql_model(
    *,
    model: SlayerModel,
    client: SqlClient,
) -> ingestion.SqlResultTypes | None:
    """Trial-execute ``model.sql`` with a 0-row guard; return its result types, or None on failure."""
    if not model.sql:
        return None
    try:
        return await ingestion.sql_result_types(sql=model.sql, client=client)
    except Exception as exc:
        logger.info(
            "validate_models: trial-execute on %r failed: %s",
            model.name,
            exc,
        )
        return None


def _has_star_projection(parsed: exp.Expression) -> bool:
    """Whether any SELECT in the statement projects ``*`` or ``t.*``."""
    for select in parsed.find_all(exp.Select):
        for proj in select.expressions:
            if isinstance(proj, exp.Star):
                return True
            if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
                return True
    return False


SourceRefs = dict[exp.Table, set[exp.Column]]


def _scope_table_sources(*, scope: Scope) -> dict[str, exp.Table]:
    """Alias/name → bare table for one scope's physical tables (CTE/derived excluded)."""
    out: dict[str, exp.Table] = {}
    for name, source in scope.sources.items():
        if isinstance(source, exp.Table) and source.name:
            bare = source.copy()
            bare.set("alias", None)
            out[name] = bare
    return out


def _add_using_join_columns(
    *,
    scope: Scope,
    local_tables: dict[str, exp.Table],
    refs: SourceRefs,
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
            table
            for op in operands
            if isinstance(op, exp.Table)
            and (table := local_tables.get(op.alias_or_name)) is not None
        ]
        if len(physical) < len(operands):
            cannot_verify = True
        keys = [exp.Column(this=ident.copy()) for ident in using if isinstance(ident, exp.Identifier)]
        for table in physical:
            refs[table].update(keys)
    return cannot_verify


def _attribute_qualified_ref(
    *,
    qualifier: str,
    column: exp.Column,
    scope: Scope,
    tables_by_scope: dict[int, dict[str, exp.Table]],
    refs: SourceRefs,
) -> bool:
    """Attribute ``qual.col`` in the nearest enclosing scope defining the qualifier; True ⇒ derived/CTE/unknown qualifier (not attributable)."""
    s = scope
    while s is not None:
        target = (tables_by_scope.get(id(s)) or {}).get(qualifier)
        if target is not None:
            refs[target].add(column)
            return False
        if qualifier in s.sources:  # CTE / derived-table alias
            return True
        s = s.parent
    return True


def _attribute_bare_ref(
    *,
    column: exp.Column,
    scope: Scope,
    tables_by_scope: dict[int, dict[str, exp.Table]],
    refs: SourceRefs,
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
    refs[next(iter(local.values()))].add(column)
    return False


def _attribute_scope_columns(
    *,
    scope: Scope,
    tables_by_scope: dict[int, dict[str, exp.Table]],
    refs: SourceRefs,
    seen: set[int],
) -> bool | None:
    """Attribute every yet-unseen column ref of one scope; ``seen`` (node ids) processes each ref once at its innermost scope, None when a bare column can't be attributed."""
    derived = False
    for col in scope.columns:
        if id(col) in seen or not isinstance(col.this, exp.Identifier):
            continue
        seen.add(id(col))
        column = exp.Column(this=col.this.copy())
        if col.table:
            verdict = _attribute_qualified_ref(
                qualifier=col.table, column=column, scope=scope,
                tables_by_scope=tables_by_scope, refs=refs,
            )
        else:
            verdict = _attribute_bare_ref(
                column=column, scope=scope,
                tables_by_scope=tables_by_scope, refs=refs,
            )
        if verdict is None:
            return None
        derived = derived or verdict
    return derived


def _sql_model_source_refs(*, sql: str, dialect: str) -> SourceRefs | None:
    """Map each source table of a sql-mode model to the columns it references (incl. JOIN USING keys); None when unparseable, a bare column isn't attributable, or a CTE/derived ref coexists with ``*``."""
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
        scopes = traverse_scope(parsed)
    except Exception:
        return None
    if not scopes:
        return None
    refs: SourceRefs = {}
    tables_by_scope: dict[int, dict[str, exp.Table]] = {}
    has_derived_ref = False
    for scope in scopes:
        local = _scope_table_sources(scope=scope)
        tables_by_scope[id(scope)] = local
        for table in local.values():
            refs.setdefault(table, set())
        if _add_using_join_columns(scope=scope, local_tables=local, refs=refs):
            has_derived_ref = True
    seen: set[int] = set()
    for scope in scopes:  # innermost first — a bare column binds innermost
        derived = _attribute_scope_columns(
            scope=scope, tables_by_scope=tables_by_scope, refs=refs, seen=seen,
        )
        if derived is None:
            return None
        has_derived_ref = has_derived_ref or derived
    if has_derived_ref and _has_star_projection(parsed):
        # A ``*`` may hide the physical column behind the derived ref.
        return None
    return refs


async def _source_tables_resolve(
    *, model: SlayerModel, client: SqlClient
) -> bool:
    """Whether every table AND column in ``model.sql`` accepts a 0-row probe; True ⇒ the SQL itself is broken (not drift), any probe failure keeps the drift classification."""
    dialect = dialect_for_ds_type(client.datasource.type)
    refs = _sql_model_source_refs(sql=model.sql or "", dialect=dialect.sqlglot_name)
    if refs is None:
        return False
    for table, columns in refs.items():
        projection = sorted(columns, key=lambda c: c.sql(dialect=dialect.sqlglot_name)) or [exp.Star()]
        probe = exp.select(*(c.copy() for c in projection)).from_(
            exp.alias_(table.copy(), "_sd_probe", table=True),
        ).where(exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(0)))
        try:
            await client.get_result_column_types(render_type_probe(probe, dialect=dialect))
        except Exception:
            return False
    return True


# Datasource-level orchestrator


def _sql_table_candidates(sql_table: str, *, dialect: SqlDialect) -> list[str]:
    """Live-map keys a ``sql_table`` may resolve through (as stored, rendered, its last two parts); a qualified name is NEVER stripped to bare, which would mask a dropped non-default twin."""
    parts = sql_table_parts(sql_table, dialect=dialect)
    return [sql_table, *(render_table_name(parts=parts[i:], dialect=dialect) for i in range(max(len(parts) - 2, 0) + 1))]


def _resolve_live_table(
    *, sql_table: str, live_tables: dict[str, LiveTable], dialect: SqlDialect,
) -> LiveTable | None:
    """Look up a model's ``sql_table`` in the live map."""
    for name in _sql_table_candidates(sql_table, dialect=dialect):
        live = live_tables.get(name)
        if live is not None:
            return live
    return None


async def _probe_validate_models_column(
    *, client: SqlClient, model: SlayerModel, col: Column, physical: str, table_name: str,
    schema_name: str | None,
) -> DataType | None:
    """Run the affinity probe for one column in a validate_models pass."""
    try:
        # Module-attr call keeps test patches on ``sqlite_introspect`` effective.
        return await sqlite_introspect.probe_sqlite_integer_column(
            client=client,
            table=table_name,
            column=physical,
            schema=schema_name,
        )
    except UNREACHABLE:
        raise
    except Exception as exc:
        logger.warning(
            "validate_models probe raised for %s.%s; ignoring: %s",
            model.name, col.name, exc,
        )
        return None


async def _sqlite_probe_int_drift_for_model(
    *,
    model: SlayerModel,
    client: SqlClient,
    live_tables: dict[str, LiveTable],
    default_schema: str | None = None,
    dialect: SqlDialect,
) -> list[RetypeFinding]:
    """A re-type finding for each INT base column, live INT too, SQLite's storage probe widens to DOUBLE/TEXT;
    ``default_schema`` for unqualified tables, failures skip."""
    if model.sql_table is None:
        return []
    live = _resolve_live_table(sql_table=model.sql_table, live_tables=live_tables, dialect=dialect)
    if live is None or not live.readable:
        return []

    schema_name, table_name = split_sql_table(model.sql_table, dialect=dialect)
    if schema_name is None:
        schema_name = default_schema or None

    drifts: list[RetypeFinding] = []
    for col in model.columns:
        physical = physical_column_sql(sql=col.sql, name=col.name)
        if col.type is not DataType.INT or not _column_is_base(col.sql) or live.columns.get(physical) is not DataType.INT:
            continue
        verdict = await _probe_validate_models_column(
            client=client, model=model, col=col, physical=physical,
            table_name=table_name, schema_name=schema_name,
        )
        if verdict is None or verdict is DataType.INT:
            continue
        drifts.append(RetypeFinding.of(model=model, column=col, live_type=verdict, live_db_type=None,
                                       remedy_kind="reingest"))
    return drifts


async def _sqlite_probe_drifts_for_models(
    *,
    datasource: DatasourceConfig,
    client: SqlClient,
    sql_table_models: list[SlayerModel],
    live_tables: dict[str, LiveTable],
) -> list[RetypeFinding]:
    """Run the SQLite probe for every model through the datasource's client."""
    if not dialect_for_ds_type(datasource.type).probes_integer_affinity:
        return []
    default_schema = datasource.schema_name or None
    dialect = dialect_for_ds_type(datasource.type)
    return [
        f for m in sql_table_models
        for f in await _sqlite_probe_int_drift_for_model(
            model=m, client=client, live_tables=live_tables, default_schema=default_schema, dialect=dialect)
    ]


def _diff_one_sql_table_model(
    *,
    model: SlayerModel,
    live_tables: dict[str, "LiveTable"],
    available_in_ds: set[str],
    dialect: SqlDialect,
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Per-model body of ``_collect_sql_table_diffs``: resolve the live table and diff."""
    live = _resolve_live_table(
        sql_table=model.sql_table or "", live_tables=live_tables, dialect=dialect,
    )
    if live is not None and not live.readable:
        return None, set()
    return diff_sql_table_model(
        model=model,
        live_table=live,
        available_models_in_ds=available_in_ds,
    )


async def _collect_sql_table_diffs(
    *,
    datasource: DatasourceConfig,
    client: SqlClient,
    sql_table_models: list[SlayerModel],
    available_in_ds: set[str],
    snapshot: DriftSnapshot | None = None,
    findings: list[RetypeFinding] | None = None,
) -> dict[str, tuple[ToDeleteEntry | None, set[str]]]:
    """Introspect live and diff each sql_table-mode model; re-type findings (SQLite's affinity probe's
    included) append to ``findings``."""
    if not sql_table_models:
        return {}
    # Cover every own-catalog schema (a model may be qualified to another own
    # schema); fall back to the models' own schemas if enumeration is unavailable.
    fallback_tokens = {
        tok for m in sql_table_models
        if (tok := split_sql_table(m.sql_table or "", dialect=dialect_for_ds_type(datasource.type))[0]) is not None
    }
    try:
        if snapshot is None:
            live_tables = await _live_schema_for_datasource(
                client=client,
                datasource=datasource,
                schema=None,
                fallback_schema_tokens=fallback_tokens,
            )
        else:
            live_tables = await snapshot.live_tables(
                client=client,
                datasource=datasource,
                sql_tables={m.sql_table for m in sql_table_models if m.sql_table},
                fallback_schema_tokens=fallback_tokens,
            )
    except IntrospectionUnavailable as exc:
        # Unknown live schema: don't report deletions (a transient error would
        # hand --force-clean a whole tenant).
        logger.warning("validate_models: skipping drift verdict: %s", exc)
        return {}
    dialect = dialect_for_ds_type(datasource.type)
    temporal_text = dialect.temporal_text_compatible
    if findings is not None:
        for m in sql_table_models:
            live = _resolve_live_table(sql_table=m.sql_table or "", live_tables=live_tables, dialect=dialect)
            findings.extend(retype_findings(
                model=m, live_table=live if live is not None and live.readable else None, temporal_text=temporal_text,
            ))
        findings.extend(await _sqlite_probe_drifts_for_models(
            datasource=datasource, client=client, sql_table_models=sql_table_models, live_tables=live_tables,
        ))
    return {
        m.name: _diff_one_sql_table_model(model=m, live_tables=live_tables, available_in_ds=available_in_ds,
                                          dialect=dialect)
        for m in sql_table_models
    }


async def _sql_trials(
    *,
    datasource: DatasourceConfig,
    client: SqlClient,
    sql_models: list[SlayerModel],
    snapshot: DriftSnapshot | None,
) -> dict[str, _SqlTrial]:
    """Trial results keyed by SQL text: one trial per distinct text not already in ``snapshot``."""

    async def _invalid_sql(model: SlayerModel, live_cols: ingestion.SqlResultTypes | None) -> bool:
        return live_cols is None and await _source_tables_resolve(model=model, client=client)

    trials: dict[str, _SqlTrial] = dict(snapshot.sql_trials) if snapshot is not None else {}
    pending = {m.sql or "": m for m in sql_models if (m.sql or "") not in trials}
    live = await asyncio.gather(
        *(_live_columns_for_sql_model(model=m, client=client) for m in pending.values())
    )
    if any(cols is None for cols in live):
        # A failed trial is drift evidence only while the datasource is reachable.
        try:
            await client.ping()
        except Exception as exc:
            if snapshot is not None:
                snapshot.unreachable = f"datasource {datasource.name!r} unreachable: {exc}"
            raise
    flags = await asyncio.gather(*map(_invalid_sql, pending.values(), live))
    for sql_text, live_cols, invalid_sql in zip(pending, live, flags):
        trials[sql_text] = _SqlTrial.of(live_cols, invalid_sql=invalid_sql)
        if snapshot is not None:
            snapshot.sql_trials[sql_text] = trials[sql_text]
    return trials


async def _collect_sql_diffs(
    *,
    datasource: DatasourceConfig,
    sql_models: list[SlayerModel],
    client: SqlClient,
    snapshot: DriftSnapshot | None = None,
    findings: list[RetypeFinding] | None = None,
) -> dict[str, tuple[ToDeleteEntry | None, set[str]]]:
    """Trial-execute each sql-mode model concurrently and produce its diff; re-type findings append to
    ``findings``."""
    out: dict[str, tuple[ToDeleteEntry | None, set[str]]] = {}
    if not sql_models:
        return out
    trials = await _sql_trials(
        datasource=datasource, client=client, sql_models=sql_models, snapshot=snapshot,
    )
    # A zero-row trial carries column types only where the cursor reports them.
    compare_types = dialect_for_ds_type(datasource.type).cursor_reports_types
    for model in sql_models:
        trial = trials[model.sql or ""]
        out[model.name] = diff_sql_model(
            model=model, live_columns=trial.live_columns, invalid_sql=trial.invalid_sql,
        )
        if findings is not None and compare_types:
            findings.extend(sql_model_retype_findings(
                model=model, live_columns=trial.live_columns,
                temporal_text=dialect_for_ds_type(datasource.type).temporal_text_compatible,
            ))
    return out


_UNSET: Any = object()


@marked_coroutine_function
def validate_datasource(
    *,
    datasource: DatasourceConfig,
    models: list[SlayerModel],
    client: SqlClient | None = None,
    available_in_ds: set[str] | None = None,
    snapshot: DriftSnapshot | None = None,
    findings_out: list[RetypeFinding] | None = None,
    sql_clients: object = _UNSET,
) -> Coroutine[Any, Any, list[ToDeleteEntry]]:
    """Validate ``models`` (all one DS) against the live schema through ``client``. Read-only.

    ``available_in_ds`` names every model of the DS (default: ``models``); a
    ``snapshot`` scopes introspection to ``models``' objects and reuses its facts;
    ``findings_out`` collects the report-only re-type findings.
    Omitting ``client`` or passing ``sql_clients`` (ignored) is deprecated.
    """
    if client is None or sql_clients is not _UNSET:
        warn_deprecated(
            "validate_datasource() without client= (or with sql_clients=, now ignored) is deprecated; pass "
            "client=engine.client_for(datasource). The old form works until SLayer 2.0.",
            removal="2.0",
        )
    validate = functools.partial(
        _validate_datasource, datasource=datasource, models=models, available_in_ds=available_in_ds,
        snapshot=snapshot, findings_out=findings_out,
    )
    if client is not None:
        return validate(client=adapt_client(client))
    return _validate_short_lived(datasource=datasource, validate=validate)


async def _validate_short_lived(
    *, datasource: DatasourceConfig, validate: Callable[..., Awaitable[list[ToDeleteEntry]]],
) -> list[ToDeleteEntry]:
    async with query_engine.short_lived_engine() as engine:
        return await validate(client=engine.client_for(datasource))


async def _validate_datasource(
    *,
    datasource: DatasourceConfig,
    models: list[SlayerModel],
    client: SqlClient,
    available_in_ds: set[str] | None,
    snapshot: DriftSnapshot | None,
    findings_out: list[RetypeFinding] | None,
) -> list[ToDeleteEntry]:
    if not models:
        return []
    if snapshot is not None:
        snapshot.raise_if_unreachable()

    if available_in_ds is None:
        available_in_ds = {m.name for m in models}
    sql_table_diffs = await _collect_sql_table_diffs(
        datasource=datasource,
        client=client,
        sql_table_models=[m for m in models if m.sql_table],
        available_in_ds=available_in_ds,
        snapshot=snapshot,
        findings=findings_out,
    )
    sql_diffs = await _collect_sql_diffs(
        datasource=datasource,
        sql_models=[m for m in models if m.sql],
        client=client,
        snapshot=snapshot,
        findings=findings_out,
    )
    return list(compute_datasource_drops(
        models=models,
        sql_table_diffs=sql_table_diffs,
        sql_diffs=sql_diffs,
    ))
