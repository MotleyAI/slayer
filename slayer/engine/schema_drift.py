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
    List,
    Literal,
    Optional,
    Set,
    Union,
)

import sqlalchemy as sa
import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp

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
from slayer.engine.ingestion import (
    _safe_get_pk_constraint,
    _sa_type_is_float,
    _sa_type_to_data_type,
)
from slayer.engine.column_expansion import resolve_ref_target
from slayer.engine.normalization import func_style_agg_to_colon
from slayer.engine.syntax import (
    AggCall,
    DottedRef,
    Ref,
    StarSource,
    parse_expr,
    walk_parsed_refs,
)
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.engine_factory import EngineCacheKey

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
    # "invalid_sql": the model's own SQL fails while its source tables and
    # referenced columns exist.
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
    # DEV-1538: persisted INT columns whose type widened (to DOUBLE or TEXT)
    # because the SQLite affinity probe disagreed with the declared type.
    widened_columns: list[str] = Field(default_factory=list)
    # Output-only, so the renderer can label a view-backed model without
    # reloading it; the durable record is ``SlayerModel.source_kind``.
    source_kind: str | None = None
    # Human-readable transition (e.g. "view → table") when a re-ingest found
    # the live object changed kind; None when nothing changed.
    kind_change: str | None = None
    # DEV-1809: columns whose empty description was filled from a DB comment
    # (on created models: all columns that arrived with a description).
    described_columns: list[str] = Field(default_factory=list)
    model_described: bool = False
    # DEV-1758: a self-heal that added a missing schema qualifier, rendered as
    # "reports → openfda_rest.reports"; None when the qualifier was untouched.
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
    # Live objects that could not be modelled at all — separate from ``errors``
    # because cause and fix differ ("can't represent this name" vs "couldn't
    # persist this model").
    skipped: list[Any] = Field(default_factory=list)
    # Every live object discovered this pass, modelled or not — lets the CLI
    # tell an empty schema (worth a hint) from a no-op re-ingest (worth
    # silence). ``Any`` avoids a circular import; entries are ``IngestableObject``.
    objects: list[Any] = Field(default_factory=list)
    # Recognised ELT/migration bookkeeping modelled ``hidden``. Effective
    # post-merge state, not the scan's verdict (the merge preserves a persisted
    # ``hidden``). ``Any`` avoids a circular import; entries are ``InternalTable``.
    hidden_internals: list[Any] = Field(default_factory=list)
    # DEV-1809: True when the datasource description was filled from the
    # BigQuery dataset description during this pass.
    datasource_described: bool = False
    # DEV-1758: a message naming schemas discovered but not ingested this pass,
    # or None (all-schemas / multi-schema runs, or nothing new to offer).
    schema_hint: str | None = None
    # DEV-1758: requested schemas dropped from scope with a reason (foreign
    # attached catalog or system schema), so an explicit request for one is
    # reported rather than silently empty. ``Any`` avoids a circular import;
    # entries are ``SkippedSchema``.
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


# ===========================================================================
# Internal live-schema input shapes
# ===========================================================================


class LiveTable(BaseModel):
    """One live table's columns/PK/FKs in SLayer's coarse type buckets.

    Internal — built by the SQLAlchemy introspection layer and consumed by
    the diff functions.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    columns: dict[str, DataType] = Field(default_factory=dict)
    pk_columns: set[str] = Field(default_factory=set)
    # Each entry: (local_column, ref_table, ref_column)
    fk_relationships: list[tuple[str, str, str]] = Field(default_factory=list)


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


def _type_buckets_conflict(*, persisted: DataType, live: DataType) -> bool:
    """True when persisted vs live types are genuinely incompatible.

    Opacity is deliberately **one-way**:

    - *live* opaque + operable persisted type → **conflict**. The physical
      column has no equality operator, so the persisted model is promising a
      ``GROUP BY`` / ``DISTINCT`` / aggregation the database will refuse. That
      is a real runtime hazard and must surface rather than be hidden.
    - *persisted* opaque + known live type → no conflict. ``UNKNOWN`` makes no
      type claim to contradict; it only says "we could not classify this", and
      the live type is strictly more information.
    - both opaque → no conflict (they agree).
    - both known → ordinary bucket comparison.

    Note this means models ingested before opaque classification existed (an
    exotic column coarsed to TEXT back then, read as UNKNOWN now) will be
    reported. That is intentional: those columns really are unusable as
    declared. The remedy is to re-ingest / retype the column to ``UNKNOWN``,
    after which the conflict disappears.
    """
    if live.is_opaque:
        return not persisted.is_opaque
    if persisted.is_opaque:
        return False
    return data_type_bucket(persisted) != data_type_bucket(live)


def _is_bare_identifier(s: str | None) -> bool:
    """``s`` is a bare SQL identifier per the canonical ``IDENTIFIER_RE``."""
    if not s:
        return False
    return IDENTIFIER_RE.match(s.strip()) is not None


def _column_is_base(col_sql: str | None) -> bool:
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
) -> tuple[list[str], list[DeleteReason]]:
    """Per-column diff of a sql_table-mode model against live columns."""
    dropped: list[str] = []
    reasons: list[DeleteReason] = []
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
    """Per-join diff of a sql_table-mode model against live FK columns and
    in-datasource model availability."""
    dropped: list[str] = []
    reasons: list[DeleteReason] = []
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
    live_table: LiveTable | None,
    available_models_in_ds: set[str],
) -> tuple[ToDeleteEntry | None, set[str]]:
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
    live_columns: dict[str, DataType] | None,
    invalid_sql: bool = False,
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Diff a sql-mode model against trial-execute cursor metadata.

    ``live_columns is None`` ⇒ trial-execute failed ⇒ ``WholeModelDelete``.
    ``invalid_sql=True`` marks that failure as the model's own SQL being
    broken (its source tables and columns still exist) rather than drift.
    """
    if live_columns is None:
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

    dropped_cols: list[str] = []
    reasons: list[DeleteReason] = []
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


def _extract_column_refs_from_sql(sql: str) -> list[tuple[str | None, str]]:
    """Return all ``(qualifier, column_name)`` refs in a SQL expression.

    ``qualifier`` is ``None`` for bare identifiers, else the full dotted
    qualifier string (DEV-1743: dots are the canonical join-path delimiter, so
    a multi-hop ``customers.regions.name`` yields ``("customers.regions",
    "name")``; a ``__``-named DIRECT target ``customer__region.label`` yields
    ``("customer__region", "label")`` — the ``__`` is part of the name, never
    split). The qualifier is resolved downstream by :func:`resolve_ref_target`.
    """
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
    """Textual name of a reference-bearing parse node.

    ``AggCall`` collapses to its aggregated source name — the agg itself is
    not a column reference, and ``*:count`` (``StarSource`` source) yields
    ``None`` because ``*`` is not a real column. Args / kwargs of the
    aggregation are opaque (legacy parity). Bare ``Ref`` / ``DottedRef``
    surface as their dotted textual form.
    """
    if isinstance(node, AggCall):
        source = node.source
        if isinstance(source, StarSource):
            return None
        node = source
    if isinstance(node, Ref):
        return node.name
    return ".".join(node.parts)


def _measure_formula_refs(
    formula: str, *, custom_agg_names: Optional[Set[str]] = None,
) -> Set[str]:
    """Best-effort: parse ``formula`` (Mode-B DSL) and return the set of
    column / measure names it references (dotted for cross-model refs, e.g.
    ``"customers.revenue"``). Returns the empty set on any parse failure.

    Textual extraction only — no scope binding. The cascade attribution
    checks each returned name against the dropped-column / dropped-measure
    sets itself, so bare named-measure refs surface by name (``aov``) rather
    than being inline-expanded; the cascade reaches the underlying column
    through the dropped-measure set in a later fixed-point pass.

    Function-style aggregations on legacy / un-normalized persisted formulas
    (``sum(amount)``) are rewritten to colon syntax first via the quiet
    ``FUNC_STYLE_AGG`` slack helper — matching the legacy ``parse_formula``
    path. ``custom_agg_names`` lets model-level custom aggregations
    (``weighted_avg(amount, weight=qty)``) rewrite too (CR); without them
    the call parses as an unknown function and the refs are lost, leaving
    the drift cascade incomplete.
    """
    try:
        parsed = parse_expr(
            func_style_agg_to_colon(
                formula,
                custom_agg_names=(
                    frozenset(custom_agg_names) if custom_agg_names else None
                ),
            )
        )
    except Exception:
        return set()
    out: Set[str] = set()
    for node in walk_parsed_refs(parsed):
        name = _parsed_ref_name(node)
        if name is not None:
            out.add(name)
    return out


def _filter_refs(filter_str: str) -> list[str]:
    """Best-effort: return list of column references in a SQL-mode filter.

    Used to scan ``Column.filter`` / ``SlayerModel.filters`` strings (Mode A
    SQL — DEV-1369). Returns ``[]`` on parse failure.
    """
    try:
        pf = parse_sql_predicate(filter_str)
    except Exception:
        return []
    return list(pf.columns)


def _filter_refs_dsl(filter_str: str) -> list[str]:
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
    models_by_name: dict[str, SlayerModel],
) -> SlayerModel | None:
    """Resolve a Mode-A qualifier to its terminal joined model (DEV-1743).

    ``table_alias`` is exact-matched first (a directly-joined model MAY contain
    ``__``), then walked as a dotted chain of exact hops — never ``__``-split.
    Returns ``None`` if any hop fails.
    """
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
    """Resolve a dotted measure/column ref like ``customers.region`` or
    ``customers.regions.name`` to ``(target_model, leaf_name)``.

    Same-model bare refs (no dot) return ``(source_model, ref)``. The dotted
    prefix is passed through verbatim (DEV-1743: dots are the canonical join
    delimiter — no ``__`` conversion).
    """
    if "." not in dotted_ref:
        return source_model, dotted_ref
    prefix, leaf = dotted_ref.rsplit(".", 1)
    target = _walk_alias_to_target_model(
        source_model=source_model,
        table_alias=prefix,
        models_by_name=models_by_name,
    )
    return target, leaf


# ===========================================================================
# Cascade helpers
# ===========================================================================


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


# ===========================================================================
# Query-backed cascade
# ===========================================================================


def _resolve_stage_source_to_base(
    *,
    source_model: object,
    prior_stages_by_name: dict[str, SlayerQuery],
) -> str | None:
    """Walk a ``source_model`` reference (str / SlayerModel / ModelExtension /
    prior-stage-name) back to a real persisted base model name.

    ``ModelExtension`` carries a ``source_name: str`` field that names the
    underlying model — we follow it transparently so query-backed drift
    attribution doesn't silently skip extension-wrapped stages.
    """
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
    """Resolved join-graph context for a query-backed stage.

    Carries the stage's resolved source name (``stage_source_name``), the
    extension-added join targets (``extension_targets``), and the set of
    every model name reachable from the source via the in-DS join graph.
    Used to attribute multi-hop dotted refs and to bound dropped-join
    checks to models the stage can actually reach.
    """

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
    """Build a ``_StageGraph`` for a single stage. ``stage_source_name`` is
    the resolved base model name (str), or ``None`` for inline / unresolved
    sources.
    """
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
    # Custom aggregations on models REACHABLE from the stage source so
    # function-style custom aggs in a stage measure rewrite to colon form
    # before ref extraction. Scoped to ``graph.reachable`` (not every model
    # in the registry) so unrelated custom-agg names can't normalize a
    # coincidental function call and produce a false cascade hit (CR).
    custom_agg_names: Set[str] = set()
    for model_name in graph.reachable:
        model = graph.models_by_name.get(model_name)
        if model is not None:
            custom_agg_names.update(a.name for a in (model.aggregations or []))
    for m in stage.measures or []:
        formula = getattr(m, "formula", None)
        if not formula:
            continue
        for ref in _measure_formula_refs(
            formula, custom_agg_names=custom_agg_names,
        ):
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
    graph: _StageGraph | None = None,
) -> set[str]:
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


def _stage_join_targets(stage: SlayerQuery) -> set[str]:
    """Return the set of join target_model names referenced by a stage.

    ``SlayerQuery`` itself has no ``joins`` field; joins on a stage live
    on its ``source_model`` when that's a ``ModelExtension``. Read off
    ``stage.source_model.joins`` via ``getattr`` with defaults so plain
    stages (str source_model, SlayerModel source_model) return the empty
    set without raising.
    """
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
    dropped_joins: dict[str, set[str]],
) -> str | None:
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
    whole_dropped_models: set[str],
    dropped_cols: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
    pk_per_model: dict[str, set[str]],
    candidate_base_names: set[str],
) -> DeleteReason | None:
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
    dropped_cols: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
    whole_dropped_models: set[str],
    pk_per_model: dict[str, set[str]],
    candidate_base_names: set[str] | None = None,
    models_by_name: dict[str, SlayerModel] | None = None,
) -> DeleteReason | None:
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
) -> tuple[SlayerModel, str] | None:
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
) -> str | None:
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
    refs: set[str],
    model: SlayerModel,
    state: _CascadeState,
) -> str | None:
    """Return the cause string for the first ref that resolves to a dropped
    column or measure, or ``None`` when nothing in ``refs`` is dropped.
    """
    for ref in refs:
        cause = _measure_drop_cause(ref=ref, model=model, state=state)
        if cause is not None:
            return cause
    return None


def _reachable_agg_names_from_state(
    *, start: SlayerModel, state: "_CascadeState",
) -> Set[str]:
    """Sync BFS over ``state.models_by_name`` collecting custom aggregation
    names reachable from ``start`` via the join graph. DEV-1500 — lets the
    measure-cascade rule recognise function-style references to custom
    aggregations defined on joined models (``rolling_avg(customers.score)``
    where ``rolling_avg`` lives on the joined ``customers``). Visited-guarded,
    unbounded depth; absent targets are skipped (best-effort).
    """
    names: Set[str] = set()
    visited: Set[str] = set()
    queue: List[SlayerModel] = [start]
    while queue:
        current = queue.pop(0)
        if current.name in visited:
            continue
        visited.add(current.name)
        if current.aggregations:
            names.update(a.name for a in current.aggregations)
        for join in current.joins:
            if join.target_model in visited:
                continue
            nxt = state.models_by_name.get(join.target_model)
            if nxt is not None:
                queue.append(nxt)
    return names


def _cascade_measures(*, model: SlayerModel, state: _CascadeState) -> bool:
    """Rule 2: ``ModelMeasure.formula`` referencing a dropped column or
    dropped measure."""
    changed = False
    dropped_set = state.dropped_measures.get(model.name, set())
    custom_agg_names = _reachable_agg_names_from_state(start=model, state=state)
    for measure in model.measures:
        if measure.name is None or measure.name in dropped_set:
            continue
        refs = _measure_formula_refs(
            measure.formula, custom_agg_names=custom_agg_names,
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
    *, models: list[SlayerModel], state: _CascadeState
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
    models: list[SlayerModel],
    models_by_name: dict[str, SlayerModel],
    edit_entries: dict[str, EditModelDelete],
    whole_entries: dict[str, WholeModelDelete],
    dropped_cols: dict[str, set[str]],
    dropped_measures: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
    pk_per_model: dict[str, set[str]],
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
    entry: ToDeleteEntry | None,
    cols: set[str],
    edit_entries: dict[str, EditModelDelete],
    whole_entries: dict[str, WholeModelDelete],
    dropped_cols: dict[str, set[str]],
    dropped_measures: dict[str, set[str]],
    dropped_joins: dict[str, set[str]],
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
    """Apply the collapse rule (whole-drop preempts edit on the same model)
    and return the final, name-sorted list of delete entries.
    """
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
    """Combine per-model base diffs with cascade walking and the collapse rule.

    Pure: takes pre-computed diffs as input and returns the final flat
    list. Caller is responsible for restricting ``models`` to a single
    datasource — cascade walking does not cross datasource boundaries.
    """
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


# ===========================================================================
# Live introspection
# ===========================================================================


class IntrospectionUnavailable(Exception):
    """Every table in the datasource failed to introspect, so the live schema
    is unknown — callers must not read that as "everything was dropped"."""


def _live_schema_refs(
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    datasource: DatasourceConfig,
    schema: str | None,
    fallback_schema_tokens: set[str] | None = None,
) -> list[SchemaRef]:
    """The schemas to introspect for a validate-models live map: an explicit
    ``schema`` scopes to just that one; otherwise every own-catalog schema.

    Both go through ``resolve_ingest_scope`` so the connection's current catalog
    is attached to the ``SchemaRef`` — a bare ``main`` token would re-arm
    DuckDB's cross-catalog sweep in the validate path.

    ``fallback_schema_tokens`` (validate): the schemas the persisted models
    reference. When the all-schemas listing fails or is empty (least-privilege
    connection, or a driver returning ``[]``), scope to exactly those schemas
    (plus the connection default, for bare models) instead of skipping — so
    validation is never unconditionally coupled to enumeration, yet a model in a
    non-default own schema is still diffed against the right table. Callers that
    pass None (type refinement) stay fail-closed via ``IntrospectionUnavailable``.
    """
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
    # Enumeration failed/empty: introspect exactly the schemas the models use
    # (+ the connection default for bare models). The requested / default
    # branches of resolve_ingest_scope don't need get_schema_names() to succeed.
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
    # Add the connection default (for bare models), deduped by token but
    # preferring the default-marked ref — so a model token that resolves to the
    # catalog-qualified default doesn't leave it non-default and un-bare-keyed.
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
    """Introspect one object and key it into ``out`` (best-effort).

    The default schema (and a single-schema scope) is keyed BOTH bare and
    qualified so a legacy unqualified model and an explicit ``main.orders``
    model both resolve via a full match — since ``_resolve_live_table`` no
    longer strips a qualifier down to bare (that would mask a dropped
    non-default twin).
    """
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
    """Introspect every object across ``refs`` into a ``{key: LiveTable}`` map.

    Keys are the persisted-``sql_table`` form (bare for the default, else
    ``schema.table``); the default schema (and a single-schema scope) is also
    exposed bare. Returns ``(map, object_count)`` — the count lets the caller
    tell a genuinely empty datasource from a total introspection failure.
    """
    from slayer.engine.ingestion import list_ingestable_objects
    single = len(refs) == 1
    out: dict[str, LiveTable] = {}
    object_count = 0
    failed: list[str] = []
    for ref in refs:
        try:
            objs = list_ingestable_objects(
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
        # A schema in scope could not be listed, so its objects are absent from
        # the map. Diffing against a PARTIAL map would report every model in
        # that schema as a WholeModelDelete (a transient error → data loss under
        # ``--force-clean``). Fail closed (CodeRabbit) — the caller catches this
        # and skips the drift verdict entirely.
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
    """Return ``{object_name: LiveTable}`` for every live table AND view in the
    DS, using the same ``Inspector`` fallback path as auto-ingestion.

    Views are included unconditionally — no ``include_views`` gate, by design.
    The map is only a lookup target, so views can't manufacture a model; but a
    model whose ``sql_table`` names a view would otherwise resolve to ``None``
    and be reported as a ``WholeModelDelete`` that ``--force-clean`` acts on.
    Gating on ``--no-views`` would re-arm that data-loss bug.

    ``fallback_schema_tokens`` is threaded to :func:`_live_schema_refs` — see there.
    """
    from slayer.engine.ingestion import _dispose_quietly
    from slayer.sql import engine_factory
    sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
    try:
        inspector = sa.inspect(sa_engine)
        # DEV-1758: when no schema is pinned, cover every own-catalog schema so
        # a model qualified to a non-default schema diffs against the right
        # twin; an explicit ``schema`` scopes to just that schema. Keys are the
        # persisted-``sql_table`` form (bare for the default, ``schema.table``
        # otherwise), with the default schema also exposed bare so a legacy
        # unqualified model resolves the way the database would.
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
        # Same rationale as ``ingest_datasource``: this is a one-shot
        # admin path. Disposing releases the underlying connection so
        # external direct file access (e.g. ``duckdb.connect(file)``)
        # in the same process isn't blocked. Quiet, so a raising dispose
        # can't replace an in-flight introspection error.
        _dispose_quietly(sa_engine)


def _introspect_one_table(
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    ref: SchemaRef | None,
) -> LiveTable:
    """Build a ``LiveTable`` for one table via the existing safe-introspection
    path used by ``slayer/engine/ingestion.py``.

    ``ref`` carries the catalog-qualified schema identity so the column / PK
    fallbacks stay catalog-scoped on DuckDB (never unioning an attached twin).
    """
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
            # is_float not relevant for bucket comparison; both INT and FLOAT
            # collapse to NUMBER.
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
) -> dict[str, DataType] | None:
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


def _has_star_projection(parsed: exp.Expression) -> bool:
    """Whether any SELECT in the statement projects ``*`` or ``t.*``."""
    for select in parsed.find_all(exp.Select):
        for proj in select.expressions:
            if isinstance(proj, exp.Star):
                return True
            if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
                return True
    return False


def _sql_model_source_refs(*, sql: str, dialect: str) -> "dict[str, set[str]] | None":
    """Map each source table of a sql-mode model to the columns it references.

    Keys are rendered table references (alias stripped, CTE names excluded);
    values are rendered column identifiers. Returns ``None`` (cannot verify)
    when the SQL does not parse, a bare column cannot be attributed to a
    single source table, or a CTE / derived-table ref coexists with a ``*``
    projection. Without a ``*``, a CTE or derived-table ref is only a rename
    of explicit refs that are all collected here, so it is safe to skip.
    """
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return None
    cte_names = {cte.alias_or_name for cte in parsed.find_all(exp.CTE)}
    refs: dict[str, set[str]] = {}
    table_by_qualifier: dict[str, str] = {}
    for table in parsed.find_all(exp.Table):
        if not table.name:
            continue
        if not table.db and table.name in cte_names:
            continue
        bare = table.copy()
        bare.set("alias", None)
        rendered = bare.sql(dialect=dialect)
        refs.setdefault(rendered, set())
        for qualifier in {table.alias_or_name, table.name}:
            table_by_qualifier[qualifier] = rendered
    has_derived_ref = False
    for col in parsed.find_all(exp.Column):
        if not isinstance(col.this, exp.Identifier):
            continue
        rendered_col = exp.Column(this=col.this.copy()).sql(dialect=dialect)
        qualifier = col.table
        if qualifier:
            target = None if qualifier in cte_names else table_by_qualifier.get(qualifier)
            if target is not None:
                refs[target].add(rendered_col)
            else:
                has_derived_ref = True
        elif len(refs) == 1:
            refs[next(iter(refs))].add(rendered_col)
        else:
            # A bare column over several source tables cannot be attributed.
            return None
    if has_derived_ref and _has_star_projection(parsed):
        # A ``*`` may hide the physical column behind the derived ref.
        return None
    return refs


async def _source_tables_resolve(
    *, model: SlayerModel, client: SlayerSQLClient
) -> bool:
    """Whether every table AND column referenced by ``model.sql`` accepts a
    0-row probe.

    ``True`` means the trial-execute failure was NOT caused by a missing
    table or column — the model's own SQL is broken. Any probe failure
    (missing object, transient error, unparseable or ambiguous SQL) returns
    ``False``, keeping the schema-drift classification.
    """
    dialect = dialect_for_ds_type(client.datasource.type).sqlglot_name
    refs = _sql_model_source_refs(sql=model.sql or "", dialect=dialect)
    if not refs:
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


# ===========================================================================
# Datasource-level orchestrator
# ===========================================================================


def _strip_ident_quotes(ident: str) -> str:
    """Strip surrounding double-quotes from an SQL identifier and unescape
    ``""`` → ``"``. Bare identifiers pass through unchanged.
    """
    ident = ident.strip()
    if len(ident) >= 2 and ident[0] == '"' == ident[-1]:
        return ident[1:-1].replace('""', '"')
    return ident


def _resolve_live_table(
    *, sql_table: str, live_tables: dict[str, LiveTable]
) -> LiveTable | None:
    """Look up a model's ``sql_table`` in the live introspection map.

    Walks the full value and its last two dotted segments
    (``catalog.schema.table`` → ``schema.table``), plus double-quote-unquoted
    variants (``prod."Company"`` → ``prod.Company``). A qualified name is NEVER
    stripped down to a *bare* same-named object: doing so would let a dropped
    non-default table (``analytics.orders``) masquerade as its default-schema
    twin (``orders``) and hide the deletion (DEV-1758, Codex review). The live
    map instead keys the default schema BOTH bare and qualified
    (``_collect_live_tables``), so a legitimate default / bare reference still
    resolves via a full or last-two match.
    """
    parts = sql_table.split(".")
    # Unquote per segment (``prod."Company"`` → ``prod.Company``), NOT the whole
    # string — only the object segment is typically quoted.
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
    schema_name: str | None,
) -> DataType | None:
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
    default_schema: str | None = None,
) -> list[tuple[str, DataType, DeleteReason]]:
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
    """Run the SQLite probe for every model in one synchronous worker so
    the engine + connection lifecycle is shared across the validate pass."""
    if not sql_table_models:
        return {}
    if (datasource.type or "").lower() != "sqlite":
        return {m.name: [] for m in sql_table_models}

    def _run() -> dict[str, list[tuple[str, DataType, DeleteReason]]]:
        from slayer.sql import engine_factory
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
    live_tables: dict[str, "LiveTable"],
    available_in_ds: set[str],
    probe_drifts: list[tuple[str, DataType, DeleteReason]],
) -> tuple[ToDeleteEntry | None, set[str]]:
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
    sql_table_models: list[SlayerModel],
    available_in_ds: set[str],
) -> dict[str, tuple[ToDeleteEntry | None, set[str]]]:
    """Run live SQLAlchemy introspection (off the event loop) and diff each
    sql_table-mode model against it. On SQLite, additionally run the
    DEV-1538 affinity probe per persisted INT base column and merge any
    drift entries into each model's diff so the cascade fixed-point walk
    sees them as regular column drops.
    """
    if not sql_table_models:
        return {}
    # Introspect every own-catalog schema (not just the persisted schema_name):
    # a pinned datasource can still hold a model qualified to another own schema,
    # which a single-schema live map would report as a false WholeModelDelete.
    # If enumeration is unavailable, fall back to exactly the models' own schemas
    # so validation still works on a least-privilege connection.
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
        # Unknown live schema — reporting every model for deletion here would
        # hand ``--force-clean`` a whole tenant on a transient credential error.
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
    # DEV-1551: SlayerQueryEngine._sql_clients is tuple-keyed
    # (connection_string, runtime_fingerprint) so Snowflake datasources
    # sharing a connection_name but differing in warehouse/role get
    # distinct clients. Mirror that key shape here via the shared
    # ``_sql_client_cache_key`` helper.
    from slayer.engine.query_engine import _sql_client_cache_key  # noqa: PLC0415
    key = _sql_client_cache_key(datasource)
    client = (sql_clients or {}).get(key)
    if client is None:
        client = SlayerSQLClient(datasource=datasource)
        # DEV-1656: cache the client back into the shared engine dict so the
        # asyncpg pool it opens (trial-execute of sql-mode models) is
        # reachable by ``SlayerQueryEngine.aclose()`` and disposed at task
        # teardown. When ``sql_clients`` is None (no engine — direct/test
        # callers own the lifecycle), behaviour is unchanged.
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
