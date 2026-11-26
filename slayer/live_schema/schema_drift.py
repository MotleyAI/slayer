"""Schema drift: diff persisted datasets against live schemas, emit minimal deletes. Read-only."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Iterable
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    Set,
)

import sqlglot
from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope
from typing_extensions import deprecated

from slayer.core import probes
from slayer.core.enums import (
    DataType, NUMERIC_DATATYPES, PRE_2_0_DATATYPES, TEMPORAL_DATATYPES, pre_2_0_datatype, stored_datatype,
)
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import QueryDimension, SlayerQuery, extension_dataset_names
from slayer.core.refs import IDENTIFIER_RE
from slayer.core.vocabulary import attribute_used, name_twins, old_keywords
from slayer.core.warnings import BrokenDependentWarning
from slayer.sql.dialects.reflection import TableKeyReadFailure, info_schema_list_element
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.live_schema.introspect_utils import reflect_columns, reflect_table_keys
from slayer.live_schema.schema_scope import (
    UNREACHABLE,
    SchemaEnumerationError,
    SchemaRef,
    default_schema_ref,
    is_system_schema,
    live_key,
    qualifies_tokens,
    resolve_ingest_scope,
    sql_table_location,
    sql_table_parts,
    table_schema_ref,
)
from slayer.live_schema import ingestion
from slayer.core.errors import StoredDocumentLoadError
from slayer.core.relationship_edges import RelationshipEdgeRef, edge_reference, is_addressable
from slayer.core.relationship_walker import OrientedRelationship
from slayer.sql.field_expansion import resolve_ref_target
from slayer.engine.extensions import QueryScope, query_scopes
from slayer.engine.statement_reads import RelationshipRead, StatementReads
from slayer.core.syntax import (
    AggCall,
    DottedRef,
    ParsedExpr,
    Ref,
    StarSource,
    parse_expr,
    walk_parsed_refs,
)
from slayer.database import sqlite_introspect
from slayer.database.client import SqlClient, render_type_probe
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type
from slayer.sql.dialects.reflection import ResultColumnType
from slayer.database.engine_factory import ClientCacheKey, client_cache_key
from slayer.sql.naming import render_table_name

if TYPE_CHECKING:
    from slayer.live_schema.ingestion import InternalTable

logger = logging.getLogger(__name__)


# Public payload types


class DeleteReason(BaseModel):
    """Reason attached to a delete entry — surfaces in CLI / MCP / REST output."""

    target: str  # e.g. "field:status", "metric:aov", "relationship:customers", "dataset:orders"
    reason: str


class RemoveSpec(BaseModel):
    """Per-entity removal spec, mirroring the MCP ``edit_dataset`` ``remove=`` shape."""

    fields: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)
    # Edge references of the dropped relationships that have one; ``relationship_edges`` addresses each exactly.
    relationships: list[str] = Field(default_factory=list)
    relationship_edges: list[RelationshipEdgeRef] = Field(default_factory=list)
    # Enforced key sets the database no longer enforces; only the enforcement record is removed.
    enforced_keys: list[list[str]] = Field(default_factory=list)

    def add_relationship(self, *, dataset: SlayerDataset, relationship: DatasetRelationship) -> None:
        if is_addressable(dataset=dataset, relationship=relationship):
            self.relationships.append(edge_reference(dataset=dataset, relationship=relationship))
        self.relationship_edges.append(RelationshipEdgeRef.of(relationship))

    @property
    @deprecated("columns is deprecated; use fields", category=None)
    def columns(self) -> list[str]:
        attribute_used(self, old="columns", new="fields")
        return self.fields

    @property
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self) -> list[str]:
        attribute_used(self, old="measures", new="metrics")
        return self.metrics

    @property
    @deprecated("join_edges is deprecated; use relationship_edges", category=None)
    def join_edges(self) -> list[_JoinEdge]:
        attribute_used(self, old="join_edges", new="relationship_edges (a different edge shape: see the upgrade guide, "
                                                  "Integrators reading SLayer objects)")
        return [_JoinEdge(target_model=e.to, name=e.name, join_pairs=e.column_pairs) for e in self.relationship_edges]


class _JoinEdge(BaseModel):
    """A dropped relationship as the pre-2.0 drift result spelled it."""

    target_model: str
    name: str | None = None
    join_pairs: list[tuple[str, str]]


#: Dataset name → exact references of its relationships dropped so far.
_DroppedRelationships = dict[str, list[RelationshipEdgeRef]]


class EditDatasetDelete(BaseModel):
    """Surgical removals on an existing dataset. Replays as ``edit_dataset``."""

    tool: Literal["edit_dataset"] = "edit_dataset"
    dataset_name: str
    data_source: str
    remove: RemoveSpec = Field(default_factory=RemoveSpec)
    remove_filters: list[str] = Field(default_factory=list)
    reasons: list[DeleteReason] = Field(default_factory=list)

    @property
    @deprecated("model_name is deprecated; use dataset_name", category=None)
    def model_name(self) -> str:
        attribute_used(self, old="model_name", new="dataset_name")
        return self.dataset_name


class WholeDatasetDelete(BaseModel):
    """Whole-dataset removal. Replays as ``delete_dataset``."""

    tool: Literal["delete_dataset"] = "delete_dataset"
    dataset_name: str
    data_source: str
    reasons: list[DeleteReason] = Field(default_factory=list)
    # "invalid_sql": dataset's own SQL fails though its tables/columns exist;
    # "unloadable": the stored document itself cannot be loaded.
    cause: Literal["schema_drift", "invalid_sql", "unloadable"] = "schema_drift"

    @property
    @deprecated("model_name is deprecated; use dataset_name", category=None)
    def model_name(self) -> str:
        attribute_used(self, old="model_name", new="dataset_name")
        return self.dataset_name


ToDeleteEntry = Annotated[
    EditDatasetDelete | WholeDatasetDelete, Field(discriminator="tool")
]


RemedyKind = Literal["reingest", "set_type"]

#: Remedy texts serialized before ``remedy_kind`` existed.
_LEGACY_REMEDIES: dict[RemedyKind, re.Pattern[str]] = {
    "reingest": re.compile(r"re-ingest datasource '.*' \(`slayer ingest`\) to re-type it"),
    "set_type": re.compile(r"set the column's type to [A-Z_]+"),
}


#: A pre-2.0 serialized finding's keys and their replacements.
_PRE_2_0_FINDING_KEYS = {"model_name": "dataset_name", "column": "field"}


class RetypeFinding(BaseModel):
    """Report-only: a field whose live column exists with a conflicting type."""

    finding: Literal["retype"] = "retype"
    dataset_name: str
    data_source: str
    field: str
    stored_type: DataType
    live_type: DataType
    live_db_type: str | None = None
    remedy_kind: RemedyKind

    @model_validator(mode="before")
    @classmethod
    def _infer_legacy_remedy_kind(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = {_PRE_2_0_FINDING_KEYS.get(k, k) if _PRE_2_0_FINDING_KEYS.get(k, k) not in data else k: v
                for k, v in data.items()}
        for key, db_type in (("stored_type", None), ("live_type", data.get("live_db_type"))):
            value = data.get(key)
            if isinstance(value, str) and value in PRE_2_0_DATATYPES and stored_datatype(value) is not None:
                data = {**data, key: pre_2_0_datatype(value, db_type=db_type)}
        if "remedy_kind" in data:
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
        return f"set the field's type to {self.live_type.value}"

    @computed_field
    @property
    def summary(self) -> str:
        live = f"{self.live_db_type} ({self.live_type.value})" if self.live_db_type else self.live_type.value
        return (f"Field '{self.dataset_name}.{self.field}' is stored as {self.stored_type.value} but its live "
                f"column is {live}; {self.remedy}.")

    @property
    @deprecated("model_name is deprecated; use dataset_name", category=None)
    def model_name(self) -> str:
        attribute_used(self, old="model_name", new="dataset_name")
        return self.dataset_name

    @property
    @deprecated("column is deprecated; use field", category=None)
    def column(self) -> str:
        attribute_used(self, old="column", new="field")
        return self.field

    @classmethod
    def of(
        cls, *, dataset: SlayerDataset, field: DatasetField, live_type: DataType, live_db_type: str | None,
        remedy_kind: RemedyKind,
    ) -> "RetypeFinding":
        if field.datatype is None:
            raise ValueError(f"field {field.name!r} is untyped: an untyped field is an UntypedFinding, not a retype")
        return cls(
            dataset_name=dataset.name, data_source=dataset.data_source, field=field.name, stored_type=field.datatype,
            live_type=live_type, live_db_type=live_db_type, remedy_kind=remedy_kind,
        )


class UntypedFinding(BaseModel):
    """A semantic-model document field that declares no datatype, and the live type cleanup writes into it."""

    finding: Literal["untyped"] = "untyped"
    dataset_name: str
    data_source: str
    semantic_model: str
    field: str
    live_type: DataType
    live_element_type: DataType | None = None
    live_db_type: str | None = None

    @computed_field
    @property
    def summary(self) -> str:
        live = f"{self.live_db_type} ({self.live_type.value})" if self.live_db_type else self.live_type.value
        return (f"Field '{self.dataset_name}.{self.field}' of semantic model '{self.semantic_model}' declares no "
                f"datatype; its live type is {live}. `slayer validate-datasets --force-clean` writes it in.")


def untyped_finding(
    *, dataset: SlayerDataset, field: DatasetField, live_type: DataType, live_db_type: str | None,
    element_type: DataType | None = None,
) -> UntypedFinding | None:
    """The untyped finding of a document dataset's undeclared field; ``None`` off a document or when typed."""
    if field.datatype is not None or dataset.semantic_model is None:
        return None
    element = element_type or (info_schema_list_element(live_db_type) if live_type is DataType.ARRAY else None)
    return UntypedFinding(dataset_name=dataset.name, data_source=dataset.data_source,
                          semantic_model=dataset.semantic_model, field=field.name, live_type=live_type,
                          live_element_type=element, live_db_type=live_db_type)


def touches(reads: StatementReads, entry: Any) -> bool:
    """Whether a drift entry or finding of a read dataset removes or re-types something the statement reads."""
    if isinstance(entry, WholeDatasetDelete):
        return entry.cause == "schema_drift"
    if isinstance(entry, RetypeFinding):
        return (entry.dataset_name, entry.field) in reads.fields
    if not isinstance(entry, EditDatasetDelete):
        return False
    dataset, remove = entry.dataset_name, entry.remove
    return bool(entry.remove_filters) or any((
        any((dataset, c) in reads.fields for c in remove.fields),
        any((dataset, m) in reads.metrics for m in remove.metrics),
        any((dataset, a) in reads.aggregations for a in remove.aggregations),
        any(_reads_edge(reads.relationships, dataset=dataset, edge=e) for e in remove.relationship_edges),
    ))


def _reads_edge(relationships: Iterable[RelationshipRead], *, dataset: str, edge: RelationshipEdgeRef) -> bool:
    pairs = frozenset((p[0], p[1]) for p in edge.column_pairs)
    return any(
        declaring == dataset and target == edge.to and read_pairs == pairs
        and (edge.name is None or name == edge.name)
        for declaring, target, name, read_pairs in relationships
    )


class NarrowedAccessFinding(BaseModel):
    """Report-only: the dataset reads tagged datasets (``narrowed_by``) whose tags do not cover its own."""

    finding: Literal["narrowed_access"] = "narrowed_access"
    dataset_name: str
    data_source: str
    narrowed_by: list[str]


#: Validation entries that report rather than delete (cleanup writes an untyped finding in).
REPORT_ONLY_FINDINGS = (NarrowedAccessFinding, RetypeFinding, UntypedFinding)

#: One entry of a ``validate_datasets`` report.
DriftReportEntry = EditDatasetDelete | WholeDatasetDelete | NarrowedAccessFinding | RetypeFinding | UntypedFinding


def unloadable_dataset_delete(*, error: StoredDocumentLoadError) -> WholeDatasetDelete:
    """The validation report's entry for a stored dataset that cannot be loaded."""
    return WholeDatasetDelete(
        dataset_name=error.name, data_source=error.data_source or "", cause="unloadable",
        reasons=[DeleteReason(target=f"dataset:{error.name}", reason=error.cause_text)],
    )


class DatasetAddition(BaseModel):
    """One dataset touched by an idempotent re-ingestion pass."""

    dataset_name: str
    data_source: str
    created: bool = False  # True if the dataset was new
    new_fields: list[str] = Field(default_factory=list)
    # Added relationships by edge reference.
    new_relationships: list[str] = Field(default_factory=list)
    # Edge names this pass generated on the dataset's relationships.
    named_relationships: list[str] = Field(default_factory=list)
    # INT fields widened to DOUBLE/TEXT by the SQLite affinity probe.
    widened_fields: list[str] = Field(default_factory=list)
    # Output-only label; durable record is SlayerDataset.source_kind.
    source_kind: str | None = None
    # e.g. "view → table" when a re-ingest found the live object changed kind.
    kind_change: str | None = None
    # Fields whose empty description was filled from a DB comment.
    described_fields: list[str] = Field(default_factory=list)
    dataset_described: bool = False
    # Self-heal that added a missing schema qualifier ("reports → openfda_rest.reports").
    sql_table_change: str | None = None
    # A metadata-only fill (field unique, relationship cardinality) that was saved.
    metadata_changed: bool = False

    @property
    def has_changes(self) -> bool:
        """Whether this pass saved any change to an existing dataset."""
        return bool(
            self.new_fields or self.new_relationships or self.named_relationships or self.widened_fields
            or self.kind_change or self.described_fields or self.dataset_described or self.sql_table_change
            or self.metadata_changed
        )


class IngestionError(BaseModel):
    """Per-dataset failure during idempotent ingestion."""

    dataset_name: str
    data_source: str
    error: str


if TYPE_CHECKING:
    _HiddenInternals = list[InternalTable]
else:
    _HiddenInternals = list  # items are InternalTable; the import is circular


class IdempotentIngestResult(BaseModel):
    """Combined return shape of the idempotent ``ingest_datasource`` pass."""

    additions: list[DatasetAddition] = Field(default_factory=list)
    to_delete: list[ToDeleteEntry] = Field(default_factory=list)
    errors: list[IngestionError] = Field(default_factory=list)
    # Live objects that couldn't be modelled at all; entries are IngestableObject.
    skipped: list[Any] = Field(default_factory=list)
    # Every live object discovered this pass; entries are IngestableObject.
    objects: list[Any] = Field(default_factory=list)
    # ELT/migration bookkeeping modelled hidden; entries are InternalTable.
    hidden_internals: _HiddenInternals = Field(default_factory=list)
    # True when the datasource description was filled from the BigQuery dataset.
    datasource_described: bool = False
    # Message naming schemas discovered but not ingested this pass, or None.
    schema_hint: str | None = None
    # Requested schemas dropped from scope with a reason; entries are SkippedSchema.
    skipped_schemas: list[Any] = Field(default_factory=list)
    # Stored definitions a list re-type this pass breaks.
    broken_dependents: list[BrokenDependentWarning] = Field(default_factory=list)
    # Key reads that failed: those keys are missing until a re-ingest reads them.
    key_read_failures: list[TableKeyReadFailure] = Field(default_factory=list)


class AppliedEntry(BaseModel):
    """A delete entry that ``apply_drift_deletes`` successfully applied."""

    tool: Literal["edit_dataset", "delete_dataset"]
    dataset_name: str
    data_source: str


class ApplyError(BaseModel):
    """Per-entry failure during ``apply_drift_deletes``."""

    tool: Literal["edit_dataset", "delete_dataset"]
    dataset_name: str
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
    # The primary-key and unconditional unique key sets the database enforces; ``None`` = not read.
    key_sets: list[list[str]] | None = None


# Type-bucket comparison


def data_type_bucket(dt: DataType) -> str:
    """Coarse bucket for comparing types; INT/DOUBLE → "number", DATE/TIMESTAMP → "temporal" so driver variance isn't drift."""
    if dt in NUMERIC_DATATYPES:
        return "number"
    if dt == DataType.STRING:
        return "string"
    if dt == DataType.BOOLEAN:
        return "boolean"
    if dt in TEMPORAL_DATATYPES:
        return "temporal"
    return str(dt)


def _type_buckets_conflict(*, persisted: DataType, live: DataType, temporal_text: bool) -> bool:
    """True when persisted vs live types are incompatible; opacity is one-way (live-opaque + operable-persisted conflicts, persisted-opaque never does); a non-list over a live list is a re-type finding, never a conflict; ``temporal_text``: a DATE/TIMESTAMP/TIME over live TEXT is the dialect's own storage of it."""
    if live is DataType.ARRAY and persisted is not DataType.ARRAY:
        return False
    if temporal_text and live is DataType.STRING and (persisted in TEMPORAL_DATATYPES or persisted.is_time_of_day):
        return False
    if live.is_opaque:
        return not persisted.is_opaque
    if persisted.is_opaque:
        return False
    return data_type_bucket(persisted) != data_type_bucket(live)


def _retyped(*, persisted: DataType, live: DataType, temporal_text: bool) -> bool:
    return (live is DataType.ARRAY and persisted is not DataType.ARRAY) or _type_buckets_conflict(
        persisted=persisted, live=live, temporal_text=temporal_text)


def _field_finding(
    *, dataset: SlayerDataset, field: DatasetField, live_type: DataType, live_db_type: str | None,
    temporal_text: bool, element_type: DataType | None = None,
) -> RetypeFinding | UntypedFinding | None:
    """The one field's untyped or re-type finding against its live column type, if any."""
    if field.datatype is None:
        return untyped_finding(dataset=dataset, field=field, live_type=live_type, live_db_type=live_db_type,
                               element_type=element_type)
    if not _retyped(persisted=field.datatype, live=live_type, temporal_text=temporal_text):
        return None
    return RetypeFinding.of(
        dataset=dataset, field=field, live_type=live_type, live_db_type=live_db_type,
        remedy_kind="reingest" if live_type in (DataType.ARRAY, DataType.TIME) else "set_type",
    )


@old_keywords({"model": "dataset"})
def retype_findings(
    *, dataset: SlayerDataset, live_table: LiveTable | None, temporal_text: bool = False,
) -> list[RetypeFinding | UntypedFinding]:
    """One finding per stored base field whose live column's type conflicts (a live list re-ingests), or that a
    document leaves untyped."""
    if live_table is None:
        return []
    out: list[RetypeFinding | UntypedFinding] = []
    for col in dataset.fields:
        if not col.reads_physical_column:
            continue
        name = col.physical_name
        live = live_table.columns.get(name)
        if live is None:
            continue
        if finding := _field_finding(dataset=dataset, field=col, live_type=live,
                                     live_db_type=live_table.db_types.get(name), temporal_text=temporal_text):
            out.append(finding)
    return out


def sql_dataset_retype_findings(
    *, dataset: SlayerDataset, live_columns: dict[str, ResultColumnType] | None, temporal_text: bool = False,
) -> list[RetypeFinding | UntypedFinding]:
    """One finding per field of a sql-mode dataset whose result column's type conflicts, or that a document leaves
    untyped."""
    out: list[RetypeFinding | UntypedFinding] = []
    for col in dataset.fields:
        live = (live_columns or {}).get(col.name)
        if live is None and col.is_base:
            live = (live_columns or {}).get(col.physical_name)
        if live is None:
            continue
        if finding := _field_finding(dataset=dataset, field=col, live_type=live.type, live_db_type=live.db_type,
                                     temporal_text=temporal_text, element_type=live.element_type):
            out.append(finding)
    return out


def _is_bare_identifier(s: str | None) -> bool:
    if not s:
        return False
    return IDENTIFIER_RE.match(s.strip()) is not None


# Pure diff functions


def _diff_sql_table_columns(
    *, dataset: SlayerDataset, live_table: LiveTable,
) -> tuple[list[str], list[DeleteReason]]:
    """Per-field diff of a sql_table-mode dataset against live columns."""
    dropped: list[str] = []
    reasons: list[DeleteReason] = []
    for col in dataset.fields:
        # Physical readers only; derived handled by cascade.
        if not col.reads_physical_column:
            continue
        bare_name = col.physical_name
        if bare_name not in live_table.columns:
            dropped.append(col.name)
            reasons.append(
                DeleteReason(
                    target=f"field:{col.name}",
                    reason=f"Live column {bare_name!r} not found",
                )
            )
    return dropped, reasons


def _diff_sql_table_relationships(
    *,
    dataset: SlayerDataset,
    live_table: LiveTable,
    available_datasets_in_ds: set[str],
) -> tuple[list[DatasetRelationship], list[DeleteReason]]:
    """Per-relationship diff of a sql_table-mode dataset against live FK columns and dataset availability."""
    dropped: list[DatasetRelationship] = []
    reasons: list[DeleteReason] = []
    for relationship in dataset.relationships:
        local_cols = [pair[0] for pair in relationship.column_pairs]
        missing_locals = [
            lc for lc in local_cols
            if (col := dataset.get_field(lc)) is None
            or (col.physical_name if col.is_base else col.name) not in live_table.columns
        ]
        if missing_locals:
            dropped.append(relationship)
            reasons.append(
                DeleteReason(
                    target=f"relationship:{edge_reference(dataset=dataset, relationship=relationship)}",
                    reason=(
                        f"Local FK column(s) {missing_locals} missing from "
                        f"live table"
                    ),
                )
            )
            continue
        if relationship.to not in available_datasets_in_ds:
            dropped.append(relationship)
            reasons.append(
                DeleteReason(
                    target=f"relationship:{edge_reference(dataset=dataset, relationship=relationship)}",
                    reason=(
                        f"Relationship target {relationship.to!r} not present in "
                        f"datasource {dataset.data_source!r}"
                    ),
                )
            )
    return dropped, reasons


def _vanished_enforced_keys(
    *, dataset: SlayerDataset, live_table: LiveTable, enforces_keys: bool,
) -> tuple[list[list[str]], list[DeleteReason]]:
    """Each enforced key set the live table no longer enforces (every one where the database enforces none)."""
    if enforces_keys and live_table.key_sets is None:
        return [], []
    live = [sorted(k) for k in live_table.key_sets or []] if enforces_keys else []
    vanished: list[list[str]] = []
    reasons: list[DeleteReason] = []
    for key in dataset.enforced_keys:
        fields = [dataset.get_field(n) for n in key]
        spelled = sorted(f.physical_name if f.is_base else f.name for f in fields if f is not None)
        if len(spelled) == len(key) and spelled in live:
            continue
        vanished.append(list(key))
        why = "the database does not enforce unique keys" if not enforces_keys else (
            "the live table no longer carries it as a primary or unique key")
        reasons.append(DeleteReason(target=f"enforced_key:{','.join(key)}",
                                    reason=f"Enforced key {list(key)} is not enforced: {why}"))
    return vanished, reasons


@old_keywords({"model": "dataset", "available_models_in_ds": "available_datasets_in_ds"})
def diff_sql_table_dataset(
    *,
    dataset: SlayerDataset,
    live_table: LiveTable | None,
    available_datasets_in_ds: set[str],
    enforces_keys: bool = True,
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Diff a sql_table-mode dataset against live introspection → ``(entry_or_None, dropped_column_names)``; ``live_table is None`` → WholeDatasetDelete."""
    if live_table is None:
        return (
            WholeDatasetDelete(
                dataset_name=dataset.name,
                data_source=dataset.data_source,
                reasons=[
                    DeleteReason(
                        target=f"dataset:{dataset.name}",
                        reason=(
                            f"Live table {dataset.sql_table!r} not found in "
                            f"datasource {dataset.data_source!r}"
                        ),
                    )
                ],
            ),
            {c.name for c in dataset.fields},
        )

    dropped_cols, col_reasons = _diff_sql_table_columns(dataset=dataset, live_table=live_table)
    dropped_relationships, relationship_reasons = _diff_sql_table_relationships(
        dataset=dataset,
        live_table=live_table,
        available_datasets_in_ds=available_datasets_in_ds,
    )

    vanished_keys, key_reasons = _vanished_enforced_keys(
        dataset=dataset, live_table=live_table, enforces_keys=enforces_keys)
    if not dropped_cols and not dropped_relationships and not vanished_keys:
        return None, set()
    reasons = col_reasons + relationship_reasons + key_reasons
    remove = RemoveSpec(fields=dropped_cols, enforced_keys=vanished_keys)
    for relationship in dropped_relationships:
        remove.add_relationship(dataset=dataset, relationship=relationship)

    return (
        EditDatasetDelete(
            dataset_name=dataset.name,
            data_source=dataset.data_source,
            remove=remove,
            reasons=reasons,
        ),
        set(dropped_cols),
    )


def _sql_trial_failure_delete(
    *, dataset: SlayerDataset, invalid_sql: bool
) -> tuple[WholeDatasetDelete, set[str]]:
    """``WholeDatasetDelete`` for a sql-mode dataset whose trial-execute failed."""
    if invalid_sql:
        reason = (
            "Trial-execute on dataset.sql failed although its source "
            "tables and referenced columns exist; the SQL itself does "
            "not execute against this datasource"
        )
    else:
        reason = (
            "Trial-execute on dataset.sql failed; the SQL no longer "
            "parses or executes against the live datasource"
        )
    return (
        WholeDatasetDelete(
            dataset_name=dataset.name,
            data_source=dataset.data_source,
            cause="invalid_sql" if invalid_sql else "schema_drift",
            reasons=[
                DeleteReason(target=f"dataset:{dataset.name}", reason=reason)
            ],
        ),
        {c.name for c in dataset.fields},
    )


def _diff_sql_dataset_fields(
    *, dataset: SlayerDataset, live_columns: Collection[str],
) -> tuple[list[str], list[DeleteReason]]:
    """Dataset fields missing from cursor metadata."""
    dropped_cols: list[str] = []
    reasons: list[DeleteReason] = []
    for col in dataset.fields:
        # Cursor exposes alias names: match col.name, fall back to the column it reads.
        if col.name not in live_columns and not (col.is_base and col.physical_name in live_columns):
            # Physical readers only; derived expressions handled by cascade.
            if col.reads_physical_column:
                dropped_cols.append(col.name)
                reasons.append(
                    DeleteReason(
                        target=f"field:{col.name}",
                        reason=(
                            f"Cursor metadata for dataset.sql does not "
                            f"include {col.name!r}"
                        ),
                    )
                )
    return dropped_cols, reasons


def diff_sql_dataset(
    *,
    dataset: SlayerDataset,
    live_columns: Collection[str] | None,
    invalid_sql: bool = False,
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Diff a sql-mode dataset against cursor metadata's column names; ``live_columns is None`` ⇒ WholeDatasetDelete,
    ``invalid_sql=True`` ⇒ the SQL itself is broken, not drift."""
    if live_columns is None:
        return _sql_trial_failure_delete(dataset=dataset, invalid_sql=invalid_sql)
    dropped_cols, reasons = _diff_sql_dataset_fields(dataset=dataset, live_columns=live_columns)
    if not dropped_cols:
        return None, set()
    return (
        EditDatasetDelete(
            dataset_name=dataset.name,
            data_source=dataset.data_source,
            remove=RemoveSpec(fields=dropped_cols),
            reasons=reasons,
        ),
        set(dropped_cols),
    )


# Reference extraction helpers (sqlglot AST + expression AST)


def _extract_field_refs_from_sql(sql: str) -> list[tuple[str | None, str]]:
    """All ``(qualifier, column_name)`` refs in a SQL expression; ``qualifier`` is None for bare identifiers, else the full dotted relationship path (``__`` never split)."""
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


def _metric_expression_refs(expression: str) -> Set[str]:
    """Field/metric names in a Mode-B expression (dotted for cross-dataset);
    textual only. Both aggregation spellings parse natively, and an
    unknown functional name defers as an ``AggCall`` candidate, so custom
    aggregations — joined-dataset ones included — need no registry walk."""
    try:
        parsed = parse_expr(expression)
    except Exception:
        return set()
    return set(_walk_ref_names(parsed))


def _filter_refs(filter_str: str) -> list[str]:
    """Field references in a SQL-mode (Mode A) filter; ``[]`` on parse failure."""
    try:
        pf = parse_sql_predicate(filter_str)
    except Exception:
        return []
    return list(pf.fields)


def _filter_refs_dsl(filter_str: str) -> list[str]:
    """Field/metric references in a DSL (Mode B) filter, in expression order
    (deduplicated); ``[]`` on parse failure."""
    try:
        parsed = parse_expr(filter_str)
    except Exception:
        return []
    return list(dict.fromkeys(_walk_ref_names(parsed)))


def _walk_alias_to_target_dataset(
    *,
    source_dataset: SlayerDataset,
    table_alias: str,
    datasets_by_name: dict[str, SlayerDataset],
) -> SlayerDataset | None:
    """Resolve a Mode-A qualifier to its terminal joined dataset; exact-matched then walked as a chain of exact hops (never ``__``-split), None if any hop fails."""
    if table_alias in (source_dataset.name, ""):
        return source_dataset
    return resolve_ref_target(
        qualifiers=tuple(table_alias.split(".")),
        source_dataset=source_dataset,
        datasets_by_name=datasets_by_name,
    )


def _resolve_dotted_ref_to_dataset(
    *,
    source_dataset: SlayerDataset,
    dotted_ref: str,
    datasets_by_name: dict[str, SlayerDataset],
) -> tuple[SlayerDataset | None, str | None]:
    """Resolve a dotted ref (``customers.regions.name``) to ``(target_dataset, leaf)``; bare refs return ``(source_dataset, ref)``."""
    if "." not in dotted_ref:
        return source_dataset, dotted_ref
    prefix, leaf = dotted_ref.rsplit(".", 1)
    target = _walk_alias_to_target_dataset(
        source_dataset=source_dataset,
        table_alias=prefix,
        datasets_by_name=datasets_by_name,
    )
    return target, leaf


# Cascade helpers


def _pk_fields(dataset: SlayerDataset) -> set[str]:
    return {c.name for c in dataset.fields if c.primary_key}


def _ensure_edit_entry(
    *,
    edit_entries: dict[str, EditDatasetDelete],
    dataset: SlayerDataset,
) -> EditDatasetDelete:
    if dataset.name not in edit_entries:
        edit_entries[dataset.name] = EditDatasetDelete(
            dataset_name=dataset.name,
            data_source=dataset.data_source,
        )
    return edit_entries[dataset.name]


def _add_dropped_field(
    *,
    edit_entries: dict[str, EditDatasetDelete],
    dropped_cols: dict[str, set[str]],
    dataset: SlayerDataset,
    field_name: str,
    reason: str,
) -> bool:
    """Record a cascade-induced field drop. Returns True if newly added."""
    if field_name in dropped_cols.get(dataset.name, set()):
        return False
    entry = _ensure_edit_entry(edit_entries=edit_entries, dataset=dataset)
    if field_name not in entry.remove.fields:
        entry.remove.fields.append(field_name)
        entry.reasons.append(
            DeleteReason(target=f"field:{field_name}", reason=reason)
        )
    dropped_cols.setdefault(dataset.name, set()).add(field_name)
    return True


def _add_dropped_metric(
    *,
    edit_entries: dict[str, EditDatasetDelete],
    dropped_metrics: dict[str, set[str]],
    dataset: SlayerDataset,
    metric_name: str,
    reason: str,
) -> bool:
    if metric_name in dropped_metrics.get(dataset.name, set()):
        return False
    entry = _ensure_edit_entry(edit_entries=edit_entries, dataset=dataset)
    if metric_name not in entry.remove.metrics:
        entry.remove.metrics.append(metric_name)
        entry.reasons.append(
            DeleteReason(target=f"metric:{metric_name}", reason=reason)
        )
    dropped_metrics.setdefault(dataset.name, set()).add(metric_name)
    return True


def _add_dropped_relationship(
    *,
    edit_entries: dict[str, EditDatasetDelete],
    dropped_relationships: _DroppedRelationships,
    dataset: SlayerDataset,
    relationship: DatasetRelationship,
    reason: str,
) -> bool:
    ref = RelationshipEdgeRef.of(relationship)
    if ref in dropped_relationships.get(dataset.name, []):
        return False
    entry = _ensure_edit_entry(edit_entries=edit_entries, dataset=dataset)
    if ref not in entry.remove.relationship_edges:
        entry.remove.add_relationship(dataset=dataset, relationship=relationship)
        entry.reasons.append(
            DeleteReason(target=f"relationship:{edge_reference(dataset=dataset, relationship=relationship)}", reason=reason)
        )
    dropped_relationships.setdefault(dataset.name, []).append(ref)
    return True


def _add_remove_filter(
    *,
    edit_entries: dict[str, EditDatasetDelete],
    dataset: SlayerDataset,
    filter_text: str,
    reason: str,
) -> bool:
    entry = _ensure_edit_entry(edit_entries=edit_entries, dataset=dataset)
    if filter_text in entry.remove_filters:
        return False
    entry.remove_filters.append(filter_text)
    entry.reasons.append(
        DeleteReason(target=f"filter:{filter_text}", reason=reason)
    )
    return True


# Query-backed cascade


def _stage_scopes(qb_dataset: SlayerDataset, *, datasets_by_name: dict[str, SlayerDataset]) -> list[QueryScope]:
    """Each stage of ``qb_dataset`` scoped as the engine resolves it, over the datasource's ``datasets_by_name``."""
    stages = list(qb_dataset.source_queries or [])
    ds = qb_dataset.data_source
    return [scopes[0] for scopes in query_scopes(stages, datasets_by_ds={ds: datasets_by_name}, data_source=ds)]


def _terminal_base(name: str, *, scope: QueryScope) -> str | None:
    """The stored dataset a reference landing on ``name`` reads: a sibling stage's chain end, else ``name``."""
    if name in scope.siblings:
        return scope.bases.get(name)
    return None if name in scope.shadowed else name


#: A reference and the dataset it is spelled from (``None``: the stage source).
_FramedRef = tuple[str | None, str]


def _resolved(ref: str, *, scope: QueryScope, root: str | None) -> tuple[str, list[OrientedRelationship]] | None:
    """The dataset ``ref``'s hops reach from ``root`` (default: the stage source) and the edges walked, or None."""
    start = scope.datasets.get(root) if root is not None else scope.host
    hops = ref.split(".")[:-1]
    if root is None:
        resolved = scope.resolve_reference(hops)
    else:
        resolved = scope.resolve(hops, root=start) if start is not None else None
    if start is None or resolved is None:
        return None
    chain = resolved[1]
    return (chain[-1].to if chain else start.name), chain


def _attribute_ref_to_base(*, ref: str, base_name: str, scope: QueryScope, root: str | None = None) -> str | None:
    """Walk ``ref`` through the stage's relationship graph; the leaf field name when it resolves to ``base_name``, else None (bare refs attribute to the stage source)."""
    resolved = _resolved(ref, scope=scope, root=root)
    if resolved is None:
        return None
    return ref.rsplit(".", 1)[-1] if _terminal_base(resolved[0], scope=scope) == base_name else None


def _ref_edges(*, ref: str, scope: QueryScope, root: str | None = None) -> list[OrientedRelationship]:
    """The edges ``ref``'s path walks over the stage scope (query-local included), a short-form ref along its route."""
    resolved = _resolved(ref, scope=scope, root=root)
    return resolved[1] if resolved is not None else []


def _stage_refs(stage: SlayerQuery) -> list[str]:
    """Every field reference of one stage: metrics, dimensions, time dimensions, filters."""
    refs = [
        ref for m in stage.metrics or [] if m.metric
        for ref in _metric_expression_refs(m.metric)
    ]
    refs += [ref for d in stage.dimensions or [] for ref in _dimension_refs(d)]
    refs += [ref for td in stage.time_dimensions or [] for ref in _metric_expression_refs(td.field)]
    # Mode-B (DSL) filters: use the DSL parser so aggregations/transforms surface.
    refs += [col for f in stage.filter_conditions for col in _filter_refs_dsl(f)]
    return refs


def _dimension_refs(dimension: QueryDimension) -> list[str]:
    ref = dimension.ref
    return sorted(_metric_expression_refs(dimension.field)) if dimension.is_computed or ref is None else [ref.full_name]


def _definition_refs(field: DatasetField | DatasetMetric) -> list[str]:
    """The refs ``field``'s definition reads, spelled from its own dataset."""
    if isinstance(field, DatasetMetric):
        return sorted(_metric_expression_refs(field.text))
    sqls = [s for s in (field.definition_sql, field.filter) if s is not None]
    return [f"{q}.{c}" if q else c for sql in sqls for q, c in _extract_field_refs_from_sql(sql)]


def _extension_field(dataset: SlayerDataset | None, name: str) -> DatasetField | DatasetMetric | None:
    """``dataset``'s query-local field or metric ``name``."""
    if dataset is None or name not in dataset.query_local_names:
        return None
    return dataset.get_field(name, quoted=True) or dataset.get_metric(name, quoted=True)


def _field_definition_refs(ref: _FramedRef, *, scope: QueryScope) -> list[_FramedRef]:
    """The refs inside the extension field ``ref`` names, each spelled from the dataset that owns the field."""
    root, text = ref
    resolved = _resolved(text, scope=scope, root=root)
    field = _extension_field(scope.datasets.get(resolved[0]), text.rsplit(".", 1)[-1]) if resolved is not None else None
    if resolved is None or field is None:
        return []
    return [(resolved[0], r) for r in _definition_refs(field)]


def _stage_dependency_refs(*, stage: SlayerQuery, scope: QueryScope) -> list[_FramedRef]:
    """Every ref ``stage`` reads: its own, its INNER restrictions, and those inside the extension fields they reach."""
    refs: list[_FramedRef] = [(None, r) for r in [*_stage_refs(stage), *(".".join(p) for p in scope.inner_refs)]]
    pending = list(refs) if stage.extensions else []
    while pending:
        new = [r for r in _field_definition_refs(pending.pop(), scope=scope) if r not in refs]
        refs += new
        pending += new
    return refs


def _refs_on_base(*, refs: list[_FramedRef], base_name: str, scope: QueryScope) -> set[str]:
    """Field names ``refs`` read *on* ``base_name``, walked through the stage's relationship graph."""
    attributed = (_attribute_ref_to_base(ref=ref, base_name=base_name, scope=scope, root=root) for root, ref in refs)
    return {leaf for leaf in attributed if leaf is not None}


def _stage_relationship_targets(stage: SlayerQuery, *, scope: QueryScope) -> set[str]:
    """The stored datasets a stage's extensions name."""
    return set(extension_dataset_names(stage)) - scope.shadowed


def _check_stage_against_base(
    *,
    stage: SlayerQuery,
    refs: list[_FramedRef],
    base_name: str,
    scope: QueryScope,
    state: _CascadeState,
) -> set[str]:
    """Dropped field (PKs excluded — rule 7) and extension-metric names on ``base_name`` the stage's ``refs`` read."""
    own_metrics = {m.name for e in stage.extensions or [] if e.dataset == base_name for m in e.metrics or []}
    cascadable = state.cascadable(base_name) | (state.dropped_metrics.get(base_name, set()) & own_metrics)
    if not cascadable:
        return set()
    return _refs_on_base(refs=refs, base_name=base_name, scope=scope) & cascadable


def _stage_uses_dropped_relationship(
    *,
    refs: list[_FramedRef],
    base_name: str,
    scope: QueryScope,
    dropped_relationships: _DroppedRelationships,
) -> str | None:
    """The reference of a relationship dropped on ``base_name`` that one of a stage's ``refs`` walks, else None."""
    dropped = dropped_relationships.get(base_name, [])
    if not dropped:
        return None
    for root, ref in refs:
        for edge in _ref_edges(ref=ref, scope=scope, root=root):
            hit = next((d for d in dropped if edge.declaring_dataset == base_name and d.matches_hop(edge)), None)
            if hit is not None:
                return hit.name or hit.to
    return None


def _check_stage_for_whole_drop(
    *,
    stage: SlayerQuery,
    base_name: str,
    qb_name: str,
    scope: QueryScope,
    whole_dropped_datasets: set[str],
    state: _CascadeState,
    candidate_base_names: set[str],
) -> DeleteReason | None:
    """First whole-drop trigger for one stage of a query-backed dataset, or None."""
    if base_name in whole_dropped_datasets and base_name not in scope.shadowed:
        return DeleteReason(
            target=f"dataset:{qb_name}",
            reason=(
                f"source_queries stage references base dataset "
                f"{base_name!r} which is being whole-dropped"
            ),
        )
    for relationship_target in _stage_relationship_targets(stage, scope=scope):
        if relationship_target in whole_dropped_datasets:
            return DeleteReason(
                target=f"dataset:{qb_name}",
                reason=(
                    f"source_queries stage relates to {relationship_target!r} "
                    f"which is being whole-dropped"
                ),
            )
    refs = _stage_dependency_refs(stage=stage, scope=scope)
    for candidate in candidate_base_names:
        broken_target = _stage_uses_dropped_relationship(
            refs=refs,
            base_name=candidate,
            scope=scope,
            dropped_relationships=state.dropped_relationships,
        )
        if broken_target is not None:
            return DeleteReason(
                target=f"dataset:{qb_name}",
                reason=(
                    f"source_queries stage references {broken_target!r} "
                    f"via {candidate!r} but that relationship has been dropped"
                ),
            )
        hits = _check_stage_against_base(stage=stage, refs=refs, base_name=candidate, scope=scope, state=state)
        if hits:
            return DeleteReason(
                target=f"dataset:{qb_name}",
                reason=(
                    f"source_queries stage references fields on "
                    f"{candidate!r} that have been dropped: {sorted(hits)}"
                ),
            )
    return None


def _stage_cascade_state(*, stage: SlayerQuery, scope: QueryScope, state: _CascadeState) -> _CascadeState:
    """``state`` as ``stage`` reads it: the drops cascaded through its extensions' definitions too."""
    if not stage.extensions:
        return state
    local = _CascadeState(
        datasets_by_name=scope.datasets,
        edit_entries={},
        whole_entries={},
        dropped_cols={name: set(cols) for name, cols in state.dropped_cols.items()},
        dropped_metrics={name: set(metrics) for name, metrics in state.dropped_metrics.items()},
        dropped_relationships={name: list(refs) for name, refs in state.dropped_relationships.items()},
        pk_per_dataset=state.pk_per_dataset,
        vanished=state.vanished,
    )
    extended = [scope.datasets[e.dataset] for e in stage.extensions if e.dataset in scope.datasets]
    changed = True
    while changed:
        changed = False
        for dataset in extended:
            changed = _cascade_dataset(dataset=dataset, state=local) or changed
    return local


def _query_backed_should_whole_drop(
    *,
    qb_dataset: SlayerDataset,
    state: _CascadeState,
    whole_dropped_datasets: set[str],
    candidate_base_names: set[str],
) -> DeleteReason | None:
    """DeleteReason when a query-backed dataset should be whole-dropped by cascade from base drift, else None."""
    stages = list(qb_dataset.source_queries or [])
    for stage, scope in zip(stages, _stage_scopes(qb_dataset, datasets_by_name=state.datasets_by_name)):
        if scope.base is None:
            continue
        stage = scope.query or stage
        reason = _check_stage_for_whole_drop(
            stage=stage,
            base_name=scope.base,
            qb_name=qb_dataset.name,
            scope=scope,
            whole_dropped_datasets=whole_dropped_datasets,
            state=_stage_cascade_state(stage=stage, scope=scope, state=state),
            candidate_base_names=candidate_base_names,
        )
        if reason is not None:
            return reason
    return None


# Cascade walker + collapse


class _CascadeState:
    """Mutable state threaded through the cascade helpers; a plain class (not Pydantic) so dict/set fields keep reference identity for in-place mutation."""

    __slots__ = (
        "datasets_by_name",
        "edit_entries",
        "whole_entries",
        "dropped_cols",
        "dropped_metrics",
        "dropped_relationships",
        "pk_per_dataset",
        "vanished",
    )

    def __init__(
        self,
        *,
        datasets_by_name: dict[str, SlayerDataset],
        edit_entries: dict[str, EditDatasetDelete],
        whole_entries: dict[str, WholeDatasetDelete],
        dropped_cols: dict[str, set[str]],
        dropped_metrics: dict[str, set[str]],
        dropped_relationships: _DroppedRelationships,
        pk_per_dataset: dict[str, set[str]],
        vanished: dict[str, set[str]] | None = None,
    ) -> None:
        self.datasets_by_name = datasets_by_name
        self.edit_entries = edit_entries
        self.whole_entries = whole_entries
        self.dropped_cols = dropped_cols
        self.dropped_metrics = dropped_metrics
        self.dropped_relationships = dropped_relationships
        self.pk_per_dataset = pk_per_dataset
        #: Per dataset, the fields the base diffs dropped because the column they read is gone.
        self.vanished = vanished or {}

    def cascadable(self, name: str) -> set[str]:
        """Cascadable field drops on ``name`` (excludes PKs — rule 7)."""
        return self.dropped_cols.get(name, set()) - self.pk_per_dataset.get(name, set())


def _field_ref_targets_dropped(
    *,
    table_alias: str | None,
    ref_col: str,
    dataset: SlayerDataset,
    state: _CascadeState,
) -> tuple[bool, SlayerDataset | None]:
    """Whether one ``(table_alias, ref_col)`` ref resolves to a dropped field → ``(is_dropped, resolved_target_dataset)``."""
    if table_alias is None:
        dropped = state.cascadable(dataset.name)
        vanished = state.vanished.get(dataset.name, set())
        return any(c.name in dropped and c.reads_physical_column and c.physical_name == ref_col
                   and (c.is_physical_reader or c.name in vanished) for c in dataset.fields), dataset
    if table_alias == dataset.name:
        return ref_col in state.cascadable(dataset.name), dataset
    target = _walk_alias_to_target_dataset(
        source_dataset=dataset,
        table_alias=table_alias,
        datasets_by_name=state.datasets_by_name,
    )
    if target is None or target.data_source != dataset.data_source:
        return False, None
    return ref_col in state.cascadable(target.name), target


def _first_dropped_sql_field_ref(
    *, col: DatasetField, dataset: SlayerDataset, state: _CascadeState
) -> tuple[SlayerDataset, str] | None:
    """``(target_dataset, ref_col)`` for the first ref in ``col``'s derived sql or filter resolving to a dropped field,
    else None."""
    for sql in _field_definition_sqls(col):
        for table_alias, ref_col in _extract_field_refs_from_sql(sql):
            is_dropped, target = _field_ref_targets_dropped(
                table_alias=table_alias,
                ref_col=ref_col,
                dataset=dataset,
                state=state,
            )
            if is_dropped and target is not None:
                return target, ref_col
    return None


def _field_definition_sqls(col: DatasetField) -> list[str]:
    """The SQL ``col``'s definition reads: its derived definition, then its filter."""
    derived = None if col.is_base else col.definition_sql
    return [s for s in (derived, col.filter) if s is not None]


def _cascade_derived_fields(
    *, dataset: SlayerDataset, state: _CascadeState
) -> bool:
    """Rules 1 + 5: derived ``DatasetField.sql`` or ``DatasetField.filter`` referencing dropped fields (same dataset or via
    relationships)."""
    changed = False
    dropped_set = state.dropped_cols.get(dataset.name, set())
    for col in dataset.fields:
        if col.name in dropped_set:
            continue
        hit = _first_dropped_sql_field_ref(col=col, dataset=dataset, state=state)
        if hit is None:
            continue
        target, ref_col = hit
        ref_label = ref_col if target is dataset else f"{target.name}.{ref_col!r}"
        if _add_dropped_field(
            edit_entries=state.edit_entries,
            dropped_cols=state.dropped_cols,
            dataset=dataset,
            field_name=col.name,
            reason=f"Derived sql {' / '.join(_field_definition_sqls(col))!r} references dropped field {ref_label}",
        ):
            changed = True
    return changed


def _metric_drop_cause(
    *, ref: str, dataset: SlayerDataset, state: _CascadeState
) -> str | None:
    """Reason string if the metric ref resolves to a dropped field/metric, else None."""
    tgt_dataset, leaf = _resolve_dotted_ref_to_dataset(
        source_dataset=dataset,
        dotted_ref=ref,
        datasets_by_name=state.datasets_by_name,
    )
    if tgt_dataset is None or tgt_dataset.data_source != dataset.data_source:
        return None
    if leaf in state.cascadable(tgt_dataset.name):
        return f"references dropped field {tgt_dataset.name}.{leaf!r}"
    if leaf in state.dropped_metrics.get(tgt_dataset.name, set()):
        return f"references dropped metric {tgt_dataset.name}.{leaf!r}"
    return None


def _first_dropped_cause(
    *,
    refs: set[str],
    dataset: SlayerDataset,
    state: _CascadeState,
) -> str | None:
    """Cause string for the first ref resolving to a dropped field/metric, else None."""
    for ref in refs:
        cause = _metric_drop_cause(ref=ref, dataset=dataset, state=state)
        if cause is not None:
            return cause
    return None


def _cascade_metrics(*, dataset: SlayerDataset, state: _CascadeState) -> bool:
    """Rule 2: ``DatasetMetric.expression`` referencing a dropped field or metric."""
    changed = False
    dropped_set = state.dropped_metrics.get(dataset.name, set())
    for metric in dataset.metrics:
        if metric.name is None or metric.name in dropped_set:
            continue
        refs = _metric_expression_refs(metric.text)
        cause = _first_dropped_cause(refs=refs, dataset=dataset, state=state)
        if cause is None:
            continue
        if _add_dropped_metric(
            edit_entries=state.edit_entries,
            dropped_metrics=state.dropped_metrics,
            dataset=dataset,
            metric_name=metric.name,
            reason=f"Expression {metric.expression!r} {cause}",
        ):
            changed = True
    return changed


def _cascade_relationships(*, dataset: SlayerDataset, state: _CascadeState) -> bool:
    """Rules 3a + 3b: local FK field dropped here, or foreign field dropped on the target."""
    changed = False
    for relationship in dataset.relationships:
        if RelationshipEdgeRef.of(relationship) in state.dropped_relationships.get(dataset.name, []):
            continue
        # Raw dropped_cols: a dropped local PK key invalidates the relationship too.
        local_missing = [
            pair[0] for pair in relationship.column_pairs
            if pair[0] in state.dropped_cols.get(dataset.name, set())
        ]
        if local_missing:
            changed = _add_dropped_relationship(
                edit_entries=state.edit_entries,
                dropped_relationships=state.dropped_relationships,
                dataset=dataset,
                relationship=relationship,
                reason=f"Local FK field(s) {local_missing} dropped from this dataset",
            ) or changed
            continue
        tgt = state.datasets_by_name.get(relationship.to)
        if tgt is None or tgt.data_source != dataset.data_source:
            continue
        # Raw dropped_cols, not cascadable: a target PK drop still invalidates the relationship.
        foreign_missing = [
            pair[1] for pair in relationship.column_pairs
            if pair[1] in state.dropped_cols.get(tgt.name, set())
        ]
        if not foreign_missing:
            continue
        if _add_dropped_relationship(
            edit_entries=state.edit_entries,
            dropped_relationships=state.dropped_relationships,
            dataset=dataset,
            relationship=relationship,
            reason=(
                f"Foreign field(s) {foreign_missing} dropped on target "
                f"dataset {relationship.to!r}"
            ),
        ):
            changed = True
    return changed


def _cascade_filters(*, dataset: SlayerDataset, state: _CascadeState) -> bool:
    """Rule 4: dataset-level filter strings referencing dropped fields."""
    changed = False
    for filter_str in dataset.filters:
        entry = state.edit_entries.get(dataset.name)
        if entry is not None and filter_str in entry.remove_filters:
            continue
        for col_ref in _filter_refs(filter_str):
            tgt_dataset, leaf = _resolve_dotted_ref_to_dataset(
                source_dataset=dataset,
                dotted_ref=col_ref,
                datasets_by_name=state.datasets_by_name,
            )
            if (
                tgt_dataset is None
                or tgt_dataset.data_source != dataset.data_source
                or leaf not in state.cascadable(tgt_dataset.name)
            ):
                continue
            if _add_remove_filter(
                edit_entries=state.edit_entries,
                dataset=dataset,
                filter_text=filter_str,
                reason=f"Filter references dropped field {tgt_dataset.name}.{leaf!r}",
            ):
                changed = True
            break
    return changed


def _cascade_dataset(*, dataset: SlayerDataset, state: _CascadeState) -> bool:
    """Rules 1-5 on one dataset's fields, metrics, relationships and filters; True if anything new was added."""
    changed = _cascade_derived_fields(dataset=dataset, state=state)
    changed = _cascade_metrics(dataset=dataset, state=state) or changed
    changed = _cascade_relationships(dataset=dataset, state=state) or changed
    return _cascade_filters(dataset=dataset, state=state) or changed


def _cascade_query_backed(
    *, datasets: list[SlayerDataset], state: _CascadeState
) -> bool:
    """Rule 6: query-backed dataset whose source_queries transitively reference dropped state — whole-drop."""
    changed = False
    whole_dropped_names = set(state.whole_entries.keys())
    # Every dataset in the DS is a candidate base for cross-dataset dotted refs.
    candidate_base_names = set(state.datasets_by_name.keys())
    for dataset in datasets:
        if dataset.name in state.whole_entries or not dataset.source_queries:
            continue
        reason = _query_backed_should_whole_drop(
            qb_dataset=dataset,
            state=state,
            whole_dropped_datasets=whole_dropped_names,
            candidate_base_names=candidate_base_names,
        )
        if reason is None:
            continue
        state.whole_entries[dataset.name] = WholeDatasetDelete(
            dataset_name=dataset.name,
            data_source=dataset.data_source,
            reasons=[reason],
        )
        # Treat all fields as dropped so further rounds propagate transitively.
        state.dropped_cols[dataset.name] = {c.name for c in dataset.fields}
        changed = True
    return changed


def _cascade_one_pass(
    *,
    datasets: list[SlayerDataset],
    datasets_by_name: dict[str, SlayerDataset],
    edit_entries: dict[str, EditDatasetDelete],
    whole_entries: dict[str, WholeDatasetDelete],
    dropped_cols: dict[str, set[str]],
    dropped_metrics: dict[str, set[str]],
    dropped_relationships: _DroppedRelationships,
    pk_per_dataset: dict[str, set[str]],
    vanished: dict[str, set[str]],
) -> bool:
    """Run one cascade pass; True if anything new was added."""
    state = _CascadeState(
        datasets_by_name=datasets_by_name,
        edit_entries=edit_entries,
        whole_entries=whole_entries,
        dropped_cols=dropped_cols,
        dropped_metrics=dropped_metrics,
        dropped_relationships=dropped_relationships,
        pk_per_dataset=pk_per_dataset,
        vanished=vanished,
    )

    changed = False
    for dataset in datasets:
        if dataset.name not in whole_entries and _cascade_dataset(dataset=dataset, state=state):
            changed = True

    if _cascade_query_backed(datasets=datasets, state=state):
        changed = True

    return changed


def _removed_relationship_refs(*, dataset: SlayerDataset | None, remove: RemoveSpec) -> list[RelationshipEdgeRef]:
    """Exact references of every relationship ``remove`` drops; a reference-only entry resolves on ``dataset``."""
    refs = list(remove.relationship_edges)
    for ref in remove.relationships:
        relationships = [] if dataset is None else (
            [j for j in dataset.relationships if j.name == ref] or [j for j in dataset.relationships if j.to == ref]
        )
        refs.extend(r for r in map(RelationshipEdgeRef.of, relationships) if r not in refs)
    return refs


def _seed_one_diff_entry(
    *,
    dataset_name: str,
    dataset: SlayerDataset | None,
    entry: ToDeleteEntry | None,
    cols: set[str],
    edit_entries: dict[str, EditDatasetDelete],
    whole_entries: dict[str, WholeDatasetDelete],
    dropped_cols: dict[str, set[str]],
    dropped_metrics: dict[str, set[str]],
    dropped_relationships: _DroppedRelationships,
) -> None:
    """Apply one ``(entry, dropped_columns)`` diff to the cascade state dicts."""
    if isinstance(entry, WholeDatasetDelete):
        whole_entries[dataset_name] = entry
    elif isinstance(entry, EditDatasetDelete):
        edit_entries[dataset_name] = entry
        refs = _removed_relationship_refs(dataset=dataset, remove=entry.remove)
        if refs:
            dropped_relationships.setdefault(dataset_name, []).extend(refs)
        if entry.remove.metrics:
            dropped_metrics.setdefault(dataset_name, set()).update(
                entry.remove.metrics
            )
    if cols:
        dropped_cols.setdefault(dataset_name, set()).update(cols)


def _seed_state_from_diffs(
    *,
    diffs_iterables: tuple[
        dict[str, tuple[ToDeleteEntry | None, set[str]]], ...
    ],
    datasets_by_name: dict[str, SlayerDataset],
    edit_entries: dict[str, EditDatasetDelete],
    whole_entries: dict[str, WholeDatasetDelete],
    dropped_cols: dict[str, set[str]],
    dropped_metrics: dict[str, set[str]],
    dropped_relationships: _DroppedRelationships,
) -> None:
    """Populate the cascade state dicts from the base per-dataset diffs."""
    for diffs in diffs_iterables:
        for dataset_name, (entry, cols) in diffs.items():
            _seed_one_diff_entry(
                dataset_name=dataset_name,
                dataset=datasets_by_name.get(dataset_name),
                entry=entry,
                cols=cols,
                edit_entries=edit_entries,
                whole_entries=whole_entries,
                dropped_cols=dropped_cols,
                dropped_metrics=dropped_metrics,
                dropped_relationships=dropped_relationships,
            )


def _collapse_entries(
    *,
    edit_entries: dict[str, EditDatasetDelete],
    whole_entries: dict[str, WholeDatasetDelete],
) -> list[ToDeleteEntry]:
    """Collapse rule: whole-drop preempts edit on the same dataset; return name-sorted entries."""
    final: list[ToDeleteEntry] = []
    for name in sorted(set(edit_entries.keys()) | set(whole_entries.keys())):
        if name in whole_entries:
            final.append(whole_entries[name])
        else:
            final.append(edit_entries[name])
    return final


@old_keywords({"models": "datasets"})
def compute_datasource_drops(
    *,
    datasets: list[SlayerDataset],
    sql_table_diffs: dict[str, tuple[ToDeleteEntry | None, set[str]]],
    sql_diffs: dict[str, tuple[ToDeleteEntry | None, set[str]]],
) -> list[ToDeleteEntry]:
    """Combine per-dataset base diffs with cascade walking and the collapse rule; pure, caller must restrict ``datasets`` to one datasource (cascade doesn't cross datasources)."""
    edit_entries: dict[str, EditDatasetDelete] = {}
    whole_entries: dict[str, WholeDatasetDelete] = {}
    dropped_cols: dict[str, set[str]] = {}
    dropped_metrics: dict[str, set[str]] = {}
    dropped_relationships: _DroppedRelationships = {}
    datasets_by_name = {m.name: m for m in datasets}

    _seed_state_from_diffs(
        diffs_iterables=(sql_table_diffs, sql_diffs),
        datasets_by_name=datasets_by_name,
        edit_entries=edit_entries,
        whole_entries=whole_entries,
        dropped_cols=dropped_cols,
        dropped_metrics=dropped_metrics,
        dropped_relationships=dropped_relationships,
    )

    pk_per_dataset = {m.name: _pk_fields(m) for m in datasets}
    vanished = {name: {r.target.removeprefix("field:") for r in entry.reasons if r.target.startswith("field:")}
                for name, entry in edit_entries.items()}

    # Iterate to fixed point — safety bound, DAGs converge in <10 passes.
    for _ in range(100):
        if not _cascade_one_pass(
            datasets=datasets,
            datasets_by_name=datasets_by_name,
            edit_entries=edit_entries,
            whole_entries=whole_entries,
            dropped_cols=dropped_cols,
            dropped_metrics=dropped_metrics,
            dropped_relationships=dropped_relationships,
            pk_per_dataset=pk_per_dataset,
            vanished=vanished,
        ):
            break

    final = _collapse_entries(
        edit_entries=edit_entries, whole_entries=whole_entries
    )
    return final


def field_dependents(*, datasets: list[SlayerDataset], dataset: SlayerDataset, field: str) -> list[ToDeleteEntry]:
    """What deleting ``dataset.field`` would cascade to among ``datasets`` (one datasource); the field itself
    included."""
    seed = EditDatasetDelete(dataset_name=dataset.name, data_source=dataset.data_source, remove=RemoveSpec(fields=[field]))
    return compute_datasource_drops(datasets=datasets, sql_table_diffs={dataset.name: (seed, {field})}, sql_diffs={})


def reads_retyped_field(
    *, datasets: list[SlayerDataset], finding: RetypeFinding, read_fields: Set[tuple[str, str]],
) -> bool:
    """Whether ``read_fields`` reads ``finding``'s field, directly or through a field depending on it."""
    owner = next((m for m in datasets if m.name == finding.dataset_name), None)
    if owner is None:
        return (finding.dataset_name, finding.field) in read_fields
    for entry in field_dependents(datasets=datasets, dataset=owner, field=finding.field):
        if isinstance(entry, WholeDatasetDelete):
            if any(m == entry.dataset_name for m, _ in read_fields):
                return True
        elif any((entry.dataset_name, c) in read_fields for c in entry.remove.fields):
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
                f"validate_datasets: could not list schemas in datasource "
                f"{datasource.name!r}; skipping drift verdict to avoid false "
                f"deletions"
            ) from exc
        refs = []
    if refs or fallback_schema_tokens is None:
        return refs
    # Enumeration failed/empty: introspect exactly the datasets' schemas + the default.
    requested = set(fallback_schema_tokens)
    if datasource.schema_name:
        requested.add(datasource.schema_name)
    resolved = (await resolve_ingest_scope(
        client=client,
        requested=sorted(requested) or None,
        all_schemas=False,
        datasource_schema=datasource.schema_name,
    )).schemas
    # Add the connection default (bare datasets), deduped by token, preferring the default-marked ref.
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
            "validate_datasets: failed to introspect %r in datasource %r: %s",
            obj_name, datasource.name, exc,
        )
        return LiveTable(readable=False)


def _live_object_keys(*, ref: SchemaRef, obj_name: str, single: bool, dialect: SqlDialect) -> list[str]:
    """The live-map keys of one object; the default/single-schema scope is keyed both bare and qualified so legacy and explicit datasets resolve."""
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
                "validate_datasets: failed to list schema %r in datasource "
                "%r: %s", ref.token, datasource.name, exc,
            )
            failed.append(str(ref.token))
            continue
        listings.append((ref, [obj.live_name for obj in objs]))
    if failed:
        # Fail closed: a partial map would report every dataset in the unlisted
        # schema as a WholeDatasetDelete — data loss under --force-clean.
        raise IntrospectionUnavailable(
            f"validate_datasets: could not list schema(s) {failed} in datasource "
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
    """``{object_name: LiveTable}`` for every live table AND view in the DS; views are included so a dataset over a view isn't falsely reported as a WholeDatasetDelete."""
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
    # Enumeration may fall back to the datasets' own schemas, so refs cover only these.
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
        columns={c.name: c.narrowed_type for c in cols},
        db_types={c.name: c.type_name for c in cols if c.type_name},
        key_sets=await _live_key_sets(client=client, table_name=table_name, ref=ref),
    )


async def _live_key_sets(*, client: SqlClient, table_name: str, ref: SchemaRef | None) -> list[list[str]] | None:
    """The table's enforced key sets by live column name; ``None`` when unread or the database enforces none."""
    if not dialect_for_ds_type(client.datasource.type).enforces_unique_keys:
        return None
    keys = await reflect_table_keys(client=client, table_name=table_name, ref=ref)
    if keys.failed_reads:
        return None
    return ([keys.primary_key] if keys.primary_key else []) + [list(k) for k in keys.unique_key_sets]


async def _live_columns_for_sql_dataset(
    *,
    dataset: SlayerDataset,
    client: SqlClient,
) -> ingestion.SqlResultTypes | None:
    """Trial-execute ``dataset.sql`` with a 0-row guard; return its result types, or None on failure."""
    if not dataset.sql:
        return None
    try:
        return await ingestion.sql_result_types(sql=dataset.sql, client=client)
    except Exception as exc:
        logger.info(
            "validate_datasets: trial-execute on %r failed: %s",
            dataset.name,
            exc,
        )
        return None


def _has_star_projection(parsed: exp.Expr) -> bool:
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


def _add_using_relationship_fields(
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
    from_expr = args.get("from_")
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
    field: exp.Column,
    scope: Scope,
    tables_by_scope: dict[int, dict[str, exp.Table]],
    refs: SourceRefs,
) -> bool:
    """Attribute ``qual.col`` in the nearest enclosing scope defining the qualifier; True ⇒ derived/CTE/unknown qualifier (not attributable)."""
    s = scope
    while s is not None:
        target = (tables_by_scope.get(id(s)) or {}).get(qualifier)
        if target is not None:
            refs[target].add(field)
            return False
        if qualifier in s.sources:  # CTE / derived-table alias
            return True
        s = s.parent
    return True


def _attribute_bare_ref(
    *,
    field: exp.Column,
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
    refs[next(iter(local.values()))].add(field)
    return False


def _attribute_scope_fields(
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
                qualifier=col.table, field=column, scope=scope,
                tables_by_scope=tables_by_scope, refs=refs,
            )
        else:
            verdict = _attribute_bare_ref(
                field=column, scope=scope,
                tables_by_scope=tables_by_scope, refs=refs,
            )
        if verdict is None:
            return None
        derived = derived or verdict
    return derived


def _sql_dataset_source_refs(*, sql: str, dialect: str) -> SourceRefs | None:
    """Map each source table of a sql-mode dataset to the columns it references (incl. JOIN USING keys); None when unparseable, a bare column isn't attributable, or a CTE/derived ref coexists with ``*``."""
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
        if _add_using_relationship_fields(scope=scope, local_tables=local, refs=refs):
            has_derived_ref = True
    seen: set[int] = set()
    for scope in scopes:  # innermost first — a bare column binds innermost
        derived = _attribute_scope_fields(
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
    *, dataset: SlayerDataset, client: SqlClient
) -> bool:
    """Whether every table AND column in ``dataset.sql`` accepts a 0-row probe; True ⇒ the SQL itself is broken (not drift), any probe failure keeps the drift classification."""
    dialect = dialect_for_ds_type(client.datasource.type)
    refs = _sql_dataset_source_refs(sql=dataset.sql or "", dialect=dialect.sqlglot_name)
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
    """Look up a dataset's ``sql_table`` in the live map."""
    for name in _sql_table_candidates(sql_table, dialect=dialect):
        live = live_tables.get(name)
        if live is not None:
            return live
    return None


async def _probe_validate_datasets_field(
    *, client: SqlClient, dataset: SlayerDataset, col: DatasetField, physical: str, table_name: str,
    schema_name: str | None,
) -> DataType | None:
    """Run the affinity probe for one field in a validate_datasets pass."""
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
            "validate_datasets probe raised for %s.%s; ignoring: %s",
            dataset.name, col.name, exc,
        )
        return None


async def _sqlite_probe_int_drift_for_dataset(
    *,
    dataset: SlayerDataset,
    client: SqlClient,
    live_tables: dict[str, LiveTable],
    default_schema: str | None = None,
    dialect: SqlDialect,
) -> list[RetypeFinding]:
    """A re-type finding for each INT base field, live INT too, SQLite's storage probe widens to DOUBLE/TEXT;
    ``default_schema`` for unqualified tables, failures skip."""
    if dataset.sql_table is None:
        return []
    live = _resolve_live_table(sql_table=dataset.sql_table, live_tables=live_tables, dialect=dialect)
    if live is None or not live.readable:
        return []

    location = sql_table_location(dataset.sql_table, dialect=dialect)
    schema_name, table_name = location.schema_name or default_schema or None, location.name

    drifts: list[RetypeFinding] = []
    for col in dataset.fields:
        if col.datatype is not DataType.INTEGER or not col.reads_physical_column:
            continue
        physical = col.physical_name
        if live.columns.get(physical) is not DataType.INTEGER:
            continue
        verdict = await _probe_validate_datasets_field(
            client=client, dataset=dataset, col=col, physical=physical,
            table_name=table_name, schema_name=schema_name,
        )
        if verdict is None or verdict is DataType.INTEGER:
            continue
        drifts.append(RetypeFinding.of(dataset=dataset, field=col, live_type=verdict, live_db_type=None,
                                       remedy_kind="reingest"))
    return drifts


async def _sqlite_probe_drifts_for_datasets(
    *,
    datasource: DatasourceConfig,
    client: SqlClient,
    sql_table_datasets: list[SlayerDataset],
    live_tables: dict[str, LiveTable],
) -> list[RetypeFinding]:
    """Run the SQLite probe for every dataset through the datasource's client."""
    if not dialect_for_ds_type(datasource.type).probes_integer_affinity:
        return []
    default_schema = datasource.schema_name or None
    dialect = dialect_for_ds_type(datasource.type)
    return [
        f for m in sql_table_datasets
        for f in await _sqlite_probe_int_drift_for_dataset(
            dataset=m, client=client, live_tables=live_tables, default_schema=default_schema, dialect=dialect)
    ]


def _diff_one_sql_table_dataset(
    *,
    dataset: SlayerDataset,
    live_tables: dict[str, "LiveTable"],
    available_in_ds: set[str],
    dialect: SqlDialect,
) -> tuple[ToDeleteEntry | None, set[str]]:
    """Per-dataset body of ``_collect_sql_table_diffs``: resolve the live table and diff."""
    live = _resolve_live_table(
        sql_table=dataset.sql_table or "", live_tables=live_tables, dialect=dialect,
    )
    if live is not None and not live.readable:
        return None, set()
    return diff_sql_table_dataset(
        dataset=dataset,
        live_table=live,
        available_datasets_in_ds=available_in_ds,
        enforces_keys=dialect.enforces_unique_keys,
    )


async def _dataset_schema_tokens(datasets: list[SlayerDataset], *, client: SqlClient) -> set[str]:
    """The schema tokens the datasets' ``sql_table``s name, each schema keeping its quote flag; a schema in a
    catalog this connection cannot reach is left out, as enumeration leaves it out."""
    dialect = dialect_for_ds_type(client.datasource.type)
    tokens: set[str] = set()
    for dataset in datasets:
        at = sql_table_location(dataset.sql_table or "", dialect=dialect)
        try:
            ref = await table_schema_ref(catalog=at.catalog, schema=at.schema_name, client=client)
        except ValueError:
            continue
        if ref is not None and ref.token is not None:
            tokens.add(ref.token)
    return tokens


async def _collect_sql_table_diffs(
    *,
    datasource: DatasourceConfig,
    client: SqlClient,
    sql_table_datasets: list[SlayerDataset],
    available_in_ds: set[str],
    snapshot: DriftSnapshot | None = None,
    findings: list[RetypeFinding | UntypedFinding] | None = None,
) -> dict[str, tuple[ToDeleteEntry | None, set[str]]]:
    """Introspect live and diff each sql_table-mode dataset; re-type findings (SQLite's affinity probe's
    included) append to ``findings``."""
    if not sql_table_datasets:
        return {}
    # Cover every own-catalog schema (a dataset may be qualified to another own
    # schema); fall back to the datasets' own schemas if enumeration is unavailable.
    fallback_tokens = await _dataset_schema_tokens(sql_table_datasets, client=client)
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
                sql_tables={m.sql_table for m in sql_table_datasets if m.sql_table},
                fallback_schema_tokens=fallback_tokens,
            )
    except IntrospectionUnavailable as exc:
        # Unknown live schema: don't report deletions (a transient error would
        # hand --force-clean a whole tenant).
        logger.warning("validate_datasets: skipping drift verdict: %s", exc)
        return {}
    dialect = dialect_for_ds_type(datasource.type)
    temporal_text = dialect.temporal_text_compatible
    if findings is not None:
        for m in sql_table_datasets:
            live = _resolve_live_table(sql_table=m.sql_table or "", live_tables=live_tables, dialect=dialect)
            findings.extend(retype_findings(
                dataset=m, live_table=live if live is not None and live.readable else None, temporal_text=temporal_text,
            ))
        findings.extend(await _sqlite_probe_drifts_for_datasets(
            datasource=datasource, client=client, sql_table_datasets=sql_table_datasets, live_tables=live_tables,
        ))
    return {
        m.name: _diff_one_sql_table_dataset(dataset=m, live_tables=live_tables, available_in_ds=available_in_ds,
                                          dialect=dialect)
        for m in sql_table_datasets
    }


async def _sql_trials(
    *,
    datasource: DatasourceConfig,
    client: SqlClient,
    sql_datasets: list[SlayerDataset],
    snapshot: DriftSnapshot | None,
) -> dict[str, _SqlTrial]:
    """Trial results keyed by SQL text: one trial per distinct text not already in ``snapshot``."""

    async def _invalid_sql(dataset: SlayerDataset, live_cols: ingestion.SqlResultTypes | None) -> bool:
        return live_cols is None and await _source_tables_resolve(dataset=dataset, client=client)

    trials: dict[str, _SqlTrial] = dict(snapshot.sql_trials) if snapshot is not None else {}
    pending = {m.sql or "": m for m in sql_datasets if (m.sql or "") not in trials}
    live = await asyncio.gather(
        *(_live_columns_for_sql_dataset(dataset=m, client=client) for m in pending.values())
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
    sql_datasets: list[SlayerDataset],
    client: SqlClient,
    snapshot: DriftSnapshot | None = None,
    findings: list[RetypeFinding | UntypedFinding] | None = None,
) -> dict[str, tuple[ToDeleteEntry | None, set[str]]]:
    """Trial-execute each sql-mode dataset concurrently and produce its diff; re-type findings append to
    ``findings``."""
    out: dict[str, tuple[ToDeleteEntry | None, set[str]]] = {}
    if not sql_datasets:
        return out
    trials = await _sql_trials(
        datasource=datasource, client=client, sql_datasets=sql_datasets, snapshot=snapshot,
    )
    # A zero-row trial carries column types only where the cursor reports them.
    compare_types = dialect_for_ds_type(datasource.type).cursor_reports_types
    for dataset in sql_datasets:
        trial = trials[dataset.sql or ""]
        out[dataset.name] = diff_sql_dataset(
            dataset=dataset, live_columns=trial.live_columns, invalid_sql=trial.invalid_sql,
        )
        if findings is not None and compare_types:
            findings.extend(sql_dataset_retype_findings(
                dataset=dataset, live_columns=trial.live_columns,
                temporal_text=dialect_for_ds_type(datasource.type).temporal_text_compatible,
            ))
    return out


@probes.probe(
    name="ingest.drift",
    extract=lambda bound: {"datasource.name": bound.arguments["datasource"].name},
    result=lambda entries: {"ingest.drift.entries": len(entries)},
)
async def validate_datasource(
    *,
    datasource: DatasourceConfig,
    datasets: list[SlayerDataset],
    client: SqlClient,
    available_in_ds: set[str] | None = None,
    snapshot: DriftSnapshot | None = None,
    findings_out: list[RetypeFinding | UntypedFinding] | None = None,
) -> list[ToDeleteEntry]:
    """Validate ``datasets`` (all one DS) against the live schema through ``client``. Read-only.

    ``available_in_ds`` names every dataset of the DS (default: ``datasets``); a
    ``snapshot`` scopes introspection to ``datasets``' objects and reuses its facts;
    ``findings_out`` collects the report-only re-type findings.
    """
    if not datasets:
        return []
    if snapshot is not None:
        snapshot.raise_if_unreachable()

    if available_in_ds is None:
        available_in_ds = {m.name for m in datasets}
    sql_table_diffs = await _collect_sql_table_diffs(
        datasource=datasource,
        client=client,
        sql_table_datasets=[m for m in datasets if m.sql_table],
        available_in_ds=available_in_ds,
        snapshot=snapshot,
        findings=findings_out,
    )
    sql_diffs = await _collect_sql_diffs(
        datasource=datasource,
        sql_datasets=[m for m in datasets if m.sql],
        client=client,
        snapshot=snapshot,
        findings=findings_out,
    )
    return list(compute_datasource_drops(
        datasets=datasets,
        sql_table_diffs=sql_table_diffs,
        sql_diffs=sql_diffs,
    ))


if TYPE_CHECKING:
    @deprecated("EditModelDelete is deprecated; use EditDatasetDelete", category=None)
    class EditModelDelete(EditDatasetDelete): ...

    @deprecated("ModelAddition is deprecated; use DatasetAddition", category=None)
    class ModelAddition(DatasetAddition): ...

    @deprecated("WholeModelDelete is deprecated; use WholeDatasetDelete", category=None)
    class WholeModelDelete(WholeDatasetDelete): ...


__getattr__ = name_twins(namespace=globals(), renames={
    "EditModelDelete": "EditDatasetDelete", "ModelAddition": "DatasetAddition", "WholeModelDelete": "WholeDatasetDelete",
})
