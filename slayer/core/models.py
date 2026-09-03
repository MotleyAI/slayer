"""Core domain models for SLayer."""

import logging
import os
import re
from typing import Annotated, Any, Optional

from pydantic import BaseModel, BeforeValidator, Field, field_validator, model_validator
from sqlalchemy.engine import URL as _SA_URL

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    DEFAULT_AGGREGATIONS_BY_TYPE,
    DataType,
    JoinCardinality,
    JoinType,
    ObjectKind,
    PRIMARY_KEY_AGGREGATIONS,
    _coerce_legacy_datatype,
)
from slayer.core.format import NumberFormat
from slayer.core.formula import ALL_TRANSFORMS
from slayer.core.keys import SCALAR_FUNCTIONS
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function
from slayer.storage.migrations import migrate as _migrate_schema

_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

logger = logging.getLogger(__name__)

# Split an embedded port out of the host field for ``URL.create``.
# ``[<ipv6>]`` or ``[<ipv6>]:<port>`` — bracketed IPv6, optional port.
_BRACKETED_HOST_RE = re.compile(r"^\[(.+)\](?::(\d+))?$")
# ``<host>:<numeric-port>`` — single-colon numeric-tail host only.
_HOST_EMBEDDED_PORT_RE = re.compile(r"^([^:\[]+):(\d+)$")


class _SubstringRule:
    """A forbidden substring inside a name, paired with its rejection rationale."""

    __slots__ = ("substring", "reason")

    def __init__(self, *, substring: str, reason: str) -> None:
        self.substring = substring
        self.reason = reason

    def check(self, name: str, context: str) -> None:
        if self.substring in name:
            raise ValueError(
                f"{context} '{name}' must not contain "
                f"{self.substring!r}; {self.reason}"
            )


# ``__`` is allowed in names (DEV-1743); only the internal ``__slayer_`` prefix
# is reserved, so user input cannot spoof colon-agg preprocessor identifiers.
_RESERVED_NAME_PREFIX = "__slayer_"


def _reject_reserved_prefix(name: str, label: str) -> None:
    """Reject the SLayer-internal ``__slayer_`` name prefix (P3)."""
    if name.startswith(_RESERVED_NAME_PREFIX):
        raise ValueError(
            f"{label} '{name}' must not start with {_RESERVED_NAME_PREFIX!r}; "
            f"that prefix is reserved for SLayer-internal identifiers."
        )


_NO_DOT = _SubstringRule(
    substring=".",
    reason="dots are the canonical-id namespace delimiter "
           "(``<ds>.<model>.<leaf>``) and the dotted-path reference "
           "syntax in queries.",
)
_NO_COLON = _SubstringRule(
    substring=":",
    reason="colons are reserved as the aggregation separator "
           "(``revenue:sum``) and the ``memory:<int>`` canonical-id "
           "prefix.",
)
_NO_FWD_SLASH = _SubstringRule(
    substring="/",
    reason="path separators break the storage layout.",
)
_NO_BACK_SLASH = _SubstringRule(
    substring="\\",
    reason="path separators break the storage layout.",
)
_NO_NUL = _SubstringRule(
    substring="\x00",
    reason="NUL bytes are filesystem-unsafe.",
)


def _require_non_empty_trimmed(v: str, context: str) -> None:
    """Reject empty/whitespace-only inputs and leading/trailing whitespace."""
    if not v or not v.strip():
        raise ValueError(
            f"{context} must be a non-empty string; got {v!r}."
        )
    if v.strip() != v:
        raise ValueError(
            f"{context} must not have leading/trailing whitespace; "
            f"got {v!r}."
        )


def _validate_model_name(name: str, context: str) -> str:
    """Reject model/query names containing ``.``/``:`` or the ``__slayer_`` prefix."""
    label = f"{context} name"
    _NO_DOT.check(name=name, context=label)
    _NO_COLON.check(name=name, context=label)
    _reject_reserved_prefix(name, label)
    return name


_DUNDER_RUN_RE = re.compile(r"_{2,}")


def sanitize_model_name(name: str) -> str:
    """Collapse runs of 2+ underscores to one (regex handles overlaps; the fallback match key for re-ingest)."""
    return _DUNDER_RUN_RE.sub("_", name)


def _validate_column_name(name: str, context: str) -> str:
    """Reject dimension/measure names containing ``.`` or ``:`` (``__`` allowed for flattened join paths)."""
    label = f"{context} name"
    _NO_DOT.check(name=name, context=label)
    _NO_COLON.check(name=name, context=label)
    _reject_reserved_prefix(name, label)
    return name


class Column(BaseModel):
    """A row-level column, usable per-query as a GROUP BY dimension or an aggregation measure."""
    name: str
    sql: str | None = None
    type: DataType = DataType.TEXT
    db_type: str | None = Field(
        default=None,
        description=(
            "Raw database type string (e.g. 'point', 'jsonb'), retained when "
            "the declared DataType loses information. Populated by ingestion "
            "for UNKNOWN (opaque) columns; None for mapped types, where the "
            "declared DataType already carries everything we need."
        ),
    )
    primary_key: bool = False
    unique: bool = False  # single-column uniqueness (non-PK); primary_key implies it
    description: str | None = None
    label: str | None = None
    hidden: bool = False
    format: NumberFormat | None = None
    allowed_aggregations: list[str] | None = None
    filter: str | None = None  # Applied inside CASE WHEN at aggregation time only
    meta: dict[str, Any] | None = None
    sampled: str | None = None  # DEV-1375: cached sample-value snapshot
    sampled_values: list[str] | None = None  # DEV-1480: structured top-N
    distinct_count: int | None = None  # DEV-1480: true cardinality at profile time

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_type(cls, data: Any) -> Any:
        # Absorb legacy lowercase ``type`` strings; drop pseudo-types to None.
        if isinstance(data, dict) and "type" in data:
            mapped = _coerce_legacy_datatype(data["type"])
            if mapped is None:
                data = {k: v for k, v in data.items() if k != "type"}
            elif mapped is not data["type"]:
                data = {**data, "type": mapped}
        return data

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_column_name(v, "Column")

    @field_validator("filter")
    @classmethod
    def _validate_filter_predicate(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            # SQL-mode: validate at construction so DSL constructs raise early.
            parse_sql_predicate(v)
        return v


class ModelMeasure(BaseModel):
    """A named formula evaluating to an aggregated value (grammar: ``slayer/core/formula.py``)."""
    formula: str
    name: str | None = None
    label: str | None = None
    description: str | None = None
    type: DataType | None = None
    meta: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_type(cls, data: Any) -> Any:
        # ``type`` declares the formula's result type; legacy strings mapped, pseudo-types dropped.
        if isinstance(data, dict) and "type" in data:
            mapped = _coerce_legacy_datatype(data["type"])
            if mapped is None:
                data = {k: v for k, v in data.items() if k != "type"}
            elif mapped is not data["type"]:
                data = {**data, "type": mapped}
        return data

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _NAME_PATTERN.match(v):
            raise ValueError(
                f"Invalid name '{v}': must contain only letters, digits, "
                f"and underscores, and start with a letter or underscore"
            )
        if v is not None:
            _reject_reserved_prefix(v, "ModelMeasure name")
        return v

    @field_validator("name")
    @classmethod
    def _reject_transform_shadowing(cls, v: str | None) -> str | None:
        """Reject names that would shadow a built-in transform (``cumsum`` etc.)."""
        if v is None:
            return v
        if v in ALL_TRANSFORMS:
            raise ValueError(
                f"ModelMeasure name '{v}' is a reserved transform name. "
                f"Reserved: {', '.join(sorted(ALL_TRANSFORMS))}"
            )
        return v

    @field_validator("formula")
    @classmethod
    def _reject_raw_window_function(cls, v: str) -> str:
        """Reject raw ``OVER (...)`` window SQL — unparseable by the formula grammar."""
        if has_window_function(v):
            raise ValueError(f"ModelMeasure formula '{v}' {WINDOW_IN_FILTER_ERROR}")
        return v

    # Formulas may contain ``__`` (flattened virtual-model join paths); typos are
    # caught by strict resolution at binding time.


class AggregationParam(BaseModel):
    """A named parameter for an aggregation formula."""
    name: str
    sql: str  # default value — column name or SQL expression


class Aggregation(BaseModel):
    """A named aggregation overriding a built-in (``formula`` optional) or fully custom (``formula`` required)."""
    name: str
    formula: str | None = None  # SQL template; None = use built-in formula
    params: list[AggregationParam] = Field(default_factory=list)
    description: str | None = None
    meta: dict[str, Any] | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        # Same identifier rule as Column.name; a dotted name would misclassify in the pg-facade catalog.
        if not _NAME_PATTERN.match(v):
            raise ValueError(
                f"Invalid name '{v}': must contain only letters, digits, "
                f"and underscores, and start with a letter or underscore"
            )
        return v

    @model_validator(mode="after")
    def _require_formula_for_custom(self) -> "Aggregation":
        if self.name not in BUILTIN_AGGREGATIONS and self.formula is None:
            raise ValueError(
                f"Aggregation '{self.name}' is not a built-in aggregation; "
                f"a 'formula' is required. Built-in aggregations: "
                f"{', '.join(sorted(BUILTIN_AGGREGATIONS))}"
            )
        return self

    @model_validator(mode="after")
    def _reject_transform_names(self) -> "Aggregation":
        # Transform-only names are forbidden to avoid ambiguity with transform detection.
        transform_only = ALL_TRANSFORMS - BUILTIN_AGGREGATIONS
        if self.name in transform_only:
            raise ValueError(
                f"Aggregation name '{self.name}' conflicts with a built-in "
                f"transform function. Reserved names: "
                f"{', '.join(sorted(transform_only))}"
            )
        # DEV-1826: scalar-function names would shadow the scalar in functional
        # form (``round(x)`` must stay the scalar call), so every legal
        # aggregation stays reachable as ``agg(col)``. Case-insensitive to
        # match the parser's scalar dispatch.
        if self.name.lower() in SCALAR_FUNCTIONS:
            raise ValueError(
                f"Aggregation name '{self.name}' conflicts with a scalar "
                f"function. Scalar-allowlist names are reserved: "
                f"{', '.join(sorted(SCALAR_FUNCTIONS))}"
            )
        return self


def _coerce_source_queries(v: Any) -> Any:
    """Parse source_queries dicts → SlayerQuery (lazy import breaks a cycle; raises ValueError for Pydantic)."""
    if v is None:
        return v
    if not isinstance(v, list):
        raise ValueError(f"source_queries must be a list, got {type(v).__name__}")
    from slayer.core.query import SlayerQuery  # ALLOW(import-not-top): circular — slayer.core.query imports SlayerModel/ModelMeasure from this module
    result = []
    for i, item in enumerate(v):
        if isinstance(item, SlayerQuery):
            result.append(item)
        elif isinstance(item, dict):
            result.append(SlayerQuery.model_validate(item))
        else:
            raise ValueError(
                f"source_queries[{i}] must be a SlayerQuery or dict, "
                f"got {type(item).__name__}"
            )
    return result


class SourceModelOrigin(BaseModel):
    """In-memory lineage breadcrumb for virtual stage models: a linked list of
    upstream names so outer-stage dotted-ref lookup can strip prefixes.

    ``agg_column_names`` marks columns safe for the cross-stage intercept to
    re-aggregate, so a dimension that merely looks like an aggregation canonical
    isn't silently re-summed.
    """
    name: str
    data_source: str | None = None
    parent: Optional["SourceModelOrigin"] = None
    agg_column_names: frozenset[str] = Field(default_factory=frozenset)


class ModelJoin(BaseModel):
    """A join relationship to another model."""
    target_model: str                               # Name of the joined model
    join_pairs: list[list[str]] = Field(...)        # [["source_dim", "target_dim"], ...]
    join_type: JoinType = JoinType.LEFT             # LEFT (default) or INNER
    # Join arity, read source->target; None = undetermined.
    cardinality: JoinCardinality | None = None
    # Optional human/agent metadata; additive, so no schema-version bump needed.
    description: str | None = None
    meta: dict[str, Any] | None = None

    @field_validator("join_pairs")
    @classmethod
    def _validate_join_pairs(cls, v: list[list[str]]) -> list[list[str]]:
        if not v:
            raise ValueError("join_pairs must be non-empty")
        for i, pair in enumerate(v):
            if len(pair) != 2 or not all(isinstance(s, str) and s for s in pair):
                raise ValueError(
                    f"join_pairs[{i}] must be [source_dim, target_dim] with non-empty strings, got {pair}"
                )
        return v


def _check_column_measure_namespace(
    *, model_name: str, columns: list, measures: list
) -> None:
    """Columns and measures share one namespace: unique within each, disjoint across."""
    col_names_seq = [c.name for c in columns]
    col_dupes = sorted({n for n in col_names_seq if col_names_seq.count(n) > 1})
    if col_dupes:
        raise ValueError(
            f"Model '{model_name}': duplicate column names: {col_dupes}. "
            f"Each column name must be unique within a model."
        )
    unnamed = [m.formula for m in measures if m.name is None]
    if unnamed:
        raise ValueError(
            f"Model '{model_name}': every ModelMeasure in 'measures' must "
            f"have a name. Unnamed formulas: {unnamed}."
        )
    measure_names_seq = [m.name for m in measures if m.name is not None]
    measure_dupes = sorted(
        {n for n in measure_names_seq if measure_names_seq.count(n) > 1}
    )
    if measure_dupes:
        raise ValueError(
            f"Model '{model_name}': duplicate measure names: {measure_dupes}. "
            f"Each named ModelMeasure must have a unique name within a model."
        )
    overlap = sorted(set(col_names_seq) & set(measure_names_seq))
    if overlap:
        raise ValueError(
            f"Model '{model_name}': name collision between columns and "
            f"measures: {overlap}. Each name must be unique within a model "
            f"(columns and measures share a namespace)."
        )


class SlayerModel(BaseModel):
    version: int = 9  # DEV-1743: v9 = ``__`` ban lift + legacy-alias load rewrite
    name: str
    sql_table: str | None = None
    # Kind of DB object ``sql_table`` names; only auto-ingestion sets it. ``None`` = unknown.
    source_kind: Optional[ObjectKind] = None
    sql: str | None = None
    source_queries: Annotated[
        list | None, BeforeValidator(_coerce_source_queries)
    ] = None  # List of SlayerQuery — query-backed source mode
    query_variables: dict[str, Any] = Field(default_factory=dict)
    backing_query_sql: str | None = None
    data_source: str = ""
    columns: list[Column] = Field(default_factory=list)
    measures: list[ModelMeasure] = Field(default_factory=list)
    aggregations: list[Aggregation] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="SlayerModel", data=data)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_model_name(v, "Model")

    @field_validator("data_source")
    @classmethod
    def _validate_data_source_format(cls, v: str) -> str:
        # Format-only checks; emptiness is enforced below so query-backed models
        # can be constructed before their cache populator fills data_source.
        if not v:
            return v
        if v.strip() != v:
            raise ValueError(
                f"Model 'data_source' must not have leading/trailing "
                f"whitespace; got {v!r}."
            )
        label = "Model 'data_source'"
        _NO_NUL.check(name=v, context=label)
        _NO_FWD_SLASH.check(name=v, context=label)
        _NO_BACK_SLASH.check(name=v, context=label)
        _NO_DOT.check(name=v, context=label)
        _NO_COLON.check(name=v, context=label)
        return v

    @model_validator(mode="after")
    def _require_data_source_unless_query_backed(self) -> "SlayerModel":
        # Table-backed models need data_source up front (storage key); query-backed
        # ones may start empty — the engine fills it in before persisting.
        if not self.data_source.strip() and not self.source_queries:
            raise ValueError(
                f"Model '{self.name}': 'data_source' must be a non-empty "
                f"string. Set it to the name of the DatasourceConfig the "
                f"model belongs to."
            )
        return self
    joins: list[ModelJoin] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)  # Model-level filters (always applied)
    default_time_dimension: str | None = None
    description: str | None = None
    hidden: bool = False
    meta: dict[str, Any] | None = None
    # In-memory breadcrumb for virtual stage models; ``exclude=True`` keeps it unpersisted.
    source_model_origin: SourceModelOrigin | None = Field(default=None, exclude=True)

    @field_validator("filters")
    @classmethod
    def _validate_filter_predicates(cls, v: list[str]) -> list[str]:
        """Validate each model filter as a SQL-mode predicate at construction time."""
        for f in v:
            parse_sql_predicate(f)
        return v

    @model_validator(mode="after")
    def _validate_column_measure_disjoint(self) -> "SlayerModel":
        """Columns and measures share one namespace (see ``_check_column_measure_namespace``)."""
        _check_column_measure_namespace(
            model_name=self.name, columns=self.columns, measures=self.measures
        )
        return self

    @model_validator(mode="after")
    def _reject_self_joins(self) -> "SlayerModel":
        """Joins resolve by target-model name, so a self-join is unaddressable."""
        if any(j.target_model == self.name for j in self.joins):
            raise ValueError(
                f"Model '{self.name}': join target_model '{self.name}' is the "
                f"model itself. Self-joins are not supported — dotted "
                f"references resolve by model name, so a self-join can never "
                f"be addressed from a query. Define the second role as a "
                f"separate model over the same table (or a view) and join to "
                f"that."
            )
        return self

    @model_validator(mode="after")
    def _validate_allowed_aggregations(self) -> "SlayerModel":
        """Enforce that ``Column.allowed_aggregations`` is a subset of the type/PK eligibility set."""
        custom_agg_names = {a.name for a in self.aggregations}
        valid_names = BUILTIN_AGGREGATIONS | custom_agg_names
        for c in self.columns:
            if c.allowed_aggregations is None:
                continue
            if c.type.is_opaque:
                db_type_note = f" (db_type={c.db_type!r})" if c.db_type else ""
                raise ValueError(
                    f"Column '{c.name}'{db_type_note}: allowed_aggregations "
                    f"cannot be declared on a column of type {c.type} — "
                    f"aggregations are not supported for that type. Remove "
                    f"allowed_aggregations, or give the column an operable type."
                )
            for agg_name in c.allowed_aggregations:
                if agg_name not in valid_names:
                    raise ValueError(
                        f"Column '{c.name}': allowed_aggregations contains "
                        f"'{agg_name}', which is not a built-in aggregation "
                        f"or defined in this model's aggregations. "
                        f"Valid: {sorted(valid_names)}"
                    )
                if c.primary_key:
                    if agg_name not in PRIMARY_KEY_AGGREGATIONS:
                        raise ValueError(
                            f"Column '{c.name}': '{agg_name}' is not allowed "
                            f"on a primary-key column. PK columns can only be "
                            f"aggregated with {sorted(PRIMARY_KEY_AGGREGATIONS)}."
                        )
                    continue
                if agg_name in custom_agg_names and agg_name not in BUILTIN_AGGREGATIONS:
                    # Custom aggregations bypass type-default eligibility; the formula decides.
                    continue
                allowed_for_type = DEFAULT_AGGREGATIONS_BY_TYPE.get(
                    c.type, frozenset()
                )
                if agg_name not in allowed_for_type:
                    raise ValueError(
                        f"Column '{c.name}': aggregation '{agg_name}' is not "
                        f"applicable to {c.type} columns. allowed_aggregations "
                        f"must be a subset of the type-default set "
                        f"{sorted(allowed_for_type)} (plus any custom "
                        f"aggregations defined on this model)."
                    )
        return self

    @model_validator(mode="after")
    def _validate_source_mode_exclusivity(self) -> "SlayerModel":
        """Exactly one of sql_table, sql, source_queries must be populated."""
        if self.source_queries is not None and len(self.source_queries) == 0:
            raise ValueError(
                f"Model '{self.name}': source_queries cannot be an empty list. "
                f"Provide one or more stages, or omit the field entirely."
            )
        populated = []
        if self.sql_table:
            populated.append("sql_table")
        if self.sql:
            populated.append("sql")
        if self.source_queries:
            populated.append("source_queries")
        if len(populated) == 0:
            raise ValueError(
                f"Model '{self.name}' must specify exactly one source: "
                f"sql_table, sql, or source_queries (none specified)."
            )
        if len(populated) > 1:
            raise ValueError(
                f"Model '{self.name}' must specify exactly one source: "
                f"sql_table, sql, or source_queries (got: {populated})."
            )
        return self

    # NOSONAR S3516 — Pydantic v2 @model_validator(mode="after") is required to
    # return ``self``; the rule's "always returns same value" warning doesn't
    # apply to validator methods.
    @model_validator(mode="after")
    def _validate_source_query_stages(self) -> "SlayerModel":
        """Non-final stages must be named; all stage names must be unique."""
        if not self.source_queries:
            return self
        stages = self.source_queries
        if len(stages) > 1:
            for i, stage in enumerate(stages[:-1]):
                if not getattr(stage, "name", None):
                    raise ValueError(
                        f"Model '{self.name}': non-final stage at index {i} "
                        f"in source_queries must have a 'name'."
                    )
        seen: set = set()
        dupes: list[str] = []
        for stage in stages:
            n = getattr(stage, "name", None)
            if not n:
                continue
            if n in seen and n not in dupes:
                dupes.append(n)
            seen.add(n)
        if dupes:
            raise ValueError(
                f"Model '{self.name}': duplicate stage name(s) in "
                f"source_queries: {sorted(dupes)}."
            )
        return self

    # NOSONAR S3516 — Pydantic v2 @model_validator(mode="after") must return self.
    @model_validator(mode="after")
    def _reject_measures_on_query_backed(self) -> "SlayerModel":
        """A query-backed model may not declare ``measures`` directly — they never take effect."""
        if self.source_queries and self.measures:
            raise ValueError(
                f"Model '{self.name}': a query-backed model (source_queries) "
                f"cannot declare measures directly — they never take effect. "
                f"Declare the measure in the backing query's final stage, or add "
                f"it with a ModelExtension at query time."
            )
        return self

    def get_column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def get_measure(self, name: str) -> ModelMeasure | None:
        for m in self.measures:
            if m.name == name:
                return m
        return None

    def get_aggregation(self, name: str) -> Aggregation | None:
        for a in self.aggregations:
            if a.name == name:
                return a
        return None


class DatasourceConfig(BaseModel):
    version: int = 2
    name: str
    type: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    connection_string: str | None = None
    schema_name: str | None = None
    # Unlike ``schema_name`` (upstream physical schema), this is the schema the
    # Postgres facade advertises this datasource's models under; ``None`` => "public".
    postgres_schema: str | None = None
    description: str | None = None
    # Snowflake-specific. ``connection_name`` is the primary auth path (creds from
    # connections.toml); inline fields + ``warehouse``/``role`` are the secondary path.
    connection_name: str | None = None
    warehouse: str | None = None
    role: str | None = None
    # BigQuery-specific. A service-account key file as JSON; when set, the engine
    # authenticates against it, else falls back to Application Default Credentials.
    credentials_json: str | None = Field(default=None, repr=False)
    # BigQuery-specific. A per-end-user OAuth authorized-user grant as JSON;
    # mutually exclusive with ``credentials_json`` (which carries a service account).
    oauth_credentials_json: str | None = Field(default=None, repr=False)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations_and_aliases(cls, data: Any) -> Any:
        data = _migrate_schema(entity="DatasourceConfig", data=data)
        if isinstance(data, dict) and "user" in data and "username" not in data:
            data["username"] = data.pop("user")
        return data

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        # Leading segment of every canonical-id and a YAML storage path component;
        # substring rules shared with ``SlayerModel.data_source``. ``__`` is allowed.
        label = "Datasource 'name'"
        _require_non_empty_trimmed(v=v, context=label)
        _NO_NUL.check(name=v, context=label)
        _NO_FWD_SLASH.check(name=v, context=label)
        _NO_BACK_SLASH.check(name=v, context=label)
        _NO_DOT.check(name=v, context=label)
        _NO_COLON.check(name=v, context=label)
        return v

    @field_validator("postgres_schema")
    @classmethod
    def _validate_postgres_schema(cls, v: str | None) -> str | None:
        # Must be a lowercase unquoted Postgres identifier (avoids quoting ambiguity).
        if v is None:
            return v
        _require_non_empty_trimmed(v=v, context="Datasource 'postgres_schema'")
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", v):
            raise ValueError(
                f"Datasource 'postgres_schema' must be a lowercase Postgres "
                f"identifier matching [a-z_][a-z0-9_]*, got {v!r}"
            )
        return v

    def _get_tsql_connection_string(self) -> str:
        return _SA_URL.create(
            "mssql+pyodbc",
            username=self.username or None,
            password=self.password or None,
            host=self.host or "localhost",
            port=self.port,
            database=self.database or "",
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
            },
        ).render_as_string(hide_password=False)

    def get_connection_string(self) -> str:
        if self.connection_string:
            return self.connection_string
        # Dialect-specific hook (e.g. Snowflake); others return None and fall through.
        dialect = dialect_for_ds_type(self.type)
        url_from_dialect = dialect.build_connection_url(self)
        if url_from_dialect is not None:
            return str(url_from_dialect)
        if self.type in ("sqlite", "duckdb"):
            return f"{self.type}:///{self.database}"
        if self.type in ("mssql", "sqlserver", "tsql"):
            return self._get_tsql_connection_string()
        driver_map = {
            "postgres": "postgresql",
            "postgresql": "postgresql",
            "mysql": "mysql+pymysql",
            "mariadb": "mysql+pymysql",
            "clickhouse": "clickhouse+http",
        }
        driver = driver_map.get(self.type, self.type)
        # Use SQLAlchemy's structured builder so reserved chars in credentials
        # are percent-encoded rather than misparsed as URL delimiters.
        host, port = self.host or "localhost", self.port
        # ``URL.create`` wants a raw host + separate port, so lift any port
        # embedded in the host field; a port set in both places is contradictory.
        embedded_port: str | None = None
        bracketed = _BRACKETED_HOST_RE.match(host)
        if bracketed:
            host = bracketed.group(1)
            embedded_port = bracketed.group(2)
        else:
            embedded = _HOST_EMBEDDED_PORT_RE.match(host)
            if embedded:
                host, embedded_port = embedded.group(1), embedded.group(2)
        if embedded_port is not None:
            if port is not None:
                raise ValueError(
                    f"Datasource '{self.name}': port is set both in the host "
                    f"field ({self.host!r}) and in the 'port' field ({port}); "
                    f"specify it in only one place."
                )
            port = int(embedded_port)
        return _SA_URL.create(
            drivername=driver,
            username=self.username or None,
            password=self.password or None,
            host=host,
            port=port,
            database=self.database or "",
        ).render_as_string(hide_password=False)

    def resolve_env_vars(self) -> "DatasourceConfig":
        data = self.model_dump()
        unresolved = []
        for key, value in data.items():
            if isinstance(value, str):
                resolved = _resolve_env_string(value)
                data[key] = resolved
                for match in re.finditer(r"\$\{(\w+)\}", resolved):
                    unresolved.append(match.group(1))
        if unresolved:
            raise ValueError(
                f"Datasource '{self.name}': unresolved environment variable(s): "
                f"{', '.join(unresolved)}"
            )
        return DatasourceConfig(**data)


def _resolve_env_string(value: str) -> str:
    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    return re.sub(r"\$\{(\w+)\}", replacer, value)
