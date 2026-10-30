"""The usage report: every leaf is SLayer vocabulary (enum token, bool, bucket, date, UUID, count)."""

import datetime
import json
import uuid
from types import UnionType
from typing import Annotated, Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, model_validator

SCHEMA_VERSION = 1
VERSION_PATTERN = r"^\d+(\.\d+)*((a|b|rc)\d+)?(\.post\d+)?(\.dev\d+)?(\+[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$"

Count = Annotated[int, Field(ge=0)]
Version = Annotated[str, Field(pattern=VERSION_PATTERN, max_length=64)]

UsageKey = Literal[
    # CLI leaf commands
    "cli:serve", "cli:flight-serve", "cli:pg-serve", "cli:mcp", "cli:query", "cli:ingest",
    "cli:validate-models", "cli:recommend-root-model",
    "cli:import-dbt", "cli:import-cube", "cli:import-osi",
    "cli:models.list", "cli:models.show", "cli:models.create", "cli:models.delete",
    "cli:datasources.list", "cli:datasources.show", "cli:datasources.create",
    "cli:datasources.delete", "cli:datasources.test",
    "cli:memory.save", "cli:memory.forget", "cli:storage.migrate-types",
    "cli:datasources.refresh-samples", "cli:inspect", "cli:search",
    "cli:other",
    # MCP tools
    "mcp:create_datasource", "mcp:create_model", "mcp:delete_datasource", "mcp:delete_model",
    "mcp:describe_datasource", "mcp:edit_datasource", "mcp:edit_model", "mcp:forget_memory",
    "mcp:get_datasource_priority", "mcp:ingest_datasource_models", "mcp:inspect",
    "mcp:inspect_model", "mcp:list_datasources", "mcp:models_summary", "mcp:query",
    "mcp:recommend_root_model", "mcp:save_memory", "mcp:search", "mcp:set_datasource_priority",
    "mcp:validate_models", "mcp:other",
    # REST route templates
    "rest:/datasources", "rest:/datasources/priority", "rest:/datasources/{name}", "rest:/health",
    "rest:/ingest", "rest:/inspect", "rest:/memories", "rest:/memories/{memory_id}", "rest:/models",
    "rest:/models/{name}", "rest:/query", "rest:/recommend-root-model", "rest:/search",
    "rest:/validate-models", "rest:other",
    # wire protocols
    "flight:query", "flight:probe", "pg:query", "pg:probe",
]

ErrorToken = Literal[
    "other", "exit", "interrupted", "cancelled", "timeout", "connection", "file_not_found",
    "permission", "os_error", "import_error", "not_implemented", "key_error", "type_error",
    "value_error", "json_error", "validation_error", "db_operational", "db_programming",
    "db_error", "slayer_error", "query_type", "ambiguous_model", "entity_resolution",
    "memory_not_found", "schema_drift", "unknown_reference", "population_inference",
    "missing_driver", "translation", "tool_error", "http_4xx", "http_5xx",
]

FlagToken = Literal[
    "multistage", "inline_source", "time_dimensions", "filters",
    "computed_dimensions", "saved_query", "to_many_handling",
    "cast", "try_cast", "aggregate_filter", "list_literal",
]

TransformToken = Literal[
    "cumsum", "change", "change_pct", "time_shift", "first", "last", "lag", "lead",
    "consecutive_periods", "rank", "percent_rank", "dense_rank", "ntile", "custom",
]

AggregationToken = Literal[
    "sum", "avg", "min", "max", "count", "count_distinct", "count_distinct_approx",
    "first", "last", "weighted_avg", "median", "percentile", "stddev_samp", "stddev_pop",
    "var_samp", "var_pop", "corr", "covar_samp", "covar_pop", "collect", "custom",
]

FunctionToken = Literal[
    "abs", "acos", "ascii", "asin", "atan", "atan2", "cbrt", "ceil", "ceiling", "chr", "coalesce", "concat",
    "concat_ws", "contains", "contains_all", "contains_any", "cos", "current_date", "date_add", "date_diff",
    "date_part", "div", "ends_with", "exp", "floor", "greatest", "ifnull", "iif", "ilike", "instr", "interval",
    "json_value", "least", "left", "length", "like", "ln", "log", "log10", "log2", "lower", "lpad", "ltrim", "mod",
    "now", "nullif", "pi", "pow", "power", "regexp_extract", "regexp_like", "regexp_replace", "repeat", "replace",
    "reverse", "right", "round", "rpad", "rtrim", "sign", "sin", "split_part", "sqrt", "starts_with", "substr",
    "substring", "tan", "trim", "trunc", "upper",
]

DialectToken = Literal[
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery",
    "redshift", "trino", "presto", "databricks", "spark", "oracle", "other",
]

DatasourceBucket = Literal["0", "1", "2-5", "6+"]
ModelCountBucket = Literal["0", "1-10", "11-50", "51-200", "200+"]
StorageToken = Literal["yaml", "sqlite", "other"]

ClientToken = Literal[
    "claude-code", "claude-desktop", "cursor", "vscode", "windsurf", "codex", "zed", "cline",
    "goose", "continue", "other",
]

OsToken = Literal["linux", "darwin", "windows", "other"]
ArchToken = Literal["x86_64", "arm64", "other"]
ExtraToken = Literal[
    "client", "postgres", "mysql", "clickhouse", "sqlserver", "snowflake", "bigquery", "dbt",
    "flight", "advanced-search", "trino",
]

USAGE_KEYS: frozenset[str] = frozenset(get_args(UsageKey))
FLAGS: frozenset[str] = frozenset(get_args(FlagToken))
TRANSFORMS: frozenset[str] = frozenset(get_args(TransformToken))
AGGREGATIONS: frozenset[str] = frozenset(get_args(AggregationToken))
FUNCTIONS: frozenset[str] = frozenset(get_args(FunctionToken))
DIALECTS: frozenset[str] = frozenset(get_args(DialectToken))
CLIENTS: frozenset[str] = frozenset(get_args(ClientToken))
EXTRAS: tuple[ExtraToken, ...] = get_args(ExtraToken)


def known_usage_key(*, surface: str, key: str) -> str | None:
    """``key`` when defined, else ``<surface>:other`` when defined, else ``None``."""
    if key in USAGE_KEYS:
        return key
    other = f"{surface}:other"
    return other if other in USAGE_KEYS else None


def fold_retired_usage_keys(data: Any) -> Any:
    """Count a usage key this version no longer defines as its surface's ``other`` (or drop it)."""
    usage = data.get("usage") if isinstance(data, dict) else None
    if not isinstance(usage, dict) or all(key in USAGE_KEYS for key in usage):
        return data
    entries = {
        key: entry.model_dump() if isinstance(entry, UsageEntry) else entry for key, entry in usage.items()
    }
    if not all(isinstance(entry, dict) for entry in entries.values()):
        return data
    folded: dict[str, dict] = {}
    for key, entry in entries.items():
        target = known_usage_key(surface=str(key).partition(":")[0], key=str(key))
        if target is None:
            continue
        merged = folded.setdefault(target, {"ok": 0, "errors": {}})
        merged["ok"] += entry.get("ok", 0)
        for token, count in (entry.get("errors") or {}).items():
            merged["errors"][token] = merged["errors"].get(token, 0) + count
    return {**data, "usage": folded}


def _known_parts(data: Any, annotation: Any) -> Any:
    """``data`` without the fields and literal dict keys ``annotation`` does not define."""
    if get_origin(annotation) is Annotated:
        return _known_parts(data, get_args(annotation)[0])
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        if not isinstance(data, dict):
            return data
        fields = {(f.alias or n): f.annotation for n, f in annotation.model_fields.items()}
        fields |= {n: f.annotation for n, f in annotation.model_fields.items()}
        return {k: _known_parts(v, fields[k]) for k, v in data.items() if k in fields}
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is dict and isinstance(data, dict):
        key_type, value_type = args
        keep = (lambda k: k in get_args(key_type)) if get_origin(key_type) is Literal else (lambda _k: True)
        return {k: _known_parts(v, value_type) for k, v in data.items() if keep(k)}
    if origin is list and isinstance(data, list):
        return [_known_parts(v, args[0]) for v in data]
    if origin in (Union, UnionType) and data is not None:
        (inner,) = [a for a in args if a is not type(None)] or [annotation]
        return _known_parts(data, inner)
    return data


def read_spooled(raw: bytes) -> "Batch":
    """A spool file's batch, keeping every counter this version names and dropping the rest."""
    return Batch.model_validate(_known_parts(fold_retired_usage_keys(json.loads(raw)), Batch))


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Period(_Strict):
    first: datetime.date
    last: datetime.date


class UsageEntry(_Strict):
    ok: Count = 0
    errors: dict[ErrorToken, Count] = Field(default_factory=dict)


class Features(_Strict):
    flags: dict[FlagToken, Count] = Field(default_factory=dict)
    transforms: dict[TransformToken, Count] = Field(default_factory=dict)
    aggregations: dict[AggregationToken, Count] = Field(default_factory=dict)
    functions: dict[FunctionToken, Count] = Field(default_factory=dict)


class Dialects(_Strict):
    queries: dict[DialectToken, Count] = Field(default_factory=dict)
    datasources: dict[DialectToken, dict[DatasourceBucket, Count]] = Field(default_factory=dict)


class Context(_Strict):
    """Histograms over processes."""

    storage: dict[StorageToken, Count] = Field(default_factory=dict)
    model_count: dict[ModelCountBucket, Count] = Field(default_factory=dict)
    demo: Count = 0


class McpClientCount(_Strict):
    client: ClientToken
    major: Annotated[int, Field(ge=0, le=999)] | None = None
    sessions: Count = 0


class Batch(_Strict):
    """Counters of one or more processes; the content of a spool file."""

    period: Period | None = None
    usage: dict[UsageKey, UsageEntry] = Field(default_factory=dict)
    features: Features = Field(default_factory=Features)
    dialects: Dialects = Field(default_factory=Dialects)
    context: Context = Field(default_factory=Context)
    mcp_clients: list[McpClientCount] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _fold_retired_usage_keys(cls, data: Any) -> Any:
        return fold_retired_usage_keys(data)


class Env(_Strict):
    slayer_version: Version
    python: Version
    os: OsToken
    arch: ArchToken
    in_container: bool
    extras: list[ExtraToken]


class UsageReport(Batch):
    """One send: a merged batch plus identity and environment."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, serialize_by_alias=True)

    schema_version: Literal[1] = Field(default=SCHEMA_VERSION, alias="schema")
    install_id: uuid.UUID | None
    batch_id: uuid.UUID
    env: Env


def _add(target: dict, source: dict) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + value


def merge(batches: list[Batch]) -> Batch:
    """Sum the counters of ``batches`` into one."""
    usage: dict = {}
    features = Features()
    dialects = Dialects()
    context = Context()
    clients: dict[tuple, int] = {}
    firsts, lasts = [], []
    for batch in batches:
        if batch.period is not None:
            firsts.append(batch.period.first)
            lasts.append(batch.period.last)
        for key, entry in batch.usage.items():
            merged = usage.setdefault(key, UsageEntry())
            merged.ok += entry.ok
            _add(merged.errors, entry.errors)
        _add(features.flags, batch.features.flags)
        _add(features.transforms, batch.features.transforms)
        _add(features.aggregations, batch.features.aggregations)
        _add(features.functions, batch.features.functions)
        _add(dialects.queries, batch.dialects.queries)
        for dialect, buckets in batch.dialects.datasources.items():
            _add(dialects.datasources.setdefault(dialect, {}), buckets)
        _add(context.storage, batch.context.storage)
        _add(context.model_count, batch.context.model_count)
        context.demo += batch.context.demo
        for client in batch.mcp_clients:
            key = (client.client, client.major)
            clients[key] = clients.get(key, 0) + client.sessions
    return Batch(
        period=Period(first=min(firsts), last=max(lasts)) if firsts else None,
        usage=usage,
        features=features,
        dialects=dialects,
        context=context,
        mcp_clients=[
            McpClientCount(client=client, major=major, sessions=sessions)
            for (client, major), sessions in sorted(clients.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0))
        ],
    )
