"""The usage report: every leaf is SLayer vocabulary (enum token, bool, bucket, date, UUID, count)."""

import datetime
import json
import uuid
from types import UnionType
from typing import Annotated, Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, model_validator

SCHEMA_VERSION = 3
VERSION_PATTERN = r"^\d+(\.\d+)*((a|b|rc)\d+)?(\.post\d+)?(\.dev\d+)?(\+[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$"

Count = Annotated[int, Field(ge=0)]
Version = Annotated[str, Field(pattern=VERSION_PATTERN, max_length=64)]

UsageKey = Literal[
    # CLI leaf commands
    "cli:serve", "cli:flight-serve", "cli:pg-serve", "cli:mcp", "cli:query", "cli:ingest",
    "cli:validate-datasets", "cli:recommend-population",
    "cli:import-dbt", "cli:import-cube", "cli:import-osi", "cli:export-osi", "cli:ossie-roundtrip",
    "cli:semantic-models.list", "cli:semantic-models.show", "cli:semantic-models.delete",
    "cli:datasets.list", "cli:datasets.show", "cli:datasets.create", "cli:datasets.delete",
    "cli:datasources.list", "cli:datasources.show", "cli:datasources.create",
    "cli:datasources.delete", "cli:datasources.test",
    "cli:memory.save", "cli:memory.forget", "cli:storage.migrate-types", "cli:storage.upgrade",
    "cli:datasources.refresh-samples", "cli:inspect", "cli:search",
    "cli:otel.status", "cli:otel.test",
    "cli:other",
    # MCP tools
    "mcp:create_datasource", "mcp:create_dataset", "mcp:delete_datasource", "mcp:delete_dataset",
    "mcp:describe_datasource", "mcp:edit_datasource", "mcp:edit_dataset", "mcp:forget_memory",
    "mcp:get_datasource_priority", "mcp:ingest_datasource", "mcp:inspect",
    "mcp:list_datasources", "mcp:datasets_summary", "mcp:query",
    "mcp:recommend_population", "mcp:save_memory", "mcp:search", "mcp:set_datasource_priority",
    "mcp:validate_datasets", "mcp:create_semantic_model", "mcp:edit_semantic_model", "mcp:delete_semantic_model",
    "mcp:other",
    # REST route templates
    "rest:/datasources", "rest:/datasources/priority", "rest:/datasources/{name}", "rest:/health",
    "rest:/ingest", "rest:/inspect", "rest:/memories", "rest:/memories/{memory_id}", "rest:/datasets",
    "rest:/datasets/{name}", "rest:/query", "rest:/recommend-population", "rest:/search",
    "rest:/validate-datasets", "rest:/semantic-models", "rest:/semantic-models/{data_source}/{name}", "rest:other",
    # wire protocols
    "flight:query", "flight:probe", "pg:query", "pg:probe",
]

ErrorToken = Literal[
    "other", "exit", "interrupted", "cancelled", "timeout", "connection", "file_not_found",
    "permission", "os_error", "import_error", "not_implemented", "key_error", "type_error",
    "value_error", "json_error", "validation_error", "db_operational", "db_programming",
    "db_error", "slayer_error", "query_type", "ambiguous_dataset", "entity_resolution",
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
    "date_part", "degrees", "div", "ends_with", "exp", "floor", "greatest", "ifnull", "iif", "ilike", "instr",
    "interval", "json_value", "least", "left", "length", "like", "ln", "log", "log10", "log2", "lower", "lpad", "ltrim",
    "mod", "now", "nullif", "nullifzero", "pi", "pow", "power", "radians", "regexp_count", "regexp_extract",
    "regexp_like", "regexp_replace", "repeat", "replace",
    "reverse", "right", "round", "rpad", "rtrim", "sign", "sin", "split_part", "sqrt", "starts_with", "substr",
    "substring", "tan", "trim", "trunc", "upper", "zeroifnull",
]

DialectToken = Literal[
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery",
    "redshift", "trino", "presto", "databricks", "spark", "oracle", "other",
]

DatasourceBucket = Literal["0", "1", "2-5", "6+"]
DatasetCountBucket = Literal["0", "1-10", "11-50", "51-200", "200+"]
QueryCountBucket = Literal["0", "1", "2-5", "6-20", "21+"]
StorageToken = Literal["yaml", "sqlite", "other"]

ClientToken = Literal[
    "claude-code", "claude-desktop", "cursor", "vscode", "windsurf", "codex", "zed", "cline",
    "goose", "continue", "other",
]

OsToken = Literal["linux", "darwin", "windows", "other"]
ArchToken = Literal["x86_64", "arm64", "other"]
ExtraToken = Literal[
    "client", "postgres", "mysql", "clickhouse", "sqlserver", "snowflake", "bigquery", "dbt",
    "flight", "advanced-search", "trino", "otel",
]

USAGE_KEYS: frozenset[str] = frozenset(get_args(UsageKey))
FLAGS: frozenset[str] = frozenset(get_args(FlagToken))
TRANSFORMS: frozenset[str] = frozenset(get_args(TransformToken))
AGGREGATIONS: frozenset[str] = frozenset(get_args(AggregationToken))
FUNCTIONS: frozenset[str] = frozenset(get_args(FunctionToken))
DIALECTS: frozenset[str] = frozenset(get_args(DialectToken))
CLIENTS: frozenset[str] = frozenset(get_args(ClientToken))
EXTRAS: tuple[ExtraToken, ...] = get_args(ExtraToken)
_QUERY_COUNT_RANK = {bucket: rank for rank, bucket in enumerate(get_args(QueryCountBucket))}


def known_usage_key(*, surface: str, key: str) -> str | None:
    """``key`` when defined, else ``<surface>:other`` when defined, else ``None``."""
    if key in USAGE_KEYS:
        return key
    other = f"{surface}:other"
    return other if other in USAGE_KEYS else None


#: Schema-2 usage keys and their schema-3 replacements.
RENAMED_USAGE_KEYS: dict[str, str] = {
    "cli:validate-models": "cli:validate-datasets", "cli:recommend-root-model": "cli:recommend-population",
    "cli:models.list": "cli:datasets.list", "cli:models.show": "cli:datasets.show",
    "cli:models.create": "cli:datasets.create", "cli:models.delete": "cli:datasets.delete",
    "mcp:create_model": "mcp:create_dataset", "mcp:edit_model": "mcp:edit_dataset",
    "mcp:delete_model": "mcp:delete_dataset", "mcp:models_summary": "mcp:datasets_summary",
    "mcp:validate_models": "mcp:validate_datasets", "mcp:recommend_root_model": "mcp:recommend_population",
    "mcp:ingest_datasource_models": "mcp:ingest_datasource", "rest:/models": "rest:/datasets",
    "rest:/models/{name}": "rest:/datasets/{name}", "rest:/validate-models": "rest:/validate-datasets",
    "rest:/recommend-root-model": "rest:/recommend-population",
}


def _schema_2_context(data: dict) -> dict:
    context = data.get("context")
    if not isinstance(context, dict) or not isinstance(context.get("model_count"), dict):
        return data
    buckets = dict(context.get("dataset_count") or {})
    _add(buckets, context["model_count"])
    rest = {k: v for k, v in context.items() if k != "model_count"}
    return {**data, "context": {**rest, "dataset_count": buckets}}


def fold_retired_usage_keys(data: Any) -> Any:
    """A schema-2 key counted under its replacement; a usage key this version no longer defines counted as its
    surface's ``other`` (or dropped)."""
    if isinstance(data, dict):
        data = _schema_2_context(data)
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
        key = RENAMED_USAGE_KEYS.get(str(key), key)
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
    dataset_count: dict[DatasetCountBucket, Count] = Field(default_factory=dict)
    demo: Count = 0


class McpClientCount(_Strict):
    client: ClientToken
    major: Annotated[int, Field(ge=0, le=999)] | None = None
    sessions: Count = 0


class McpSessionCount(_Strict):
    """MCP sessions whose successful and failed ``query`` calls fall into these buckets."""

    queries_ok: QueryCountBucket
    queries_err: QueryCountBucket
    sessions: Count = 0


def session_counts(counts: dict[tuple[str, str], int]) -> list[McpSessionCount]:
    """One row per ``(queries_ok, queries_err)`` bucket pair, in bucket rank order."""
    pairs = sorted(counts, key=lambda pair: (_QUERY_COUNT_RANK[pair[0]], _QUERY_COUNT_RANK[pair[1]]))
    return [McpSessionCount.model_validate({"queries_ok": ok, "queries_err": err, "sessions": counts[(ok, err)]})
            for ok, err in pairs]


class Batch(_Strict):
    """Counters of one or more processes; the content of a spool file."""

    period: Period | None = None
    usage: dict[UsageKey, UsageEntry] = Field(default_factory=dict)
    features: Features = Field(default_factory=Features)
    dialects: Dialects = Field(default_factory=Dialects)
    context: Context = Field(default_factory=Context)
    mcp_clients: list[McpClientCount] = Field(default_factory=list)
    mcp_sessions: list[McpSessionCount] = Field(default_factory=list)

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

    schema_version: Literal[3] = Field(default=SCHEMA_VERSION, alias="schema")
    install_id: uuid.UUID | None
    first_query_ok: datetime.date | None = None
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
    session_pairs: dict[tuple[str, str], int] = {}
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
        _add(context.dataset_count, batch.context.dataset_count)
        context.demo += batch.context.demo
        for client in batch.mcp_clients:
            key = (client.client, client.major)
            clients[key] = clients.get(key, 0) + client.sessions
        for row in batch.mcp_sessions:
            pair = (row.queries_ok, row.queries_err)
            session_pairs[pair] = session_pairs.get(pair, 0) + row.sessions
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
        mcp_sessions=session_counts(session_pairs),
    )
