"""Query engine — central orchestrator for SLayer queries.

Flow: SlayerQuery → plan_query() → PlannedQuery → SQLGenerator → SQL → execute
"""

import asyncio
import copy
import decimal
import logging
import re
import warnings as _warnings_module
from collections.abc import Callable
from typing import Any, Dict, List, Optional

import sqlalchemy as sa
from sqlglot import exp
from pydantic import (
    BaseModel,
    ConfigDict as PydanticConfigDict,
    Field as PydanticField,
    model_validator,
)

from slayer.async_utils import run_sync
from slayer.core.enums import DEFAULT_AGGREGATIONS_BY_TYPE, DataType, JoinCardinality
from slayer.core.errors import (
    AmbiguousModelError,
    BroadcastGrainWarning,
    ForcedFilterError,
    ModelSqlValidationError,
    SchemaDriftError,
    SlayerError,
    UnreachableFilterDroppedWarning,
)
from slayer.engine.cardinality import (
    CardinalityVerdict,
    JoinCardinalityFinding,
    JoinCardinalityReport,
    SideStats,
    classify_cardinality,
    compute_verdict,
    declares_solo_unique,
)
from slayer.core.policy import JoinFilterRuleset, SessionPolicy
from slayer.core.format import format_number
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import (
    SlayerQuery,
    _contains_block_delimiter,
    coerce_declared_list_variables,
    declares_variables,
    extract_variable_refs,
    list_valued_variable_names,
    substitute_variables,
)
from slayer.core.warnings import (
    AnySlayerWarning,
    BroadcastDimension,
    BroadcastGrainWarningPayload,
    DroppedFilterWarning,
    NormalizationWarning,
)
from slayer.core.recommend import (
    CandidateCoverage,
    ItemPath,
    RootModelRecommendation,
)
from slayer.engine.syntax import split_entity_agg_ref
from slayer.engine.cache import (
    CacheConfig,
    QueryCache,
    RefreshError,
    RefreshKeyValue,
    RefreshResult,
    _CacheEntry,
)
from slayer.engine.normalization import normalize_query
from slayer.core.keys import REGROUP_LEAF_PREFIX
from slayer.engine.planned import PlannedQuery
from slayer.engine.schema_drift import (
    AppliedEntry,
    ApplyDriftResult,
    ApplyError,
    ToDeleteEntry,
    validate_datasource,
)
from slayer.engine.response_meta import (
    FieldMetadata as FieldMetadata,  # re-export for slayer_client / tests
    ResponseAttributes,
    build_response_metadata,
    projection_result_keys,
)
from slayer.engine.column_expansion import expand_derived_refs_sync
from slayer.engine.source_bundle import (
    ResolvedSourceBundle,
    build_resolved_source_bundle,
    expand_query_backed_models_in_bundle,
)
from slayer.engine.stage_ordering import topologically_order_stages
from slayer.engine.stage_planner import _topo_sort, plan_stages
from slayer.engine.variables import apply_variables_to_query
from slayer.engine.introspect_utils import _safe_get_columns
from slayer.engine.schema_scope import SchemaRef
from slayer.engine.join_graph import JoinGraph, min_hops_root
from slayer.memories.resolver import (
    _all_models_in_datasource,
    resolve_entity,
)
from slayer.sql.client import (
    SlayerSQLClient,
    _is_auth_failure,
    _is_transient_db_error,
    _is_unreachable_db_error,
    build_sql_model_trial_query,
    is_data_modifying_sql,
)
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type, get_dialect
from slayer.sql import engine_factory
from slayer.sql.engine_factory import EngineCacheKey, _sql_client_cache_key
from slayer.sql.generator import generate_planned_stages
from slayer.sql.session_policy import ScopedTable, apply_session_policy
from slayer.sql.stage_wrapper import build_flat_rename_wrapper
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class _ResolvedItem(BaseModel):
    input_item: str
    data_source: str
    model: str
    leaf: str
    suffix: str | None = None


def _emit_recommend_path(graph: JoinGraph, root: str, item: "_ResolvedItem") -> str:
    """Join-qualified path to ``item`` from ``root`` (root excluded), suffix re-attached."""
    hops = graph.shortest_path(root, item.model) or []
    core = item.leaf if not hops else ".".join(hops) + "." + item.leaf
    return core if item.suffix is None else f"{core}:{item.suffix}"


def _resolve_root_hint(
    raw_hint: str | None, *, data_source: str, all_names: list[str]
) -> tuple[str, str] | None:
    """Resolve ``root_hint`` to ``(model, display)`` (display verbatim in diagnostics); raises if malformed."""
    if raw_hint is None:
        return None
    display = raw_hint.strip()
    if not display:
        return None
    if "." in display:
        segs = display.split(".")
        if len(segs) == 2 and segs[0] == data_source:
            model = segs[1]
        else:
            raise ValueError(
                f"root_hint '{display}' must be a bare model name or "
                f"'{data_source}.<model>' within datasource '{data_source}'."
            )
    else:
        model = display
    if model not in all_names:
        raise ValueError(
            f"root_hint '{display}' is not a model in datasource '{data_source}'."
        )
    return model, display


def _build_recommend_coverage(
    graph: JoinGraph,
    all_names: list[str],
    mentioned: set[str],
    resolved: list["_ResolvedItem"],
    *,
    force_include: set[str] | None = None,
) -> list[CandidateCoverage]:
    """Pareto frontier of partial-root candidates; ``force_include`` rows appear even if dominated."""
    forced = force_include or set()
    items_in_order = [r.input_item for r in resolved]
    model_of_item = {r.input_item: r.model for r in resolved}

    candidates: list[tuple[str, set[str], dict[str, int]]] = []
    for name in all_names:
        reach = {m for m in mentioned if graph.shortest_path(name, m) is not None}
        if not reach and name not in forced:
            continue
        hops = {m: len(graph.shortest_path(name, m) or []) for m in reach}
        candidates.append((name, reach, hops))

    def dominates(a: tuple, b: tuple) -> bool:
        _an, ar, ah = a
        _bn, br, bh = b
        if ar > br:  # strict superset → covers strictly more
            return True
        if ar == br:  # same coverage, no path longer, at least one shorter
            return all(ah[m] <= bh[m] for m in ar) and any(ah[m] < bh[m] for m in ar)
        return False

    frontier = [
        c for c in candidates
        if c[0] in forced
        or not any(dominates(o, c) for o in candidates if o[0] != c[0])
    ]

    entries: list[tuple[CandidateCoverage, int]] = []
    for name, reach, hops in frontier:
        reachable_items = [it for it in items_in_order if model_of_item[it] in reach]
        unreachable_items = [it for it in items_in_order if model_of_item[it] not in reach]
        entries.append((
            CandidateCoverage(
                model_name=name,
                reachable_items=reachable_items,
                unreachable_items=unreachable_items,
            ),
            sum(hops.values()),
        ))
    entries.sort(key=lambda e: (-len(e[0].reachable_items), e[1], e[0].model_name))
    return [e[0] for e in entries]

_PLACEHOLDER_FILL_VALUE = "0"


def _merge_query_variables(
    *,
    outer: Optional[Dict[str, Any]],
    stage: Optional[Dict[str, Any]],
    runtime: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge variable layers per precedence ``runtime > stage > outer``."""
    return {**(outer or {}), **(stage or {}), **(runtime or {})}


def _model_has_optional_block(model: SlayerModel) -> bool:
    """True if any Mode-A surface carries an optional ``{? ... ?}`` block."""
    surfaces = [model.sql, *(model.filters or [])]
    for col in model.columns:
        surfaces.append(col.sql)
        surfaces.append(col.filter)
    return any(s and _contains_block_delimiter(s) for s in surfaces)


def _model_needs_substitution_pass(model: SlayerModel) -> bool:
    """True if substitution must run with no variables (a ``{? ?}`` block or declared variables)."""
    return _model_has_optional_block(model) or declares_variables(model)


def _substitute_model_sql_surfaces(
    *, model: SlayerModel, variables: dict[str, Any], dialect: SqlDialect
) -> SlayerModel:
    """Copy of ``model`` with ``{var}`` substituted into its four Mode-A surfaces (no-op when unneeded; never mutates input)."""
    if not variables and not _model_needs_substitution_pass(model):
        return model

    variables = coerce_declared_list_variables(
        variables, list_valued=list_valued_variable_names(model)
    )
    backslash_escapes = dialect.backslash_escapes_strings

    def _sub(text: str) -> str:
        return substitute_variables(
            filter_str=text,
            variables=variables,
            escape="sql",
            backslash_escapes=backslash_escapes,
        )

    new_columns = []
    for col in model.columns:
        updates: dict[str, Any] = {}
        if col.sql is not None:
            updates["sql"] = _sub(col.sql)
        if col.filter is not None:
            updates["filter"] = _sub(col.filter)
        new_columns.append(col.model_copy(update=updates) if updates else col)

    model_updates: dict[str, Any] = {"columns": new_columns}
    if model.sql is not None:
        model_updates["sql"] = _sub(model.sql)
    if model.filters:
        model_updates["filters"] = [_sub(f) for f in model.filters]
    return model.model_copy(update=model_updates)


def _render_probe_model(model: SlayerModel, *, dialect: SqlDialect) -> SlayerModel:
    """Substitute a template model's own ``query_variables`` defaults for type-probing (raises on undefaulted)."""
    if model.source_model_origin is None and (
        model.query_variables or _model_needs_substitution_pass(model)
    ):
        return _substitute_model_sql_surfaces(
            model=model, variables=model.query_variables, dialect=dialect
        )
    return model




def _build_explain_sql(dialect: str, sql: str) -> str:
    """Dialect-appropriate EXPLAIN; raises for dialects without SQL-level EXPLAIN (e.g. BigQuery)."""
    return get_dialect(dialect).build_explain_sql(sql)


def _stage_location(*, stages, index: int, member: Optional[str] = "filters") -> str:
    """Human-readable stage pointer; part of the dedup identity (distinguishes same-text stages)."""
    name = getattr(stages[index], "name", None) if index < len(stages) else None
    base = f"stage {name!r}" if name else f"stages[{index}]"
    return f"{base}.{member}" if member else base


def _walk_regroup_attaches(planned):
    """Every ``RegroupAttachPlan`` on ``planned``, recursively (a producer is a nested plan)."""
    for attach in getattr(planned, "regroup_attach_plans", ()) or ():
        yield attach
        yield from _walk_regroup_attaches(attach.producer_plan)


def _collect_dropped_filter_warnings(
    *, planned_list, stages,
) -> List[DroppedFilterWarning]:
    """Dropped-filter payloads, one per user filter; identity ``(location, filter text)``, reasons merged."""
    reasons_by_identity: "dict[tuple[str, str], list[str]]" = {}

    def _record(w, location: str) -> None:
        reasons = reasons_by_identity.setdefault((location, w.filter_text), [])
        if w.reason not in reasons:
            reasons.append(w.reason)

    for index, planned in enumerate(planned_list):
        location = _stage_location(stages=stages, index=index)
        for attach in _walk_regroup_attaches(planned):
            for w in attach.dropped_filter_warnings or ():
                _record(w, location)
    return [
        DroppedFilterWarning(
            filter_text=text, location=location, reason="; ".join(reasons),
        )
        for (location, text), reasons in reasons_by_identity.items()
    ]


def _collect_broadcast_warnings(
    *, planned_list, stages,
) -> List[BroadcastGrainWarningPayload]:
    """One broadcast payload per ``(stage location, measure label)``; dimensions unioned."""
    dims_by_key: "dict[tuple[str, str], list[tuple[str, str]]]" = {}
    for index, planned in enumerate(planned_list):
        location = _stage_location(stages=stages, index=index, member=None)
        for attach in _walk_regroup_attaches(planned):
            measure = attach.broadcast_measure
            if not measure:
                continue
            dims = dims_by_key.setdefault((location, measure), [])
            seen = {d for d, _ in dims}
            for dim, reason in attach.broadcast_dimensions:
                if dim not in seen:
                    dims.append((dim, reason))
                    seen.add(dim)
    return [
        BroadcastGrainWarningPayload(
            measure=measure, location=location,
            dimensions=[BroadcastDimension(dimension=d, reason=r) for d, r in dims],
        )
        for (location, measure), dims in dims_by_key.items()
    ]


def _raise_on_strict_events(
    *, broadcasts: List[BroadcastGrainWarningPayload],
    dropped: List[DroppedFilterWarning],
) -> None:
    """Strict mode: turn any silent-semantics event into an error naming metric/filter + remedy."""
    remedy = (
        "declare join cardinality, a covering unique key, or remove the "
    )
    if broadcasts:
        w = broadcasts[0]
        dims = ", ".join(d.dimension for d in w.dimensions)
        reason = w.dimensions[0].reason if w.dimensions else ""
        raise SlayerError(
            f"strict mode: metric {w.measure!r} would broadcast across "
            f"unattributable dimension(s) {dims} ({reason}); {remedy}dimension."
        )
    if dropped:
        d = dropped[0]
        raise SlayerError(
            f"strict mode: filter {d.filter_text!r} would be dropped from a "
            f"producer ({d.reason}); {remedy}filter."
        )


def _emit_dropped_filter_warnings(response) -> None:
    """Emit one Python ``UserWarning`` per dropped filter / broadcast metric."""

    for w in response.warnings or ():
        if isinstance(w, DroppedFilterWarning):
            _warnings_module.warn(
                UnreachableFilterDroppedWarning(
                    filter_text=w.filter_text, reason=w.reason,
                ),
                stacklevel=3,
            )
        elif isinstance(w, BroadcastGrainWarningPayload):
            reason = w.dimensions[0].reason if w.dimensions else ""
            _warnings_module.warn(
                BroadcastGrainWarning(measure=w.measure, reason=reason),
                stacklevel=3,
            )


class SlayerResponse(BaseModel):
    """Response from a SLayer query."""

    data: List[Dict[str, Any]]
    columns: List[str] = PydanticField(default_factory=list)
    sql: Optional[str] = None
    attributes: ResponseAttributes = PydanticField(default_factory=ResponseAttributes)
    # Query advisories, discriminated on ``kind`` (normalization rewrites,
    # dropped cross-model filters); empty for a clean query.
    warnings: List[AnySlayerWarning] = PydanticField(default_factory=list)

    @model_validator(mode="after")
    def _populate_columns(self) -> "SlayerResponse":
        if not self.columns and self.data:
            self.columns = list(self.data[0].keys())
        return self

    @property
    def row_count(self) -> int:
        return len(self.data)

    def _format_value(self, column: str, value: Any) -> str:
        """Format a single cell value using column format metadata if available."""
        if value is None:
            return ""
        fm = self.attributes.get(column)
        if fm and fm.format and isinstance(value, (int, float, decimal.Decimal)):
            return format_number(value=value, format_spec=fm.format)
        return str(value)

    def to_markdown(self) -> str:
        """Format data as a Markdown table with number formatting applied."""
        if not self.data:
            return "No results."
        header = "| " + " | ".join(self.columns) + " |"
        separator = "| " + " | ".join("---" for _ in self.columns) + " |"
        body_lines = []
        for row in self.data:
            cells = [self._format_value(column=c, value=row.get(c, "")) for c in self.columns]
            body_lines.append("| " + " | ".join(cells) + " |")
        return "\n".join([header, separator] + body_lines)


class _Prepared(BaseModel):
    """DB-free product of ``_prepare_pipeline``; ``sql`` is the final rewritten SQL, ds-fingerprint computed lazily."""

    model_config = PydanticConfigDict(arbitrary_types_allowed=True)

    sql: str
    dialect: str
    datasource: DatasourceConfig
    resolved_data_source: Optional[str] = None
    attributes: Any
    expected_columns: List[str]
    touched: set
    model: SlayerModel
    slack_warnings: List[Any] = PydanticField(default_factory=list)


class SlayerQueryEngine:
    """Central orchestrator: resolves queries via storage, generates SQL, executes."""

    def __init__(
        self,
        storage: StorageBackend,
        *,
        policy: Optional[SessionPolicy] = None,
        cache_config: Optional[CacheConfig] = None,
    ):
        self.storage = storage
        # Per-engine, opt-in query result cache (defaults to cache-indefinitely).
        self._cache = QueryCache(config=cache_config or CacheConfig())
        # Keyed so same-name Snowflake datasources (differing warehouse/role) get distinct clients.
        self._sql_clients: dict[EngineCacheKey, SlayerSQLClient] = {}
        # Engine-global forced-filter policy; tenant-scopes every generated SQL.
        self.policy = policy
        # Column-presence facts; an unconfirmable ``None`` is re-probed, never cached.
        self._column_presence_cache: dict[tuple, bool] = {}
        # Cached ClickHouse version per datasource; missing/None fails closed.
        self._ch_version_cache: dict[EngineCacheKey, tuple[int, int] | None] = {}

    @property
    def cache_config(self) -> CacheConfig:
        """The active :class:`CacheConfig` (read-only view of ``_cache``)."""
        return self._cache.config

    @cache_config.setter
    def cache_config(self, config: CacheConfig) -> None:
        """Reassign the cache policy; clears the cache (preserving the clock)."""
        self._cache = QueryCache(config=config, clock=self._cache._clock)

    @property
    def cache_size(self) -> int:
        return self._cache.size()

    def clear_cache(self) -> None:
        self._cache.clear()

    def _apply_policy(
        self, *, sql: str, dialect: str, datasource: DatasourceConfig
    ) -> str:
        """Rewrite ``sql`` to enforce the forced-filter policy; unchanged when no policy."""
        if not self.policy:
            return sql
        return apply_session_policy(
            sql,
            dialect=dialect,
            policy=self.policy,
            has_column=lambda scoped, column: self._column_present(
                datasource=datasource, scoped_table=scoped, column=column
            ),
            on_correlated_emitted=self._clickhouse_correlated_guard(
                dialect=dialect, datasource=datasource
            ),
        )

    def _policy_has_join_rules(self) -> bool:
        return bool(
            self.policy
            and isinstance(self.policy.ruleset, JoinFilterRuleset)
            and self.policy.ruleset.joins
        )

    @staticmethod
    def _parse_clickhouse_version(raw: Any) -> tuple[int, int] | None:
        """Parse a ClickHouse ``version()`` string to ``(major, minor)``; ``None`` if unparseable."""
        if not isinstance(raw, str):
            return None
        match = re.match(r"\s*v?(\d+)\.(\d+)", raw)
        if not match:
            return None
        return (int(match.group(1)), int(match.group(2)))

    def _clickhouse_correlated_guard(
        self, *, dialect: str, datasource: DatasourceConfig
    ) -> Callable[[], None] | None:
        """Guard for a correlated ``EXISTS`` rewrite; raises ``ForcedFilterError`` when version unknown or ``< (25, 4)``."""
        if dialect != "clickhouse":
            return None
        ds_key = _sql_client_cache_key(datasource)

        def guard() -> None:
            version = self._ch_version_cache.get(ds_key)
            if version is None:
                raise ForcedFilterError(
                    "ClickHouse join-based forced filter needs a correlated "
                    "subquery (server >= 25.4), but the server version could "
                    "not be determined; failing closed."
                )
            if version < (25, 4):
                raise ForcedFilterError(
                    "ClickHouse join-based forced filter needs a correlated "
                    "subquery, which requires server >= 25.4; detected "
                    f"{version[0]}.{version[1]}; failing closed."
                )
            logger.warning(
                "Applying a join-based forced filter on ClickHouse via an "
                "experimental correlated subquery "
                "(allow_experimental_correlated_subqueries=1); requires "
                "server >= 25.4 (detected %d.%d).",
                version[0],
                version[1],
            )

        return guard

    async def _preflight_clickhouse_correlated(
        self, *, dialect: str, datasource: DatasourceConfig
    ) -> None:
        """Probe + cache the ClickHouse version once per datasource (join-rule policies); failure caches ``None``."""
        if dialect != "clickhouse" or not self._policy_has_join_rules():
            return
        ds_key = _sql_client_cache_key(datasource)
        if ds_key in self._ch_version_cache:
            return  # already probed (value may be None)
        try:
            if ds_key not in self._sql_clients:
                self._sql_clients[ds_key] = SlayerSQLClient(datasource=datasource)
            client = self._sql_clients[ds_key]
            rows = await client.execute("SELECT version()")
            raw = None
            if rows and isinstance(rows[0], dict):
                raw = next(iter(rows[0].values()), None)
            self._ch_version_cache[ds_key] = self._parse_clickhouse_version(raw)
        except Exception as exc:
            logger.warning(
                "ClickHouse version preflight failed for datasource '%s'; "
                "join-based forced filters will fail closed: %s",
                datasource.name,
                exc,
            )
            self._ch_version_cache[ds_key] = None

    def _column_present(
        self,
        *,
        datasource: DatasourceConfig,
        scoped_table: ScopedTable,
        column: str,
    ) -> bool | None:
        """Whether ``column`` exists on ``scoped_table``: ``True``/``False``/``None`` (only confirmed results cached)."""
        schema = scoped_table.schema_name or datasource.schema_name
        # Cross-catalog refs can't be confirmed (introspection takes no catalog
        # argument) — fail closed rather than risk an under-filter.
        if scoped_table.catalog and (
            not datasource.database
            or scoped_table.catalog.casefold() != datasource.database.casefold()
        ):
            return None
        # Catalog in the key so BigQuery-style catalog twins never share a fact.
        key = (
            _sql_client_cache_key(datasource),
            scoped_table.catalog,
            schema,
            scoped_table.name,
            column,
        )
        if key in self._column_presence_cache:
            return self._column_presence_cache[key]
        try:
            sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
            inspector = sa.inspect(sa_engine)
            # Carry the catalog so the DuckDB fallback never unions a same-named twin.
            ref = SchemaRef(catalog=scoped_table.catalog, name=schema)
            cols = _safe_get_columns(
                inspector=inspector, sa_engine=sa_engine,
                table_name=scoped_table.name, ref=ref,
            )
        except Exception as exc:  # introspection failed -> cannot confirm
            logger.warning(
                "Forced filter: column-presence probe failed for %s.%s "
                "(column %r): %s",
                schema or "<default>",
                scoped_table.name,
                column,
                exc,
            )
            return None
        if not cols:
            return None  # no columns resolved -> cannot confirm
        names = {str(c.get("name", "")).lower() for c in cols}
        present = column.lower() in names
        self._column_presence_cache[key] = present
        return present

    @classmethod
    def _topologically_order_queries(
        cls,
        queries: List["SlayerQuery"],
    ) -> List["SlayerQuery"]:
        """Shim delegating to :func:`topologically_order_stages`."""
        return topologically_order_stages(queries)

    async def aclose(self) -> None:
        """Dispose cached clients' async engines (avoids leaking connections); keep the clients."""
        for client in self._sql_clients.values():
            await client.aclose()

    async def execute(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        *,
        dry_run: bool = False,
        explain: bool = False,
        data_source: Optional[str] = None,
        cache: bool = False,
    ) -> SlayerResponse:
        runtime_kwarg = variables or {}
        main_query, named_queries, prefer_data_source = await self._normalize_input(
            query, runtime_kwarg=runtime_kwarg, prefer_data_source=data_source
        )
        response = await self._execute_pipeline(
            query=main_query,
            named_queries=named_queries,
            runtime_kwarg=runtime_kwarg,
            dry_run=dry_run,
            explain=explain,
            prefer_data_source=prefer_data_source,
            cache=cache,
            original_input=query,
            original_data_source=data_source,
        )
        # The one Python-warnings emission: after the response is built (under
        # ``-W error`` this raises) and path-independent (dry_run/explain/execute).
        _emit_dropped_filter_warnings(response)
        return response

    async def _normalize_input(  # NOSONAR S3776 — public dispatch over str/dict/list/SlayerQuery; splitting hides the input-shape contract
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        *,
        runtime_kwarg: Dict[str, Any],
        prefer_data_source: Optional[str],
    ) -> "tuple[SlayerQuery, Dict[str, SlayerQuery], Optional[str]]":
        """Resolve the user input union into ``(main_query, named_queries, prefer_data_source)``."""
        # Run-by-name: ``execute("model_name", ...)`` runs the backing query.
        if isinstance(query, str):
            return await self._normalize_by_name(
                name=query,
                runtime_kwarg=runtime_kwarg,
                prefer_data_source=prefer_data_source,
            )

        if isinstance(query, list):
            if not query:
                raise ValueError(
                    "'query' must be a non-empty list when passing staged queries."
                )
            queries = [SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in query]
            # Reorder so each stage follows the siblings it references (last stays
            # the entry point); validates names / dups / self-refs / cycles.
            queries = self._topologically_order_queries(queries)
            main_query = queries[-1]
            named_queries = {q.name: q for q in queries[:-1] if q.name}
        else:
            if isinstance(query, dict):
                query = SlayerQuery.model_validate(query)
            main_query = query
            named_queries = {}

        # Merge ``variables=`` kwarg into query.variables (runtime wins).
        if runtime_kwarg:
            merged_top = {**(main_query.variables or {}), **runtime_kwarg}
            if merged_top != (main_query.variables or {}):
                main_query = main_query.model_copy(update={"variables": merged_top})

        return main_query, named_queries, prefer_data_source

    async def _normalize_by_name(
        self,
        *,
        name: str,
        runtime_kwarg: Dict[str, Any],
        prefer_data_source: Optional[str],
    ) -> "tuple[SlayerQuery, Dict[str, SlayerQuery], Optional[str]]":
        """Normalize a run-by-name input into the shared prepare tuple (``prefer_data_source`` pins the lookup)."""
        model = await self.storage.get_model(name, data_source=prefer_data_source)
        if model is None:
            raise ValueError(f"Model '{name}' not found")
        if not model.source_queries:
            raise ValueError(
                f"Model '{name}' is not query-backed; pass a SlayerQuery "
                f"with source_model='{name}'."
            )

        # Stored ``source_queries`` may be non-topological for
        # ``joins[].target_model`` deps; topo-sort to match save-time semantics.
        stages = topologically_order_stages(list(model.source_queries))
        main_query = stages[-1]
        named_queries: Dict[str, SlayerQuery] = {}
        for q in stages[:-1]:
            if q.name:
                if q.name in named_queries:
                    raise ValueError(
                        f"Duplicate query name '{q.name}' in source_queries "
                        f"of model '{name}'"
                    )
                named_queries[q.name] = q

        # Precedence ``runtime > stage > model_defaults`` (no outer query here,
        # so ``model.query_variables`` is the lowest layer).
        merged = _merge_query_variables(
            outer=model.query_variables,
            stage=main_query.variables,
            runtime=runtime_kwarg,
        )
        if merged != (main_query.variables or {}):
            main_query = main_query.model_copy(update={"variables": merged})

        return main_query, named_queries, model.data_source or prefer_data_source

    async def _prepare_pipeline(  # NOSONAR S3776 — linear pipeline (resolve→bind→generate→policy); breaking it up obscures the order of operations
        self,
        query: SlayerQuery,
        named_queries: Dict[str, SlayerQuery],
        runtime_kwarg: Dict[str, Any],
        *,
        prefer_data_source: Optional[str] = None,
        override_datasource: Optional[DatasourceConfig] = None,
    ) -> _Prepared:
        """Prepare portion shared by execute / evict / refresh (resolve→…→response-metadata).

        Produces the final executed SQL; no SQL client on the no-policy path (so
        ``evict()`` recomputes a key without connecting).
        """
        query = query.strip_source_model_prefix()
        named_queries = {
            name: q.strip_source_model_prefix()
            for name, q in named_queries.items()
        }

        if query.whole_periods_only:
            query = query.snap_to_whole_periods()

        # Build the resolved bundle once — the only storage consult; the binder
        # then reads from the bundle purely.
        bundle = await build_resolved_source_bundle(
            query=query,
            storage=self.storage,
            data_source=prefer_data_source,
            runtime_variables=runtime_kwarg,
            named_queries=named_queries,
        )

        # Expand every query-backed model in the bundle and re-apply root
        # inline_extensions. Shared with ``_expand_query_backed_model`` so both
        # surfaces consume the identical expansion contract.
        original_source_model = bundle.source_model
        bundle = await expand_query_backed_models_in_bundle(
            bundle=bundle,
            outer_vars=query.variables,
            runtime_kwarg=runtime_kwarg,
            dry_run_placeholders=False,
            expander=self._expand_query_backed_model,
        )
        # ``build_resolved_source_bundle`` raises if unresolved, so it's populated.
        model = bundle.source_model
        assert model is not None

        # ``override_datasource`` pins the connection identity (refresh re-exec):
        # re-run against the exact datasource the entry was cached under, so a
        # same-name repoint can't migrate the entry. Resolved here (before Mode-A
        # substitution) because escaping is dialect-aware; safe since substitution
        # never touches ``model.data_source``.
        datasource = override_datasource or await self._resolve_datasource(model=model)

        # Substitute {var} into the direct source model's Mode-A surfaces before
        # anything parses them; the substituted copy replaces the model as both
        # ``source_model`` and its ``referenced_models`` entry. Rendered virtual
        # models are skipped. Called unconditionally: a no-op for a variable-free,
        # block-free model, but a block-bearing model must run so its ``{? ?}``
        # collapse to ``(1=1)`` even on a zero-variable call.
        if model.source_model_origin is None:
            substituted = _substitute_model_sql_surfaces(
                model=model,
                variables=bundle.query_variables,
                dialect=dialect_for_ds_type(datasource.type),
            )
            bundle = bundle.model_copy(
                update={
                    "source_model": substituted,
                    "referenced_models": [
                        substituted if m.name == model.name else m
                        for m in bundle.referenced_models
                    ],
                }
            )
            model = substituted

        # Slack-normalization: rewrite slack-but-unambiguous input (func-style
        # aggs, misplaced measures) to canonical form, each stage against its
        # own resolved model.
        sibling_names = set(named_queries)
        query, slack_warnings = self._normalize_stage(
            query=query, bundle=bundle, sibling_names=sibling_names,
        )
        normed_named: Dict[str, SlayerQuery] = {}
        for nm, nq in named_queries.items():
            nq2, nq_warnings = self._normalize_stage(
                query=nq, bundle=bundle, sibling_names=sibling_names,
            )
            normed_named[nm] = nq2
            slack_warnings.extend(nq_warnings)

        # Substitute variables into filters. Root uses the bundle's merged
        # variables; each sibling re-merges its own stage layer.
        query = apply_variables_to_query(
            query=query, variables=bundle.query_variables,
        )
        root_vars = query.variables
        normed_named = {
            nm: apply_variables_to_query(
                query=nq,
                variables={
                    # Lowest layer: the stage's own source-model defaults
                    # (sibling-sourced stages fall back to the root model's).
                    **(
                        (
                            bundle.stage_source_models[nm].query_variables
                            if nm in bundle.stage_source_models
                            else (model.query_variables if model else None)
                        )
                        or {}
                    ),
                    **(root_vars or {}),
                    **(nq.variables or {}),
                    **(runtime_kwarg or {}),
                },
            )
            for nm, nq in normed_named.items()
        }

        # Plan the DAG (root last) and render the whole chain to one SQL string.
        stages = [*normed_named.values(), query]
        planned_list = plan_stages(queries=stages, bundle=bundle)
        root_planned = planned_list[-1]

        # Collect + dedup dropped-filter payloads across every plan (nested
        # subplans included). ``plan_stages`` returns plans topo-ordered — align
        # the stage list the same way so each warning names its own stage.
        ordered_stages = _topo_sort(stages) if len(stages) > 1 else stages
        dropped_warnings = _collect_dropped_filter_warnings(
            planned_list=planned_list, stages=ordered_stages,
        )
        broadcast_warnings = _collect_broadcast_warnings(
            planned_list=planned_list, stages=ordered_stages,
        )
        if getattr(query, "strict", False):
            _raise_on_strict_events(
                broadcasts=broadcast_warnings, dropped=dropped_warnings,
            )
        slack_warnings.extend(dropped_warnings)
        slack_warnings.extend(broadcast_warnings)

        dialect = self._dialect_for_type(datasource.type)
        sql = generate_planned_stages(
            planned_list, bundle=bundle, dialect=dialect,
            # Plan-derived canonical projection keys drive the write-side length
            # fit; the read side decodes against the same set.
            projection_aliases=projection_result_keys(root_planned=planned_list[-1]),
        )
        # Forced-filter rewrite before dry-run / explain / execute so all three
        # (and the cache key) see the policy-rewritten SQL; no-op without a policy.
        await self._preflight_clickhouse_correlated(
            dialect=dialect, datasource=datasource
        )
        sql = self._apply_policy(sql=sql, dialect=dialect, datasource=datasource)
        logger.debug("Generated SQL:\n%s", sql)

        attributes, expected_columns = build_response_metadata(
            root_planned=root_planned, bundle=bundle, sql=sql, dialect=dialect,
        )

        # Models whose schema a query-time DBAPI error could be attributed to.
        touched = self._touched_models_for_plan(
            bundle=bundle,
            planned_list=planned_list,
            original_source_model=original_source_model,
        )

        return _Prepared(
            sql=sql,
            dialect=dialect,
            datasource=datasource,
            resolved_data_source=datasource.name,
            attributes=attributes,
            expected_columns=list(expected_columns),
            touched=touched,
            model=model,
            slack_warnings=slack_warnings,
        )

    @staticmethod
    def _ds_fingerprint(datasource: DatasourceConfig) -> str:
        """Cache-key datasource fingerprint (``connection_string|runtime_fingerprint``)."""
        return "|".join(_sql_client_cache_key(datasource))

    def _client_for(self, datasource: DatasourceConfig) -> SlayerSQLClient:
        """Reuse (or lazily construct) the cached SQL client for a datasource."""
        ds_key = _sql_client_cache_key(datasource)
        if ds_key not in self._sql_clients:
            self._sql_clients[ds_key] = SlayerSQLClient(datasource=datasource)
        return self._sql_clients[ds_key]

    async def _execute_pipeline(
        self,
        query: SlayerQuery,
        named_queries: Dict[str, SlayerQuery],
        runtime_kwarg: Dict[str, Any],
        *,
        dry_run: bool = False,
        explain: bool = False,
        prefer_data_source: Optional[str] = None,
        cache: bool = False,
        original_input: Any = None,
        original_data_source: Optional[str] = None,
    ) -> SlayerResponse:
        """Prepare then dry-run / explain / cache-hook / execute (a miss scans baselines first, then stores)."""
        prepared = await self._prepare_pipeline(
            query=query,
            named_queries=named_queries,
            runtime_kwarg=runtime_kwarg,
            prefer_data_source=prefer_data_source,
        )

        # dry_run: return SQL without executing. NEVER cached.
        if dry_run:
            return SlayerResponse(
                data=[], columns=prepared.expected_columns, sql=prepared.sql,
                attributes=prepared.attributes, warnings=prepared.slack_warnings,
            )

        use_cache = cache and not dry_run and not explain
        # Bind the cache once so a concurrent ``cache_config`` reassignment can't
        # split the get / put across two caches.
        cache_obj = self._cache
        key: Optional[str] = None
        if use_cache:
            key = QueryCache.make_key(prepared.sql, self._ds_fingerprint(prepared.datasource))
            entry = await cache_obj.get(key)
            if entry is not None:
                # Deep copy so caller mutation can't poison the cached response.
                return entry.response.model_copy(deep=True)

        # Miss (or cache=False) → a SQL client is required.
        client = self._client_for(prepared.datasource)

        # explain: run dialect-appropriate EXPLAIN on the query. NEVER cached.
        if explain:
            explain_sql = _build_explain_sql(dialect=prepared.dialect, sql=prepared.sql)
            try:
                rows = await client.execute(sql=explain_sql)
            except Exception as exc:
                await self._maybe_raise_schema_drift(
                    err=exc, model=prepared.model, touched_models=prepared.touched
                )
                raise
            return SlayerResponse(
                data=rows, sql=prepared.sql, attributes=prepared.attributes,
                warnings=prepared.slack_warnings,
            )

        # Capture refresh-key baselines before the data query (cached data then
        # reflects a state >= the baseline); a scan failure propagates.
        applicable: list[tuple[str, str]] = []
        refresh_key_values: list[RefreshKeyValue] = []
        if use_cache:
            applicable, refresh_key_values = await self._scan_refresh_key_baselines(
                prepared=prepared, client=client, cache=cache_obj
            )

        rows = await self._run_data_query(prepared=prepared, client=client)
        columns = prepared.expected_columns if not rows else []  # [] triggers auto-derive
        response = SlayerResponse(
            data=rows, columns=columns, sql=prepared.sql,
            attributes=prepared.attributes, warnings=prepared.slack_warnings,
        )

        if use_cache:
            entry = self._build_cache_entry(
                prepared=prepared,
                response=response,
                original_input=original_input,
                variables=runtime_kwarg,
                data_source=original_data_source,
                created_at=cache_obj.now(),
                applicable=applicable,
                refresh_key_values=refresh_key_values,
            )
            await cache_obj.put(key, entry)
        return response

    async def _run_data_query(
        self, *, prepared: _Prepared, client: SlayerSQLClient
    ) -> "list[dict]":
        """Run the prepared data query (schema-drift attribution on error); decodes result keys."""
        try:
            rows = await client.execute(sql=prepared.sql)
        except Exception as exc:
            await self._maybe_raise_schema_drift(
                err=exc, model=prepared.model, touched_models=prepared.touched
            )
            raise
        # Pass canonical aliases so length-fitted keys are restored.
        return get_dialect(prepared.dialect).decode_result_keys(
            rows, aliases=prepared.expected_columns,
        )

    async def _scan_one_table_values(
        self,
        *,
        table: str,
        exprs: "list[str]",
        dialect: str,
        datasource: DatasourceConfig,
        client: SlayerSQLClient,
    ) -> "dict[str, Any]":
        """One batched refresh-key scan → ``{expression: value}``; policy-rewritten like the data query."""
        scan_sql = self._cache.build_refresh_key_sql(table, exprs, dialect)
        scan_sql = self._apply_policy(sql=scan_sql, dialect=dialect, datasource=datasource)
        rows = await client.execute(sql=scan_sql)
        row0 = rows[0] if rows else {}
        return {e: row0.get(self._cache.rk_alias(i)) for i, e in enumerate(exprs)}

    async def _scan_refresh_key_baselines(
        self, *, prepared: _Prepared, client: SlayerSQLClient, cache: QueryCache
    ) -> "tuple[list[tuple[str, str]], list[RefreshKeyValue]]":
        """Capture write-time refresh-key baselines ``(applicable, refresh_key_values)`` (same order; failures propagate)."""
        applicable = cache.applicable_keys(prepared.sql, prepared.dialect)
        if not applicable:
            return [], []
        by_table = self._group_expressions_by_table(applicable)
        scanned: dict[str, dict[str, Any]] = {}
        for table, exprs in by_table.items():
            scanned[table] = await self._scan_one_table_values(
                table=table, exprs=exprs, dialect=prepared.dialect,
                datasource=prepared.datasource, client=client,
            )
        values = [
            RefreshKeyValue(table=t, expression=e, value=scanned[t][e])
            for (t, e) in applicable
        ]
        return applicable, values

    @staticmethod
    def _group_expressions_by_table(
        applicable: "list[tuple[str, str]]",
    ) -> "dict[str, list[str]]":
        """Collate ``(table, expression)`` pairs into ``{table: [exprs]}``, deduped, order-stable."""
        by_table: dict[str, list[str]] = {}
        for table, expr in applicable:
            exprs = by_table.setdefault(table, [])
            if expr not in exprs:
                exprs.append(expr)
        return by_table

    def _build_cache_entry(
        self,
        *,
        prepared: _Prepared,
        response: SlayerResponse,
        original_input: Any,
        variables: Optional[Dict[str, Any]],
        data_source: Optional[str],
        created_at: float,
        applicable: "list[tuple[str, str]]",
        refresh_key_values: "list[RefreshKeyValue]",
    ) -> _CacheEntry:
        """Build a ``_CacheEntry`` with deep copies of the response and original input."""
        ds_key = _sql_client_cache_key(prepared.datasource)
        return _CacheEntry(
            response=response.model_copy(deep=True),
            sql=prepared.sql,
            ds_fingerprint="|".join(ds_key),
            dialect=prepared.dialect,
            ds_key=ds_key,
            resolved_data_source=prepared.resolved_data_source,
            original_input=copy.deepcopy(original_input),
            # Deep copy so nested values can't be mutated into a refresh replay.
            variables=copy.deepcopy(dict(variables)) if variables else None,
            data_source=data_source,
            created_at=created_at,
            applicable=list(applicable),
            refresh_key_values=list(refresh_key_values),
        )

    async def evict(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        *,
        data_source: Optional[str] = None,
    ) -> bool:
        """Remove one cached entry, recomputing its key DB-free; ``True`` if present."""
        runtime_kwarg = variables or {}
        main_query, named_queries, prefer_ds = await self._normalize_input(
            query, runtime_kwarg=runtime_kwarg, prefer_data_source=data_source
        )
        prepared = await self._prepare_pipeline(
            query=main_query,
            named_queries=named_queries,
            runtime_kwarg=runtime_kwarg,
            prefer_data_source=prefer_ds,
        )
        key = QueryCache.make_key(prepared.sql, self._ds_fingerprint(prepared.datasource))
        return await self._cache.delete(key)

    def evict_sync(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        *,
        data_source: Optional[str] = None,
    ) -> bool:
        """Synchronous wrapper for :meth:`evict`."""

        async def _run() -> bool:
            try:
                return await self.evict(query, variables=variables, data_source=data_source)
            finally:
                await self.aclose()

        return run_sync(_run())

    async def _reexecute_entry(self, entry: _CacheEntry, now: float) -> _CacheEntry:
        """Re-prepare + re-execute a stale entry, pinning its connection identity via
        ``override_datasource`` so no config change migrates it; raises if the client is gone."""
        client = self._sql_clients.get(entry.ds_key)
        if client is None:
            raise RuntimeError(
                f"no cached SQL client for datasource fingerprint {entry.ds_key!r}; "
                "cannot pin re-execution to the entry's connection identity"
            )
        main_query, named_queries, prefer_ds = await self._normalize_input(
            entry.original_input,
            runtime_kwarg=entry.variables or {},
            prefer_data_source=entry.resolved_data_source,
        )
        prepared = await self._prepare_pipeline(
            query=main_query,
            named_queries=named_queries,
            runtime_kwarg=entry.variables or {},
            prefer_data_source=prefer_ds,
            override_datasource=client.datasource,
        )
        applicable, refresh_key_values = await self._scan_refresh_key_baselines(
            prepared=prepared, client=client, cache=self._cache
        )
        rows = await self._run_data_query(prepared=prepared, client=client)
        columns = prepared.expected_columns if not rows else []
        response = SlayerResponse(
            data=rows, columns=columns, sql=prepared.sql,
            attributes=prepared.attributes, warnings=prepared.slack_warnings,
        )
        return self._build_cache_entry(
            prepared=prepared,
            response=response,
            original_input=entry.original_input,
            variables=entry.variables,
            data_source=entry.data_source,
            created_at=now,
            applicable=applicable,
            refresh_key_values=refresh_key_values,
        )

    async def refresh(self) -> RefreshResult:  # NOSONAR S3776 — Cube-style refresh: snapshot → collate scans → per-entry TTL/refresh-key decision
        """Cube-style explicit refresh over all cached entries.

        Per entry: TTL-expired ⇒ re-exec (``expired_refreshed``); applicable scan
        failed ⇒ ``unchanged``; a refresh-key value moved ⇒ re-exec (``refreshed``);
        else ``unchanged``. Errors become :class:`RefreshError`, keeping the entry.
        """
        result = RefreshResult()
        snapshot = await self._cache.snapshot()
        if not snapshot:
            return result

        # Collate {ds_key: {table: ordered exprs}}, keyed by SQL-client
        # fingerprint (not the bare name) so each entry scans its own identity.
        collate: dict[tuple[str, str], dict[str, list[str]]] = {}
        for entry in snapshot.values():
            if not entry.applicable:
                continue
            tables = collate.setdefault(entry.ds_key, {})
            for table, expr in entry.applicable:
                exprs = tables.setdefault(table, [])
                if expr not in exprs:
                    exprs.append(expr)

        # One batched scan per (ds_key, table), continue-on-error per table,
        # through the write-time client (no name re-resolution).
        scanned: dict[tuple[tuple[str, str], str], dict[str, Any]] = {}
        failed: set[tuple[tuple[str, str], str]] = set()
        for ds_key, tables in collate.items():
            client = self._sql_clients.get(ds_key)
            for table, exprs in tables.items():
                if client is None:
                    # Write-time client gone (shouldn't happen). Fail-soft: keep
                    # the entry rather than scan a re-resolved, wrong database.
                    failed.add((ds_key, table))
                    result.errors.append(RefreshError(
                        key=table, phase="refresh_key_scan",
                        message=f"no cached SQL client for datasource fingerprint {ds_key!r}",
                    ))
                    continue
                try:
                    datasource = client.datasource
                    dialect = self._dialect_for_type(datasource.type)
                    # Warm the ClickHouse version cache before policy-applying the
                    # scan SQL so a join-policy refresh matches normal execution.
                    await self._preflight_clickhouse_correlated(
                        dialect=dialect, datasource=datasource
                    )
                    scanned[(ds_key, table)] = await self._scan_one_table_values(
                        table=table, exprs=exprs, dialect=dialect,
                        datasource=datasource, client=client,
                    )
                except Exception as exc:
                    failed.add((ds_key, table))
                    result.errors.append(RefreshError(
                        key=table, phase="refresh_key_scan", message=str(exc),
                    ))

        for key, entry in snapshot.items():
            now = self._cache.now()
            ttl = self._cache.config.ttl_seconds
            ttl_expired = ttl is not None and (now - entry.created_at) > ttl
            if ttl_expired:
                bucket = result.expired_refreshed
            else:
                dk = entry.ds_key
                if any((dk, t) in failed for (t, _e) in entry.applicable):
                    result.unchanged.append(key)
                    continue
                moved = any(
                    QueryCache.values_differ(
                        scanned.get((dk, rkv.table), {}).get(rkv.expression),
                        rkv.value,
                    )
                    for rkv in entry.refresh_key_values
                )
                if not moved:
                    result.unchanged.append(key)
                    continue
                bucket = result.refreshed

            # Bucket only after a successful re-exec AND a landed commit: a
            # failure or a guard-skipped commit must not report as refreshed.
            try:
                new_entry = await self._reexecute_entry(entry, now)
            except Exception as exc:
                result.errors.append(RefreshError(
                    key=key, phase="re_execute", message=str(exc),
                ))
                continue
            new_key = QueryCache.make_key(new_entry.sql, new_entry.ds_fingerprint)
            replaced = await self._cache.commit_replace(
                old_key=key, expected=entry, new_key=new_key, new_entry=new_entry,
            )
            if replaced:
                bucket.append(key)

        return result

    def refresh_sync(self) -> RefreshResult:
        """Synchronous wrapper for :meth:`refresh`."""

        async def _run() -> RefreshResult:
            try:
                return await self.refresh()
            finally:
                await self.aclose()

        return run_sync(_run())

    def _normalize_stage(
        self,
        *,
        query: SlayerQuery,
        bundle: ResolvedSourceBundle,
        sibling_names: "set[str]",
    ) -> "tuple[SlayerQuery, list[NormalizationWarning]]":
        """Slack-normalize one stage against its resolved model (sibling-sourced → ``model=None``)."""
        sm = query.source_model
        model: Optional[SlayerModel] = None
        if query.name and query.name in bundle.stage_source_models:
            # A named non-root stage normalizes against its own source model, not
            # the root; sibling-sourced stages fall through to model=None.
            model = bundle.stage_source_models[query.name]
        elif isinstance(sm, str):
            if sm not in sibling_names:
                model = bundle.get_referenced_model(sm)
                if model is None and (
                    bundle.source_model is not None
                    and bundle.source_model.name == sm
                ):
                    model = bundle.source_model
        else:
            model = bundle.source_model
        norm = normalize_query(query, model=model)
        out = norm.query if norm.query is not None else query
        return out, list(norm.warnings)

    def _touched_models_for_plan(
        self,
        *,
        bundle: ResolvedSourceBundle,
        planned_list: "list[PlannedQuery]",
        original_source_model: Optional[SlayerModel],
    ) -> "set[str]":
        """Names of every model this query touched, for schema-drift attribution."""
        touched: set[str] = {m.name for m in bundle.referenced_models}
        for pq in planned_list:
            for attach in _walk_regroup_attaches(pq):
                if attach.producer_root_model:
                    touched.add(attach.producer_root_model)
        if original_source_model is not None and original_source_model.source_queries:
            touched.add(original_source_model.name)
            touched |= self._collect_query_backed_base_names(original_source_model)
        return touched

    @staticmethod
    def _collect_query_backed_base_names(model: SlayerModel) -> "set[str]":
        """Base model names referenced by a query-backed model's stages (sources + joins)."""
        out: set[str] = set()
        if not model.source_queries:
            return out
        stages = list(model.source_queries)
        stage_names = {
            getattr(s, "name", None) for s in stages if getattr(s, "name", None)
        }
        for stage in stages:
            sm = getattr(stage, "source_model", None)
            if isinstance(sm, str) and sm not in stage_names:
                out.add(sm)
            elif isinstance(sm, SlayerModel):
                out.add(sm.name)
            # Joins live on the stage's source_model (a ModelExtension); getattr
            # makes plain str / SlayerModel source_models no-ops.
            for j in (getattr(sm, "joins", None) or []):
                target = getattr(j, "target_model", None)
                if target is not None:
                    out.add(target)
        return out

    async def _expand_join_graph(
        self, *, touched: "set[str]", data_source: Optional[str]
    ) -> None:
        """Add transitively-reachable join targets to ``touched`` (visited-guarded)."""
        frontier = list(touched)
        visited: set[str] = set()
        while frontier:
            name = frontier.pop()
            if name in visited:
                continue
            visited.add(name)
            try:
                m = await self.storage.get_model(name, data_source=data_source)
            except Exception:
                m = None
            if m is None:
                continue
            for j in m.joins:
                if j.target_model not in touched:
                    touched.add(j.target_model)
                    frontier.append(j.target_model)

    async def _maybe_raise_schema_drift(
        self,
        *,
        err: BaseException,
        model: SlayerModel,
        touched_models: "set[str]",
    ) -> None:
        """Raise ``SchemaDriftError`` if ``err`` is attributable to drift in the touched
        models; else return so the caller re-raises. ``validate_models`` errors are swallowed."""

        try:
            touched = set(touched_models)
            await self._expand_join_graph(
                touched=touched, data_source=model.data_source or None
            )
            # Cross-DS joins are rejected at resolve time, so attribution only
            # needs the parent's data_source.
            data_sources: set[str] = {model.data_source} if model.data_source else set()

            collected: List[Any] = []
            for ds_name in data_sources or {None}:
                try:
                    entries = await self.validate_models(data_source=ds_name)
                except Exception as inner:
                    logger.debug(
                        "validate_models attribution failed for ds=%r: %s",
                        ds_name,
                        inner,
                    )
                    continue
                collected.extend(entries)
            # An "invalid_sql" entry is not drift evidence — it restates the
            # query failure itself, so the original error must propagate.
            filtered = [
                e
                for e in collected
                if getattr(e, "model_name", None) in touched
                and getattr(e, "cause", "schema_drift") != "invalid_sql"
            ]
            if filtered:
                raise SchemaDriftError(
                    models=sorted(touched),
                    to_delete=filtered,
                    original=err,
                )
        except SchemaDriftError:
            raise
        except Exception as inner:
            logger.debug(
                "schema-drift attribution swallowed an internal error: %s",
                inner,
            )

    def _build_type_probe_query(self, model: SlayerModel) -> SlayerQuery:
        """SlayerQuery type-probing a model's columns (prefers ``max``, skips primary keys)."""
        measures: List[ModelMeasure] = []
        for c in model.columns:
            if c.hidden or c.primary_key:
                continue
            if c.allowed_aggregations is not None:
                allowed = list(c.allowed_aggregations)
            else:
                allowed = sorted(DEFAULT_AGGREGATIONS_BY_TYPE.get(c.type, frozenset()))
            if not allowed:
                continue
            agg = "max" if "max" in allowed else allowed[0]
            measures.append(ModelMeasure(formula=f"{c.name}:{agg}"))
        return SlayerQuery(source_model=model.name, measures=measures)

    async def get_column_types(  # NOSONAR(S3776) — linear probe pipeline: query-backed prelude → bundle → expand-nested → plan → render → execute → result-key map-back. Splitting hides the order; each step is its own try/except + early-return so flatness is the easier read.
        self,
        model_name: str,
        data_source: Optional[str] = None,
    ) -> Dict[str, str]:
        """Infer column types via a type-probe query → {column: "number"|"string"|"time"|"boolean"}."""
        model = await self.storage.get_model(model_name, data_source=data_source)
        if model is None:
            return {}

        # Expand query-backed models first so the resolved virtual model (fresh
        # data_source + inner columns) drives datasource + probeable-columns, not
        # a stale stored value.
        if model.source_queries:
            try:
                # No caller variables here; fill placeholders with ``0`` (the
                # save-time render) so an undefaulted {var} doesn't fail SQL-gen.
                model = await self._resolve_model(
                    model_name=model_name,
                    dry_run_placeholders=True,
                    prefer_data_source=model.data_source or data_source,
                )
            except Exception:
                logger.warning(
                    "get_column_types: failed to resolve query-backed model '%s'",
                    model_name,
                )
                return {}

        probeable = [c for c in model.columns if not c.hidden and not c.primary_key]
        if not probeable:
            return {}

        try:
            datasource = await self._resolve_datasource(model=model)
        except ValueError:
            return {}

        ds_key = _sql_client_cache_key(datasource)
        if ds_key not in self._sql_clients:
            self._sql_clients[ds_key] = SlayerSQLClient(datasource=datasource)
        client = self._sql_clients[ds_key]

        probe_query = self._build_type_probe_query(model=model)
        # An expanded (query-backed) model is virtual sql-mode — pass it inline
        # so the bundle uses the expanded shape, not stale storage.
        if not model.source_queries:
            probe_query = probe_query.model_copy(update={"source_model": model})
        try:
            # Render a template model's Mode-A {var} surfaces from its own
            # defaults before probe SQL; an undefaulted {var} degrades to {}.
            model = _render_probe_model(
                model, dialect=dialect_for_ds_type(datasource.type)
            )
            if not model.source_queries:
                probe_query = probe_query.model_copy(update={"source_model": model})
            bundle = await build_resolved_source_bundle(
                query=probe_query,
                storage=self.storage,
                data_source=model.data_source or None,
                runtime_variables={},
                named_queries={},
            )
            # Expand nested query-backed models so the planner sees sql-mode shapes.
            bundle = await expand_query_backed_models_in_bundle(
                bundle=bundle,
                outer_vars=None,
                runtime_kwarg=None,
                dry_run_placeholders=True,
                expander=self._expand_query_backed_model,
            )
            planned = plan_stages(queries=[probe_query], bundle=bundle)
            root = planned[-1]
            dialect = self._dialect_for_type(datasource.type)
            sql = generate_planned_stages(
                planned, bundle=bundle, dialect=dialect,
                projection_aliases=projection_result_keys(root_planned=root),
            )
            # Type probing honours the forced-filter policy too; a policy failure
            # degrades to {} rather than leaking an unscoped probe.
            await self._preflight_clickhouse_correlated(
                dialect=dialect, datasource=datasource
            )
            sql = self._apply_policy(sql=sql, dialect=dialect, datasource=datasource)
        except Exception:
            logger.warning(
                "get_column_types plan/generate failed for model '%s'",
                model_name,
            )
            return {}

        try:
            raw_types = await client.get_column_types(sql=sql)
        except Exception:
            logger.warning(
                "get_column_types probe failed for model '%s'", model_name,
            )
            return {}

        # Decode emitted (alias-mangled / length-fitted) keys back to canonical
        # dotted form for the ``full`` lookups below.
        raw_types = get_dialect(dialect).decode_result_keys(
            [raw_types], aliases=projection_result_keys(root_planned=root),
        )[0]

        # Map qualified aliases back to bare measure names.
        result: Dict[str, str] = {}
        source_relation = root.source_relation
        for slot in root.aggregate_slots:
            if slot.hidden:
                continue
            src = getattr(slot.key, "source", None)
            bare = (
                getattr(src, "leaf", None)
                or getattr(src, "column_name", None)
            )
            if bare is None:
                continue
            public = slot.public_name or slot.declared_name
            full = f"{source_relation}.{public}"
            if full in raw_types:
                result[bare] = raw_types[full]
        # A measure answered by a regroup producer surfaces as a placeholder ROW
        # slot; its source lives on the substitution's original key.
        slot_by_key = {s.key: s for s in root.row_slots}
        for attach in root.regroup_attach_plans:
            for sub in attach.substitutions:
                ph_slot = slot_by_key.get(sub.placeholder)
                if ph_slot is None or ph_slot.hidden:
                    continue
                src = getattr(sub.original_key, "source", None)
                bare = (
                    getattr(src, "leaf", None)
                    or getattr(src, "column_name", None)
                )
                if bare is None or bare in result:
                    continue
                public = ph_slot.public_name or ph_slot.declared_name
                full = f"{source_relation}.{public}"
                if full in raw_types:
                    result[bare] = raw_types[full]
        return result

    def execute_sync(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        *,
        dry_run: bool = False,
        explain: bool = False,
        data_source: Optional[str] = None,
        cache: bool = False,
    ) -> SlayerResponse:
        """Synchronous wrapper for execute(); disposes per-call async engines in ``finally``."""

        async def _run_and_cleanup() -> SlayerResponse:
            try:
                return await self.execute(
                    query, variables=variables, dry_run=dry_run,
                    explain=explain, data_source=data_source, cache=cache,
                )
            finally:
                await self.aclose()

        return run_sync(_run_and_cleanup())

    async def _scope_bare_name_to_datasource(
        self, *, raw: str, name: str, data_source: str
    ) -> "tuple[str, str, str]":
        """Resolve a dotless item within one datasource → ``(ds, model, leaf)``."""
        models = await _all_models_in_datasource(self.storage, data_source)
        if any(m.name == name for m in models):
            raise ValueError(
                f"'{raw}' resolves to model '{name}' in '{data_source}', which "
                f"is not a column or metric. recommend_root_model needs "
                f"'model.column' / 'model.metric' items."
            )
        # Ownership is column/metric only; a same-named custom aggregation drives
        # only the aggregation-specific error below, never ambiguity.
        owners = [
            m for m in models
            if m.get_column(name) is not None or m.get_measure(name) is not None
        ]
        if len(owners) == 1:
            return data_source, owners[0].name, name
        if len(owners) > 1:
            names = sorted(m.name for m in owners)
            raise ValueError(
                f"'{raw}' is ambiguous in datasource '{data_source}' — matches "
                f"{names}. Qualify it as '<model>.{name}'."
            )
        if any(m.get_aggregation(name) is not None for m in models):
            raise ValueError(
                f"'{raw}' names a custom aggregation in datasource "
                f"'{data_source}'. Aggregations are operators applied to "
                f"columns, not join-path targets — pass the column."
            )
        raise ValueError(
            f"'{raw}' does not name a column or metric in datasource "
            f"'{data_source}'."
        )

    async def _resolve_recommend_item(
        self, *, raw: str, data_source: str | None
    ) -> "tuple[_ResolvedItem, list[str]]":
        """Resolve + validate one recommend_root_model item; must be a column or metric."""
        entity_ref, suffix = split_entity_agg_ref(raw)
        warnings: list[str] = []
        if data_source and "." not in entity_ref:
            ds, model_name, leaf = await self._scope_bare_name_to_datasource(
                raw=raw, name=entity_ref, data_source=data_source,
            )
        else:
            # Force a dotted item into ``data_source`` unless it already leads
            # with it, so a model colliding with a datasource name still resolves
            # and a foreign-datasource ref fails as cross-datasource.
            resolution_input = entity_ref
            if data_source and entity_ref.split(".")[0] != data_source:
                resolution_input = f"{data_source}.{entity_ref}"
            res = await resolve_entity(resolution_input, storage=self.storage)
            warnings = res.warnings
            segs = res.canonical_forms[0].split(".")
            if len(segs) < 3:
                raise ValueError(
                    f"'{raw}' resolves to '{res.canonical_forms[0]}', which is "
                    f"not a column or metric. recommend_root_model needs "
                    f"'model.column' / 'model.metric' items."
                )
            ds, model_name, leaf = segs[0], segs[1], segs[2]
        owning = await self.storage.get_model(model_name, data_source=ds)
        if owning is None:
            raise ValueError(
                f"'{raw}' resolves to model '{model_name}' in '{ds}', which "
                f"is not a saved model."
            )
        if owning.get_column(leaf) is None and owning.get_measure(leaf) is None:
            if owning.get_aggregation(leaf) is not None:
                raise ValueError(
                    f"'{raw}' names the custom aggregation '{leaf}' on "
                    f"'{model_name}'. Aggregations are operators applied to "
                    f"columns, not join-path targets — pass the column."
                )
            raise ValueError(
                f"'{raw}' does not name a column or metric on '{model_name}'."
            )
        item = _ResolvedItem(
            input_item=raw, data_source=ds, model=model_name,
            leaf=leaf, suffix=suffix,
        )
        return item, warnings

    async def _recommend_resolve_items(
        self, *, items: list[str], data_source: str | None
    ) -> "tuple[list[_ResolvedItem], list[str]]":
        """Resolve every input item, then enforce single-datasource + dedup."""
        resolved: list[_ResolvedItem] = []
        warnings: list[str] = []
        seen_inputs: set[str] = set()
        for original in items:
            raw = original.strip()
            if not raw or raw in seen_inputs:
                continue
            seen_inputs.add(raw)
            item, item_warnings = await self._resolve_recommend_item(
                raw=raw, data_source=data_source,
            )
            resolved.append(item)
            warnings.extend(item_warnings)

        if not resolved:
            raise ValueError("recommend_root_model requires at least one item.")
        datasources = {r.data_source for r in resolved}
        if len(datasources) > 1:
            raise ValueError(
                f"items span multiple datasources {sorted(datasources)}; "
                f"cross-datasource queries are not supported. Pass items from "
                f"a single datasource (optionally set data_source=...)."
            )
        deduped_warnings: list[str] = []
        seen_w: set[str] = set()
        for w in warnings:
            if w not in seen_w:
                seen_w.add(w)
                deduped_warnings.append(w)
        return resolved, deduped_warnings

    async def recommend_root_model(
        self,
        items: list[str],
        *,
        data_source: str | None = None,
        root_hint: str | None = None,
    ) -> RootModelRecommendation:
        """Recommend the query root for ``model.column`` / ``model.metric`` items, plus each item's path.

        Selection minimizes total hops, prefers a mentioned model on ties, then the
        smallest name; no common root ⇒ ``reachable=False`` with a Pareto ``coverage``.
        A feasible ``root_hint`` overrides min-hops (forcing a bridge model); an
        infeasible one falls back with a warning, a malformed one raises.
        """
        resolved, base_warnings = await self._recommend_resolve_items(
            items=items, data_source=data_source
        )
        ds = resolved[0].data_source
        models = await _all_models_in_datasource(self.storage, ds)
        graph = JoinGraph.build_from_models(models)
        all_names = sorted(m.name for m in models)
        mentioned = {r.model for r in resolved}
        warnings = list(base_warnings)

        hint = _resolve_root_hint(root_hint, data_source=ds, all_names=all_names)
        hint_model = hint[0] if hint is not None else None
        hint_display = hint[1] if hint is not None else None

        def reaches_all(root: str) -> bool:
            return all(graph.shortest_path(root, m) is not None for m in mentioned)

        def missing_models(root: str) -> list[str]:
            return sorted(m for m in mentioned if graph.shortest_path(root, m) is None)

        auto = min_hops_root(graph, all_names, mentioned)
        if auto is not None:
            if hint_model is not None and reaches_all(hint_model):
                root = hint_model
                message = (
                    f"All items are reachable from '{root}' (requested via root_hint)."
                )
            else:
                root = auto
                if hint_model is not None:
                    missing = ", ".join(missing_models(hint_model))
                    warnings.append(
                        f"root_hint '{hint_display}' cannot reach {{{missing}}}; "
                        f"fell back to '{auto}'."
                    )
                message = f"All items are reachable from '{root}'."
            item_paths = [
                ItemPath(input_item=r.input_item, path=_emit_recommend_path(graph, root, r))
                for r in resolved
            ]
            return RootModelRecommendation(
                data_source=ds, root_model=root, reachable=True,
                item_paths=item_paths, warnings=warnings, message=message,
            )

        force_include = {hint_model} if hint_model is not None else None
        coverage = _build_recommend_coverage(
            graph, all_names, mentioned, resolved, force_include=force_include
        )
        if hint_model is not None:
            missing = ", ".join(missing_models(hint_model))
            warnings.append(
                f"root_hint '{hint_display}' cannot reach {{{missing}}}; "
                f"no single model reaches every item — see coverage."
            )
        return RootModelRecommendation(
            data_source=ds, root_model=None, reachable=False, item_paths=[],
            coverage=coverage, warnings=warnings,
            message=(
                "No single model reaches every requested item. See 'coverage' "
                "for the best partial roots — split the request into a "
                "multi-stage query rooted at those models."
            ),
        )

    def recommend_root_model_sync(
        self,
        items: list[str],
        *,
        data_source: str | None = None,
        root_hint: str | None = None,
    ) -> RootModelRecommendation:
        """Synchronous wrapper for :meth:`recommend_root_model`."""

        async def _run() -> RootModelRecommendation:
            try:
                return await self.recommend_root_model(
                    items, data_source=data_source, root_hint=root_hint
                )
            finally:
                await self.aclose()

        return run_sync(_run())


    async def edit_model_remove(
        self,
        *,
        model_name: str,
        data_source: Optional[str],
        remove_columns: Optional[List[str]] = None,
        remove_measures: Optional[List[str]] = None,
        remove_aggregations: Optional[List[str]] = None,
        remove_joins: Optional[List[str]] = None,
        remove_filters: Optional[List[str]] = None,
    ) -> SlayerModel:
        """Apply surgical removals (by name, plus verbatim filters) to a persisted model."""
        existing = await self.storage.get_model(model_name, data_source=data_source)
        if existing is None:
            raise ValueError(
                f"Model {model_name!r} not found in datasource {data_source!r}."
            )
        if existing.source_queries:
            # Query-backed models manage columns / backing_query_sql as cache;
            # bypassing save_model would persist stale, mismatched cache.
            raise ValueError(
                f"edit_model_remove() does not support query-backed models "
                f"({model_name!r}); edit source_queries via engine.save_model() "
                f"instead."
            )
        cols_to_remove = set(remove_columns or [])
        measures_to_remove = set(remove_measures or [])
        aggs_to_remove = set(remove_aggregations or [])
        joins_to_remove = set(remove_joins or [])
        filters_to_remove = list(remove_filters or [])

        new_columns = [c for c in existing.columns if c.name not in cols_to_remove]
        new_measures = [
            m for m in existing.measures if m.name not in measures_to_remove
        ]
        new_aggs = [a for a in existing.aggregations if a.name not in aggs_to_remove]
        new_joins = [
            j for j in existing.joins if j.target_model not in joins_to_remove
        ]
        new_filters = [f for f in existing.filters if f not in filters_to_remove]

        updated = existing.model_copy(
            update={
                "columns": new_columns,
                "measures": new_measures,
                "aggregations": new_aggs,
                "joins": new_joins,
                "filters": new_filters,
            }
        )
        SlayerModel.model_validate(updated.model_dump())
        await self.storage.save_model(updated)
        # Cascade-strip dropped leaves from memory entity tags. Joins/filters
        # don't cascade (a joined-leaf ref canonicalizes to the target's own id).
        existing_ds = existing.data_source
        for removed in (
            list(cols_to_remove)
            + list(measures_to_remove)
            + list(aggs_to_remove)
        ):
            await self.storage.strip_dangling_entities_from_memories(
                canonical_id=f"{existing_ds}.{model_name}.{removed}",
            )
        return updated

    async def delete_model_by_name(
        self, *, model_name: str, data_source: Optional[str]
    ) -> bool:
        """Delete a persisted model by name. Returns True if the model existed."""
        return await self.storage.delete_model(model_name, data_source=data_source)

    async def apply_drift_deletes(
        self, deletes: "List[Any]"
    ) -> "Any":
        """Apply each ``ToDeleteEntry`` → ``ApplyDriftResult``; per-entry failures land in ``errors``, ``residual`` from a re-validate."""
        applied: List[AppliedEntry] = []
        errors: List[ApplyError] = []
        touched_ds: set[str] = set()

        for entry in deletes:
            # Track the datasource up front so re-validation runs even if every
            # mutation on it fails.
            touched_ds.add(entry.data_source)
            try:
                if entry.tool == "delete_model":
                    await self.delete_model_by_name(
                        model_name=entry.model_name,
                        data_source=entry.data_source,
                    )
                elif entry.tool == "edit_model":
                    await self.edit_model_remove(
                        model_name=entry.model_name,
                        data_source=entry.data_source,
                        remove_columns=list(entry.remove.columns),
                        remove_measures=list(entry.remove.measures),
                        remove_aggregations=list(entry.remove.aggregations),
                        remove_joins=list(entry.remove.joins),
                        remove_filters=list(entry.remove_filters),
                    )
                else:
                    raise ValueError(f"Unknown delete tool: {entry.tool!r}")
                applied.append(
                    AppliedEntry(
                        tool=entry.tool,
                        model_name=entry.model_name,
                        data_source=entry.data_source,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — best-effort per-entry isolation
                errors.append(
                    ApplyError(
                        tool=entry.tool,
                        model_name=entry.model_name,
                        data_source=entry.data_source,
                        error=str(exc),
                    )
                )

        # Re-validate the touched datasources to compute residual drift.
        residual: List[Any] = []
        for ds_name in touched_ds:
            try:
                residual.extend(await self.validate_models(data_source=ds_name))
            except Exception as inner:
                logger.debug(
                    "post-apply validate_models failed for ds=%r: %s",
                    ds_name,
                    inner,
                )
        return ApplyDriftResult(
            applied=applied,
            errors=errors,
            residual=list(residual),
        )

    async def detect_join_cardinality(
        self,
        *,
        data_source: str | None = None,
        model: str | None = None,
        persist: bool = False,
    ) -> JoinCardinalityReport:
        """Profile each join's two sides and classify its cardinality (report-only unless ``persist``).

        Only ``sql_table`` models with bare-column join keys are profiled; the rest
        report ``SKIPPED_UNSUPPORTED``.
        """
        ds_names = (
            [data_source] if data_source
            else await self.storage.list_datasources()
        )
        findings: list[JoinCardinalityFinding] = []
        persist_map: dict[tuple[str, str], list[tuple]] = {}

        for ds_name in ds_names:
            ds_findings, ds_persist = await self._detect_datasource_joins(
                ds_name=ds_name, model=model,
            )
            findings.extend(ds_findings)
            for model_name, signature, detected in ds_persist:
                persist_map.setdefault((ds_name, model_name), []).append(
                    (signature, detected)
                )

        if persist:
            for (ds_name, model_name), items in persist_map.items():
                await self._persist_join_cardinality(
                    data_source=ds_name, model_name=model_name, items=items,
                )
        return JoinCardinalityReport(findings=findings)

    async def _resolve_detection_scope(self, *, ds_name, model):
        """In-scope models + a whole-datasource name lookup (join targets must still resolve)."""
        all_models = await _all_models_in_datasource(self.storage, ds_name)
        by_name = {m.name: m for m in all_models}
        if model is None:
            return all_models, by_name
        return ([by_name[model]] if model in by_name else []), by_name

    async def _detect_datasource_joins(
        self, *, ds_name, model,
    ) -> "tuple[list[JoinCardinalityFinding], list[tuple]]":
        """Profile every join of every in-scope model → ``(findings, persist_entries)``."""
        scope, by_name = await self._resolve_detection_scope(
            ds_name=ds_name, model=model,
        )
        if not scope:
            return [], []
        ds_cfg = await self.storage.get_datasource(ds_name)
        if ds_cfg is None:
            return [], []

        sqlglot_name = dialect_for_ds_type(ds_cfg.type).sqlglot_name
        findings: list[JoinCardinalityFinding] = []
        persist_entries: list[tuple] = []
        client = SlayerSQLClient(datasource=ds_cfg)
        try:
            for m in scope:
                for join in m.joins:
                    try:
                        finding, detected = await self._detect_one_join(
                            model=m, join=join, by_name=by_name,
                            client=client, sqlglot_name=sqlglot_name,
                            data_source=ds_name, datasource_cfg=ds_cfg,
                        )
                    except ForcedFilterError:
                        # Fail-closed: a report line would leak unscoped stats.
                        raise
                    except Exception as exc:  # noqa: BLE001
                        # Contain per join: one unreadable table mustn't fail all.
                        findings.append(
                            _scan_failed_finding(
                                data_source=ds_name, model=m, join=join, exc=exc,
                            )
                        )
                        continue
                    findings.append(finding)
                    if detected is not None:
                        persist_entries.append(
                            (m.name, _join_signature(join), detected)
                        )
        finally:
            await client.aclose()
        return findings, persist_entries

    async def _detect_one_join(
        self, *, model, join, by_name, client, sqlglot_name, data_source,
        datasource_cfg,
    ) -> "tuple[JoinCardinalityFinding, JoinCardinality | None]":
        pairs = [[p[0], p[1]] for p in join.join_pairs]
        src_cols = [p[0] for p in join.join_pairs]
        tgt_cols = [p[1] for p in join.join_pairs]
        target = by_name.get(join.target_model)

        note = _detection_skip_reason(
            model=model, target=target, src_cols=src_cols, tgt_cols=tgt_cols,
        )
        if note is not None:
            return JoinCardinalityFinding(
                data_source=data_source, model=model.name,
                target_model=join.target_model, join_pairs=pairs,
                stored=join.cardinality, detected=None,
                verdict=CardinalityVerdict.SKIPPED_UNSUPPORTED, note=note,
            ), None

        src_side = await self._side_stats(
            client=client, table=model.sql_table,
            key_cols=src_cols, sqlglot_name=sqlglot_name,
            datasource=datasource_cfg,
        )
        tgt_side = await self._side_stats(
            client=client, table=target.sql_table,
            key_cols=tgt_cols, sqlglot_name=sqlglot_name,
            datasource=datasource_cfg,
        )
        # 0 == 0 reads as observed_unique, so an empty side would falsely detect
        # one_to_one; no rows is no evidence.
        empty_sides = [
            name
            for name, side in (("source", src_side), ("target", tgt_side))
            if side.row_count == 0
        ]
        if empty_sides:
            return JoinCardinalityFinding(
                data_source=data_source, model=model.name,
                target_model=join.target_model, join_pairs=pairs,
                stored=join.cardinality, detected=None,
                source_side=src_side, target_side=tgt_side,
                verdict=CardinalityVerdict.NO_EVIDENCE,
                note=(
                    f"no non-null key rows on the {' and '.join(empty_sides)} "
                    f"side; an empty scan is no evidence of arity"
                ),
            ), None

        detected = classify_cardinality(
            source_unique=src_side.observed_unique,
            target_unique=tgt_side.observed_unique,
        )
        verdict = compute_verdict(
            stored=join.cardinality, detected=detected,
            source_observed_unique=src_side.observed_unique,
            target_observed_unique=tgt_side.observed_unique,
        )
        contradictions = _unique_contradictions(
            model=model, target=target, src_cols=src_cols, tgt_cols=tgt_cols,
            src_side=src_side, tgt_side=tgt_side,
        )
        return JoinCardinalityFinding(
            data_source=data_source, model=model.name,
            target_model=join.target_model, join_pairs=pairs,
            stored=join.cardinality, detected=detected,
            source_side=src_side, target_side=tgt_side,
            verdict=verdict, unique_contradictions=contradictions,
        ), detected

    @staticmethod
    def _side_stats_sql(*, table, key_cols, sqlglot_name) -> tuple[str, str]:
        """(row-count, distinct-count) profiling SQL; NULL key rows excluded from both."""
        tbl = exp.to_table(sql_path=table, dialect=sqlglot_name)
        cols = [exp.column(c, quoted=True) for c in key_cols]
        predicate = None
        for col in cols:
            term = exp.Not(this=exp.Is(this=col.copy(), expression=exp.null()))
            predicate = term if predicate is None else exp.and_(predicate, term)

        count_star = exp.func("COUNT", exp.Star()).as_("c")
        rows_q = exp.select(count_star).from_(tbl.copy()).where(predicate)
        inner = (
            exp.select(*[c.copy() for c in cols])
            .from_(tbl.copy())
            .where(predicate.copy())
            .distinct()
        )
        dist_q = exp.select(count_star.copy()).from_(inner.subquery("d"))
        return (
            rows_q.sql(dialect=sqlglot_name, identify=True),
            dist_q.sql(dialect=sqlglot_name, identify=True),
        )

    async def _side_stats(
        self, *, client, table, key_cols, sqlglot_name, datasource,
    ) -> SideStats:
        """Full-scan one side of a join: non-null key rows vs distinct key-tuples."""
        rows_sql, dist_sql = self._side_stats_sql(
            table=table, key_cols=key_cols, sqlglot_name=sqlglot_name,
        )
        # Give the correlated-subquery guard a version to gate on.
        await self._preflight_clickhouse_correlated(
            dialect=sqlglot_name, datasource=datasource
        )
        # Profile the tenant-scoped rows, like every execution path (no-op without a policy).
        rows_sql = self._apply_policy(
            sql=rows_sql, dialect=sqlglot_name, datasource=datasource
        )
        dist_sql = self._apply_policy(
            sql=dist_sql, dialect=sqlglot_name, datasource=datasource
        )
        row_rows = await client.execute(sql=rows_sql)
        dist_rows = await client.execute(sql=dist_sql)
        row_count = int(next(iter(row_rows[0].values())))
        distinct_count = int(next(iter(dist_rows[0].values())))
        return SideStats(
            row_count=row_count,
            distinct_count=distinct_count,
            observed_unique=(row_count == distinct_count),
        )

    async def _persist_join_cardinality(
        self, *, data_source, model_name, items,
    ) -> None:
        m = await self.storage.get_model(model_name, data_source=data_source)
        if m is None:
            return
        sig_to_detected = dict(items)
        changed = False
        new_joins = []
        for j in m.joins:
            d = sig_to_detected.get(_join_signature(j))
            if d is not None and j.cardinality != d:
                new_joins.append(j.model_copy(update={"cardinality": d}))
                changed = True
            else:
                new_joins.append(j)
        if changed:
            await self.storage.save_model(m.model_copy(update={"joins": new_joins}))

    async def validate_models(
        self, data_source: Optional[str] = None
    ) -> "List[Any]":
        """Diff persisted models against live schemas → minimal deletes (read-only; ``None`` = all datasources)."""

        if data_source is not None:
            ds = await self.storage.get_datasource(data_source)
            if ds is None:
                return []
            identities = await self.storage._list_all_model_identities()
            ds_model_names = [n for d, n in identities if d == data_source]
            models: List[SlayerModel] = []
            for name in ds_model_names:
                m = await self.storage.get_model(name, data_source=data_source)
                if m is not None:
                    models.append(m)
            return await validate_datasource(
                datasource=ds,
                models=models,
                sql_clients=self._sql_clients,
            )

        ds_names = await self.storage.list_datasources()
        if not ds_names:
            return []

        async def _validate_one(name: str) -> "List[ToDeleteEntry]":
            return await self.validate_models(data_source=name)

        results = await asyncio.gather(
            *(_validate_one(n) for n in ds_names), return_exceptions=True
        )
        out: List = []
        for r in results:
            if isinstance(r, BaseException):
                logger.warning("validate_models: per-DS validation failed: %s", r)
                continue
            out.extend(r)
        return out

    def create_model_from_query_sync(
        self,
        query: "SlayerQuery | list[SlayerQuery] | dict | list[dict]",
        name: str,
        description: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        save: bool = True,
    ) -> SlayerModel:
        """Synchronous wrapper for create_model_from_query()."""

        return run_sync(
            self.create_model_from_query(
                query=query,
                name=name,
                description=description,
                variables=variables,
                save=save,
            )
        )

    async def _expand_query_backed_model(  # NOSONAR S3776 — linear render pipeline (topo-sort → bundle → expand-nested → normalize → variables → plan → render → wrap); splitting hides the order of operations
        self,
        model: SlayerModel,
        outer_vars: Optional[Dict[str, Any]],
        runtime_kwarg: Optional[Dict[str, Any]],
        dry_run_placeholders: bool,
        _resolving: Optional[set],
    ) -> SlayerModel:
        """Expand a query-backed ``model`` into a virtual ``sql``-mode model (read-only).

        Mirrors ``_execute_pipeline``'s mid-section, wrapping the backing SQL in a
        flat-rename SELECT; nested targets recurse via ``expand_query_backed_models_in_bundle``.
        """
        if not model.source_queries:
            return model

        # Topo-sort + validate (root-as-sink, joins.target_model, inline-nested).
        stages = topologically_order_stages(list(model.source_queries))
        final_stage = stages[-1]
        named_q = {q.name: q for q in stages[:-1] if q.name}

        # Build the bundle for the final stage WITHOUT a DS hint, so inner
        # resolution falls back to the priority-list resolver — letting
        # ``get_column_types`` recover from a stale persisted ``data_source``.
        bundle = await build_resolved_source_bundle(
            query=final_stage,
            storage=self.storage,
            data_source=None,
            runtime_variables=runtime_kwarg,
            outer_variables={**model.query_variables, **(outer_vars or {})},
            named_queries=named_q,
        )

        # Expand nested query-backed models. ``_resolving`` propagates in-flight
        # names so a target referencing its parent short-circuits via cached SQL.
        # Pass the bundle's MERGED variables (not the bare stage dict) so nested
        # expansions keep the outer model's ``query_variables`` layer.
        bundle = await expand_query_backed_models_in_bundle(
            bundle=bundle,
            outer_vars=bundle.query_variables,
            runtime_kwarg=runtime_kwarg,
            dry_run_placeholders=dry_run_placeholders,
            expander=self._expand_query_backed_model,
            _resolving=(_resolving or set()) | {model.name},
        )

        # Per-stage normalize + variable substitution.
        sibling_names = set(named_q)
        final_stage, _slack = self._normalize_stage(
            query=final_stage, bundle=bundle, sibling_names=sibling_names,
        )
        normed_named: Dict[str, SlayerQuery] = {}
        for nm, nq in named_q.items():
            nq2, _ = self._normalize_stage(
                query=nq, bundle=bundle, sibling_names=sibling_names,
            )
            normed_named[nm] = nq2
        final_stage = apply_variables_to_query(
            query=final_stage,
            variables=bundle.query_variables,
            dry_run_placeholders=dry_run_placeholders,
        )
        # Sibling substitution needs the merged ``bundle.query_variables``, not
        # the bare stage dict (which drops the ``model.query_variables`` layer).
        normed_named = {
            nm: apply_variables_to_query(
                query=nq,
                variables={
                    **(
                        (
                            bundle.stage_source_models[nm].query_variables
                            if nm in bundle.stage_source_models
                            else (
                                bundle.source_model.query_variables
                                if bundle.source_model else None
                            )
                        )
                        or {}
                    ),
                    **(bundle.query_variables or {}),
                    **(nq.variables or {}),
                    **(runtime_kwarg or {}),
                },
                dry_run_placeholders=dry_run_placeholders,
            )
            for nm, nq in normed_named.items()
        }

        # Plan + render the DAG.
        plan_input = [*normed_named.values(), final_stage]
        planned_list = plan_stages(queries=plan_input, bundle=bundle)
        root_planned = planned_list[-1]
        inner_source_model = bundle.source_model
        assert inner_source_model is not None
        datasource = await self._resolve_datasource(model=inner_source_model)
        dialect = self._dialect_for_type(datasource.type)
        # Backing SQL is persisted on the virtual model, so length-fit here too.
        aliases = projection_result_keys(root_planned=root_planned)
        rendered = generate_planned_stages(
            planned_queries=planned_list, bundle=bundle, dialect=dialect,
            projection_aliases=aliases,
        )

        # Wrap with a flat-renamed SELECT; public StageColumn entries only
        # (hoisted hidden slots are internal intermediates, never columns).
        public_cols = [
            c for c in (
                root_planned.stage_schema.columns
                if root_planned.stage_schema is not None else []
            )
            if c.public_alias is not None
        ]
        expected = [c.name for c in public_cols]
        wrapped_ast = build_flat_rename_wrapper(
            source_relation=root_planned.source_relation,
            stage_sql=rendered,
            expected_columns=expected,
            dialect=dialect,
            projection_aliases=aliases,
        )
        wrapped_sql = wrapped_ast.sql(dialect=dialect, pretty=True)

        # Build the virtual model. Slot types drive Column.type; ``Column.sql``
        # carries the length-fitted alias while ``Column.name`` stays canonical.
        fit_map = get_dialect(dialect).alias_rewrite_map(expected)
        # Stamp grain uniqueness only when the backing query provably dedups it
        # (aggregates, or dimension-only with ``distinct_dimension_values``).
        stamp_grain = bool(root_planned.aggregate_slots) or (
            final_stage.distinct_dimension_values
        )
        # A combined regroup attach is a ROW-phase placeholder outside the grain
        # — exclude it so its column is never stamped as a key.
        grain_public_names = {
            s.public_name for s in root_planned.row_slots
            if s.public_name is not None
            and not str(getattr(s.key, "leaf", "")).startswith(REGROUP_LEAF_PREFIX)
        }
        cols = [
            Column(
                name=sc.name,
                sql=fit_map.get(sc.name, sc.name),
                type=sc.type or DataType.DOUBLE,
                label=sc.label,
                description=sc.description,
                format=sc.format,
                primary_key=(
                    stamp_grain and sc.public_alias in grain_public_names
                ),
            )
            for sc in public_cols
        ]
        return SlayerModel(
            name=model.name,
            sql=wrapped_sql,
            data_source=inner_source_model.data_source,
            columns=cols,
            default_time_dimension=inner_source_model.default_time_dimension,
            # source_model_origin intentionally unset: the typed pipeline uses
            # the flat StageSchema namespace, not a lineage walk.
        )

    async def _resolve_model(
        self,
        model_name: str,
        _resolving: set = None,
        outer_vars: Optional[Dict[str, Any]] = None,
        runtime_kwarg: Optional[Dict[str, Any]] = None,
        dry_run_placeholders: bool = False,
        prefer_data_source: Optional[str] = None,
    ) -> SlayerModel:
        """Resolve a model by name from storage, expanding a query-backed one."""
        _resolving = _resolving if _resolving is not None else set()

        # Circular-reference guard (per-call set, concurrency-safe).
        if model_name in _resolving:
            raise ValueError(
                f"Circular reference detected: '{model_name}' references itself "
                f"(resolution chain: {' → '.join(_resolving)} → {model_name})"
            )
        _resolving.add(model_name)
        try:
            return await self._resolve_model_inner(
                model_name,
                _resolving=_resolving,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
                prefer_data_source=prefer_data_source,
            )
        finally:
            _resolving.discard(model_name)

    async def _resolve_model_inner(
        self,
        model_name: str,
        _resolving: set = None,
        outer_vars: Optional[Dict[str, Any]] = None,
        runtime_kwarg: Optional[Dict[str, Any]] = None,
        dry_run_placeholders: bool = False,
        prefer_data_source: Optional[str] = None,
    ) -> SlayerModel:

        # With ``prefer_data_source`` the lookup is strict (joins never cross
        # datasources silently); otherwise it consults the priority list.
        if prefer_data_source:
            model = await self.storage.get_model(model_name, data_source=prefer_data_source)
        else:
            model = await self.storage.get_model(model_name)
        if model is None:
            if prefer_data_source:
                raise ValueError(
                    f"Model '{model_name}' not found in data_source "
                    f"'{prefer_data_source}'."
                )
            raise ValueError(f"Model '{model_name}' not found")

        # Re-expand a query-backed model; model defaults fold into outer_vars.
        return await self._expand_query_backed_model(
            model=model,
            outer_vars=outer_vars,
            runtime_kwarg=runtime_kwarg,
            dry_run_placeholders=dry_run_placeholders,
            _resolving=_resolving,
        )

    async def create_model_from_query(
        self,
        query: "SlayerQuery | list[SlayerQuery] | dict | list[dict]",
        name: str,
        description: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        save: bool = True,
    ) -> SlayerModel:
        """Create a query-backed model; cache fields come from a save-time dry-run, ``save=False`` skips persisting."""
        raw = query if isinstance(query, list) else [query]
        stages = [
            SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in raw
        ]
        model = SlayerModel(
            name=name,
            description=description,
            source_queries=stages,
            query_variables=variables or {},
        )
        if save:
            return await self.save_model(model)
        return await self._validate_and_populate_cache(model)

    async def save_model(self, model: SlayerModel) -> SlayerModel:
        """Persist a SlayerModel verbatim (author spelling preserved); query-backed models reject cache fields and validate via dry-run."""
        # DEV-1826: save preserves the author's formula spelling — no slack
        # rewriting; both aggregation spellings are first-class parser input.
        # Capture the previous data_source so a moved query-backed model's stale
        # storage entry can be cleaned up below.
        prior_data_source: Optional[str] = None
        if model.source_queries:
            try:
                identity = await self.storage.resolve_model_identity(model.name)
                if identity is not None:
                    prior_data_source = identity[0]
            except AmbiguousModelError:
                # Multiple entries for this name — don't silently mass-delete.
                prior_data_source = None
        if model.source_queries:
            if model.columns:
                raise ValueError(
                    f"Model '{model.name}' is query-backed; columns are "
                    f"auto-generated and must not be supplied "
                    f"(got {len(model.columns)} columns)."
                )
            if model.backing_query_sql is not None:
                raise ValueError(
                    f"Model '{model.name}' is query-backed; backing_query_sql "
                    f"is auto-managed and must not be supplied."
                )
            model = await self._validate_and_populate_cache(model)
        await self._validate_mode_a_join_paths(model)
        await self.validate_sql_model_source(model)
        await self.storage.save_model(model)
        # Clean up the stale entry if the model moved datasource.
        if (
            prior_data_source is not None
            and prior_data_source != model.data_source
        ):
            await self.storage.delete_model(
                model.name, data_source=prior_data_source
            )
        return model

    async def validate_sql_model_source(self, model: SlayerModel) -> None:
        """Trial-execute a raw-``sql`` source before it persists: raise only when a
        reachable datasource rejects it, else warn (inconclusive never blocks).
        Skips ``sql_table`` / query-backed / parameterized SQL."""
        if not model.sql or model.sql_table or model.source_queries:
            return
        bare, blocked = extract_variable_refs(model.sql)
        if bare or blocked:
            logger.info(
                "Skipping save-time SQL validation for model %r: model.sql "
                "carries %d placeholder(s), which cannot be trial-filled.",
                model.name, len(bare | blocked),
            )
            return
        ds = (
            await self.storage.get_datasource(model.data_source)
            if model.data_source
            else None
        )
        sqlglot_name = dialect_for_ds_type(ds.type).sqlglot_name if ds else None
        if is_data_modifying_sql(model.sql, dialect=sqlglot_name):
            raise ModelSqlValidationError(
                model_name=model.name,
                data_source=model.data_source or "",
                ds_type=ds.type if ds else None,
                reason="model SQL must be a read-only query; it contains a "
                "data-modifying (DML/DDL) statement",
            )
        if ds is None:
            logger.warning(
                "Skipping save-time SQL validation for model %r: datasource "
                "%r is unset or not configured.", model.name, model.data_source,
            )
            return
        trial_sql = build_sql_model_trial_query(model.sql)
        try:
            await self._client_for(ds).get_column_types(trial_sql)
        except Exception as exc:
            if (
                _is_transient_db_error(exc)
                or _is_auth_failure(exc)
                or _is_unreachable_db_error(exc)
            ):
                logger.warning(
                    "Save-time SQL validation for model %r was inconclusive "
                    "(datasource %r, type %r): %s. Saving anyway.",
                    model.name, model.data_source, ds.type,
                    getattr(exc, "orig", exc),
                )
                return
            raise ModelSqlValidationError(
                model_name=model.name,
                data_source=model.data_source,
                ds_type=ds.type,
                reason=str(getattr(exc, "orig", exc)),
            ) from exc

    async def _validate_mode_a_join_paths(self, model: SlayerModel) -> None:
        """Reject a broken dotted chain / legacy ``__`` split-alias at save time via the generator's resolver."""
        if not model.joins:
            return  # no join graph → no chain qualifiers to resolve
        loaded = await self._preload_join_targets(model)

        def _resolve(name: str) -> Optional[SlayerModel]:
            return loaded.get(name)

        # Parse with the datasource's own dialect (matching generation), else
        # valid non-Postgres Mode-A SQL could be mis-rejected. Missing → postgres.
        datasource = await self.storage.get_datasource(model.data_source)
        dialect = self._dialect_for_type(datasource.type if datasource else None)

        surfaces: List[str] = []
        for col in model.columns:
            if col.sql:
                surfaces.append(col.sql)
            if col.filter:
                surfaces.append(col.filter)
        surfaces.extend(model.filters or [])
        for sql in surfaces:
            expand_derived_refs_sync(
                sql=sql,
                model=model,
                alias_path=model.name,
                resolve_model=_resolve,
                dialect=dialect,
            )

    async def _preload_join_targets(
        self, model: SlayerModel,
    ) -> Dict[str, Optional[SlayerModel]]:
        """BFS-load join-reachable models into a sync dict; missing targets map to ``None``."""
        loaded: Dict[str, Optional[SlayerModel]] = {model.name: model}
        queue: List[str] = [j.target_model for j in (model.joins or [])]
        while queue:
            name = queue.pop()
            if name in loaded:
                continue
            target: Optional[SlayerModel] = None
            try:
                target = await self.storage.get_model(
                    name, data_source=model.data_source,
                )
            except Exception:
                target = None
            loaded[name] = target
            if target is not None:
                queue.extend(j.target_model for j in (target.joins or []))
        return loaded

    async def _validate_and_populate_cache(self, model: SlayerModel) -> SlayerModel:
        """Dry-run-validate a query-backed model → copy with cache fields populated (undefaulted ``{var}`` → ``"0"``)."""
        if not (model.source_queries or []):
            return model
        virtual = await self._expand_query_backed_model(
            model=model,
            outer_vars=dict(model.query_variables),
            runtime_kwarg={},
            dry_run_placeholders=True,
            _resolving=set(),
        )
        return model.model_copy(update={
            "columns": list(virtual.columns),
            "backing_query_sql": virtual.sql,
            # Refreshed from the resolved virtual model: the backing query may now
            # resolve through a different datasource, which downstream callers read
            # before expanding the model.
            "data_source": virtual.data_source,
        })

    async def _resolve_datasource(self, model: SlayerModel) -> DatasourceConfig:
        ds_name = model.data_source
        if not ds_name:
            raise ValueError(
                f"Model '{model.name}' has no data_source configured. "
                f"Set data_source on the model or ensure the source model has one."
            )
        ds = await self.storage.get_datasource(ds_name)
        if ds is None:
            raise ValueError(f"Datasource '{ds_name}' not found for model '{model.name}'")
        return ds

    @staticmethod
    def _dialect_for_type(ds_type: Optional[str]) -> str:
        """Map a datasource ``type`` to its sqlglot dialect name (unknown/None → postgres)."""
        return dialect_for_ds_type(ds_type).sqlglot_name


# Join-cardinality detection helpers


def _join_signature(join) -> tuple:
    """Stable identity for a join: (target_model, sorted key pairs)."""
    return (join.target_model, tuple(sorted((p[0], p[1]) for p in join.join_pairs)))


def _scan_failed_finding(*, data_source, model, join, exc) -> JoinCardinalityFinding:
    """Report a join whose profiling scan raised, instead of aborting."""
    logger.warning(
        "detect_join_cardinality: %s -> %s failed: %s",
        model.name, join.target_model, exc,
    )
    return JoinCardinalityFinding(
        data_source=data_source,
        model=model.name,
        target_model=join.target_model,
        join_pairs=[[p[0], p[1]] for p in join.join_pairs],
        stored=join.cardinality,
        detected=None,
        verdict=CardinalityVerdict.SCAN_FAILED,
        note=f"profiling scan failed: {exc}",
    )


def _detection_skip_reason(*, model, target, src_cols, tgt_cols) -> str | None:
    """Why a join can't be profiled; ``None`` when it can."""
    if model.sql_table is None:
        return (
            f"model {model.name!r} is not table-backed (sql/query-backed); "
            f"cardinality profiling supports sql_table models only"
        )
    if target is None or target.sql_table is None:
        tn = target.name if target is not None else "?"
        return f"join target {tn!r} is not a table-backed model; skipped"
    for mdl, cols in ((model, src_cols), (target, tgt_cols)):
        for c in cols:
            col = next((x for x in mdl.columns if x.name == c), None)
            if col is not None and col.sql is not None and col.sql.strip() != c:
                return (
                    f"join key {mdl.name}.{c!r} is a SQL expression; "
                    f"cardinality profiling supports bare-column keys only"
                )
    return None


def _unique_contradictions(
    *, model, target, src_cols, tgt_cols, src_side, tgt_side,
) -> list[str]:
    """Single-column join keys declared unique/PK but observed to have dups."""
    out: list[str] = []
    for mdl, cols, side in (
        (model, src_cols, src_side),
        (target, tgt_cols, tgt_side),
    ):
        if len(cols) != 1 or side.observed_unique:
            continue
        c = cols[0]
        col = next((x for x in mdl.columns if x.name == c), None)
        if col is not None and declares_solo_unique(columns=mdl.columns, column=col):
            out.append(
                f"{mdl.name}.{c} is declared unique but the data has duplicates"
            )
    return out
