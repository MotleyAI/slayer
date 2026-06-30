"""Query engine — central orchestrator for SLayer queries.

Flow: SlayerQuery → _enrich() → EnrichedQuery → SQLGenerator → SQL → execute
"""

import decimal
import logging
from contextvars import ContextVar
from typing import Any

from pydantic import BaseModel, Field as PydanticField, model_validator
import sqlalchemy as sa
from sqlglot import exp

from slayer.core.enums import DEFAULT_AGGREGATIONS_BY_TYPE, DataType
from slayer.core.errors import AmbiguousModelError
from slayer.core.policy import SessionPolicy
from slayer.core.format import NumberFormat, NumberFormatType, format_number
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
    SourceModelOrigin,
)
from slayer.core.query import (
    ColumnRef,
    SlayerQuery,
    TimeDimension,
    extract_placeholder_names,
)
from slayer.engine.enriched import (
    CrossModelMeasure,
    EnrichedMeasure,
    EnrichedQuery,
    public_projection_aliases,
)
from slayer.engine.enrichment import enrich_query
from slayer.engine.introspect_utils import _safe_get_columns
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects import dialect_for_ds_type, get_dialect
from slayer.sql import engine_factory
from slayer.sql.engine_factory import _runtime_fingerprint
from slayer.sql.generator import SQLGenerator
from slayer.sql.session_policy import ScopedTable, apply_session_policy
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


def _sql_client_cache_key(datasource: DatasourceConfig) -> tuple[str, str]:
    """Cache key for ``SlayerQueryEngine._sql_clients``.

    Mirrors ``engine_factory``'s cache key so two datasources differing
    in (e.g.) Snowflake ``warehouse`` get distinct ``SlayerSQLClient``
    instances (and therefore distinct factory-cached engines with the
    correct per-connection ``USE`` listener).
    """
    return (datasource.get_connection_string(), _runtime_fingerprint(datasource))


# Per-task in-flight join-target names. Used by _resolve_join_target to break
# loops when a query-backed target's own join graph references it back. Lives
# in a ContextVar (not on the engine) so concurrent requests through the same
# engine don't see each other's in-flight state — each asyncio task gets its
# own copy of the context. The default=None + lazy-init pattern below means
# only tasks that actually hit a query-backed join target allocate a set.
_join_target_resolving_var: ContextVar[set | None] = ContextVar(
    "_join_target_resolving", default=None
)


# Per-task "forbidden sibling stage names" — names that exist in the enclosing
# source_queries list but are NOT visible from the stage currently being
# resolved (i.e. forward references and self references). Used by
# _resolve_model_inner to differentiate forward/self refs from genuine
# misspellings, so the user gets a clear error instead of "Model 'X' not found".
# Each entry maps a forbidden target name to the stage that tried to reach it.
_forbidden_sibling_refs_var: ContextVar[dict[str, str] | None] = ContextVar(
    "_forbidden_sibling_refs", default=None
)


class _NoJoinError(Exception):
    """Internal sentinel raised by ``_walk_join_chain`` when
    ``strict_missing_join=False`` and a hop has no matching join. Lets
    callers like ``_resolve_dimension_with_terminal`` map a missing
    join to a ``None`` return without re-walking the path."""
    def __init__(self, hop_name: str) -> None:
        super().__init__(f"no join target named {hop_name!r}")
        self.hop_name = hop_name


# EXPLAIN prefix/postfix moved to per-dialect classes in
# ``slayer/sql/dialects/`` (DEV-1542). ``_build_explain_sql`` below
# delegates via ``get_dialect``.


_PLACEHOLDER_FILL_VALUE = "0"


def _merge_query_variables(
    *,
    outer: dict[str, Any] | None,
    stage: dict[str, Any] | None,
    runtime: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge variable layers per spec precedence: ``runtime > stage > outer``.

    Model-level defaults are folded into ``outer`` by the caller before
    invoking this helper.
    """
    return {**(outer or {}), **(stage or {}), **(runtime or {})}


def _apply_placeholder_fill(
    query: SlayerQuery, effective: dict[str, Any]
) -> dict[str, Any]:
    """Add ``{var: '0'}`` for any unresolved ``{var}`` placeholder in
    ``query.filters`` so save-time dry-run SQL generation can proceed even
    when a runtime variable has no default.

    Existing values in ``effective`` are preserved.
    """
    placeholders = extract_placeholder_names(query)
    missing = {p: _PLACEHOLDER_FILL_VALUE for p in placeholders if p not in effective}
    if not missing:
        return effective
    return {**missing, **effective}


def _build_explain_sql(dialect: str, sql: str) -> str:
    """Build a dialect-appropriate EXPLAIN statement.

    Delegates to the dialect strategy registered in
    ``slayer.sql.dialects`` (DEV-1542). Both branches that previously
    raised ``ValueError`` (unsupported dialect via the None-prefix sentinel,
    AND today's ``_EXPLAIN_PREFIX.get(...)`` miss for typo-ed dialect
    names) preserve that exception type — the strict ``KeyError`` from
    ``get_dialect`` is converted to ``ValueError`` to match.
    """
    try:
        active = get_dialect(dialect)
    except KeyError:
        raise ValueError(
            f"EXPLAIN is not supported for dialect '{dialect}'. "
            "Use dry_run=True to inspect the generated SQL instead."
        ) from None
    return active.build_explain_sql(sql)


class FieldMetadata(BaseModel):
    """Metadata for a single field in the query response."""

    label: str | None = None
    format: NumberFormat | None = None


class ResponseAttributes(BaseModel):
    """Field metadata for a query response, split by type."""

    dimensions: dict[str, FieldMetadata] = PydanticField(default_factory=dict)
    measures: dict[str, FieldMetadata] = PydanticField(default_factory=dict)

    def get(self, column: str) -> FieldMetadata | None:
        """Look up metadata for a column across both dicts."""
        return self.dimensions.get(column) or self.measures.get(column)


class SlayerResponse(BaseModel):
    """Response from a SLayer query."""

    data: list[dict[str, Any]]
    columns: list[str] = PydanticField(default_factory=list)
    sql: str | None = None
    attributes: ResponseAttributes = PydanticField(default_factory=ResponseAttributes)

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


def _infer_aggregated_format(
    model: SlayerModel,
    measure_name: str,
    aggregation: str,
) -> NumberFormat | None:
    """Infer NumberFormat for an aggregated measure based on aggregation type and source measure format.

    Rules:
    - count, count_distinct: always INTEGER
    - avg, weighted_avg, median: always FLOAT
    - sum, min, max, first, last: inherit from source measure
    - *:count (measure_name="*"): INTEGER
    """
    if measure_name == "*":
        return NumberFormat(type=NumberFormatType.INTEGER)

    if aggregation in ("count", "count_distinct"):
        return NumberFormat(type=NumberFormatType.INTEGER)

    if aggregation in ("avg", "weighted_avg", "median"):
        return NumberFormat(type=NumberFormatType.FLOAT)

    # sum, min, max, first, last: inherit from source column's format
    source_col = model.get_column(measure_name)
    if source_col and source_col.format:
        return source_col.format

    return None


class SlayerQueryEngine:
    """Central orchestrator: resolves queries via storage, generates SQL, executes.

    The engine enriches a SlayerQuery (user-facing, just names) into an
    EnrichedQuery (fully resolved SQL expressions), then passes it to the
    SQLGenerator for SQL generation.
    """

    def __init__(
        self,
        storage: StorageBackend,
        *,
        policy: SessionPolicy | None = None,
    ):
        self.storage = storage
        # Cache key: (connection_string, runtime_fingerprint) — matches
        # ``engine_factory``'s cache so Snowflake datasources sharing a
        # connection_name but differing in warehouse/role/database/schema
        # get distinct clients (DEV-1551).
        self._sql_clients: dict[tuple[str, str], SlayerSQLClient] = {}
        # DEV-1578: immutable, engine-global forced-filter policy. When set,
        # every generated SQL is rewritten to scope each physical table to the
        # configured tenant before execution / dry-run / explain / profiling.
        self.policy = policy
        # Cache of confirmed column-presence facts keyed by
        # (ds_key, schema, table, column). Only ``True``/``False`` are cached;
        # an unconfirmable ``None`` is re-probed so a transient introspection
        # failure self-heals once the datasource recovers.
        self._column_presence_cache: dict[tuple, bool] = {}

    def _apply_policy(
        self, *, sql: str, dialect: str, datasource: DatasourceConfig
    ) -> str:
        """Rewrite ``sql`` to enforce the forced-filter policy, or return it
        unchanged when no policy is configured (zero overhead)."""
        if not (self.policy and self.policy.data_filters):
            return sql
        return apply_session_policy(
            sql,
            dialect=dialect,
            policy=self.policy,
            has_column=lambda scoped, column: self._column_present(
                datasource=datasource, scoped_table=scoped, column=column
            ),
        )

    def _column_present(
        self,
        *,
        datasource: DatasourceConfig,
        scoped_table: ScopedTable,
        column: str,
    ) -> bool | None:
        """Return whether ``column`` exists on ``scoped_table`` in
        ``datasource``: ``True`` / ``False`` / ``None`` (cannot confirm).

        Introspects via the shared ``_safe_get_columns`` helper (Inspector
        with INFORMATION_SCHEMA fallback). The schema is the table's parsed
        qualifier, else the datasource default. Only confirmed ``True`` /
        ``False`` results are cached; a ``None`` (any introspection error or
        empty result) is returned uncached so a transient failure is
        re-probed on the next query.
        """
        schema = scoped_table.schema_name or datasource.schema_name
        # Cross-catalog refs can't be confirmed: SQLAlchemy's column
        # introspection takes no catalog argument, so a three-part
        # ``catalog.schema.table`` naming a catalog other than the
        # connection's own would probe the wrong relation. Fail closed
        # (consistent with the unconfirmable-presence rule) rather than risk
        # an under-filter under ``on_unapplicable="pass"``. Single-catalog
        # refs (catalog matches the connection, or no catalog) probe normally.
        if scoped_table.catalog and (
            not datasource.database
            or scoped_table.catalog.casefold() != datasource.database.casefold()
        ):
            return None
        # Include catalog in the key so two tables differing only by catalog
        # (e.g. BigQuery project) never share a cached presence fact.
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
            cols = _safe_get_columns(
                inspector, sa_engine, scoped_table.name, schema
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

    def _get_join_target_resolving(self) -> set:
        """Return the per-task in-flight join-target name set, allocating one
        on first access in this asyncio context. See ``_join_target_resolving_var``.
        """
        s = _join_target_resolving_var.get()
        if s is None:
            s = set()
            _join_target_resolving_var.set(s)
        return s

    @staticmethod
    def _scope_named_queries_to_prior(
        named_queries: dict[str, "SlayerQuery"], stage_name: str | None
    ) -> dict[str, "SlayerQuery"]:
        """Slice an insertion-ordered named-queries dict to entries that
        come strictly before ``stage_name``.

        When a non-final stage of a ``source_queries`` list is being
        resolved, only its *prior* siblings are visible to it. This keeps
        the DAG acyclic. Runtime query lists pre-sort via
        :meth:`_topologically_order_queries` so the insertion order here
        is already a valid topological order; ``SlayerModel.source_queries``
        retains strict-order semantics and relies on this slice plus the
        forward-reference error in ``_resolve_model_inner`` to catch
        out-of-order references.

        Returns ``named_queries`` unchanged when ``stage_name`` is None or
        absent from the dict (e.g. the final stage, or an externally-named
        stored model).
        """
        if not stage_name or stage_name not in named_queries:
            return named_queries
        out: dict[str, "SlayerQuery"] = {}
        for k, v in named_queries.items():
            if k == stage_name:
                return out
            out[k] = v
        return out

    @staticmethod
    def _extract_sibling_refs(query: "SlayerQuery", against: set) -> set:
        """Names from ``query.source_model`` / inline joins that match ``against``.

        Walks the three shapes ``source_model`` can take — plain string,
        dict (``ModelExtension`` or inline ``SlayerModel``), or typed
        instance — and collects every name that resolves against the
        ``against`` set. Used by both the dependency-graph builder and
        the self-reference / root-as-sink validators.
        """
        out: set = set()
        sm = query.source_model
        if isinstance(sm, str):
            if sm in against:
                out.add(sm)
            return out
        # Dict shape — disambiguate ModelExtension vs inline SlayerModel
        # by presence of ``source_name``.
        if isinstance(sm, dict):
            src = sm.get("source_name")
            if isinstance(src, str) and src in against:
                out.add(src)
            for j in sm.get("joins") or []:
                tgt = j.get("target_model") if isinstance(j, dict) else getattr(j, "target_model", None)
                if isinstance(tgt, str) and tgt in against:
                    out.add(tgt)
            return out
        # Typed ModelExtension / SlayerModel: source_name lives on
        # ModelExtension only; SlayerModel.name is the inline model's
        # own identifier, not a reference.
        src = getattr(sm, "source_name", None)
        if isinstance(src, str) and src in against:
            out.add(src)
        for j in getattr(sm, "joins", None) or []:
            tgt = getattr(j, "target_model", None)
            if isinstance(tgt, str) and tgt in against:
                out.add(tgt)
        return out

    @staticmethod
    def _index_query_list_by_name(
        rest: list["SlayerQuery"], root: "SlayerQuery",
    ) -> dict[str, "SlayerQuery"]:
        """Build ``{name: query}`` for non-final entries, validating that
        every non-final entry has a unique name and that the root's
        name (if any) doesn't collide.
        """
        rest_by_name: dict[str, "SlayerQuery"] = {}
        for q in rest:
            if not q.name:
                raise ValueError(
                    "Every non-final entry in a query list must have a "
                    "'name' (siblings reference each other by name)."
                )
            if q.name in rest_by_name:
                raise ValueError(f"Duplicate stage name '{q.name}' in query list.")
            rest_by_name[q.name] = q
        if root.name and root.name in rest_by_name:
            raise ValueError(
                f"Stage name '{root.name}' is duplicated: the final entry "
                f"shares a name with an earlier entry."
            )
        return rest_by_name

    @classmethod
    def _validate_query_list_invariants(
        cls,
        queries: list["SlayerQuery"],
        rest: list["SlayerQuery"],
        root: "SlayerQuery",
        sibling_names: set,
    ) -> None:
        """Reject self-references and any sibling that depends on the root.

        Self-references are caught for every entry (including the root).
        Root-as-sink: no non-final stage may reference the root by name.
        """
        for q in queries:
            if q.name and q.name in cls._extract_sibling_refs(q, {q.name} | sibling_names):
                raise ValueError(
                    f"Stage '{q.name}' references itself — self-references "
                    f"are not allowed."
                )
        if root.name:
            referrers = sorted(
                q.name for q in rest if root.name in cls._extract_sibling_refs(q, {root.name})
            )
            if referrers:
                raise ValueError(
                    f"The final entry '{root.name}' is the DAG root and must "
                    f"not be referenced by other stages. Referenced by: "
                    f"{referrers}."
                )

    @classmethod
    def _build_dependency_graph(
        cls,
        rest_by_name: dict[str, "SlayerQuery"],
        sibling_names: set,
    ) -> tuple:
        """Build the (in_degree, dependents) adjacency for Kahn's.

        Edge direction: prerequisite → dependent. ``in_degree[X]`` is
        the count of siblings ``X`` depends on; ``dependents[X]`` is the
        list of siblings that depend on ``X``.
        """
        in_degree: dict[str, int] = dict.fromkeys(rest_by_name, 0)
        dependents: dict[str, list[str]] = {name: [] for name in rest_by_name}
        for name, q in rest_by_name.items():
            for prereq in cls._extract_sibling_refs(q, sibling_names):
                dependents[prereq].append(name)
                in_degree[name] += 1
        return in_degree, dependents

    @staticmethod
    def _kahn_sort(
        in_degree: dict[str, int],
        dependents: dict[str, list[str]],
    ) -> list[str]:
        """Topologically sort by Kahn's algorithm. Cycle → ValueError.

        Mutates ``in_degree`` in place; callers shouldn't reuse it.
        The frontier is kept sorted for deterministic output order across
        runs.
        """
        frontier: list[str] = sorted(n for n, d in in_degree.items() if d == 0)
        sorted_names: list[str] = []
        while frontier:
            n = frontier.pop(0)
            sorted_names.append(n)
            unlocked: list[str] = []
            for dep in dependents[n]:
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    unlocked.append(dep)
            frontier.extend(sorted(unlocked))
        if len(sorted_names) < len(in_degree):
            cycle = sorted(set(in_degree) - set(sorted_names))
            raise ValueError(
                f"Cycle in query list: stages {cycle} form a cyclic "
                f"dependency. The reference graph must be acyclic."
            )
        return sorted_names

    @classmethod
    def _topologically_order_queries(
        cls,
        queries: list["SlayerQuery"],
    ) -> list["SlayerQuery"]:
        """Re-order a runtime query list so every stage appears after the
        siblings it references via ``source_model`` or
        ``joins.target_model``. Lets callers submit a DAG in any order —
        cycles and self-references are rejected; the input order itself
        no longer needs to be a valid topological order.

        The last entry of the input is the entry point / DAG root: its
        result is what ``execute`` returns. It stays last; only the
        non-final entries are reordered. Stages that aren't reachable
        from the root are accepted as utility sub-queries — they flow
        through the sort like any other node and remain in the
        ``named_queries`` dict (the SQL generator emits them only if
        something references them).

        Hand-rolled Kahn's algorithm; no ``graphlib`` dependency. The
        actual work is delegated to four single-purpose helpers
        (:meth:`_index_query_list_by_name`,
        :meth:`_validate_query_list_invariants`,
        :meth:`_build_dependency_graph`, :meth:`_kahn_sort`) so this
        orchestrator stays under the cognitive-complexity gate.

        Raises ``ValueError`` on: missing ``name`` on any non-final
        entry; duplicate stage names; self-references; the root being
        depended on by any other stage; or a cycle among non-final
        stages.
        """
        if len(queries) <= 1:
            return list(queries)
        rest = list(queries[:-1])
        root = queries[-1]
        rest_by_name = cls._index_query_list_by_name(rest, root)
        sibling_names: set = set(rest_by_name)
        cls._validate_query_list_invariants(queries, rest, root, sibling_names)
        in_degree, dependents = cls._build_dependency_graph(rest_by_name, sibling_names)
        sorted_names = cls._kahn_sort(in_degree, dependents)
        return [rest_by_name[n] for n in sorted_names] + [root]

    async def execute(  # NOSONAR S3776 — public dispatch over str/dict/list/SlayerQuery; splitting hides the input-shape contract
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: dict[str, Any] | None = None,
        *,
        dry_run: bool = False,
        explain: bool = False,
        data_source: str | None = None,
    ) -> SlayerResponse:
        runtime_kwarg = variables or {}

        # Run-by-name dispatch: ``execute("model_name", variables=...)`` runs
        # the backing query of a query-backed model.
        if isinstance(query, str):
            return await self._execute_by_name(
                name=query,
                runtime_kwarg=runtime_kwarg,
                dry_run=dry_run,
                explain=explain,
                data_source=data_source,
            )


        # Accept dicts and validate them into SlayerQuery objects
        if isinstance(query, list):
            if not query:
                raise ValueError(
                    "'query' must be a non-empty list when passing staged queries."
                )
            queries = [SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in query]
            # Auto-sort: caller submits a DAG in any order; we reorder so
            # every stage appears after the siblings it references. The
            # last entry stays last as the entry point. Validates names,
            # duplicates, self-refs, root-as-sink, and cycles up front.
            queries = self._topologically_order_queries(queries)
            query = queries[-1]
            named_queries = {q.name: q for q in queries[:-1] if q.name}
        else:
            if isinstance(query, dict):
                query = SlayerQuery.model_validate(query)
            named_queries = {}

        # Merge ``variables=`` kwarg into query.variables so filter
        # substitution and downstream resolution see the merged set.
        # ``runtime_kwarg`` always wins (per spec precedence).
        if runtime_kwarg:
            merged_top = {**(query.variables or {}), **runtime_kwarg}
            if merged_top != (query.variables or {}):
                query = query.model_copy(update={"variables": merged_top})

        return await self._execute_pipeline(
            query=query,
            named_queries=named_queries,
            runtime_kwarg=runtime_kwarg,
            dry_run=dry_run,
            explain=explain,
            prefer_data_source=data_source,
        )

    async def _execute_by_name(
        self,
        name: str,
        runtime_kwarg: dict[str, Any],
        dry_run: bool = False,
        explain: bool = False,
        data_source: str | None = None,
    ) -> SlayerResponse:
        """Run the backing query of a query-backed model by name."""
        model = await self.storage.get_model(name, data_source=data_source)
        if model is None:
            raise ValueError(f"Model '{name}' not found")
        if not model.source_queries:
            raise ValueError(
                f"Model '{name}' is not query-backed; pass a SlayerQuery "
                f"with source_model='{name}'."
            )

        stages = list(model.source_queries)
        main_query = stages[-1]
        named_queries: dict[str, SlayerQuery] = {}
        for q in stages[:-1]:
            if q.name:
                if q.name in named_queries:
                    raise ValueError(
                        f"Duplicate query name '{q.name}' in source_queries "
                        f"of model '{name}'"
                    )
                named_queries[q.name] = q

        # Merge precedence at the run-by-name entry point:
        # ``runtime_kwarg > stage > model_defaults``. There's no enclosing
        # outer query for direct execution, so ``model.query_variables`` acts
        # as the lowest layer.
        merged = _merge_query_variables(
            outer=model.query_variables,
            stage=main_query.variables,
            runtime=runtime_kwarg,
        )
        if merged != (main_query.variables or {}):
            main_query = main_query.model_copy(update={"variables": merged})

        return await self._execute_pipeline(
            query=main_query,
            named_queries=named_queries,
            runtime_kwarg=runtime_kwarg,
            dry_run=dry_run,
            explain=explain,
            prefer_data_source=model.data_source or data_source,
        )

    async def _execute_pipeline(  # NOSONAR S3776 — linear pipeline (resolve→enrich→generate→execute); breaking it up obscures the order of operations
        self,
        query: SlayerQuery,
        named_queries: dict[str, SlayerQuery],
        runtime_kwarg: dict[str, Any],
        *,
        dry_run: bool = False,
        explain: bool = False,
        prefer_data_source: str | None = None,
    ) -> SlayerResponse:
        """Shared pipeline used by both ``execute()`` and ``_execute_by_name()``.

        Assumes ``query.variables`` already reflects the resolved variable
        context for the top of the chain (kwarg merged in by the caller).
        """
        # Pre-processing: strip redundant source model name prefixes from all references
        query = query.strip_source_model_prefix()
        named_queries = {
            name: q.strip_source_model_prefix()
            for name, q in named_queries.items()
        }

        # Preprocessing
        if query.whole_periods_only:
            query = query.snap_to_whole_periods()

        # Resolve model from query.source_model (str, SlayerModel, or ModelExtension).
        # Pass query.variables as the outer-vars context for any nested
        # query-backed model resolution; runtime_kwarg threads through unchanged.
        resolving: set = set()
        model = await self._resolve_query_model(
            query_model=query.source_model,
            named_queries=named_queries,
            _resolving=resolving,
            outer_vars=query.variables,
            runtime_kwarg=runtime_kwarg,
            prefer_data_source=prefer_data_source,
        )

        # Auto-correct: move bare field names to dimensions if they match
        query = await self._auto_move_fields_to_dimensions(query, model, named_queries)

        datasource = await self._resolve_datasource(model=model)

        # Enrich: SlayerQuery + model → EnrichedQuery
        enriched = await self._enrich(query=query, model=model, named_queries=named_queries)

        # Generate SQL from EnrichedQuery
        dialect = self._dialect_for_type(datasource.type)
        generator = SQLGenerator(dialect=dialect)
        # DEV-1444: this is the final-stage SQL that gets executed and
        # shown to the user — pin ``outer`` mode so the projection is
        # trimmed to public_projection_aliases(enriched).
        sql = generator.generate(enriched=enriched, render_mode="outer")
        # DEV-1578: forced-filter rewrite. Applied here — before the dry_run
        # return and before execution — so dry_run, explain, real execution,
        # and profiling all see the same tenant-scoped SQL.
        sql = self._apply_policy(sql=sql, dialect=dialect, datasource=datasource)
        logger.debug("Generated SQL:\n%s", sql)

        # DEV-1444: the response's attributes + expected_columns must mirror
        # the trimmed outer projection — never include hoisted intermediates.
        public_aliases = set(public_projection_aliases(enriched))

        # Collect field metadata from enriched query, split by type. Each
        # entry is included only if its alias is part of the public
        # projection (filter-extracted hidden transforms, ORDER-BY
        # aggregates, and window-arg hoists are silently dropped).
        dim_meta: dict[str, FieldMetadata] = {}
        measure_meta: dict[str, FieldMetadata] = {}
        for d in enriched.dimensions:
            if d.alias in public_aliases and (d.label or d.format):
                dim_meta[d.alias] = FieldMetadata(label=d.label, format=d.format)
        for td in enriched.time_dimensions:
            if td.alias in public_aliases and td.label:
                dim_meta[td.alias] = FieldMetadata(label=td.label)
        for m in enriched.measures:
            if m.alias not in public_aliases:
                continue
            measure_fmt = _infer_aggregated_format(
                model=model,
                measure_name=m.source_measure_name or m.name,
                aggregation=m.aggregation,
            )
            if m.label or measure_fmt:
                measure_meta[m.alias] = FieldMetadata(label=m.label, format=measure_fmt)
        for e in enriched.expressions:
            if e.alias not in public_aliases:
                continue
            measure_meta[e.alias] = FieldMetadata(
                label=e.label,
                format=NumberFormat(type=NumberFormatType.FLOAT),
            )
        for t in enriched.transforms:
            if t.alias not in public_aliases:
                continue
            measure_meta[t.alias] = FieldMetadata(
                label=t.label,
                format=NumberFormat(type=NumberFormatType.FLOAT),
            )
        for cm in enriched.cross_model_measures:
            if cm.alias in public_aliases and (cm.label or cm.format):
                measure_meta[cm.alias] = FieldMetadata(label=cm.label, format=cm.format)
        attributes = ResponseAttributes(dimensions=dim_meta, measures=measure_meta)

        # DEV-1444: expected_columns matches the outer SELECT projection
        # exactly (helper-driven). Fall back to the legacy bucket-union if
        # ``public_projection`` is empty (e.g. enrichment paths that don't
        # yet populate ``user_projection``).
        expected_columns = list(public_projection_aliases(enriched)) or (
            [d.alias for d in enriched.dimensions]
            + [td.alias for td in enriched.time_dimensions]
            + [m.alias for m in enriched.measures if not m.name.startswith(("_inner_", "_ft"))]
            + [e.alias for e in enriched.expressions]
            + [t.alias for t in enriched.transforms if not t.name.startswith(("_inner_", "_ft"))]
            + [cm.alias for cm in enriched.cross_model_measures]
        )

        # dry_run: return SQL without executing
        if dry_run:
            return SlayerResponse(data=[], columns=expected_columns, sql=sql, attributes=attributes)

        # Execute — reuse SQL client (and its connection pool) per
        # datasource. DEV-1551: include Snowflake runtime overrides
        # (warehouse / role / database / schema_name) in the cache key
        # so two datasources sharing the same connection_name but
        # differing in (e.g.) warehouse don't accidentally share a client
        # whose engine's per-connection ``USE WAREHOUSE`` listener was
        # set up for the other datasource.
        ds_key = _sql_client_cache_key(datasource)
        if ds_key not in self._sql_clients:
            self._sql_clients[ds_key] = SlayerSQLClient(datasource=datasource)
        client = self._sql_clients[ds_key]

        # explain: run dialect-appropriate EXPLAIN on the query
        if explain:
            explain_sql = _build_explain_sql(dialect=dialect, sql=sql)
            try:
                rows = await client.execute(sql=explain_sql)
            except Exception as exc:
                await self._maybe_raise_schema_drift(
                    err=exc, model=model, enriched=enriched
                )
                raise
            return SlayerResponse(data=rows, sql=sql, attributes=attributes)

        try:
            rows = await client.execute(sql=sql)
        except Exception as exc:
            await self._maybe_raise_schema_drift(
                err=exc, model=model, enriched=enriched
            )
            raise
        # Dialect-driven read-side decode: BigQuery reverses its alias
        # mangling here so the response keys match SLayer's universal
        # dotted shape. Default hook is identity for every other dialect.
        rows = get_dialect(dialect).decode_result_keys(rows)
        columns = expected_columns if not rows else []  # fallback for empty results; [] triggers auto-derive
        return SlayerResponse(data=rows, columns=columns, sql=sql, attributes=attributes)

    @staticmethod
    def _collect_query_backed_base_names(model: SlayerModel) -> "set[str]":
        """Return the set of base model names referenced by a query-backed
        ``model``'s ``source_queries`` stages — including stage source_models
        (excluding prior-stage names) and joins declared on each stage's
        ``source_model`` when that's a ``ModelExtension``.
        """
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
            # SlayerQuery has no ``.joins``; joins on a stage live on its
            # source_model when that's a ModelExtension. Read off
            # ``stage.source_model.joins`` via getattr with defaults so
            # plain str/SlayerModel source_models are no-ops.
            for j in (getattr(sm, "joins", None) or []):
                target = getattr(j, "target_model", None)
                if target is not None:
                    out.add(target)
        return out

    async def _collect_models_touched(
        self, *, model: SlayerModel, enriched: "EnrichedQuery"
    ) -> "set[str]":
        """Compute the set of model names that participated in this query.

        Includes the source model, every cross-model measure root, every
        query-backed base name (resolved from storage when ``model`` is a
        virtual stage produced by ``_query_as_model``), and (transitively)
        every join target reachable through the join graph.
        """
        touched: set[str] = {model.name}
        for cm in enriched.cross_model_measures:
            touched.add(cm.target_model_name)
            touched.add(cm.source_model_name)
        touched |= self._collect_query_backed_base_names(model)
        # The resolved ``model`` may be a virtual stage from
        # _query_as_model() — its ``source_queries`` is already expanded,
        # so the base-name walk above turns up nothing. Fall back to the
        # persisted record under ``model.name`` (if any) so query-backed
        # drift attribution still names the real persisted base models.
        if model.data_source:
            try:
                persisted = await self.storage.get_model(
                    model.name, data_source=model.data_source
                )
            except Exception:
                persisted = None
            if persisted is not None and persisted.source_queries:
                touched |= self._collect_query_backed_base_names(persisted)
        await self._expand_join_graph(
            touched=touched, data_source=model.data_source or None
        )
        return touched

    async def _expand_join_graph(
        self, *, touched: "set[str]", data_source: str | None
    ) -> None:
        """Follow each touched model's joins transitively, adding reachable
        target_model names to ``touched``. Visited-set guarded to avoid
        infinite loops on diamond / cyclic join graphs.
        """
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
        enriched: "EnrichedQuery",
    ) -> None:
        """Attribute a query-time exception to schema drift via
        ``validate_models``. If drift is found in the touched models, raise
        ``SchemaDriftError`` (with ``err`` as ``__cause__``); otherwise
        return so the caller re-raises the original exception untouched.

        Any error from ``validate_models`` itself is swallowed so the
        original exception is never masked.
        """
        from slayer.core.errors import SchemaDriftError

        try:
            touched = await self._collect_models_touched(model=model, enriched=enriched)
            # Cross-model measure source models share the parent's DS in
            # validated queries (cross-DS joins are rejected at resolve
            # time), so attribution only needs the parent's data_source.
            data_sources: set[str] = {model.data_source} if model.data_source else set()

            collected: list[Any] = []
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
            filtered = [
                e for e in collected if getattr(e, "model_name", None) in touched
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
        """Build a SlayerQuery for type-probing all of a model's columns.

        Picks an aggregation per column from its effective allowed set:
        explicit ``allowed_aggregations`` if present, otherwise the type
        default. Prefers ``max`` (preserves the column's SQL type for orderable
        types) and falls back to the first allowed aggregation otherwise.
        Skips primary-key columns (they're identifiers, not values to probe).
        """
        measures: list[ModelMeasure] = []
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

    async def get_column_types(
        self,
        model_name: str,
        data_source: str | None = None,
    ) -> dict[str, str]:
        """Infer column types for a model's columns via a type-probe query.

        Builds a real query through the engine's enrich+generate pipeline
        so cross-model measures (with JOINs) are resolved correctly.

        Returns {column_name: type_category} where type_category is
        "number", "string", "time", or "boolean".
        """
        model = await self.storage.get_model(model_name, data_source=data_source)
        if model is None:
            return {}

        # For query-backed models, expand FIRST so the resolved virtual model
        # (with refreshed ``data_source`` from its final stage AND with
        # ``columns`` derived from the inner query) drives both the
        # datasource selection and the probeable-columns check. Otherwise a
        # stale or blank stored ``model.data_source``/``columns`` would point
        # us at the wrong backend or short-circuit on an empty column list.
        if model.source_queries:
            try:
                # Type probing has no caller-supplied variables, so any
                # required-but-undefaulted ``{var}`` placeholder would fail at
                # SQL-gen and we'd return {}. Use the same canonical
                # placeholder-fill render that save-time validation uses
                # (``dry_run_placeholders=True`` substitutes literal ``0``).
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
        try:
            enriched = await self._enrich(query=probe_query, model=model)
            dialect = self._dialect_for_type(datasource.type)
            generator = SQLGenerator(dialect=dialect)
            # DEV-1444: type probing is a user-visible call site; pin
            # ``outer`` mode explicitly so a future default-change cannot
            # silently shift type-probe behaviour.
            sql = generator.generate(enriched=enriched, render_mode="outer")
            # DEV-1578: type probing is a user-visible execution path, so it
            # must honour the forced-filter policy too. A policy failure
            # (block / fail-closed) degrades to {} via this try/except rather
            # than leaking an unscoped probe.
            sql = self._apply_policy(sql=sql, dialect=dialect, datasource=datasource)
        except Exception:
            logger.warning("get_column_types enrich/generate failed for model '%s'", model_name)
            return {}

        try:
            raw_types = await client.get_column_types(sql=sql)
        except Exception:
            logger.warning("get_column_types probe failed for model '%s'", model_name)
            return {}

        # Map qualified aliases (e.g., "orders.revenue_max") back to bare measure names
        result: dict[str, str] = {}
        for em in enriched.measures:
            if em.alias in raw_types:
                result[em.source_measure_name or em.name] = raw_types[em.alias]
        return result

    async def aclose(self) -> None:
        """Dispose every cached client's async engine; keep the clients themselves.

        Per-instance async engines bind their asyncpg/aiomysql pool to the loop
        that first opened a connection; closing that loop without disposing
        leaks the server-side connections (asyncpg.Connection.close needs a
        live loop). Clients are kept so ``_sync_engine`` survives — important
        for ``:memory:`` SQLite, whose StaticPool pins the connection holding
        all data.
        """
        for client in self._sql_clients.values():
            await client.aclose()

    def execute_sync(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: dict[str, Any] | None = None,
        *,
        dry_run: bool = False,
        explain: bool = False,
    ) -> SlayerResponse:
        """Synchronous wrapper for execute(). Disposes per-call async engines
        in ``finally`` so they don't outlive their owning loop — see ``aclose``.
        """
        from slayer.async_utils import run_sync

        async def _run_and_cleanup() -> SlayerResponse:
            try:
                return await self.execute(
                    query, variables=variables, dry_run=dry_run, explain=explain,
                )
            finally:
                await self.aclose()

        return run_sync(_run_and_cleanup())

    async def edit_model_remove(
        self,
        *,
        model_name: str,
        data_source: str | None,
        remove_columns: list[str] | None = None,
        remove_measures: list[str] | None = None,
        remove_aggregations: list[str] | None = None,
        remove_joins: list[str] | None = None,
        remove_filters: list[str] | None = None,
    ) -> SlayerModel:
        """Apply surgical removals to a persisted model.

        Removes columns / measures / aggregations / joins by name, plus
        verbatim filter strings, and persists the resulting model. Returns
        the updated model.
        """
        existing = await self.storage.get_model(model_name, data_source=data_source)
        if existing is None:
            raise ValueError(
                f"Model {model_name!r} not found in datasource {data_source!r}."
            )
        if existing.source_queries:
            # Query-backed models manage ``columns`` / ``backing_query_sql``
            # as engine-side cache; bypassing engine.save_model would
            # persist stale cache that no longer matches source_queries.
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
        # Re-validate via Pydantic, then save.
        SlayerModel.model_validate(updated.model_dump())
        await self.storage.save_model(updated)
        # DEV-1428: cascade-strip dropped leaves from every memory's
        # entity tags. Joins / filters don't cascade — filters are not
        # named entities, and a joined-leaf ref canonicalizes to the
        # target model's own ``<ds>.<model>.<leaf>`` (independent of
        # the source model's join edge).
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
        self, *, model_name: str, data_source: str | None
    ) -> bool:
        """Delete a persisted model by name. Returns True if the model existed."""
        return await self.storage.delete_model(model_name, data_source=data_source)

    async def apply_drift_deletes(
        self, deletes: "list[Any]"
    ) -> "Any":
        """Apply each ``ToDeleteEntry`` via the engine helpers and return
        the combined ``ApplyDriftResult`` (applied, errors, residual).

        Order is irrelevant for correctness: each entry is a pure storage
        mutation on a single model. Per-entry failures are captured in
        ``errors`` and processing continues; after all entries are
        attempted, ``validate_models`` re-runs on the touched datasources
        and the result populates ``residual``.
        """
        from slayer.engine.schema_drift import (
            AppliedEntry,
            ApplyDriftResult,
            ApplyError,
        )

        applied: list[AppliedEntry] = []
        errors: list[ApplyError] = []
        touched_ds: set[str] = set()

        for entry in deletes:
            # Track every entry's datasource up front so post-apply
            # re-validation runs even when every mutation on that DS fails.
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
        residual: list[Any] = []
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

    async def validate_models(
        self, data_source: str | None = None
    ) -> "list[Any]":
        """Diff persisted models against live database schemas.

        Returns the minimal list of deletes needed for SQL generation to
        remain valid. Read-only — never mutates storage. When
        ``data_source`` is ``None``, every datasource is validated
        concurrently and results are concatenated.
        """
        import asyncio as _asyncio

        from slayer.engine.schema_drift import (
            ToDeleteEntry,
            validate_datasource,
        )

        if data_source is not None:
            ds = await self.storage.get_datasource(data_source)
            if ds is None:
                return []
            identities = await self.storage._list_all_model_identities()
            ds_model_names = [n for d, n in identities if d == data_source]
            models: list[SlayerModel] = []
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

        async def _validate_one(name: str) -> "list[ToDeleteEntry]":
            return await self.validate_models(data_source=name)

        results = await _asyncio.gather(
            *(_validate_one(n) for n in ds_names), return_exceptions=True
        )
        out: list = []
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
        description: str | None = None,
        variables: dict[str, Any] | None = None,
        save: bool = True,
    ) -> SlayerModel:
        """Synchronous wrapper for create_model_from_query()."""
        from slayer.async_utils import run_sync

        return run_sync(
            self.create_model_from_query(
                query=query,
                name=name,
                description=description,
                variables=variables,
                save=save,
            )
        )

    async def _expand_query_backed_model(
        self,
        model: SlayerModel,
        outer_vars: dict[str, Any] | None,
        runtime_kwarg: dict[str, Any] | None,
        dry_run_placeholders: bool,
        _resolving: set | None,
    ) -> SlayerModel:
        """If ``model`` is query-backed, expand its ``source_queries`` into a
        virtual model (with rendered SQL). Otherwise return ``model`` unchanged.

        Read-only — never writes to storage. The persisted cache
        (``columns`` / ``backing_query_sql`` / ``data_source``) is populated
        only by ``engine.save_model`` / ``create_model_from_query(save=True)``.
        """
        if not model.source_queries:
            return model
        stages = list(model.source_queries)
        merged_outer = {**model.query_variables, **(outer_vars or {})}
        named_q = {q.name: q for q in stages[:-1] if q.name}
        return await self._query_as_model(
            inner_query=stages[-1],
            named_queries=named_q,
            override_name=model.name,
            _resolving=_resolving,
            outer_vars=merged_outer,
            runtime_kwarg=runtime_kwarg,
            dry_run_placeholders=dry_run_placeholders,
        )

    async def _resolve_query_model(  # NOSONAR S3776 — type-dispatch on str/SlayerModel/ModelExtension/dict; flat is clearer than per-shape helpers here
        self,
        query_model,
        named_queries: dict = None,
        _resolving: set = None,
        outer_vars: dict[str, Any] | None = None,
        runtime_kwarg: dict[str, Any] | None = None,
        dry_run_placeholders: bool = False,
        prefer_data_source: str | None = None,
    ) -> SlayerModel:
        """Resolve query.source_model — handles str, SlayerModel, and ModelExtension."""
        from slayer.core.query import ModelExtension

        named_queries = named_queries or {}

        if isinstance(query_model, str):
            return await self._resolve_model(
                model_name=query_model,
                named_queries=named_queries,
                _resolving=_resolving,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
                prefer_data_source=prefer_data_source,
            )
        elif isinstance(query_model, SlayerModel):
            # Inline SlayerModel may itself be query-backed; expand its
            # source_queries the same way storage-backed models do, otherwise
            # the outer enrichment can't see the virtual columns.
            return await self._expand_query_backed_model(
                model=query_model,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
                _resolving=_resolving,
            )
        elif isinstance(query_model, ModelExtension):
            base = await self._resolve_model(
                model_name=query_model.source_name,
                named_queries=named_queries,
                _resolving=_resolving,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
                prefer_data_source=prefer_data_source,
            )
            # Extend the base model with extra columns/measures/joins
            # ModelJoin already imported at the top of the file.

            extra_cols = [
                Column.model_validate(c) if isinstance(c, dict) else c for c in (query_model.columns or [])
            ]
            extra_measures = [
                ModelMeasure.model_validate(m) if isinstance(m, dict) else m for m in (query_model.measures or [])
            ]
            extra_joins = [ModelJoin.model_validate(j) if isinstance(j, dict) else j for j in (query_model.joins or [])]
            return base.model_copy(
                update={
                    "columns": list(base.columns) + extra_cols,
                    "measures": list(base.measures) + extra_measures,
                    "joins": list(base.joins) + extra_joins,
                }
            )
        elif isinstance(query_model, dict):
            # Dict — could be ModelExtension or SlayerModel
            if "source_name" in query_model:
                ext = ModelExtension.model_validate(query_model)
                return await self._resolve_query_model(
                    ext,
                    named_queries,
                    _resolving=_resolving,
                    outer_vars=outer_vars,
                    runtime_kwarg=runtime_kwarg,
                    dry_run_placeholders=dry_run_placeholders,
                )
            else:
                model = SlayerModel.model_validate(query_model)
                return await self._expand_query_backed_model(
                    model=model,
                    outer_vars=outer_vars,
                    runtime_kwarg=runtime_kwarg,
                    dry_run_placeholders=dry_run_placeholders,
                    _resolving=_resolving,
                )
        else:
            raise ValueError(f"Invalid query.source_model type: {type(query_model)}")

    async def _resolve_model(
        self,
        model_name: str,
        named_queries: dict[str, SlayerQuery] = None,
        _resolving: set = None,
        outer_vars: dict[str, Any] | None = None,
        runtime_kwarg: dict[str, Any] | None = None,
        dry_run_placeholders: bool = False,
        prefer_data_source: str | None = None,
    ) -> SlayerModel:
        """Resolve a model by name — checks named queries first, then storage."""
        named_queries = named_queries or {}
        _resolving = _resolving if _resolving is not None else set()

        # Circular reference protection (per-call set, safe for concurrent requests)
        if model_name in _resolving:
            raise ValueError(
                f"Circular reference detected: '{model_name}' references itself "
                f"(resolution chain: {' → '.join(_resolving)} → {model_name})"
            )
        _resolving.add(model_name)
        try:
            return await self._resolve_model_inner(
                model_name,
                named_queries,
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
        named_queries: dict[str, SlayerQuery],
        _resolving: set = None,
        outer_vars: dict[str, Any] | None = None,
        runtime_kwarg: dict[str, Any] | None = None,
        dry_run_placeholders: bool = False,
        prefer_data_source: str | None = None,
    ) -> SlayerModel:
        # Named query overrides stored model
        if model_name in named_queries:
            return await self._query_as_model(
                inner_query=named_queries[model_name],
                named_queries=named_queries,
                _resolving=_resolving,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
            )

        # v4 (DEV-1330): bare-name lookups consult the priority list (via
        # storage.get_model's None branch) and the ``prefer_data_source``
        # hint (the parent model's datasource for join targets, or an
        # explicit ``data_source=`` kwarg on ``engine.execute``). When a
        # hint is present the lookup is *strict* — joins never cross
        # datasource boundaries silently; the caller must opt in by setting
        # the priority list or passing the right hint.
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
            forbidden = _forbidden_sibling_refs_var.get()
            if forbidden and model_name in forbidden:
                offender = forbidden[model_name]
                if offender == model_name:
                    raise ValueError(
                        f"Stage '{offender}' cannot reference itself via "
                        f"'joins.target_model' (or as 'source_model'); a "
                        f"stage may only resolve to prior named stages in "
                        f"the same source_queries list."
                    )
                raise ValueError(
                    f"Stage '{offender}' cannot reference stage "
                    f"'{model_name}': forward references are not allowed. "
                    f"A stage may only resolve to prior named stages in "
                    f"the same source_queries list."
                )
            raise ValueError(f"Model '{model_name}' not found")

        # If model has source_queries, re-enrich from stored queries.
        # Model-level defaults are folded into outer_vars by the helper
        # (precedence: runtime > stage > outer > model_defaults).
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
        description: str | None = None,
        variables: dict[str, Any] | None = None,
        save: bool = True,
    ) -> SlayerModel:
        """Create a query-backed model from a query (or list of stages).

        The returned model has ``source_queries`` populated, plus ``columns``
        and ``backing_query_sql`` populated from a save-time dry-run of the
        final stage (with literal ``0`` substituted for any unresolved
        ``{var}`` placeholder). ``query_variables`` is set from the
        ``variables=`` kwarg.

        Args:
            query: One ``SlayerQuery`` or a list of stages (last is the
                final/main query). Dicts are accepted and validated.
            name: Name for the new model.
            description: Optional model description.
            variables: Default values for ``{var}`` placeholders in the
                stages — saved as ``model.query_variables``.
            save: If True (default), persist to storage immediately.
        """
        raw = query if isinstance(query, list) else [query]
        stages = [
            SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in raw
        ]
        # Construct the SlayerModel — Pydantic validators enforce source-mode
        # exclusivity and stage-name rules.
        model = SlayerModel(
            name=name,
            description=description,
            source_queries=stages,
            query_variables=variables or {},
        )
        if save:
            return await self.save_model(model)
        # save=False: still validate and populate the cache so the caller
        # can use the returned model directly.
        return await self._validate_and_populate_cache(model)

    async def save_model(self, model: SlayerModel) -> SlayerModel:
        """Persist a SlayerModel through the engine.

        For query-backed models, rejects user-supplied cache fields and runs
        save-time dry-run validation before populating the cache. For non-
        query-backed models, persists as-is.
        """
        # Capture the *previous* data_source for this name so we can clean
        # up the old storage entry when a query-backed model's resolved
        # data_source changes (e.g. its backing query now points at a
        # different upstream datasource).
        prior_data_source: str | None = None
        if model.source_queries:
            try:
                identity = await self.storage.resolve_model_identity(model.name)
                if identity is not None:
                    prior_data_source = identity[0]
            except AmbiguousModelError:
                # Multiple existing entries for this name — leave them be;
                # the caller already lives in a partially-stale world and
                # we don't want save_model to silently mass-delete.
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
        await self.storage.save_model(model)
        # Clean up the stale storage entry if the cache populator moved a
        # query-backed model to a different datasource.
        if (
            prior_data_source is not None
            and prior_data_source != model.data_source
        ):
            await self.storage.delete_model(
                model.name, data_source=prior_data_source
            )
        return model

    async def _validate_and_populate_cache(self, model: SlayerModel) -> SlayerModel:
        """Run save-time dry-run validation on a query-backed model and
        return a copy with ``columns``, ``backing_query_sql``, and
        ``data_source`` populated from the virtual model.
        """
        stages = list(model.source_queries or [])
        if not stages:
            return model
        virtual = await self._query_as_model(
            inner_query=stages[-1],
            named_queries={q.name: q for q in stages[:-1] if q.name},
            override_name=model.name,
            _resolving=set(),
            outer_vars=dict(model.query_variables),
            runtime_kwarg={},
            dry_run_placeholders=True,
        )
        return model.model_copy(update={
            "columns": list(virtual.columns),
            "backing_query_sql": virtual.sql,
            # data_source is refreshed from the resolved virtual model: the
            # backing query may now resolve through a different upstream
            # datasource than the caller passed (or the previous save), and
            # downstream callers like get_column_types() open the SQL client
            # from the persisted data_source BEFORE expanding the model.
            "data_source": virtual.data_source,
        })

    async def _enrich(  # NOSONAR S3776 — orchestrates resolve-callback closures + cross-model post-processing; splitting into helpers obscures the closure variables threaded through enrich_query
        self,
        query: SlayerQuery,
        model: SlayerModel,
        named_queries: dict[str, SlayerQuery] = None,
        dialect: str | None = None,
        *,
        drop_unreachable_filters: bool = False,
    ) -> EnrichedQuery:
        """Resolve a SlayerQuery against model definitions into an EnrichedQuery.

        Delegates to enrich_query() in enrichment.py, passing engine callbacks
        for model resolution (joins, cross-model measures, join targets).

        ``dialect`` controls how Column.sql is parsed during derived-reference
        expansion. Falls back to the model's resolved datasource type, then to
        ``"postgres"`` if neither is available (e.g., in unit tests with a
        fake data_source name).
        """

        if dialect is None:
            dialect = "postgres"
            try:
                if model.data_source and self.storage:
                    ds = await self.storage.get_datasource(model.data_source)
                    if ds is not None:
                        dialect = self._dialect_for_type(ds.type)
            except Exception:  # noqa: BLE001 — diagnostics only; never block enrichment
                pass

        async def _resolve_join_target(target_model_name, named_queries):
            nq = named_queries or {}
            if target_model_name in nq:
                # Named-query stages inherit the variable context of the query
                # being enriched (its filter substitutions) so nested query-
                # backed model resolution works through joins as well.
                target = await self._query_as_model(
                    inner_query=nq[target_model_name],
                    named_queries=nq,
                    outer_vars=query.variables,
                )
            elif self.storage:
                # v4 (DEV-1330): joins must stay inside the parent model's
                # logical database (cross-datasource joins aren't executable).
                # When parent has a ``data_source``, do a *strict* lookup —
                # no bare-name fallback that could silently pick the same
                # name from another datasource. Only fall through to the
                # priority/unique-match resolver when the parent has no
                # datasource hint to give.
                if model.data_source:
                    target = await self.storage.get_model(
                        target_model_name, data_source=model.data_source
                    )
                else:
                    target = await self.storage.get_model(target_model_name)
                if target is None:
                    # When the lookup misses, distinguish a forward / self
                    # reference (sibling stage that's not in this stage's
                    # scope) from a genuinely-missing storage model so the
                    # caller gets a clear error instead of a generic "not
                    # found" — same logic as ``_resolve_model_inner``
                    # (DEV-1340).
                    forbidden = _forbidden_sibling_refs_var.get()
                    if forbidden and target_model_name in forbidden:
                        offender = forbidden[target_model_name]
                        if offender == target_model_name:
                            raise ValueError(
                                f"Stage '{offender}' cannot reference itself "
                                f"via 'joins.target_model'; a stage may only "
                                f"resolve to prior named stages in the same "
                                f"source_queries list."
                            )
                        raise ValueError(
                            f"Stage '{offender}' cannot reference stage "
                            f"'{target_model_name}': forward references are "
                            f"not allowed. A stage may only resolve to prior "
                            f"named stages in the same source_queries list."
                        )
                if target and target.source_queries:
                    target = await self._render_query_backed_join_target(
                        target=target,
                        outer_query_variables=query.variables,
                    )
            else:
                target = None
            if target and target.sql_table:
                return target.sql_table, target
            elif target and target.sql:
                return f"({target.sql})", target
            return None

        async def _resolve_model_for_expansion(model_name, named_queries):
            """Adapter for column_expansion: returns ``SlayerModel`` or None.
            Catches lookup errors so unknown alias paths don't blow up the
            whole enrichment — the expander treats them as opaque.

            v4: pass the *outer* model's ``data_source`` as the hint so
            ``B.col`` references inside ``A``'s derived columns resolve
            within ``A.data_source``, never across the join graph into a
            sibling datasource.
            """
            try:
                return await self._resolve_model(
                    model_name=model_name,
                    named_queries=named_queries or {},
                    prefer_data_source=model.data_source or None,
                )
            except Exception:  # noqa: BLE001 — opaque alias is expected for CTE/sub-query refs
                return None

        enriched = await enrich_query(
            query=query,
            model=model,
            named_queries=named_queries,
            resolve_dimension_via_joins=self._resolve_dimension_with_terminal,
            resolve_cross_model_measure=self._resolve_cross_model_measure,
            resolve_join_target=_resolve_join_target,
            resolve_model=_resolve_model_for_expansion,
            dialect=dialect,
            drop_unreachable_filters=drop_unreachable_filters,
        )

        # Post-process: build re-rooted enriched queries for cross-model measures
        for cm in enriched.cross_model_measures:
            cm.rerooted_enriched = await self._build_rerooted_enriched(
                cm=cm, query=query, model=model,
                named_queries=named_queries or {},
            )

        return enriched

    async def _render_query_backed_join_target(
        self,
        target: SlayerModel,
        outer_query_variables: dict[str, Any] | None,
    ) -> SlayerModel:
        """Resolve a query-backed model used as a JOIN target.

        Threads the enclosing query's variables into the target's stage filter
        substitution so a target with ``filters=["amount > {threshold}"]`` sees
        the runtime value, not the cached/default fill.

        Recursion guard: ``self._join_target_resolving`` blocks re-entry on the
        same target name. The call stack crosses ``_enrich`` invocations
        (target's source_queries → target's own joins → _resolve_join_target
        again), so this guard lives on the engine instance, not on a closure.
        Re-entry returns the cached SQL if available, else returns the raw
        target unchanged so enrichment fails with a clear "no sql" error
        instead of looping.
        """
        resolving = self._get_join_target_resolving()
        if target.name in resolving:
            if target.backing_query_sql:
                return target.model_copy(update={"sql": target.backing_query_sql})
            return target
        # When the enclosing query has no variables AND a canonical cache
        # exists, prefer the cached SQL (avoids the second render).
        if not outer_query_variables and target.backing_query_sql:
            return target.model_copy(update={"sql": target.backing_query_sql})
        # Otherwise render fresh with merged variables (target defaults +
        # enclosing query's vars; enclosing wins).
        stages = list(target.source_queries or [])
        if not stages:
            return target
        merged = {**dict(target.query_variables), **(outer_query_variables or {})}
        resolving.add(target.name)
        try:
            return await self._query_as_model(
                inner_query=stages[-1],
                named_queries={q.name: q for q in stages[:-1] if q.name},
                override_name=target.name,
                outer_vars=merged,
                runtime_kwarg=outer_query_variables or None,
            )
        finally:
            resolving.discard(target.name)

    async def _query_as_model(  # NOSONAR S3776 — variable-precedence + enrich + SQL-gen + virtual-model assembly is a single conceptual unit
        self,
        inner_query: SlayerQuery,
        named_queries: dict[str, SlayerQuery] = None,
        override_name: str = None,
        _resolving: set = None,
        outer_vars: dict[str, Any] | None = None,
        runtime_kwarg: dict[str, Any] | None = None,
        dry_run_placeholders: bool = False,
    ) -> SlayerModel:
        """Build a virtual SlayerModel from a nested query's result.

        Enriches and generates SQL for the inner query, then creates a model
        whose `sql` is the inner query's SQL and whose dimensions/measures
        are derived from the inner query's enriched columns.

        ``outer_vars``, ``runtime_kwarg``, and ``dry_run_placeholders`` thread
        the variable-precedence machinery through nested query-backed model
        resolution; see ``_merge_query_variables`` and
        ``_apply_placeholder_fill``.
        """
        named_queries = named_queries or {}

        # Compute effective variables for this stage and stamp them onto a
        # copy of the inner query so substitution at enrichment time uses
        # the merged set.
        effective = _merge_query_variables(
            outer=outer_vars,
            stage=inner_query.variables,
            runtime=runtime_kwarg,
        )
        if dry_run_placeholders:
            effective = _apply_placeholder_fill(inner_query, effective)
        if effective != (inner_query.variables or {}):
            inner_query = inner_query.model_copy(update={"variables": effective})

        # Scope ``named_queries`` to the prior siblings of this stage. A
        # non-final stage may only resolve names that come BEFORE it in the
        # source_queries list; forward references and self references fall
        # out of scope here and surface a clear error from
        # ``_resolve_model_inner``. (For top-level stages — final stage,
        # un-named query-backed wrapper, or stored-model lookup — the scope
        # is unchanged.)
        scoped = self._scope_named_queries_to_prior(
            named_queries, inner_query.name
        )
        forbidden_now: dict[str, str] = {}
        if scoped is not named_queries and inner_query.name:
            for k in named_queries:
                if k not in scoped:
                    forbidden_now[k] = inner_query.name

        # Stack the new forbidden refs on top of any from an enclosing
        # stage; restore on the way out so concurrent / sibling resolutions
        # don't see this frame's bans.
        prev_forbidden = _forbidden_sibling_refs_var.get()
        if forbidden_now:
            merged_forbidden = dict(prev_forbidden) if prev_forbidden else {}
            # Outer frames win on the same key (a closer ancestor's ban is
            # the more specific one), but in practice keys don't overlap
            # because each frame names a distinct stage.
            for k, v in forbidden_now.items():
                merged_forbidden.setdefault(k, v)
            token = _forbidden_sibling_refs_var.set(merged_forbidden)
        else:
            token = None

        try:
            # Resolve the inner model (handles str, SlayerModel, ModelExtension).
            # Pass ``effective`` as the next layer's outer_vars so nested
            # query-backed models inherit this stage's resolved context.
            inner_model = await self._resolve_query_model(
                query_model=inner_query.source_model,
                named_queries=scoped,
                _resolving=_resolving,
                outer_vars=effective,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
            )

            # Enrich the inner query — pass scoped named_queries so any
            # ``joins.target_model`` referencing a prior named sibling is
            # resolvable here too (DEV-1340).
            enriched = await self._enrich(
                query=inner_query, model=inner_model, named_queries=scoped
            )
        finally:
            if token is not None:
                _forbidden_sibling_refs_var.reset(token)

        # Generate SQL
        datasource = await self._resolve_datasource(model=inner_model)
        dialect = self._dialect_for_type(datasource.type)
        generator = SQLGenerator(dialect=dialect)
        # DEV-1444: _query_as_model wraps the inner query as a virtual model;
        # downstream references reach EVERY hoisted alias, so the inner SQL
        # must keep its full projection rather than getting trimmed.
        inner_sql = generator.generate(enriched=enriched, render_mode="wrapped")

        # Build virtual model from enriched columns.
        # Inner query columns have aliases like "orders.count" (with dots).
        # We wrap the inner SQL in a renaming subquery so the virtual model
        # has clean column names that work naturally in JOINs and references.
        virtual_name = override_name or inner_query.name or f"_subquery_{inner_model.name}"

        # Build lookups for labels/descriptions from the source model.
        # In v2 there is no dim/measure split — every column carries both
        # potential roles, so a single map per attribute is sufficient.
        source_label = {c.name: c.label for c in inner_model.columns if c.label}
        source_desc = {c.name: c.description for c in inner_model.columns if c.description}

        # Collect all inner aliases and their short names.
        # Short names must be valid SQL identifiers (no dots). We derive them
        # from the alias by stripping the source model prefix and replacing
        # dots with underscores.
        def _alias_to_short(alias: str) -> str:
            """Convert result alias to a flat column name for the virtual model.

            The query result is a self-contained table without the joins the
            source model may have had, so dot syntax (join paths) is not
            applicable. We use ``__`` to preserve the path information:

            'orders.customers.regions.name' → 'customers__regions__name'
            'orders.count'                  → 'count'
            """
            # Strip source model prefix
            stripped = alias.split(".", 1)[-1] if "." in alias else alias
            # Replace remaining dots with __ to encode the original join path
            return stripped.replace(".", "__")

        # (inner_alias, short_name, data_type, label, description, format)
        column_map = []
        for d in enriched.dimensions:
            short = _alias_to_short(d.alias)
            label = d.label or source_label.get(d.name)
            desc = source_desc.get(d.name)
            column_map.append((d.alias, short, d.type, label, desc, d.format))
        for td in enriched.time_dimensions:
            short = _alias_to_short(td.alias)
            label = td.label or source_label.get(td.name)
            desc = source_desc.get(td.name)
            column_map.append((td.alias, short, DataType.TIMESTAMP, label, desc, None))
        for m in enriched.measures:
            src_name = m.source_measure_name or m.name
            label = m.label or source_label.get(src_name)
            desc = source_desc.get(src_name)
            fmt = _infer_aggregated_format(
                model=inner_model,
                measure_name=src_name,
                aggregation=m.aggregation,
            )
            column_map.append((m.alias, m.name, DataType.DOUBLE, label, desc, fmt))
        for t in enriched.transforms:
            column_map.append(
                (t.alias, t.name, DataType.DOUBLE, t.label, None, NumberFormat(type=NumberFormatType.FLOAT))
            )
        for e in enriched.expressions:
            column_map.append(
                (e.alias, e.name, DataType.DOUBLE, e.label, None, NumberFormat(type=NumberFormatType.FLOAT))
            )
        for cm in enriched.cross_model_measures:
            # DEV-1448: when the user supplied an explicit ``name``, cm.name is
            # a bare identifier (ModelMeasure.name forbids dots). Use it
            # directly as the downstream short form so callers reference the
            # user's chosen name without learning the ``__``-flattened
            # encoding. Auto-derived names always contain a dot (e.g.
            # ``customers.revenue_sum``) so they fall through to the legacy
            # ``_alias_to_short`` flatten path.
            #
            # Codex review round 3 on PR #136: gate the short-circuit on
            # ``cm.user_declared`` — hidden cross-model measures auto-
            # extracted from arithmetic / transform formulas (in enrichment.py
            # ``_ensure_measure_from_spec`` / ``_flatten_spec``) have bare
            # internal placeholder names (e.g. ``__agg0__``) that must NOT
            # leak into the virtual model's column set. Only user-declared
            # renames qualify for the bare-name short.
            if cm.user_declared and cm.name and "." not in cm.name:
                short = cm.name
            else:
                short = _alias_to_short(cm.alias)
            column_map.append((cm.alias, short, DataType.DOUBLE, cm.label, None, cm.format))

        # Wrap inner SQL: SELECT <inner_alias> AS <short>, ... FROM (inner) AS _inner
        # DEV-1571 Bug 3 follow-up: identifier quoting must match the
        # dialect ``inner_sql`` was generated for. On MySQL the inner CTEs
        # use backticks; on T-SQL the inner CTEs use brackets AND have
        # their dotted aliases mangled. Hardcoded ANSI double quotes
        # would either fail to parse (MySQL) or reference an alias the
        # mangled inner subquery doesn't expose (T-SQL).
        rename_parts = [
            f'{exp.Identifier(this=alias, quoted=True).sql(dialect=dialect)} AS {short}'
            for alias, short, _, _, _, _ in column_map
        ]
        wrapped_sql = f"SELECT {', '.join(rename_parts)} FROM ({inner_sql}) AS _inner"
        # DEV-1571 Bug 2: apply the dialect's emitted-SQL rewrite (e.g.
        # T-SQL bracket-mangling) so the rename clause's inner-alias
        # references match what the inner subquery actually projects.
        wrapped_sql = get_dialect(dialect).rewrite_emitted_sql(wrapped_sql)

        # One Column per result column — each is potentially both a dimension
        # (group-by) or measure (with colon-aggregation) at query time.
        cols: list[Column] = []
        for _, short, dtype, label, desc, fmt in column_map:
            cols.append(Column(name=short, sql=short, type=dtype, label=label, description=desc, format=fmt))

        # DEV-1449 / Codex round 10: record only columns that are
        # reliably the same cross-model aggregate the outer-stage
        # intercept would resolve a `customers.revenue:sum` reference
        # to. Includes:
        #   * Auto-derived cross-model canonical-flats (`_alias_to_short(cm.alias)`).
        #   * Intercept-produced EnrichedMeasures (from a downstream
        #     stage re-using the intercepted projection).
        # Excludes:
        #   * User-renamed CMM shorts: a user-supplied `name` could
        #     coincidentally match a different aggregate's canonical-flat.
        #   * Plain measures / transforms / expressions: their names are
        #     user-supplied and could collide with cross-model canonicals
        #     by coincidence.
        agg_shorts = set()
        for cm in enriched.cross_model_measures:
            if not (cm.user_declared and cm.name and "." not in cm.name):
                agg_shorts.add(_alias_to_short(cm.alias))
        for m in enriched.measures:
            if m.from_cross_model_intercept:
                agg_shorts.add(m.name)

        # DEV-1449: record the lineage breadcrumb so outer-stage dotted-ref
        # lookup can strip the right ancestor prefix and find the flat
        # column in this wrapped projection. ``parent`` carries any
        # existing chain on ``inner_model``, so chained nested-DAGs
        # build a linked list down to the original table-backed root.
        return SlayerModel(
            name=virtual_name,
            sql=wrapped_sql,
            data_source=inner_model.data_source,
            columns=cols,
            default_time_dimension=inner_model.default_time_dimension,
            source_model_origin=SourceModelOrigin(
                name=inner_model.name,
                data_source=inner_model.data_source,
                parent=inner_model.source_model_origin,
                agg_column_names=frozenset(agg_shorts),
            ),
        )

    async def _resolve_dimension_via_joins(
        self,
        model: SlayerModel,
        parts: list[str],
        named_queries: dict = None,
    ) -> "Column | None":
        """Walk the join graph to resolve a multi-hop column reference.

        For "customers.regions.name", walks: model → customers → regions,
        then looks up "name" on the regions model.
        """
        result = await self._resolve_dimension_with_terminal(
            model=model, parts=parts, named_queries=named_queries,
        )
        return result[0] if result is not None else None

    async def _resolve_dimension_with_terminal(
        self,
        model: SlayerModel,
        parts: list[str],
        named_queries: dict = None,
    ) -> "tuple[Column, SlayerModel] | None":
        """Like ``_resolve_dimension_via_joins`` but also returns the
        terminal model so callers (column-SQL expansion) can recurse into
        the resolved column's own ``sql``.
        """
        try:
            terminal_model, _first_join = await self._walk_join_chain(
                source_model=model,
                hop_names=parts[:-1],
                named_queries=named_queries,
                strict_missing_join=False,
            )
        except _NoJoinError:
            return None

        col = terminal_model.get_column(parts[-1])
        if col is None:
            return None
        return col, terminal_model

    async def _walk_join_chain(
        self,
        *,
        source_model: SlayerModel,
        hop_names: list[str],
        named_queries: dict = None,
        strict_missing_join: bool = True,
    ) -> "tuple[SlayerModel, ModelJoin | None]":
        """Walk the join graph from ``source_model`` through ``hop_names``,
        returning ``(terminal_model, first_join)``. Single source of
        truth for both dimension and cross-model-measure resolution
        (DEV-1369 — consolidates two prior near-duplicate walkers).

        Cycle detection: a hop name that already appears on the visited
        stack (including ``source_model.name``) raises ``ValueError`` with
        the offending path.

        Missing-join behaviour:

        * ``strict_missing_join=True`` (cross-model-measure callers) —
          raise ``ValueError`` listing the available joins.
        * ``strict_missing_join=False`` (dimension callers) — raise the
          internal :class:`_NoJoinError` sentinel so the caller can map
          to a ``None`` return.
        """
        current_model = source_model
        visited = {source_model.name}
        first_join: "ModelJoin | None" = None
        for i, hop_name in enumerate(hop_names):
            if hop_name in visited:
                raise ValueError(
                    f"Circular join detected while resolving "
                    f"'{'.'.join(hop_names)}': '{hop_name}' already visited "
                    f"({' → '.join(visited)} → {hop_name})"
                )
            join = next(
                (j for j in current_model.joins if j.target_model == hop_name),
                None,
            )
            if join is None:
                if strict_missing_join:
                    raise ValueError(
                        f"Model '{current_model.name}' has no join to "
                        f"'{hop_name}'. Available joins: "
                        f"{[j.target_model for j in current_model.joins]}"
                    )
                raise _NoJoinError(hop_name)
            if i == 0:
                first_join = join
            current_model = await self._resolve_model(
                model_name=hop_name,
                named_queries=named_queries or {},
                prefer_data_source=current_model.data_source or None,
            )
            visited.add(hop_name)
        return current_model, first_join

    async def _auto_move_fields_to_dimensions(
        self,
        query: SlayerQuery,
        model: SlayerModel,
        named_queries: dict,
    ) -> SlayerQuery:
        """Move bare (no-colon) measure-formula entries to dimensions when they
        name a column that isn't a (named) ModelMeasure formula.

        LLMs frequently place column names in ``measures`` instead of
        ``dimensions``. When an entry has no colon (no aggregation) and
        resolves as a column but NOT as a model-level ModelMeasure formula,
        silently move it to ``dimensions`` with a warning.
        """
        if not query.measures:
            return query

        kept: list = []
        extra_dims = list(query.dimensions or [])
        moved = False

        for f in query.measures:
            formula = f.formula.strip()
            # Only consider bare names (no colon, no operators, no parens)
            if ":" not in formula and not any(c in formula for c in "+-*/()"):
                if "." not in formula:
                    # Local reference
                    is_col = model.get_column(formula) is not None
                    is_named_measure = model.get_measure(formula) is not None
                    if is_col and not is_named_measure:
                        logger.warning(
                            "Auto-moved '%s' from measures to dimensions (not a named measure formula)",
                            formula,
                        )
                        extra_dims.append(ColumnRef(name=formula))
                        moved = True
                        continue
                else:
                    # Cross-model reference — walk the full join path
                    parts = formula.split(".")
                    try:
                        col_def = await self._resolve_dimension_via_joins(
                            model=model, parts=parts, named_queries=named_queries,
                        )
                    except ValueError:
                        col_def = None  # Circular join — leave in measures
                    if col_def is not None:
                        # parts[-2] is the terminal model containing the column at parts[-1]
                        terminal_model_name = parts[-2]
                        try:
                            terminal_model = await self._resolve_model(
                                model_name=terminal_model_name,
                                named_queries=named_queries or {},
                                prefer_data_source=model.data_source or None,
                            )
                        except ValueError:
                            terminal_model = None
                        is_named_measure = (
                            terminal_model.get_measure(parts[-1]) is not None
                            if terminal_model else False
                        )
                        if not is_named_measure:
                            logger.warning(
                                "Auto-moved '%s' from measures to dimensions (not a named measure formula)",
                                formula,
                            )
                            extra_dims.append(ColumnRef(name=formula))
                            moved = True
                            continue
            kept.append(f)

        if not moved:
            return query
        return query.model_copy(update={"measures": kept or None, "dimensions": extra_dims})

    async def _resolve_cross_model_measure(
        self,
        spec_name: str,
        field_name: str,
        model: SlayerModel,
        query,
        dimensions: list,
        time_dimensions: list,
        label: str = None,
        named_queries: dict = None,
        aggregation_name: str = None,
        agg_kwargs: dict = None,
    ) -> CrossModelMeasure:
        """Resolve a cross-model measure reference like 'customers.avg_score'.

        Supports multi-hop paths: 'claim_coverage.claim_amount.total_claim_amount'
        walks the join graph hop-by-hop to reach the final model.

        Looks up the join from the source model, loads the target model
        (checking named queries first), finds shared dimensions, and returns
        a CrossModelMeasure for SQL generation.
        """
        parts = spec_name.split(".")
        if len(parts) < 2:
            raise ValueError(f"Invalid cross-model measure reference: '{spec_name}'")
        measure_name = parts[-1]
        hop_names = parts[:-1]  # e.g. ["claim_coverage", "claim_amount"]

        # Walk the join chain to find the final target model. v4 (DEV-1330):
        # ``_walk_join_chain`` keeps each hop scoped to the source model's
        # datasource, so ``customers.revenue:sum`` against ``orders@db_a``
        # never silently pulls ``customers@db_b``.
        target_model, first_join = await self._walk_join_chain(
            source_model=model,
            hop_names=hop_names,
            named_queries=named_queries,
            strict_missing_join=True,
        )

        target_model_name = hop_names[-1]
        join = first_join  # For join_pairs: source model → first hop

        # Find the column in the target model
        if measure_name == "*":
            measure_def = Column(name="*", sql=None)
        else:
            from slayer.core.enums import NUMERIC_ONLY_AGGREGATIONS

            col_def = target_model.get_column(measure_name)
            if col_def is None:
                raise ValueError(
                    f"Column '{measure_name}' not found in model '{target_model_name}'. "
                    f"Available columns: {[c.name for c in target_model.columns]}"
                )
            if (
                aggregation_name
                and aggregation_name in NUMERIC_ONLY_AGGREGATIONS
                and str(col_def.type) == "string"
            ):
                raise ValueError(
                    f"Aggregation '{aggregation_name}' is not applicable to "
                    f"string column '{measure_name}' in model '{target_model_name}'."
                )
            measure_def = col_def

        # The cross-model sub-query starts FROM the source table with JOIN to
        # the target, so all source dimensions are available for grouping.
        # Use all query dimensions and time dimensions as the grouping context.
        shared_dims = list(dimensions)
        shared_time_dims = list(time_dimensions)

        query_model_name = query.source_model if isinstance(query.source_model, str) else model.name

        # Resolve aggregation: explicit colon syntax required
        if aggregation_name:
            agg = aggregation_name
            canonical = f"_{aggregation_name}" if measure_name == "*" else f"{measure_name}_{aggregation_name}"
        else:
            raise ValueError(
                f"Cross-model measure '{spec_name}' must include an aggregation (e.g., '{spec_name}:sum')."
            )

        hop_path = ".".join(hop_names)
        alias = f"{query_model_name}.{hop_path}.{canonical}"
        aggregation_def = target_model.get_aggregation(agg)

        # Infer format from the target model's measure and aggregation
        cm_format = _infer_aggregated_format(
            model=target_model,
            measure_name=measure_name,
            aggregation=agg,
        )

        # Expand derived references inside the target column's sql so that
        # cross-model measures over chained derivations work. measure_def.sql
        # is None for ``*:count``; nothing to expand there.
        from slayer.engine.column_expansion import expand_derived_refs

        expanded_measure_sql = measure_def.sql
        if measure_def.sql:
            try:
                ds = await self.storage.get_datasource(target_model.data_source) \
                    if self.storage and target_model.data_source else None
            except Exception:  # noqa: BLE001
                ds = None
            cross_dialect = self._dialect_for_type(ds.type) if ds else "postgres"

            async def _resolve_for_cross(model_name, named_queries):
                try:
                    return await self._resolve_model(
                        model_name=model_name,
                        named_queries=named_queries or {},
                        prefer_data_source=target_model.data_source or None,
                    )
                except Exception:  # noqa: BLE001
                    return None

            expanded = await expand_derived_refs(
                sql=measure_def.sql,
                model=target_model,
                alias_path=target_model_name,
                resolve_model=_resolve_for_cross,
                named_queries=named_queries or {},
                dialect=cross_dialect,
            )
            if expanded is not None:
                expanded_measure_sql = expanded

        return CrossModelMeasure(
            name=field_name,
            alias=alias,
            target_model_name=target_model_name,
            target_model_sql_table=target_model.sql_table,
            target_model_sql=target_model.sql,
            measure=EnrichedMeasure(
                name=canonical,
                sql=expanded_measure_sql,
                aggregation=agg,
                alias=f"{target_model_name}.{canonical}",
                model_name=target_model_name,
                aggregation_def=aggregation_def,
                agg_kwargs=agg_kwargs or {},
                source_measure_name=measure_name,
            ),
            join_pairs=join.join_pairs,
            join_type=str(join.join_type),
            shared_dimensions=shared_dims,
            shared_time_dimensions=shared_time_dims,
            source_model_name=model.name,
            source_sql_table=model.sql_table,
            source_sql=model.sql,
            label=label,
            format=cm_format,
        )

    async def _build_rerooted_enriched(
        self,
        cm: CrossModelMeasure,
        query: SlayerQuery,
        model: SlayerModel,
        named_queries: dict,
    ) -> EnrichedQuery:
        """Build a re-rooted EnrichedQuery for a cross-model measure.

        Instead of the minimal source→target CTE, this constructs a full query
        with the target model as source. All of the target model's joins are
        available, so filters on related tables (e.g., premium.has_premium)
        are applied correctly.

        Dimensions and filters referencing models not reachable from the
        target are dropped.
        """
        import re

        from slayer.core.formula import parse_filter

        target_model = await self._resolve_model(
            model_name=cm.target_model_name,
            named_queries=named_queries,
            prefer_data_source=model.data_source or None,
        )

        source_model_name = model.name
        target_model_name = cm.target_model_name

        # --- Build re-rooted field (measure becomes local) ---
        measure_name = cm.measure.source_measure_name or cm.measure.name
        aggregation = cm.measure.aggregation
        if cm.measure.agg_kwargs:
            kwargs_str = ", ".join(f"{k}={v}" for k, v in cm.measure.agg_kwargs.items())
            field_formula = f"{measure_name}:{aggregation}({kwargs_str})"
        else:
            field_formula = f"{measure_name}:{aggregation}"

        # --- Remap dimensions ---
        rerooted_dims = []
        for dim in (query.dimensions or []):
            if dim.model is None:
                # Source-local dimension → cross-model from target's perspective
                rerooted_dims.append(ColumnRef(name=f"{source_model_name}.{dim.name}"))
            elif dim.model == target_model_name:
                # Dimension on target model → now local
                rerooted_dims.append(ColumnRef(name=dim.name))
            elif dim.model.startswith(target_model_name + "."):
                # Path through target → strip target prefix
                new_model = dim.model[len(target_model_name) + 1:]
                rerooted_dims.append(ColumnRef(name=f"{new_model}.{dim.name}"))
            else:
                # Other cross-model dim → keep as-is (enrichment resolves via target's joins)
                rerooted_dims.append(ColumnRef(name=dim.full_name))

        # --- Remap time dimensions ---
        rerooted_time_dims = []
        for td in (query.time_dimensions or []):
            dim_ref = td.dimension
            if dim_ref.model is None:
                new_ref = ColumnRef(name=f"{source_model_name}.{dim_ref.name}")
            elif dim_ref.model == target_model_name:
                new_ref = ColumnRef(name=dim_ref.name)
            elif dim_ref.model.startswith(target_model_name + "."):
                new_model = dim_ref.model[len(target_model_name) + 1:]
                new_ref = ColumnRef(name=f"{new_model}.{dim_ref.name}")
            else:
                new_ref = ColumnRef(name=dim_ref.full_name)
            rerooted_time_dims.append(TimeDimension(
                dimension=new_ref,
                granularity=td.granularity,
                date_range=td.date_range,
                label=td.label,
            ))

        # --- Remap filters ---
        rerooted_filters = []
        target_prefix = target_model_name + "."
        _custom_agg_names = frozenset(
            a.name for m in (model, target_model)
            for a in m.aggregations
        ) or None
        for f_str in (query.filters or []) + list(model.filters):
            remapped = f_str
            # Strip target model prefix from dotted references
            # e.g., "policy_amount.premium.has_premium = '1'" → "premium.has_premium = '1'"
            if target_prefix in remapped:
                remapped = remapped.replace(target_prefix, "")
            # For unqualified column references that are source model dimensions,
            # prepend source model name (they're now on a joined table)
            parsed = parse_filter(remapped, extra_agg_names=_custom_agg_names)
            for col in parsed.columns:
                if "." not in col:
                    src_col = model.get_column(col)
                    if src_col:
                        remapped = re.sub(
                            rf"(?<!\.)(?<!\w)\b{re.escape(col)}\b(?!\.)",
                            f"{source_model_name}.{col}",
                            remapped,
                        )
            rerooted_filters.append(remapped)

        # --- Build and enrich re-rooted query ---
        rerooted_query = SlayerQuery(
            source_model=target_model_name,
            measures=[ModelMeasure(formula=field_formula)],
            dimensions=rerooted_dims or None,
            time_dimensions=rerooted_time_dims or None,
            filters=rerooted_filters or None,
        )

        # Re-rooted enrichment intentionally inherits the outer query's
        # filter list; some filters reference models reachable from the
        # outer source but not from ``target_model``. ``drop_unreachable_filters``
        # tells the resolver to drop those entries from the result instead
        # of raising the DEV-1367 strict-resolution error.
        rerooted_enriched = await self._enrich(
            query=rerooted_query,
            model=target_model,
            named_queries=named_queries,
            drop_unreachable_filters=True,
        )

        # --- Fix aliases to match main query's expectations ---
        # Dimensions: rerooted aliases are "target.source.dim", main expects "source.dim"
        main_dim_aliases = [d.alias for d in cm.shared_dimensions]
        for i, dim in enumerate(rerooted_enriched.dimensions):
            if i < len(main_dim_aliases):
                dim.alias = main_dim_aliases[i]

        main_td_aliases = [td.alias for td in cm.shared_time_dimensions]
        for i, td in enumerate(rerooted_enriched.time_dimensions):
            if i < len(main_td_aliases):
                td.alias = main_td_aliases[i]

        # Measure alias
        if rerooted_enriched.measures:
            rerooted_enriched.measures[0].alias = cm.alias

        # --- Strip unreachable dimensions and filters ---
        available_aliases = {target_model_name}
        for _, alias, _, _ in rerooted_enriched.resolved_joins:
            available_aliases.add(alias)

        rerooted_enriched.dimensions = [
            d for d in rerooted_enriched.dimensions
            if d.model_name == target_model_name or d.model_name in available_aliases
        ]
        rerooted_enriched.time_dimensions = [
            td for td in rerooted_enriched.time_dimensions
            if td.model_name == target_model_name or td.model_name in available_aliases
        ]
        rerooted_enriched.filters = [
            f for f in rerooted_enriched.filters
            if all(
                col.split(".")[0] in available_aliases or "." not in col
                for col in f.columns
            )
        ]

        return rerooted_enriched

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
    def _dialect_for_type(ds_type: str | None) -> str:
        """Map a datasource-config ``type`` string to a sqlglot dialect name.

        Delegates to ``dialect_for_ds_type`` in ``slayer.sql.dialects``
        (DEV-1542). Returns a string for back-compat with the 24+ test
        sites that assert ``_dialect_for_type("postgres") == "postgres"``.
        Unknown / ``None`` / empty ds-types fall back to ``"postgres"``.
        """
        return dialect_for_ds_type(ds_type).sqlglot_name
