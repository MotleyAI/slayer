"""Per-engine, in-memory query result cache (DEV-1587).

A query-level result cache local to a single :class:`SlayerQueryEngine`
instance, modelled on Cube's in-memory cache. Caching is opt-in per call
via ``execute(query, cache=True)``. Staleness is governed by an optional
time-to-live (``ttl_seconds``, checked lazily on read) and an optional set
of Cube-style ``(physical_table, select_expression)`` refresh keys scanned
by an explicit ``engine.refresh()``.

This module holds the DB-free half of the feature: the cache dict, the
cache key, TTL bookkeeping, sqlglot-based table detection (CTE-alias
excluding), refresh-key applicability, refresh-key scan-SQL building, and
value comparison. All of it is unit-testable without a database. The
engine (``slayer/engine/query_engine.py``) owns the DB awaits (data query,
refresh-key scans, re-execution) and never holds the cache lock across
one.
"""

import asyncio
import hashlib
import time
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

# Marker alias prefix for refresh-key scan projections. Also lets tests
# distinguish a refresh-key scan query from a data query.
_RK_ALIAS_PREFIX = "slayer_rk_"

# Normalized physical-table identity: (catalog, db, name). Parts absent
# from the source expression are ``None``.
NormalizedTable = tuple[str | None, str | None, str]


class CacheConfig(BaseModel):
    """Per-engine cache configuration.

    ``ttl_seconds`` bounds wall-clock entry age (``None`` => no time-based
    expiry). ``refresh_keys`` is a list of ``(physical_table,
    select_expression)`` pairs; the same table may repeat with different
    expressions. Each ``select_expression`` is a scalar SQL expression
    evaluated verbatim as ``SELECT <select_expression> FROM <table>`` — the
    user supplies it in full (SLayer does NOT wrap it in ``MAX(...)``).
    """

    ttl_seconds: float | None = None
    refresh_keys: list[tuple[str, str]] = Field(default_factory=list)


class RefreshKeyValue(BaseModel):
    """A captured refresh-key baseline: the value of ``expression`` scanned
    from ``table`` at cache-write (or last-refresh) time."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    table: str
    expression: str
    value: Any = None


class RefreshError(BaseModel):
    """A per-entry / per-table failure recorded during ``refresh()``.

    ``phase`` is ``"refresh_key_scan"`` (a table's batched scan raised) or
    ``"re_execute"`` (re-running a stale entry raised). ``key`` is the cache
    key for a re-execute failure, or the physical table for a scan failure.
    """

    key: str
    phase: str
    message: str


class RefreshResult(BaseModel):
    """Outcome of ``engine.refresh()``.

    ``refreshed`` — keys re-run because an applicable refresh-key value
    moved. ``expired_refreshed`` — keys re-run because their TTL lapsed.
    ``unchanged`` — keys left as-is. ``errors`` — continue-on-failure
    diagnostics.
    """

    refreshed: list[str] = Field(default_factory=list)
    expired_refreshed: list[str] = Field(default_factory=list)
    unchanged: list[str] = Field(default_factory=list)
    errors: list[RefreshError] = Field(default_factory=list)


class _CacheEntry(BaseModel):
    """One cached result plus everything needed to re-scan its refresh keys
    and re-prepare + re-execute it from the original user input.

    ``response`` holds a :class:`SlayerResponse` (typed ``Any`` to avoid a
    circular import with ``query_engine``). ``original_input`` is the raw
    user input shape (``SlayerQuery`` / ``dict`` / ``list`` / ``str``) so
    ``refresh()`` can replay through the full ``execute`` normalization.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    response: Any
    sql: str
    ds_fingerprint: str
    dialect: str
    ds_key: tuple[str, str]
    resolved_data_source: str | None = None
    original_input: Any = None
    variables: dict[str, Any] | None = None
    data_source: str | None = None
    created_at: float = 0.0
    applicable: list[tuple[str, str]] = Field(default_factory=list)
    refresh_key_values: list[RefreshKeyValue] = Field(default_factory=list)


class QueryCache:
    """In-memory ``dict[str, _CacheEntry]`` with TTL-aware reads.

    The :class:`asyncio.Lock` guards only in-memory dict operations
    (get / put / delete / snapshot / commit) — the engine performs every DB
    await outside the lock. The ``clock`` is injectable for deterministic
    TTL testing (``time.monotonic`` by default).
    """

    def __init__(
        self,
        config: CacheConfig,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self._clock = clock
        self._entries: dict[str, _CacheEntry] = {}
        self._lock = asyncio.Lock()

    # ---- key / clock / size ------------------------------------------------

    @staticmethod
    def make_key(sql: str, ds_fingerprint: str) -> str:
        """``sha256(final_sql + "|" + ds_fingerprint)``.

        ``ds_fingerprint`` is the engine's SQL-client cache fingerprint
        (``connection_string|runtime_fingerprint``), so a config edit under
        the same datasource name never serves the wrong rows.
        """
        return hashlib.sha256(f"{sql}|{ds_fingerprint}".encode()).hexdigest()

    def now(self) -> float:
        """Read the injectable clock (used for entry ``created_at``)."""
        return self._clock()

    def size(self) -> int:
        return len(self._entries)

    def clear(self) -> None:
        self._entries.clear()

    # ---- lock-guarded dict ops --------------------------------------------

    async def get(self, key: str) -> _CacheEntry | None:
        """Return the live entry, or ``None``. TTL-expired entries are
        deleted and reported as a miss (re-execution re-populates them)."""
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if self.config.ttl_seconds is not None:
                if (self._clock() - entry.created_at) > self.config.ttl_seconds:
                    del self._entries[key]
                    return None
            return entry

    async def put(self, key: str, entry: _CacheEntry) -> None:
        async with self._lock:
            self._entries[key] = entry

    async def delete(self, key: str) -> bool:
        async with self._lock:
            if key in self._entries:
                del self._entries[key]
                return True
            return False

    async def snapshot(self) -> dict[str, _CacheEntry]:
        """A shallow copy of the ``{key: entry}`` map so ``refresh()`` can
        iterate without racing concurrent ``execute()`` writes."""
        async with self._lock:
            return dict(self._entries)

    async def commit_replace(
        self,
        *,
        old_key: str,
        expected: _CacheEntry,
        new_key: str,
        new_entry: _CacheEntry,
    ) -> bool:
        """Identity-guarded write used by ``refresh()``.

        Only mutate if the live entry at ``old_key`` is still the SAME
        object ``refresh()`` snapshotted. If it was evicted / cleared /
        replaced by a newer ``execute()`` during ``refresh()``'s DB awaits,
        skip the write (return ``False``) — never resurrect a gone entry or
        clobber a newer result. On a re-key (``new_key != old_key``) the old
        key is dropped; a ``new_key`` collision is last-writer-wins (both
        results are interchangeable — identical SQL + ds fingerprint).
        """
        async with self._lock:
            if self._entries.get(old_key) is not expected:
                return False
            if new_key != old_key:
                self._entries.pop(old_key, None)
            self._entries[new_key] = new_entry
            return True

    # ---- table detection ---------------------------------------------------

    def parse_referenced_tables(
        self, sql: str, dialect: str
    ) -> list[NormalizedTable]:
        """Normalized physical tables referenced by ``sql``.

        Parses with sqlglot, then subtracts CTE names (SLayer wraps queries
        in CTEs, so a ``FROM cte`` reference otherwise looks like a table)
        and derived-table / subquery aliases. Each remaining
        :class:`exp.Table` is normalized to ``(catalog, db, name)`` with the
        dialect's identifier folding (quoted identifiers preserved exactly,
        unquoted folded per dialect).
        """
        tree = parse_one(sql, dialect=dialect)
        excluded = {c.alias for c in tree.find_all(exp.CTE)}
        for sub in tree.find_all(exp.Subquery):
            if sub.alias:
                excluded.add(sub.alias)
        out: list[NormalizedTable] = []
        for t in tree.find_all(exp.Table):
            if t.name in excluded:
                continue
            out.append(self._normalize_table_expr(t, dialect))
        return out

    @staticmethod
    def _normalize_table_expr(table: exp.Table, dialect: str) -> NormalizedTable:
        norm = normalize_identifiers(table.copy(), dialect=dialect)
        return (norm.catalog or None, norm.db or None, norm.name)

    @classmethod
    def _normalize_config_table(cls, table: str, dialect: str) -> NormalizedTable:
        return cls._normalize_table_expr(exp.to_table(table, dialect=dialect), dialect)

    @staticmethod
    def _table_matches(config: NormalizedTable, sql_table: NormalizedTable) -> bool:
        """A config table matches a SQL table iff their normalized parts are
        equal, treating parts unspecified (``None``) in the config as
        wildcards. So ``orders`` matches ``orders`` / ``public.orders`` /
        ``db.public.orders``; ``public.orders`` matches only ``db=public``.
        """
        c_cat, c_db, c_name = config
        s_cat, s_db, s_name = sql_table
        if c_name != s_name:
            return False
        if c_db is not None and c_db != s_db:
            return False
        if c_cat is not None and c_cat != s_cat:
            return False
        return True

    def applicable_keys(self, sql: str, dialect: str) -> list[tuple[str, str]]:
        """The configured ``(table, expression)`` refresh keys whose table is
        referenced by ``sql``. The original config table string is preserved
        (duplicate expressions per table are kept)."""
        sql_tables = self.parse_referenced_tables(sql, dialect)
        out: list[tuple[str, str]] = []
        for table, expr in self.config.refresh_keys:
            config_norm = self._normalize_config_table(table, dialect)
            if any(self._table_matches(config_norm, s) for s in sql_tables):
                out.append((table, expr))
        return out

    def build_refresh_key_sql(
        self, table: str, expressions: list[str], dialect: str
    ) -> str:
        """``SELECT (<expr0>) AS "slayer_rk_0", ... FROM <table>``.

        Each user expression is parsed with the entry's dialect and
        re-emitted so quoting/dialect are consistent; the table identifier is
        quoted dialect-safely. One scan covers all of a table's applicable
        refresh keys.
        """
        projections = [
            exp.alias_(
                exp.paren(parse_one(e, dialect=dialect)),
                f"{_RK_ALIAS_PREFIX}{i}",
                quoted=True,
            )
            for i, e in enumerate(expressions)
        ]
        select = exp.select(*projections).from_(exp.to_table(table, dialect=dialect))
        return select.sql(dialect=dialect)

    @staticmethod
    def rk_alias(index: int) -> str:
        """The scan projection alias for the ``index``-th expression."""
        return f"{_RK_ALIAS_PREFIX}{index}"

    @staticmethod
    def values_differ(a: Any, b: Any) -> bool:
        """Equality comparison across DB scalar types (timestamps / ints /
        strings / concatenations). Any inequality — including a decrease —
        signals staleness."""
        return a != b
