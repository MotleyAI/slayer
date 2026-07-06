# Query Cache

SLayer has an optional, in-memory, per-query result cache **local to a single
`SlayerQueryEngine` instance**. It is opt-in per call via `cache=True`, modelled
on [Cube's in-memory cache](https://cube.dev/docs/product/caching). This is a
**Python-API-only** feature — there is no REST / MCP / CLI / Flight / pg-facade
surface, and no `SlayerClient` plumbing.

Two engines with different connection settings keep separate caches, so a cached
result is never served across datasource identities.

## Enabling the cache

Construct the engine with a `CacheConfig`, then pass `cache=True` per call:

```python
from slayer.engine.cache import CacheConfig
from slayer.engine.query_engine import SlayerQueryEngine

engine = SlayerQueryEngine(
    storage,
    cache_config=CacheConfig(
        ttl_seconds=300,
        refresh_keys=[("public.orders", "MAX(updated_at)")],
    ),
)

# Any input shape works — SlayerQuery / dict, a multi-stage list, or a
# run-by-name string. Each funnels into one final SQL that is cached.
resp = await engine.execute({"source_model": "orders",
                             "measures": [{"formula": "amount:sum"}]},
                            cache=True)
```

`cache_config` defaults to an empty `CacheConfig()` when not supplied.
Reassigning `engine.cache_config = CacheConfig(...)` **clears the cache**.

`cache=True` is ignored (no caching, no error) when `dry_run` or `explain` is
set.

### `CacheConfig`

| Field | Meaning |
| --- | --- |
| `ttl_seconds: Optional[float]` | Wall-clock age bound. `None` (default) means no time-based expiry. |
| `refresh_keys: List[Tuple[str, str]]` | Cube-style `(physical_table, select_expression)` pairs. The same table may repeat with different expressions. |

An empty/default `CacheConfig` caches indefinitely with no automatic staleness —
only `evict` / `clear_cache` / an explicit re-execution change an entry.

## Cache key

The key is `sha256(final_generated_sql + "|" + connection_string + "|" +
runtime_fingerprint)`. Variables are already substituted into the SQL before the
key is computed, so different variable sets produce different keys automatically.
The datasource identity is the engine's SQL-client fingerprint (not the bare
datasource name), so a config edit under the same name never serves the wrong
rows.

Because the key needs the SQL, the resolve → enrich → SQL-generation pipeline
runs on **every** cached call (cheap, no DB hit). A cache hit skips only the DB
execution and result decode.

## Staleness

Each entry has two independent staleness signals.

**TTL** is checked lazily on read: if an entry's age exceeds `ttl_seconds`, the
read is treated as a miss and the entry is re-executed synchronously.

**Refresh keys** are acted on only by an explicit `engine.refresh()`. For each
entry, the *applicable* refresh keys are those whose table is referenced by the
entry's SQL. A baseline value per applicable key is captured at cache-write time
(scanned **before** the data query, so cached data always reflects a state at
least as new as the baseline). `refresh()` re-evaluates each applicable key and
re-executes the entry if any value differs.

Refresh-key sensitivity is your choice of expression:

- `MAX(updated_at)` misses in-place updates that don't move the column, backfills
  below the current max, and inserts with old timestamps.
- A `COUNT(*)`-bearing expression additionally catches deletes.
- A hash / concatenation (e.g. `MAX(updated_at) || '|' || COUNT(*)`) catches more.

`ttl_seconds` is the time-based backstop. SLayer evaluates each
`select_expression` verbatim as `SELECT <select_expression> FROM <table>` — it is
**not** wrapped in `MAX(...)`.

## Management methods

```python
await engine.refresh()                 # -> RefreshResult   (+ engine.refresh_sync())
await engine.evict(query)              # -> bool            (+ engine.evict_sync())
engine.clear_cache()                   # drop all entries
engine.cache_size                      # -> int (live entry count)
```

`evict(query, variables=None, data_source=None)` accepts the same input union as
`execute`, recomputes the SQL + datasource key (no DB execution), and removes
that one entry. `refresh()` returns a `RefreshResult`:

```python
class RefreshResult:
    refreshed: List[str]          # re-run because a refresh-key value moved
    expired_refreshed: List[str]  # re-run because the TTL lapsed
    unchanged: List[str]
    errors: List[RefreshError]    # continue-on-failure diagnostics
```

`refresh()` re-prepares each stale entry from its **original input** through the
full `execute` pipeline (not the frozen SQL), so `whole_periods_only` boundaries
re-snap and model/schema edits are picked up. If the freshly-prepared SQL differs
(a new period, an edited model), the entry is re-keyed under the new SQL hash.
`refresh()` is continue-on-failure: a failed refresh-key scan or re-execution
leaves the existing entry unchanged and records a `RefreshError`.

The synchronous wrappers `execute_sync(..., cache=True, data_source=...)`,
`refresh_sync()`, and `evict_sync(...)` are available for CLI / notebook /
script use.

## Limitations

- **In-memory, per-process.** The cache lives on the engine instance; it is not
  persisted across restarts and is not shared across engines or processes.
- **No request coalescing.** Concurrent identical misses both execute
  (last-writer-wins on store).
- **Unbounded.** There is no LRU or size cap; manage memory with `evict` /
  `clear_cache`.
- **Python API only.** No REST / MCP / CLI / Flight / pg-facade / `SlayerClient`
  surface.
