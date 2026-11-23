## Why

A stored 1.x query with no pinned datasource and no population fails to load in 2.0, even on a single-datasource store: its qualified-reference upgrade finds no datasource to read. A root-less or `time_spine` memory that buckets by a custom granularity breaks too: its colon buckets are not rewritten to `date_trunc` and a library-colliding granularity is not renamed. The store upgrade first ships in 2.0, so a 1.x store must load in 2.0.0 as it loaded in 1.x; this gates the release.

## What Changes

- An unpinned stored query that names no population, or whose population spells the time spine, is read in one datasource by every load-time upgrade: the datasources holding a dataset its references or extensions name, else every datasource, the datasource priority first, then the datasource name.
- Its qualified-reference upgrade infers the population there and qualifies; its literals are typed there; its custom-granularity buckets are rewritten with that datasource's granularities; a library-colliding granularity of that datasource is renamed in it.
- An explicit population that names no stored dataset is unchanged: it still fails to load naming the population.
- Run-time datasource scoping of root-less queries is unchanged.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/stored-upgrade`: adds the requirement that a stored query whose population names no stored dataset is read in one datasource by every load-time upgrade.

## Impact

- `slayer/storage/qualified_references.py` (`_datasource_of`), `slayer/storage/base.py` (`_memory_data_sources`, `_rename_granularities`), `slayer/storage/granularity_rename.py`, a shared fallback and raw-memory name reader in `slayer.storage`.
- Memories, saved queries and query-backed stages; no API, wire or dependency change.
