## Why

A stored 1.x query with no pinned datasource and no population fails to load in 2.0, even on a single-datasource store: its qualified-reference upgrade finds no datasource to read. A root-less or `time_spine` memory that buckets by a custom granularity breaks too: its colon buckets are not rewritten to `date_trunc` and a library-colliding granularity is not renamed. The store upgrade first ships in 2.0, so a 1.x store must load in 2.0.0 as it loaded in 1.x; this gates the release. No 1.x release could save such a memory through its service; they come from the raw storage API, hand edits, and pre-release 2.0 builds that saved a root-less query with its entities.

## What Changes

- An unpinned stored memory that names no population, or whose population spells the time spine, is read by every load-time upgrade in the datasource its entities record (`<datasource>.<dataset>…`), else in the store's only datasource.
- Its qualified-reference upgrade infers the population there and qualifies; its literals are typed there; its custom-granularity buckets are rewritten with that datasource's granularities; a library-colliding granularity of that datasource is renamed in it.
- With neither, such a memory whose upgrade is pending fails to load with a warning naming it and saying its datasource cannot be determined.
- An explicit population that names no stored dataset is unchanged: it still fails to load naming the population.
- A memory's conversion is never written back; only the library-granularity rename persists.
- Run-time datasource scoping of root-less queries is unchanged.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/stored-upgrade`: adds the requirement that a stored memory whose population names no stored dataset is read in one datasource by every load-time upgrade.

## Impact

- `slayer/storage/qualified_references.py` (`_datasource_of`), `slayer/storage/base.py` (`_memory_from_stored`, `_memory_data_sources`, `_rename_granularities`), a shared raw reader of a memory's datasource in `slayer.storage`.
- Memories only (saved queries and query-backed stages convert in their dataset's datasource); no API, wire or dependency change.
