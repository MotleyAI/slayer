## Why

The stored date-only upgrade (SlayerModel v23→v24, SlayerQuery v14→v15, Memory v10→v11) rewrites a comparison only when it can type the operand as TIMESTAMP, and it types against the datasource's stored models alone. The built-in time spine is not stored, so `time_spine.timestamp <= '2024-01-31'` keeps its text and silently stops at midnight, dropping Jan 31; a query rooted at the spine loses typing for every operand. Upgraded stores cannot later tell which comparisons were skipped, so the fix must land before 2.0 ships the upgrade.

## What Changes

- The upgrade's typing source declares the built-in time spine as one more model of the datasource, under every spelling the engine resolves to it (`time_spine`, `TIME_SPINE`, other case variants) unless a stored model owns that spelling.
- A path may reach the spine from any model, and a query rooted at the spine reaches the datasource's models; a path that crosses the spine stays untypable.
- The legacy time-literal repair resolves a stored query's host through the same typing source, so it sees the spine and case-variant spellings too.
- No new document versions: the upgrade is unreleased and is amended in place.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/stored-upgrade`: "Stored date-only comparisons keep their whole-day meaning" decides temporal operands with the datasource's models including the built-in time spine.

## Impact

- `slayer/storage/date_only_migration.py` (`StoredModels` name resolution and hops), `slayer/storage/base.py` (`_query_host` legacy-repair host lookup), `slayer/core/time_spine.py` (the spine's definition, read for its columns).
- No API, schema or dependency change; no docs change (docs/upgrading/2.0.md already states the behaviour the fix restores).
