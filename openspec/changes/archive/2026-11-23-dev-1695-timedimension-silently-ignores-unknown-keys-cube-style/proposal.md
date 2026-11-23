## Why

A time dimension silently drops keys it does not read, so a Cube-style `dateRange` runs the query without its date filter and returns plausible, wrong numbers. Query objects disagree on unknown keys (a typed error, a bare Pydantic error, or nothing), and none validates an assignment after construction, so the same failure can arrive through either door.

## What Changes

- **BREAKING** Every query input object (query, refinement, dimension, metric, time dimension and order entries, query extension) refuses a key it does not read with a typed `UnknownKeyError` naming the key and listing the keys the object takes. Previously a time dimension dropped it silently, and four objects raised a bare Pydantic `extra_forbidden`.
- A key that is the camelCase spelling of a key the object accepts (`dateRange`, `timeDimensions`, `sourceModel`) is converted silently to that key, before any pre-2.0 spelling converts; both spellings together are refused as a spelling conflict.
- `compareDateRange` is refused with a pointer to SLayer's period comparison (`change`, `change_pct`, `time_shift`).
- Stored queries follow the same rules: a camelCase key converts on load and dumps canonical; any other unknown key fails that document's load, leaving the rest of the store loadable.
- **BREAKING** Query input objects validate assignments; a failed assignment raises and leaves the object unchanged.
- A new `python.core` principle: input refuses what it does not read.

## Capabilities

### New Capabilities

### Modified Capabilities
- `compatibility/ossie-vocabulary`: queries refuse unknown keys with a typed error, heal camelCase spellings, and validate assignments atomically.
- `queries/time-dimensions`: Cube's `dateRange` and `compareDateRange` on a time dimension.

## Impact

- `slayer/core/query.py` (all query input objects, their input conversion and refusal), `slayer/core/vocabulary.py` (spelling conversion with camelCase heal), `slayer/core/errors.py` (reused errors).
- REST / MCP / Python client: unknown query keys now raise a typed error (REST still 422); camelCase keys are accepted.
- Stored saved queries: those holding `dateRange` now apply the range; those holding other unknown query keys stop loading, with a typed error.
- `architecture/python.core.arc42.md` §3 gains principle 8.
- Docs: `docs/concepts/queries.md`, `docs/concepts/datasets.md`, `docs/upgrading/2.0.md`.
- Dataset-side assignment validation is out of scope (DEV-2398).
