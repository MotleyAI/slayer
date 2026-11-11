## Why

Merging the 2.0 line into `main` resolved list-column conflicts toward the 2.0 side and silently dropped two 1.x rules, so a 1.8.x store relying on them fails to load on 2.0: an `sql`-mode `ARRAY` column without `element_type`, and `collect` on a sole primary key. An audit of every 1.x requirement whose text differs on `main` found four more clauses lost from the spec (their code survived, their tests did not). Nothing today stops the next forward merge from dropping 1.x behaviour the same way.

## What Changes

- An `sql`-mode model's `ARRAY` column without a declared `element_type` loads and saves with `element_type: UNKNOWN` again (table-backed models still require one).
- `collect` joins the aggregations a sole primary key allows; `collect(id)` and `allowed_aggregations: [count, collect]` on a sole primary key work again.
- Four spec clauses lost in the merge are restored, with their tests: a library-named aggregation `contains` answering, the shadow warning on REST and MCP, a re-type's field effects converging, and the granularity rename converging on both stores.
- A forward-merge spec guard: a CI check that every requirement 1.x adds or modifies in the commits a pull request brings into its base exists verbatim on the head, or is ledgered with a reason.
- A committed store written by SLayer 1.8.2 over the list-column surface must load, re-save, re-ingest and return 1.8.2's rows.

## Capabilities

### New Capabilities
- `compatibility/forward-merge-specs`: the forward-merge spec guard and its ledger.

### Modified Capabilities
- `models/list-columns`: an `sql`-mode list column's undeclared element type is `UNKNOWN`.
- `models/identifier-columns`: a sole primary key allows `collect`.
- `models/aggregation-names`: a stored aggregation named `contains` answers; the shadow warning reaches REST and MCP.
- `models/schema-drift`: a re-type to a list keeps description, label and meta, drops `format` and time granularity, and converges.
- `models/stored-upgrade`: the 1.8.2 corpus opens with its release's results; the granularity rename holds on both stores and converges.

## Impact

- Code: `slayer/core/models.py` (element-type validation), `slayer/core/enums.py` (`PRIMARY_KEY_AGGREGATIONS`), new `scripts/forward_merge_specs.py`, `.github/workflows/ci.yml` (lint job), new `openspec/forward-merge-ledger.yaml`.
- Tests: new `tests/fixtures/upgrade/v1_8_2/` corpus and its test, the guard's tests, ported 1.x tests.
- Docs: `docs/concepts/models.md` (primary-key aggregations, list-column `allowed_aggregations`), sql-mode list-column docs.
- No stored-document version bump; no migration-guide entry (1.8.x behaviour is kept).
