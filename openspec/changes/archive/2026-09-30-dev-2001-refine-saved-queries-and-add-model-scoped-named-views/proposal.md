## Why

A saved (query-backed) model can run by name or be stacked under a new query, but both only
post-process its output: nothing can add a dimension, measure or row filter to the saved query
itself (semantic-layer comparison row Q23, where Malloy's `view + { ... }` refinement is the
reference). Agents also cannot find the saved queries built on a model from that model.

## What Changes

- Run-by-name accepts a `refine` argument (a typed `QueryRefinement`) on every surface — Python
  engine and client, REST `POST /query` next to `name`, the MCP `query` tool, and the CLI
  `--refine` flag. Its clauses merge into the saved query's final stage, which then runs as one
  hand-written query: dimensions and measures union (identical duplicates collapse, conflicting
  same-named entries raise `RefinementConflictError`), filters AND, time dimensions merge per
  (column, granularity), and `order` / `limit` / `offset` and the scalar settings replace.
- `refine` with anything but a saved-query name is rejected; REST flat query fields next to `name`
  stay rejected (now also when supplied as `null`).
- Saving a query-backed model pins each stage's inferred population as its stored `source_model`,
  so later model edits and refinements never move a saved query's source; a stage whose inferred
  model shares its name with a stage of the same saved query is refused at save.
- `inspect` of a model lists the saved queries whose stages read it (`saved_queries`).
- The cached result of a refined run refreshes with its refinement.

## Capabilities

### New Capabilities
- `queries/saved-query-refinement`: merging a refinement into a saved query's final stage at
  run-by-name — merge rules, conflicts, variables, surfaces, caching, and model discoverability.

### Modified Capabilities
- `mcp/query-tool`: the tool gains the `refine` wrapper argument; run-by-name strings accept it.
- `queries/population`: a saved query's inferred populations are pinned at save.

## Impact

- `slayer/core/query.py` (`QueryRefinement`, `refine_query`, shared field-type aliases),
  `slayer/core/errors.py` (`RefinementConflictError`).
- `slayer/engine/query_engine.py` (`refine` on `execute` / `execute_sync` / `evict` /
  `evict_sync`, an internal saved-query-run input, save-time population pinning).
- Surfaces: `slayer/api/server.py`, `slayer/mcp/server.py`, `slayer/cli.py`,
  `slayer/client/slayer_client.py`, `slayer/inspect/`.
- `architecture/engine.arc42.md` P8 gains the population-pinning exception (approved).
- Docs: `docs/concepts/queries.md`, `docs/concepts/models.md`, run-by-name reference and
  interface pages, `slayer/memories/help_content/01_models.md`.
