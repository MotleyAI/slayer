## Why

A query payload can reach the database's own SQL (Mode A) or physical columns a model hides, bypassing the model layer and with it per-user model visibility and row-level security. Five routes are reproduced on SQLite:
1. extension column `sql` / `filter`;
2. a sql-less extension column naming a hidden physical column;
3. an inline `source_model`;
4. query `variables` spliced as text into a stored model's `{var}` Mode-A text;
5. a quoted query-side aggregation argument parsed as dialect SQL.

The tenant database role scopes a tenant, not a user, so the model layer has to be the boundary for every caller.

## What Changes

- **BREAKING** `ModelExtension.columns` is removed. An extension carrying `columns` is refused at construction, pointing at an expression in the position that uses it. `ModelExtension` keeps `measures` and `joins`.
- **BREAKING** `source_model` names a stored model or stage, or is omitted. An inline model is refused at construction, and every inline-source code path is removed.
- **BREAKING** A `{var}` in a model's Mode-A text binds as a literal node of the parsed SQL (string-literal content, a bare number or boolean, a whole `IN` list). Any other position, including identifier, function name and fragment positions, is refused at model save and at query time. `{? … ?}` blocks keep their meaning. This absorbs the Mode-A half of DEV-2044.
- **BREAKING** A query-side string aggregation argument naming a placeholder is read as Mode B, exactly as its unquoted spelling. A definition default stays Mode A.
- Stored queries move to `SlayerQuery` v14. Extension columns are inlined at their use sites, with result keys kept and one warning per converted column; the conversion binds against the stored models. A document that cannot be converted fails to load, untouched on disk. Query-backed models and saved queries are written back; memories convert on each load.
- A `Column.sql` that is a statement, or several, is refused at construction.
- Column profiling samples with plain `min` / `max`; usage reporting's `inline_source` flag fires on extensions only.
- `architecture/semantics.arc42.md` §4 P2 gains sub-clause 2.3: a query carries no Mode A.

## Capabilities

### New Capabilities
(none)

### Modified Capabilities
- `queries/extensions`: extensions carry measures and joins only; the new requirement "Queries carry no Mode A"; profiling is no longer an extension producer.
- `queries/population`: a population is a name or omitted; an inline model is refused.
- `queries/multi-stage`: sibling references through inline models are gone; "Inline query-backed sources are rejected" is subsumed and removed.
- `queries/query-backed-inline`: extensions over a spliced model carry measures and joins.
- `queries/variables`: Mode-A surfaces bind variables as literals.
- `queries/identifier-fitting`, `queries/dotted-dimension-routing`, `queries/transforms`: inline-model and extension-column mentions removed.
- `aggregations/formula-templates`: a query-side string argument is Mode B.
- `aggregations/functional-form`: inline-model measure positions removed.
- `models/stored-upgrade`: stored extension columns are inlined; unconvertible documents fail to load.
- `models/save-validation`: a parameterized model source parses with literal placeholders; an identifier-position placeholder is refused.
- `models/column-filters`: variables bind into `sql` and `filter` as literals.
- `models/column-definitions`: `Column.sql` is one expression.
- `models/join-keys`, `models/join-cardinality`, `models/model-access`: extension-column and inline-model mentions removed.
- `mcp/query-tool`, `mcp/tool-surface`: the query tool's `source_model` forms and guidance.
- `usage-reporting/collection`: the extension flag.

## Impact

- **Code:**
  - `slayer/core/query.py` (`ModelExtension`, `SourceSpec`, `SlayerQuery`) and `slayer/core/models.py` (`Column.sql` check);
  - `slayer/ir/variables.py` plus the Mode-A substitution in `slayer/core/query.py`;
  - `slayer/engine/{extensions,bundle_builder,stage_ordering,model_reads,param_binding,profiling}.py`;
  - `slayer/storage/{migrations,base}.py`, the YAML / SQLite load paths and stored-dict walkers;
  - `slayer/live_schema/schema_drift.py`, `slayer/memories/resolver.py`, `slayer/inspect/model_render.py`, `slayer/usage_reporting/features`, `slayer/osi` export;
  - the MCP / REST field descriptions and `slayer/mcp/server.py` instructions.
- **Stored data:** `SlayerQuery` v13 → v14, with `SlayerModel` and `Memory` bumped. Some stored documents become unloadable by design.
- **Callers:** Storyline's query tool, which has accepted extension columns since storyline#765, breaks on purpose. So does any script passing inline models or quoted dialect SQL as aggregation arguments.
- **Docs:**
  - `docs/concepts/queries.md`, including the "Trusted input" note, plus `docs/reference/mcp.md`, `docs/getting-started/mcp.md`, `docs/concepts/ingestion.md`;
  - examples 02, 06, 08, 13 and 14, with their notebooks re-executed;
  - the help memories.
- **Related:**
  - DEV-2102 must archive first: its `queries/extensions` capability and several requirements this change modifies live in its change folder.
  - DEV-2044 is absorbed.
  - DEV-1768 (SLayer SQL) lowers onto the Mode-B-only payload.
