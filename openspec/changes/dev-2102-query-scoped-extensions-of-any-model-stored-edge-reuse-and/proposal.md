## Why

A query can extend only its root (`source_model: ModelExtension`). An extension join reuses a stored edge only on an exact field match, and a query-declared INNER join has no defined meaning. SLayer SQL (DEV-1768) lowers every `JOIN` to a query extension of whichever model sits on its left side, so extensions must apply to any model, reuse stored edges in either orientation, and give INNER a population meaning that never depends on spelling.

## What Changes

- New `SlayerQuery.extensions: list[ModelExtension]`: query-scoped columns, measures and joins on any model a stage reads (stored, inline, sibling stage, stored query-backed), with one extension per model per stage.
- **BREAKING** `source_model` accepts only a name or an inline model. The legacy extension object in `source_model` is converted to `source_model: <name>` plus `extensions`:
  - stored queries: silently, by the SlayerQuery v9 → v10 migration;
  - fresh input: with a deprecation warning, removed in 3.0;
  - the extension form is no longer documented or advertised in the JSON schema.
- An extension join is matched against the edges between its two models in either orientation, on its key pairs:
  - a unique match is that edge, and the declared name becomes a query-local alias;
  - several matches, a name or cardinality conflict, or two different names for one new edge are errors;
  - no match makes a new query-local edge.

  The declared join type takes no part in identity or traversal.
- A query-declared INNER join becomes the field filter "the target is matched" along the declaring model's route from the population. That filter restricts the population by association, never multiplies rows, and is unaffected by measures. LEFT never restricts, and stored INNER edges are unchanged.
- An extension of a stored query-backed model may add measures in every stage (previously rejected outside the root).
- Extensions of `time_spine` are rejected. Stored stages whose extensions are guaranteed to fail are refused at save.
- Every consumer reads extensions through one helper: bundle building, stage ordering, schema drift, memory entity extraction, inspection rendering, profiling, usage reporting and the facade translator.
- `architecture/semantics.arc42.md` Axiom 14 gains the INNER-join clause.

## Capabilities

### New Capabilities
- `queries/extensions`: the `extensions` field, its scope and name resolution, the overlay of any model, the legacy-form conversion and deprecation, query-declared INNER joins as population filters, rejected targets, and save-time refusal.

### Modified Capabilities
- `models/join-traversal`: "An extension join repeating a stored edge is that edge" becomes orientation-aware identity on key pairs, with aliases, conflict errors and type-independent traversal.
- `queries/population`: "Population may be omitted" and "Population specs are typed at construction" now cover two forms (name, inline model), plus the legacy conversion.
- `queries/multi-stage`: stage ordering, query-local stage names and the inline query-backed rule refer to `extensions` instead of an extension `source_model`.
- `queries/query-backed-inline`: "Extensions over a spliced model" applies once, in every stage, measures included.
- `queries/time-dimensions`: "Sibling-stage stand-ins carry the bucket" is spelled with an extension of the sibling.
- `models/join-cardinality`: "Stage outputs carry their provable uniqueness" is spelled with an extension of the sibling.
- `models/stored-upgrade`: added a requirement for the stored root-extension migration.

## Impact

- Code:
  - `slayer/core/query.py` (`SlayerQuery`, `SourceSpec`), `slayer/core/join_edges.py` (edge merge replaces `same_edge`);
  - `slayer/ir/source_bundle.py`, `slayer/engine/{bundle_builder,plan,model_reads,stage_ordering,profiling}.py`, the binder's path resolution (stage alias map), population and filter assembly;
  - `slayer/storage/migrations.py` plus dict walkers, `slayer/live_schema/schema_drift.py`, `slayer/memories/resolver.py`, `slayer/inspect/model_render.py`, `slayer/usage_reporting/features`, `slayer/facade/translator.py`, the MCP / REST field descriptions.
- Stored data: SlayerQuery documents move to v10 on load; saves write v10.
- Docs:
  - `docs/concepts/queries.md`, `docs/reference/mcp.md`, `docs/getting-started/mcp.md`, `docs/concepts/ingestion.md`;
  - examples 02, 06, 08, 13 and 14, with their notebooks re-executed;
  - help memory `06_joins.md`.
- Related: DEV-2250 routes the deprecation through `deprecated_input` and the migration guide, whichever merges second. DEV-2021 later removes Mode A from extension columns. DEV-1768 lowers SQL `JOIN` onto this.
