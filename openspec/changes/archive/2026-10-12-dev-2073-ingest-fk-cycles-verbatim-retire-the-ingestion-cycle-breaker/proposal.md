## Why

One directed FK cycle anywhere in a schema (e.g. `transactions.coupon_usage_id → coupon_usages` +
`coupon_usages.transaction_id → transactions`) makes ingestion build every model with zero joins
(GitHub #477). The query side already handles cyclic edge sets, so the fix is to ingest every FK
as-is — which also requires that parallel FK edges are addressable and that every join-mutation
surface addresses one edge, not every join to a target. Dropping "back-edges" (PR #479) is rejected:
the dropped edge is chosen by table-name order and silently re-keys the schema's main relationship.

## What Changes

- Remove the ingestion cycle check and transitive-closure machinery (`RollupGraphError`, `_build_fk_graph`,
  `_check_acyclic`, `_compute_transitive_closure`, dotted joined-column introspection). Every in-schema,
  non-self FK becomes a join, whatever cycles the FK graph contains.
- A failing FK reflection on one table loses only that table's joins, never the table.
- Each live FK relationship is represented by at most one edge: mutual FKs on one key pair (exact
  inverses) ingest as one edge; a FK already represented by an edge in either orientation is not re-added.
- Ingestion names the edges of every parallel set (2+ edges between one model pair) that contains an
  FK-backed edge: FK column stem, else `<declaring_model>_<stem>`, else a numeric suffix. User-set
  names are never changed. Singleton edges stay unnamed.
- Re-ingest reconciles joins datasource-wide: new FKs to an already-joined target are added (the old
  same-target conflict error and lost merge are gone), stored unnamed members of a newly parallel set
  are named, and the outcome converges from any partially saved state.
- A table whose model name equals an existing edge name in the datasource is skipped by ingestion;
  saving such a model is rejected (the edge-name / model-name namespace is checked in both directions).
- `edit_model` addresses joins by edge: upsert matches name, then (target, pairs), then the sole join to
  the target; `remove.joins` takes edge references (target when unique on the model, else the edge
  name); a new `remove.join_edges` takes exact `{target_model, name, join_pairs}` references.
- Schema drift drops exactly the broken edge: `RemoveSpec.joins` holds edge references (unchanged for
  every non-parallel join) and a new additive `RemoveSpec.join_edges` carries exact references, which
  drift apply uses.
- Ingest reports list joins by edge reference and a new additive `named_joins`.
- `inspect` shows edge names; `models_summary` lists addressable hop tokens and marks unaddressable pairs;
  the wire-facade catalog exposes edge names and incident edges in both orientations, and the translator
  matches a reverse-direction SQL JOIN to the declared edge.
- Docs and notebooks stop describing an acyclic FK requirement.

## Capabilities

### New Capabilities
- `models/fk-ingestion`: how ingestion turns live FK constraints into joins — cycles verbatim, one edge
  per FK relationship, parallel-edge naming, datasource-wide re-ingest reconciliation, reflection-failure
  tolerance, model/edge name collisions.

### Modified Capabilities
- `models/join-traversal`: edge-name save validation becomes symmetric (a model may not be saved under an
  existing edge name); joins are addressed by edge reference on mutation surfaces; inspection surfaces
  show edge names and addressable tokens.
- `models/schema-drift`: a join drop addresses exactly one edge.

## Impact

- Code: `slayer/engine/ingestion.py`, `slayer/storage/base.py`, `slayer/engine/schema_drift.py`,
  `slayer/engine/query_engine.py` (`edit_model_remove`, `apply_drift_deletes`), `slayer/mcp/server.py`
  (`edit_model`, ingest rendering), `slayer/cli.py` (ingest/drift rendering), `slayer/inspect/`,
  `slayer/facade/catalog.py`, `slayer/facade/translator.py`.
- API: additive JSON fields `RemoveSpec.join_edges`, `ModelAddition.named_joins`; MCP `edit_model.remove`
  accepts a `join_edges` key. Canonical path spellings and result keys change only for parallel FK
  pairs, whose bare model token was already ambiguous.
- Docs: `docs/examples/05_joins/`, `docs/examples/03_auto_ingest/` (incl. both notebooks),
  `docs/concepts/ingestion.md`, `docs/concepts/terminology.md`.
