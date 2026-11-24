## Why

When data violates a relationship's declared cardinality, SLayer and Ossie give different answers to an Ossie-valid
query: on Appendix B with customer C1 duplicated, `customers.region × orders.total_revenue` returns EAST 650 where Ossie
returns 350 (Layer 3 §4: declared cardinality affects only which paths exist; a home row counts once in each group it
matches). It is the one known case blocking the Ossie-superset goal of 2.0 (DEV-2372).

## What Changes

- **BREAKING** (fresh models on dirty data only): a declared or key-proven to-one hop no longer lets the engine skip
  de-duplication. Every aggregation counts each home row once in each group it matches, after the filter, whatever the
  data. Declared cardinality keeps deciding paths, determination, attribution, warnings and modes, unchanged.
- A hop is **trusted** — lowered inline exactly as today — when its relationship is marked `trusted: true`, its target
  key is enforced by the database, or its target key is unique by construction (a stage's grain, the spine instant, a
  keyless home's per-query row number, a list child's hop to its parent).
- An untrusted to-one hop is lowered through the distinct projection of its target subtree (dimensions and the
  subtree's own conjuncts computed inside), falling back to the association producer where that projection is not
  equivalent.
- An aggregate input read across an untrusted to-one hop counts its home row once with one of its matching values;
  `to_many_handling: "error"` refuses it.
- New `DatasetRelationship.trusted` and `SlayerDataset.enforced_keys`; a dialect capability `enforces_unique_keys`.
- Ingestion records reflected keys as enforced where the dialect enforces uniqueness; schema drift clears an enforced
  key the live schema no longer reports; changing a dataset's source drops its enforced keys.
- `trusted`, `enforced_keys` and an unset cardinality round-trip through Ossie documents (the latter was silently
  written as `many_to_one`).
- Stored models, saved queries and memories upgrade with `trusted: true` on every relationship, so they keep their
  results and SQL.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `models/join-cardinality`: provable to-one decides paths only; trusted hops defined.
- `queries/semantics`: declared cardinality never changes a value; untrusted-hop aggregate inputs.
- `models/fk-ingestion`: ingestion records enforced keys.
- `models/schema-drift`: drift clears a vanished enforced key.
- `models/save-validation`: enforced keys name declared keys, are refused on sql/query-backed datasets, and drop when
  the source changes.
- `models/semantic-model-documents`: trust, enforced keys and unset cardinality round-trip; imports are never trusted.
- `models/stored-upgrade`: relationships stored before this change upgrade as trusted.

## Impact

- Engine: the lowering decision points in `slayer/engine/compile/discovery.py`, `slayer/engine/compile/stages.py`,
  `slayer/sql/generator.py`, `slayer/engine/cardinality/relationship_safety.py`; a trusted-hop predicate beside
  `provably_to_one` in `slayer/core/relationship_walker.py`; a new distinct-projection lowering in `slayer/sql`.
- Models: `slayer/core/models.py`; dialects (`slayer/sql/dialects`); ingestion and drift (`slayer/live_schema`);
  storage migrations and document mapping (`slayer/storage`).
- Normative: `architecture/semantics.arc42.md` Axiom 4 and Law 6 (approved wording).
- Docs: `docs/concepts/datasets.md`, `semantic-models.md`, `ingestion.md`, `schema-drift.md`, `docs/upgrading/2.0.md`,
  `docs/osi/osi_export.md`.
- Goldens: only where SQL for an unflagged declared hop changes.
