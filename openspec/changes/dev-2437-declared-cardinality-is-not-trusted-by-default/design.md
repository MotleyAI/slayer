## Context

`provably_to_one` (`slayer/core/relationship_walker.py`) — declared cardinality or a covered declared key — feeds both
typing (paths, determination, attribution, warnings, modes, partition-key legality, spine and anchor routing) and the
emission decision to join a hop inline and aggregate without de-duplicating. On data violating the declaration the
inline join counts a home row once per matching target row, breaking Axiom 4 ("each counted exactly once"). Probes on
main: the duplicated-C1 seed returns 650 with and without `customers.primary_key` (the Ossie importer stamps
`many_to_one` on every relationship, which also explains the keyless 650 in the issue's notes); forcing `many_to_many`
routes through the association producer and returns 350, with an EXISTS semi-join for the filter.

Benchmarks (best of 3; DuckDB 10M orders / 1M customers / 50 regions; SQLite 1M orders, indexed keys):

| lowering | DuckDB | SQLite |
|---|---|---|
| inline join + SUM | 52 ms | 372 ms |
| association producer (group by dims + home key, re-aggregate) | 155 ms | 625 ms |
| join to `SELECT DISTINCT to_key, dims` of the target subtree | 64 ms | 334 ms |
| filter inline WHERE | 35 ms | 150 ms |
| filter EXISTS | 47 ms | 404 ms |
| filter as inner join on DISTINCT keys | 7 ms | 38 ms |

The emission points reading the to-one result (inventory): `discovery.py` `_host_inline` / `local_broadcasts`
routing; `stages.py` `_synthesize_cross_dataset_producer` dimension split (`attributable_at_home`, ~2366/2409, the
`associate` arm ~2424), `_conjunct_disposition` (~1838/1844) with `fanning_paths` (~2006, kept consistent per the assert
~1983), the inline host-locus (~2490–2495); `generator.py` `_filtered_dataset` `proven` (~5871); the routing shortcuts
in `relationship_safety.py` (~376, ~483). `may_inline_crossing_inputs` stays `False`. Stage datasets get their grain
uniqueness stamped at runtime in `slayer/ir/source_bundle.py` (~327). Partial and expression unique indexes are already
excluded at reflection (`slayer/sql/dialects/reflection.py` ~678–685). Document storage never shipped in a release
(absent in v1.9.1); released stores hold standalone `SlayerModel` YAML (current v28), `SlayerQuery` v18, `Memory` v13.

Applicable principles: semantics Axiom 4 (the rule), Axioms 7–8 (typing unchanged), Laws 5, 6, 8, Principle 4 (Ossie
superset); sql 12 (attach cardinality-neutral), 13 (one relation door — the distinct projection is emitted through the
dataset relation door, so model `filters` and RLS sit inside it); system 4 (sqlglot AST), 5, 6, 10 (one typing), 11
(fail at save time); storage 3 (versioned persistence).

## Goals / Non-Goals

**Goals:**
- Each home row counts once in each group it matches after the filter, unless every hop is trusted.
- Typing unchanged; clean data gives identical values trusted or not.
- Trust from three sources only: the relationship flag, an enforced database key, a key unique by construction.
- Upgraded stores keep their results and SQL.

**Non-Goals:**
- Live query-time constraint checks.
- Empirical scan certificates (DEV-1884).
- Moving the general association lowering over fanning hops onto the distinct projection.
- Changing EXISTS for filters on undetermined paths.

## Decisions

1. **Typing stays on declarations; lowering reads one trusted-hop predicate** beside `provably_to_one`, consumed only at
   the emission points listed above. Alternative — making determination itself require trust — would warn and refuse
   on every cross-dataset dimension. (default, structural)
2. **Untrusted lowering is the distinct projection (B), the association producer (A) its fallback.** B: a subtree beyond
   its first untrusted hop joins the home as `SELECT DISTINCT <hop source-side keys>, <each dimension value computed
   inside>`; the subtree's own conjuncts go inside, the join is INNER when it carries a conjunct and LEFT otherwise; a
   filter-only subtree joins as an inner join on its DISTINCT keys. Computed and time-truncated dimensions de-duplicate on
   their grouped value. A is used when a dimension reads an untrusted subtree together with anything outside it; a
   conjunct doing so is pushed as a correlated EXISTS semi-join. An aggregate input read across
   the hop is projected inside the distinct projection as one matching value per projected row
   (`ANY_VALUE`, grouped by the keys and dimension values) (decision 18). Alternative A-only costs ~3× on
   DuckDB. (user)
3. **Declared keys do not confer trust**: Ossie's retail model declares `customers.primary_key` and must return 350
   (L2 §11.3). (default, forced by acceptance)
4. **Keys unique by construction are trusted**: a stage's complete grain, the spine instant, a keyless home's per-query
   row number, a list child's hop to its own parent row — full coverage required; a list position alone is not one.
   (default; list-child precision (codex))
5. **Enforced database keys confer trust**, recorded at ingest, kept true by drift. Alternatives: live checks; deferring
   to DEV-1884. (user)
6. **Ingest trust lives on the key** (`SlayerDataset.enforced_keys`), not the relationship, so drift edits only facts it
   reads from the database and never an author's `trusted`. Alternatives: provenance on the relationship; warn-only
   drift. (user)
7. **Dialect capability `enforces_unique_keys`**, stated by every dialect; false for engines with informational
   constraints. (default)
8. **Drift gains a key-constraint rule** clearing vanished enforced keys. (user: "It bloody well should")
9. **`trusted` covers every provably to-one orientation of its edge**, so the migration preserves key-proven hops
   exactly. (default — the only option without a regression)
10. **A vacuous `trusted` is inert, not refused**: a peer re-key can make any flag vacuous after the fact. (default)
11. **Stage-inherited edges inherit trust.** (default)
12. **Untrusted declared hops raise no association warning and no error-mode refusal**: typing is unchanged. (default)
13. **Importers never set trust.** (default)
14. **Migration**: `SlayerModel` 28→29, `SlayerQuery` 18→19 and `Memory` 13→14 (via `stamp_memory_query`), all
    stored-only, set `trusted: true` on every stored relationship, backing queries included; documents are not migrated.
    (default; Memory bump (codex))
15. **`trusted`, `enforced_keys` and an unset cardinality travel in the SLAYER entry**; the document writer's `default`
    predicate no longer treats unset cardinality as default. (default)
16. **The unset-cardinality round-trip is fixed in scope.** (user)
17. **Ships in 2.0** (DEV-2372, Principle 4). (default)
18. **An aggregate input across an untrusted hop** counts its home row once with one of its matching values; under
    `to_many_handling: "error"` it is refused naming the hop and the remedies. Alternatives: refuse in every mode
    (regresses fresh warehouse models); keep the inline join (breaks Axiom 4). (user)
19. **Enforced keys are refused unless they name a declared key of a table-backed dataset, and drop when the dataset's
    source changes.** (codex)
20. **Partial / expression unique indexes are never enforced keys** — already excluded at reflection; a test pins it.
    rejected (codex) as a defect: already handled.
21. **Tests cover the B/A boundary**: same-branch filter + dimension, mixed home/target predicates under `OR`/`NOT`, two
    target branches, model `filters`, RLS — values and join shape. (codex)
22. **Every aggregation family is tested on dirty data.** (codex)
23. **The keyless 650** is the Ossie importer's `many_to_one` stamp, not a separate bug. (default)
24. **Approved arc42 wording** (user, approved wording):
    - Axiom 4 gains: "This holds even on data violating declared cardinality (Ossie L3 §4); only a **trusted** hop — its
      relationship marked trusted, or its target key unique by construction or enforced by the database — may be
      lowered assuming it, and on data violating a trusted relationship the result is undefined." plus the tag
      `[enforced: test:tests/test_dev2437_trusted_cardinality.py]`.
    - Law 6's list becomes: "inlining an association-restricting filter or an aggregate on a trusted to-one path
      (Axiom 4), evaluating association over distinct target tuples, and fusing pipeline phases into one SELECT".

## Risks / Trade-offs

- [Two lowerings of one meaning must agree] → Law 6 instances pin B ≡ A on dirty data wherever B applies.
- [Fresh models pay B's overhead] → ~1.2× on DuckDB, ≤1× on SQLite; enforced keys and `trusted` restore the inline path.
- [A constraint dropped between drift runs] → results on newly dirty data may inflate until the next drift apply; the
  same window exists for dropped columns.
- [A misclassified dialect] → the capability table is pinned by a test, one row per dialect.
- [Golden churn] → SQL changes only for unflagged declared hops in fixtures without migrated or enforced keys; any
  existing test pinning that shape is aligned only with the owner's approval.
