## Context

See proposal.md — Why. Two structural causes sit under every symptom:

1. Ingestion decides joins per table (`_build_one_model`), with no view of the pair-wise edge set, so
   cycles, exact inverses and parallel pairs cannot be judged where they arise.
2. Every join-mutation surface identifies a join by `target_model` (`_merge_joins_strict`, MCP
   `edit_model` upsert/remove, `edit_model_remove`, schema-drift `dropped_joins`), which is not an edge
   identity once two edges share a target.

The query side already terminates on cycles (all walkers have visited sets / distance maps / depth caps)
and fails closed on ambiguous hops; the dotted-column introspection per join target is discarded by
`_columns_to_model` and only costs reflection calls.

Applicable principles: system §3.9 (canonical spelling = edge name else target), §3.13 (one identity per
meaning), §3.14 (ingestion idempotent and additive-only — a name fill is a gap-fill, never an overwrite),
§3.16 (stems come from `Column.name`); engine §3.7 (a slack rule never overrides grammar, hence
literal-edge-first stays); semantics Axiom 1 (a reference never leaves its path ambiguous — fail
closed). No arc42 / `.c4` edit; the new `models/fk-ingestion` spec attaches through the cross-cutting
`models` entry in `architecture/index.yaml`.

## Goals / Non-Goals

**Goals:**
- One datasource-wide FK→edge pass (dedup, reconcile, name) shared by first ingest and re-ingest.
- Two explicit identity concepts used by every surface (below), replacing `target_model` keying.

**Non-Goals** (deferral comments go on DEV-2074, the walker-consolidation issue):
- Self-referencing FKs (still skipped; needs the revisit-by-edge rule — DEV-2074 choice B).
- Cross-schema FKs (still skipped).
- Literal-edge-first vs to-one routing, route tie-breaks (DEV-2074 choices C/D).
- A diagnostic for purely hand-authored unaddressable pairs in the facade catalog walk
  (DEV-2074 choice A).

## Decisions

**D1 — Two identity concepts.**
- *Relationship signature*: the unordered model pair plus the orientation-normalised key-pair set. Used
  only for FK reconciliation: "is this live FK already represented?" and "is this stored edge
  FK-backed?". Name-insensitive, so naming an edge never changes it.
- *Declaration reference* `JoinEdgeRef{target_model, name, join_pairs}` (a Pydantic model), resolved
  within the declaring model's join list: matches when target and pairs are equal and, if the ref has a
  name, the name is equal. More than one match is an error, never first-match. Its string face, the
  *edge reference*, is `target_model` when that is the model's only join to the target, else the edge
  name — byte-identical to today's strings for every non-parallel join.
- Alternative rejected: a declaration index (fragile across edits); a single name-including identity
  (breaks name-fill matching).

**D2 — Ingestion pipeline.** Scan builds models with unnamed FK joins (the per-schema
`_get_fk_constraint_groups` now swallows reflection failure). Then one pure planning pass over
(stored datasource models ∪ fresh models):
1. Fresh exact-inverse FK pairs keep the `_inverse_survivor` half.
2. A fresh FK whose relationship signature matches a stored edge (either side, any name) is dropped
   (existing cardinality gap-fill retained); otherwise it is added.
3. Naming: for every unordered pair with ≥2 edges of which ≥1 is FK-backed, every unnamed member gets
   a name; user-set names are fixed and reserved.
4. A fresh table whose model name equals a stored edge name is skipped (`SkippedTable`).
The plan yields per-model edits (columns, added joins, name fills); first ingest runs the same pass with
no stored models. `_merge_joins_strict`'s same-target raise disappears.

**D3 — Naming algorithm.** Candidates per edge: stem (declaring-side `Column.name`s, trailing
`_id`/`_fk` stripped case-insensitively, composite stems joined by `_`, empty stem → column name), then
`<declaring_model>_<stem>`, then `_2`, `_3`, …; invalid model-name identifiers are skipped. Collision
set: datasource model names (stored ∪ fresh) and edge names incident to either endpoint (stored ∪
assigned so far). Edges are assigned in sorted (unordered pair, declaring model, stem, pairs) order.
Column-name equality is not a collision (different arity from a hop).

**D4 — Convergence instead of atomicity.** Storage has no multi-model transaction. Names are computed
in full before any save; on a rerun, names persisted by an interrupted run are fixed/reserved and the
rest are assigned in the same order. A name assigned later can block an earlier edge only if they share
an endpoint, in which case the first run already avoided it — so a rerun from any save prefix
reproduces the uninterrupted result. Saves run name-only updates first so intermediate states raise
only transient unnamed-parallel warnings.

**D5 — Symmetric namespace check at save.** `save_model` rejects a model whose name equals an edge
name stored in its datasource (the existing check only covers a model's own edge names vs model names).

**D6 — Mutation surfaces.** MCP `edit_model` upsert matches name → (target, pairs) → sole join to
target; unmatched named spec adds; unmatched unnamed spec to an already-joined target is rejected.
`remove.joins` takes edge references; `remove.join_edges` takes `JoinEdgeRef`s (`VALID_REMOVE_KEYS`
and the `remove` input type widened). `edit_model_remove(remove_join_edges=…)`; `apply_drift_deletes`
forwards `RemoveSpec.join_edges`.

**D7 — Drift.** `_diff_sql_table_joins`, `_cascade_joins`, `_add_dropped_join` and the stage checks key
on `JoinEdgeRef`; `RemoveSpec.joins` lists edge references and `RemoveSpec.join_edges` the exact refs
(additive, so no consumer of the REST 422 / ingest JSON sees a shape change); `DeleteReason.target` is
`join:<edge reference>`.

**D8 — Reporting and inspection.** `ModelAddition.new_joins` by edge reference, additive
`ModelAddition.named_joins`, rendered by MCP and CLI. `inspect` joins table gains a `name` column;
names-only / skeleton views list edge references; `models_summary._join_targets` lists addressable
hop tokens (resolved like the facade's `_resolvable_token`) and marks unaddressable pairs
`<target> (ambiguous: name the edges)`.

**D9 — Facade.** `FacadeJoin` gains `name`; `FacadeTable.joins` is built from `neighbors()` (both
orientations, oriented pairs, hidden targets excluded); `_classify_against_parent_joins` matches emitted
ON columns against oriented pairs of incident edges to the target and the path uses the matched edge's
canonical token; no match with an incident edge still raises; no incident edge still takes the dynamic
fallback.

## Risks / Trade-offs

- [Canonical spellings change for parallel FK pairs and for hand-authored members of FK parallel sets]
  → only where the bare token was already ambiguous (or became so by the new FK); ingest report lists
  every named edge.
- [Ingestion writes a name onto a hand-authored join] → only to fill an unset name in a parallel set
  that contains a FK-backed edge; never overwrites.
- [Interrupted re-ingest leaves a partially named datasource] → D4 convergence + fault-injection test.
- [`RemoveSpec` carries two views of one drop] → `joins` for display/back-compat, `join_edges` exact;
  apply uses only `join_edges`.
- [Directed 3-cycles need explicit `source_model` for rootless queries] → pinned fail-closed; DEV-2074
  owns the rule.
