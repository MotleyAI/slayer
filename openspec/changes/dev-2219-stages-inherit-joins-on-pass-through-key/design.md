## Context

- `model_from_stage_schema` (`slayer/ir/source_bundle.py`) is the one stage→dataset builder (DEV-1967). Today it attaches only a saved query-backed model's declared joins; an ordinary stage's dataset has none.
- A stage read as a later stage's population binds against the flat `StageSchema` scope (`slayer/engine/plan.py` `_stage_scope_and_bundle_unframed`, `resolve_scope`), unless it is a saved query-backed model or the later stage extends or relates to it (`apply_query_extensions` then switches to a model scope). Two scope kinds that must agree by hand.
- Stage ordering reads only `population`, extension `source_name` and relationship targets (`stage_sibling_reads`), while every already-planned sibling is threaded into each stage's join graph (`stage_bundle_with_siblings`). Probe on `main` `e6978e9a`: a non-root stage measuring `max(z_ev.ne)` of an unreferenced sibling fails with "no route to target" when its name sorts before `z_ev`, and works after renaming.
- Edge identity: stored datasets treat differently named edges on the same keys as parallel edges (probe: `orders` with `buyer` and `payer` on `customer_id → id` constructs as two edges); query extensions reuse a same-keys edge and record the new name as an alias (`slayer/core/join_edges.py`).
- Starting point after DEV-1607 merges: its vocabulary (`population`, `relationships`, `to`, `from_columns` / `to_columns`, `{"field"}` / `{"metric"}` entries, `filter`) and its inspect `relationships` section. DEV-2021 is merged: extensions carry metrics and relationships only, and inline models are gone.

**Principles that apply:**
- `python.engine` P5 (stages compose only through schemas): reworded, approved (decisions 5–6).
- `python.engine` P8 (models persist verbatim): inherited edges are derived, never stored.
- `python.engine` P10 (dependencies are closures): pass-through is judged on the bound key, never on text.
- `python.engine` P11 (stage names are query-local): inherited edges carry resolved target identities.
- `python.ir` P1 (representation only): `model_from_stage_schema` merges edges as a pure function; inheritance is computed in `engine`.
- `system` P8 (one reader of Mode B): the ordering extractor reads parsed references, never regex.
- `semantics` Axiom 1 (determination), the join-cardinality per-orientation rules, and `semantics` §4 P1 (canonical paths: an inherited hop is spelled by its edge name, else its target).

The specs live in the cross-cutting `queries` group (`architecture/index.yaml`); no new capability, so no attachment change.

## Goals / Non-Goals

**Goals:**
- A stage keeps every edge whose key it passes through, in both directions, wherever declared.
- One binding scope for every dataset a query reads, a stage included.
- Stage ordering and join-graph membership decided by one extractor, independent of stage names.
- Saved query-backed models expose their inherited edges in `inspect`.

**Non-Goals:**
- An identity edge back to a dataset whose own key passes through, beyond the hop-target case of rule 1 (decision 7).
- Changing the extension edge-merge rules (`slayer/core/join_edges.py`) or stored-edge identity.
- Reference anchoring and stage references by first segment (DEV-2277).
- Persisting inherited edges.

## Decisions

1. Candidate edges include both directions: which dataset an edge is declared on is incidental. (user)
2. Relationships declared by the stage's own extensions are inherited, as new edges of the stage's dataset carrying the relationship's name. (user; clarified per codex finding 2)
3. `inspect` and the summary list a saved query-backed model's inherited edges, marked as inherited. (user)
4. One binding scope: a stage read by a later stage binds as the dataset `model_from_stage_schema` builds, and the flat `StageSchema` scope and its branches are deleted. Structural fix for two scope kinds that must agree by hand. A probe switching the scope failed 12 of 1,316 stage tests: error-message assertions, one golden SQL (`tests/test_dev1958_golden_sql.py` stage/share), one time-dimension default (`tests/test_dev1471_stage_time_axis.py`). (default)
5. arc42 `python.engine` P5 (user, approved wording): "5. **Stages compose only through schemas**: every stage emits an explicit `StageSchema` (its columns, grain and inherited edges), and a later stage reads a stage only as the dataset that schema defines, bound like any other model. [review]"
6. arc42 `python.engine` §4 rationale (user, approved wording): "schema-only composition (P5) is what limits a downstream stage to the edges its upstream schema carries, never the rest of the upstream join graph."
7. Issue rule 4 is read as the hop-target proxy: passing through `T.tᵢ` for the hop that reached `T` inherits that hop's edge. Not a general identity edge to any dataset whose key passes through. (default: the issue's wording)
8. Cardinality: the target side keeps the source edge's arity in that direction; the stage side is "one" iff the stage is provably unique on the key (grain stamping); the label is composed when the source edge declares one, else none. (default)
9. A stored edge's join type is kept; an edge inherited from a query-declared relationship is LEFT (DEV-2102: a query's INNER is a population filter, not part of the edge). (default)
10. A declared edge replaces an inherited edge of the same name on other keys, silently; "declared" covers a saved query-backed model's relationships and a consuming query object's extension relationships on the stage. (default: the issue's rule 5 extended)
11. Each source edge at a position is inherited once, a pass-through of `D.dᵢ` winning over the `T.tᵢ` proxy. (default)
12. Written in DEV-1607's vocabulary against pre-DEV-2277 reference rules; implemented after DEV-1607 merges. (default)
13. Inherited edges are derived on every read, never stored. (default: engine P8)
14. One extractor decides ordering and join-graph membership: it also returns every sibling an expression names (parsed through the one Mode-B reader), and a stage's join graph holds exactly those siblings. Fixes the existing alphabetical-order bug and keeps routing from passing through an unnamed sibling. (user; codex finding 1)
15. `StageColumn` carries a typed pass-through origin — the bound field key with its resolved path (each hop's edge identity) and field — emitted only for untransformed public row-level columns; inheritance reads nothing else. (codex finding 3)
16. Identical edges: an inherited edge with the same target and key pairs as a declared edge is applied once, as the declared edge, when their names match or one is unnamed; an inherited name the declared edge lacks becomes its alias. Two differently named edges on the same keys stay parallel, as in stored datasets. (codex finding 4, revised after review: stored datasets keep same-keys edges with distinct names apart)
17. Inherited edges carry the target's resolved identity (stored dataset or a stage's minted identity), never re-resolved by name. (codex finding 5)
18. The scope unification replaces the "downstream dotted reference stays illegal" rules explicitly, with regression tests for list-element references, stale flat spellings, the root qualifier and population inference over a stage root. (codex finding 6)
19. Inspect never fails on inherited-edge derivation: when the stages cannot be planned, it lists no inherited edge and carries the typed error's message (cycle included), the same on MCP, REST and CLI. (codex finding 7)
20. Each inherited edge carries its origin (source edge identity and direction); deduplication across transitive and diamond-shaped stage graphs uses it. (codex finding 8)

## Risks / Trade-offs

- **Results change** where a dataset was previously unrelated to a stage → upgrade-guide entry; each existing test whose values change is aligned only with the owner's approval.
- **More edges, more routes**: a stage over a widely referenced dataset inherits many reverse edges → existing route tie-breaks apply; only siblings a stage names are candidates (decision 14).
- **Scope unification touches every `StageSchema`-scope branch** → the 12 probe failures plus the decision-18 regressions pin edge-less behaviour before the branches go.
- **The extractor now parses expressions** → it uses the one Mode-B reader; unparseable text is left to binding to reject, never guessed.
- **DEV-2277 re-touches references** → whichever merges second respells the other's scenarios.
- **Inspect plans the model's stages** → bounded by the existing dry-run expansion; failures degrade to a listed error.
