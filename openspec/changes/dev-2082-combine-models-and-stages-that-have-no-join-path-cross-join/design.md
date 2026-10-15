## Context

See proposal.md (Why). The walker (`slayer/core/join_walker.py`) already models a keyless meeting: a dataset with no to-one chain to an axis meets the time spine through a product edge (`_detached_edge`, `join_pairs: []`, many-to-many), and its aggregates attach at the empty grain by `CROSS JOIN` with a `broadcast` warning. Outside spine queries, a first hop with no edge and no route fails in binding (`slayer/engine/dimension_routing.py`). The binding bundle (`slayer/engine/bundle_builder.py`) holds only the root's connected component plus explicit join targets, and binding never reads storage afterwards (engine P3).

Governing principles: semantics Axioms 1, 2.2/2.8, 3 and the approved 3.1, 4, 7, 8, 10, 12, 14, 15; engine P3, P5, P9, P10, P11; sql P7, P12; core P5; system P13, P15.

## Goals / Non-Goals

**Goals:**
- Reduce unrelated datasets to the one existing product concept; every downstream behaviour comes from existing machinery or from one central product rule.
- Make "no row explosion" and "no quadratic evaluation" structural: guaranteed by what the checker admits, not by runtime guards.

**Non-Goals:**
- Row-level cross joins, including reading a one-row stage's field without aggregating it.
- The unit population for measures-only queries (DEV-2095) and the zero-match filter corner case (DEV-2094).
- Unrelated references inside stored definitions.

## Decisions

### D1. One product hop, discriminated
A product hop is an oriented hop with a typed discriminator (not inferred from empty `join_pairs` or from the spine target). The spine's product and detached edges and the new unrelated hop share it. Generic consumers fail closed on it: join rendering, rerooting, association-kernel building, row scopes, and semi-join correlation. Only the checker-admitted paths (D3) consume it. *Alternative:* an ordinary `OrientedJoin` with empty pairs (rejected; Codex finding 3). `is_product_edge` today keys on the spine target, so any generic path it doesn't cover would render a cartesian join.

### D2. Resolution only in strict first-hop binding
The unrelated hop is minted only when strict resolution of a query frame's first token finds no edge and no route, and the token names a dataset in the bundle's unrelated set. `neighbors`, `edges_between` and every `join_search` function never yield it. So route counting, short-form routing, population inference, `blocking_pairs` and the determination probe are unchanged by construction.

### D3. One central product rule on dependency closures
"Crosses a product hop" is a property of a reference's dependency closure (engine P10). The checker admits a product-crossing closure in exactly two places:
- beneath an aggregation whose whole input closure (source, column-valued parameters, definition defaults, column filters) lies on the unrelated dataset's side;
- in a field-filter conjunct whose closure lies wholly on one side.

Everything else raises the new `QueryTypeError` subclass, or the existing input-safety error for mixed aggregation inputs. This one rule covers dimensions, time dimensions, computed-dimension operands, order keys, raw rows, hidden filter and order slots, and derived or saved definitions reached through the hop.

### D4. Bundle collection before binding
A pre-binding syntax pass collects, per query frame, the first-hop tokens of every reference (measures, dimensions, filters, order, `partition_by`, computed dimensions) that the connected bundle cannot resolve. It resolves them once against the caller's access-scoped storage for the population's datasource, plus the query-local stage names, and records the found datasets as the bundle's unrelated set, each with its connected component. It never loads the whole datasource, and it records nothing for a hidden, unknown or other-datasource name, so those keep their existing errors (system P15).

### D5. Emission: one producer per dataset and grain, attached cardinality-neutrally
An aggregate homed across the hop compiles as the existing producer, rooted at the unrelated dataset over its own rows, at its attributable grain: empty, or the spine bucket through its own axis. It attaches by `CROSS JOIN` at the empty grain (sql P7) or on the bucket. Associate mode uses a dedicated product disposition that emits the identical producer and differs only in the warning, never the association kernel (Codex finding 4). A one-sided `D` filter conjunct becomes a predicate inside `D`'s producers plus an uncorrelated `EXISTS` on the population. A one-sided population conjunct becomes an uncorrelated `EXISTS` inside each unrelated producer. Mixed conjuncts are refused (D3), so no emitted subquery correlates across the hop.

### D6. Warning payload
`UnrelatedDatasetWarningPayload(kind="unrelated_dataset", dataset, population, measures, location)` in `slayer/core/warnings.py`. It is emitted once per unrelated dataset per stage and surfaced twice (core P5). The existing broadcast and associated payloads are untouched and fire independently.

### D7. Root recommendation
Determination probing is unchanged. With at least one determination item, an attachment whose dataset has no join path to a candidate is excluded from that candidate's reachability test. After selection, it gets the product-hop path from the chosen root and `ItemPath.unrelated = True`. `ItemPath` gains `unrelated: bool = False`, serialised on every surface.

## Risks / Trade-offs

- [A wrong-model reference now returns values instead of failing] → the always-on `unrelated_dataset` warning names the dataset, and an unknown name still errors.
- [A generic code path consumes a product hop and emits a cartesian join] → the D1 discriminator fails closed in every generic consumer, and an SQL-shape test asserts each unrelated table appears only in its own producers and uncorrelated `EXISTS`.
- [The pre-binding collection misses a reference position] → spec scenario "References in every position are found" covers filters, order and `partition_by`; a missed position fails with today's error, never silently.
- [The zero-match filter empties the population] → pinned by a spec scenario as today's association semantics; DEV-2094 may revisit.
- [Merge conflicts with the in-flight MCP surface change (DEV-2083) on the `measures` description] → resolve at merge; the description budget test guards the result.
