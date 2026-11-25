## Context

See proposal.md › Why. The mechanism, reproduced on `main` @ `a652279c`:

- `axis_relationship` (`slayer/core/time_spine.py`) builds the axis link unnamed, so its canonical token (`canonical_token`, `slayer/core/relationship_walker.py`) is the target spelling `time_spine`. `_product_edge` (a spine population's product hop) is *named* `time_spine`.
- Every emitting route search walks addressable hops only (`addresses_alone`: the canonical token resolves back to exactly that edge via `matching_edges`, where edge names outrank dataset names). A declared edge named `time_spine` therefore makes both virtual hops unaddressable: `spine_routes` / `fanning_spine_routes` (`slayer/engine/cardinality/relationship_safety.py`) find nothing, and the dataset is treated as unrelated.
- Routes travel as spelled token paths (`canonical_path`, `resolve_route` in `slayer/engine/population/dimension_routing.py`), so a hop admitted by the search must also be re-followable by its spelling. Some consumers recognise the product hop by the literal token `time_spine` (`_is_product_path`).
- A `Time_Spine` relationship leaves both routes intact (the exact-case pass reaches the virtual hop first) and only makes an explicit hop ambiguous.
- Paths become SQL join aliases verbatim (`slayer/sql/generator.py` `_join_alias`), so any respelling of an unshadowed hop would change SQL.
- Precedent: `product_token` gives the population's product hop to an unrelated dataset the reserved token `__slayer_product__<target>` only when one of the source's relationships is spelled like the target.

Applicable principles: semantics.arc42 Axiom 15 (the code is made to comply; no arc42 edit); python.core 2 (the join path is data: paths stay spelled); python.core 4 (no new key kind, no reserved user name); python.core 6 (no new warning). The spec group is the cross-cutting `queries` in `architecture/index.yaml`; no new capability.

## Goals / Non-Goals

**Goals:**
- Both virtual spine hops (axis link, population product) are addressable and re-followable whatever the source's relationships are named, in every route search and in both directions.
- Unshadowed queries are byte-identical: SQL, results, warnings, errors.

**Non-Goals:**
- Reserving, refusing, renaming or warning about any relationship name (the issue's original scope; see Decisions 1).
- Changing how user-written hops resolve, including the ambiguity of an explicit `orders.time_spine…`.
- Any change to the stored-upgrade "Stored names shadow the spine" behaviour or to the query-extension refusal of an extension relationship named `time_spine`.

## Decisions

1. **No reservation; spine attribution independent of relationship names. (user)** Options: reserve `time_spine` as a relationship name and refuse it at save (the issue as filed), or keep names free and fix the spine's links. Reserving refuses valid Ossie documents and conflicts with the strict-Ossie-superset goal; Ossie relationship names appear in no expression, so the only defect is the spine link. Taken: the re-scope.
2. **Which save paths refuse is moot. (user, via 1)** The Ossie-document save path question no longer arises: nothing is refused anywhere.
3. **An explicit hop keeps the general ambiguity rule. (user)** Options: keep `_refuse_edge_target_clash` (an edge name equal to another incident edge's target dataset name is ambiguous) or let the declared relationship win. Taken: keep — one rule for every dataset, Ossie meaning unaffected, and the spine's own hop stays spellable.
4. **Tracker records updated. (user)** DEV-2435 retitled and rewritten to the new scope; DEV-2451's stored-format critical path, DEV-2452 item 7 and the post-2.0 page no longer list it.
5. **A virtual spine hop takes a reserved spelling only when a declared name shadows it. (codex)** Options: a reserved spelling always (my original default), only when shadowed, or passing edge objects instead of spelled paths. "Always" changes SQL aliases of every spine query, breaking byte-identity; edge objects are a large refactor against python.core 2. Taken: the existing `product_token` rule shape — the ordinary spelling while it addresses the hop alone, else a reserved never-declared token naming the spine; one rule already in the codebase, not a second path.
6. **The product hop gets the same treatment, recognised structurally. (codex)** `_product_edge` collides the same way, and `_is_product_path` matches the literal token; product-path predicates read the hop's identity, not its spelling.
7. **Display vs executable paths. (codex)** Human-readable surfaces spell the reserved token `time_spine`; anything meant to be executed or round-tripped (suggested paths, qualified stored text, migrations) either resolves back to the same chain or is omitted.
8. **Coverage additions. (codex)** Spine-rooted reverse routes; population-spine product; fanning axis; query-backed dataset/stage inheriting the relationship; query-extension refusal kept as regression; route diagnostics (`unaddressable_pairs` / `blocking_pairs`) no longer report the fixed virtual hop while genuinely ambiguous declared parallel edges stay reported; explicit no-leak checks over SQL, columns, warnings, errors, inspect, suggestions and qualified stored text; an unaffected spine query's SQL snapshotted.
9. **Case variants: regression only. (default)** A `Time_Spine` relationship does not detach the axis link today; scenarios pin that it keeps routing and that its explicit hop stays ambiguous.
10. **No docs change. (default)** `docs/concepts/time.md` already states each dataset's axis joins the spine.
11. **Test file named by behaviour. (default)** `tests/test_spine_axis_relationship_names.py`, per the no-issue-ids-in-code rule.

## Risks / Trade-offs

- [The reserved spelling reaches a surface that renders raw paths (aliases, key rendering, warnings)] → the no-leak test sweeps every listed surface in the shadowed case; display goes through one mapping.
- [Another consumer re-resolves a spine-bound path by the literal `time_spine`] → grep every `TIME_SPINE_DATASET` comparison against a path token during implementation; the reverse-route, product, fanning and query-backed scenarios exercise the known consumers end to end.
- [The stored-upgrade "Stored names shadow the spine" outcome shifts once routes work] → its existing tests are in the local test scope; any change stops the implementation for a ruling.
