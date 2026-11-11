## Context

Today an extension exists only as `source_model: ModelExtension`, and each code path handles it on its own:
- the root path (`bundle_builder._resolve_root`, `inline_extensions`);
- the sibling-stage path (`plan._stage_scope_and_bundle_unframed`);
- the per-stage source path (`bundle_builder._stage_source_models`, which rejects measures over a query-backed model);
- the splice path (`plan._StagePlanner._resolve_stage_model` / `_base_time_dimension`);
- `model_reads`, `stage_ordering`, `schema_drift`, `memories/resolver`, `inspect/model_render`, `usage_reporting/features`, `engine/profiling` (which builds the legacy form itself), and the facade translator's dynamic joins.

Edge reuse is `same_edge` / `added_joins`: an exact match on target, name, type, cardinality and pairs, on the extended model's own joins only. See proposal.md for the motivation.

Principles that bind the design:
- `semantics.arc42.md`: Axioms 1, 2, 3 (association), 12 (population), 14 (filters, with the approved INNER clause), and §4 P1 (canonical spelling);
- `system.arc42.md`: P8 (one reader of Mode B), P11 (fail at save time);
- `python.engine.arc42.md`: P1 (typed pipeline), P3 (resolution is pure; storage is consulted once), P5, P11 (query-local stage names);
- `python.core.arc42.md`: P4 (one key kind per meaning), P5 (typed errors), P6 (structured warnings);
- `python.storage.arc42.md`: P3 (versioned persistence; an upgrade path is never a 2.0/3.0 removal).

## Goals / Non-Goals

**Goals:**
- One canonical representation, `SlayerQuery.extensions`, and one per-stage overlay step that every extension flows through, so the "root only" and "not in this stage" special cases cannot recur.
- An edge-merge function and a per-stage alias map, so a query-declared join never splits a stored relationship.
- A query-declared INNER join reduced to an existing construct: a field filter bound by the ordinary reference binder.

**Non-Goals:**
- Mode A inside extension columns (DEV-2021).
- SLayer SQL grammar and lowering (DEV-1768).
- Facade semantics; the facade only switches representation here (DEV-2103).
- The `deprecated_input` mechanism and the migration guide (DEV-2250).
- Population-inference changes (DEV-2212).

## Decisions

1. **A query-declared join's type is not part of edge identity; INNER is only the root-existence field filter.** Traversal follows the stored type, or LEFT for a new edge. (user)
   - *Alternatives*: type in identity, which turns the commonest SQL `JOIN` over a default-LEFT foreign key into a parallel edge and makes bare hops ambiguous; or the declared type overriding traversal per query, which lets a query redefine a stored edge and gives INNER two meanings.
2. **The INNER anchor is the ordinary reference binder.** The restriction is the field filter `M.<edge>.<target key> IS NOT NULL`, built as a parsed node (never text, system P8) and bound exactly as if the user had written it in that query object. That gives incident-hop precedence, adjacent parallel edges failing as `AmbiguousJoinPathError`, then short-form routing (unique route, else unique fan-out-free route). There is no separate routing call. If no route reaches `M`, including Axiom 3.1 unrelated datasets, it is an error naming the INNER declaration rather than a product read. (user; binder entry point folded from codex)
   - `<target key>` is the target-side column of the first key pair **in M → T orientation**. A match implies every equality operand is non-NULL, so one column decides "matched"; a null-extended target is NULL in all of them. Target model filters apply inside the joined relation (Axiom 3.3), so a filtered-out row counts as unmatched.
3. **The legacy `source_model: {source_name…}` form is converted.** It is no longer documented or advertised in the schema. Fresh input warns with `removal="3.0"`. (user)
   - One conversion function serves both the registered SlayerQuery v10 → v11 migration (after the colon-spelling upgrade at v9 → v10) and fresh input. The freshness test sits once at the `_migrate_and_rewrite` entry, as `strict`'s does: no `version`, or an extension instance passed in Python.
4. **Deprecation uses `warn_deprecated` now.** DEV-2250 sweeps it into `deprecated_input` and its guide; whichever merges second does the routing. A comment is posted on DEV-2250. (user)
5. **An extension of a query-backed model may add measures in every stage**, applied to the spliced output exactly like an extension of a sibling stage. The `_stage_source_models` rejection is deleted. (user)
6. **Axiom 14 gains, verbatim:** "A query-declared INNER join `J` is the field filter `matched(r.J)` — some row is reached along `r.J`, `r` the route from the population to `J`'s declaring dataset — and never changes how `J` is traversed. [enforced: test:tests/test_dev2102_query_inner_joins.py]" (user, approved wording)
7. **One overlay step per stage, plus one walker helper** (default, structural):
   - The overlay runs where a stage's model universe is assembled, after edge merge. It replaces the extended models (stored, inline source, synthetic sibling-stage model, spliced query-backed output) with their overlaid copies.
   - The walker helper yields `(source_name, extension)` for consumers: stage ordering, bundle loading of extension sources and targets, schema drift, memory entity extraction, inspection rendering, usage-reporting features (the existing `inline_source` flag fires on extensions too, and extension measures are counted), and profiling (which emits the new form).
   - `source_model` never carries an extension, so every root/sibling/stage branch on `ModelExtension` is deleted.
8. **The alias map** (codex). A matched declaration's alias is recorded in a per-stage map from alias token to edge identity, carried by the stage's scope / bundle. Path-segment resolution consults it after edge names and model names fail. Routing, binding, canonical-token generation, schema drift and dependency analysis go through the same resolution, and every emitted path carries the canonical token. A stored `ModelJoin` is never renamed.
9. **One extension per `source_name` per `SlayerQuery`.** (default)
10. **Scope and resolution order**: sibling stage, else inline source, else stored model; extensions are invisible to other stages and to stored definitions. (default, per engine P11)
11. **Extensions of `time_spine` are rejected.** (default)
12. **Cardinality compatibility** means equal after orientation, or unset on either side; a reused edge keeps its stored value. (default)
13. **New query-local edge names and aliases follow the stored edge-name rules**; an alias equal to the target model's name is that model's spelling. (default)
14. **One new relationship declared twice** under two different names is an error; one name, or none, gives one edge. (default, folded from codex)
15. **Save-time refusal**, through the existing save-time stage planning, of stored stages whose extensions are guaranteed to fail. (default, system P11)
16. **Every stored-dict walker in `slayer/storage/*`** that reads a query's `source_model` extension also walks `extensions`, because walkers that run at or after v11 see the new shape. (default)
17. **Codex findings folded**: alias map (8), binder entry point (2), two names for one relationship (14), the orientation of "target key" plus the INNER edge-case tests (2), profiling and usage reporting as consumers (7), the nested memory-query migration tests. None were rejected or deferred. (codex)

## Risks / Trade-offs

- [A LEFT declaration over a stored INNER edge still restricts when traversed (stored semantics win)] → Accepted (Q1); this is the stored edge's own meaning, as the facade already behaves today.
- [On a fanning chain, the restriction also narrows every aggregate read along that path to its matching rows (as the written filter does); aggregates off the path lose only the dropped population rows] → Accepted (user): the restriction is the written filter, as SQL's `INNER JOIN` without fan-out. Covered by the fanning-path scenario.
- [Around 45 test files construct the legacy form and will emit `FutureWarning`] → They are respelled mechanically to `extensions` (no logic change). The legacy form keeps dedicated deprecation tests.
- [Stored-dict walkers that run on every load may miss `extensions`] → Decision 16, plus migration tests over nested model and memory queries.
- [DEV-2250 and DEV-2102 merge order] → Each issue records the second-merger's duty (decision 4).

## Migration Plan

SlayerQuery v10 → v11 runs on load. Stored documents upgrade silently; a save writes v11. Rollback: a v11 document read by older code fails validation on the unknown `extensions` key (forward-versioned documents are best-effort, as for every version bump).
