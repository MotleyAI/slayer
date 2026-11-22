## Context

Every transform today is a window over the final result rows: `_run_transform_chain` (`slayer/sql/generator.py`) emits `stepN` CTEs whose `OVER (…)` reads the chain tail — the grouped `_base`, or the combined SELECT that DEV-2212's `_keys` row supply feeds. Calendar-step transforms either re-aggregate frame-free in a shifted producer (non-series regime, unaffected by rows) or re-read the chain tail (series regime, `consecutive_periods`). Two "own relation, then join back" paths already exist outside measure position: the re-aggregation carrier (`_synthesize_reaggregation_producer`) and computed-dimension transforms (`_synthesize_local_regroup` over `effective_root_grain`), both reading host rows grouped at the union grain rather than the operand's cells.

Spine queries are carved out of the row supply at `query_engine._infer_population_of` (`is_spine_query` → `_spine_population_of`): P is the dimension anchor, `host_mask` strips spine bounds from P, and rows are P's distinct tuples CROSS the bucket series. A dataset with no to-one chain to an axis is "detached" (`relationship_walker._detached_names`) and meets the spine through a keyless product edge, so association counts every entity in every bucket.

### Symptom, structural cause, fix

- Symptom: sibling measures shift `lag` / rank-family / `first` / `last` values in root-less queries (Laws 3, 7); `rank(sum(x, partition_by=region))` by `[region, city]` ranks city rows; spine queries keep fact-less grid rows; an axis-less home is associated with every bucket.
- Cause: a transform's evaluation domain is the consumer's rows, not its operand (Axioms 6, 9, 11 require the operand alone); the spine path keeps its own population rule beside the row supply.
- Fix: one domain producer for every transform in every position, built from the operand's constituents' cells (the computed-dimension path, re-sourced); spine queries join the row supply with spine dimensions factored out; spine bounds become field filters; a fanning-axis route replaces the detached product edge for spine attribution only.

### Principles that apply

semantics Axioms 2.3, 3.3, 4, 6, 8, 9, 10, 11 (11.3 and new 11.6 edited), 12, 14, 15 (edited); Laws 3, 4, 7 (tags edited), 8, 9. system §3 P4 (AST-built SQL), P7 (RLS fails closed), P10 (one typing). engine P4 (one interned producer per value and phase), P6 (phase and stage are properties of the value), P9 (user-facing type errors in the checker), P10 (dependency closures). No `model.c4` / `views.c4` / `index.yaml` change: work stays in `engine.compile`, `engine.population`, `engine.cardinality`, `engine.elaborate_env`, `ir`, `sql`, `core` (`relationship_walker`, `time_spine`) along existing arrows. All touched specs are in the `queries` cross-cutting group; no new capability.

## Goals / Non-Goals

**Goals:**

1. **Domain** (Axiom 11.6): every transform, every position, every mode, evaluates over its operand's domain — constituents' cells at the operand grain, built recursively: an aggregate constituent's cells are the tuples its filtered home rows reach (Axiom 3.3: model filters and RLS first); a nested transform constituent is its own result cells (Axiom 11.5); constituents full at the operand grain supply cells, coarser ones extend to the agreeing cells of finer constituents, else of `A` (12.3 read at the operand grain); an aggregate-free operand takes `A`'s cells. A query cell outside the domain reads NULL (Axiom 10). Result attaches on the result grain by the null-safe grain join (Axiom 6).
2. **Spine domain**: the operand's non-time tuples with a cell in a visible bucket × every bucket in range.
3. **Calendar-step**: the non-series shifted producer is unchanged; the series regime and `consecutive_periods` read the transform's domain relation instead of the chain tail.
4. **Carve-out removal**: `PositionClasses.transform_dim_keys` / `dim_keys_for` deleted; `check_metrics_at_query_grain`, discovery `_Walker.consumer` / `check_combined_partition_keys` and `stages._rewrite_regrouped_prebound` read `dim_keys`.
5. **Spine rows**: a spine query omitting `population`, or pinning it with `population_inferred: true`, goes through `infer_rows` / `plan_rows` with the spine dimensions factored out; P = union of each measure's non-time tuples with a cell in a visible bucket (spine-specific supply classification over each measure's operand cells with the spine axis removed — windowed, ranked, shifted, `first`/`last` and second-order forms included); `A`'s bounded tuples with no measures; the unit with no other dimensions. Rows render as the spine series CROSS `_keys`.
6. **Two kinds of bounds**: supply bounds (the visible frame: tuple discovery and visible buckets) vs evaluation bounds (today's widened or shifted ranges for windows and `time_shift`). Spine bounds restrict homes and `A` as field filters through the axis — to-one inline, fanning by association; detached and unrelated datasets unrestricted.
7. **Fanning axis**: in the spine-route API only (`relationship_safety.spine_routes` / `home_spine_route` and the compile path) — `_detached_names` keeps its meaning for graph construction. Nearest route with ≥1 fanning or unproven hop ending at an axis dataset, never through the spine; to-one routes win; ties raise the extended `spine_route_error`. Association reroots the spine bucket over that route (association arm, one identity per home shared by the supply, domain and aggregation producers, dedup on `(identity, bucket, non-time tuple)`).
8. **Reporting**: `population` `time_spine × P` (named) / `time_spine × A`; `row_sources` = supplying homes ∪ each anchor component used for extension, canonical, sorted, `["unit"]` only when the unit supplies.

**Non-Goals:**

- Route-qualified spine references (`customers.orders.time_spine.timestamp`); a tie is remedy-only (decision 2).
- Fanning time partition keys (DEV-1941).
- A data-derived default lower spine bound.
- Any change to the non-series `time_shift` re-aggregation regime.

## Decisions

1. Transform domain = the operand's own cells in every mode. Options: (A) every mode; (B) population cells in anchored queries. A: one path, Axioms 6 and 9, Law 4 with no mode fork; the spine is the dense-series tool; anchored calendar and empty-group consequences go in the migration guide. (user)
2. Tied fanning axes are remedy-only. Options: (i) the error names fact time-column routes; (ii) route-qualified spine references. (i): same family as the equal-length to-one error, no new grammar; (ii) would be additive (2.x). (user)
3. One domain producer for every position: the computed-dimension path's host-rows domain becomes the operand-cells domain (structural fix, Law 4). (default)
4. An aggregate-free operand's domain is `A`'s cells. (default)
5. Calendar-step series regime and `consecutive_periods` read the domain relation. (default)
6. Spine bounds are field filters restricting homes and a dimension-only `A`, by association through a fanning axis (issue "act like any filter" + Axiom 14). Consequence: on the spine scenario datasource, `customers` has two equal-distance fanning axes, so a customer-homed measure or a dimension-only `customers.region` spine query raises the tie error in the default mode. (default)
7. Detached / unrelated datasets are not restricted by bounds and stay associated with every bucket. (default)
8. Other dimensions with no in-frame tuple → zero rows; no other dimensions → every bucket (12.4 unit). (default)
9. To-one axis route beats any fanning route; under `broadcast` / `error` a fanning-axis home follows Axiom 8 and the tie is not raised under `broadcast`; the `associated` warning. (default)
10. Spine reporting per the DEV-2212 contract (`time_spine × A`, supplying homes). (default)
11. Saved / pinned spine stages follow DEV-2212's `population_inferred` rule. (default)
12. Measure-position carve-out removed as specified in the DEV-1963 carry-over comment on the issue. (user, issue comment)
13. Lower spine bound stays required (issue scope 5). (user, issue)
14. DEV-1941 out of scope. (default)
15. Transforms added to the root-less law pool. (default)
16. semantics.arc42.md edits (user, approved wording):
    - Axiom 11.3: "never a neighbour; `lag` / `lead` alone step over the domain's cells (11.6)."
    - Axiom 11.6: "- **11.6 Domain.** A transform evaluates over its operand's **domain**: its inner aggregates' cells at the operand grain (Axiom 6), `A`'s cells when it has none, never the query's rows; over the spine, the domain's non-time tuples × every in-frame bucket. A cell outside the domain takes NULL (Axiom 10)." plus `[enforced: test:tests/test_dev2218_transform_domain.py]` after Axiom 11's existing tags.
    - Axiom 15: "sole one — joins it to-one; an expression is never an axis, so a spine dimension over one is a type error; a dataset reaches the spine by its nearest axis on a to-one chain, else by association (Axiom 8) across its nearest **fanning axis** (one reached across a fanning hop), equal distances failing loudly, and a path may begin or end at the spine but never cross it. The spine has no countable rows. A query with spine dimensions has population `spine × P`, P named, else inferred per Axiom 12 from its other dimensions; its spine bounds are field filters (Axiom 14), and it must bound the spine from below." plus `[enforced: test:tests/test_dev2218_spine_rows.py]` and `[enforced: test:tests/test_dev2218_fanning_axis.py]`.
    - Law 3: "an inferred query may add cells (Law 7); transforms [enforced: test:tests/test_dev2218_transform_domain.py]; sibling-measure independence"
    - Law 4: "to the value. [review] [enforced: test:tests/test_dev2218_transform_domain.py]"
    - Law 7: "cell); transforms [enforced: test:tests/test_dev2218_transform_domain.py]."
17. Mixed-grain domains follow 12.3 at the operand grain: full constituents supply, coarser constituents extend to agreeing finer-constituent cells, else `A`'s — never a UNION of branches at different grains. (codex)
18. Domains are built recursively from immediate constituents (aggregates, or nested transforms at their result grain); nested transform producers interned by key. (codex)
19. Supply bounds (visible frame: tuple discovery, visible buckets) are separate from evaluation bounds (widened or shifted ranges for windows and `time_shift`). (codex)
20. In a spine query a measure supplies the non-time tuples of its cells in a visible bucket; classification reads each measure's operand cells with the spine axis removed. For a plain aggregate this equals the issue's "rows inside the frame"; for a windowed or shifted measure it keeps a group whose value lands in a visible bucket (S's January order in February's two-month window), which the literal reading would drop — the only non-regressing option. (codex)
21. Fanning-axis discovery lives in the spine-route API and compile path; `_detached_names` keeps its meaning; non-spine SQL and results unchanged. (codex)
22. Unpinned and `population_inferred: true` spine queries both go through `infer_rows`, the pinned value used only as `A`; grids stay for an explicit or legacy-pinned population. (codex)
23. Keyless fanning-axis homes: one identity per home shared by every producer; dedup on `(identity, bucket, non-time tuple)`. (codex)
24. Position twins: the same nested / mixed-grain transform projected, filter-only and order-only read identical values; hidden transforms supply no rows; filters never change surviving values. (codex)
25. Model filters and RLS apply before route association and tuple extraction (Axiom 3.3). (codex)
26. `row_sources` = supplying homes ∪ each concrete anchor component used for extension, canonical, sorted, `["unit"]` only when the unit supplies. (codex)
27. A time-ordered transform whose operand grain lacks the query's active time bucket fails with the typed time-axis error in every position (Axiom 11.3), as it already does in dimension and parameter positions. (user)
28. A NULL time bucket is adjacent to no bucket in every time-ordered transform and on every dialect (Axiom 11.3, as `consecutive_periods` already specifies); collapsing picks skip it. (user, issue comment)

## Risks / Trade-offs

- Breaking for anchored queries over populations with cells the operand lacks (calendar models, empty groups): migration-guide section with the spine as the dense-series remedy.
- Breaking for spine queries: fact-less groups disappear; customer-homed spine measures on datasets with several dated children raise the tie error (decision 6). Migration guide states the remedies (name `population`, query a fact time column, or a stage).
- Extra CTEs per transform root (domain union + producer joins) — mitigated by interning; goldens re-blessed only where shapes change.
- Window endpoints and shifted producers read evaluation bounds while visible rows read supply bounds; the first-visible-bucket tests guard the split.
- Spec scenario values for fixtures not executed at plan time (e.g. the `spend_band` rank direction, the cross-model `cumsum(sum(customers.spend, partition_by=region))` scenario whose operand grain lacks the month axis) are verified at `pr-tests`; a contradiction is raised, never silently re-specified.
