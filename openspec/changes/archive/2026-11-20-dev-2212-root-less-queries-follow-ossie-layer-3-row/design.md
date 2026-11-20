## Context

Root-less queries today run exactly as if the user had named one inferred dataset (`slayer/engine/query_engine.py` `_infer_populations` fills `population` before planning). Rows are that dataset's filtered rows grouped by the dimensions; local aggregates compute in the base `GROUP BY`; cross-model aggregates are producers (virtual models keyed by their grain, Axiom 6) LEFT-joined on the null-safe grain (`slayer/sql/render/joins.py` `build_grain_joinback_condition`), either inside the base `FROM` (row phase) or onto a `_base` CTE (combined phase). No key-union relation exists. A one-row base already exists (`EmptyBaseGrainPlan`). DEV-2277 (merged in, unarchived) makes keys independent of the population and fixes the anchor-routing rule; DEV-2216 makes association the default and pins associated producers' cell sets; DEV-2210 makes model filters define every read.

### Symptom, structural cause, fix

- Symptom: a root-less query takes its rows from ONE inferred dataset chosen from dimensions **and field filters** (D1–D4, N1–N3 of the issue).
- Cause: Axiom 12 conflates the quantifier with the dimension anchor, and the planner treats an inferred query as if the user had named that dataset. Every consequence (orphan rows lost, filters re-quantifying, measures-only refused) follows from that one choice.
- Fix (planner level, one code path): an inferred query's population is the **row supply** — the union of the cells (Axiom 6: an aggregate is a virtual model keyed by its grain) of its top-level measures' full-grain aggregates (semantics 12.3/12.4 as approved). Every inferred row source is a producer that already exists today; the only new relation is the key set they are unioned into. Values attach exactly as producers attach today (null-safe grain join, Axiom 6), so value semantics never fork between modes.

### Principles that apply

semantics Axioms 1, 2 (2.9 edited), 3.1 (edited), 3.3, 4, 6, 7–8, 10, 12 (rewritten), 14 (edited), 15 (edited); Laws 3 (edited), 6, 7 (split), new 8–10; §4 P1 (tag flipped to enforced), P4 (Ossie superset). system §3 P4 (AST-built SQL), P7 (RLS fails closed), P10 (one typing), P11. engine P1, P3, P4, P5, P8 (edited), P9 (every user-facing type error in the checker), P10. No model.c4 / views.c4 / index.yaml change: all work stays in `engine.population`, `engine.compile`, `engine.elaborate*`, `ir`, `sql`, `storage`, surfaces, along existing arrows. Specs touched are all in the `queries` cross-cutting group.

## Goals / Non-Goals

**Goals:**

1. **Dimension anchor A** (12.2): smallest dataset determining every dimension (dimensions only — never filters or measures), unproven hops counting by association except in raw-row mode; groups of dimensions with no join path between them take the product of their anchors (reported `a × b`); no dimensions → the unit. Tie / connected-but-undetermined → A undefined. A is computed lazily: a query fails closed (`PopulationInferenceError`, today's reasons) only when it **needs** A — no measures, or a measure with no full-grain operand. (Ossie §4/§5.6: metric queries route from each home; only a dimension-only query needs the anchor.) A's cells = the rows of the anchored query at A with the query's dimensions and filters.
2. **Supply** (12.3): full-grain aggregate (typed by the query's dimensions, its home determining or associating each) supplies its cells — the tuples its filtered home rows reach along the home's own LEFT-joined paths (orphan → NULL tuple; associated producers already pin this). Any other aggregate (`partition_by=`, unrelated → its cell is the unit, broadcast-fallback) has cells on a coarser grain, extended to A's cells agreeing with them there. A measure supplies the union of its full-grain operands' cells (through combinations and transforms); with none, the union of its operands' extended cells. "Top-level measures" = the query's `metrics` only; aggregates appearing only in filters, order entries or computed dimensions supply nothing.
3. **Rows** (12.4): NULL-safe union of every measure's supply; A's cells with no measures; exactly one row with no dimensions (even with `filter: 1 = 0`).
4. **Values**: every term attaches on its own grain (full-grain exact, coarser broadcast), cell lacking → the term's empty value (Axioms 4, 10).
5. **Filters**: restrict every home's rows (and A's) before tuples are taken — directly where determined, else by association (existing disposition: inline / EXISTS semi-join / product-hop one-sided association); never choose A or a supply. Routed from the dataset they restrict (P1).
6. **Routing from the home** (P1, both modes): each aggregate's grain dimensions and the filters restricting its home are routed from the home by the DEV-2277 rule (only to-one path, else only route, else ambiguous; no route → unrelated). Acceptance: `orders → customers → regions`, `regions → managers`, `orders → managers`; `{"dimensions": ["regions.name"], "metrics": ["orders.total_revenue"]}` answers via `orders → customers → regions` (today: refused as ambiguous from `regions`). Applies in anchored mode too (Axiom 6: a virtual model's rows never depend on the root).
7. **Rows removed by row-level security or model filters do not exist** (Axiom 3.3, unchanged): a home row pointing at one is an orphan and lands in the NULL tuple. Issue rule 5 is superseded.
8. **Spine queries**: `spine × P`, P = A of the other dimensions (filters no longer choose P; they restrict by association). The row-supply union for spine queries is DEV-2218 (Axiom 15 tagged).
9. **Transforms** evaluate over the result rows as today; monotonicity for transforms is DEV-2218 (Laws 3/7 tagged). A transform's supply is its operand's (calendar-step transforms included).
10. **Raw-row mode** (12.5): one row per row of `population`, else of A (dimensions only); every dimension provably determined (no association, no product fallback) else fail closed; measures stay banned.
11. **Multi-stage**: each stage omitting `population` is inferred by the same rule; sibling-stage-anchored **dimensions, filters and metrics** keep the SIBLING_STAGE refusal (A inference reads stored datasets only).
12. **Datasource scoping**: anchors now include metric anchors (so measures-only queries scope); saved-measure filter refs stay excluded.
13. **Anchored mode** (`population` named): unchanged — today's Axiom 12 / Law 7 closure — except P1 home routing (item 6).

Reporting:

- `population` = A (`a × b` for a product, `time_spine × A` for spine), `null` when A is the unit or undefined-but-unneeded; `population_inferred` as today.
- New `row_sources: list[str]` on `SlayerResponse`, REST `QueryResponse`, Python client: the homes of supplying full-grain aggregates plus A's dataset(s) when A's cells are supplied; `["unit"]` with no dimensions; for an anchored query `[population]`. MCP prints population and row sources only when inferred (JSON keys, markdown footer, CSV comment).
- `unrelated_dataset` in a dimensionless query: when the measure homes fall into ≥2 join-disconnected components, one entry per measure home, `population: null` (N2 flags `orders` and `tickets`; N1 nothing). With dimensions: unchanged (`associated` + `unrelated_dataset`).
- `EMPTY_DETERMINATION` is no longer raised for measures-only queries.

Saved queries:

- New public `SlayerQuery.population_inferred: bool = False`; with `true`, `population` is the pinned anchor A and row semantics stay inferred; `population_inferred` without `population` is refused (typed validation error). Terse Field description ("set by saving; keeps the rows inferred while `population` pins the anchor").
- Pinning on save writes A plus `population_inferred: true` (P8 as approved); legacy pinned stages (no marker) stay anchored; refinement of a legacy unpinned stage pins A + marker in memory as today.
- SlayerQuery stored version 17 → 18 (stamp only; default `false`); whichever slice merges second renumbers.
- A pinned A that does not determine a dimension a refinement adds: A's cells are those of the anchored query at A (item 1), so nothing new is refused.

**Non-Goals:**

- Row supply for spine queries and transforms evaluated over their operand's own cells (DEV-2218).
- Strict Ossie Layer 3 conformance mode and Ossie error codes (DEV-2217).
- Public output names for unaliased entries (DEV-2279).
- Relaxing the raw-row measure ban.
- Issue rule 5 ("hidden rows count as filtered out"): superseded by Axiom 3.3 (decision 1).

## Decisions

### SQL emission

- Single row source → that source is the base (no keys relation; today's shape when it is A).
- Several → `_keys` = `UNION` (distinct; NULL-safe by SQL set semantics) of each source's grain tuples; FROM `_keys` LEFT JOIN each producer on the null-safe grain (`build_grain_joinback_condition`); coarser producers on their grain subset. Never chained FULL JOINs.
- Missing cell → existing `COALESCE(value, empty value)` attachment. No separate presence marker: by Axiom 4 a present cell's NULL already equals the empty value, so the two are indistinguishable by definition (deviation from the issue's sketch, logged).
- Unit: today's `EmptyBaseGrainPlan` one-row base, every measure a grand-total producer cross-joined.
- Extended cells: A's cells semi-joined to the coarse producer's cells on its grain.
- Built as sqlglot AST; dialect-portable (`UNION DISTINCT` where the dialect requires it).

### Decisions log

1. Hidden rows: rows removed by RLS or model filters are logically nonexistent (Axiom 3.3); a home row pointing at one lands in the NULL tuple; rule 5 superseded. Options: keep DEV-2210 / rule 5 for model filters only. (user)
2. Marker: public `SlayerQuery.population_inferred`, valid with `population`, refused without. Options: public / stored-only hidden. Public: one shape, no silent anchored flip on hand edits. (user)
3. Dimensionless `unrelated_dataset`: symmetric — every measure home when homes span ≥2 disconnected components, `population: null`. Options: symmetric / never warn. (user)
4. Coarser aggregates (`partition_by=`, unrelated, broadcast-fallback) supply their own cells extended by agreeing A cells; full-grain-first kept. Options: issue's "A's tuples" / extension. (user)
5. semantics.arc42.md edits (user, approved wording):
   - Axiom 2.9: "- **2.9 Home is not population.** The home is per aggregation node (where it is counted); the population (Axiom 12) is per query (which cells exist) and never moves a home. A query dimension the home does not determine is broadcast, associated or refused (Axioms 7–8)."
   - Axiom 3.1 second sentence: "Only aggregates, filters (Axiom 14) and a dimension anchor over unconnected dimension groups (12.2) read across `×`; any other row-level read is a type error, so `×` never multiplies a population."
   - Axiom 12: "12. **Population**: the population is the query's quantifier — exactly one result row per combination of dimension values among its row-filtered rows (raw rows are 12.5; spec: `queries/semantics` › Grain guarantee) [enforced: test:tests/test_distinct_dimension_values.py] [enforced: test:tests/test_dev2063_grain_guarantee.py]; it is reported back and resolved by: - **12.1 Anchored.** A named population may be any dataset; measures never change its rows (Law 7). [enforced: test:tests/test_law_population_invariance.py] - **12.2 Dimension anchor.** `A` is the smallest dataset determining every dimension, read from dimensions only, an unproven hop counting by association (Axiom 8); dimension groups no join path connects take the product of their anchors (Axiom 3.1); with no dimensions `A` is the **unit**, one row. Tied minimal candidates, or connected groups no dataset determines, leave `A` undefined. - **12.3 Supply.** An aggregate typed by the query's dimensions whose home determines or associates each (Axioms 7–8) is **full-grain**: its cells (Axiom 6) are query cells. Any other aggregate's cells lie on a coarser grain and extend to the cells of `A` agreeing with them there. A measure supplies the union of its full-grain operands' cells, through combinations and transforms; with none, the union of its operands' extended cells. - **12.4 Inferred.** An omitted population is the **row supply**: the union of every top-level measure's supply — `A`'s cells with no measures, the unit with no dimensions. Field filters restrict each supply before it is taken (Axiom 14) and never choose one; a query needing an undefined `A` fails closed. [enforced: test:tests/test_dev2212_row_supply.py] - **12.5 Raw rows.** Raw-row mode lists one row per row of the population, else of `A` (never 12.4); every dimension must be provably determined — no association, no product. [enforced: test:tests/test_dev2212_raw_rows.py]"
   - Axiom 14 first sentence + stratification: "14. **Filters**: a field-typed filter masks the rows of the population and of every home before any aggregation, and never chooses a population (12.4); a measure-typed filter masks result cells after all values are computed, never changing a surviving cell's values; valid-as-both types as field, valid-as-neither is a type error; evaluation is stratified — aggregate-free field filters fix the rows every aggregation runs over"; INNER-join sentence: "`r` the route from each dataset the filter restricts to `J`'s declaring dataset".
   - Axiom 15: "P named, else inferred per Axiom 12 from its other dimensions [target: DEV-2218]".
   - Law 3: "3. **Compositionality**: a term's value in a cell depends only on the term, the cell, the row-level filters, and the mode — never on sibling terms, which in an inferred query may add cells (Law 7); transforms over result rows [target: DEV-2218]; sibling-measure independence […unchanged tail…]"
   - Law 7: "7. **Measure closure**: before measure-typed filters, order and limit, adding a measure to an anchored query never changes its rows or any other field's values; to an inferred one with a measure (**measure monotonicity**) it never removes a row or changes a value in an existing row, a new row carrying each other measure's value on its own grain there (its empty value where it has no cell); transforms [target: DEV-2218]. [enforced: test:tests/test_dev1837_dimension_measure_matrix.py] [enforced: test:tests/test_law_measure_monotonicity.py]"
   - Law 8: "8. **Home-row preservation**: in an inferred query every home row of a full-grain aggregate surviving the filters lands in a row — exactly one when the home determines the dimensions and the data respects declared cardinality (Ossie S1). [enforced: test:tests/test_law_home_row_preservation.py]"
   - Law 9: "9. **Filter non-expansion**: adding a field filter never adds a row or changes a result key (Ossie S6). [enforced: test:tests/test_law_filter_non_expansion.py]"
   - Law 10: "10. **Unit**: a query with measures and no dimensions returns exactly one row (Ossie §3.6). [enforced: test:tests/test_law_unit.py]"
   - §4 P1: remove ` [target: DEV-2212]`; add `[enforced: test:tests/test_dev2212_home_routing.py]`.
6. python.engine.arc42.md P8 (user, approved wording): "exception: a saved query stores each inferred stage's dimension anchor as its `population`, marked `population_inferred`. [review]"
7. A computed lazily; failure fatal only when A is needed. Grounded in Ossie §4/§5.6 and P4 (Ossie superset). (default)
8. P1 home routing applies in both modes (Axiom 6 + P1). (default)
9. Spine queries keep `spine × A` (filters no longer choose P); union rows for spine = DEV-2218. (default)
10. Transforms over result rows unchanged; their monotonicity = DEV-2218. (default)
11. Only `metrics` supply rows; filter-, order- and computed-dimension aggregates never do (issue: "top-level measure"). (default)
12. `population` = A, `null` for unit / unneeded-undefined A, `a × b` for a product; `row_sources` per the Reporting section. (default)
13. No presence marker: COALESCE-to-empty is exact by Axiom 4. Deviation from the issue's implementation sketch. (default)
14. `count(*)`: no work — DEV-2277 refuses every root-less `count(*)`. Unit-keys decision dropped (moot under DEV-2277). (default)
15. Datasource scoping reads metric anchors too. (default)
16. Sibling-stage-anchored dimensions, filters and metrics keep the SIBLING_STAGE refusal: binding gives a same-named stage precedence, inference reads stored datasets only. (user)
17. Pinned A's cells = anchored query at A; refinements never refused for it. (default)
18. SlayerQuery 17 → 18 stamp. (default)
19. `queries/references` › "The anchor is reached by its only to-one path" MODIFIED on DEV-2277's text: the reader is the population, or an aggregate's home for its grain dimensions and filters; the one-rule clause names producer synthesis. (codex)
20. OpenSpec states spine queries keep `spine × A` (A from dimensions only) until DEV-2218; test pins an orphan beside a spine axis not yet being a row. (codex)
21. MODIFIED `queries/semantics` › "Filters restrict by association or fail loudly" and `unrelated-datasets` › "Filters across a product hop are one-sided associations": per-home restriction, INNER joins from each restricted dataset, filters touching several homes, filters on a non-supplying dataset. (codex)
22. OpenSpec Compositionality states the transform-over-result-rows exception; strict-xfail (DEV-2218) tests for `lag` / `ntile` monotonicity. (codex)
23. A product hop is never full-grain (association needs a join path, Axiom 3): an unrelated aggregate's unit cell extends to A's cells; warning stays `associated` + `unrelated_dataset`; error/broadcast modes unchanged. (codex)
24. Datasource scoping: the one datasource holding every anchor (dimensions, filters, metrics, saved metrics expanded), else fail closed naming them; test with homes in two datasources. (codex)
25. Every `_keys` branch casts each key column to its bound dimension type (system P10); tests on SQLite/DuckDB (mixed nullable, decimals, timestamps) and the ClickHouse integration file. (codex)
26. Presence marker rejected (codex): keys come from producer cells (grain columns), never values; Axiom 4 makes absent and all-NULL cells equal by definition. Folded: test that a `sum(...) IS NULL` measure filter treats both alike. (codex)
27. One executed root-less test per producer form (ranked, windowed, shifted, `first`/`last`, second-order, list child, keyless home), each beside a second supplying measure, with orphan keys and association dedup. (codex)
28. `row_sources` contract: canonical dataset names (stage = query-local name), deduplicated, sorted; a product lists each group anchor; `["unit"]` alone with no dimensions; a spine query lists P only; hidden datasets never appear (not anchors under DEV-2277). In "Population is reported". (codex)
29. Filter restriction for inferred queries is an ADDED `queries/semantics` requirement ("Filters restrict every home of an inferred query") rather than a rewrite of "Filters restrict by association or fail loudly", whose text stays true for a named population and for the anchor (OpenSpec: a new concern without changing existing behaviour is ADDED). The not-yet-true transform-monotonicity scenario lives in strict-xfail tests, not in the live spec. (default)

## Risks / Trade-offs

- Row-phase vs combined-phase attach: today local aggregates compute in the base `GROUP BY` over the population; with a key-union base, the anchor's own aggregates become one more producer joined onto the keys. Value equality with the anchored twin is pinned by the producer-forms scenario and the law harness.
- Window endpoints (`_window_endpoints`) and shifted producers read "the population's cells" today; under a key-union base they must read the row supply's cells at their grain, or a windowed value can differ from its standalone run. Covered by the producer-forms scenario.
- Single-source optimisation must give the same rows as the union form; the law harness runs both shapes.
- Transforms over result rows can change values in existing rows when a sibling adds rows until DEV-2218; documented exception, strict xfails pin the target.
- The OpenSpec deltas for `queries/population`, `queries/references`, `queries/unrelated-datasets` and `queries/saved-query-refinement` are written on top of DEV-2277's unarchived deltas; DEV-2277 must archive first, and any later change to those DEV-2277 texts must be re-layered here before archive.
- Breaking for root-less users (D1/D2 rows disappear; filters no longer move the population): the migration guide section states the remedies (add a measure from the dimension's dataset, or name `population`).
