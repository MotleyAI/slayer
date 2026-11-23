## Context

See proposal.md (Why). Today every query reference binds as a path from the population (`binding._anchored_parts`
replaces the anchor with the population's route), so an `AggregateKey`'s source leaves carry host-relative paths. The
home is found from those paths (`population/home.py`, `row_supply._home`), producer synthesis walks
`host → home` (`compile/stages._synthesize_cross_dataset_producer`, `walk_key_path(host, target_path)`), DEV-2212's home
routing patches grain and filter leaves back through `prebound.reference_anchors` (first spelling per host path), the
association arm joins back on host keys, `_host_membership` drops home rows no host row reaches, and a re-aggregation's
carrier is grouped over the host's rows. `anchored_rows` re-hosts a named population that cannot route to a metric's
home; `plan_rows` hosts a multi-branch inferred stage at a supplying home because of the carrier.

### Symptom, structural cause, fix

- Symptom: the four acceptance cases of the issue plus the carrier probe (proposal.md).
- Cause: aggregations live in host coordinates — against semantics Axioms 2.4, 2.6, 2.9 and 6 and §4 Principle 1 ("keys
  carry the anchor and its canonical route, never the population").
- Fix (one code path): bind every aggregation interior from its source anchor; key every reference by its anchored
  spelling; root every producer at its home; meet the result only on grain values; build the re-aggregation operand as
  the constituents' row supply. No host route is consulted anywhere inside an aggregation.

### Principles that apply

semantics Axioms 2 (2.4, 2.6 and 2.9 as edited), 3.1, 3.3, 4, 6, 7–8, 10, 12.1, 12.3–12.4, 14; Laws 3, 7, 8, 10; §4 P1
(edited), P4. system §3 P4 (AST-built SQL), P10 (one typing). engine P1 (typed pipeline), P3 (pure resolution, one
bundle), P4 (interning), P9 (every user-facing type error in the checker), P10 (dependency closures). No `model.c4`,
`views.c4` or `index.yaml` change: the work stays in `engine.binding`/`bind_inputs`, `engine.population`,
`engine.compile`, `ir`, `core.keys`, along existing arrows. All touched specs are in the `queries` and `models`
cross-cutting groups.

## Goals / Non-Goals

**Goals:** the specs of this change; deletion of `anchored_rows`, `_anchored_rows`, `_StageInference.anchored`,
`_host_membership` and the host-row carrier; every acceptance scenario executed on SQLite and DuckDB.

**Non-Goals:**

- The synthesised host-grain ORDER BY wrap (`locus="host"` over an unprojected joined sort column) is not a user
  aggregation and keeps its evaluation; only aggregations the query writes bind from their anchor.
- Row supply for spine queries and transforms over their operand's own cells stay DEV-2218.
- Root-less product anchors with a metric homed in one group stay DEV-2390.
- No synthetic keys dataset as the compiler host of an inferred stage (rejected codex finding 10).

## Decisions

### Binding

- A bottom-up aggregate-binding pass resolves each aggregation's source anchor before binding its interior: source,
  parameters, definition defaults, `FILTER (WHERE …)` and ranking keys bind in a scope whose reading dataset is the
  source anchor (engine P3: the bundle already holds every dataset a reference names; an anchor the population cannot
  reach is loaded like an unrelated anchor today).
- Source anchor: one shared anchor → it; anchored apart → the smallest dataset determining every leaf's anchor over
  provably to-one hops (Axiom 2.2 determination, no product, no association); ties → `UnresolvableDimensionRelationshipError`
  naming one explicit single-anchor spelling per candidate, sorted by canonical name, declared case; no candidate → the
  existing input-safety error.
- Key identity: a reference's bound identity is its anchor plus canonical route (P1). Two spellings with different
  anchors are two keys even when the population reaches one column; the population's reach is derived where the
  population reads the key, each producer's where its home reads it. This replaces `reference_anchors` (first spelling
  per host path) and `_source_anchor`'s longest-prefix guess.
- An aggregation's leaves are anchor-relative; the aggregate carries its anchor (and resolved home) so it never needs a
  host path. Saved metrics bind from their declaring dataset, as today's definition frames.

### Producers

- Rooted at the home resolved from the anchor; `target_path` (host → home) disappears from synthesis.
- Grain dimensions and filter conjuncts restricting the home are read from the home by each key's anchored spelling
  (P1); a reference anchored at the source anchor is read back along the source's written path.
- Per-home filter disposition (DEV-2212: inline / semi-join / one-sided product association) in both modes; each home
  judged alone. The population's routability decides only the population's own restriction (a filter it cannot route
  stays refused).
- Association: a private home-row identity (primary key; per-query row number for keyless homes; parent plus element for
  list children) inside the producer; dedup on (grain, identity); only grain columns exposed; join-back on grain values
  (`build_grain_joinback_condition`), never host keys.
- Ranked / windowed / first-last: ordering keys bind from the anchor and must be determined by the home (existing 2.4
  rule); the window axis is the query bucket read from the home; association per bucket as today.
- A grain dimension the home cannot reach keeps today's unrelated handling (Axiom 3.1 product association; coarser cells
  under 12.3).
- No `_host_membership`: a restricted population narrows a home only where the home's own paths read it (Axiom 3.3).

### Re-aggregation

- The operand dataset is the row supply of an inner query whose dimensions are the union grain and whose metrics are
  the constituents (Axiom 2.4 as edited): union of full-union-grain constituents' cells; else the union grain's
  dimension anchor's cells agreeing with the coarser constituents; undefined anchor → `PopulationInferenceError`.
  Coarser constituents broadcast onto the cells; a lacking cell takes the constituent's empty value (Axiom 10).
- The outer aggregate groups the operand cells by its attributable outer grain and joins the result rows on grain
  values. Existing outer-window / FILTER refusals and transform-grain rules are unchanged.
- `plan_rows` stops hosting a multi-branch inferred stage at a supplying home for the carrier's sake; the stage host is
  only the binder's scope (the dimension anchor, else a full-grain home — either routes every dimension it is asked
  for), and the spec requires that no value or refusal depends on it.

### Decisions log

1. Fold the re-aggregation carrier fix in; `plan_rows` stops hosting at a supplying home. Options: fold in / separate
   issue. Probe showed host-row carrier values and a Law 7 violation. (user)
2. Multi-anchor source: the smallest dataset determining every leaf's anchor; ties refused. Options: tie refused / tie
   broken by the population (contradicts Axiom 2.9, differs anchored vs inferred). (user)
3. semantics.arc42.md edits (user, approved wording):
   - Axiom 2 intro: "Datasets are the model's datasets and the join paths between them, a reference naming one by its
     anchor (Principle 1); "D determines X" is Axiom 1, …"
   - Axiom 2.6 first sentence: "The source anchor is where the source lives — the longest common prefix of its leaves'
     paths from their shared anchor (Principle 1); leaves anchored apart meet at the smallest dataset determining each
     leaf's anchor over provably to-one hops (2.2), tied candidates refused as ambiguous: …"
   - Axiom 2.9: "… the population (Axiom 12) is per query (which cells exist), never moves a home nor routes to one, and
     meets it only on grain values (Axiom 6)."
   - Axiom 2.4 last sentence: "A source with no row-level leaf is a second-order aggregation: its home is the operand
     dataset — the row supply (12.3–12.4) of its constituents at their union grain — not a model."
   - §4 Principle 1: "Each dataset reading the reference — the population; an aggregation's source anchor (Axiom 2.6),
     for its source; its home, for its grain and filters — reaches the anchor …"
   - Tags: `[enforced: test:tests/test_dev2388_own_anchor.py]` on Axiom 2 and Principle 1.
4. No host membership: a named population decides only the rows (brief, Axioms 2.9 / 6 / 12.1, Ossie A.1). (default)
5. Association from the home, joining back on grain values (Ossie §4). (default)
6. Each spelling routed independently by every reader. (default)
7. Multi-anchor tie error = `UnresolvableDimensionRelationshipError`, as Principle 1's other ambiguities. (default)
8. The a16-shaped test lives in this change (DEV-2243 not merged). (default)
9. Carrier lacking cell takes the empty value (Axiom 10) over the old spec text "NULL". (default)
10. Inferred-host invariance stated as a requirement. (default)
11. Re-aggregation operand rows = row supply of the constituents at the union grain (12.3–12.4), undefined anchor fails
    closed. Options: row supply / full-grain constituents else the population's rows (keeps host dependence). The
    owner's alternative — cartesian product of non-overlapping members joined on shared ones — was discussed: it
    invents cells across connected grains and inner-joins away cells one constituent lacks; row supply already yields
    the product exactly where grain groups are unconnected (12.2). (user)
12. Bottom-up aggregate binding in an anchor scope; aggregates carry anchor and home. (codex, finding 1)
13. Multi-anchor candidates by Axiom 2.2 determination only; disconnected anchors → input-safety error; tests for
    diamond, disconnected, inherited edge, list child, keyless. (codex, finding 2)
14. Three restriction cases spelled out: model/RLS filters bind reads of their dataset; a population's restriction never
    narrows a non-reading home; a home reading a restricted stage on its path sees only its rows. Tests: filtered stage,
    metric-filtered stage, inherited reverse edge, model-filtered population, RLS. (codex, finding 3)
15. Reference identity = anchor + canonical route; tests for reversed order, two spellings in one expression, saved
    metrics through two frames, defaults, FILTER, ranking keys. (codex, finding 4)
16. Carrier defined relationally as the inner row supply (decision 11). (codex, finding 5)
17. Per-home filter disposition in both modes; two-home filter and OR/NOT tests. (codex, finding 6)
18. Private home-row identity in association producers; repeated / NULL list elements, keyless duplicates tests.
    (codex, finding 7)
19. Ranked / windowed binding subsection; host-grain sort wrap a Non-Goal. (codex, finding 8)
20. Spine stays the row source; anchored spine test with an empty month. (codex, finding 9)
21. rejected (codex, finding 10): synthetic keys dataset as compiler host — a full-grain home or the dimension anchor
    routes every dimension anyway, so no host choice can add a refusal; folded its reordered-metrics test.
22. Datasource: the existing anchored-datasource test stays green without `anchored_rows`; nested saved-metric anchor
    case added. (codex, finding 11)
23. Every requirement pinning host-rooted values modified: population, references, multi-stage, semantics,
    model-filters, cross-model-aggregates. `cross-model-aggregates` › Producer filter routing keeps its population
    filter inheritance: it covers producers whose home is the population. The plan-time scan missed
    `models/model-filters`, `cross-model-aggregates` › Empty cells and `semantics` › Home dataset / Grain-union broadcasting;
    the pr-tests sweep of every main spec added them (decisions 27–29, 31). (codex, finding 12)
24. Consumer positions: filter, order, computed dimension with `partition_by=`, attached parameter under a two-route
    population. (codex, finding 13)
25. Error contract: candidates sorted by canonical name, declared case; tie → ambiguity, none → input safety.
    (codex, finding 14)
26. `queries/references` scenarios "A unique fanning route is taken", "Several fanning routes are refused" and "A single
    direct inherited relationship is taken first" rewritten (titles kept) to exercise references the population reads:
    under this change an aggregation's source is never routed from the population. `queries/multi-stage` scenario "A
    filtered stage restricts what is reached through it" rewritten (title kept) per decision 4. (default)
27. `models/model-filters` › Model filters define the model's dataset: a model filter restricts only reads of its
    model, "the root of a cross-model or isolated aggregate" read as its home; scenario "Isolated, windowed and shifted
    evaluations read the filtered root" rewritten (title kept): the root's own aggregates stay over orders 1–3, an
    isolated `count(returns.*)` counts 4, grouped by `done_orders.id` 2 / 1 / 0 (decision 4). (default)
28. `queries/cross-model-aggregates` › Empty cells take the aggregation's empty value: as an aggregation source the
    empty value fills operand cells only; scenario "Re-aggregation counts the zeros" rewritten (title kept): 9/5 over
    the customers with an order, and a `FILTER (WHERE …)` variant (5/5) whose filtered-out customer still counts as 0,
    keeping the title true (decision 11). (default)
29. `queries/semantics` › Home dataset of a row-level aggregation source: the fallback candidate, default cancellation
    and root-anchored defaults are stated from the source's reference anchor and as query references (arc42 2.4 / 2.6
    as edited), never from the query root; every scenario keeps its values (each query's root is there the unique
    smallest dataset determining the inputs' anchors). (default)
30. `queries/population` › A restricted population narrows a home only on the home's own paths: the `orders.status`
    rows include C4's NULL-status row with a NULL value — the anchored rows are unchanged. (default)
31. `queries/semantics` › Grain-union broadcasting: inside a second-order aggregation's source the operand dataset,
    not the population, supplies the cells (Axiom 2.4 as edited); scenarios unchanged. (codex)
32. `queries/attribution-modes`: a partition key's own dependency closure is judged from the key's anchor
    (Principle 1), never from the population ("host"); requirement renamed "Partition key fanning from its anchor fails
    closed in every mode", "Error mode refuses silent semantics" aligned, scenario bodies de-hosted, outcomes kept, one
    scenario added for a key fanning from the population but local to its anchor; `queries/transforms` scenario
    "Partition key across a fanning hop stays a typed error" body aligned (codex). Options: key's anchor (keeps every
    scenario and today's engine behaviour) / aggregate's home (Axiom 8's former literal; flips "Host-safe partition key
    keeps its mode-aware resolution" to an error). Axiom 8 reworded to match (approved separately). (user)
33. `queries/references` › The directed cycle is announced: its example reads `z.amount` in a filter, a reference the
    population reads; an aggregation's source is no longer routed from the population (decision 6). (user)
34. DEV-2392 folded in: every dataset a statement reads binds its Mode-A `{var}` with its own `query_variables`
    defaults under the query's layers (outer, stage, runtime) at one substitution point; another dataset's defaults
    never reach it, and a spliced stage rebinds its source and cells under its own layers. Hosting an inferred
    population at its dimension anchor makes joined datasets common, so an unbound `{var}` would silently empty
    their values. (user)

## Risks / Trade-offs

- [Key identity change ripples through interning, dedup and result keys] → keys stay structurally equal for one spelling;
  result-key tests (`queries/references` › Result keys never depend on the population) and the dimension-alias tests run
  in the local scope.
- [Producer SQL shape changes (join-back, association, carrier) re-bless many goldens] → re-bless only after the
  executed-value suites for the same shapes pass; every re-blessed golden's executed twin must be unchanged.
- [Behaviour break for restricted populations and re-aggregations] → `docs/upgrading/2.0.md` names both, with the
  remedy (filter the query instead of the population; add the constituent at the full union grain).
- [Mixed-grain operands with an undefined union-grain anchor now fail closed] → typed `PopulationInferenceError`
  naming the candidates; remedy in the message.
- [Existing tests pinning the old behaviour (membership, carrier counts, the rewritten scenarios)] → aligned in
  pr-tests with per-file owner approval.
