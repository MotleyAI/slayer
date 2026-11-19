## Context

See proposal.md › Why. Facts the approach rests on (read on 2026-11-12 to 2026-11-15):

- **First segment.** `binding._resolve_dotted` resolves a first segment in this order:
  1. the root qualifier (`drop_root_qualifier`, DEV-2187);
  2. the query alias map;
  3. the join walk: `join_walker.resolve_hop` tries an edge name, then a stage spelling, then the target model name, then a query-local alias, each exact then case-insensitive;
  4. unrelated datasets (`_unrelated_hops`);
  5. short-form routing (`_route_edgeless_hop` → `dimension_routing.route_dotted_target`), single-hop misses only.
- **Bare names.** `binding._resolve_ref` tries the alias map, then a column, then a saved measure. A root-less query is rewritten to the inferred `source_model` before binding (`query_engine._infer_population_of`).
- **Inference.** `population/inference.py` treats a first segment as a model name (`_anchor_names`). `probe_item` strips the candidate's own name and walks the rest literally before falling back to the short-form route; that is the literal-first rule this change removes.
- **Keys.** They come from `sql/naming.py`: `result_key_from_alias(source_relation, alias)` and `slot_result_key`, with `source_relation` set to the root spelling (`compile/stages.py`). Unaliased aggregates use `canonical_aggregate_alias` over the root-relative `source_anchor_path`. Stage column names are `flat_name(key, strip_relation=population)`.
- **Recommendation.** `query_engine._resolve_recommend_item` splits off the aggregation and passes `orders.*` to `resolve_entity`, which refuses it (the DEV-2045 bug).
- **Tests.** Nearly every test query names `source_model` and uses bare names: about 6,900 bare dimension strings, 1,550 `count(*)`, and only 66 of 1,946 `SlayerQuery` literals without `source_model`.
- **Builds on DEV-2021 and DEV-2318,** both on `main`, and on DEV-1607 (PR #604), whose branch is merged into this one (`11cd2c03b`, 2026-11-19). The MODIFIED texts are re-based on that merged corpus (task 1.3).
- **Notices follow DEV-2280's convention:** a marked informational notice is a structured response warning built through `slayer.core.deprecation.scheduled_notice`, never a `FutureWarning` (`compatibility/removal-tripwire`).

**Principles that apply:**
- `semantics` §4 P1, rewritten here, and Axioms 2.4, 2.6 and 3.1, reworded here.
- `system` P8 (one reader of Mode B: conversion and classification work on `core.syntax` parse trees) and P11 (fail at save time: saved queries store the qualified twin).
- `python.engine`:
  - P3 (resolution is pure: the anchor resolver reads the bundle only);
  - P4 (interning is the dedup mechanism; public names are a separate namespace, which is where the written anchor lives);
  - P5 (stages compose through schemas);
  - P7 (one slack pass with typed warnings: the legacy conversion);
  - P8 (a deprecated spelling is stored as its canonical twin);
  - P11 (stage names are query-local).
- `python.core` P5 (typed errors) and P6 (warnings surfaced twice).
- `python.storage` P3 (versioned persistence; an upgrade path is never a removal).

None is strained beyond the approved arc42 edits.

## Goals / Non-Goals

**Goals:**
- A valid Ossie Layer 3 reference means in SLayer what it means in Ossie. The one exception is the path origin in root-less queries, which DEV-2212 owns.
- One anchor resolver, used by every surface that reads a reference.
- Result keys that never depend on the population.

**Non-Goals:**
- Public output names for unaliased entries (`region` vs `customers.region`): DEV-2279.
- Routing from each aggregate's home instead of the population, and the root-less row rule: DEV-2212. semantics §4 P1 carries the `[target: DEV-2212]` clause.
- Model-scoped metrics referenced by bare name: DEV-2243.
- The strict `conformance="ossie-l3"` switch and Ossie's error codes: DEV-2217.
- References inside stored definitions: they stay dataset-local.

## Decisions

1. **Plan on DEV-1607's committed plan and DEV-2021's end state.** (user)
2. **Two or more to-one paths are refused, even with a direct relationship; no length preference.** Alternative: a direct to-one edge wins. Rejected: it is the shortest-path preference Ossie refuses on purpose, its answer depends on where an edge happens to be declared, and strict mode would need to switch it off. (user, Q1 A)
3. **The path origin is written as the target state.** The reading dataset is the population, or an aggregate's home for its grain and filters, `[target: DEV-2212]`. A comment on DEV-2212 records the gap (a valid Ossie query that SLayer refuses until slice 5), what slice 5 must do, and every other ruling below that affects it. (user)
4. **An aliased entry keys by its bare alias.** Alternative: `<common anchor>.<alias>`. Rejected: the key would change with the expression, and the query would refer to the entry by a different name than its key. (user, Q2 A)
5. **Root-less `count(*)` is always refused,** with or without dimensions. This supersedes the with-dimensions half of DEV-2212's ruling ("binds to A"). Alternative: bind to the inferred population as a permanent shorthand. Rejected: what it counts would change when a dimension is added. (user, Q3 A)
6. **An informational notice announces the one silent meaning change,** a dataset-first reference that the 1.x reading sent through a direct edge. It is removed in 3.0. Alternative: migration guide only. (user, Q4 A) After merging DEV-1607 it follows the merged notice convention: a structured response warning marked through `scheduled_notice`, not a `FutureWarning` and not a separate Python warning. (user, re-base approval 2026-11-19)
7. **`architecture/semantics.arc42.md` §4 Principle 1** (user, approved wording) is replaced by:

   > 1. **Dotted-canonical references**: dots denote join paths in queries and model
   >    SQL; the legacy `__` split-alias input form is a hard error; `__` survives
   >    only as an internal generated-SQL join alias (`__slayer_` prefix reserved).
   >    A query reference is **anchored** at its first segment, a dataset — a query
   >    stage by its name in its own query list, which precedes a same-named model;
   >    the rest is an explicit route. Each dataset reading the reference — the
   >    population; an aggregate's home, for its grain and filters
   >    [target: DEV-2212] — reaches the anchor by its only to-one path, else its
   >    only route, else the reference is refused as ambiguous; with no route the
   >    anchor is unrelated (Axiom 3.1). A bare name is a metric. A resolved path is
   >    **canonical**: each hop spelled by its edge name, else its target model
   >    (`customers.regions ≡ customers.hr` for an edge named `hr`); keys carry the
   >    anchor and its canonical route, never the population (an alias keys as
   >    itself), and join aliases carry the canonical path from the reading dataset.
   >    [enforced: test:tests/test_dev1743_resolution.py]
   >    [enforced: test:tests/test_dev1954_canonical_paths.py]
   >    [enforced: test:tests/test_dev2277_qualified_references.py]

8. **Axioms 2.4, 2.6 and 3.1** (user, approved wording):
   - In Axiom 2.4, "any other unreachable qualifier anchored at the query root. Only definition defaults cancel — a revisiting query path is refused (its root qualifier is no revisit, Principle 1)" becomes "any other unreachable qualifier read as a query reference (Principle 1). Only definition defaults cancel — a revisiting query path is refused (its anchor is no revisit, Principle 1)".
   - In Axiom 2.6, "any other qualifier the anchor cannot reach forward anchored at the query root instead" becomes "any other qualifier the anchor cannot reach forward read as a query reference (Principle 1) instead".
   - In Axiom 3.1, "reached only by naming `D` as a query path's first hop, never by route search" becomes "reached only by naming `D` as a reference's anchor (Principle 1), never by route search".
9. **An edge name is never a first segment;** role-playing is spelled `<dataset>.<edge>.<field>`. A first segment naming both a dataset and an edge reads the dataset; an edge named like a dataset stays legal and is reached after an anchor (`orders.orders.name`). (default)
10. **Keys use the written anchor; different anchors are different entries,** even when they read one column. Spelling twins of one hop stay one value (DEV-1954). (default)
11. **Expression keys** are the common anchored prefix plus the sanitized text relative to it, which keeps today's keys for single-dataset expressions; with no common anchor, the bare sanitized text. (default)
12. **The legacy conversion runs only when a population is named,** once at pipeline entry over parse trees, with the bundle to classify first segments. It warns once per form per document, and a saved query stores the converted twin. (default; the issue's scope, engine P7/P8)
13. **A dataset-first reference is always the new syntax,** even with a population. Otherwise a valid Ossie query would warn or change meaning. (default)
14. **Bare-name order:** query alias, then model-scoped metric (DEV-2243), then the legacy conversion, then an unknown-metric error with qualified suggestions. (default)
15. **The stored migration is model-aware and meaning-preserving,** and keeps stage column names. Unconvertible documents become unloadable; memories convert on each load. This follows DEV-2021's precedent. (default)
16. **Stored definitions keep dataset-local references.** (default)
17. **`recommend_population`:** `count(D.*)` is an attachment on D; `count(*)` and bare names are refused with qualified suggestions. This absorbs DEV-2045. (default)
18. **Spec corpus.** Rule-bearing requirements are MODIFIED here. After DEV-1607 merges, every MODIFIED text is re-based on the respelled corpus. Then a sweep MODIFIES each scenario whose asserted keys or outcome change, and the owner OKs the exact diff (tasks 1.3–1.4). Scenarios that use a legacy spelling whose outcome is unchanged stay as they are. (default)
19. **The whole test suite is respelled** to the qualified syntax by applying the conversions recorded during a test run, through the deterministic tooling, so the suite exercises the qualified syntax and emits no deprecation outside the dedicated legacy tests. (default)
20. **Refinement identity and cache keys compare converted forms.** (default)
21. **New capability `queries/references`** under the cross-cutting `queries` group, so no `model.c4` or `index.yaml` change. (default)
22. **Anchors come from the reading dataset's datasource only.** A visible dataset of another datasource keeps today's unknown-dataset error and never becomes an unrelated product. (codex)
23. **One anchor resolver serves binding, population inference, recommendation, schema-drift attribution and the stored migration.** Inference's literal-first precedence is removed (`queries/population` › Dimension-determined default is MODIFIED). Tests assert that inference and binding agree on cycles and parallel paths. (codex)
24. **A bare alias is referenceable only where an alias is referenceable today** (filters, order, later stages). Transforms over aliases are not added. (codex)
25. **DEV-2277 sets the keys** of response columns, response metadata, stage schemas, SQL projection aliases and cached results. DEV-2279 may later change only the public column names of unaliased entries. (codex)
26. **The written anchor is carried as an explicit reference identity** (anchor, canonical route, leaf) through projection, separate from the bound value key, so interning never merges two anchors and never splits two hop spellings. (codex)
27. **Stored definitions bind in an explicit dataset-local mode** that never applies query anchoring or unrelated products. They get a test matrix over `Column.sql`, dataset filters, saved metrics and custom-aggregation defaults. (codex)
28. **A migrated path must round-trip to the same oriented relationship sequence,** else the document is unloadable with a targeted warning. (codex)
29. **First segments follow DEV-2215's identifier-case rule;** a hidden dataset is unknown and never revealed. Tests cover quoting, case, stage-over-dataset precedence and hidden datasets. (codex)
30. **`queries/multi-stage`** gets a MODIFIED delta of "Stages are ordered by every sibling reference" (decision 35); stage references by name are specified in `queries/references`. (user, 2026-11-19, supersedes the earlier "not modified")
31. **`compatibility/ossie-vocabulary`** gets its MODIFIED delta: "The rename changes no result" compares the two spellings only, dropping "and the same as before this change" and the scenarios' "equal to those recorded before the rename". (default; added in the re-base, user-approved 2026-11-19)
33. **Re-base on the merged corpus** (user, 2026-11-19): deltas respelled to the merged vocabulary; the MODIFIED delta of the renamed "A joined stage's flattened columns are read by their flat names from any root" uses its new scenario title; the tripwire block is re-copied from the merged text before this change's items are added.
32. **The idiom list in `mcp/tool-surface`** › Documented query idioms behave as documented is re-spelled qualified in the post-DEV-1607 sweep. The new requirement "Documented examples are written qualified" governs the texts from this change on. (default)
34. **In a query, `source` is an ordinary first segment** (a dataset or stage named `source`, else unknown). The `source.x` source-column form of `semantics.arc42.md` §4 P3 belongs to definitions only. (user, 2026-11-19, merge of DEV-2175)
35. **Stage ordering reads every first segment naming a sibling,** with no relationship-name exception: `queries/multi-stage` › "Stages are ordered by every sibling reference" gets a MODIFIED delta (supersedes decision 30). A first segment naming both a sibling and a relationship of the population reads the sibling (decision 9); the relationship is reached as `<population>.<relationship>.<field>`. (user, 2026-11-19, merge of DEV-2219)
36. **`semantics.arc42.md` §4 gains Principle 4** (user, approved wording):
   > 4. **Ossie superset**: every input valid under the Ossie core specification is valid here and means what Ossie says it means. An extension may give meaning to input that Ossie leaves undefined or declares invalid, and never changes the result of an Ossie-valid input. [target: DEV-2372]
37. **Merge of `origin/main` (`fd158c644`)** (user, 2026-11-19): four merges, one per PR, oldest first. `semantics.arc42.md` §4 P1 keeps this change's text and P2–P3 take main's; main's P1 clause "except inside a row definition (P3)" is not carried. Axioms 2.4 / 2.6 take the auto-merged text. The four deltas main rewrote are rebased on main's text with no change of meaning, and the qualified-references `SlayerModel` upgrade moves to 27 → 28.
38. **Inherited relationships never decide over declared ones** (user, 2026-11-19, merge of DEV-2219). The anchor rule runs over declared relationships (the datasets' own, the query's extensions, and an inherited one beside a declared one between the same two datasets, keeping DEV-2219's "differently named edges on the same keys stay parallel"). Only with no declared route are inherited ones added, a single one joining the reader to the anchor taken first. `semantics.arc42.md` §4 P1 (user, approved wording): "reaches the anchor over declared relationships, an inherited one counting only beside a declared one between the same two datasets: by its only to-one path, else its only route, else the reference is refused as ambiguous. Only when declared relationships give no route are inherited ones added, a single inherited relationship joining the reader to the anchor taken first; with no route at all the anchor is unrelated (Axiom 3.1)."
39. **Main's DEV-2219 parallel-edge tests expect the plan's ambiguity error** (`UnresolvableDimensionRelationshipError`, user-approved alignment, 2026-11-19); their follow-up reads are spelled from the stage (`per_customer.buyer.name`).

## Risks / Trade-offs

- **[Keys change for every caller of joined fields, cross-dataset aggregates and aliases]** → Intended. The migration guide shows a before and after; stage column names stay stable.
- **[Root-less Ossie queries whose path from the inferred population is ambiguous are refused until DEV-2212]** → The target clause in P1 and the DEV-2212 comment track it; the refusal names explicit spellings.
- **[The respelling of about 10,000 test references could change what a test checks]** → Conversions are recorded from the engine's own converter, so each respelled reference means exactly what the bare one meant. Changes to key assertions and test logic get per-file owner approval.
- **[Silent meaning change in directed cycles]** → The notice, and stored queries keep their old path.
- **[The MODIFIED texts drift if PR #604 changes before it merges]** → Re-run the re-base of task 1.3 after merging `origin/main`, with `openspec validate --strict` as the gate.

## Migration Plan

- `SlayerQuery` 16 → 17, `SlayerModel` 26 → 27 and `Memory` 12 → 13, renumbered if another slice merges first. The model-aware conversion runs in DEV-2021's storage load pipeline. Saved queries and query-backed models are written back; memories convert on each load.
- Rollback: a v16 document read by older code fails on the unknown version, as for every bump.
