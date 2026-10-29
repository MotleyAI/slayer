## Context

See proposal.md (Why). Root cause: a bare name in `Column.sql`, a model filter or a formula resolves to a logical column
first and to a physical column only when no column has that name (`slayer/sql/column_expansion.py`
`_process_reference_site`); the importers write the source's physical names bare. Grounding: system.arc42.md
principles 16 (logical names, physical once) and 17 (importers preserve meaning). The `importers` bucket has no element
arc42; its specs map to `openspec/specs/importers/*`. This change is stacked on DEV-2137; `origin/main` is merged only
once the DEV-2101 / DEV-2137 stack lands.

Probe at the DEV-2137 tip (DuckDB): Cube `SUM(CASE WHEN {CUBE}.status = 'paid' THEN 1 END)` with dimension
`status: {CUBE}.state` → 1 (Cube: 2); a measure filter `{CUBE}.status = 'paid'` → 20 (Cube: 30);
`UPPER({CUBE}.status)` groups by `UPPER(state)`; `status: UPPER({CUBE}.status)` → `ColumnCycleError` at save, cube lost;
`{orders.paid_sum}` dropped. dbt reproduces the collision (1 vs 2) and types a list source `DOUBLE`. sqlglot
transpiles `'a' = ANY(tags)` and `CARDINALITY(tags)` unchanged to Snowflake / BigQuery, where neither runs.

## Goals / Non-Goals

**Goals:**
- One shared package `slayer.importers`: the physical-reference resolver, the aggregate translator (moved from
  `slayer/osi/expression.py`), and the column-typing helper — used by Cube, dbt and OSI.
- Principle 17 enforced by `tests/test_dev2171_importers_preserve_meaning.py`.

**Non-Goals:**
- Recovering list element types from the type probe (probed list expressions get `element_type: UNKNOWN`; extending the
  probe is a driver-level change of DEV-2163's kind).
- Schema-drift changes for persisted scalars over live lists (DEV-2170 item 5).
- Re-writing models imported before this change (users re-import).
- Re-exporting calculated measures through Cube view facades (still reported, unchanged).
- Non-bare calc-measure operands that read another cube (dropped with a report).

## Decisions

### Resolver pipeline

1. Reserve every source member name of the model (dimensions, measures, segments) and fix the source → emitted name
   map (allocator suffixes, duplicates, namespace collisions) before any SQL is resolved.
2. Classify Cube braces: physical (`{CUBE}.x`, `{own}.x`, bare `x`), logical member (`{m}`, `{CUBE.m}`, `{own.m}` →
   emitted name via the map), join path (`{other.m}`). FILTER_PARAMS stay opaque, parse-safe placeholders.
3. Parse each fragment in the datasource dialect (sqlglot default when unknown); walk root-scope column references
   with `slayer.sql`'s root-scope walker (the expander's own scoping); rewrite only physical references whose target
   differs, by span, keeping all other text verbatim.
4. Resolve each FILTER_PARAMS `body_template` separately, then insert the required / optional blocks. Text holding
   sentinels or `{? ?}` blocks is never parsed.
5. Physical `x` → (a) a same-named unfiltered base column reading `x`; (b) else another unfiltered base column reading
   `x`, first declared; (c) else, with no column named `x`, bare `x` in Mode A (a Mode-B operand gets a base column
   `x`); (d) else a hidden base column `x_raw` (`sql: x`, `_raw` repeated until free), one per model.
6. Matching: quoted identifiers exactly; unquoted with the dialect's folding; more than one match → the construct is
   dropped with an ambiguity report. Emitted base-column SQL keeps the source's quoting.
7. Join keys: a separate two-sided phase after all cubes are converted maps each physical `ON` operand to an existing or
   hidden base column on its owning cube and writes those names into `join_pairs`.

### Typing order

Database (base columns: live reflection incl. list element types; expressions: LIMIT-0 probe, sql-mode sources via
`SELECT expr FROM (<sql>) alias`) → declared (dbt `data_type`; Cube dimension type, or a dimension over the same
physical column) → implied (`sum` / `avg` / numeric → `DOUBLE`, `sum_boolean` → `INT`) → count family `TEXT` →
order-dependent aggregation over an untyped source: measure dropped. Both CLIs resolve `--datasource` and connect
best-effort; the offline report entry names why (absent / resolution failed / connection or probe failed).

### Decisions log

1. Scope: the shared resolver covers Cube, dbt and OSI (options: all three / Cube only) — all three, so principle 17
   can be enforced and the acceptance fixture omits no known form. (user)
2. Typing: database type when reachable, then declared, then implied; offline import still works with an info entry
   (options: live with fallback / live required) — live with fallback keeps offline Cube import. (user)
3. OSI array functions: map all three to the library, documenting NULL differences (options: map all / refuse all /
   map `contains` + `length` only) — map all. (user)
4. Principle 17 amended with the NULL-conventions carve-out (options: amend / keep strict and refuse `ARRAY_AGG`):
   "**Importers preserve meaning**: an importer emits a construct only when SLayer reads it with the source's meaning,
   up to SLayer's documented NULL conventions; anything else is dropped with a report." (user, approved wording)
5. New package claimed in the importers bucket of `architecture/model.c4`, applied once the package exists:
   `packages ['slayer.dbt', 'slayer.cube', 'slayer.osi', 'slayer.ingest_report', 'slayer.importers']`
   (user, approved wording)
6. Offline last resort: count family → `TEXT`; `min` / `max` over an untyped source → dropped with a report (options:
   that / `DOUBLE` guess) — an inferred aggregate is cast to its column's type, so a guess can change a result. (user)
7. OSI field expressions with array functions render through SLayer's `SqlDialect` list hooks (options: render via the
   library / refuse) — one renderer for list functions (principle 10). (user)
8. Principle 17 tag flip at the end of pr-implement: `[target: DEV-2171]` →
   `[enforced: test:tests/test_dev2171_importers_preserve_meaning.py]` (user, approved wording)
9. Cube operand policy: a non-bare operand is always a hidden Mode-A column (verbatim native SQL), a bare one
   aggregates its column — Cube's SQL is native-dialect SQL, Mode A is its meaning. (default)
10. OSI keeps its Mode-B inline operand policy — OSI's selected form is portable SQL; the library is its meaning.
    (default)
11. Cube calc measures reuse the OSI aggregate translator, moved to `slayer.importers` — one code path. (default)
12. Hidden raw base column spelled `<x>_raw`, not `x__raw` — `__` is the reserved legacy split-alias form. (default)
13. A filtered measure-source column never takes a physical name (`<measure>_col`). (default)
14. Cube join `ON` brace-dot operands resolve physically — consistency with every other surface. (default)
15. Cube views carry hidden dependency columns transitively. (default)
16. A non-bare Cube operand reading another cube is dropped with a report. (default)
17. OSI `COUNT(DISTINCT list)` / `MIN` / `MAX` / `SUM` / `AVG` over a list refused, not mapped to element-level
    aggregates — a different meaning. (default)
18. `_SAFE_STRING` removed: `'` emitted as `''`, backslash literal — the grammar's rules. (default)
19. Division by zero → NULL accepted as a documented NULL convention. (default)
20. `origin/main` merged only after the DEV-2101 / DEV-2137 stack lands. (default)
21. Both CLIs resolve `--datasource` and connect best-effort regardless of flags; the offline entry names the reason.
    (codex)
22. Cube resolver pipeline order with opaque FILTER_PARAMS placeholders and per-`body_template` resolution. (codex)
23. Source member names reserved first; logical refs rewrite through the source → emitted name map. (codex)
24. Join keys resolved in a separate two-sided phase. (codex)
25. `measure_dependency_problems` gains keyword-only column names and a join-path resolver (backward compatible); Cube
    and dbt pass them, so aggregation inputs are checked. (codex)
26. NULL conventions stated exactly as the list-functions spec defines them. (codex)
27. Base columns typed by reflection; probed list expressions get `element_type: UNKNOWN`; sql-mode probe over a
    subquery. (codex; `UNKNOWN` is the default)
28. Identifier matching: quoted exact, unquoted dialect-folded, multi-match reported as ambiguous. (codex)
29. A filtered aggregate always gets its own helper, deduplicated by (resolved operand, resolved filter, type); the
    base column is never mutated or reused. (codex)
30. Test list extended: FILTER_PARAMS bodies, quoting / case, composite and renamed join keys, root vs nested scope,
    query-backed sources, helper dedup, current-spec vs legacy OSI, facade name collisions and transitive helpers,
    unreachable datasource, probed lists with known vs unknown element types. (codex)

## Risks / Trade-offs

- [Span rewriting depends on sqlglot node positions] → fall back to dropping the construct with a report when a
  position is unavailable; never regenerate the author's SQL.
- [Live typing adds a database dependency to Cube import] → best-effort; an unreachable datasource only adds an info
  entry.
- [Offline `TEXT` count sources over numeric live columns can draw drift proposals] → the offline info entry says to
  re-import against the datasource.
- [The NULL carve-out could be over-used] → it covers only conventions documented in the specs; review enforces it.
- [Existing tests may pin reversed behaviour] → listed for user consent in pr-tests before any is changed.
