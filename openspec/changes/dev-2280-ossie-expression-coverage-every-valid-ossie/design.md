## Context

See proposal.md for why. State on `main` (reproduced 2026-11-12 on SQLite and DuckDB, orders fixture of `tests/_dev2215_fixtures.py`, order 2 at `2024-04-10 09:30`):

- **Date-only strings.** `core.time_points` classifies `'YYYY-MM-DD'` as a day period wherever a string meets a temporal operand. `created_at <= '2024-04-10'` → orders 1, 2 (Ossie: 1); `> '2024-04-10'` → 3–6 (Ossie: 2–6); `= '2024-04-10'` → 2 (Ossie: none); `BETWEEN '2024-03-05' AND '2024-04-10'` → 1, 2 (Ossie: 1). The same holds inside `count(*) FILTER (WHERE …)` and `CASE` in measures. DATE operands, `>=` and `<` agree with Ossie. A tuple `IN ('2024-04-10', …)` is already a plain comparison.
- **Export.** Every time-point comparison binds to `TimePointCmpKey`, which the exporter cannot render, so each such measure is dropped as SLayer-only.
- **Facade.** It already rewrites date-only bounds to midnight instants (`facade/time-filter-translation` › Translated time bounds keep their SQL meaning); it is unaffected.
- **SQLite casts.** `CAST(x AS DATE)` renders `DATE(x)`. `TIMESTAMP`, `DATETIME`, `TIMESTAMP_NTZ` and `TIME` render as plain casts, and numeric affinity yields the year (`CAST(order_date AS TIMESTAMP)` → `2024`). `TRY_CAST` to DATE / TIMESTAMP already goes through the `slayer_try_cast` UDF. `declared_cast_type` already avoids DATE / TIMESTAMP casts for declared types for this reason.
- **Numerics.** Rendered SQL on all nine Tier-1 dialects: `/` casts to a floating type where the engine divides integers (SQLite, Postgres, SQL Server, Trino) and is native true division elsewhere. Two-argument `round` casts to DECIMAL on Postgres. Nothing pins either.
- **Stored text.** DEV-2215 built the walkers (`storage/mode_b_texts.py`, `rank_direction_migration.rewrite_model_texts_at`), the span rewriter and the load-path pipeline with datasource context (`StorageBackend` load, `_custom_bucket_calls`). They visit measures, query fields, nested source models and memory queries, but not Mode-B `Column.filter`. The load path gates context-aware work on the outer document's version.
- **Removal marker.** `core.deprecation.warn_deprecated` is the only marker the tripwire reads, and it always emits `FutureWarning`.

Principles that apply:

- system §3 P4 (AST-built SQL), P8 (one reader of Mode B);
- core P4 (one key kind per meaning), P5 (typed errors), P6 (warnings are structured payloads, surfaced twice);
- engine P2 (parsing is pure syntax), P8 (models persist verbatim: fresh saves are not rewritten);
- storage P3 / P3.1 (versioned persistence; migrations on load);
- sql P1 (AST), P2 (dialect quirks only in `dialects/`), P9 (fail closed);
- semantics §4 P2.1 / P2.2 (one meaning per dialect; refuse rather than mistranslate);
- importers P1 (importers preserve meaning).

No arc42 or `.c4` edit is needed, and no new capability is added, so no spec attachment changes.

## Goals / Non-Goals

**Goals:**

- One classification point for the date-only reading (`core.time_points`), so no position can keep the old meaning (structural fix: the symptom in filters, measures and saved measures is one classifier).
- One cast-family check in the cast renderer, so SQLite cannot render a family-changing cast for any temporal type.
- Stored artefacts keep their results through one load-path upgrade, triggered per nested document's own version.

**Non-Goals:**

- Everything additive from the original DEV-2280 scope, now DEV-2282: `OVER` and Ossie's window NULL ordering, `SUM`/`AVG`/`MIN`/`MAX(DISTINCT)`, `WITHIN GROUP` and `PERCENTILE_DISC`, the missing REQUIRED functions, the time-of-day type, the importer's residual `CASE` / scalar handling and `ARRAY_AGG` fidelity, and quoted identifiers.
- Period literals other than `YYYY-MM-DD`, relative tokens, single-string `in`, `date_range` and `whole_periods_only`: unchanged.
- Mode-A text (`Column.sql`, model `filters`, `sql_table`): already the database's meaning. The Mode-A numerics half is DEV-2175.
- Migrating comparisons whose date-only value arrives through a variable (decision 7).
- Exporting period and relative-token comparisons as ranges.

## Decisions

### Structure

- **Classification.** `core.time_points` reads `YYYY-MM-DD` as an instant (midnight) in a comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `NOT BETWEEN`) and as a day period only as a `date_range` element or the right-hand side of single-string `in` / `not in`. A variable-bound literal goes through the same reader. Instant comparisons keep their existing key and lowering, so frame-bound detection treats date-only range comparisons as instants.
- **Notice.** Binding records a notice when a date-only literal (or a variable bound to one) meets a TIMESTAMP operand under `<=`, `>`, `=`, `!=`, `BETWEEN` or `NOT BETWEEN`. It is emitted once per spelling per query or save as a structured warning through the existing warning collector (core P6). The emission site is marked through a new tripwire-read marker in `core.deprecation` that records `removal="3.0"` and emits no `FutureWarning`. `warn_deprecated` stays the only `FutureWarning` emitter, and the tripwire scanner and its pinned site set include the new marker.
- **Stored upgrade.** One bump: SlayerModel 21→22, SlayerQuery 13→14, Memory 9→10 (renumbered if a concurrent bump lands first).
  - The rewrite runs in the storage load path with the datasource's models, like DEV-2215's custom-granularity step.
  - It is gated per nested entity on that entity's own stored version, so a current parent with an older nested query, inline model, extension or memory query is upgraded where it nests.
  - The walker is DEV-2215's, extended to Mode-B `Column.filter`.
  - Each candidate comparison is parsed by the one Mode-B reader, its operand typed against the models, and rewritten by the shared span rewriter, leaving the rest of the text byte-identical. Untypable operands are logged and left.
  - Rewrites, with `<D+1>` the next calendar day:
    - `<= 'D'` → `< '<D+1>'`
    - `> 'D'` → `>= '<D+1>'`
    - `= 'D'` → `in 'D'`
    - `!= 'D'` → `not in 'D'`
    - `x BETWEEN 'A' AND 'B'` → `(x >= 'A' and x < '<B+1>')`
    - `x NOT BETWEEN 'A' AND 'B'` → `(x < 'A' or x >= '<B+1>')`
    - a literal on the left is mirrored; `>=` and `<` are untouched. In `BETWEEN`, only date-only bounds are rewritten; an instant bound stays as written.
- **Export.** The exporter renders an instant comparison as a standard-SQL comparison with the same operator and literal. Period, single-string `in` and relative-token comparisons stay SLayer-only.
- **SQLite casts.** The cast renderer asks the dialect whether it can express the authored type. SQLite expresses an unmodified timestamp type by rendering the same conversion `TRY_CAST` uses (`slayer_try_cast(x, 'TIMESTAMP')`), so `CAST` and `TRY_CAST` agree and microseconds survive. It refuses `TIME`, any time-zone modifier and any precision parameter with the typed "cannot express" error. DATE keeps `DATE(x)`.
- **Numerics.** Library-matrix cases on every Tier-1 dialect for INT `/` INT and for DECIMAL ties under two-argument `round`, over declared DECIMAL columns with the planned type asserted. Float ties are documented as best effort and asserted separately where the dialect is deterministic. A dialect failing in CI is fixed in its `dialects/` file.

### Decisions log

1. Rescope DEV-2280 to the 2.0 breaking items; additive items move to a 2.x issue. Options: keep the full scope (one huge PR, `OVER` built before DEV-2218's transform domains), or split breaking from additive. Taken: **split**, because 2.0 is the only release that may change meaning, while additive items can ship in any 2.x. (user)
2. DEV-2282 filed with the approved text (Ossie Layer 1 additive coverage). (user)
3. Quoted identifiers. Options: Ossie outright (`"id"` stops matching `id`), or superset (exact spelling, plus Ossie's normalised upper-case form when nothing matches exactly). Taken: **superset**. It is additive, so it moved to DEV-2282. (user)
4. Date-only strings. Options: adopt Ossie (midnight instant), or keep whole-day. Taken: **adopt Ossie**; the whole day stays as single-string `in` and `date_range`. (user)
5. Stored documents. Options: a meaning-preserving load-path migration with model context, or migration-guide only. Taken: **migrate**; operands that can't be typed are left unchanged and logged. (user)
6. Fresh-input notice. Options: none, or an informational structured warning on the four differing operators against TIMESTAMP, removed in 3.0. Taken: **warning**. (user)
7. Variable-bound date-only values. Options: treat them as fresh input (new meaning plus a notice naming the variable), migrate saved defaults, or a version-aware compatibility reading. Taken: **fresh input**; the migration guide names stored variable defaults. (user)
8. `BETWEEN` / `NOT BETWEEN` follow the comparison rule, are covered by the notice and the migration, and are rewritten only when the tested expression is the temporal operand. (default)
9. Other period literals and relative tokens keep period semantics, since they aren't valid Ossie timestamps (superset). (default)
10. Numerics need only pins, since emission already matches Ossie on Tier-1. One-argument `round` (not an Ossie form) is unchanged. (default)
11. The notice's removal marker is a new tripwire-read call in `core.deprecation` that emits no `FutureWarning`, with a MODIFIED `compatibility/removal-tripwire`. (default)
12. The exporter writes instant comparisons as standard SQL; periods and relative tokens stay SLayer-only. (default)
13. The facade is unchanged; it already sends midnight instants. (default)
14. SQLite timestamp casts reuse the `TRY_CAST` conversion, and `TIME`, time-zone modifiers and precision parameters are refused, through one family check. This refines the approved "timestamp normaliser, refuse precision above 3": the `TRY_CAST` conversion keeps microseconds and gives one SQLite reading for both cast forms. (default)
15. Window NULL ordering goes with `OVER` to DEV-2282. (user, via 1)
16. Context-aware rewrites are gated per nested document's own stored version, with current-parent / old-child tests for source queries, inline models, extensions and memories. (codex)
17. The notice marker is an explicit MODIFIED of the removal-tripwire requirement, with the scanner and the exact-site set updated. (codex)
18. The `BETWEEN` rewrite has exact parenthesised forms, other layouts are left and logged, and tests cover each position and nesting under `and` / `or`. (codex)
19. The stored-text walker is explicitly extended to Mode-B `Column.filter`, tested direct, inline, nested and extension-contained. (codex)
20. Cast tests cover `CAST`, `::` and `TRY_CAST`, plus the refusals for `TIME`, time-zone modifiers and precision parameters. (codex)
21. The numeric pins use declared DECIMAL columns with the planned type asserted, positive and negative ties at scales 0 and 2, and float ties kept separate. (codex)

## Risks / Trade-offs

- [Risk] A silent meaning change for fresh input: agents writing `<= '2024-12-31'` lose the last day. → The structured notice names the whole-day spelling, and the agent help and the migration guide are updated.
- [Risk] Stored variable defaults holding a date-only value change meaning (decision 7). → The notice fires naming the variable at execution, and the migration guide lists the case.
- [Risk] Typing operands at load needs models. A store whose model is missing or broken leaves the comparison unchanged with new semantics. → Logged per document; the next load after the model is repaired retries, because the document stays below the bump until it is saved.
- [Risk] Concurrent stored-version bumps (DEV-1607, DEV-2277, DEV-2243). → Whichever merges second renumbers its step; the migration tests pin source versions symbolically.
- [Risk] `docs/upgrading/2.0.md` lives on PR #590 (DEV-2250 re-land), not yet on `main`. → The docs task waits on #590 merging and is added on top of it.
- [Trade-off] Two readings of `'YYYY-MM-DD'` remain by position: an instant in a comparison, a day in `in` and `date_range`. This is the price of keeping the whole-day spelling without a new syntax.

## Migration Plan

- One versioned step plus the load-path rewrite with model context. Both are idempotent, and an interrupted load completes on the next load.
- A store written by this release opens on the previous release: `<`, `>=`, `in '…'` and `not in '…'` already parse there with the same meaning.
- There is no rollback beyond restoring the store, since saves persist the upgraded text.
