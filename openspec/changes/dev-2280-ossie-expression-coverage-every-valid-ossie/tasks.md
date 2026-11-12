## 1. Tests first (pr-tests)

- [ ] 1.1 Before writing tests, find every existing test whose assertions pin the whole-day reading of a date-only literal in a comparison (`<=`, `>`, `=`, `!=`, `BETWEEN` against a temporal operand), the dropping of instant comparisons on export, and the tripwire's pinned site set. Search tests, notebooks, docs examples run by `tests/test_doc_examples.py`, and agent help. List them by file with what changes, and get the owner's per-test approval before aligning them.
- [ ] 1.2 Write failing tests for every scenario in this change's delta specs. They are unit tests on in-process SQLite and DuckDB, plus Tier-1 matrix entries for the numeric pins and SQLite emission for the casts. Use the orders fixture of `tests/_dev2215_fixtures.py` where it fits (order 2 at `2024-04-10 09:30`). Verify each new test fails on `main` for the intended reason.
- [ ] 1.3 Migration tests: each operator, `BETWEEN` / `NOT BETWEEN` in each layout and nested under `and` / `or`, literal on the left; every walked field (query measures, filters, dimensions, time dimensions, order, `main_time_dimension`, saved-measure formulas, Mode-B `Column.filter` direct, inline, nested and extension-contained, memory queries); TEXT, untypable and variable comparisons untouched (untypable logged); a current parent with an older nested query, inline model, extension and memory query; idempotence; a post-bump document untouched; YAML and SQLite backends. Verify each fails before implementation.
- [ ] 1.4 Tripwire pin: the new marker's site appears in `PINNED` with removal 3.0, and `warn_deprecated` remains the only `FutureWarning` emitter. Verify it fails before implementation.

## 2. Date-only literals are instants

- [ ] 2.1 `core/time_points.py`: classify `YYYY-MM-DD` as the midnight instant in comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `NOT BETWEEN`) and as a day period only as a `date_range` element or the right-hand side of single-string `in` / `not in`. Variable-bound values go through the same reader. Verify with the `queries/time-points` scenarios (instant, measure, saved measure, DATE operand, variable).
- [ ] 2.2 Frame bounds: date-only range comparisons are instant frame bounds, and date-only `=` is an ordinary row filter. Verify with `Period equality is a frame bound, instant equality is not` and `Date-only range bound is a frame bound`.
- [ ] 2.3 The notice: binding records it for a date-only literal or variable meeting a TIMESTAMP operand under `<=`, `>`, `=`, `!=`, `BETWEEN`, `NOT BETWEEN`, emitted once per spelling per query or save as a structured warning (core P6). Verify with the `A date-only comparison that changed meaning is announced` scenarios.
- [ ] 2.4 `core/deprecation.py`: add the tripwire-read marker for informational notices (literal `removal`, no `FutureWarning`), use it at the notice site, and extend `tests/test_law_removal_tripwire.py`'s scanner. Verify with the `compatibility/removal-tripwire` scenarios.

## 3. Stored upgrade

- [ ] 3.1 Bump SlayerModel 21→22, SlayerQuery 13→14 and Memory 9→10 in `storage/migrations.py` (renumber if a concurrent bump has landed). Gate the rewrite on each nested document's own stored version. Verify with the current-parent / old-child tests.
- [ ] 3.2 Extend DEV-2215's Mode-B walker to Mode-B `Column.filter`. Add the context-aware rewrite in the storage load path: type each candidate operand against the datasource's models with the one Mode-B reader, and rewrite through the shared span rewriter per `design.md` › Stored upgrade. Leave and log untypable operands; leave TEXT and variable comparisons untouched. Verify with the `Stored date-only comparisons keep their whole-day meaning` scenarios on YAML and SQLite storage.
- [ ] 3.3 Verify each upgraded fixture returns its pre-change rows by executing it before and after the upgrade.

## 4. Export and import

- [ ] 4.1 `osi/exporter.py`: render an instant comparison (date-only included) as a standard-SQL comparison; periods, single-string `in` and relative tokens stay SLayer-only. Verify with the `Instant comparisons export as standard SQL` scenarios, including the re-import round trip.
- [ ] 4.2 Verify the `Date literals in imported predicates keep Ossie's meaning` scenarios pass (no importer change is expected; fix the importer if one fails).

## 5. SQLite casts and numeric pins

- [ ] 5.1 Cast renderer: one family check asking the dialect whether it can express the authored type. SQLite renders unmodified timestamp types through the `TRY_CAST` conversion, keeps `DATE(x)`, and refuses `TIME`, time-zone modifiers and precision parameters with the typed error, for `CAST`, `::` and `TRY_CAST`. Verify with the `SQLite casts to timestamp and time types` scenarios.
- [ ] 5.2 Library matrix: INT `/` INT and DECIMAL ties under two-argument `round` on every Tier-1 dialect, over declared DECIMAL columns with the planned type asserted; float ties as a separate best-effort check. Fix any failing dialect in its `dialects/` file. Verify with the `Division and rounding follow Ossie's numeric semantics` scenarios in CI.

## 6. Docs

- [ ] 6.1 `docs/concepts/time.md` and `slayer/memories/help_content`: a date-only literal in a comparison is midnight; the whole day is `in 'YYYY-MM-DD'` or `date_range`. Fix every docs example that relied on the whole-day reading. Verify with `tests/test_doc_examples.py`.
- [ ] 6.2 `docs/dbt/dbt_import.md` and `docs/osi/osi_import.md`: date filters described with the new meaning; the exporter's instant comparisons noted. `docs/concepts/formulas.md`: the numeric semantics (true division, `round` ties) in one sentence each.
- [ ] 6.3 `docs/upgrading/2.0.md` (arrives with PR #590, the DEV-2250 re-land; if #590 hasn't merged, merge `origin/main` after it lands before this task): a before/after entry for date-only comparisons, the stored upgrade, stored variable defaults (not migrated), and the notice. Verify the page is in the `zensical.toml` nav.

## 7. Finish

- [ ] 7.1 Run the full unit suite, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors against the baseline), `la-arch-check` and `npx -y likec4@1.47.0 validate architecture`. Verify that all are green.
