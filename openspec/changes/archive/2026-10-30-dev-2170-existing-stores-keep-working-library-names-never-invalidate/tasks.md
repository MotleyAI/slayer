## 1. Tests first (pr-tests stage)

- [x] 1.1 Write `tests/test_library_growth.py`: a vendored frozen snapshot of every reserved vocabulary (functions, spellings, date aliases, transforms, built-in granularities, built-in aggregations) with the unpinned-name guard; generated per-name cases (stored aggregation for every function/spelling loads and answers; stored granularity for every reserved name loads after migration and answers). Verify: the tests fail on the current code for the right reason.
- [x] 1.2 Write the shadow tests (spec `models/aggregation-names`): functional/colon/method forms with kwargs; owner scoping; literal argument; each `SCALAR_SPELLINGS` member separately; case resolution and ambiguity; undefined name keeps the library error; transform name still refused. Verify: they fail now.
- [x] 1.3 Write the shadow-warning tests: on save, on load, on executed, dry-run, EXPLAIN and cache-hit responses; through direct queries, stored measures, saved queries and memories; no warning on a formula override of a built-in aggregation. Verify: they fail now.
- [x] 1.4 Write the `collect` override tests on SQLite and DuckDB (spec `aggregations/collect`). Verify: SQLite raises `ListTypeError` today; on DuckDB the formula already answers, but the empty value and ranked `first`/`last` overrides fail.
- [x] 1.5 Write the granularity-rename tests (spec `models/stored-upgrade`): `left`, `collect`, the taken-name suffix, every reference position the grammar accepts (enumerate from the grammar), dependant documents of each kind, a failure-injection convergence test on YAML and SQLite, and the save-time refusal. Verify: they fail now.
- [x] 1.6 Write the `agg(distinct)` upgrade tests (main-shaped model with column `distinct`; `count(DISTINCT x)` untouched). Verify: they fail now.
- [x] 1.7 Write the old-grammar gate tests: `::`, `||`, `CAST`, `FILTER`, `BETWEEN` in old-version documents load unchanged and warn; genuine old text is still upgraded; the 0.10.2 corpus still passes. Verify: the new-only cases fail now.
- [x] 1.8 Write the rollback-safety tests: a vendored v1.4.1 field-set snapshot, generated from the `v1.4.1` tag; no `null` in any stored kind from either backend; the memory `nulls` case; YAML memory byte stability, with `id`/`learning` never in frontmatter. Verify: the memory cases fail now.
- [x] 1.9 Write the drift tests (spec `models/schema-drift`): unrelated drifted column → original error (the SQLite `ln(0)` repro, re-shaped so `ts` drifts INT-over-TEXT, since temporal-over-TEXT is no longer drift); read, derived and join-key drift wrap; the `invalid_sql` whole-model drop does not wrap; SQLite temporal-over-TEXT reports nothing; persisted `DOUBLE` over a live list → re-type finding, and re-ingest re-types it with only `count`. Verify: they fail now (the existing read-set scenarios still pass).
- [x] 1.10 Write the `sql`-mode re-type tests on DuckDB and Postgres (bare, aliased, `*` and subquery projections; an untypeable projection falling back to the cursor): the validation finding, a failing query carrying it, and re-ingest re-typing in place with the element type and only `count`. Verify: they fail now.

## 2. Aggregation resolution and shadowing

- [x] 2.1 Introduce the single aggregation-definition resolver (exact, then unique case-fold, else ambiguity error) and route `aggregation_definition`, eligibility and binding through it. Verify: 1.2 case tests pass.
- [x] 2.2 Introduce the "built-in semantics iff no formula-backed definition" predicate and route every name-classification site through it (list typing/refusal, element typing, empty value, ranked dispatch, param order). Verify: 1.4 passes and the full suite stays green.
- [x] 2.3 Parser: keep the callee as written on parsed calls; emit deferred aggregation candidates (with the scalar's error attached) for scalar- or spelling-named calls that are ill-formed as scalars but well-formed as aggregation calls. Verify: parser unit tests, and no change for undefined names. Gotcha: `tests/test_dev2120_date_trunc.py` (`test_malformed_rejected`, `test_date_part_names_are_not_units`) expect `parse_expr` to raise; a deferred spelling error raises at bind instead, so check those still hold or move them to bind.
- [x] 2.4 Binding: resolve shadowing against the owning model; raise the attached scalar error when the name is not defined. Lift the `RESERVED_SCALAR_NAMES` ban in `Aggregation`. Verify: 1.2 passes.
- [x] 2.5 Add the structured `LibraryNameShadowWarning`; emit it on save and load, accumulate it during binding into the prepared warnings, and carry it through the cache-hit path. Verify: 1.3 passes.

## 3. Stored-upgrade migrations

- [x] 3.1 DatasourceConfig v2→v3: deterministic granularity rename map; cross-document reference rewrite in the storage load path with dependants persisted before the datasource; rename warnings; the save-time ban kept. Verify: 1.5 passes on both backends.
- [x] 3.2 SlayerModel v17→18, SlayerQuery v8→9, Memory v6→7: the `agg(distinct)` rewrite. Bump `CURRENT_VERSIONS`. Verify: 1.6 passes and the upgrade corpus tests stay green.
- [x] 3.3 Old-grammar gate in the string migration (old colon pre-pass + `ast.parse(mode="eval")`), with a warning naming the document and field. Verify: 1.7 passes.
- [x] 3.4 Shared recursive `None` removal applied after each backend's shape conversion for all four kinds. Verify: 1.8 passes.

## 4. Schema drift

- [x] 4.1 A typed per-model element read set from the plan (dependency closure, join keys, joins, measures, aggregations), carried next to the model read set. Verify: unit tests on read-set extraction.
- [x] 4.2 Element-level attribution in `_maybe_raise_schema_drift` (`schema_drift` whole-model drops only; model filters always read). Verify: 1.9 attribution tests pass, and the existing schema-drift tests stay green.
- [x] 4.3 Dialect capability `temporal_text_compatible` (SQLite) used by the bucket rule. Verify: the 1.9 SQLite temporal test passes.
- [x] 4.4 Extend the re-type finding and `_retyped_list` to every non-`ARRAY` type. Verify: the 1.9 re-type tests pass.
- [x] 4.5 `sql`-mode parity (system 17): result typing by sqlglot over the reflected live schema with the cursor fallback; the finding in `_collect_sql_diffs`; the re-ingest re-type of stored `sql`-mode models. Verify: 1.10 passes.

## 5. Architecture, docs, gates

- [x] 5.1 Apply the approved arc42 edits: 11.2 wording + `[enforced: test:tests/test_library_growth.py]`, and principle 14 "a non-list column". Verify: `poetry run la-arch-check` passes.
- [x] 5.2 Docs: `docs/concepts/schema-drift.md` (re-type finding, in-place re-type, error carrying the finding, element attribution, SQLite temporal, output shape), `docs/concepts/ingestion.md` (lines ~359/364), `docs/concepts/models.md` (§Schema versioning: versions 18/9/3/7, the upgrade list, shadow rule, granularity rename, `distinct`, the gate residue; the line-42 version and the aggregations reserved-name text), `docs/development.md` (one-sentence library-growth rule). Verify: grep finds no stale "never re-types" or old version numbers.
- [x] 5.3 Run the full unit suite, the CI integration suite invocation, `ruff`, `basedpyright` (ratchet) and `la-arch-check`. Verify: all green.
- [x] 5.4 Apply the approved arc42 principle 17; one sentence each in `docs/concepts/schema-drift.md` and `docs/concepts/ingestion.md` on `sql`-mode re-typing. Verify: `poetry run la-arch-check` passes.
