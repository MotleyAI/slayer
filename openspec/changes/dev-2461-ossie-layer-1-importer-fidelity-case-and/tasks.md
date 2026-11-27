## 1. Tests (pr-tests)

- [ ] 1.1 Write `tests/test_dev2461_importer_fidelity.py`, all through `import_osi`, checked against a raw-SQL oracle per group on DuckDB and SQLite, in a typed and an untyped document. Verify that the untyped SQLite cases fail before implementation and everything else passes. It covers:
  - the fifteen scalar / `CASE` forms (dataset metrics, model-scoped metrics and fields; the probe list is in design.md "Context");
  - the `ARRAY_AGG(DISTINCT x)` pins (DuckDB values, SQLite typed list error);
  - symmetric `FIRST_VALUE` / `LAST_VALUE` import scenarios (ascending, descending, explicit `NULLS FIRST`, NULL keys) against the oracle, differing from `first` / `last`.
- [ ] 1.2 Add the quoted-name tests: normalized match, exact beats normalized, ambiguity, a must-quote name, per-segment, result keys, strict `ossie-l3` (`E_NAME_NOT_FOUND` for `"Customers"`), source-column passthrough, and the round trip through YAML and SQLite stores and export. Verify the new-rule cases fail before implementation.
- [ ] 1.3 Add the table-driven `declared_name` parity test against the vendored `tests/fixtures/ossie_corpus/validation/validate.py` (design decision 12). Verify it fails before implementation.
- [ ] 1.4 Add the SQLite typing tests: the untyped REAL / INTEGER / expression fields aggregate, the ingestion-parity branches, and the empty-table expression staying String. Verify each fails or passes before implementation as the current behaviour predicts.
- [ ] 1.5 Add the corpus not-evaluable manifest to `tests/test_dev2243_corpus.py` (design decision 8). Verify it passes on today's code.
- [ ] 1.6 Align the existing tests listed in design decision 9, respelling each "wrong case" to one that still misses. Verify the aligned tests pass on today's code.

## 2. Quoted names (pr-implement)

- [ ] 2.1 Add the Ossie normalized form of a declared name to `slayer/core/names.py` (upstream `declared_name`), and make `match_name` / `match_path` fall back to it for a quoted segment, raising `AmbiguousReferenceError` when several names match. Verify the 1.2 and 1.3 tests pass.
- [ ] 2.2 Find every resolution path that compares a quoted name outside the matcher (strict `ossie-l3`, `slayer/engine/extensions.py`, scope lookups, stage and join names) and route it through the matcher. Leave physical source-column reads native. Verify the strict and per-segment tests pass.

## 3. SQLite unknown types (pr-implement)

- [ ] 3.1 Extract one SQLite classifier from `slayer/database/sqlite_introspect.py` (declared type refined by the bounded sample) and call it from both ingestion and unknown-type resolution for single-column fields. Expression fields get the bounded census in place of the zero-row probe. Keep it in the SQLite dialect / introspection modules (sql P2) and run it through the client (database P1). Verify the 1.4 tests pass and the existing ingestion tests stay green.

## 4. Docs and tracker

- [ ] 4.1 `docs/concepts/references.md` line 41: state the quoted-name rule as exact spelling first, then Ossie's normalized form, then the ambiguity error. Verify by rereading against the identifier-case spec.
- [ ] 4.2 `docs/osi/osi_import.md`: `CASE` and scalar functions around aggregates evaluate as written; `ARRAY_AGG(DISTINCT x)` reads as `collect(x)` (NULLs dropped, `[]` on no rows, sorted). Verify by rereading against the semantic-model-documents spec.
- [x] 4.3 Post the quoted-name ruling as a comment on DEV-2452 (done in pr-plan).

## 5. Gates

- [ ] 5.1 Run the local test scope (`la-affected-tests --base main` plus the corpus and aligned tests), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors) and `poetry run la-arch-check`. Verify all are green.
