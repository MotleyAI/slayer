## 0. Re-plan housekeeping (pr-plan)

- [x] 0.1 Delete the abandoned plan's tests (`tests/test_ossie_validity_*.py`, `tests/_ossie_validity_fixtures.py`; recoverable at 94c4309f0); verify `git status` shows them removed

## 1. Tests first (pr-tests)

- [x] 1.1 Write repeated-key tests through `import-osi`, storage load of a hand-placed document, save and edit, in YAML and JSON: every repeated key listed with its line, nothing stored, the import's other files stored, exit non-zero; verify each fails on the current code
- [x] 1.2 Write upstream-loader parity tests (repeated key at depth, two `<<` merge directives allowed, merge override kept, JSON duplicate, keys equal by value `1`/`true`, unbuildable scalar `2024-02-30` and `!!int abc` with its line), each also run through upstream `validate.py` (skips without uv); verify SLayer's verdict matches upstream's on every case and the SLayer side fails on the current code
- [x] 1.3 Write the unbuildable-scalar import test: refused with value and line, exit non-zero, no traceback; verify it fails on the current code
- [x] 1.4 Write not-evaluable tests: a field `(SELECT 1)` and fields using `EXISTS`, `IN (SELECT …)`, `UNION` load with a warning naming the parse failure, shown by `inspect` and reported by `import-osi`, while other entities query normally and reading the entity raises the typed error; the form matrix (`sql` alone, portable entry alone, both, vendor-only, mixed); keywords inside literals stay evaluable; verify the warning tests fail on the current code
- [x] 1.5 Confirm the superset cases keep today's behaviour (a 129-character name, `customers.region` in an `orders` field, a model metric `FILTER` on another dataset, a core model metric naming another one: each stored and evaluated); verify they pass on the current code
- [x] 1.6 Align existing tests that pin last-wins keys or the raw `ValueError` for document expressions (ask approval file by file); verify the vendored corpus tests (`tests/test_dev2243_corpus.py`) still pass unchanged

## 2. Shared document reader

- [x] 2.1 Make `parse_document_text` the one document reader: compose, scan every mapping for repeated explicit keys (merge directives allowed), construct with unbuildable scalars reported by value and line; verify the 1.2 tests pass
- [x] 2.2 Route every document-text parse site through it (`slayer/osi/importer.py` `_file` and `_flat`, `slayer/storage/base.py` `_document_name`, the edit path in `slayer/core/document_text.py`); verify the 1.1 and 1.3 tests pass

## 3. Load-time evaluability

- [x] 3.1 At decomposition, run SLayer's expression reader on each field's and metric's runnable form and set a load warning naming the parse failure when it rejects it; verify the 1.4 tests pass and the 1.5 tests still pass

## 4. Docs and gates

- [x] 4.1 Update the Ossie import page (repeated keys and unbuildable scalars refuse; unparseable expressions load not evaluable) and add a `docs/upgrading/2.0.md` before/after for repeated keys; verify `zensical.toml` nav still lists every page
- [ ] 4.2 Run the local test scope, `poetry run ruff check slayer/ tests/`, `la-typecheck`, `la-check-conventions --base main` and `poetry run la-arch-check`; verify all pass
