## 1. Tests (pr-tests stage)

- [ ] 1.1 Write `tests/test_law_query_input_refuses_unknown_keys.py` per design decision 12 (reachable-model walk stopping at dataset-side models; forbid, validate_assignment, stray key → `UnknownKeyError` naming it, camelCase heal where eligible, pinned multi-word-key inventory, minimal-payload table that fails on a missing entry); verify it fails on `main` for `TimeDimension` and for the bare-`extra_forbidden` objects
- [ ] 1.2 Write tests for every scenario in `specs/compatibility/ossie-vocabulary/spec.md` and `specs/queries/time-dimensions/spec.md`, covering the entry paths of design decision 13 (dict, already-built instance, assignment incl. unchanged-after-failure state, refinement merge, storage round-trip incl. a migrated document, REST 422, MCP, Python client); verify each fails on `main` for the right reason
- [ ] 1.3 Ask the user's approval, naming each file and change, to align `tests/test_dev2102_extensions_ir.py:100,109` and `tests/test_dev1922_source_model_union.py:148-158` (they pin `extra_forbidden`); then align them

## 2. Unknown-key path

- [ ] 2.1 Add camelCase heal to spelling conversion in `slayer/core/vocabulary.py`, matched against each object's full accepted-spelling map and run before pre-2.0 conversion, with conflicts on the final canonical key raising `SpellingConflictError` (design 8); verify the heal and conflict scenarios pass
- [ ] 2.2 Replace `_refuse_unknown_entry_keys`' hard-coded tuples with one refusal derived from the model: accepted = model fields, listed = advertised input-schema properties (design 9); apply it to all seven objects after conversion, keeping `extra="forbid"`; add the `compareDateRange` hint and keep the `name` hint; verify the refusal, internal-key round-trip and Cube-key scenarios pass
- [ ] 2.3 Verify stored-query behaviour (heal + canonical dump; per-document load failure) through the storage load paths, including migrated documents; verify the stored-query scenario passes

## 3. Assignment

- [ ] 3.1 Add the atomic-assignment mixin (snapshot `__dict__` + fields-set, restore on `ValidationError`) and `validate_assignment=True` on all seven objects (design 10); verify the assignment scenarios and the law test pass

## 4. Architecture and docs

- [ ] 4.1 Add principle 8 to `architecture/python.core.arc42.md` §3 with the exact approved wording in design decision 4; verify `poetry run la-arch-check` passes
- [ ] 4.2 Update `docs/concepts/queries.md` (unknown keys refused, camelCase accepted), `docs/concepts/datasets.md:629` (forward-tolerance wording) and `docs/upgrading/2.0.md` (unknown query keys refused; assignments validated; stored `dateRange` now filters; stored junk keys stop that query loading); verify by reading the rendered sections

## 5. Gates

- [ ] 5.1 Run the local test scope (`la-affected-tests --base main` plus the storage, REST and MCP query tests), `poetry run ruff check slayer/ tests/`, `la-typecheck`, `poetry run la-arch-check` and `npx -y likec4@1.47.0 validate architecture`; verify all pass
