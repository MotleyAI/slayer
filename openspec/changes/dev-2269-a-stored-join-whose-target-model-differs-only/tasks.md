## 1. Tests (pr-tests)

- [x] 1.1 Save refusals for stored join targets — single case-only match (forward), target saved second (reverse, plus the respell-first remedy), ambiguous including an unchanged stored document, Unicode fold `STRASSE`/`straße`, dangling target accepted — and verify each fails on `main`
- [x] 1.2 Access-filtered saves: the case-only error precedes the unknown-join-target error in the view; a hidden peer's case-only target surfaces as `HiddenContentConflictError` without its name; the bypass path behaves as the inner store
- [x] 1.3 Version-23 upgrade on raw documents (YAML and SQLite stores): case-only target respelled and written back; ambiguous and dangling targets unchanged; repair precedes join-key canonicalization, key declaration and exact-inverse dedup; a second load is idempotent; a spine join is dropped with the warning
- [x] 1.4 Query-time backstop: an ambiguous stored target fails with `AmbiguousReferenceError` naming both candidates (SQLite and DuckDB)
- [x] 1.5 Spine respelling (SQLite and DuckDB): `TIME_SPINE` / `Time_Spine` in `source_model`, extension model, inline-source join target, and path heads in dimensions, time dimensions, measures, filters and order return the same rows and keys as `time_spine`; quoted `"TIME_SPINE"` fails as unresolved; a string literal stays unchanged; a stage named `Time_Spine` wins
- [x] 1.6 Reserved spine name and grandfathered variant: `time_spine` and `Time_Spine` new saves refused; a stored `Time_Spine` loads, saves and answers exact-spelled queries, each with the name-collision warning; `TIME_SPINE` is ambiguous in its datasource and not in another one
- [x] 1.7 Stored join into the spine refused at save in `time_spine` and `TIME_SPINE` spellings with `TimeSpineError`
- [x] 1.8 `core.syntax` path-head rewriter unit tests: only head identifiers change; quoted heads, string literals, nested calls and malformed text are preserved byte for byte

## 2. Storage

- [x] 2.1 Add `JoinTargetCaseError(SlayerError, ValueError)` to `slayer/core/errors.py`; verify tests 1.1 import it
- [x] 2.2 Forward and reverse case-only target checks, the ambiguity refusal and the spine-join refusal in `validate_for_save`; verify 1.1 and 1.7 pass
- [x] 2.3 Make the reserved-name check case-insensitive for names not already stored, and emit the name-collision warning on load and save of a stored spine variant; verify the save/load parts of 1.6 pass
- [x] 2.4 Order the case-only check before the unknown-join-target check in `AccessFilteredStorage.save_model`; verify 1.2 passes
- [x] 2.5 Bump `SlayerModel` to 23 with a stored-only no-op registry entry at 22, and add the async target respelling and spine-join drop in `_migrate_and_refine_on_load` (`pre_version < 23`) before the join-key repairs; verify 1.3 passes

## 3. Query side

- [x] 3.1 Add the `core.syntax` path-head rewriter; verify 1.8 passes
- [x] 3.2 Respell spine spellings once per query of the list at the engine entry, before population inference, applying stage precedence and recording the rewritten spellings; verify the non-ambiguous parts of 1.5 pass
- [x] 3.3 In the bundle build, check recorded spellings against the effective datasource's stored spine variants (ambiguity), emit the collision warning on responses reading a stored variant, and raise `AmbiguousReferenceError` for an ambiguous stored join target; verify 1.4 and 1.6 pass

## 4. Docs and gates

- [x] 4.1 One-sentence updates to the `docs/concepts/` models (join targets) and time-spine pages; verify `zensical.toml` nav is unchanged or updated
- [x] 4.2 Run `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline) and `poetry run la-arch-check`; verify all pass
