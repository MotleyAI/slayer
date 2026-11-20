## 1. Prerequisites

- [x] 1.1 Merge `origin/main` and the local DEV-2277 branch (`egor/dev-2277-qualified-references-every-name-is-dataset`, still in progress) into this branch (merge commits, never rebase). Verify: `git log` shows both merges.
- [x] 1.2 Before pr-implement, merge `origin/main` again, plus DEV-2277's latest state if it has moved (its branch or, once merged, `main`). Verify: `git log` shows the merges; `openspec validate dev-2373-stage-output-fields-are-always-typed-close-the --strict` still passes. Gotcha: at planning time `tests/test_docs_guards.py::test_every_doc_example_parses` failed on DEV-2277's WIP (`docs/concepts/references.md:129`), not on this change; recheck after the merge.
- [x] 1.3 Load `la:living-architecture`; run `la:make-refactor-target-compliant` over `slayer/engine/key_metadata.py`, `slayer/core/keys.py`, `slayer/engine/compile/projection.py` before changing signatures. Verify: `poetry run la-typecheck` reports no new errors.

## 2. Tests first (pr-tests)

Write scenarios with DEV-2277's qualified reference spelling (`sum(orders.amount)`, `s.col`). Executed tests run on in-process SQLite and DuckDB (no `integration` marker); rendered tests add Postgres.

- [x] 2.1 `queries/multi-stage` › Every stage column carries a type: one test per scenario. Verify: collected and failing for the right reason (FLOAT or `None` where the real type is expected).
- [x] 2.2 Per newly typed category (design decision 7, Codex #5) — arithmetic, scalar, transform (each op of design decision 15), `IN`, literal, regroup placeholder, cross-dataset aggregate, custom aggregate, NULL-only, builtin overridden by a formula: the stage column's type, the executed values on SQLite and DuckDB, and the rendered Postgres SQL's cast (or its absence, per the cast policy). Verify: failing for the right reason.
- [x] 2.3 `aggregations/formula-templates` › A custom aggregation's result type: one test per scenario, including the facade scenario and save-time validation; a round-trip test of `Aggregation.datatype` through storage and the Ossie export's SLayer extension. Verify: failing for the right reason.
- [x] 2.4 `aggregations/boolean-inputs` › the new scenario "A custom aggregation over a boolean takes its formula's type" and the updated facade scenario. Verify: failing for the right reason.
- [x] 2.5 `queries/time-dimensions` › "Untyped stage column fails closed" (now over a NULL-only, INT stage column). Verify: failing for the right reason.
- [x] 2.6 Registry merge (design decision 13): a key interned hidden first and later projected with an explicit query `type`, and with a saved metric `datatype`, carries the explicit type. Verify: failing for the right reason.
- [x] 2.7 Structural guarantees: constructing a `StageField` or a `ValueSlot` without a type raises a validation error; `dataset_from_stage_schema` has no fallback (a test that a stage dataset's field type equals its `StageField.datatype` for every field of a multi-stage query). Verify: failing for the right reason.
- [x] 2.8 List every EXISTING test that pins the old behaviour — at least `tests/test_dev2177_conditional_branches.py::test_untyped_stage_column_is_absorbed` (realign to real types), tests building `StageField` / `ValueSlot` without a type (mechanical), tests asserting the old custom-aggregation types (source type; `None` over a boolean), the existing test of `queries/time-dimensions` "Untyped stage column fails closed" (its scenario body now describes a NULL-only INT column), and goldens gaining casts — and get the owner's per-file approval before changing any of them.

## 3. Implementation

- [x] 3.1 Formula typer in `slayer/sql/` (design decisions 8, 9, 12): annotate a custom aggregation's formula with typed placeholder columns, booleans in numeric positions re-read as INT, map through the existing database-type → `DataType` mapping, UNKNOWN → `None`. Verify: the formula-type unit tests of 2.3 pass.
- [x] 3.2 `Aggregation.datatype` (optional `DataType`) in `slayer/core/models.py`; persists verbatim; carried by `slayer/osi/slayer_extension.py`. Verify: the round-trip and save-validation tests of 2.3 pass.
- [x] 3.3 Make `core/keys.py::value_type` total (design decisions 5, 11, 15): casts → target type; transforms per op; aggregates through `aggregation_result_type` with the formula hook and the chain declared → inferred → input type → INT over `*`; a NULL-only value INT at the slot while branch folding keeps absorbing NULL. Verify: tests 2.1, 2.2 pass for non-placeholder categories.
- [x] 3.4 Registry typing (design decision 13): `ValueRegistry` holds the typing context and types every fresh slot (hidden included); explicit types win and override inferred ones on merge; regroup placeholders resolve to their original key's type; the producer slots in `stages.py` use it with the bundle. Verify: tests 2.2 (placeholders), 2.6 pass.
- [x] 3.5 Retire the parallel derivations: delete or reduce `metric_key_type`, `aggregated_type`, `date_scalar_type` and the type part of `dimension_key_metadata` to calls into the derivation; bind-time `DeclaredMetric` types and the facade catalogue read it. Verify: tests 2.3 (facade), 2.4 pass; `grep` finds no other type derivation in `engine/key_metadata.py`.
- [x] 3.6 `ValueSlot.type` and `StageField.datatype` required; delete the FLOAT fallback in `dataset_from_stage_schema`. Verify: tests 2.5, 2.7 pass.
- [x] 3.7 Re-bless golden SQL; review each diff for added casts only. Verify: golden tests pass and every golden diff is a cast.
- [x] 3.8 Docs: custom aggregations (`datatype`, formula inference, fallback) and multi-stage (stage columns carry their real type) pages under `docs/`; MCP schema description of the aggregation `datatype`; check `zensical.toml` nav for any new page. Verify: `tests/test_docs_guards.py` passes.
- [ ] 3.9 Comment on DEV-2180: DEV-2373 retires the slot-typing derivations (`metric_key_type`, `aggregated_type`, `date_scalar_type`, the type part of `dimension_key_metadata`, the stage-producer typing); the check-side ones in `elaborate_env` remain. Verify: the comment is on the issue.

## 4. Gates

- [ ] 4.1 Local test scope per `/la:pr` (`la-affected-tests --base main` plus goldens), `poetry run ruff check slayer/ tests/`, `poetry run la-typecheck`, `poetry run la-arch-check`, `npx -y likec4@1.47.0 validate architecture`. Verify: all pass.
