## 1. Tests (pr-tests stage)

- [x] 1.1 Moved-module stubs: `MOVED_MODULES` gains `slayer.engine.syntax`, `slayer.core.formula` (→ `slayer.core.syntax`) and `slayer.osi.expression` (→ `slayer.importers.aggregates`); `PINNED` gains the three stub files at 3.0; verify they fail before the stubs exist
- [x] 1.2 Tripwire: `HELPERS` gains `deprecated_input`; `PINNED` gains `("slayer/core/query.py", "strict")` → 3.0; the alias test covers `deprecated_input` spellings; verify the pinned-set and next-major tests fail before the conversion exists
- [x] 1.3 Mechanism unit tests: one `DeprecatedInputFutureWarning` per call carrying a `DeprecatedInputWarning` payload (`old`, `new`, `removal`), attributed to the first frame outside `slayer`/`pydantic` (direct construction, `model_validate`, nested inside a `SlayerModel`); the payload round-trips through `AnySlayerWarning`
- [x] 1.4 `strict` conversion scenarios (`queries/attribution-modes` › "The strict flag is a deprecated alias"): true / legacy true tokens / false / `"off"` / explicit mode wins / unrecognized token typed error (fresh and stored) / malformed version is fresh / stored pre-v4 migrates silently / run-by-name top-level `strict` stays rejected
- [x] 1.5 Harvest scenarios (`compatibility/removal-tripwire` › "Deprecated input forms convert…"): built once run three times; root, one named stage, several stages, refinement (in process, REST, MCP run-by-name, reused across three runs); no Python re-emission from the response; `SlayerClient` sync and async over a mapping, a list of stages, a prebuilt query and a refinement
- [x] 1.6 Save scenarios: REST `POST`/`PUT /models` return `warnings` with the deprecation and with `narrowed_access` (`models/model-access` › "A REST model save returns the warning"); MCP `create_model`/`edit_model` list the deprecation; memory saves (MCP and REST) carry its message; rootless and staged query-backed models, access-filtered storage, and a reload proving no `strict` is persisted
- [x] 1.7 Align existing tests with the plan — needs owner approval naming each file: `tests/test_dev1841_mode_field.py::TestStrictRetired` (three rejection tests → conversion tests; `test_strict_field_is_gone` stays), `tests/test_dev1841_surfaces.py` both `test_strict_rejected` (→ conversion plus warning), `tests/test_saved_query_refinement.py::test_forbidden_keys_rejected` (drop the `("strict", True)` case)

## 2. Implementation (pr-implement stage)

- [x] 2.1 Add the three `raise_moved` stubs; verify 1.1 passes
- [x] 2.2 Add `DeprecatedInputWarning` to `core/warnings.py` and `AnySlayerWarning`; add `deprecated_input` and `DeprecatedInputFutureWarning` to `core/deprecation.py`; verify 1.3 passes
- [x] 2.3 Move the `strict` converter into `core`, used by `_query_v3_to_v4` and by `wrap` validators on `SlayerQuery` and `QueryRefinement`; add the dedicated `SlayerError` subclass; verify 1.2 and 1.4 pass
- [x] 2.4 Harvest in `_normalize_input()` (query objects + `SavedQueryRun.refinement`) into `SlayerResponse.warnings`; verify the in-process and REST/MCP parts of 1.5 pass
- [x] 2.5 `SlayerClient` keeps locally converted payloads and appends them to returned responses; verify the client part of 1.5 passes
- [x] 2.6 Harvest at the public `save_model()` entry; REST model saves pass `warnings_out` and return `warnings`; memory saves append messages; verify 1.6 passes
- [x] 2.7 Docs: remove the `strict` spelling from `docs/concepts/queries.md` (the `to_many_handling` row and the cross-model section), `docs/reference/mcp.md`, `docs/reference/rest-api.md`; document the REST save `warnings` field; verify `grep -rn strict docs/` hits only the guide
- [x] 2.8 Migration guide `docs/upgrading/2.0.md` per design.md › Migration guide, with the `zensical.toml` nav entry after Getting Started; reproduce each Part 1 entry on 1.x vs the 2.0 line before writing it, checking credentials for the BigQuery and pg-wire/Flight entries and omitting them if still unverifiable; verify the docs-guard test suite passes

## 3. Gates

- [x] 3.1 Full unit suite, `ruff check slayer/ tests/`, `basedpyright` (no new errors vs baseline), `la-arch-check`, `npx -y likec4@1.47.0 validate architecture`
- [ ] 3.2 Archive gate: archive this change only after DEV-2231's archive is merged forward into this branch; then re-run `openspec validate dev-2250-20-groundwork-moved-module-stubs-deprecated --strict`
