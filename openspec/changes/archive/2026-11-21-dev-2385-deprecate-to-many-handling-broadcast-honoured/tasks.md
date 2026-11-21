## 1. Tests (pr-tests)

- [x] 1.1 New test file for `queries/attribution-modes` › Broadcast is a deprecated mode: one test per scenario (fresh on Python / REST / MCP; three runs; two stages incl. a pruned one; never-broadcasting stage executed / dry run / explain; stored stage on every run with no load `FutureWarning`; refinements set / inherit / override; REST / MCP / memory saves persisting `"broadcast"`; concrete `partition_by` with the round trip on SQLite + DuckDB; aliased / repeated / filter-only / order-only call naming; schema enum + validation; other modes silent). Verify: the new tests fail for the expected reason before implementation.
- [x] 1.2 Tripwire: add the `broadcast` site to `PINNED` in `tests/test_law_removal_tripwire.py` and a test for the kept-form scenario (fresh run, save, reload, run). Verify: fails before implementation.
- [x] 1.3 `tests/test_mcp_documented_idioms.py`: align "Cross-model aggregate broadcast vs associate" to `sum(customers.credit, partition_by=[])` with no warning — needs the user's approval (existing test contradicting the plan). Verify: fails before the description/idiom change.
- [x] 1.4 Duplicate guards (D1): a fresh broadcast query run directly, staged, as a refined saved query and through a save reports each payload exactly once. Verify: fails before implementation.
- [x] 1.5 Gotcha: existing tests that pin exact warning lists on fresh-`"broadcast"` queries will gain one `deprecated_input` entry — collect them (run the local scope) and ask the user once, naming each file, before aligning.

## 2. Deprecation reporting (D1, D2)

- [x] 2.1 `slayer/core/deprecation.py`: let the marked `deprecated_input` emit its `FutureWarning` without the caller keeping a payload (still one marked call site, literal `removal="3.0"`). Verify: tripwire test passes with the new `PINNED` entry.
- [x] 2.2 `slayer/core/query.py`: emit the fresh-input `FutureWarning` for `"broadcast"` in `SlayerQuery` and `QueryRefinement` construction (no `version`, outside a stored document, after migration); keep no payload on the object. Verify: 1.1 "three runs" and "stored stage" `FutureWarning` counts.
- [x] 2.3 One function computing the broadcast payloads of a run's input stages (after refinement merge, effective mode, keyed by stage identity, emitted or pruned); the engine's response assembly (executed, dry run, explain; in process, REST, MCP, HTTP client) appends them. Verify: 1.1 run-reporting scenarios + 1.4.
- [x] 2.4 Saves (REST model save, MCP model save, memory save) report the same function's payloads on their responses, not the dry-run warning set; `"broadcast"` persists as given. Verify: 1.1 save scenario, 1.2 kept-form scenario.

## 3. Concrete replacement on the broadcast warning (D3)

- [x] 3.1 `slayer/ir/planned.py` / `slayer/engine/compile/stages.py`: the regroup attach carries the aggregate's original call text and its public `partition_by` spelling (attributable grain dims as the query spells them; time dim by field; `[]`). Verify: 1.1 concrete-partition and call-naming scenarios.
- [x] 3.2 `slayer/core/warnings.py`: `BroadcastGrainWarningPayload.partition_by: list[str]`; hint names the call and `partition_by=[…]`, keeps `DICE_SLICE_HINT`; `_collect_broadcast_warnings` fills them. Verify: same scenarios; existing `"associate" in w.hint.lower()` assertions still pass.

## 4. Not offered (D4, D5)

- [x] 4.1 Callable `json_schema_extra` on `SlayerQuery.to_many_handling` and `QueryRefinement.to_many_handling` publishing `["associate", "error"]`; unrecognized-value error names only those two. Verify: 1.1 schema scenario.
- [x] 4.2 Rewrite agent-facing text (also removing the stale DEV-1914 fallback wording): `SlayerQuery.to_many_handling` and metrics field descriptions (`partition_by=[]` repeats the total), `relationship_safety.py` unproven-hop message, `elaborate_env.py` re-aggregation suggestion, `UnrecognizedStrictValueError` text, `slayer/memories/help_content/06_relationships.md`. Verify: `grep -n broadcast` over these shows no offer of the mode; MCP description-budget tests pass.
- [x] 4.3 Docs: `docs/concepts/queries.md`, `datasets.md`, `expressions.md` drop the mode; `docs/reference/mcp.md`, `rest-api.md` list it as deprecated (3.0, `partition_by=`); `docs/upgrading/2.0.md` entry points to `partition_by=`; `examples/comparisons/matrix.yaml` `how` row (regenerate `matrix.json` if generated from it). Verify: docs build / nav check unaffected; idioms test (1.3) passes.

## 5. Architecture and gates

- [x] 5.1 Apply the approved `architecture/semantics.arc42.md` edits verbatim (design.md decisions log 5 and 6). Verify: `poetry run la-arch-check` green.
- [x] 5.2 Local test scope (`la-affected-tests --base main` + tests reading changed help/docs), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline). Verify: all green.
- [x] 5.3 Gate before merge: re-check the Ossie Metrics and Querying working group for an answer on a named `associate` / `broadcast` / `error` control (DEV-2221); if it standardises `broadcast`, stop and raise it with the user before merging.
