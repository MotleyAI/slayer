## 1. Failing tests (spec-tests stage)

- [x] 1.1 `tests/test_dev1922_source_model_union.py`: schema pin (`source_model` is `anyOf: [{oneOf: [string, $ref ModelExtension, $ref SlayerModel]}, {type: null}]`); MCP `query` tool inputSchema `$defs` carries `ModelExtension` and `SlayerModel`; negatives (`42`, `[1, 2]`, `{"source_name": …, "filters": […]}`, `{"foo": 1}`) raise `ValidationError`; `{"source_name", "name", "sql_table", "data_source"}` raises naming the three foreign keys on the extension branch; extension dict → `ModelExtension` with `Column`/`ModelJoin` items; inline dict → `SlayerModel`; instances pass through by identity for `SlayerQuery` and `QueryRequest`; `model_copy(update={"source_model": …})` keeps a union member; python/JSON dump→validate round trips and the `exclude_none` minimal shape; `source_model_name` over the four cases; helper parity (`source_name_if_sibling`, `as_extension_over_nonsibling`, `follow_sibling_chain`, `_walk_spec`) with typed inputs. Verify: schema, negatives, both-keys, instance, property and helper tests fail before implementation; round-trip tests fail (dict survives today).
- [x] 1.2 Same file: `TimeDimension` — `{"column": …}` validates and dumps as `dimension`; `{"dimension": …}` and the `dimension=` keyword still validate; schema property is `dimension`, no `column`; through `SlayerQuery(time_dimensions=[…])`. Verify: the `column` cases fail before implementation.
- [x] 1.3 Same file: REST — `POST /query` with `{"source_model": {"source_name": "orders", "filters": […]}}` returns 422; the existing valid extension / inline-model bodies still 200. Verify: 422 case fails (returns 400) before implementation.
- [x] 1.4 Existing tests (consented): rewrite `tests/test_migrations.py::test_query_v1_to_v2_inline_model_extension`, `::test_query_v1_to_v2_inline_slayer_model_dict_is_left_for_model_migration` (renamed: the v1 inline dict is migrated into a current-version `SlayerModel` during query validation) and `::test_model_v1_to_v2_source_queries_with_inline_extension` to typed assertions. Verify: all three fail before implementation.

## 2. Core typing (`slayer/core/query.py`)

- [x] 2.1 `ModelExtension`: `extra="forbid"`, `columns: list[Column] | None`, `joins: list[ModelJoin] | None`. Verify: 1.1 negatives and typed-coercion tests green.
- [x] 2.2 `_source_spec_tag` + `SourceSpec` discriminated union (design decision 1); `source_model: SourceSpec | None` with the description rewritten to the three forms (no `filters`). Verify: 1.1 schema, both-keys, instance and round-trip tests green.
- [x] 2.3 Delete `_get_source_model_name`; add the `source_model_name` property; `strip_source_model_prefix` uses it. Verify: 1.1 property test green.
- [x] 2.4 `TimeDimension.dimension`: `Field(validation_alias=AliasChoices("dimension", "column"))`. Verify: 1.2 green.

## 3. Consumers

- [x] 3.1 `slayer/ir/source_bundle.py`: import `SourceSpec` from core (drop the local alias and its `__all__` entry); delete the dict arms in `apply_extension_overlay`, `source_name_if_sibling`, `spec_adds_measures`, `as_extension_over_nonsibling` (and the late `model_validate`); `follow_sibling_chain(spec: SourceSpec | None) -> SourceSpec | None`. Verify: helper-parity tests, `tests/test_source_bundle*.py` green.
- [x] 3.2 `slayer/engine/bundle_builder.py`: `_resolve_source_spec(spec: SourceSpec | None)` without the dict arm, raise tail kept. Verify: `tests/test_source_bundle_builder.py` green.
- [x] 3.3 `slayer/engine/plan.py`: delete `_coerce_extension`; branch 1 narrows with `isinstance(src, ModelExtension)`. Verify: multi-stage suites (`tests/test_dev1838_cte_body_lifts.py`, `tests/test_query_backed_*.py`) green.
- [x] 3.4 `slayer/engine/stage_ordering.py`: `_walk_spec(spec: SourceSpec | None, …)` as explicit three-type dispatch, dict and `getattr` arms deleted, NOSONAR comment dropped; `_extract_sibling_refs(query: SlayerQuery, …)`. Verify: `tests/test_topologically_order_stages.py` green (dict-form cases pass via coercion).
- [x] 3.5 `slayer/engine/schema_drift.py`: `_resolve_stage_source_to_base(source_model: SourceSpec | None, …)` via `isinstance`; `_stage_join_targets` / `_stage_extension_hops` typed access reading joins from `ModelExtension` and `SlayerModel`. Verify: schema-drift suites green.
- [x] 3.6 `slayer/engine/query_engine.py`: use `query.source_model_name`; `_collect_query_backed_base_names` without `getattr`. Verify: `tests/test_dev1866_reporting.py`, query-backed suites green.
- [x] 3.7 `slayer/memories/resolver.py`: typed dispatch, `getattr` fallback deleted. Verify: memories resolver tests green.
- [x] 3.8 `slayer/facade/translator.py`: `source_model: SourceSpec`. Verify: `tests/facade/test_translator.py` green.
- [x] 3.9 `slayer/api/server.py`: `QueryRequest.source_model: SourceSpec | None`, comment deleted. Verify: 1.3 green; `tests/test_api_server.py` green.

## 4. Docs and notebook

- [x] 4.1 `docs/examples/02_sql_vs_dsl/sql_vs_dsl.md` 15–28: extension column carrying the raw SQL, filtered by name in the DSL; prose adjusted. Verify: no `"filters"` key inside a `source_model` object anywhere under `docs/`.
- [x] 4.2 Notebook cells `b09911a1`, `e3947bac`, `920a227b` and the summary-table row rewritten to the same pattern with a threshold whose rows differ from cell `56baba38`; `poetry run jupyter nbconvert --to notebook --execute --inplace docs/examples/02_sql_vs_dsl/sql_vs_dsl_nb.ipynb`. Verify: executed outputs committed, filtered rows differ from the unfiltered cell.
- [x] 4.3 `docs/concepts/queries.md`: one sentence at §ModelExtension (unknown keys rejected) and one at §TimeDimension (`column` alias). Verify: sentences present; no nav change needed.

## 5. Baseline and gates

- [x] 5.1 Regenerate the basedpyright baseline; confirm the diff only removes entries. Verify: `poetry run basedpyright` green, `git diff .basedpyright/baseline.json` deletions only.
- [x] 5.2 `poetry run pytest -m "not integration" -n auto`; goldens byte-identical; `poetry run ruff check slayer/ tests/`; `poetry run python tools/arch_check.py`; `npx -y likec4@1.47.0 validate architecture`; `~/.claude/skills/process-reviews/scripts/check-conventions.sh`. Verify: all green.
- [x] 5.3 Codex pass over the working tree before the push. Verify: no open findings.
- [x] 5.4 With go-ahead: commit, push, open the PR against `main`. Verify: PR URL recorded here — https://github.com/MotleyAI/slayer/pull/401
