# Tasks

## 1. Tests first (TDD — all initially failing, one per spec scenario)

- [x] 1.1 `tests/test_dev1929_column_granularity.py` — construction: temporal + granularity constructs; `string` type and default type with a granularity raise naming column, granularity and type; dict input with `"month"` string accepted; `model_dump` → `model_validate` and a YAML save/load keep the enum; fails until 2.1
- [x] 1.2 Same file — binder + checker: the `ModelScope` arm of `bind_time_dimension` returns the hand-set granularity for a bare column and for a dotted join path; `check_time_dimension_column`'s message names both granularities and the remedy and contains no "upstream stage"; fails until 2.2 / 2.3
- [x] 1.3 Same file — engine dry-run on SQLite storage: `create_model_from_query` at `month` yields cached `created_at` = MONTH with a temporal type and `rev` = None, identical after reload from storage; a raw projected `created_at` and a `created_at:max` output carry None; `yearly` over `monthly` carries YEAR; fails until 2.4
- [x] 1.4 Same file — executed on SQLite + DuckDB via `tests/_dev1471_fixtures.py`: the issue repro raises `TimeDimensionColumnError`; `month` over `monthly` equals `monthly`'s rows; `year` over `monthly` gives per-year sums; `day` over `yearly` rejected; hand-set DAY on `orders.created_at` rejects `hour` and executes at `day` and `month`; hand-set MONTH on a joined `customers` column rejects dotted `day` and binds at `year`; fails until 2.2 / 2.4
- [x] 1.5 Same file — sibling stand-ins: `ModelExtension` over a `month`-bucketed named stage rejects `day` and executes at `year`; a stage join to that sibling rejects the dotted `day` and binds at `month`; fails until 2.5
- [x] 1.6 Same file — ingestion: ingesting the seeded SQLite table leaves every column's granularity unset; green from the start (pins decision 4), keep as a regression pin
- [x] 1.7 Same file — MCP: `create_model` accepts a `granularity` column; `edit_model` sets `month`, preserves it on a description-only upsert, clears it on `null`, and rejects it on a `string` column with the invalid-column text; `inspect_model`'s JSON column payload shows `granularity` only when set; fails until 2.6
- [x] 1.8 `tests/_dev1871_raise_ledger.py` (approved edit): the re-bucketing row's message becomes the generalised text; verify the parity test is red until 2.3 and green after

## 2. Implementation

- [x] 2.1 `slayer/core/models.py`: `Column.granularity: TimeGranularity | None = None` with the cautionary description + after-validator rejecting it on a non-temporal type; verify 1.1 green
- [x] 2.2 `slayer/engine/binding.py`: the `ModelScope` arm of `_time_dimension_column_facts` returns `col.granularity`; docstring widened; `slayer/ir/bound.py` docstring widened; verify the binder cases in 1.2 green
- [x] 2.3 `slayer/engine/elaborate_env.py`: generalised message + docstring; verify the checker case in 1.2, 1.8 and `tests/test_dev1471_stage_time_dimensions.py` green
- [x] 2.4 `slayer/engine/query_engine.py` `_expand_query_backed_model`: `granularity=sc.granularity` on each cached `Column`; verify 1.3 and 1.4 green on SQLite + DuckDB
- [x] 2.5 `slayer/ir/source_bundle.py` `synthetic_model_from_stage_schema`: `granularity=c.granularity`; verify 1.5 green
- [x] 2.6 `slayer/mcp/server.py`: `granularity` named in the `create_model` and `edit_model` column-field docstrings (one clause each); `slayer/inspect/model_render.py`: `render_model_inspection` emits `granularity` in its JSON column payload when set (`_model_to_summary` is unused dead code — not the inspect surface); verify 1.7 green

## 3. Docs and spec hygiene

- [x] 3.1 One sentence each: `docs/concepts/models.md` (Column field table row; "What gets cached" notes the recorded bucket), `docs/concepts/queries.md` (the stage re-bucketing sentence widened to model columns carrying a granularity), `docs/concepts/terminology.md` (Column entry lists `granularity`); verify by grep that every Column field table lists the field
- [x] 3.2 `openspec validate dev-1929-query-backed-model-columns-carry-no-time-granularity-a-finer --strict` green after any wording adjustments made during implementation

## 4. Verification

- [x] 4.1 Full non-integration suite green: `poetry run pytest -m "not integration" -n auto`
- [x] 4.2 `poetry run ruff check slayer/ tests/`, `poetry run python tools/arch_check.py`, `poetry run basedpyright` (no new errors vs baseline), conventions gate clear; `legacy_arrows` / `guards` baselines unchanged
- [x] 4.3 Codex read-only pass over the working tree before the push; PR vs `main` opened with Egor's go-ahead
