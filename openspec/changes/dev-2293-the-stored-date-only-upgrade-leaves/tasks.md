## 1. Tests (pr-tests stage; all fail before the fix)

- [x] 1.1 New `tests/test_stored_upgrade_spine_date_only.py` (yaml + sqlite via `tests/_stored_upgrade_fixtures`: `raw_store`, `orders_v10(version=23)`, `query_backed_v10`, `memory_v2`): end-to-end load scenarios "Spine operand in a query on a stored model" (stored text after load AND the query's rows include Jan 31's bucket), "Spine spelt in another case" (byte-exact text), "Query rooted at the spine" (`time_spine` and `TIME_SPINE` sources); verify they fail on the current code
- [x] 1.2 Same file: "Spine operands in every stored document": model saved measure + Mode-B column filter (v23 model), nested source query (v24 model holding a v14 query), memory query with and without a datasource (v10), query extension column filter on `orders`, extension with `source_name: TIME_SPINE` filtering `timestamp`; and "Spine reached through an extension's join"
- [x] 1.3 Same file: "Spine reached from a joined model, with or without a time axis of its own": `customers` with a time column, axis-less `customers` reached by a to-one chain, and a detached axis-less model; each upgraded (design D2)
- [x] 1.4 Same file: "Spine spelling that is untypable" (grandfathered stored `Time_Spine` + `TIME_SPINE.timestamp`; `orders.time_spine.customers.created_at` crossing the spine): byte-identical and a logged warning (caplog) naming the document and text
- [x] 1.5 Same file: "Stored names shadow the spine": a stored join named `time_spine` to a TEXT-typed `code`; a sibling source query (stage) named `time_spine` (untyped, warned; design D3); a grandfathered stored model named exactly `time_spine` with a TEXT `timestamp` (no rewrite, no warning)
- [x] 1.6 Same file: "Legacy time literal on the spine": a pre-v4 query rooted at `time_spine` with a legacy literal for `2024-01-31` under `<=` is repaired and upgraded (design D4); check `slayer/storage/legacy_time_literals.py` for the legacy literal shape
- [x] 1.7 Unit tests of `StoredModels` in the same file: `host()` / `column_type()` resolve the spine under each spelling, honour exact stored names and stages, refuse the crossing, and read the spine's columns from `spine_model()` (no TIMESTAMP literal in storage)

## 2. Implementation (pr-implement stage)

- [ ] 2.1 `StoredModels`: declare the spine (dict derived from `spine_model()`) in name resolution unless a stored model is exactly `time_spine` (design D1); verify 1.1, 1.2, 1.7
- [ ] 2.2 `StoredModels._incident` / `_hop` / `column_type`: virtual spine edges both ways at target-name tier, stage spellings before target names, no hop past the spine (design D2, D3); verify 1.3–1.5
- [ ] 2.3 `StorageBackend._query_host` / `_stored_column_type`: resolve the source name through `StoredModels.host` with the query's stage names (design D4); verify 1.6
- [ ] 2.3a `StorageBackend._repair_source_query_filters`: run the nested-stage pass inside `migrating_document(<model name>, stored=True)`, so an unnamed stage's warning names its saved model; verify `test_sibling_stage_named_like_the_spine`
- [ ] 2.4 Run the local test scope (`la-affected-tests --base main` + `tests/test_dev2280_*`, `tests/test_stored_upgrade_*`, `tests/test_dev2269*` if present), `poetry run ruff check slayer/ tests/`, `la-typecheck`, `la-arch-check`; all green
