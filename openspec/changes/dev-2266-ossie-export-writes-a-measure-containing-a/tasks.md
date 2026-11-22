## 1. Tests first (pr-tests)

- [ ] 1.1 Export tests for "Time buckets in metrics export as standard SQL" (`importers/osi-export` delta), one test per scenario, in a new `tests/test_dev2266_osi_bucket_export.py`, built on `tests/_osi_export_helpers.py` (`make_env`, `export_datasets`, `reimport`, `rows`, `normalized`, `expression_sql`) and `tests/_osi_export_fixtures.py` (`orders`, `slayer_metric`). Round trips run with and without extensions on DuckDB and SQLite; the custom-granularity datasource is saved with `granularities` through `DatasourceConfig.model_validate` (a raw dict in `model_copy(update=…)` is not validated). Verify: each test fails today with the refusal or the wrong shape.
- [ ] 1.2 Logical-type test: the exported DATE bucket expressions (`day`, `month`, `hour`) read with `osi.dialects.read`/`render` and evaluated on DuckDB and on Postgres via `pytest-postgresql` (`factories.postgresql_proc(port=None)`, TCP only in the sandbox: `-c unix_socket_directories=`), the session `SET TimeZone` to a non-UTC zone; pattern in `tests/test_dev2175_osi_postgres.py`. Unit test, not `integration`. Verify: fails today (metric refused).
- [ ] 1.3 Import tests for the `importers/osi-import` delta scenarios (date truncation, date addition, the three extraction spellings, `dayofweek` keeps the helper) through `convert_document` / `reimport` on SQLite, asserting the stored formula, the absence of hidden helper fields, and the values. Verify: fail today (helper field created).
- [ ] 1.4 Renderer regression: under default `AliasFacilities`, an unslotted bucket still raises `RenderContextMissingFacilityError`; with leaf-only slotted kinds it renders over the slotted leaves. Verify: the second half fails today.
- [ ] 1.5 SQLite registry test: every function a fresh SLayer SQLite connection registers is in the dialect's registry of SLayer-registered functions (compare against the names actually registered on the connection). Verify: fails today (no registry).
- [ ] 1.6 Binding test: `bind_dataset_expression` with `granularities=` binds `date_trunc('fiscal_year', ordered_on)`; without it the call still raises `UnknownGranularityError`. Verify: fails today (no keyword).

## 2. Renderer and dialects

- [ ] 2.1 `AliasFacilities` carries the key kinds it resolves only by slot (default: the five `_ALIAS_SLOTTED_KINDS`); `render_value_key` reads it; the bucket branch resolves a field source through the alias facility when one is present. Verify: 1.4 passes and the existing generator suites stay green.
- [ ] 2.2 New `slayer/sql/dialects/ossie.py`: the Ossie document dialect, a `SqlDialect` subclass whose date addition emits `exp.DateAdd` (Snowflake writer: `DATEADD(DAY, n, x)`), keeping the base's typing casts around it. Verify: unit pin that `week_sunday` over a DATE and a TIMESTAMP renders with `DATEADD` and no `INTERVAL`.
- [ ] 2.3 Apply the approved sql P2 wording to `architecture/python.sql.arc42.md` verbatim (design.md decision 4). Verify: `poetry run la-arch-check` green.
- [ ] 2.4 SQLite: one registry of the functions SLayer registers on a connection (all `slayer_*` functions, the math overrides such as `floor`); connection setup registers from it; base dialect exposes an empty registry. Verify: 1.5 passes.

## 3. Exporter

- [ ] 3.1 `bind_dataset_expression` gains optional `granularities` filling `ResolvedSourceBundle.granularities`; the exporter passes the datasource's definitions (converter call site unchanged). Verify: 1.6 passes.
- [ ] 3.2 Exporter renders portable metrics with the Ossie dialect and passes leaf-only slotted kinds. Verify: 1.1 built-in-unit, FILTER, scalar, model-scoped, `week_sunday`, `date_add` and extraction tests pass.
- [ ] 3.3 A measure containing a custom-granularity bucket exports as a datasource-dialect entry (the `count_distinct_approx` path), unless its native rendering calls a function in the dialect's registry, then SLayer-only with the existing report entry. Verify: 1.1 custom-granularity tests pass on DuckDB and SQLite.

## 4. Importer

- [ ] 4.1 `InlineOperands._inline` carries `TimestampTrunc`/`DateTrunc` (Ossie's eight parts) → `date_trunc('<part>', x)`, `Cast` (every Ossie target) → `CAST(x AS T)`, `DateAdd` (parts `date_add` supports) → `date_add(x, n, '<part>')`, `Extract`/`Year`/`Quarter`/`Month`/`Day`/`DayOfYear`/`Hour`/`Minute`/`Second` (parts year, quarter, month, day, dayofyear, hour, minute, second) → `date_part('<part>', x)`; any other part keeps the helper path. Verify: 1.3 passes, and 1.1/1.2 round trips pass on SQLite.

## 5. Docs and gates

- [ ] 5.1 `docs/osi/osi_export.md`: one line each on bucket export shape, `week_sunday`/`date_add` as `DATEADD`, the custom-granularity rule with the SQLite exception; `docs/osi/osi_import.md`: one line on the date functions carried into formulas. Verify: pages render in `zensical.toml` nav (already linked).
- [ ] 5.2 Local test scope per `/la:pr` (`la-affected-tests --base main` plus the OSI export/import suites), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`, `npx -y likec4@1.47.0 validate architecture`. Verify: all green.
