## 1. Tests first (pr-tests)

- [x] 1.1 Export tests for "Time buckets in metrics export as standard SQL" (`importers/osi-export` delta), one test per scenario, in a new `tests/test_osi_export_buckets.py`, built on `tests/_osi_export_helpers.py` (`make_env`, `export_datasets`, `reimport`, `rows`, `normalized`, `expression_sql`) and `tests/_osi_export_fixtures.py` (`orders`, `slayer_metric`). Round trips run with and without extensions on DuckDB and SQLite; the custom-granularity datasource is saved with `granularities` through `DatasourceConfig.model_validate` (a raw dict in `model_copy(update=…)` is not validated). Verify: each test fails today with the refusal or the wrong shape.
- [x] 1.2 Logical-type test: the exported DATE bucket expressions (`day`, `month`, `hour`) read with `osi.dialects.read`/`render` and evaluated on DuckDB and on Postgres via `pytest-postgresql` (`factories.postgresql_proc(port=None)`, TCP only in the sandbox: `-c unix_socket_directories=`), the session `SET TimeZone` to a non-UTC zone; pattern in `tests/test_dev2175_osi_postgres.py`. Unit test, not `integration`. Verify: fails today (metric refused).
- [x] 1.3 Renderer regression: under default `AliasFacilities`, an unslotted bucket still raises `RenderContextMissingFacilityError`; with leaf-only slotted kinds it renders over the slotted leaves. Verify: the second half fails today.
- [x] 1.4 SQLite registry test: every function a fresh SLayer SQLite connection registers is in the dialect's registry of SLayer-registered functions (compare against the names actually registered on the connection). Verify: fails today (no registry).
- [x] 1.5 Binding test: `bind_dataset_expression` with `granularities=` binds `date_trunc('fiscal_year', ordered_on)`; without it the call still raises `UnknownGranularityError`. Verify: fails today (no keyword).
- [x] 1.6 Grammar test for the `queries/date-functions` scenario "Bare part word", plus a bare non-part word (`status`) still refused. Verify: fails today.

## 2. Renderer and dialects

- [x] 2.1 `AliasFacilities` carries the key kinds it resolves only by slot (default: the five `_ALIAS_SLOTTED_KINDS`); `render_value_key` reads it; the bucket branch resolves a field source through the alias facility when one is present. Verify: 1.3 passes and the existing generator suites stay green.
- [x] 2.2 New `slayer/sql/dialects/ossie.py`: the Ossie document dialect, a `SqlDialect` subclass whose date addition emits `exp.DateAdd` (Snowflake writer: `DATEADD(DAY, n, x)`), keeping the base's typing casts around it. Verify: unit pin that `week_sunday` over a DATE and a TIMESTAMP renders with `DATEADD` and no `INTERVAL`.
- [x] 2.3 Apply the approved sql P2 wording to `architecture/python.sql.arc42.md` verbatim (design.md decision 4). Verify: `poetry run la-arch-check` green.
- [x] 2.4 SQLite: one registry of the functions SLayer registers on a connection (all `slayer_*` functions, the math overrides such as `floor`); connection setup registers from it; base dialect exposes an empty registry. Verify: 1.4 passes.
- [x] 2.5 `slayer/core/syntax.py`: `date_part`'s first argument accepts a bare part word or alias, folded to the same string part, as the `EXTRACT` rule does. Verify: 1.6 and the 1.1 extraction round trips pass on DuckDB and SQLite.

## 3. Exporter

- [x] 3.1 `bind_dataset_expression` gains optional `granularities` filling `ResolvedSourceBundle.granularities`; the exporter passes the datasource's definitions (converter call site unchanged). Verify: 1.5 passes.
- [x] 3.2 Exporter renders portable metrics with the Ossie dialect and passes leaf-only slotted kinds. Verify: 1.1 built-in-unit, FILTER, scalar, model-scoped, `week_sunday`, `date_add` and extraction tests pass.
- [x] 3.3 A measure whose native rendering is a dialect entry (`count_distinct_approx`, a custom-granularity bucket) exports as that entry only when the rendering binds as a SLayer expression and calls no function in the dialect's registry; otherwise SLayer-only with the existing report entry. Verify: 1.1 custom-granularity tests pass on DuckDB and SQLite (both SLayer-only), and the `count_distinct_approx` export suites stay green.
- [x] 3.4 `decompose` takes the datasource's custom granularities (part of its cache key); its callers pass them and `restore_metrics` binds with them. Verify: a unit test restoring a custom-bucket SLAYER formula keeps it (fails without the fix).

## 4. Docs and gates

- [x] 4.1 `docs/osi/osi_export.md`: one line each on bucket export shape, `week_sunday`/`date_add` as `DATEADD`, the custom-granularity rule with the SQLite exception; `docs/concepts/references.md`: one line that `date_part`'s part may be written bare. Verify: pages render in `zensical.toml` nav (already linked).
- [x] 4.2 Local test scope per `/la:pr` (`la-affected-tests --base main` plus the OSI export suites), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`, `npx -y likec4@1.47.0 validate architecture`. Verify: all green.
