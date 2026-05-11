# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## What is SLayer?

SLayer (Semantic Layer) is a lightweight, open-source (MIT) semantic layer for AI agents, built by MotleyAI. Instead of writing raw SQL, agents describe what data they want — measures, dimensions, filters — and SLayer generates and executes the query.

Default API port: **5143**.

When generating SLayer query examples or answering questions about SLayer syntax and capabilities, always read the documentation files in `docs/` first (especially `docs/concepts/queries.md`, `docs/concepts/formulas.md`, `docs/concepts/models.md`, and `docs/examples/`) to understand the current syntax and features.

## Common Commands

```bash
# Install with all extras
poetry install -E all

# Run unit tests (excludes integration tests)
poetry run pytest

# Run SQLite integration tests
poetry run pytest tests/integration/test_integration.py -m integration

# Run Postgres integration tests (auto-spawns temp Postgres via pytest-postgresql)
poetry run pytest tests/integration/test_integration_postgres.py -m integration

# Run DuckDB integration tests (no Docker, runs in-process)
poetry run pytest tests/integration/test_integration_duckdb.py -m integration

# Run a specific test file
poetry run pytest tests/test_sql_generator.py -v

# Start API server (uses platform default storage path, override with --storage)
poetry run slayer serve

# Start MCP server
poetry run slayer mcp

# Lint
poetry run ruff check slayer/ tests/
```

## Key Conventions

- Python 3.11+, Pydantic v2 for all models
- Use `poetry run` for all Python commands
- Use keyword arguments for functions with more than 1 parameter
- Imports at the top of files
- SQL generation uses sqlglot AST building (not string concatenation)
- **Two reference modes** (DEV-1369): SLayer has exactly two expression layers and the rules differ by design. **Mode A (SQL)** covers `Column.sql`, `Column.filter`, and `SlayerModel.filters` — sqlglot-parsed free SQL accepting any function call (`json_extract`, `coalesce`, `CASE WHEN`, …), bare names referencing the underlying table, and `__`-delimited join paths (`customers__regions.name` — `__` between hops, single dot before the leaf). **Mode B (DSL)** covers `ModelMeasure.formula`, `SlayerQuery.measures`, `SlayerQuery.filters`, and every other query field — Python-AST DSL accepting only `Column` / `ModelMeasure` references, single-dot dotted paths through joins, aggregation colon syntax (`revenue:sum`, `*:count`), transform calls, and arithmetic/boolean ops. DSL mode rejects raw SQL function calls, `__` in user input, and bare names that don't resolve. The internal carve-out: `Column.name` accepts `__` because `_query_as_model` flattens joined columns into virtual-model columns like `stores__name`. Single source of truth: [docs/concepts/references.md](docs/concepts/references.md). Predicate-promotion (DEV-1336) is removed — a query filter naming a `Column` whose `sql` contains a window function now raises with a suggestion to use a rank-family transform (`rank(<measure>) <= N`, etc.).
- Models, columns, measures (formulas), and aggregations have an optional `meta: Dict[str, Any]` field for arbitrary user-defined JSON metadata. Persisted in storage, editable via MCP (`edit_model`), HTTP API, and CLI. `inspect_model` renders `meta` for any entity that has it set; the column is auto-pruned when no entity in the section uses meta.
- **Schema versioning**: `SlayerModel`, `SlayerQuery`, and `DatasourceConfig` carry a `version: int` (currently `6` for `SlayerModel`, `3` for `SlayerQuery`, `1` for `DatasourceConfig`). On load, older versions are upgraded via the converter chain in `slayer/storage/migrations.py` before Pydantic validates the dict. Saves always emit the current version. The hook is on the Pydantic class itself (`@model_validator(mode="before")`), so every storage backend — YAML, SQLite, third-party backends, plus MCP/API/dbt entry points — gets migrations automatically without backend changes. The v1→v2 converter (in `slayer/storage/v2_migration.py`) merges v1 `dimensions`+`measures` into v2 `columns`, repurposes `measures` to hold `ModelMeasure` formulas, and renames `SlayerQuery.fields`→`measures`. The v2→v3 converter (in `slayer/storage/v3_migration.py`) drops the legacy `dry_run`/`explain` fields from `SlayerQuery` (they are now engine kwargs only — `engine.execute(query, dry_run=..., explain=...)`) and walks `SlayerModel.source_queries` entries through the SlayerQuery chain. The v3→v4 converter (in `slayer/storage/v4_migration.py`) requires non-empty `data_source` on table-backed SlayerModel dicts (`sql_table` or `sql` mode); query-backed models (`source_queries` set) are exempt because their `data_source` is filled by `engine._validate_and_populate_cache` from the resolved virtual model before save. The v4 converter also ships layout migrators that move legacy `models/<name>.yaml` flat files into `models/<data_source>/<name>.yaml` and rebuild the SQLite `models` table with a composite `(data_source, name)` primary key. The v4→v5 converter (in `slayer/storage/v5_migration.py`, DEV-1361) coarse-renames `Column.type` legacy values to the new sqlglot-aligned vocabulary (`string→TEXT`, `number→DOUBLE`, `time→TIMESTAMP`, etc.) and strips the dead aggregation pseudo-types (`count`/`sum`/...). The v5→v6 converter (in `slayer/storage/v6_migration.py`, DEV-1375) is a no-op forward — v6 introduces a single new optional field, `Column.sampled: Optional[str]`, that caches the per-column sample-value snapshot consumed by [`search`](docs/concepts/search.md) and `inspect_model`. Storage backends additionally introspect each model's datasource on first load and refine `DOUBLE → INT` for base columns whose live SQL type is integer (`slayer/storage/type_refinement.py`); the refined model is written back so subsequent loads skip both steps. `SlayerQuery` v3 sets `extra="forbid"` so unknown fields raise. See [docs/concepts/models.md](docs/concepts/models.md#schema-versioning).
- **DataType / CAST emission** (v5, DEV-1361): `DataType` values match sqlglot's `exp.DataType.Type` byte-for-byte: `TEXT`, `INT`, `DOUBLE`, `BOOLEAN`, `DATE`, `TIMESTAMP`. Auto-ingestion narrows integer DB types (INTEGER/BIGINT/SERIAL/INT8…/UINT8…) to `INT`, and floats/numerics (FLOAT/DOUBLE/DECIMAL with scale>0) to `DOUBLE`; NUMERIC(p,0) and DECIMAL(p,0) are integer-shaped and narrow to `INT`. The SQL generator wraps **non-bare** `Column.sql` (function calls, arithmetic, CASE WHEN) in `CAST(... AS <type>)` driven by `Column.type`; bare identifiers and `sql=None` are emitted unchanged. `TEXT` is a no-op (skipped, since `CAST AS TEXT` is cosmetic and doesn't unwrap SQLite's JSON-quoted strings anyway). `ModelMeasure.type` (also `Optional[DataType]`) declares the formula's result type; when set, the aggregation expression is wrapped in an outer CAST. Lenient `before`-validators on `Column.type` and `ModelMeasure.type` absorb legacy lowercase agent input (`"string"` → `TEXT`, `"number"` → `DOUBLE`, etc.) and silently drop dropped pseudo-types. `slayer storage migrate-types --dry-run [--data-source X]` runs the storage refinement step explicitly for batch / inspectable usage. Schema-drift detection (`data_type_bucket`) keeps `INT` and `DOUBLE` in the same `"number"` bucket so a `DOUBLE`-typed persisted column does not flag as drift against an `INT` live column.
- **Datasource-scoped storage** (v4, DEV-1330): Models are keyed by `(data_source, name)`, not bare `name`. Two datasources can share a table name without collision. `storage.get_model(name, data_source=None)` and `storage.delete_model(name, data_source=None)` resolve bare names by: (1) returning the unique match if exactly one model has that name, (2) walking `storage.get_datasource_priority()` to pick the first datasource in the priority list that has the name, (3) raising `slayer.core.errors.AmbiguousModelError` otherwise. The priority list is configured via `storage.set_datasource_priority(["db_a", ...])`, the MCP `set_datasource_priority` tool, the REST `PUT /datasources/priority`, or the `slayer datasources priority` CLI subcommand. `engine.execute(query, data_source=...)` passes a hint that wins over the priority list. Joins resolve targets within the parent model's `data_source` only — cross-datasource joins are not auto-mirrored. A sibling Linear issue ([DEV-1342](https://linear.app/motley-ai/issue/DEV-1342)) tracks whether to add `datasource.model_name` dot syntax inside query strings.
- **Source modes**: a `SlayerModel` has exactly one source: `sql_table` (physical table), `sql` (explicit SQL subquery), or `source_queries` (query-backed: `List[SlayerQuery]`). Validators enforce mutual exclusivity, reject empty `source_queries=[]`, require `name` on every non-final stage, and reject duplicate stage names. `SlayerModel.source_queries` entries given as dicts are auto-parsed into `SlayerQuery` instances by a Pydantic before-validator.
- **Stages form a DAG, not just a chain**: any stage in `source_queries` (or in a runtime query list) may use a *prior* named sibling as `source_model` or as `joins.target_model`. Forward references and self references are rejected at resolve time with a clear message naming the offending stage (`Stage 'a' cannot reference stage 'b': forward references are not allowed.`). Scoping is implemented in `_query_as_model` via `_scope_named_queries_to_prior` plus a per-task `_forbidden_sibling_refs_var` `ContextVar` that `_resolve_model_inner` and `_resolve_join_target` consult on lookup miss.
- **Query-backed models**: `SlayerModel.query_variables: Dict[str, Any]` provides defaults for `{var}` placeholders in `source_queries`. `engine.create_model_from_query(query, name, variables=..., save=True)` saves a query as a query-backed model and populates the `columns` + `backing_query_sql` cache. `engine.save_model(model)` is the engine-side save helper that runs source-mode validation + cache refresh + persistence; user-supplied `columns` and `backing_query_sql` on a query-backed model are rejected at save with a clear error. The cache is refreshed only on save paths (`engine.save_model` / `create_model_from_query(save=True)`); `engine.execute` never writes to storage, even on stale or empty caches (closes #74 — `tests/test_query_backed_models.py::test_execute_never_writes_to_storage` pins this).
- **Run-by-name execution**: `engine.execute(str, variables=..., dry_run=..., explain=...)` and `execute_sync(str, ...)` run the stored backing query for a query-backed model. Errors surface as `Model '<name>' not found` or `Model '<name>' is not query-backed; pass a SlayerQuery with source_model='<name>'.`. Variable precedence (highest first): runtime kwarg > stage > outer query > model defaults. The `variables=` kwarg works uniformly for str, dict, SlayerQuery, and list inputs. Runtime kwargs are merged into the available variable set; extra keys not referenced by any `{var}` placeholder simply remain unused. `dry_run`/`explain` are engine kwargs (not query fields) and apply to every input shape. Surfaced via REST `POST /query` with `{"name": "...", "variables": {...}}`, MCP `query` tool with `variables=`, CLI `slayer query <model_name> --variables k=v`.
- **Unified columns** (v2): `SlayerModel.columns: List[Column]` replaces v1's separate `dimensions` and `measures`. A `Column` carries name, sql, type (`DataType`), `primary_key`, `description`, `label`, `hidden`, `format`, `allowed_aggregations` (whitelist), `filter` (CASE-WHEN at aggregation time), `meta`. What a column is "used as" (group-by dim vs aggregation source) is decided per query.
- **Measures are named formulas**: `SlayerModel.measures: List[ModelMeasure]` is a library of saved formulas of shape `{formula, name, label, description}`. Same shape as the inline `SlayerQuery.measures` entries. Queries can reference them by bare name (`{formula: "aov"}`) or expand them inline.
- **Aggregations are query-time**: specified via **colon syntax** in formulas — `"revenue:sum"`, `"*:count"`, `"price:weighted_avg(weight=quantity)"`, `"price:corr(other=quantity)"`. Built-in aggregations: sum, avg, min, max, count, count_distinct, first, last, weighted_avg, median, percentile, stddev_samp, stddev_pop, var_samp, var_pop, corr, covar_samp, covar_pop. Custom aggregations defined at model level in `aggregations` list.
- **`*:count`** for COUNT(*) — `*` means "all rows", `count` is just a regular aggregation. `col:count` = COUNT(col) for non-nulls.
- Columns can have `allowed_aggregations` whitelist — validated at model creation and query time. Primary-key columns are always restricted to `count`/`count_distinct` regardless of type. Default eligibility per data type lives in `slayer/core/enums.py:DEFAULT_AGGREGATIONS_BY_TYPE`.
- Auto-ingestion emits one `Column` per non-joined column. PK columns get `primary_key=True`. Columns named "count" rename to "count_col" to avoid clashing with `*:count`.
- **Idempotent auto-ingestion** (DEV-1356): `slayer ingest` / `ingest_datasource_models` MCP tool / `POST /ingest` REST endpoint are idempotent by default. Re-runs are additive only — new columns/joins/tables are appended; existing column metadata (`description`, `label`, `format`, `meta`, `allowed_aggregations`) is never overwritten. `sql`-mode and query-backed models are skipped silently. The return shape is `IdempotentIngestResult(additions, to_delete, errors)` where `to_delete` is the verbatim `validate_models` output (so type drift surfaces in the same call). Implemented in `slayer/engine/ingestion.py:ingest_datasource_idempotent`.
- **Schema-drift validation** (DEV-1356): `engine.validate_models(data_source=None)` returns the minimal list of deletes (`EditModelDelete` / `WholeModelDelete`) needed for SQL generation to remain valid against the live schema. Read-only — never writes to storage. Surfaced as MCP tool `validate_models`, REST `POST /validate-models`, and CLI `slayer validate-models [--datasource X]`. Compares persisted columns/types/joins to live introspection via SQLAlchemy `Inspector` (sql_table mode) or trial-execute cursor metadata (sql mode). Type comparison uses coarse buckets (`number`/`string`/`boolean`/`temporal`); INTEGER↔FLOAT and DATE↔TIMESTAMP collapse. PK drops do not cascade. Cascade walking stays within the parent datasource. FK introspection limitations: ClickHouse, BigQuery, Snowflake don't expose FK metadata via `Inspector` — joins on those backends must be defined manually.
- **`SchemaDriftError`** (DEV-1356): when `engine.execute()` raises a DBAPI error, the engine attempts to attribute it via `validate_models` against the touched models' datasources. If drift is found, raises `SchemaDriftError(models, to_delete, original)` (with `original` as `__cause__`). Healthy queries pay zero overhead. REST translates to HTTP 422 with `{"error": "schema_drift", "models": [...], "to_delete": [...], "original": "..."}`.
- **`apply_drift_deletes`** (DEV-1356): `await engine.apply_drift_deletes(deletes)` applies each entry via `engine.edit_model_remove` / `engine.delete_model_by_name` and returns `ApplyDriftResult(applied, errors, residual)`. Per-entry failures are captured; processing continues. Surfaced **only** via `slayer validate-models --force-clean [--yes]` — destructive auto-application is opt-in at the CLI layer. Not exposed via MCP or REST.
- Queries support `measures` (renamed from `fields` in v2) — list of `{"formula": "...", "name": "...", "label": "..."}` parsed by `slayer/core/formula.py`. `label` is an optional human-readable display name (also supported on `ColumnRef` and `TimeDimension`).
- **Result column naming**: `revenue:sum` → `orders.revenue_sum` (colon becomes underscore). `*:count` → `orders._count` — the `*` is dropped but the underscore is kept as a leading marker so the alias never collides with a user-defined column literally named `count`. When converting queries to models (`create_model_from_query`), the same colon-to-underscore mapping applies. An explicit `name` on the measure spec overrides the canonical form for **both** simple aggregations and arithmetic/transform formulas — `{"formula": "amount:sum", "name": "rev"}` surfaces as `orders.rev`. This matters most for inner stages of multi-stage `source_queries`, where downstream stages reference inner-stage outputs by the chosen name.
- **Response attributes**: `SlayerResponse.attributes` is a `ResponseAttributes` with `.dimensions` and `.measures` dicts, each mapping column alias → `FieldMetadata(label, format)`. Split by type so consumers can distinguish dimension metadata from measure metadata.
- Available formula transforms: cumsum, time_shift, change, change_pct, rank, percent_rank, dense_rank, ntile, first (FIRST_VALUE window ASC), last (FIRST_VALUE window DESC), lag, lead, consecutive_periods. time_shift uses a self-join CTE where the shifted sub-query has the time column expression offset by INTERVAL (calendar-based, gap-safe). change and change_pct are desugared at enrichment time into a hidden time_shift + arithmetic expression. lag/lead use LAG/LEAD window functions directly (more efficient but produce NULLs at edges). Non-transform SQL function calls (`nullif`, `coalesce`, `ln`, `sqrt`, etc.) may also wrap aggregated refs inside arithmetic expressions, e.g. `"*:count / nullif(revenue:max, 0)"` — the call passes through to emitted SQL while the inner refs resolve to their measure aliases
- **Rank-family transforms** (DEV-1353): `rank`, `percent_rank`, `dense_rank`, and `ntile` are timeless window-function transforms emitted as `RANK() / PERCENT_RANK() / DENSE_RANK() / NTILE(n) OVER (... ORDER BY <measure> DESC)`. They default to **no `PARTITION BY`** (rank across the entire result set, unlike cumsum/lag/lead which auto-partition by query dimensions), and accept an optional `partition_by=col` or `partition_by=[col1, col2]` kwarg to opt into per-partition ranking; the columns referenced must be query dimensions or time dimensions. `ntile` additionally requires `n=<positive int>`. Standard SQL across SQLite (≥3.25), Postgres, DuckDB, MySQL, and ClickHouse — no UDFs needed.
- Filters can reference computed field names or contain inline transform expressions (e.g., `"change(revenue:sum) > 0"`, `"last(change(revenue:sum)) < 0"`). These are auto-extracted as hidden fields and applied as post-filters on the outer query
- **Window functions in filters**: filter strings and `ModelMeasure.formula` cannot contain raw `OVER (...)` SQL — SLayer's formula parser is Python-AST-based and rejects with an actionable error pointing at the `rank()` / `first()` / `last()` / `lag()` / `lead()` transforms. Filtering on a `Column` whose `sql` contains a window function is also rejected (DEV-1369; the prior auto-promotion escape hatch from DEV-1336 is removed). For top-N use the inline `rank(<measure>) <= N` transform (or `dense_rank` / `percent_rank` / `ntile(n=<N>)`); for non-standard window expressions, factor them into an earlier stage of a multi-stage `source_queries` model.
- Filters support `{variable}` placeholders substituted from `query.variables: Dict[str, Any]`. Values must be str/number, inserted as-is. `{{`/`}}` for literal braces. Undefined variables raise errors.
- Models can have explicit `joins` to other models (LEFT JOINs). Cross-model measures use dotted syntax with colon aggregation (`customers.revenue:sum`) and multi-hop (`customers.regions.name`). Joins are auto-resolved by walking the join graph. Transforms work on cross-model measures (`cumsum(customers.revenue:sum)`)
- **Path-based table aliases**: Joined tables use `__`-delimited path aliases in SQL to disambiguate diamond joins. In queries, dots denote paths (`customers.regions.name`); in model SQL definitions, `__` denotes the table alias (`customers__regions.name`). For diamond joins (same table reached via different paths, e.g., `orders → customers → regions` AND `orders → warehouses → regions`), each path gets a unique alias (`customers__regions` vs `warehouses__regions`). Auto-ingestion creates only direct joins (one per FK on the source table); multi-hop paths are resolved at query time by walking each intermediate model's own joins
- `SlayerQuery.source_model` accepts a model name, inline `SlayerModel`, or `ModelExtension` (extends a model with extra `columns`/`measures` formulas/`joins`). `create_model_from_query()` saves a query as a permanent model
- Models can have `filters` (always-applied WHERE conditions, e.g., `"deleted_at IS NULL"`)
- **Core principle**: adding a measure/field must never affect result cardinality or other fields' values — achieved via CTEs, sub-queries, and correct JOIN dimensions
- Functions needing time ordering: single time_dimensions entry is used automatically; with 2+ time dimensions, `main_time_dimension` disambiguates (or model's `default_time_dimension` if among query's time dims); with none, falls back to model default
- SlayerModel has optional `default_time_dimension` field for time-dependent formula resolution
- SQLite dialect uses STRFTIME instead of DATE_TRUNC (handled automatically by sqlglot)
- See "Database Support" section below for dialect tiers and testing expectations
- Result column keys use `model_name.column_name` format (e.g., `"orders._count"` for `*:count`, `"orders.revenue_sum"` for `revenue:sum`). For multi-hop joined dimensions, the full path is included: `"orders.customers.regions.name"`
- Datasource configs support `${ENV_VAR}` references resolved at read time
- Integration tests are marked with `@pytest.mark.integration` and skip when DB is unavailable
- NEVER use dataclasses, if you want to use dataclasses, use Pydantic classes instead. 

- **Memories + semantic search** (DEV-1357 + DEV-1375): An agent-memory layer indexed by canonical entity strings. Two write-side tools — `save_memory(learning, linked_entities)` and `forget_memory(id)` — record per-entity notes (optionally bundled with an example `SlayerQuery`). Retrieval is unified into a single `search(entities, query, question, max_memories=5, max_example_queries=2, max_entities=5)` tool — there is no separate `recall_memories` surface. `linked_entities` accepts either a list of entity strings (resolved strictly) or an inline `SlayerQuery`/dict (entities auto-extracted; warnings non-fatal; the query is persisted on the memory). The canonical form is exactly one of `<ds>`, `<ds>.<model>`, `<ds>.<model>.<leaf>` (≤ 3 dotted segments after canonicalisation). Aggregation suffixes are stripped (`revenue:sum` → `<ds>.<model>.revenue`); `*:count` collapses to the source model; multi-hop dotted paths keep only the leaf (`orders.customers.regions.name` → `{<orders.ds>.orders, <regions.ds>.regions.name}`). The resolver lives in `slayer/memories/resolver.py`; the unified `Memory` row + storage primitives are concrete on `StorageBackend` (ID format / monotonic non-reuse / entity-intersection filter), with backends only implementing the row-shaped CRUD + a single `memory_seq` counter. `inspect_model` auto-renders a `Learnings` section listing only memories where `query is None`; query-bearing memories surface only via `search` (in the `example_queries` bucket). Memory ids are monotonic positive ints and never reused.

  `search` runs up to three parallel channels merged by RRF (DEV-1386 adds the third). **Channel 1** is entity-overlap BM25 over memories (`slayer/memories/ranker.py` using `rank_bm25.BM25Plus`, DEV-1365) — a precisely-tagged memory outranks one with a long entity list that overlaps incidentally. **Channel 2** is a fresh in-memory tantivy index built per call over memories ∪ entities (datasources / non-hidden models / non-hidden columns / named measures / aggregations), using tantivy's `en_stem` analyzer (Porter stemmer + default tokenizer, splits on `_` and `.`). **Channel 3** (DEV-1386, optional via the `embedding_search` pip extra) is dense embedding similarity over the same memories ∪ entities corpus, computed numpy-only against rows persisted in a sidecar `embeddings` table keyed by `(canonical_id, embedding_model_name)`. The active embedding model is read from `SLAYER_EMBEDDING_MODEL` (default `openai/text-embedding-3-small`) and dispatched via litellm; provider credentials are read by litellm directly (`OPENAI_API_KEY`, etc.). When the extra is not installed, the model has no rows, or the query embedding call fails, channel 3 contributes nothing and emits a single warning into `SearchResponse.warnings`; tantivy + BM25 continue to work. Refresh runs inline on `slayer ingest` / `edit_model` / `save_memory` and skips the litellm call when the rendered `content_hash` matches the stored row (cheap idempotent re-runs). Per-entity embed failures are non-fatal — search degrades gracefully. Memory rankings from every active channel are fused via Reciprocal Rank Fusion (`k=60`, hand-rolled in `slayer/search/rrf.py`); **entity hits from channels 2 and 3 are now also RRF-fused** (channel 1 contributes only to memory ranking). Memory hits are partitioned by `Memory.query is None` into `memories` (learning-only, small) and `example_queries` (query-bearing, bulky) — independent caps via `max_memories` and `max_example_queries` so bulky examples cannot crowd out small learnings. The response also echoes `resolved_input_entities` for diagnostics. Empty-input fallback returns the newest `max_memories` learning-only + newest `max_example_queries` query-bearing memories with a warning. Each indexed entity carries a `text` field rendered by `slayer/search/render.py` — named children (columns / measures / aggregations / join targets) are mentioned by name + kind only (no descriptions, since each child has its own indexed doc), while non-named children (model filters, model `sql` block, join `pairs`, aggregation `params`) are included in full. `meta` is **excluded** from indexed text (DEV-1377 hardening). Hidden models / hidden columns are skipped.

  Sample-value snapshots cached on `Column.sampled` (v6 schema bump, no-op forward migration in `slayer/storage/v6_migration.py`); refreshed on every `slayer ingest` for table-backed models, on `slayer search refresh-samples`, on `edit_model` (column-level edits → that column; `model.filters` / `model.sql` / `source_queries` change → all columns), and lazily on `inspect_model` cache miss (best-effort write-back). sql-mode and query-backed sample-value coverage is deferred to [DEV-1377](https://linear.app/motley-ai/issue/DEV-1377). Surfaces: write side via MCP, REST (`POST /memories`, `DELETE /memories/{id}`), CLI (`slayer memory {save,forget}`), and `SlayerClient`; retrieval via MCP (`search`), REST (`POST /search`), CLI (`slayer search [--entity ...] [--query ...] [--question ...] [--max-example-queries N]`, `slayer search refresh-samples`), and `SlayerClient.search()`. See [docs/concepts/memories.md](docs/concepts/memories.md) and [docs/concepts/search.md](docs/concepts/search.md).

## Async Architecture

- **Engine is async-first**: `SlayerQueryEngine.execute()` is `async`. Use `execute_sync()` for CLI/notebooks/scripts.
- **Storage backends are async**: All `StorageBackend` methods are `async def`. YAMLStorage uses sync I/O inside async (fast local files). SQLiteStorage uses `asyncio.to_thread`. Future Postgres storage can use true async (asyncpg).
- **SQL client**: Uses native async drivers for Postgres (`asyncpg`) and MySQL (`aiomysql`). Falls back to `asyncio.to_thread` for SQLite, DuckDB, ClickHouse. Connection pools are cached per `SlayerSQLClient` instance.
- **Tests use `pytest-asyncio`** with `asyncio_mode = "auto"` — test functions can be `async def` and `await` directly.
- **Sync wrappers**: `run_sync()` in `async_utils.py` bridges async→sync for CLI and MCP tools. Handles both "no event loop" and "inside Jupyter" cases.

## CLI

- All commands accept `--storage` (directory for YAML, `.db` file for SQLite). Defaults to platform-appropriate path (`~/.local/share/slayer` on Linux, `~/Library/Application Support/slayer` on macOS). Override with `$SLAYER_STORAGE` env var. Legacy `--models-dir` still works.
- `slayer query` supports `--dry-run` (preview SQL) and `--explain` (execution plan, dialect-aware).
- `slayer datasources create-inline` supports `--password-stdin` for secure credential input.
- `slayer datasources test` verifies connectivity.
- `slayer datasources create demo [--ingest]` spins up the bundled Jaffle Shop DuckDB (idempotent). `slayer serve --demo` and `slayer mcp --demo` do the same at server startup. Requires the `duckdb` extra and `jafgen` (git-only install); missing deps trigger a clean install-hint message. Lives in `slayer/demo/jaffle_shop.py`.
- `slayer validate-models [--datasource X] [--force-clean] [--yes]` (DEV-1356) — read-only diff against live schemas; with `--force-clean`, prompts to apply each delete via `engine.apply_drift_deletes`. See [docs/concepts/schema-drift.md](docs/concepts/schema-drift.md).
- `slayer storage migrate-types [--data-source X] [--dry-run]` (DEV-1361) — refine `DOUBLE → INT` on base columns whose live SQL type is integer for every persisted model, then write the refined v5 dict back. Hard-fails if a datasource is unreachable. The same refinement runs transparently inside `storage.get_model` on first load; this CLI is a batch / inspectable alternative.
- `slayer search [--entity ENT ...] [--query JSON_OR_@FILE] [--question TEXT] [--max-memories N] [--max-entities N] [--format json|text]` (DEV-1375) — two-channel semantic search over memories + canonical entities. See [docs/concepts/search.md](docs/concepts/search.md).
- `slayer search refresh-samples [--data-source X] [--model M ...]` (DEV-1375) — re-profile and persist `Column.sampled` for table-backed models. Best-effort: per-column failures are reported but don't abort.
- MCP `query()` tool has a `format` parameter: `"markdown"` (default), `"json"`, or `"csv"`.

## Database Support

SLayer uses sqlglot for dialect-aware SQL generation. Databases are supported at two tiers:

**Tier 1 — fully tested** (integration tests + Docker examples, must not regress):
- **SQLite** — integration tests in `tests/integration/test_integration.py`, embedded example
- **Postgres** — integration tests in `tests/integration/test_integration_postgres.py`, Docker example
- **DuckDB** — integration tests in `tests/integration/test_integration_duckdb.py` (no Docker, runs in-process)
- **MySQL** — Docker example with `verify.py`
- **ClickHouse** — Docker example with `verify.py`

**Tier 2 — code-covered** (unit tests for SQL generation, no live instance verification):
- Snowflake, BigQuery, Redshift, Trino/Presto, Databricks/Spark, MS SQL Server, Oracle

Dialect mapping lives in `query_engine.py:_dialect_for_type()`. Dialect-specific SQL lives in `generator.py` — mainly `_build_date_trunc` (SQLite branch), `_build_time_offset_expr` (date arithmetic for shifted CTEs), `_build_median`, `_build_percentile`, and `_build_stat_agg` (stddev/var/corr). Calendar-based time shifts use timestamp offset inside DATE_TRUNC with simple equality joins (no per-dialect join logic). All other SQL differences are handled by sqlglot transpilation. When adding a new dialect: add it to `_dialect_for_type`, add a `_build_time_offset_expr` branch if it doesn't use Postgres-style `INTERVAL`, and add parameterized tests in `TestMultiDialectGeneration`.

**Aggregation caveats:**
- **SQLite**: `median`, `percentile_cont`, `percentile_disc`, `stddev_samp`, `stddev_pop`, `var_samp` (also aliased as `variance`), `var_pop` (also aliased as `variance_pop`), `corr`, `covar_samp`, `covar_pop` are provided via Python aggregate UDFs registered on every new connection (`slayer/sql/sqlite_udfs.py`); SQLite has no native equivalent. Scalar UDFs `ln`, `log10`, `log2`, `exp`, `sqrt`, `pow`, `power` are also registered there; `log2` overrides SQLite ≥3.35's silent-NULL built-in to keep the strict math-domain-error semantics. The 2-arg `log(B, X)` UDF (returns log_B(X) — base first, value second) is registered on **every** SQLite version, including ≥3.35 where it overrides the built-in's silent-NULL behaviour to match Postgres's strict error semantics. Same B-first arg order in both.
- **ClickHouse**: `percentile` emits the parametric `quantile(p)(x)` syntax; `median` uses native `median(x)`. `stddev_samp`/`stddev_pop`/`var_samp`/`var_pop`/`corr` are native (sqlglot transpiles to dialect-appropriate spelling).
- **MySQL**: `median`, `percentile`, `corr`, `covar_samp`, `covar_pop` are not supported — MySQL has no native function and no Python-UDF mechanism. The generator raises `NotImplementedError` at SQL generation time. Use MariaDB or compute client-side. `stddev_samp`/`stddev_pop`/`var_samp`/`var_pop` are native on MySQL.
- **Postgres / DuckDB**: native `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` (DuckDB via sqlglot's `QUANTILE_CONT` translation). `STDDEV_SAMP`/`STDDEV_POP`/`VAR_SAMP`/`VAR_POP`/`CORR`/`COVAR_SAMP`/`COVAR_POP` are native on both.

**In-memory SQLite caveat:** `sqlite:///:memory:` (and equivalent URI variants — `sqlite://`, `sqlite:///file::memory:?…`, `mode=memory`) works across `await` calls on a single `SlayerSQLClient` because the client owns a per-instance `StaticPool` engine with `check_same_thread=False`. Two separate `SlayerSQLClient` instances on `:memory:` are isolated from each other. Use a file path or `mode=memory&cache=shared` URI form to share state across clients. File-backed SQLite is unaffected — it routes through the module-level engine cache as before.

**SQLite JSON extraction:** `json_extract(col, '$.path')` in `Column.sql` (or any expression `SQLGenerator` parses on SQLite) is preserved as the function-call form, not rewritten to `col -> '$.path'`. The `->` operator in SQLite returns the JSON-quoted form (e.g. `'"Owned"'` with literal quotes), which silently breaks equality / CASE WHEN matches against bare-string literals; the function form returns the unquoted scalar. Implemented via `slayer/sql/sqlite_dialect.py::rewrite_sqlite_json_extract`, applied uniformly through `SQLGenerator._parse`. Use `->>` (`exp.JSONExtractScalar`) directly if you specifically want the dialect operator — SLayer leaves it untouched.

**`log10` / `log2` literal preservation:** A user-written `log10(x)` or `log2(x)` in `Column.sql` / `ModelMeasure.formula` / filters is emitted verbatim as `log10(x)` / `log2(x)`, not canonicalised to `LOG(10, x)` / `LOG(2, x)`. sqlglot's default behaviour normalises both into a generic `Log(base, expression)` AST and re-emits as `LOG(base, x)`, which is correct numerically but breaks formula-text round-tripping for benchmark agents reading `inspect_model.last_sql` and trips dialects without a 2-arg `LOG`. Implemented via `SQLGenerator._rewrite_log_aliases`, applied through `_parse`. Allowlists in `slayer/sql/generator.py` (`_LOG10_NATIVE_DIALECTS`, `_LOG2_NATIVE_DIALECTS`) cover every supported backend except Oracle (no `LOG10` / `LOG2`) and T-SQL (no `LOG2`); those fall through to the canonical 2-arg form. Other 2-arg calls — `log(3, x)`, `log(some_col, x)` — always emit as `LOG(B, X)`.

## Testing

**Important**: Always use `poetry run` to run tests — this ensures the correct Poetry-managed virtualenv is used (not the system or conda Python).

```bash
# Run ALL tests (unit + integration)
poetry run pytest tests/ -m "integration or not integration" -v

# Run unit tests only (default, excludes integration)
poetry run pytest

# Run all integration tests
poetry run pytest tests/integration/ -m integration

# Run specific integration suite
poetry run pytest tests/integration/test_integration.py -m integration        # SQLite
poetry run pytest tests/integration/test_integration_postgres.py -m integration  # Postgres
poetry run pytest tests/integration/test_integration_duckdb.py -m integration    # DuckDB
```

- Unit tests: `tests/test_models.py`, `test_sql_generator.py`, `test_storage.py`, `test_sqlite_storage.py`, `test_mcp_server.py`
- Integration tests (SQLite): `tests/integration/test_integration.py`
- Integration tests (Postgres): `tests/integration/test_integration_postgres.py` — uses pytest-postgresql (auto-spawns temp Postgres)
- Integration tests (DuckDB): `tests/integration/test_integration_duckdb.py` — uses duckdb directly (no Docker)
- Shared fixtures in `tests/conftest.py`

## Linting

**ALWAYS run the linter at the end of every task and fix any issues before finishing.**

```bash
poetry run ruff check slayer/ tests/
```

To auto-fix fixable issues:
```bash
poetry run ruff check --fix slayer/ tests/
```

## Documentation Requirements

**ALWAYS update documentation when making API or user-facing changes.** Check and update ALL of these locations:

1. **`CLAUDE.md`** — Key Conventions, Async Architecture, CLI, Database Support sections
2. **`docs/`** — concept docs (`models.md`, `queries.md`, `formulas.md`, `ingestion.md`), getting-started guides, reference docs
3. **`.claude/skills/`** — `slayer-query.md`, `slayer-models.md`, `slayer-overview.md`
4. **`docs/configuration/`** — datasources, storage backends

When renaming a field, adding a parameter, or changing response structure, **grep all docs and skills** for the old name and update every occurrence.
