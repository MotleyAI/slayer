## 1. Tests first (pr-tests)

- [x] 1.1 Write failing tests for every scenario in this change's delta specs. They are unit tests on in-process SQLite and DuckDB, plus Postgres via pytest-postgresql where a scenario names Postgres. Verify each new test fails on `main` for the intended reason.
- [x] 1.2 Before writing tests, find every existing test whose assertions pin the old behaviour. That covers bucket `month(x)` and siblings, `gran(x)` time-dimension strings, `==` / `None` in CASE-lowering output, exact-case lookups, `first`/`last` native NULL order, keyword argument versus comparison, and the tripwire and library-growth pins. List them by file with what changes, and get the owner's per-test approval before aligning them.
- [x] 1.3 Law and pin tests: one name matcher (no namespace lookup compares names itself), the library-growth `reserved` vocabulary with `agg` and its migration, and the tripwire `PINNED` entries for the deprecated Mode-B spellings at removal 3.0. Verify each fails before implementation.

## 2. Date-part flip

- [x] 2.1 `core/syntax.py`: `year` … `second` become date-part aliases; granularity names leave call dispatch; `date_trunc` parses to a dedicated node (literal unit + expression). Verify with the parser tests and the `queries/date-functions` alias scenarios.
- [x] 2.2 Binding: build `TimeTruncKey` from the `date_trunc` node, validating built-in and datasource units. A granularity-named call that resolves to no aggregation raises the typed `date_trunc` remedy error. Verify with the `date_trunc is the granularity call` and `Granularity names have no call form` scenarios, including the custom granularity plus same-named aggregation case.
- [x] 2.3 `core/query.py` and `bind_inputs`: the functional rewrite recognises only `date_trunc`; `time_dimensions` strings must be `date_trunc` calls (remedy for any one-argument call); order keys follow. Verify with the `queries/time-dimensions` scenarios.
- [x] 2.4 One remedy helper for positions that require a bucket: transform axis, windowed aggregates, spine axis and countable-rows errors, `whole_periods_only`. Verify with the `queries/transforms`, `queries/time-spine` and `queries/time-points` scenarios.
- [x] 2.5 Checker: refuse an extraction compared with a date-shaped literal. Numeric strings and bare years stay legal. Verify with `Extraction compared with a date-shaped literal is refused`.
- [x] 2.6 Result keys: `month(x)` shares its `date_part` twin's key, and a migrated `date_trunc` keeps the old bucket key (named and unnamed, two buckets on one source, order, stage schemas, refinement). Verify with the key scenarios and the existing SQL goldens.
- [x] 2.7 Usage reporting counts `year(x)` … `second(x)` as `date_part`. Verify with the `usage-reporting/collection` scenario.

## 3. Facade

- [x] 3.1 `facade/translator.py`: extraction projections for `MONTH(col)` and its siblings, `EXTRACT` and `date_part`, under one canonical alias resolvable by GROUP BY and ORDER BY. `DATE_TRUNC`, `TIMESTAMP_TRUNC` and the Sunday-week wrapper stay buckets. Verify with the `facade/projection` and `facade/time-filter-translation` scenarios over the Postgres wire and Flight test clients.

## 4. Identifier case

- [x] 4.1 `core`: one name matcher (exact spelling, else a unique case-insensitive match, else the typed `AmbiguousReferenceError`). Per-segment quoting on `DottedRef`, preserved through every path rewrite (`drop_root_qualifier` included). Verify with the matcher law test and the quoting scenarios.
- [x] 4.2 Route every lookup through the matcher: columns, measures, aggregations, join walker, models, stages, alias map, and transform names. Result keys carry the declared spelling. Verify with the `queries/identifier-case` resolution and canonical-key scenarios.
- [x] 4.3 Save-time collision check against the persisted raw document of the same identity, covering every namespace in the spec. Grandfathered collisions warn on load, on save and on every response reading the model. Verify with the save scenarios, including the warning on query and facade responses.
- [x] 4.4 Refuse fresh-query result names that collide case-insensitively, on every dialect. Verify with the DEV-1731 scenario.
- [x] 4.5 Ingestion: case-only-distinct physical columns get the smallest free `_<n>` with `sql` reading the physical column, plus a warning, deterministically. Verify with the ingestion scenarios on Postgres (pytest-postgresql) and SQLite.

## 5. `agg`, keyword arguments, NULL keys

- [x] 5.1 Reserve `agg`: refuse the call and refuse saving a new custom aggregation named `agg`. Add `agg` to the library-growth `reserved` vocabulary. Verify with the `models/aggregation-names` scenarios and the library-growth test.
- [x] 5.2 `=` arguments: ordered neutral nodes in the parser, classified at binding by the callee's declared parameters (case-insensitive), with duplicate and order checks run after classification. Verify with the `Keyword arguments versus comparison` scenarios.
- [x] 5.3 `first`/`last`: ranked SQL with a null-flag sort key so non-NULL keys come first in both directions, the picked key carried through, and a NULL result for an all-NULL cell. Plain, partitioned, windowed and cross-model forms are covered. Verify with the functional-form and trailing-window scenarios on SQLite, DuckDB and Postgres, plus emission tests for every Tier-1 dialect.

## 6. Deprecated spellings

- [x] 6.1 The reader records legacy spans (`==`, `is [not] None`, bare `None`, `[…]` after `IN`, the method-call form) with their twins. A deprecation sink emits `warn_deprecated(..., removal="3.0")` and a structured warning once per spelling per query or save. Verify with the `Legacy spellings keep their meaning` scenarios and the tripwire test.
- [x] 6.2 Save stores the canonical twin in place of each legacy span, with the rest byte-identical (engine P8 as amended). Verify with `A save stores the canonical twin`.
- [x] 6.3 SLayer never emits the deprecated spellings: CASE lowering (`NULL`, `=`), canonical text, remedies and internal producers. Verify with `SLayer never self-warns` and the `queries/expression-keywords` scenarios.

## 7. Stored upgrade

- [x] 7.1 One versioned step (SlayerModel 18→19, SlayerQuery 9→10, Memory 7→8, `stored_only`) that rewrites built-in bucket calls to `date_trunc`, legacy spans to their twins, and `agg` aggregations to `agg_aggregation` with every call rewritten. One recursive walker over the Mode-B field matrix (settle `Column.filter`'s mode from its parser first) covers nested documents. Verify with the `models/stored-upgrade` scenarios.
- [x] 7.2 Storage load path: capture raw versions, settle datasource granularity names, rewrite pre-bump custom-granularity calls, then run the versioned migrations. Idempotent. Verify with the custom-granularity memory scenario, an interrupted-load test, already-current documents, and inline and query-backed models.
- [x] 7.3 Library growth: the `reserved` vocabulary and `MIGRATED` entry for `agg`. Verify with `tests/test_library_growth.py`.

## 8. Architecture, docs, help

- [x] 8.1 Apply the approved wording verbatim (design decisions 9 and 10) to `architecture/python.engine.arc42.md` P8 and `architecture/python.storage.arc42.md` P3.2. Verify with `poetry run la-arch-check` and `npx -y likec4@1.47.0 validate architecture`.
- [x] 8.2 Docs: `docs/concepts/{time,formulas,queries,references}.md`, `docs/examples/04_time/time.md`, `docs/reference/mcp.md`, `docs/interfaces/pg-facade.md`, `docs/interfaces/flight-sql.md`, `docs/getting-started/flight-sql.md`. Buckets are written only as `date_trunc`; extraction, case rules, the `agg` reservation and `first`/`last` NULL keys each get one sentence; deprecated spellings are removed. Verify with grep that no doc shows a bucket as `<unit>(x)` or a deprecated spelling.
- [x] 8.3 MCP `help.*` memories and tool `Field` descriptions are updated the same way. Verify with the `mcp/tool-surface` scenario.

## 9. Gates

- [x] 9.1 Full non-integration suite green: `poetry run pytest -m "not integration"`.
- [x] 9.2 `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors against the baseline), `poetry run la-arch-check`, and `openspec validate dev-2215-ossie-layer-1-alignment-date-part-functions --strict`.

## 10. Tracker follow-ups (after merge)

- [ ] 10.1 Post comments: on DEV-1768 (granularity calls → TimeDimension rule now `date_trunc`), DEV-2098 (re-spell examples), DEV-2202 (exporter writes `DATE_TRUNC`), DEV-1731 (absorbed), DEV-1920 (the reader's legacy-span machinery is here; coordinate migration numbering), and DEV-2220 (release-note lines: the silent `month(x)` dimension change, `first`/`last` NULL keys, case rules, the `agg` reservation). Verify that each comment is posted.
