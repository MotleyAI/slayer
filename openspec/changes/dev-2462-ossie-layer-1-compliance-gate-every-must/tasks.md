## 1. Harness (tests)

- [x] 1.1 One corpus-root resolver (`$SLAYER_OSSIE_CORPUS`, else vendored) used by every corpus-reading module before any collection-time constant (`DOCUMENTS`, `CASES`, `RUNGS`, spec path) is built; verify with the alternate-corpus test (task 2.6).
- [ ] 1.2 One `TIER1` source and one engine provider per Tier-1 database (in-process SQLite/DuckDB; Postgres via pytest-postgresql; MySQL/ClickHouse/SQL Server/Trino via testcontainers; Snowflake/BigQuery via credentials), failing rather than skipping when a provider the run requires is unavailable; verify each provider opens an executing engine.
- [x] 1.3 Canonical fixture form: parse upstream wide-table fixture SQL (`retail.sql`, `stores.sql`) with sqlglot into tables, column types and rows, failing closed on any statement it cannot classify; emit per-engine create / insert / cleanup with the engine's schema, dataset or catalog; the Layer 1 seed (`_dev2215_fixtures.TABLES`) and the DEV-2395 synthetic fixture go through the same emitter; verify with the per-engine seeding smoke test (task 2.5).
- [x] 1.4 Logical-type comparator (exact `Decimal`, bool ≠ int, temporal at the row's precision, NULL only to NULL, tolerance only for approximate rows) and multiset row comparison; verify with its own unit tests (task 2.4).
- [x] 1.5 Cell-state declarations keyed by (row or case id, engine): pass / refused (error class) / gap (issue, error class); discovered cells must equal declared cells both ways.

## 2. Tests (written failing first, pr-tests stage)

- [x] 2.1 Layer 1 table: add RECOMMENDED rows (approx aggregates, regex, trig, `TRY_CAST`); `APPROX_PERCENTILE` → refused; repoint every `DEV-2282` gap to its child (DEV-2457 windows, DEV-2458 aggregates, DEV-2459 functions, DEV-2460 time of day, DEV-2461 importer fidelity / quoted names) and the DEV-2244 format cases to DEV-2464; a gap no family covers → stop and ask the user before any issue is created.
- [x] 2.2 Completeness: REQUIRED and RECOMMENDED headings at any depth, both directions, argument counts; fail closed on an unclassified line in such a section; mutation tests for a new heading, table row, list item and argument form.
- [x] 2.3 Current date/time rows assert the clock bracket (± one day) instead of `>` comparisons, on every engine.
- [x] 2.4 Comparator unit tests: `TRUE` vs `1`, `12.35` vs `12.350000000000001`, NULL vs `''`, temporal precision.
- [x] 2.5 Per-engine seeding smoke test: every fixture table reads back with the same rows on every Tier-1 engine.
- [x] 2.6 Alternate-corpus test: a temporary corpus copy with a sentinel construct, wide-table case and document is collected by the completeness check, the wide-table suite and the corpus suites.
- [x] 2.7 Layer 1 table and wide-table cases (both conformances) parametrised over all nine engines: SQLite/DuckDB in the unit suite, the seven server engines in one `tests/integration` module selectable per engine.
- [x] 2.8 DEV-2395 cell: synthetic mixed-case column (`"externalId"`) read through a relationship as dimension, filter and metric input, on all nine engines; synthetic data only (the issue has a confidential section).
- [x] 2.9 DEV-2449 rows in the Layer 1 table (`CAST AS DECIMAL`, evaluated as dimensions): `'9007199254740993e0'`→`DECIMAL(20,0)`, `'12345e-2'`→`DECIMAL(10,2)`, `'1.2345e1'`→`DECIMAL(10,3)`, `'1.25e0'`→`DECIMAL(5,1)` on every Tier-1 engine.
- [x] 2.10 Corpus compile on all nine dialects (dry run, unit suite): SQL or the same typed error in every dialect.
- [x] 2.11 Compliance-page check: per-engine pass / refused / gap counts and lists equal the declarations; Layer 3 claimed on all nine with the Layer 1 caveat; "full Layer 1" claimed iff zero REQUIRED gaps or refusals.
- [ ] 2.12 Probe: run the whole gate on every engine; group engine-specific failures (fail on an engine, pass on DuckDB) by root cause; more than 10 root causes → stop and ask the user; a REQUIRED cell proposed as refused (no translation on that engine) → ask the user.

## 3. Fixes (pr-implement stage)

- [ ] 3.1 DEV-2449: exponent-form numeric text → `DECIMAL(p, s)` exact on Trino and SQL Server, in `slayer/sql/dialects/` (no `if dialect ==` elsewhere); verify the 2.9 rows pass on Trino and SQL Server.
- [ ] 3.2 DEV-2395: a joined dataset's source column renders with the same identity, quoting included, as on its own root, through the one naming / quoting authority; verify 2.8 on Postgres and Snowflake.
- [ ] 3.3 Every engine-specific failure from 2.12, each in its dialect file; verify its cells pass on the engine.

## 4. CI

- [ ] 4.1 Composite actions for per-engine setup (testcontainers extras, ODBC driver, credentials) shared by `integration-<db>.yml` and the weekly job; verify each integration workflow still runs.
- [ ] 4.2 PRs: Postgres in `ci.yml`'s integration job; MySQL / ClickHouse / SQL Server / Trino / Snowflake / BigQuery workflows run the compliance module for their engine; add the module and harness to every path filter.
- [ ] 4.3 Weekly (`ossie-corpus-weekly.yml`): the existing job adds the in-process Layer 1, wide-table and corpus-compile modules; a matrix job runs the compliance module per server engine against the fetched corpus; an aggregation job fails unless all nine engines passed (a skip fails); the compliance-page check is excluded; Snowflake / BigQuery fail closed without secrets. Verify with a `workflow_dispatch` run.

## 5. Docs

- [ ] 5.1 `docs/interfaces/ossie-compliance.md`: Layer 3 full on all nine (with the Layer 1 caveat for expressions inside a query); Layer 1 per-engine counts and refused / gap lists; corpus compile on all nine dialects; verify with 2.11.
- [ ] 5.2 Remove the DEV-2449 limitation sentence from `docs/upgrading/2.0.md`; update `docs/database-support.md` where it states per-engine Ossie coverage.

## 6. Close-out

- [ ] 6.1 The PR closes DEV-2462, DEV-2449 and DEV-2395 (DEV-2405 as duplicate of DEV-2395).
