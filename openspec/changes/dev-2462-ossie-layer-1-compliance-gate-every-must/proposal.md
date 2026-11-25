## Why

SLayer's Ossie claims are certified on DuckDB (Layer 3) and SQLite + DuckDB (Layer 1) only, and only the corpus, oracle and
grader re-run against upstream `main`. Nothing proves that the seven other Tier-1 databases agree, or notices when upstream
adds or changes a construct or case. This change claims full Ossie Layer 3 on every Tier-1 database, fixes what blocks that
claim, and puts one gate in place that checks every upstream compliance suite SLayer claims on all nine engines, on PRs and
weekly against upstream `main`. Layer 1 gaps are recorded, not fixed; the gate fills in as SLayer grows.

## What Changes

- One compliance gate over all nine Tier-1 engines (SQLite, DuckDB, Postgres, MySQL, ClickHouse, SQL Server, Trino,
  Snowflake, BigQuery): the Layer 1 construct table and the Layer 3 wide-table cases execute on each; the corpus compiles
  in each dialect; the engine-independent suites (expression fixtures, portability ladder, corpus load and round trip) run
  once.
- The Layer 1 construct table covers every REQUIRED and RECOMMENDED construct of `expression_language.md`, with a
  fail-closed completeness check in both directions. Every (row, engine) and (case, engine) cell is declared pass, refused
  or gap; gaps are strict expected failures naming their construct-family issue.
- Upstream's wide-table fixtures and the Layer 1 seed are seeded into every engine from one canonical fixture form.
- One corpus-root resolver: every suite reads `$SLAYER_OSSIE_CORPUS` when set, the vendored snapshot otherwise.
- PRs run the in-process gate (SQLite, DuckDB, every-dialect compile); a nightly workflow runs every server engine on the
  vendored snapshot; a weekly run covers every engine against upstream `main`. Each scheduled run fails unless all its
  engines pass.
- Fixes: exponent-form numeric text casts to `DECIMAL(p, s)` exactly on Trino and SQL Server; a source column whose name
  needs quoting renders quoted when read through a relationship, on every dialect; every other cell that fails on one
  engine but passes on DuckDB.
- The compliance page claims full Layer 3 on all nine engines and reports per-engine Layer 1 pass / refused / gap counts;
  the "full Layer 1" claim is derived from the gap count.

## Capabilities

### New Capabilities
- `compatibility/ossie-compliance`: the compliance gate — suites, engines, cell states, completeness, corpus root, weekly
  run, and the compliance page's claims.

### Modified Capabilities
- `models/semantic-model-documents`: the wide-table oracle runs on every Tier-1 database, rows compared as a multiset by
  logical type; corpus metrics compile in every Tier-1 dialect.
- `queries/casts`: exponent-form numeric text keeps every digit when cast to `DECIMAL(p, s)` on Trino and SQL Server.
- `models/field-expressions`: a source column keeps its identity, quoting included, when read through a relationship.

## Impact

- Tests: `tests/_dev2446_required_manifest.py`, `tests/test_dev2446_required_constructs.py`,
  `tests/test_dev2220_layer1_gaps.py`, `tests/test_dev2220_compliance_page.py`, `tests/_dev2220_compliance_gaps.py`,
  `tests/_dev2217_fixtures.py`, `tests/test_dev2243_*.py`, a new shared harness and a new `tests/integration` module.
- CI: `.github/workflows/ossie-compliance.yml` (renamed from `ossie-corpus-weekly.yml`), `ci.yml` (the server gate module
  excluded from the integration job), new composite actions for per-engine setup and the upstream corpus.
- Production: `slayer/sql/dialects/` (Trino, SQL Server decimal casts; whatever the probe finds), the column renderer for
  joined datasets.
- Docs: `docs/interfaces/ossie-compliance.md`, `docs/upgrading/2.0.md`, `docs/database-support.md`.
- Closes DEV-2449 and DEV-2395 (DEV-2405 is its duplicate).
