## Context

- Layer 1 construct table: `tests/_dev2446_required_manifest.py` (rows `ok`/`gap`, `GAP = "DEV-2282"`), run by
  `tests/test_dev2446_required_constructs.py` on `BACKENDS = ("sqlite", "duckdb")` (`tests/_dev2215_fixtures.py:38`), gap
  xfails in `tests/test_dev2220_layer1_gaps.py`. Its completeness parser reads `CORPUS` directly, ignoring
  `$SLAYER_OSSIE_CORPUS`, so the weekly run checks the vendored spec, not upstream's.
- Layer 3: `tests/test_dev2243_wide_table_oracle.py` and `tests/test_dev2217_*` over upstream fixtures loaded into DuckDB
  only (`tests/_dev2217_fixtures.py:48`). Upstream fixture SQL is Postgres/DuckDB-flavoured (`DROP SCHEMA … CASCADE`, bare
  `VARCHAR`, schema-qualified tables).
- Per-engine execution today is copy-per-engine: a shared `check_*` function plus a fixture in each
  `tests/integration/test_integration_<db>.py`; seeding helpers `server_table_statements` (`tests/_dev2120_fixtures.py:767`,
  `tests/_dev1737_fixtures.py:446`) take structured `TableSpec`s. `TIER1` is listed in several fixture modules.
- `PinnedClock` pins only SLayer's own time resolution; `CURRENT_DATE` renders to the server clock.
- Weekly: `.github/workflows/ossie-corpus-weekly.yml` runs unit modules only. Per-engine PR workflows are path-gated on
  `slayer/**`, `tests/_*.py`, `tests/integration/_*.py`. Snowflake/BigQuery skip without credentials and run on PRs under
  `expensive-job-gate.yml`.
- The compliance page (`docs/interfaces/ossie-compliance.md`) is checked by `tests/test_dev2220_compliance_page.py`
  against the manifest gaps plus `tests/_dev2220_compliance_gaps.py`.

Applicable principles: system §3.4 (SQL as sqlglot AST), §3.6 (Pydantic, no dataclasses — production); `python.sql` P2
(dialect quirks only in `dialects/`, no `if dialect ==` elsewhere), P3 (one naming authority — DEV-2395 quoting lives in
`slayer/sql/naming.py` or the dialect's quoting, not at a call site), P9 (fail closed: refuse, never emit wrong SQL), P14
(emitted types are logical types — the DEV-2449 decimal cast). No arc42 or `.c4` edits; test-only modules sit outside the
model.

## Goals / Non-Goals

**Goals:**
- Full Ossie Layer 3 claimed on all nine Tier-1 databases, backed by executed cases.
- Uniform coverage: no cell fails on one engine while passing on DuckDB.
- One gate, run on PRs (vendored snapshot) and weekly (upstream `main`), that fails on any upstream change SLayer does not
  meet.
- Layer 1 gaps recorded per cell against their construct-family issue, with the page's claims derived from them.

**Non-Goals:**
- Implementing Layer 1 gaps (DEV-2457 windows, DEV-2458 aggregates, DEV-2459 functions, DEV-2460 time of day, DEV-2461
  importer fidelity / quoted names) and every-document-loads (DEV-2464). (user)
- Ossie Layer 2 (`compliance/query/scott`, DEV-2400); EXPERIMENTAL constructs.
- Implementing RECOMMENDED constructs SLayer refuses today: they are declared refused.

## Decisions

1. **Scope**: Layer 1 table plus Layer 3 cases on every Tier-1 database, weekly against upstream `main`, on one shared
   harness, growing toward full conformance. Options: both on one harness / Layer 3 only / Layer 1 here and Layer 3 as a
   separate issue. Taken: both — the per-engine seeding is the substance of both. (user)
2. **This PR claims full Layer 3 now, fixes every engine-specific blocker, adds the weekly job and records Layer 1 gaps;
   fixing Layer 1 gaps comes later.** DEV-2462's scope and acceptance were edited to say so; it closes with this PR. (user)
3. **Engine-specific failures** (fail on an engine, pass on DuckDB) are fixed in this PR; more than 10 distinct root causes
   → stop and ask. Deliberate Layer 1 gaps are strict xfails against per-construct-family issues; a gap no existing family
   covers is put to the user before any issue is created. Options: per-engine issues / per-family issues / fix everything.
   (user; the threshold is a default)
4. **RECOMMENDED constructs get rows**: pass or refused, refused is a legitimate final state, a wrong value or a
   non-uniform acceptance fails; completeness covers them. Options: no rows / rows / report-only. (user)
5. **DEV-2449 and DEV-2395 are folded in**, plus a gate cell exercising DEV-2395 (a mixed-case column read through a
   relationship as dimension, filter and metric input): both are needed for an honest Layer 3 claim on Trino, SQL Server,
   Postgres and Snowflake, and the upstream cases alone would not hit them. Options: fold in / xfail against them. (user)
6. Layer 2 (scott) excluded — DEV-2400 owns it; EXPERIMENTAL constructs get no rows. (default)
7. Engine-independent suites (expression fixtures, portability ladder, corpus load and round trip) run once; the corpus
   compiles in all nine dialects, since it carries no data. (default)
8. One engine-parametrised harness replaces copy-per-engine test files; `TIER1` comes from one place. (default, structural)
9. One corpus-root resolver, used before any collection-time constant is built. (default, structural)
10. PRs run only the in-process gate (unit job). One scheduled workflow runs a per-server-engine matrix nightly on the
    vendored snapshot and weekly against upstream `main` (plus the in-process suites); per-engine setup is a composite
    action; an aggregation job fails unless every engine passed; Snowflake/BigQuery run with repo secrets and fail
    closed without them. Per-engine PR workflows are unchanged. Options: every engine per PR / in-process per PR +
    nightly servers. Taken: the latter, to keep PR CI light. (user)
11. Gap rows move from DEV-2282 (now a parent) to its children; a parent carries no work items. (default)
12. `APPROX_PERCENTILE` (RECOMMENDED) moves from "planned" gap to refused. (default, from 4)
13. **Canonical fixture form** (codex): upstream fixture SQL is parsed with sqlglot into tables, column types and rows,
    then emitted per engine with that engine's schema / dataset / catalog setup and cleanup; a seeding smoke test per
    engine runs before the cases. The Layer 1 seed goes through the same emitter.
14. **Clock** (codex): server SQL reads the database clock, so current date/time rows assert a bracket captured around the
    query, widened by one day for the session time zone, replacing today's `>` comparisons.
15. **Alternate-corpus proof** (codex): a test over a temporary corpus copy with a sentinel construct, case and document
    proves every suite reads the resolver.
16. **Logical-type comparison** (codex): exact numbers stay `Decimal`, booleans are distinct from integers, temporal values
    compare at the row's precision, NULL only to NULL; tolerance only for approximate constructs.
17. **Multiset rows** (codex): wide-table rows compare as a multiset, as today; order only where Ossie defines it.
18. **Fail closed on skips** (codex): the scheduled aggregation job requires every engine job to pass, not skip.
19. **Declared cells equal discovered cells** (codex): state declarations are keyed by (row or case id, engine) with issue
    and error class; stale or missing declarations fail.
20. **Fail-closed completeness parser** (codex): any unclassified line inside a REQUIRED/RECOMMENDED section fails, a
    REQUIRED/RECOMMENDED heading at any depth counts, and mutation tests cover a new heading, table row, list item and
    argument form. A Markdown-AST dependency was rejected: the fail-closed line check gives the same guarantee without one.
21. **Page claims derived from gaps** (codex): per-engine pass / refused / gap counts; "full Layer 1" iff zero REQUIRED gaps
    or refusals, asserted by the page check; Layer 3 claimed on all nine with the Layer 1 caveat for expressions inside a
    query. (user, approved wording for the issue: "claims full Layer 3 on every Tier-1 database. Expressions inside a query
    are subject to the listed Layer 1 gaps. It reports per-engine pass, refused and gap counts for Layer 1.")
22. Rejected (codex): requiring every REQUIRED cell to pass before DEV-2462 closes — superseded by decision 2, the issue's
    acceptance was edited instead.

## Risks / Trade-offs

- [Engine-specific failures may be many] → pr-tests runs the full gate on every engine first and reports root causes;
  more than 10 → stop and ask (decision 3).
- [Snowflake/BigQuery cost weekly] → small fixtures, one run per week; PR runs stay under `expensive-job-gate.yml`.
- [Upstream `main` churn turns the weekly run red] → intended: it is the signal to re-vendor and adjust declarations.
- [Fixture SQL beyond what the canonical form parses] → the parser fails closed naming the statement, so a new upstream
  fixture construct turns the weekly run red rather than seeding wrong rows.
- [DEV-2395 has a confidential customer section] → tests and fixtures use only the synthetic repro.
