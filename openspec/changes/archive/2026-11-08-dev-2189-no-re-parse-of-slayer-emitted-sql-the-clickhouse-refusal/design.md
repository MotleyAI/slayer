## Context

See proposal.md — Why. Today `generate_planned_stages` finishes the statement to text
(`_finish_statement`: `rewrite_statement` → render → `rewrite_emitted_sql` fitting →
scope validation → over-limit check) and every later consumer re-parses that text:
`_require_correlated_subqueries` → `ClickhouseDialect.correlated_subquery_refusal`,
`_apply_policy` → `apply_session_policy` (parse → rewrite → non-pretty `ast.sql`),
`build_response_metadata` → `expected_columns_from_sql`, and the cache's
`applicable_keys` → `parse_referenced_tables` on the post-policy text. The refresh-key
scan (`build_refresh_key_sql`) and join-cardinality side stats (`_side_stats_sql`) build
ASTs, render them, then `_apply_policy` re-parses them. `get_column_types` of a
query-backed model renders the backing statement in `_expand_query_backed_model` and the
type probe re-parses that virtual model `sql` (`_embed_model_sql`).

Two pr-plan probes over the full unit suite ground the approach:
- 27,210 rendered statements: the pre-render AST and the re-parsed text agree on outer
  `named_selects` (text side decoded), physical tables (`traverse_scope`, CTE/derived
  skipped) and the EXISTS-beside-ARRAY-JOIN predicate, except 2 test artifacts (text side
  undecoded) and 2 Snowflake spines where the AST sees `TABLE(GENERATOR(...))` as a
  nameless table (RLS's number-series skip already drops it). No opaque node hides a
  physical table from the AST.
- A record-only runtime law found the seven production sites listed in proposal.md, and
  showed substring matching false-positives on user fragments.

Architecture: python.sql §3.1 (AST end to end), §3.3 (one naming authority), §3.9 and
system §3.15 (fail closed), §3.13 (verbatim execution), system §3.6.

## Goals / Non-Goals

**Goals:**
- One render per executed statement, after its last rewrite; no consumer parses it.
- RLS scopes identical tables with identical predicates; every fail-closed refusal kept,
  per statement kind.
- A runtime law that makes a re-parse of an execution's own statement fail anywhere
  below any public engine method.

**Non-Goals:**
- The DEV-1972 value layer (`VALUE_LAYER_EXEMPT` renders and their re-parses) and DEV-1961.
- The emitted-text validators (`scope_check.assert_dependency_ordered_ctes`,
  `check_scope_closed`, `naming.assert_unique_cte_names`).
- `rewrite_emitted_sql` identifier fitting: a text substitution inside the single render,
  not a parse.

## Decisions

**D1 — The engine carries the statement tree to one render (user, DEV-2185 Q8 = A).**
`_prepare_pipeline`: build the statement tree (settings attached as today) → ClickHouse
refusal on the tree → policy rewrite on the tree → response metadata from the tree →
physical tables for the cache from the tree → one `_finish_statement` → verbatim execution.
The generator exposes a build step returning a Pydantic `PlannedStatement` (statement,
projection aliases, exemptions); `generate_planned_stages` stays as build + render for
policy-free callers. `_Rendered` always carries the statement (its `sql` field and the
`as_statement` flag go); `_Prepared` carries the physical tables beside `sql`.
Rejected B (ClickHouse site only): leaves three known violators incl. the RLS round-trip,
and the law still cannot see nested re-parses (user).

**D2 — ClickHouse refusal on the tree (user, DEV-2185 Q8 = A).**
`correlated_subquery_refusal(profile, *, statement: exp.Expression)`, required keyword (a
dialect hook takes typed, non-`str` operands, python.sql §3.1). `_exists_beside_array_join`
runs the same "same SELECT" predicate on the tree. A plan-level predicate was rejected: it
only approximates "same SELECT". Evaluated on the pre-policy statement, as today (default,
parity; the ClickHouse policy rewrite emits `IN`, never `EXISTS`).

**D3 — Scope: all seven sites; the text policy entry is deleted (user).**
`apply_session_policy(statement, *, sql_dialect, policy, has_column) -> Expression`.
No production caller passes SQL SLayer did not generate (the facades translate client SQL
into a `SlayerQuery`), so the text entry is removed rather than kept for an empty use case.
Callers: data query, refresh-key scan (`build_refresh_key_sql` returns a tree), side stats
×2 (`_side_stats_sql` returns trees), column-type probe. Each renders once after the policy.
The fail-closed checks are unchanged code (`_ALLOWED_ROOTS`, `has_column` `None`/`False`,
uncovered table, `_validated_hops`). Tests that feed SQL literals parse them in the test
(mechanical).

**D4 — The single render validates post-RLS SQL (default).** `_finish_statement` learns
whether a policy rewrote the statement and passes `allow_rls_correlation` to the scope
validator. Policy-scoped SQL therefore goes through the full normal render (pretty,
`rewrite_statement`, fitting, scope validation, over-limit check) where today it was a
non-pretty re-render: goldens change at most in whitespace. The `rewrite_statement`
overrides do not touch RLS wrappers — MySQL/ClickHouse rewrite ORDER BY terms (RLS
wrappers have none), T-SQL value-position predicates (RLS predicates sit in WHERE).

**D5 — One physical-table walk (default, structural; codex).** A single
`physical_tables(statement)` in `slayer/sql` with RLS's rule (scope-resolved CTE/derived
sources and number-series table functions skipped; a schema- or catalog-qualified reference
is never a CTE) serves the policy and the cache, which
today implement the same concept twice. The cache takes the normalized tables of the
post-policy tree (RLS hop tables still count, as today); `applicable_keys(tables=…,
dialect=…)`. The only delta — number-series table functions no longer appear as
empty-named tables — never matched a configured refresh key; the policy now also scopes a
qualified table sharing a CTE's name in one SELECT (today it escapes the filter);
before/after applicability
tests pin it (codex): Snowflake `TABLE(GENERATOR(...))`, ClickHouse `numbers(...)`,
qualified physical tables, a CTE-name collision.

**D6 — `expected_columns` from the tree (default).** The outer statement's
`named_selects` are canonical before fitting, so the decode step goes;
`expected_columns_from_sql` is deleted. Reading the plan's keys instead was not chosen: the
tree gives exact parity with what is executed (probe: identical on every statement).

**D7 — Query-backed type probe without an intermediate render (codex).**
`_expand_query_backed_model` is split: an AST-level expansion result (planned stages, root
stage schema, resolved datasource, virtual columns) that `get_column_types` consumes
directly, planning the probe with the model spliced as stages; and the rendering wrap, kept
only for the save-time path (`_validate_and_populate_cache`), where the render runs the
scope and over-limit validators on the backing statement. A law test asserts a
query-backed type probe renders only its final probe statement.

**D8 — Runtime law (user: law + `enforced:` tag; default: matching rule).**
`tests/_emitted_reparse_law.py`, installed by the conftest autouse fixture beside the
render law. An execution is the dynamic extent of every public `SlayerQueryEngine`
method, discovered from the class so a new entry is guarded without a list. Inside it the
law records each `_finish_statement` result and each render of an `exp.Query` root, and
fails when `Dialect.parse` / `parse_into` receives a string **equal** to a recorded one
(the probe showed containment false-positives). Exempt: the three validators, the
DEV-1972 value layer and the OSI importer's renders of an author's expression into
persisted model text (renders inside `VALUE_LAYER_EXEMPT` / `AUTHORING_RENDERS` are not
recorded), and test
doubles standing in for the database, which may read the SQL sent to them (`as_database`). Self-tests: a
planted re-parse fails directly in `_prepare_pipeline` and three calls below it; the
validators pass; a parse after the execution returns passes; every target resolves.

arc42 edit (user, approved wording) — python.sql §3.1 gains one tag line:
"[enforced: test:tests/test_law_no_emitted_reparse.py]"

**D9 — RLS parity and fail-closed matrices (default; codex).**
`tests/test_dev2189_rls_parity.py`: policies {column pass / block, join one-hop /
multi-hop / whitelist} × shapes {plain, joined dims, multi-stage, spliced query-backed,
sql-mode, semi-join EXISTS, list unnest, time spine, cross-model aggregate, pagination} ×
dialects {SQLite, DuckDB, Postgres executed with rows compared; BigQuery, T-SQL,
Snowflake, ClickHouse, MySQL on emission}. Reference = the previous text path
reconstructed in the test (parse the policy-free render → policy → render); both sides
normalized through sqlglot and compared equal. Fail-closed matrix over all five policy
callers (codex): each refusal kind raises (or, for the type probe, yields `{}`) exactly as
before, and no SQL client method runs after a policy failure.

## Risks / Trade-offs

- [The tree differs from the re-parsed text somewhere the probe did not reach] → the
  parity matrix covers every shape × dialect, ClickHouse integration runs unchanged in its
  CI workflow, and any discrepancy is fixed in the tree, never exempted.
- [Post-RLS SQL now meets the scope validator and over-limit check] → policy predicates
  carry physical names from the policy, which exist in the database; the validator admits
  the `_rls_src` correlation via `allow_rls_correlation`.
- [Cache keys change because policy SQL is now pretty-printed] → the cache is per-engine
  and in memory; nothing persisted is keyed on the old text.
- [A policy failure now raises before the render instead of after it] → same error type
  and payload, still before execution; the fail-closed matrix asserts both.

## Migration Plan

Internal API only (`apply_session_policy`, `correlated_subquery_refusal`,
`expected_columns_from_sql`, `parse_referenced_tables`); no stored data or public surface
changes. Rollback = revert the PR.
