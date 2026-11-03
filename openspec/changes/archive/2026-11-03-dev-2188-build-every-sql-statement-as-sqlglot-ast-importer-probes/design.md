## Context

See proposal.md — Why. Constraints that shape the approach:

- system §3.6 (AST-built SQL), python.sql §3.1 (AST end to end; no text round-trip of emitted SQL), §3.2 (dialect
  quirks only in `dialects/`), §3.8 (one reserved-keyword set), §3.13 (rendered SQL executes verbatim through one
  door), system §3.20 (one treatment per model source), §3.21 (importers preserve meaning).
- The generator's statement is finished as text: `_finish_statement` renders, then fits identifiers
  (`rewrite_emitted_sql`); `_apply_policy` reparses that text. The engine's model-typing probe therefore cannot be
  wrapped as AST after generation without the reparse §3.1 forbids.
- `install_reserved_keywords` (run when `slayer.sql` loads) makes sqlglot quote reserved-word identifiers in any
  AST it renders, so an AST-built table name `order` renders `"order"` with no further rule.
- `classify_model_sql` already refuses unparseable `model.sql` before the save-time trial runs.
- sqlglot (30.17) has no expression-context parse entry that reads `REPLACE(a, 'x', 'y')` on SQLite/MySQL as a
  function (`into=` targets fail or mis-parse), so the two wrap-to-parse idioms must stay text-wrapped.

## Goals / Non-Goals

**Goals:**
- Every statement SLayer composes is an sqlglot AST rendered once.
- One bounding builder for type probes; a probe reaching the client is bounded by construction (typed).
- Table names and user SQL enter through one parse door the generator also uses.
- A static law makes statement-text composition fail CI.

**Non-Goals:**
- Making `_apply_policy` / `_finish_statement` AST end to end (the generator's post-render text fitting stays).
- Constant SQL strings (`SET TRANSACTION READ ONLY`, `server_profile_sql`): they compose nothing.
- EXPLAIN prefix/postfix wrapping of rendered SQL (not a statement-starting composition).
- The wrap-to-parse idioms `render/parse.py:131` and `sql_expr.py:64` (parse only; never executed).

## Decisions

### Building blocks

| Block | Where | Contract |
|---|---|---|
| `parse_table(name, *, dialect)` | `sql/render/parse.py` | `exp.to_table(name, dialect=…)` + `naming.quote_mixed_case_identifiers`. Used by `SQLGenerator._to_table` (now dialect-aware), `query_engine._side_stats_sql`, every probe. |
| `parse_query(sql, *, target_dialect, parse_dialect)` | `sql/render/parse.py` | `parse_expression` + a read-only query-root check (root in the read-only roots, no data-modifying node). `classify_model_sql` uses the same check; the root/modifying constants move here from `client.py`. Raises on anything else. |
| `SqlDialect.type_probe(query) -> exp.Select` | `sql/dialects/base.py` | `SELECT * FROM (<query>) AS _types LIMIT <type_probe_rows>`; `type_probe_rows` 0, `SqliteDialect` 1. T-SQL `TOP` comes from sqlglot. Replaces `_NEEDS_ROW_FOR_TYPES` / `_TSQL_DB_TYPES`. |
| `TypeProbe` / `mint_type_probe` | `sql/type_probe.py` (re-exported by `client.py`) | Frozen pydantic model holding a rendered statement (`sql: str`), never constructed directly: `mint_type_probe(statement, *, dialect, render)` checks the AST is a read-only query bounded to `dialect.type_probe_rows`, then renders it once. |
| `render_type_probe(query, *, dialect) -> TypeProbe` | `sql/client.py` | `dialect.type_probe(query)` rendered once. The only way an AST caller reaches the door. |
| `generate_planned_stages(..., type_probe=True)` | `sql/generator.py` | Applies `dialect.type_probe` to the composed statement AST before its one render; statement settings (sequence / correlated) attach to the outer statement. |
| `sql_model_trial_query(sql, *, dialect) -> exp.Select` | `sql/client.py` | `SELECT * FROM (<parse_query(sql)>) AS _sd_validate WHERE 1=0`; replaces `build_sql_model_trial_query`. |
| Type door | `sql/client.py` | `get_column_types` / names (sync + async) take a `TypeProbe` and execute `probe.sql` verbatim in the read-only rolled-back transaction; one call returns both driver type names and categories from a single execution. |

### Sites

| Site | Treatment |
|---|---|
| `importers/column_types.py` `LiveSource` | Source relation AST: `parse_table(sql_table)` or `Subquery(parse_query(sql), alias=_slayer_src)` (parse dialect = source dialect, target = datasource). `_described`: `SELECT *` over it, one execution for names + categories. `probe(expr)`: `SELECT (<parse_expression(expr)>) AS _slayer_probe FROM <source>`. Parse/read-only failure → `source()` reason string / `probe()` `None`. |
| `LiveSource._reflect` | `SchemaRef(catalog=…, name=…)` from `parse_table` parts (quotes stripped, dots in quoted parts kept); `introspect_table_to_model` takes `ref: SchemaRef | None` instead of a dotted `schema` token, callers migrated. A backend that cannot reach the catalog fails into the reason-string path. |
| `osi/converter.py` | `_probe_expression_type` and `_build_sql_mode_model`'s typing go through a `LiveSource` (datasource `target_dialect`; parse dialect = the dialect its stored SQL is in). The twin is deleted. |
| `engine/schema_drift.py` | `_sql_model_source_refs` yields AST nodes (table expression, column identifiers); probe `SELECT <cols> FROM <table> AS _sd_probe WHERE 1=0` via `render_type_probe`. |
| `engine/query_engine.py` `get_column_types` | `generate_planned_stages(..., type_probe=True)` returns a minted `TypeProbe`; a session policy re-mints it from its rewritten AST (`apply_session_policy_to_probe`). Same SQL shape as today's wrapper. |
| `query_engine.py` trial, `ingestion.sql_result_types` | `render_type_probe(sql_model_trial_query(...))`. |
| `sql/sqlite_introspect.py` | sqlite AST from raw table/column names (no pre-quoted `qtable`/`qcol`). |
| `facade/catalog_sql.py` | `exp.Create` + `ColumnDef`s; `exp.insert` with `exp.Placeholder` values (DuckDB positional `?`); the empty subquery as `exp.select(CAST(NULL AS VARCHAR) AS <ident>…).where(FALSE)`; no `parse_one`. |
| Dialect session hooks | `statement_timeout(seconds) -> exp.Expression | None` (postgres `SET LOCAL`, mysql/mariadb `SET`, snowflake `ALTER SESSION SET`), clickhouse `correlated_setting_probe(profile) -> exp.Select | None`; the `_sql` names retire; the client renders each once in the dialect. |
| `dialects/snowflake.py` `USE …` | `exp.Use` rendered in Snowflake; existing unquoted-identifier validation kept. |
| `pg_facade` command tag | `encode_command_complete(command="SELECT", rows=n)` — a wire tag, not SQL. |
| `storage/sqlite_storage.py:630`, `storage/sidecar_embedding_store.py:137` | sqlite AST with `exp.Placeholder`; parameters unchanged. |
| `demo/jaffle_shop.py` | DuckDB AST: `Update`, `Create` view, `Describe`, `Insert … Select`, `Drop`, `Select COUNT`. |

### Static law — `tests/test_law_sql_built_as_ast.py`

AST scan of `slayer/`, no file allowlist. Flags a composed string — `JoinedStr`, `BinOp` `+` / `%`, or
`"…".format(...)` — whose leading literal, after whitespace / `(`, starts with an uppercase SQL statement keyword
(`SELECT WITH INSERT UPDATE DELETE MERGE CREATE ALTER DROP TRUNCATE SET USE DESCRIBE EXPLAIN PRAGMA ATTACH DETACH COPY
GRANT REVOKE CALL EXEC EXECUTE SHOW VALUES BEGIN COMMIT ROLLBACK ANALYZE VACUUM REPLACE UPSERT`) followed by
whitespace or the end. Shape exemptions: an argument of `raise` / a logging call / `warnings.warn`; the label shape
`KEYWORD WORD:`; a composed string with exactly one interpolation passed directly as the text argument of
`sqlglot.parse_one` / `sqlglot.parse`. Planted cases: every form trips it; every exemption passes.

### Decisions log

1. All six issue groups AST-built plus a static law, no allowlist; rejected B (only `column_types.py` + the OSI twin:
   leaves four known violators and no guard). (user, DEV-2185 Q7 = A)
2. Wrap-to-parse idioms: shape exemption for a composed string passed directly to `sqlglot.parse_one` / `parse`;
   token-level composition rejected (couples to sqlglot internals, worse error text). (user)
3. The law also flags `%` and `.format` composition (extends the issue's "f-string or `+`"). (user)
4. arc42 wording: `6. **SQL is built as sqlglot AST**, never by string concatenation of fragments.
   [review] [enforced: test:tests/test_law_sql_built_as_ast.py]` (user, approved wording)
5. Probe bounding: one dialect hook `type_probe`; the generator applies it to the AST before its single render
   (`type_probe=True`) so the engine probe keeps today's wrapper shape; the door takes a typed `TypeProbe`, so an
   unbounded string cannot reach it. Replaces the earlier `SlayerQuery.limit` default (aggregate `LIMIT 0`
   equivalence unproven; public door contract changed silently). (codex)
6. Probe dialect quirks live in `dialects/` (`type_probe_rows`; T-SQL `TOP` via sqlglot). (default, sql §3.2)
7. One table / SQL parse door shared with the generator (`parse_table`, `parse_query`, `parse_expression`); the
   generator's `_to_table` parses with its dialect. (default, §3.20)
8. OSI's probe twin is replaced by `LiveSource`. (default, structural)
9. Drift refs carry AST nodes, not re-joined text. (default)
10. Sites the law finds beyond the issue list (Snowflake `USE`, own-store `?` queries, demo, ClickHouse `getSetting`)
    are made AST; the pg wire tag gets typed arguments (not SQL). (default; the issue prefers AST over allowlisting)
11. Message false positives are tuned by shape (raise / log / warn args, `KEYWORD WORD:` label), never by file.
    (default, per the issue)
12. Session hooks return AST and drop the `_sql` suffix; constant-returning hooks untouched. (default)
13. Spec: MODIFIED importer requirement "Column types come from the database first" with new scenarios, not
    `skip_specs`. (default)
14. `LiveSource.probe` composes `FROM <source relation>`; tested for `sql_table=` and `sql=` sources with a
    non-literal expression. (codex)
15. `catalog.schema.table` reflection goes through `SchemaRef` (catalog + name) structurally; no fabricated dotted
    schema; a backend that cannot reach the catalog takes the reason-string path. (codex)
16. `parse_query` requires a read-only query root, sharing one check with `classify_model_sql`; a rejection takes
    the skipped / `None` paths. (codex)
17. Row-bound matrix tests: SQLite `LIMIT 1`, others `LIMIT 0`, T-SQL `TOP 0` with no trailing `LIMIT`. (codex)
18. `_described` takes driver type names and categories from ONE execution (scope extension). (codex, user)
19. Catalog insert execution test: several rows, mixed-case / quoted columns, values containing `?` and `'`. (codex)
20. The parse exemption is tightened to exactly one interpolation (the wrap-one-fragment shape). (codex, user)
21. Reserved-word table names need no new rule: AST-built identifiers are quoted by the installed one reserved set
    (sql §3.8). (default)
22. Rejected (codex): route probe parsing through `ScopeFrame.enter_*` — sql §3.4 governs entry into a generator
    scope; a probe has none, and `parse_expression` is the same normalization the generator uses for `model.sql`.
23. Rejected (codex): drop `[review]` beside the new tag — §4 allows several tags per clause (python.sql §3.1 does
    the same) and the user approved the wording.
24. `TypeProbe` is minted only from a checked AST (read-only root, literal row bound equal to `type_probe_rows`); direct
    construction is refused and no emitted text is parsed for the check (python.sql §3.1, the client re-parse law).
    Rejected: a parse-based check in `client.py`. (codex, user)

## Risks / Trade-offs

- [Parsing and re-rendering user `sql=` changes the probed text vs today's verbatim wrap] → it is exactly what the
  generator executes for an sql-mode model (§3.20); `classify_model_sql` already refuses what sqlglot cannot read.
- [Generator `_to_table` becoming dialect-aware could shift goldens] → re-bless only for quoting; any other diff is
  a bug.
- [`TypeProbe` is minted in three places (`render_type_probe`, the generator, the session policy)] → each goes through
  `mint_type_probe`, which checks the AST before its render.
- [Catalog reachability differs per backend] → calibrated per backend in tests; unsupported falls back with a reason.
- [Law false positives on future messages] → tuned by shape; a genuine non-SQL hit is restructured (e.g. typed
  arguments), never allowlisted.
