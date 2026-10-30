## Context

`SqlDialect` (`slayer/sql/dialects/base.py`) mixes two kinds of library default: neutral sqlglot AST, which sqlglot
spells per dialect, and verbatim Postgres spellings (`exp.Anonymous` `REGEXP_MATCH`, `JSONB_*`, `SPLIT_PART`,
`CONCAT_WS`, `REPEAT`, `CHR`, `ASCII`, `CBRT`, `PI`, `LPAD`, `REVERSE`, `CARDINALITY` / `ARRAY_REMOVE`, `&&` / `@>`,
`PG_INPUT_IS_VALID`, `REGEXP_REPLACE … 'g'`). A dialect without an override silently emits Postgres. Probes on the
stack tip showed neutral compositions are not enough on Tier 2 either: Oracle reads `''` as NULL, Spark's `CHR` is
mod 256, Redshift's `SPLIT_PART` rejects a negative index, Oracle's `GREATEST` propagates NULL, Oracle has no `PI` /
`CBRT`. So support must be a per-function opt-in, and the base must hold no vendor spelling.

Arc42 grounding: system 10 / 10.1 / 10.2 (one library, one meaning, translate-or-refuse), 13 (dispatch fails closed);
python.sql 2 (quirks only in `dialects/`, a data-shaped Tier-2 table), 9 (fail closed), 1 (AST end to end).

Live probes recorded during planning (local Postgres 16, DuckDB, SQL Server 2022 / 2025 in Docker):
- Postgres `varchar[] && ARRAY['a']` → "No operator matches"; `CAST(v AS TEXT[]) && ARRAY['a']` and
  `b @> CAST(ARRAY[1] AS BIGINT[])` work.
- Postgres `COALESCE(array_agg(numeric), '{}')` → `numeric[]`; `COALESCE(array_agg(numeric), CAST(ARRAY[] AS DOUBLE
  PRECISION[]))` also coerces. DuckDB `COALESCE(list(decimal), [])` → `DECIMAL(10,2)[]`.
- SQL Server 2022 and 2025: lax `JSON_VALUE` of a 5,000-character scalar → NULL; `'strict …'` raises "String value in
  the specified JSON path would be truncated" (13625); `RETURNING` is a syntax error on both; an `OPENJSON` scalar
  subquery reads it but is rejected inside `GROUP BY`, and T-SQL emission repeats dimension expressions in `GROUP BY`.
  The guard `CASE WHEN lax IS NOT NULL THEN lax WHEN JSON_PATH_EXISTS(doc, p) = 1 AND JSON_QUERY(doc, p) IS NULL THEN
  JSON_VALUE(doc, 'strict p') END` reads short / object / null / boolean / missing correctly and raises on the long
  scalar, grouped, on both versions.
- Overflow is not uniform: DuckDB `exp(1000.0)`, `1e308*10`, `pow(10.0, 400.0)` → `inf`; Postgres raises for all three.

## Goals / Non-Goals

**Goals:**
- Every registered dialect (plus MariaDB) × every portable-library construct emits SQL written for that dialect or
  refuses with `UnsupportedServerFunctionError` / `ListTypeError` / `ServerVersionError` — enforced by
  `tests/test_dev2169_dialect_library_support.py`, which tags system 10.2 `[enforced: …]`.
- The base class holds neutral AST or a refusal, never a vendor spelling, in every hook group.
- Items 3–9 of DEV-2169 fixed, `docs/database-support.md` matching the code, checked by a test.

**Non-Goals:**
- Tier-2 translations beyond the approved set (decision 1); Tier-2 meaning gaps in passthroughs, operators, date
  functions and aggregates (decision 2) — documented, e.g. Oracle `'' IS NULL`.
- Unifying overflow behaviour (decision 5).
- GIN-index-preserving list casts on Postgres (decision 3).
- A Presto CI job (decision 13): Presto is checked against a throwaway local container.
- Drift attribution of SQLite UDF errors (DEV-2170).

## Decisions

### Mechanism

- **D1 — declared support set + one check.** `SqlDialect.library_functions: ClassVar[frozenset[str]]` names the
  library functions the dialect translates: the `LIBRARY_FUNCTIONS` names (text / math / extremum / `split_part` /
  `like` / `ilike` / `json_value` / regex — `trim`, `ltrim`, `rtrim`, `replace`, `strpos`, `instr` are `TextFn`
  members) plus `try_cast`. Base default: empty. One check, `require_library(dialect, name)`, raises
  `UnsupportedServerFunctionError` ("`<fn>()` is not supported on <database> yet.", suggesting `Column.sql`); the
  coercion message names the text-to-number operation. It runs at elaboration after every rewriting / coercion pass,
  over the final keys (coercers insert `CastKey(try_=True)` after `check_list_operands`), and again at render
  dispatch (`render_library_call`, `render_cast`), so stored or planner-built keys cannot bypass it. Aliases and
  operators (`~`, `~*`, `->>`, `json_extract*`, `POSITION`, `strpos`) reach it through their canonical key.
  Lists keep their `native_lists` gate (`ListTypeError`); its suggestion lists Presto. Version gates stay at render.
- **D2 — no vendor spelling on the base.** Neutral compositions stay on the base as helpers (`_left_right`,
  `portable_pad`, `_non_positive_empty`, `field_count`, `skipped_group_empty`, the `asin` / `acos` / `div` / `chr`
  guards, LIKE with `ESCAPE '\'`, …). Every vendor-spelled primitive hook on the base raises the typed error; the
  Postgres spellings move to `PostgresDialect`; each other dialect that inherited one states its own (DuckDB,
  BigQuery, Snowflake, ClickHouse, MySQL, T-SQL, SQLite, the Presto family), verified by the existing Tier-1 matrix.
  The same holds for the list hooks (`list_length`, `list_contains`, `list_overlaps`, `list_contains_all`,
  `build_collect`, `empty_list`, and any Postgres-only part of `list_relation` / `list_exists`).
  `extremum_propagates_null: ClassVar[bool]` replaces the per-dialect `build_extremum` overrides (Trino family,
  MySQL, Oracle → `coalesced_extremum`). Hooks outside the library (passthroughs, date functions, aggregates) stay
  neutral AST; a scan asserts none of their base defaults is an `exp.Anonymous` vendor spelling.
- **D3 — the sets.** Tier 1 (Postgres, DuckDB, BigQuery, Snowflake, ClickHouse, Trino, SQL Server, MySQL, SQLite):
  every function it translates today. MariaDB: MySQL's set minus regex and `json_value` (its numeric-`TRY_CAST`
  refusal stays its own hook). Presto / Athena: the Trino overrides and `regex_flavour = JAVA` move to
  `PrestoFamilyDialect`, `TrinoDialect` keeps what differs; functions Presto lacks (checked against prestodb docs and
  a local container) are left out. Redshift, Databricks, Spark, Oracle: `like`, `ilike`, `trim`, `ltrim`, `rtrim`,
  `replace`, `instr` / `strpos`, `greatest`, `least`, each a translation for that dialect.

### Items

- **MySQL gates (item 3).** `MysqlDialect` reads `SELECT VERSION()` (`server_profile_sql` / `parse_server_profile`);
  `profile_needed` covers the gated keys, including implicit numeric `TRY_CAST`s, aliases and regex operators.
  `ServerProfile.version` becomes a normalised `(major, minor, patch)`; Postgres / SQL Server parsers adapt and
  `require_version` prints all parts. Regex (and numeric `TRY_CAST`'s `REGEXP_LIKE` guard) need 8.0, `json_value`
  8.0.21; below → `ServerVersionError`, unknown → emit. MariaDB keeps its refusals; its `10.x-MariaDB` string never
  trips a MySQL gate.
- **Postgres list typing (item 4).** `contains_any` / `contains_all` cast both operands to the canonical Postgres
  array type of their shared SLayer element type (TEXT → `TEXT[]`, INT → `BIGINT[]`, DOUBLE → `DOUBLE PRECISION[]`,
  BOOLEAN / DATE / TIMESTAMP likewise); differing element types keep `list_each`.
- **`collect` fallback (item 5).** Each `build_collect` builds its own untyped empty-list fallback that takes the
  aggregate's type (Postgres `'{}'`, DuckDB / BigQuery `[]`, Trino `ARRAY[]`; ClickHouse / Snowflake aggregates
  already return `[]`). The join-back `EmptyList` fallback (`generator.py`) uses the same untyped empty list, so a cross-model `collect` takes the aggregate's type too; the typed `empty_list(element)` goes.
- **Type codes (item 6).** `_PG_OID_MAP` maps `timestamptz[]` 1185, `bpchar[]` 1014, `uuid[]` 2951, `json[]` 199,
  `jsonb[]` 3807, `bytea[]` 1001 to `list`.
- **ClickHouse escapes (item 7).** `_quoted` reads `\\`, `\'`, `\"`, `\n`, `\t`, `\r`, `\0`, `\b`, `\f`, `\a`, `\v`,
  `\e`, `\xHH`; an unknown escape is read as a live ClickHouse round-trip shows.
- **Domain guards (item 8).** `ln`, `log10`, `log2`, `log` (x ≤ 0; two-argument also b ≤ 0 or b = 1), `sqrt` (x < 0),
  `pow` / `power` (negative base with non-integer exponent; 0 to a negative exponent) take a NULLed argument out of
  domain — `SQRT(CASE WHEN x >= 0 THEN x END)` — robust to eager CASE evaluation (ClickHouse), applied after the
  log-spelling fix-up. SQLite `slayer_*` math UDFs return `None`. `check_domain` covers literal arguments (`pow` /
  `log` on the literal pair). Overflow is the database's own, one sentence in `references.md`.
- **SQL Server (item 9).** `json_value`: the strict re-read guard above on 2022+ and unknown version (the AST builds
  the `'strict …'` path literal from the parsed path); before 2022 the lax read stays, documented. `split_part`:
  nested `CHARINDEX` over `NVARCHAR(MAX)` (literal index → fixed depth; negative index via the field count).
  `json_value` also reads a JSON scalar document: `ISJSON(x, VALUE)` on 2022+ and unknown version; before 2022
  `ISJSON` takes only objects and arrays, so a scalar document reads NULL there, documented.
- **Docs (item 9).** `docs/database-support.md` per the issue's list, plus a per-dialect support table parsed by a
  unit test against every dialect's `library_functions`; `docs/concepts/references.md` out-of-domain list, overflow
  sentence and list-rejecting databases.

### Decisions log

1. Tier-2 policy: refuse by default, translate the pre-stack set now — options: refuse all / this / translate all;
   taken: this, no regression vs main, 10.2 holds. (user)
2. 10.2 scope = the portable library; passthroughs, operators, date functions, aggregates stay sqlglot-translated,
   with a no-vendor-spelling scan — options: this / every Mode B construct; taken: this. (user)
3. Postgres list typing: cast both operands to the canonical array type — options: this / cast to the first
   operand's `db_type`; taken: this, one path, no `db_type` plumbing, GIN index unused. (user)
4. SQL Server long JSON scalar: fail loud via strict re-read on 2022+ — options: this / document only; taken: this. (user)
5. Overflow: domain only, overflow stays the database's — options: this / guard `exp` / `pow` overflow; taken: this. (user)
6. Docs support table kept in step by a parsing test — options: this / by hand; taken: this. (user)
7. Mechanism D1 + D2: declared support set (data) + one check + vendor primitives refuse on the base, Postgres
   spellings in `PostgresDialect` — the structural fix; satisfies python.sql 2's data-shaped Tier-2. (default)
8. `collect` fallback typed by the aggregate (untyped empty list), not by DataType or `db_type`. (default)
9. T-SQL `split_part` via nested `CHARINDEX`, no 4,000-character cliff. (default)
10. MySQL version gates mirror T-SQL; `ServerProfile.version` widened. (default)
11. `extremum_propagates_null` flag replaces per-dialect `build_extremum` overrides. (default)
12. Oracle `'' IS NULL` is a documented caveat, not a refusal (consequence of 2). (default)
13. Presto checked via a throwaway local container, no CI job. (default)
14. System 10.2 wording: "A dialect without its own translation of a Mode B construct refuses it with a typed error;
    a base-class default is never another dialect's SQL." (user, approved wording)
15. `ilike` stays in the Tier-2 set although the stack (not pre-stack main) added it. (user)
16. System 10.2 tag: `[target: DEV-2169]` → `[enforced: test:tests/test_dev2169_dialect_library_support.py]`.
    (user, approved wording)
17. The support check runs after every rewriting / coercion pass, over the final keys. (codex)
18. One check called at elaboration and at render dispatch, so stored / planner keys cannot bypass it. (codex)
19. `collect` builds its own untyped fallback, and the join-back `EmptyList` fallback uses it too (the typed `empty_list` goes): a typed fallback there fails on UUID / exact numerics as the aggregate's did. (codex; amended in pr-implement, user)
20. Domain guards NULL the argument, not only wrap the call; applied after the log-spelling fix-up. (codex)
21. The all-dialect test covers the generator's `list_relation` / `list_exists` paths: list children, element
    access, child filters, nested hops, stages. (codex)
22. SQL Server strict-reread integration cases: missing, JSON null, object / array, malformed, short / long scalar,
    root / nested / quoted paths, GROUP BY / HAVING / ORDER BY, RETURNING. (codex)
23. `ServerProfile.version` normalised to `(major, minor, patch)`; messages print all parts. (codex)
24. Every spelling reaches the same gate decision (tested); the claim that `trim` / `replace` / `instr` sit outside
    the library is rejected — they are `TextFn` members. (codex, partly rejected)
25. Presto / Tier-2 opt-ins assert an expected shape plus forbidden tokens, not only a sqlglot parse; doc references
    go in the PR description, not test data. (codex, partly rejected)
26. ClickHouse unknown-escape handling settled by a live round-trip (Windows paths, doubled / trailing backslash,
    malformed `\x`), pinned by the test. (codex)
27. Rejected (codex): an unknown server version should refuse — the issue and the existing scalar-functions
    requirement say unknown emits and the database decides. Folded: `profile_needed` sees implicit numeric
    `TRY_CAST`s, aliases and regex operators. (codex, rejected)
28. SQL Server `json_value` over a JSON scalar document (`"x"`): `ISJSON` without a type rejects it, found live
    in pr-tests — options: fix here / leave; taken: fix here via `ISJSON(x, VALUE)` on 2022+. (user)

## Risks / Trade-offs

- Tier-2 users lose library functions that emitted Postgres SQL before (most failed at the database anyway); the
  refusal names `Column.sql` as the way out. → Documented in the support table.
- Tier-2 and Presto translations are verified only by vendor docs, sqlglot parse, shape / forbidden-token assertions
  and (Presto) a local container — no CI live run. → Kept to a small, pre-stack set.
- Moving Postgres spellings off the base touches every Tier-1 dialect's inheritance. → The Tier-1 library matrix and
  live suites are the regression net; the all-dialect test catches accidental inheritance either way.
- The SQL Server guard triples the JSON read; below 2022 long scalars still read NULL. → Documented.
- Casting a Postgres list column to its canonical array type prevents GIN index use for `&&` / `@>`.
