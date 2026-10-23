## Context

See proposal.md for motivation. After DEV-2101 (merged into this branch): Mode B is parsed by `slayer/core/syntax.py`; casts are `CastKey(source, type_text, data_type, try_)` rendered through `SqlDialect.build_try_cast` (input read as text first); the engine fetches a cached `ServerProfile` only when the dialect sets `try_cast_reads_profile` and `plans_try_cast(planned)` finds a `TRY_CAST` key. Scalars are `ScalarCallKey`s over the `SCALAR_SPECS` allowlist in `slayer/core/keys.py`, rendered by `render_scalar_call` as `exp.func(name)` plus sqlglot transpilation (`rewrite_target_ast`); date functions already use typed dialect hooks (`build_date_part`, …). SQLite functions are registered in `register_sqlite_udfs`. Ingestion keeps `Column.db_type` only where `DataType` is lossy (opaque types, exact NUMERIC). Booleans in a numeric context are read as 1 / 0 at render (`numeric_parts`).

## Goals / Non-Goals

**Goals:** one portable meaning per function, operator and pattern language, held by construction (explicit lowerings, re-emitted patterns) and enforced by a registry-driven test matrix.

**Non-Goals:** a DECIMAL or JSON member of `DataType`; capturing column collations; Postgres versions before 16 getting NULL-on-error semantics; JSON builders / table functions; `initcap`.

## Decisions

### D1. Every function has an explicit portable lowering

The symptom (`greatest` / `least` NULLs, `LIKE` case and metacharacters, regex flavours) is one structural cause: semantics delegated to sqlglot's per-dialect translation, so "same meaning everywhere" was never a property of the code. Each library function now has an explicit lowering: `ScalarSpec` gains the function's result-type rule; `render_scalar_call` dispatches each new function to a typed `SqlDialect` hook per function family (sql P1: operands are rendered `Expression`s and typed values, never a function-name `str`), with the base class holding the default form and each Tier-1 dialect file overriding only its quirks (sql P2). Existing passthrough scalars keep their path. The matrix test (`tests/test_dev2120_library_matrix.py`) iterates `SCALAR_SPECS` and the Mode-B operators, fails for any entry without cases, executes the cases on SQLite and DuckDB, and emits them for every Tier-1 dialect; the integration suites execute the same case table live. This flips system P10's "every Mode B construct means the same on every dialect" from `[target: DEV-2120]` to `[enforced: test:tests/test_dev2120_library_matrix.py]` (approved).

Alternative: keep sqlglot passthrough and fix instances — rejected: every new function would re-open the bug class.

### D2. Patterns and paths are parsed in core, emitted per dialect

Three pure-syntax parsers in `slayer/core/` (no SLayer imports beyond core): regex patterns, LIKE patterns and JSON paths, each producing a small frozen Pydantic AST, `lru_cache`d. The binder validates every literal and variable-supplied pattern / path at plan time (typed errors naming the construct and, for variables, the variable). Keys keep the literal text (identity unchanged, core P1); the renderer re-parses through the cache and the dialect emits from the AST:

- **Regex** — one emitter in `slayer/sql/dialects/` parameterised by a per-dialect flavour record: Postgres ARE, Snowflake POSIX, RE2 (DuckDB, ClickHouse, BigQuery, SQL Server 2025), Java / Joni (Trino), ICU (MySQL), Python `re` (SQLite). The record says how to spell dot-matches-newline (`(?s)`, match parameter `'n'` / `'s'`, or native), the string-end anchors (`$`, `\z`, `\Z`, `\A`), case-insensitivity, and the replacement-reference syntax. `\d \w \s` always emit as explicit ASCII classes (inside brackets as ranges); `\n \t \r` as the engine's escape or a literal character. Replacement strings are re-emitted the same way (`\1` → `$1` on MySQL / Trino, literal `$` / `&` / `\` escaped per engine).
- **LIKE** — a literal pattern is re-emitted; a column-valued pattern is translated at run time (SQL Server `REPLACE(p, '[', '[[]')`, SQLite inside `slayer_like_glob`).
- **JSON paths** — emitted as the dialect's path or key-list form.

Alternative: pass pattern text through with `(?s)` prefixes — rejected: `\d` inside brackets, `$` before a trailing newline and Unicode classes differ under identical text.

### D3. Spellings collapse in the parser (engine P2)

`~` / `~*` / `!~` / `!~*` parse to `regexp_like` (negated by `NOT`); `->` / `->>` chains to `json_value(x, path)` (the path assembled from the operands; a chain ending in `->` is a parse error with the `->>` hint); `json_extract` / `json_extract_scalar` / `json_extract_path_text` / `jsonb_extract_path_text` to `json_value`; `date_trunc(unit, x)` to the granularity call `<unit>(x)`; `strpos` to `instr`. The arrows sit at the `||` level of the precedence ladder, left-associative, so every Postgres-accepted expression parses with Postgres's meaning. `json_value(x, p RETURNING t)` parses to `TRY_CAST(json_value(x, p) AS t)` — a `CastKey` over the TEXT `json_value` (system P13: no new key kind; its meaning is exactly `TRY_CAST` of the text). Dialects may lower that pair natively where equivalent (ClickHouse `JSONExtract(x, …, 'Nullable(T)')` guarded by `JSONType`, SQLite `json_extract` + `json_type` so booleans and numbers never go through text, Snowflake `GET_PATH` then `build_try_cast`).

### D4. Coercion is a TRY_CAST key inserted by the checker

Where the checker types a comparison / `IN` / `BETWEEN` / `IS DISTINCT FROM` / arithmetic / `greatest` / `least` operand as TEXT against INT / DOUBLE, it wraps the TEXT operand in `CastKey(type_text="DOUBLE", data_type=DOUBLE, try_=True)`; a TEXT literal is converted to a number literal at plan time or raises a `QueryTypeError` (engine P9). Because the coercion is an ordinary `TRY_CAST` key, `plans_try_cast` sees it (the Postgres profile is fetched), the existing cast renderer emits it, and arithmetic types DOUBLE through the cast's type. An author's `CastKey` is never touched.

Alternative: wrap at render like `numeric_parts` — rejected: invisible to the profile predicate and to the checker's typing.

### D5. Version gating through the server profile

`try_cast_reads_profile` and `plans_try_cast` generalise to one dialect predicate over the planned keys, "does rendering this plan read the profile". Postgres answers for `TRY_CAST` keys; SQL Server for regex and `greatest` / `least`. SQL Server gains `server_profile_sql` (`SERVERPROPERTY('ProductMajorVersion')`) and `parse_server_profile`. The version check raises a typed plan-time error naming the function and the version; an unknown profile emits. `regexp_like` on SQL Server lowers to `REGEXP_INSTR(…) > 0`, which runs at every compatibility level (`REGEXP_LIKE` needs level 170), so the profile carries no compatibility level.

### D6. Out-of-domain → NULL

Dialects guard the domain where the database raises or returns a non-NULL substitute: `asin` / `acos` by `CASE WHEN x BETWEEN -1 AND 1`, `div` by `NULLIF(b, 0)`, `chr` by the code-point range minus surrogates, `split_part` index by the plan-time literal check. A literal argument is checked in the binder and raises.

### D7. greatest / least

On NULL-propagating engines (SQLite scalar `MAX` / `MIN`, MySQL, BigQuery, ClickHouse before 24.12 or with the legacy setting) the form is `GREATEST(COALESCE(a, b, c), COALESCE(b, a, c), COALESCE(c, a, b))`: every term is non-NULL unless all arguments are, and each non-NULL argument appears as its own term. Snowflake keeps `GREATEST_IGNORE_NULLS`. SQL Server before 2022 is the D5 version error. Coercion (D4) applies to mixed TEXT / number arguments.

### D8. SQLite

Registered deterministic functions on the existing connect hook: `slayer_regexp_like`, `slayer_regexp_extract`, `slayer_regexp_replace` (Python `re`, the pattern already re-emitted for the Python flavour), `slayer_like_glob` (LIKE pattern → GLOB pattern: `%`→`*`, `_`→`?`, `*`, `?`, `[` bracketed, backslash escapes resolved), plus any string function SQLite lacks (`concat_ws` on old SQLite builds, code-point `chr` / `ascii` where the built-ins differ). Mode-B `LIKE` renders `x GLOB slayer_like_glob(p)` (deterministic, so a constant pattern is evaluated once per statement); `ILIKE` renders native `LIKE`. Mode-A `LIKE` is untouched because nothing connection-wide changes.

### D9. LIKE on the other dialects

A typed `build_like(value, pattern, case_insensitive)` hook. Base: `LIKE … ESCAPE '\'` (Postgres, MySQL, DuckDB, Snowflake, Trino); `ILIKE` native where it exists, else `LOWER(x) LIKE LOWER(p)`. ClickHouse and BigQuery: native backslash escaping, no `ESCAPE` clause. SQL Server: `ESCAPE '\'` and `[` bracketed (literal patterns re-emitted, column patterns through `REPLACE`).

### D10. JSON inputs and ingestion

Ingestion keeps `db_type` for JSON-family types (Postgres `json` / `jsonb`, MySQL / DuckDB / BigQuery `JSON`, Snowflake `VARIANT` / `OBJECT` / `ARRAY`, ClickHouse `JSON`); `json_value` accepts an opaque (UNKNOWN) column whose `db_type` is JSON-family. Lowering reads the operand's `db_type`: Postgres `CAST(x AS JSONB)` for every input (a no-op on `jsonb`) with `jsonb_typeof` excluding objects / arrays / null and, on 16+, `pg_input_is_valid(x, 'jsonb')` guarding malformed text; Snowflake `VARIANT` direct, else `TRY_PARSE_JSON(TO_VARCHAR(x))`; ClickHouse `JSON` column vs `String`; others take TEXT and native JSON alike. A missing `db_type` reads as TEXT.

### D11. Verification

Unit: matrix on SQLite and DuckDB plus emission on every dialect. Integration (live, same case table): Postgres (local pytest-postgresql, 16+; emission for 15 and unknown), MySQL 8.0, ClickHouse, Trino via testcontainers; SQL Server parameterised over 2022 (version error) and 2025 (regex); Snowflake and BigQuery in their secret-gated CI suites. C9 flips only after a PR run in which the Snowflake and BigQuery suites executed (not skipped).

### D12. Digit-and-comma braces in string literals

A brace pair inside a Mode-B string literal whose contents are only digits, commas and spaces (`{3}`, `{2,}`, `{1,2}`, `{1, 2, 3}`, `{}`) is literal text, so regex quantifiers work as written; it is an invalid-variable-name error today, so nothing that runs changes meaning. Name-shaped pairs stay placeholders, other pairs keep the invalid-name error, `{{` / `}}` keep escaping, and bare braces outside a string literal are unchanged. An undefined-variable error from a regex pattern argument adds the `{{` / `}}` hint.

Alternatives: doubling quantifier braces (`'[0-9]{{3}}'`) — rejected: standard regex fails as written; every non-name pair literal — rejected: a mistyped placeholder (`'{start-date}'`) would silently become text; exempting pattern positions from substitution — rejected: substitution precedes parsing and would forbid placeholders in patterns.

### D13. BigQuery TIMESTAMP is DATETIME

SLayer's TIMESTAMP is naive, so on BigQuery every SLayer-produced timestamp is `DATETIME`: `CURRENT_DATETIME()`, `DATETIME_ADD` / `DATETIME_DIFF` / `DATETIME_TRUNC`, and `SAFE_CAST(… AS DATETIME)` for `TRY_CAST` / `RETURNING` (a plain `CAST` stays the database's own). Ingestion keeps `db_type` for the dialect's zoned type (`SqlDialect.zoned_timestamp_db_types`, BigQuery `TIMESTAMP`), and the binder casts the other side of a comparison with such a column to `TIMESTAMP`, so the column stays bare and partition pruning holds. Query time zones are DEV-2000.

## Risks / Trade-offs

- [Golden SQL churn: every Mode-B `LIKE` gains `ESCAPE '\'`; SQLite `LIKE` becomes `GLOB`] → regenerate baselines; release note.
- [Behaviour changes for live queries: `greatest` / `least` NULLs, SQLite `LIKE` case, LIKE `\` / `[`, TEXT vs number] → release notes; no stored-text migration (stored text keeps parsing; its meaning moves to the portable one by design).
- [Trino's default regex library (Joni) and Java-style anchors] → the flavour record emits `\A` / `\z`; trailing-newline probes in the Trino suite.
- [SQLite registered Python functions are slower than native] → only for regex and LIKE; constant patterns are evaluated once.
- [DOUBLE coercion loses precision beyond 2^53] → documented, `TRY_CAST(x AS BIGINT)` remedy.
- [JSON number to TEXT differs by database (`1.0` vs `1`)] → documented; `RETURNING DOUBLE` is the exact form.
- [`db_type` absent on models ingested before this change] → TEXT form; Snowflake's TEXT form also reads `VARIANT`; ClickHouse `JSON` columns need a re-ingest (documented).
- [MCP schema budget] → Field-description additions stay within `tests/test_mcp_surface_budget.py`.
- [DEV-2101's `//` hint names `div` before this change lands] → accepted; scenario asserts the hint works here.
