## Why

The portable Mode-B library's default lowerings live on the `SqlDialect` base class and are Postgres SQL, so Presto,
Athena, Redshift, Databricks, Spark and Oracle silently emit Postgres-only functions (`JSONB_*`, `REGEXP_MATCH`,
`REGEXP_I_LIKE`, `'g'` flags) or neutral SQL whose meaning differs there. This breaks system principle 10.1 and the
new 10.2 ("a dialect without its own translation of a Mode B construct refuses it with a typed error"). The
pre-release audit of the DEV-2101 / 2119 / 2120 / 2137 stack also found wrong Postgres list typing, a `collect`
fallback that cannot unify with exact numerics on BigQuery, missing MySQL version gates, out-of-domain math that
errors instead of yielding NULL, SQL Server truncating long JSON scalars to NULL, and a stale `database-support.md`.

## What Changes

- Every dialect declares the library functions it translates; any other library call or `TRY_CAST` (including the
  text-to-number coercion that lowers through it) fails at plan time with a typed error naming the function and the
  database. The base class keeps only neutral AST; Postgres spellings move to the Postgres dialect.
- Presto / Athena get the Presto-family lowerings Trino uses (one regex language with `(?s)`, `JSON_EXTRACT_SCALAR`,
  …) and refuse what Presto lacks.
- **BREAKING** (Tier 2): Redshift, Databricks, Spark and Oracle translate only `like`, `ilike`, `trim`, `ltrim`,
  `rtrim`, `replace`, `instr` / `strpos`, `greatest` and `least`; every other library function, `TRY_CAST` and
  text-to-number coercion is refused (it emitted Postgres SQL before).
- MySQL reads its server version: regex functions need 8.0, `json_value` 8.0.21.
- Postgres `contains_any` / `contains_all` work across `VARCHAR[]` / `TEXT[]` and `BIGINT[]` / `INTEGER[]`.
- `collect`'s empty-list fallback takes the aggregate's own type (BigQuery `NUMERIC` / `BIGNUMERIC` columns).
- `ln`, `log`, `log10`, `log2`, `sqrt` and `pow` / `power` yield NULL out of domain on every dialect; a literal
  out-of-domain call fails at plan time. Overflow stays each database's own.
- SQL Server: a `json_value` scalar over 4,000 characters raises instead of reading NULL (2022+); `split_part` has no
  4,000-character limit.
- Postgres array result type codes for `timestamptz[]`, `bpchar[]`, `uuid[]`, `json[]`, `jsonb[]`, `bytea[]`; the
  ClickHouse array cell reader keeps every escape.
- `docs/database-support.md` gains a per-dialect support table, checked against the code by a test, and is brought in
  line with the code.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/scalar-functions`: a library function runs only where its dialect translates it; out-of-domain math
  functions; MySQL version gates.
- `queries/list-functions`: `contains_any` / `contains_all` over differing physical array types.
- `aggregations/collect`: the empty fallback over exact numerics; the databases that refuse `collect`.
- `models/list-columns`: the databases that reject list constructs.
- `queries/json`: SQL Server scalars over 4,000 characters.
- `queries/regex`: the one pattern meaning holds on every dialect that runs the regex functions (Presto included).
- `queries/mixed-type-coercion`: coercion runs where `TRY_CAST` is translated, else is refused.

## Impact

- Code: `slayer/sql/dialects/*` (base, postgres, trino, _tier2, mysql, tsql, sqlite, duckdb, bigquery, snowflake,
  clickhouse, `_clickhouse_cells`), `slayer/sql/render/{library,row_expr}.py`, the elaboration-time support check,
  `slayer/core/scalar_args.py`, `slayer/sql/client.py`.
- Docs: `docs/database-support.md`, `docs/concepts/references.md`.
- Architecture: `architecture/system.arc42.md` 10.2 becomes `[enforced: test:tests/test_dev2169_dialect_library_support.py]`.
- Tier-2 users lose library functions that emitted Postgres SQL before (most failed at the database).
