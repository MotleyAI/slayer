## Why

Mode B passes most scalars to sqlglot and documents where the databases disagree (`greatest` / `least` NULLs, SQLite `LIKE` case, LIKE metacharacters), and it has no regex, JSON, TEXT-vs-number coercion or common string / math functions — so agents fall back to query-time Mode-A SQL (which DEV-2021 removes) and comparison-matrix row C9 "Portable expression library" is only `partial`. This change gives every Mode-B function, operator and pattern language one documented meaning on every Tier-1 dialect, verified per dialect.

## What Changes

- Global rules for the library: Postgres is the reference meaning when databases disagree; an out-of-domain argument yields NULL on every dialect (a literal one is a plan-time error); a version-gated feature fails closed at plan time when the server version is known.
- Strings: `left`, `right`, `starts_with`, `ends_with`, `contains`, `strpos`, `lpad`, `rpad`, `reverse`, `repeat`, `split_part` (non-zero integer-literal index), `concat_ws`, `chr`, `ascii` (Unicode code points).
- Math: `div` (truncates toward zero), `pi`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `cbrt`.
- `date_trunc(unit, x)` as a spelling of the granularity call `<unit>(x)`.
- **BREAKING** `greatest` / `least` ignore NULL arguments on every dialect (SQLite, MySQL, BigQuery and ClickHouse before 24.12 propagated NULL); SQL Server before 2022 is a plan-time error.
- Regex: `regexp_like`, `regexp_extract`, `regexp_replace`, the `~` / `~*` / `!~` / `!~*` operators, one portable pattern subset re-emitted per dialect; SQL Server needs 2025+.
- JSON: `json_value(x, path [RETURNING type])`, `->` / `->>` chains, the `json_extract` / `json_extract_scalar` / `json_extract_path_text` / `jsonb_extract_path_text` aliases; ingestion keeps `db_type` for JSON-family columns.
- **BREAKING** TEXT against a number compares and computes numerically (`TRY_CAST(text AS DOUBLE)`), on every dialect.
- **BREAKING** One LIKE pattern language: `%` and `_` the only wildcards, `\` escapes `%`, `_` and `\`, `[` always literal (DuckDB, Trino, SQLite and SQL Server read these differently today). SQLite Mode-B `LIKE` becomes case-sensitive; Mode-A SQL is untouched.
- Comparison matrix: C9 SLayer verdict → yes, with `C9-slayer-*` probes.
- Not added: `initcap` (no single-expression form on MySQL, SQL Server, DuckDB), `random()`, format-string parsers, JSON builders and table functions.

## Capabilities

### New Capabilities
- `queries/scalar-functions`: the library's global rules (reference meaning, out-of-domain NULL, version gating) and its string, math and `greatest` / `least` functions.
- `queries/regex`: regex functions and operators, the portable pattern subset, its one meaning, replacement strings.
- `queries/json`: `json_value`, path subset, `RETURNING`, `->` / `->>` chains, aliases, input forms.
- `queries/mixed-type-coercion`: TEXT against a number in comparisons and arithmetic.
- `queries/string-matching`: collation rule, the LIKE / ILIKE pattern language, SQLite case.

### Modified Capabilities
- `queries/date-functions`: `date_trunc(unit, x)` is the granularity call.

## Impact

- Code: `slayer/core/keys.py` (scalar registry), new pure-syntax modules in `slayer/core/` (regex pattern, JSON path, LIKE pattern), `slayer/core/syntax.py` (regex and JSON operators, `date_trunc`), `slayer/engine/binding.py` / `elaborate_env.py` (pattern and path validation, coercion typing), `slayer/sql/render/row_expr.py`, `slayer/sql/dialects/*` (per-function lowerings, SQLite registered functions, SQL Server version gate), `slayer/engine/ingestion.py` (JSON `db_type`).
- Docs: `docs/concepts/references.md`, `formulas.md`, `queries.md`, MCP Field descriptions, `slayer/memories/help_content/*`.
- Comparison: `examples/comparisons/probes.yaml`, `matrix.yaml`, `matrix.json`.
- Architecture: `system.arc42.md` principle 10 status tag.
- Behaviour changes for live queries: `greatest` / `least` with NULLs, SQLite `LIKE` case, LIKE backslash / `[`, TEXT vs number, SQL Server version errors.
