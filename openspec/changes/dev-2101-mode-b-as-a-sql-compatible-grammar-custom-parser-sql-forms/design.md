## Context

See proposal.md for motivation. Today `slayer/engine/syntax.py` runs about eight text pre-passes (colon, CASE, extract, count-distinct, star-args, LIKE, filter operators, `||`) and then `ast.parse`; filters alone go through `parse_filter_expr`. `slayer/core/formula.py` holds a second, older Python-AST parser (`parse_formula` / `FieldSpec`) used only by the cube, dbt and OSI importers (and, outside the repo, by Storyline). `{var}` substitution is textual (`escape="python"`), run before parsing. The IR already carries `OrderEntry.nulls`; nothing sets it. SQL generation already probes a cached per-engine `ServerProfile` (ClickHouse uses it to refuse correlated subqueries).

This change is PR 1 of three: DEV-2119 (expressions in every column position) and DEV-2120 (portable function library, regex, JSON, coercion, collation, C9) stack on it.

## Goals / Non-Goals

**Goals:** one Mode-B grammar and one parse tree for every position and every consumer; SQL forms in every position; SQL string / identifier quoting; literal-only, structural `{var}` binding; casts; `NULLS FIRST/LAST`; stored text upgraded so no stored meaning changes.

**Non-Goals:** new scalar functions, regex, JSON, TEXT↔number coercion, collation (DEV-2120); expressions in column-only positions (DEV-2119); removing query-time Mode A (DEV-2021); a `DECIMAL` member of `DataType`.

## Decisions

### D1. The parser lives in `core` and is pure syntax

`slayer/engine/syntax.py` moves to `slayer/core/syntax.py` (declared child `core.syntax`) with `la:deterministic-refactor`. It imports only `slayer.core.*`: the transform tables (`ALL_TRANSFORMS`, `TIME_TRANSFORMS`, `TIMELESS_TRANSFORMS`) move from `core/formula.py` to `core/enums.py` next to `RANK_FAMILY_TRANSFORMS`. Engine P2 ("parsing is pure syntax") keeps governing it; `la-arch-check` model-truth enforces the boundary. Approved harness edits H1–H7: `model.c4` (`core.syntax` child; arrows `core.query → core.syntax`, `engine.binding → core.syntax`, `engine.bind_inputs → core.syntax`; legacy `core.query → engine.syntax` removed), `index.yaml` baseline 8 → 7, `system.arc42.md` P2 and P10, `core.arc42.md` §1 / §2, `engine.arc42.md` §2 / P9, regenerated diagrams. P10's "every Mode B construct means the same on every dialect" carries `[target: DEV-2120]`.

### D2. pyparsing grammar, one entry point

`Forward()` recursion; `infix_notation` ladder (loosest first) `OR`, `AND`, `NOT`, comparison / `IS` / `IN` / `BETWEEN` / `LIKE` / `ILIKE`, `||`, `+ -`, `* / %`, unary `-`, `**` (right-associative, right operand may be unary-negated: Python semantics, `-2**2 = -4`), `::`, postfix (call, `FILTER`, `.`); `ParserElement.enable_packrat()`; operator alternations as `Regex`; `lru_cache` on `parse_expr`. Keywords are case-insensitive, ASCII-exact (the DEV-1833 Unicode rules carry over) and reserved only in their slot, so `case`, `end`, `with`, `model`, `date`, … stay identifiers elsewhere; `CASE` is a conditional only when `WHEN` follows. CASE still lowers to `iif`, `EXTRACT` to `date_part`, `count(distinct x)` to `count_distinct`, `POSITION` / `SUBSTRING … FROM … FOR` to `instr` / `substr`, `BETWEEN` to its conjunction, `CURRENT_DATE` / `CURRENT_TIMESTAMP` to the clock calls, interval literals to `interval(n, unit)` — so every spelling collapses to one node and downstream stays spelling-insensitive (engine P2). `parse_filter_expr` is deleted; the kwarg-vs-comparison rule (an aggregation / transform / unknown call reads `name =` after `(` or `,` as a kwarg; a scalar call and everything else reads `=` as comparison) is the one rule for every position. Syntax errors carry the pyparsing location.

Alternatives: keep `ast.parse` with more pre-passes (the status quo the issue retires); sqlglot as the parser (it would read Mode-B constructs — kwargs, colon, transforms, `{var}` — as SQL and need its own rewrite layer).

### D3. New node and key kinds

Casts are a new `Cast` parsed node and a new `CastKey(source, type_text, data_type, try_)` (system P13: a cast is not a scalar call). The key implements `children()` / `map_children()`, equality, phase (its source's), binding, checker typing (`data_type` per `queries/casts`; UNKNOWN for unmapped types) and render dispatch. `FILTER (WHERE c)` attaches to the `AggCall` node as an optional `filter` predicate and to its aggregate key; `IS [NOT] DISTINCT FROM` reuses the dialect's null-safe-equality builder; `x IS TRUE` lowers to `x IS NOT NULL AND x` (`IS FALSE`: `x IS NOT NULL AND NOT x`), and the `NOT` forms negate those, so NULL never propagates.

### D4. Casts: the database's cast, plus a portable `TRY_CAST`

`CAST` / `::` render `exp.Cast` with the author's `exp.DataType` (sqlglot-parsed, transpiled by the target dialect's generator). The plan-time "type the dialect cannot express" check is: sqlglot raises, or the round-trip of the type through the target dialect loses it (unsupported-type warning) → typed error. `TRY_CAST` lowers per dialect: native `TRY_CAST` (DuckDB, SQL Server, Snowflake, Trino), `SAFE_CAST` (BigQuery), `accurateCastOrNull` (ClickHouse); Postgres ≥ 16 `CASE WHEN pg_input_is_valid(CAST(x AS TEXT), '<type>') THEN CAST(x AS <type>) END`; SQLite a registered `slayer_try_cast(value, target)` connection function; MySQL a numeric `REGEXP` guard (dates already yield NULL). The Postgres form needs the server version at render time: the Postgres dialect gains `server_profile_sql()` (`SELECT current_setting('server_version_num')`) and `parse_server_profile`; the engine fetches the cached profile before rendering a Postgres query that contains a `TRY_CAST` and passes it to the dialect; no profile (dry run without a connection, failed probe) → the plain `CAST`, which runs on every version. Guards cast the input to TEXT first so a non-text source never meets a regex.

Rejected alternatives (user rulings): normalising `CAST` value semantics (rounding, boolean words, ISO-only, NULL-on-error) — `CAST` must be the author's database cast; a calendar-exact regex on old Postgres — performance on recent versions; refusing on old Postgres — a plain cast is strictly better.

### D5. Strings, identifiers and `{var}` binding

Strings are SQL: `'…'`, `''` escape, backslash literal. `"…"` is a quoted identifier everywhere (`""` escape), producing the same `Ref` / `DottedRef` as a bare name. A quoted name that fails to resolve gets the "strings use single quotes" hint in the binder's unresolved-reference error.

Placeholders are masked before parsing with offset-preserving sentinels of the placeholder's exact length, so error positions and names stay true. The grammar admits a placeholder in three slots only: inside a string literal (a template literal of text and placeholder parts), a bare literal slot, and the whole `IN` list; anything else is a parse error naming "variables supply literal values". Values bind at the existing substitution points (`ir/variables.py`, `core/query.py`), now into the parse tree: a template literal renders the value's text; a bare slot takes a number or boolean (a numeric string reads as a number; a string value errors with the quote hint); the `IN` slot takes a list. Result names are computed from the text as written, before binding, for measures and computed dimensions alike (the `_template` / `template` plumbing stays, `ComputedDimension.substituted`'s text splice goes). `date_range` bounds are not Mode B: they keep their own literal-only substitution, then time-point validation. `escape="python"` for Mode B is deleted; Mode-A substitution is untouched.

### D6. Stored-text migration

New stored-only steps at SlayerModel 15 → 16, SlayerQuery 6 → 7 and Memory 4 → 5, registered in `slayer/storage/migrations.py` alongside `rank_direction_migration.py`, recursing like it does (`source_queries`, saved queries, inline `ModelExtension.measures`, memories' queries). The rewrite uses Python's `tokenize` over the legacy text: each STRING token is decoded with the old grammar's rules (`ast.literal_eval`) and re-emitted as a SQL single-quoted literal; an `OP` token `|` becomes `||`; everything else is copied byte-for-byte; untokenisable text is left unchanged. It runs before the existing load-path repairs (legacy time literals, aliases), which parse filter text with sqlglot and therefore already read `"…"` as an identifier. Mode-A fields are never visited.

### D7. NULL ordering

`OrderItem.nulls: Literal["first", "last"] | None` (validated; serialised; preserved by `_strip_order`, saved-query refinement, variable binding and every other path that rebuilds an order item) feeds `OrderEntry.nulls` at `engine/compile/stages.py`. `SqlDialect.build_ordered` returns a list of order terms; base returns one `exp.Ordered` with `nulls_first`; T-SQL emits a leading `CASE WHEN <resolved expression> IS NULL THEN … END` key (NULLs-first or NULLs-last per the policy) followed by the expression, built over the resolved ordering expression rather than its bracketed alias, fixing the alias mis-resolution noted in `tsql.py`.

### D8. One parser repo-wide (absorbs DEV-1831)

`parse_formula`, the `FieldSpec` classes, `_rewrite_funcstyle_aggregations`, `_preprocess_agg_refs` and `_expand_named_measures` are deleted. The importers call `parse_expr`; the dbt importer's dangling-reference and cycle checks are re-implemented as a namespace-aware dependency walk over `ParsedExpr` (named measures and custom-aggregation names as the namespace, with cycle detection), shared by the three importers. The facade translator's and the importers' emitted Mode-B text is checked against the new grammar (single-quoted strings, no Python escapes, no `|`).

## Risks / Trade-offs

- [The parser swap touches every query] → SQL golden baselines must regenerate byte-identically for every pre-existing shape; the existing parser tests port to the new parser and stay green.
- [pyparsing performance on long formulas] → packrat plus `lru_cache`; a benchmark test over the longest formulas in the suite and docs, compared with the old parser.
- [Live payloads with `"…"` strings or backslash escapes break] → typed errors with the single-quote hint; release note; the migration covers everything stored.
- [Storyline imports `parse_formula`] → accepted break, recorded on DEV-2034; the port is `parse_expr` plus `walk_parsed_refs`.
- [Postgres before 16: `TRY_CAST` raises on invalid input] → documented; a plain cast is the only form that runs there.
- [Render-time server profile on Postgres] → read from the existing per-engine cache; probed only when the query contains a `TRY_CAST`.
- [Retargeting tests that assert on deleted helpers] → listed individually in tasks.md, approved by the user.

## Migration Plan

Stored data upgrades on load (D6) and re-saves in the new spelling. No rollback step beyond the usual: an older SLayer opening a store re-saved by this version reads `'…'` strings correctly but fails on `"…"` identifiers, as for any forward-written store.
