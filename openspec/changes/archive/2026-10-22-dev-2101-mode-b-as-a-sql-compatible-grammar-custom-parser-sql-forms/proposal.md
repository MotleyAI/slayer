## Why

Mode B is parsed by Python's `ast` behind a stack of text pre-passes, so SQL spellings work only in some positions (`=`, `IS NULL`, `AND`, `LIKE` only in filters), `"abc"` is a string, and there is no `CAST`, `BETWEEN`, `ILIKE`, `FILTER (WHERE …)` or `NULLS FIRST/LAST`. The SLayer SQL programme (DEV-1768) needs Mode B to be a SQL-compatible grammar, and replacing query-time Mode-A columns (DEV-2021) needs Mode B to express what agents put there — `CAST` alone blocks a third of those expressions.

## What Changes

- Mode B gets its own grammar (pyparsing), replacing `ast.parse` and every text pre-pass; one entry point for every position. The parser moves to `slayer/core/syntax.py` (declared child `core.syntax`), killing the legacy arrow `core.query → engine.syntax`.
- SQL forms are valid in every position (measures, dimensions, order, filters), with every existing spelling kept: `=`, `<>`, `IS [NOT] NULL`, `[NOT] IN`, `AND`/`OR`/`NOT`, `[NOT] LIKE` over any operand, `[NOT] ILIKE`, `||`, `[NOT] BETWEEN`, `IS [NOT] DISTINCT FROM`, `IS [NOT] TRUE|FALSE`, `count(distinct x)`, `CAST(x AS t)` / `x::t`, `EXTRACT(f FROM x)`, `INTERVAL '3 day'` / `INTERVAL '3' DAY` / `INTERVAL 3 DAY`, `DATE '…'` / `TIMESTAMP '…'`, `CURRENT_DATE` / `CURRENT_TIMESTAMP`, `POSITION(a IN b)`, `SUBSTRING(x FROM a [FOR b])`, `agg(x) FILTER (WHERE c)`.
- `CAST` / `::` are the database's own cast of the author's type (spelling transpiled to the dialect, parameters kept); `TRY_CAST(x AS t)` is the portable cast that yields NULL for invalid input on every dialect (Postgres before 16: a plain cast).
- `ORDER BY … NULLS FIRST|LAST` through `OrderItem.nulls`, honoured on every dialect.
- **BREAKING** `"…"` is a quoted identifier everywhere; strings are `'…'` with `''` as the only escape (backslash is literal). Stored Mode-B text is migrated; a live double-quoted name that resolves to nothing errors with a "use single quotes for strings" hint.
- **BREAKING** `{var}` placeholders bind into the parse tree and supply literal values only: inside a string literal, in a bare number / boolean position, or as a whole `IN` list. Anything else is an error.
- **BREAKING** `|` as concatenation and `//` are rejected with hints (`||`; `div(a, b)` / `floor(a / b)`); stored `|` is migrated to `||`. `**` is lowered to `pow` (it crashed before).
- The importers' legacy parser (`parse_formula`, `FieldSpec`, the colon rewriters in `slayer/core/formula.py`) is deleted; cube / dbt / OSI importers use `parse_expr` with a namespace-aware dependency check (absorbs DEV-1831). **BREAKING** for Python callers of `parse_formula` (Storyline ports on its next bump).

## Capabilities

### New Capabilities
- `queries/expression-grammar`: the Mode-B grammar — SQL forms in every position, identifiers and quoting, string literals, precedence, legacy spellings and the forms rejected with hints.
- `queries/casts`: `CAST` / `::` as the database's cast of the author's type, the inferred result type, and the portable `TRY_CAST`.

### Modified Capabilities
- `queries/expression-keywords`: CASE conditions accept SQL spellings natively; LIKE rewriting is replaced by the grammar's LIKE / ILIKE operators.
- `queries/variables`: placeholders bind as literals in three positions; the textual Mode-B escaping regime is gone.
- `aggregations/functional-form`: `agg(x) FILTER (WHERE c)`.
- `queries/date-functions`: SQL interval and typed-literal spellings; `CURRENT_DATE` / `CURRENT_TIMESTAMP`.
- `queries/positions`: order items carry an explicit NULL placement.
- `models/stored-upgrade`: stored Mode-B text is upgraded to the new string and identifier rules.

## Impact

- Code: `slayer/engine/syntax.py` → `slayer/core/syntax.py`; `slayer/core/query.py`, `slayer/core/keys.py` (cast key), `slayer/core/formula.py` (parser deleted; transform tables to `slayer/core/enums.py`), `slayer/engine/binding.py`, `bind_inputs.py`, `population.py`, `model_reads.py`, `schema_drift.py`, `unrelated.py`, `query_engine.py`, `slayer/memories/resolver.py`, `slayer/ir/variables.py`, `slayer/sql/render/*`, `slayer/sql/dialects/*` (casts, NULL ordering, server profile on Postgres), `slayer/sql/client.py`, `slayer/storage/migrations.py` + a new migration module, `slayer/dbt|cube|osi/converter.py`, `slayer/facade/translator.py` (emitted Mode-B text).
- Dependencies: `pyparsing` added.
- Architecture: `model.c4`, `index.yaml` (legacy baseline 8 → 7), `system.arc42.md` P2 / P10, `core.arc42.md`, `engine.arc42.md`.
- External: Storyline's `parse_formula` use (`storyline/api/adapters/models.py`); agents writing double-quoted strings or backslash escapes.
- Follow-ups stacked on this change: DEV-2119 (expressions in every column position), DEV-2120 (portable function library).
