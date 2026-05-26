# Bug: model names that are reserved SQL keywords are emitted unquoted, producing invalid SQL

| | |
|---|---|
| **Reported** | 2026-05-22 |
| **Reporter** | artemy@motley.ai |
| **Component** | `slayer/sql/generator.py` (SQL rendering), `slayer/engine/column_expansion.py` (column qualification) |
| **Version / commit** | 0.6.9 / `a8b8f2c` (branch `main`) |
| **Dialect** | postgres (default), affects any dialect with reserved keywords |
| **Severity** | High — any query against a model whose name is a reserved SQL keyword fails outright |
| **Found via** | Storyline datasource ingestion in shadow-compare mode (SLayer as shadow); customer schema has a table `Grant` |

## Summary

When a model's name is a **reserved SQL keyword** (e.g. `grant`, `order`, `user`, `select`),
SLayer emits that name as an **unquoted** identifier in the generated SQL — both as the
`FROM` table alias and as the column qualifier. The database then rejects the statement
with a syntax error.

The trigger in the wild: a source table named `Grant` becomes a SLayer model named `grant`.
The table name itself is correctly quoted (`"Grant"`, because it is mixed-case), but the
derived alias/qualifier `grant` is not — and `grant` is a reserved keyword.

## Symptoms

Observed during a shadow-compare ingestion run:

```
'grant."idempotencyKey"' contains unsupported syntax. Falling back to parsing as a 'Command'.
shadow_compare: load_data shadow backend raised ProgrammingError:
(sqlalchemy.dialects.postgresql.asyncpg.ProgrammingError)
<class 'asyncpg.exceptions.PostgresSyntaxError'>: syntax error at or near "grant"
[SQL: SELECT
  grant ."idempotencyKey" AS "grant.idempotencyKey"
FROM "Grant" AS grant
GROUP BY
  grant ."idempotencyKey"
LIMIT 21]
```

There are **two distinct failure surfaces**, both caused by `grant` being an unquoted
reserved keyword:

### A. Rendering (the fatal error)

The generated SQL contains `FROM "Grant" AS grant` and `grant."idempotencyKey"`. PostgreSQL
rejects `grant` as an unquoted table alias / column qualifier because it is a reserved word →
`PostgresSyntaxError: syntax error at or near "grant"`.

### B. Parsing (the warning + the mangled `grant .` spacing)

The warning comes from `sqlglot.parse_one` being handed a qualified-column **string** of the
form `grant."idempotencyKey"`. Because `grant` begins a `GRANT` statement, sqlglot cannot
parse it as a column and falls back to a `Command`. The misparse is what leaks the tell-tale
`grant .` spacing (space before the dot) into the final SQL.

## Root cause

`grant` is a reserved SQL keyword, but SLayer builds it into an `exp.Identifier` **without
`quoted=True`**, and sqlglot's default generator only auto-quotes identifiers it deems unsafe
by *case / special-char* rules — it does **not** quote reserved keywords. A bare lowercase
`grant` looks "safe" to sqlglot, so it is emitted verbatim.

### Affected code

The FROM-clause builder for the simple single-model path:

```python
# slayer/sql/generator.py:1920
def _build_from_clause(self, enriched: EnrichedQuery) -> exp.Expression:
    if enriched.sql_table:
        return exp.to_table(enriched.sql_table, alias=enriched.model_name)   # <-- alias unquoted
    elif enriched.sql:
        parsed = self._parse(enriched.sql)
        return exp.Subquery(this=parsed, alias=exp.to_identifier(enriched.model_name))  # <-- unquoted
    ...
```

The column qualifier for a bare-column dimension:

```python
# slayer/sql/generator.py:2116
if sql is None:
    return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(model_name))  # <-- table unquoted
if sql.isidentifier():
    return exp.Column(this=exp.to_identifier(sql), table=exp.to_identifier(model_name))   # <-- table unquoted
```

The column-expansion qualifier rewrite:

```python
# slayer/engine/column_expansion.py:202
col.set("table", exp.to_identifier(canonical_alias))   # <-- unquoted
```

Other sites that build identifiers from model names the same way (cross-model / join / window
paths) and would fail identically for a reserved-keyword model name:

- `generator.py:669` — source subquery `alias=exp.to_identifier(cm.source_model_name)`
- `generator.py:672` — `exp.to_table(cm.source_sql_table, alias=cm.source_model_name)`
- `generator.py:679` — target subquery `alias=exp.to_identifier(cm.target_model_name)`
- `generator.py:682` — `exp.to_table(cm.target_model_sql_table, alias=cm.target_model_name)`
- `generator.py:685-686` — join `ON` column qualifiers `table=exp.to_identifier(cm.{source,target}_model_name)`
- `generator.py:803, 806` — join-target alias (last/ranked path)
- `generator.py:1210, 1213` — join-target alias (window path)

(The synthetic internal aliases `_src`, `_base`, `_w_time`, etc. are underscore-prefixed and
never reserved, so they are unaffected.)

## Minimal reproduction

```python
import sqlglot
from sqlglot import exp

# Mirrors SQLGenerator._build_from_clause + _resolve_sql for a bare-column dimension.
frm = exp.to_table('"Grant"', alias="grant", dialect="postgres")
sel = (
    exp.Select()
    .select(
        exp.Column(
            this=exp.to_identifier("idempotencyKey", quoted=True),
            table=exp.to_identifier("grant"),
        ).as_("grant.idempotencyKey")
    )
    .from_(frm)
)
print(sel.sql(dialect="postgres", pretty=True))
# SELECT
#   grant."idempotencyKey" AS "grant.idempotencyKey"
# FROM "Grant" AS grant      <-- invalid: `grant` is a reserved keyword

# sqlglot does NOT quote reserved-keyword identifiers by default:
for name in ["grant", "orders", "select"]:
    print(name, "->", exp.to_table('"T"', alias=name, dialect="postgres").sql(dialect="postgres"))
# grant  -> "T" AS grant
# orders -> "T" AS orders
# select -> "T" AS select

# And the parse-side fallback (surface B):
sqlglot.parse_one('grant."idempotencyKey"', dialect="postgres")
# WARNING: 'grant."idempotencyKey"' contains unsupported syntax. Falling back to parsing as a 'Command'.
# parsed as Command -> 'grant ."idempotencyKey"'   (note the `grant .` spacing)
```

(Reproduced with sqlglot 26.33.0; the no-quote-for-reserved-keywords behavior is sqlglot's
documented default and is stable across versions.)

## Suggested fix

Quote every identifier derived from a **model name** (alias and column qualifier), at every
site listed above. The lowest-risk approach is a single helper used uniformly:

```python
def _model_identifier(name: str) -> exp.Identifier:
    """Identifiers derived from model names may collide with reserved
    keywords (e.g. a `Grant` table -> model `grant`); always quote them."""
    return exp.to_identifier(name, quoted=True)
```

- Replace `alias=enriched.model_name` (string) with `alias=_model_identifier(enriched.model_name)`
  in `_build_from_clause`, and likewise the cross-model/join/window alias sites.
- Replace `table=exp.to_identifier(model_name)` with `table=_model_identifier(model_name)` in
  `_resolve_sql` and `column_expansion.py:202`.

Surface B additionally needs the **parse** sites that round-trip a qualified column **string**
through `sqlglot.parse_one` (e.g. the `self._parse(sql)` branch in `_resolve_sql`, predicate
parsing, and column-expansion enrichment) to either build the qualifier as a quoted identifier
before parsing, or avoid the string round-trip. Otherwise the `Command` fallback can still
silently corrupt the expression even if the final render is quoted.

Note: rendering the final statement with `.sql(dialect=..., identify=True)` would blanket-quote
all identifiers and fix surface A, but it does **not** fix surface B (the parse fallback happens
before rendering) and broadly changes emitted SQL, so the targeted helper is preferred.

## Suggested regression test

Add a model whose name is a reserved keyword (e.g. a `Grant` table → model `grant`, with a
mixed-case column like `idempotencyKey`) and assert that:

1. A dimension-only query renders valid SQL (`FROM "Grant" AS "grant"`, `"grant"."idempotencyKey"`)
   and executes against Postgres without a syntax error.
2. No `Falling back to parsing as a 'Command'` warning is emitted while building the query.

Parametrizing over a few reserved words (`grant`, `order`, `user`, `select`) and dialects
(postgres, sqlite) would guard the cross-model/join/window paths too.
