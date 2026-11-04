## Why

Reviewing the Mode-B grammar (DEV-2101, PR #509) before merge found grammar and cast bugs that give wrong or dialect-dependent results: the CAST type grammar refuses valid SQL types with a misleading error and silently drops precision, `a --b` reads as `a - (-b)` although `--` starts a SQL comment, `TRY_CAST('1.5' AS INT)` is 1, 2 or NULL depending on the dialect, and SLayer's canonical expression text drops grouping parentheses, so `(a + b) * c` and `a + b * c` print — and are identified — alike. The other findings of that review are split out (DEV-2186, DEV-2187, DEV-2188, DEV-2189).

## What Changes

- The CAST / `::` type grammar accepts parameters between type words (`TIMESTAMP(6) WITH TIME ZONE`, `CHARACTER VARYING(10)`, `DECIMAL(10,2) UNSIGNED`) and a one-dimensional `[]` suffix; a malformed type gets an error about the type.
- A cast whose rendering for the target dialect would drop or change any part of the author's type is refused at plan time with a typed error (the existing "cannot express" rule, now applied to lossy renderings too).
- `CAST(x AS T[])` is accepted where the database has array types and is typed as a list of `T`'s type, usable with the list functions; `T[][]` and `TRY_CAST` to an array type are refused.
- Mode B accepts SQL comments: `--` to end of line and `/* … */` (nesting), outside string literals and quoted identifiers. **BREAKING** for unreleased Mode-B text only: `a --b` now means `a`; stored pre-grammar text keeps its meaning through the stored-text upgrade.
- `TRY_CAST` to an integer type accepts only integer text (optionally signed digits, surrounding spaces ignored); `'1.5'`, `'1e3'` and a fractional number yield NULL on every Tier-1 dialect.
- SLayer's canonical expression text keeps the grouping an expression parses to (`(a + b) * c`, `-(-b)`).

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `queries/casts`: the type grammar, lossy rendering as "cannot express", array targets and their typing, `TRY_CAST` to arrays refused, the integer-family validity rule for `TRY_CAST`.
- `queries/expression-grammar`: SQL comments; canonical expression text preserves grouping.
- `models/stored-upgrade`: stored pre-grammar text with adjacent minus signs keeps its meaning.

## Impact

- `slayer/core/syntax.py` — type grammar, comment masking, precedence-aware canonical printer.
- `slayer/core/keys.py` — `cast_data_type`, `CastKey` element type, `list_element_type`.
- `slayer/engine/binding.py` (`_bind_cast`), `slayer/engine/key_metadata.py` — array cast typing.
- `slayer/sql/dialects/*.py`, `slayer/sql/render/row_expr.py` — per-dialect cast-target expressibility hook, integer-text `TRY_CAST` guards.
- `slayer/storage/sql_string_migration.py` — adjacent-minus re-emission.
- `docs/concepts/references.md`.
- No API or schema change; no new dependency.
