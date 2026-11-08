# Fit over-limit internal CTE column aliases at emission

## Why

Internal CTE column aliases concatenate canonical key names (up to 305 bytes in the
`lift/nested_attach` goldens); DEV-1756 fits only the outermost projection aliases.
Postgres truncates identifiers to 63 bytes silently, and the discriminating part of a
canonical alias (partition keys, composite suffix) sits at the end — exactly what
truncation discards — so two distinct internal aliases sharing their first 63 bytes
collapse: "ambiguous column" at best, silently wrong binding at worst. Nothing guards it.

## What Changes

- The final emission pass (`SqlDialect.rewrite_emitted_sql`) additionally scans the
  assembled SQL for over-limit quoted identifiers (string literals and comments masked)
  and fits each through the pure `fit_identifier`, so definitions and references stay
  consistent by construction.
- The generator builds an explicit exemption set from every user-authored raw-SQL
  surface in the bundle (model `sql`/`sql_table`/`filters`, column `name`/`sql`/`filter`,
  resolved post-extension-overlay models including `stage_source_models`); exempt
  identifiers pass through byte-identical. Exempt wins on a spelling tie.
- `substitute_quoted` becomes literal/comment-safe (masking shared with the scan) —
  also hardening the existing DEV-1756 projection-alias pass.
- Two fail-closed guards: an `IdentifierCollisionError` when fitted forms collide (with
  each other or any existing token), and an always-on post-rewrite backstop rejecting
  any surviving non-exempt over-limit identifier.
- Golden re-bless limited to `lift/nested_attach` × postgres/tsql/bigquery/duckdb.

## Capabilities

### New Capabilities

- `queries/identifier-fitting`: how SLayer-minted SQL identifiers (projection aliases
  and internal CTE columns) are fitted to each dialect's identifier byte limit at
  emission, how user-authored identifiers are exempted, and how failures surface.

### Modified Capabilities

(none)

## Impact

- `slayer/sql/_identifier_fit.py` (scan + masking helpers), `slayer/sql/dialects/base.py`
  (`rewrite_emitted_sql` extension, backstop), `slayer/sql/generator.py` (exemption-set
  build, threading, backstop call).
- Emitted SQL changes only where an internal alias exceeds the dialect limit (4 golden
  entries); result keys, plan-level canonical names, and dev-1871 alias-stability pins
  are untouched.
- No arc42/c4 edits: the change lives inside the existing naming/emission seams
  (sql principles 1, 3, 9).
