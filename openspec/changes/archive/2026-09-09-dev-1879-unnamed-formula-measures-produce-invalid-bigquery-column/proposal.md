# Proposal: Sanitize auto-generated aliases of unnamed formula measures

## Why

An unnamed query measure whose formula is an arithmetic composite or transform
(`logo_churn:sum / logo_bop:sum`, `time_shift(cmrr_eop:sum, -1, 'year')`) derives
its auto-generated name from a defective text fallback that leaves `:`, `/`,
spaces, `-`, and quotes in the SQL projection alias. BigQuery rejects such
aliases (`400 Invalid field name`), so on BigQuery tenants every ratio and
period-over-period question written as an inline formula fails. Postgres accepts
any quoted alias, so existing tests never caught it.

## What Changes

- The auto-generated name of an unnamed formula measure whose bound root is not
  a plain aggregate becomes a sanitized identifier: the canonical formula text
  passed through the shared DEV-1826 sanitizer `auto_name_from_expression`
  (lowercase, `\W+` → `_`, digit guard, >48-char hash fold).
- **BREAKING**: result keys of such measures change on every dialect (uniformly),
  e.g. `mart.logo_churn:sum / logo_bop_sum` → `mart.logo_churn_sum_logo_bop_sum`.
  The old keys were malformed on BigQuery and awkward everywhere.
- Explicit `name`, saved measures, and every aggregate-rooted formula
  (`col:agg`, `*:count`, parametric, expression aggregation, cross-model) keep
  byte-identical aliases and result keys.
- The existing unnamed-collision guard (different formulas deriving the same
  key → error with the "set 'name'" remedy) and raw-formula-text referencing in
  `filters`/`order` are kept and pinned by tests.
- Docs: `docs/concepts/formulas.md` auto-naming sentence updated.

## Capabilities

### New Capabilities

- `queries/measure-naming`: auto-generated result keys and SQL aliases for
  unnamed formula measures — derivation rule, dialect uniformity, alias
  validity, collision behavior, raw-formula referencing.

### Modified Capabilities

<!-- none — existing capabilities' requirements are unchanged -->

## Impact

- `slayer/engine/stage_planner.py` — `_canonical_alias_for_formula` fallback
  rewritten to use `auto_name_from_expression` (`slayer/core/refs.py`).
- No changes to `slayer/sql/naming.py`, dialects, or the MCP surface.
- Result-key contract change for unnamed composite/transform measures on all
  dialects (see **BREAKING** above); existing tests asserting old keys updated.
- New tests: result-key units, collision, raw-formula referencing, BigQuery
  SQL-generation check, and a fixed formula-shape × dialect alias-validity
  battery.
