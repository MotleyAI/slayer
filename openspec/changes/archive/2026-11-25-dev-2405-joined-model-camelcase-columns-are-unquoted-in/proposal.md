## Why

On a case-folding dialect (Postgres, Snowflake, Oracle) a plain column of a **joined** model with a
mixed-case name fails with an undefined-column error: the projection, `GROUP BY`, `ORDER BY` source and
time truncation read it unquoted (`accounts.extRef`), while `WHERE` and aggregate arguments quote the
same column (`accounts."extRef"`). The host model's own columns are quoted everywhere. Every joined
camelCase dimension on such a source fails — reported on 1.9.1 / `1.x`, which this change targets.

## What Changes

- A plain column reference — on the host or across any join path — renders as the same identifier in
  every clause that reads it, quoted exactly as the dialect quotes any other emitted column identifier.
- The generator's second plain-column renderer is removed; every plain-column render goes through the
  scope resolver, which already quotes mixed-case names and registers the joins it crosses.
- Base-select row columns resolve before the `FROM` is built, so their joins register as a side effect
  of resolving; the separate joined-path collector is removed.
- A plain column whose terminal model or leaf does not resolve fails closed with a typed error in every
  clause, including filters.
- Lowercase column names emit byte-identical SQL; the mixed-case quoting rule itself is unchanged.
- Out of scope: the forward-port to `main` (a different SQL shape, its own PR after this merges), the
  1.x version bump, session-policy (RLS) ruleset identifiers.

## Capabilities

### New Capabilities

### Modified Capabilities
- `sql/statement-assembly`: adds the requirement that a column reference renders the same in every
  clause of one relational scope.

## Impact

- `slayer/sql/generator.py` — the base SELECT, ranked CTE, window producers, raw time-column and hidden
  `ORDER BY` paths resolve plain columns through a `ScopeFrame`; the second renderer and the joined-path
  collector are deleted.
- `slayer/sql/scope.py` — the plain-column branch fails closed on an unresolvable terminal model or leaf.
- Tests: dry-run emission on Postgres and Snowflake for every route, and a Postgres execution test
  (pytest-postgresql) on camelCase tables.
- No API, wire, storage or docs change.
