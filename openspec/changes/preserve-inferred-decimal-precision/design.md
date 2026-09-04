# Design — preserve-inferred-decimal-precision

## Context

See proposal.md — Why. PR #365's mechanism (`Column.db_type` capture +
`preserve_native_type` flag suppressing the inferred cast) is sound; the gaps
are in *which* columns get `db_type` captured and *how* the string is matched.
Detection currently lives in five call sites across `ingestion.py`,
`introspect_utils.py`, and `prebound.py` with divergent semantics
(exact-set membership vs substring containment).

## Goals / Non-Goals

- Goal: one detection semantic, one helper, all call sites.
- Non-goals: MONEY/SMALLMONEY (pg drivers return locale strings; mssql
  `SUM(money)` doesn't promote precision → the DOUBLE cast is load-bearing);
  explicit `type:` casts; Phase.ROW; INT inference.

## Decisions

- **Containment, not prefix or exact-set.** Token = pre-paren, uppercased;
  match `"DECIMAL" in token or "NUMERIC" in token`. Exact-set misses
  `Decimal64`; prefix misses `BIGNUMERIC` (Codex review catch). Containment is
  what `_info_schema_type` already did, is a strict superset of both, and no
  real SQL type contains DECIMAL/NUMERIC without being exact-numeric.
- **Helper lives in `introspect_utils`.** It is the dependency-free leaf both
  `ingestion` and `prebound` may import (verified cycle-safe); the wrapped
  string deliberately does NOT match — unwrapping stays the caller's job so
  the helper remains a pure string predicate.
- **Uniform unwrap at the inspector call sites.** `db_type =
  _raw_db_type_str(_unwrap_clickhouse_wrappers(col_type))` for both the
  exact-numeric and opaque branches: one code path, and it converges with what
  the info_schema fallback path produces (bare inner type string).
- **Import hygiene via hoist + targeted waivers.** Hoist the mechanically safe
  in-function imports; waive only `schema_drift` (genuine cycle —
  `schema_drift` imports `ingestion` at module top) and `search.service`
  (documented optional-dep cold-start guard).

## Risks / Trade-offs

- [Widening: CH short decimals / BIGNUMERIC now preserve] → intended; pinned
  by unit scenarios and a real-ClickHouse integration test.
- [CH opaque columns' stored `db_type` changes from `Nullable(X)` to `X`] →
  metadata-only field; new value matches the fallback path's existing output.
- [Hoisted import reveals an indirect cycle at import time] → deterministic:
  full test run catches it; fall back to the pre-approved waiver form.
