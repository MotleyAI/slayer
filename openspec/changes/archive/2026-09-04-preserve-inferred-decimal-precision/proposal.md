# Proposal: preserve-inferred-decimal-precision

## Why

Aggregates over exact NUMERIC/DECIMAL columns are cast to the inferred logical
DOUBLE, silently losing precision beyond 2^53 (GitHub issue #364). PR #365 added
the preservation mechanism (`Column.db_type` capture + `preserve_native_type`
flag suppressing the cast), but review found two gaps: ClickHouse
`Nullable(...)`/`LowCardinality(...)`-wrapped decimals store the wrapped string
so preservation never fires, and the DECIMAL/NUMERIC detection is duplicated in
four places with divergent semantics (exact-set vs substring), so ClickHouse
`Decimal32/64/128/256` and BigQuery `BIGNUMERIC` are captured on one path but
dropped on another.

## What Changes

- One shared detection helper `is_exact_numeric_db_type(db_type: str | None)`
  in `slayer/engine/introspect_utils.py` with containment semantics
  (`"DECIMAL" in token or "NUMERIC" in token`, token = pre-paren, uppercased),
  replacing all five in-tree checks (`_sa_type_is_exact_numeric`,
  `_sa_type_is_float`, `_sa_type_to_data_type` scale branch,
  `_info_schema_type`, `_get_columns_fallback`, and
  `prebound.measure_key_preserves_native_type`); the `_NUMERIC_DECIMAL_TYPES`
  frozenset is deleted.
- Unwrap ClickHouse wrappers before storing `db_type` at both inspector call
  sites, uniformly (exact-numeric AND opaque branches), so
  `Nullable(Decimal(18, 2))` stores `Decimal(18, 2)` and preservation fires.
- Sonar S107 suppression on `ValueRegistry.intern` (14 keyword-only params).
- Hoist pre-existing in-function imports in `slayer/engine/ingestion.py` and
  `tests/test_ingestion.py` to module top; `ALLOW(import-not-top)` waivers for
  the genuinely circular `schema_drift` imports and the optional-dep
  `search.service` import.

## Capabilities

### New Capabilities

- `aggregations/native-type-preservation`: inferred (non-explicit) aggregates
  over exact-numeric columns keep the database's native exact type instead of
  being cast to the lossy inferred logical type; explicit types still cast.

### Modified Capabilities

(none)

## Impact

- `slayer/engine/ingestion.py` (helper call sites, uniform unwrap, import
  hoists), `slayer/engine/introspect_utils.py` (new helper, fallback capture),
  `slayer/engine/prebound.py` (`measure_key_preserves_native_type`),
  `slayer/engine/planning.py` (NOSONAR comment only).
- Tests: `tests/test_ingestion.py` (new unit coverage + import hoists),
  `tests/integration/test_integration_clickhouse.py` (one end-to-end decimal
  test; touching it path-triggers the ClickHouse CI workflow).
- Behaviour widening: ClickHouse wrapped/short decimals and BigQuery
  `BIGNUMERIC` now preserve; CH opaque columns store the unwrapped inner type
  string. Non-goals: SQL Server/Postgres MONEY family (DOUBLE cast is
  load-bearing there), explicit `type:` casts, Phase.ROW, INT inference.
