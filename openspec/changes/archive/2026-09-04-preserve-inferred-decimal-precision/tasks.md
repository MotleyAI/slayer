# Tasks — preserve-inferred-decimal-precision

## 1. Tests first (spec-tests stage)

- [x] 1.1 Unit tests for `is_exact_numeric_db_type`: `"DECIMAL(18,2)"`,
      `"NUMERIC"`, `"Decimal64(4)"`, `"BIGNUMERIC"`, lowercase → True;
      `None`, `"DOUBLE"`, `"INT"`, `"MONEY"`, `"Nullable(Decimal(18, 2))"` →
      False. Verify: tests fail (helper absent), pass after 2.1.
- [x] 1.2 Unit tests for `_sa_type_is_exact_numeric` on clickhouse-sqlalchemy
      `Nullable(Decimal(18,2))` and `LowCardinality(Nullable(Decimal(...)))` →
      True. Verify: fail before 2.2, pass after.
- [x] 1.3 Unit test: inspector-path ingest of a CH-wrapped decimal column
      stores the bare inner string in `db_type`; wrapped opaque type stores
      the unwrapped string. Verify: fail before 2.3, pass after.
- [x] 1.4 Unit test: info_schema fallback with `data_type_str="Decimal64(4)"`
      stores `db_type` and maps a numeric logical type (not TEXT). Verify:
      fail before 2.2, pass after.
- [x] 1.5 Unit test: `measure_key_preserves_native_type` with
      `db_type="Decimal64(4)"` and `"BIGNUMERIC"` → True; `"Nullable(...)"`,
      `"MONEY"`, `None` → False. Verify: fail before 2.2, pass after.
- [x] 1.6 Integration test in `tests/integration/test_integration_clickhouse.py`:
      table with `Nullable(Decimal(18, 2))` and `Decimal64(4)` columns; ingest
      → bare `db_type` captured; `:sum` over each emits no lossy float cast and
      returns exact decimals. Verify: passes in the ClickHouse CI workflow.

## 2. Implementation (spec-implement stage)

- [x] 2.1 Add `is_exact_numeric_db_type` (containment semantics) to
      `slayer/engine/introspect_utils.py`. Verify: 1.1 green.
- [x] 2.2 Convert all detection call sites to the helper and delete
      `_NUMERIC_DECIMAL_TYPES`: `_sa_type_is_exact_numeric`,
      `_sa_type_is_float`, `_sa_type_to_data_type` scale branch,
      `_info_schema_type`, `_get_columns_fallback`,
      `measure_key_preserves_native_type`. Verify: 1.2/1.4/1.5 green, zero
      remaining grep hits for the frozenset.
- [x] 2.3 Uniform unwrap at both inspector call sites:
      `_raw_db_type_str(_unwrap_clickhouse_wrappers(col_type))`. Verify: 1.3
      green.
- [x] 2.4 `# NOSONAR(S107)` comment on `ValueRegistry.intern`
      (`slayer/engine/planning.py:169`). Verify: line present; Sonar issue
      clears on next PR analysis.
- [x] 2.5 Hoist in-function imports in `slayer/engine/ingestion.py`
      (`_resolve_fallback_ref`, `sqlite_introspect`, `engine_factory` ×3) and
      all 27 in `tests/test_ingestion.py`; add
      `# ALLOW(import-not-top): circular — schema_drift imports ingestion`
      on the 3 `schema_drift` imports and
      `# ALLOW(import-not-top): optional embedding extra off the cold-start
      path` on the `search.service` import. Verify: `check-conventions.sh 365`
      exits 0.
- [x] 2.6 Full verification: `poetry run pytest -m "not integration"` all
      green; `poetry run ruff check slayer/ tests/` clean.

## 3. Review convergence (spec-review stage)

- [x] 3.1 `/process-reviews` loop until CI, Sonar, CodeRabbit, Codex, and the
      conventions gate are all green; ClickHouse workflow green on the PR.
