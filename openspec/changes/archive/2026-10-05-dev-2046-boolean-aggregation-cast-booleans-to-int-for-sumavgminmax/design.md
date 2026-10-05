## Context

See proposal.md › Why. Today the decision "what does this aggregate return" lives in
`classify_aggregation` (keyed on the aggregation name only) read by `aggregated_type`,
`stage_measure_type` and `_infer_aggregated_format`, plus the facade's independent
`_agg_output_type`. Applying an aggregate to a raw value is hand-built in
`generator._build_agg`, `value_expr._render_builtin_aggregate`, the association producer's
level-1 `MAX` pick (`generator._render_association_producer_body`) and the HAVING seam.
"Is this boolean" lives only in binding (`_expression_is_confidently_boolean`, best-effort,
`any(...)` over coalesce-family arguments — unsound). Probes: DuckDB returns `True` for
`CAST(SUM(flag) AS BOOLEAN)`; the HAVING path renders `SUM(t.flag)` without the slot cast;
SQL Server already emits `SUM(orders.amount) > 50 AS [...]` for a boolean measure (invalid).

Applicable principles: sql P1 (AST hooks take typed operands), P2 (dialect quirks only in
`dialects/`), P5 (one renderer), P9 (fail closed); engine P1 (typed pipeline); core P1 (keys
carry identity only); system P6 (AST-built SQL). No arc42 / `.c4` edit is needed: the facade
(`protocols`) already may import `core` and `engine`.

## Goals / Non-Goals

**Goals:** one boolean-type authority, one aggregate classifier, one aggregate-application
helper — so no render site or type surface can disagree again.

**Non-Goals:** changing `count` semantics over booleans (counts non-NULL, as today);
Oracle predicate-as-value (Oracle 23ai accepts boolean expressions; older Oracle has no boolean
columns).

## Decisions

1. **`boolean_valued(key, *, column_type)` in `slayer/core/keys.py`**, beside `temporal_type`,
   total over value-key kinds (see the spec's recognition requirement). Branch rule: an
   expression is boolean only if every returnable branch is (`iif` branches; all args of
   `coalesce` / `ifnull` / `greatest` / `least`; `nullif`'s first arg). Columns resolve through
   the `ColumnTypeFn`, so host, joined and stage columns share one path. Binding's
   `_expression_is_confidently_boolean` is deleted; the binding gate, the type lifts and the
   renderer all call this function. *Alternative rejected (Codex):* stamping an input kind on
   the plan — it adds a second store of type facts keyed by aggregate that HAVING, composites
   and producers would all have to consult; the input type is a pure function of the key and
   declared column types, exactly as `temporal_type` already feeds rendering.

2. **`classify_aggregation(*, measure_name, aggregation, source_type)`**. With
   `source_type == BOOLEAN`: `sum` → `COUNT` (INT / INTEGER — a count of trues, reusing the
   existing class); `avg` → `FLOAT_SOURCE_UNITS` with a PERCENT format default when the column
   declares none; `min` / `max` → `PRESERVING`. `aggregated_type`, `stage_measure_type`,
   `measure_key_type` (including expression sources, typed via decision 1) and
   `_infer_aggregated_format` pass the source type. The facade's `_agg_output_type` is deleted
   and `INFORMATION_SCHEMA.METRICS.data_type` reads the engine's typing; a custom aggregation
   therefore reports the source type there, as the engine does (Codex's preserve-`None`
   suggestion rejected: that disagreement is what this change removes).

3. **One aggregate-application helper in `slayer/sql/render/aggregates.py`** taking the
   registry entry, the input expression and the input's `DataType`. A BOOLEAN input to any
   numeric aggregation is read as `CAST(input AS INT)` — applied where the builders read the
   aggregated value (`AggRenderSpec.input_type`, set from decision 1 where the spec is built),
   so the simple, stat, dialect-hook (`median` / `percentile`) and `weighted_avg` builders all
   receive the integer; for `min` / `max` the aggregate is wrapped back to the dialect's declared
   BOOLEAN cast (`declared_cast_type`), so the expression is boolean wherever it is used and the
   slot cast (idempotent `_wrap_cast_for_type`) does not double it. The count family,
   `first` / `last` and custom aggregations receive the raw boolean. `_build_agg`,
   `_render_builtin_aggregate` (and the HAVING seam that uses them), the association producer's
   level-1 pick and the ranked `first` / `last` pick all apply aggregates through it; the pick's
   level-2 aggregate receives the picked value's BOOLEAN type. *Alternative rejected:* per-dialect
   `bool_and` / `bool_or` — sqlglot's `LogicalOr` renders `LOGICAL_OR` on T-SQL and
   ClickHouse (invalid there); the cast round-trip transpiles on all twelve dialects probed.

4. **Predicates are booleans, not a separate input kind.** A comparison / connective / IN
   source is lowered exactly like a boolean column (decision 3). Where a dialect cannot
   hold a predicate as a value, that is a dialect quirk handled once (decision 5) — so
   `count(amount > 15)` and custom aggregations receive the predicate like any boolean.

5. **SQL Server predicate values in `slayer/sql/dialects/tsql.py`**: one target rewrite over
   the assembled statement, before its single render, replaces each predicate node
   (comparison, connective, NOT, IN, BETWEEN — reachable from Mode-A column SQL — LIKE, IS)
   whose parent is a value position with
   `CAST(CASE WHEN p THEN 1 WHEN NOT p THEN 0 END AS BIT)`; condition positions (WHERE, HAVING,
   JOIN ON, CASE WHEN condition, and an operand of `and` / `or` / `not`) are left alone — a
   comparison's operand is a value. NULL stays NULL (neither WHEN matches).

6. **Grammar and sources.** `_AGG_SOURCE_KINDS` gains `Cmp` and `BoolOp` (a bare `TupleLit`
   stays rejected). A `Cmp` binds to `ArithmeticKey`, `InKey` or the transient
   `TimePointCmpKey`; the latter two pass every source gate on the row-level and
   re-aggregation paths (folds DEV-1970): `InKey` joins `EXPRESSION_SOURCE_KINDS` (bind,
   naming, metadata, render) and `_AggregateSource`; `TimePointCmpKey` is admitted only at
   bind and in `_AggregateSource`, lowered by `resolve_time_points` before compilation (its
   fail-closed guard unchanged). Any shape still unsupported raises a typed SLayer error at
   bind, never a pydantic `ValidationError`. Mode B has no `BETWEEN` (the internal
   date-range `BetweenKey` was deleted by DEV-1999 and never DSL-reachable): a range is
   `lo <= x and x <= hi`, and the stale `BETWEEN` claims of `consecutive_periods` (spec,
   `formulas.md`, the predicate-shape error strings) are removed. *Alternative rejected
   (Codex-reviewed):* a SQL `x [not] between a and b` rewrite to two comparisons — a
   grammar feature needing a precedence-aware rewrite (a token-local one silently mis-scopes
   `a - b between 1 and 2`), outside this change.

7. **Gates.** `DEFAULT_AGGREGATIONS_BY_TYPE[BOOLEAN]` is the numeric set.
   `_reject_non_numeric_expression_agg` treats a boolean-valued expression as numeric; only text
   and temporal expressions stay rejected. A non-aggregatable bound source raises
   `AggregationNotAllowedError` at bind.

8. **Booleans in numeric positions.** Outside aggregation, a boolean-valued operand (decision 1)
   of `+ - * / %`, a boolean argument in a numeric scalar position (the math scalars; a
   `coalesce` / `ifnull` / `nullif` / `greatest` / `least` / `iif` branch mixed with numeric
   ones), and a boolean compared with a number (`flag = 1`, `p in (1, 0)`) render as
   `CAST(x AS INT)` and type as INT; a boolean compared with a boolean stays boolean. One
   lowering pass in the row and composite renderers, keyed on decision 1. `consecutive_periods`
   accepts these shapes (a boolean-shaped node in a numeric position is a value); it still rejects
   a top-level string-family call.

9. **Expression sources as AST.** `AggRenderSpec.value` carries an expression source's rendered
   AST; `_spec_value` / `_resolve_value_ast` read it directly, so a SLayer-rendered expression is
   never re-parsed (`sum(True)` no longer becomes a column `"TRUE"`). Derived-column expansions,
   string parameters and the `unmangle_dotted_table_refs` repair stay with DEV-1972, which gets a
   comment.

10. **All-NULL inputs.** A built-in aggregation skips NULL inputs, so an all-NULL cell takes the
    empty value (0 for the count family, NULL otherwise) in every location — local, cross-model,
    stage, window, partition, association; boolean `sum` is a `SUM`, never coerced to 0. Stated
    as semantics Axiom 4, enforced by `tests/test_dev2046_all_null_inputs.py`.

## Risks / Trade-offs

- [The source type is unknown at a render site (e.g. a `ColumnSqlKey` spec sets
  `column_type=None` to avoid re-casting)] → the helper takes the input type from decision 1
  over the source key, never from `spec.column_type`; a test per render site pins it.
- [The T-SQL rewrite misclassifies a position] → emission tests for each value position
  (projection, aggregate argument, cast operand, arithmetic operand) and each condition
  position; execution only in the path-gated `integration-sqlserver` workflow (no local ODBC
  driver).
- [Format change surfaces in goldens] → boolean `avg` gains PERCENT and boolean `sum` INTEGER;
  re-bless only goldens whose old value was the bug.
- [`tests/test_dev1847_gate.py:51-52` asserts the old row-level comparison rejection] → flips to
  acceptance (user-approved in planning).
- [Existing rejection tests and goldens for boolean-in-numeric shapes] → flip to acceptance, as
  listed in proposal.md › Impact (user-approved 2026-10-05).
