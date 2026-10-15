## Context

See proposal.md — Why. The heals sit where each meaning already lives: parser grammar
(`slayer/engine/syntax.py`), binding (`binding.py` / `bind_inputs.py`), the query model's validators
and refinement merge (`slayer/core/query.py`), and model validation (`slayer/core/models.py`).

Architecture constraints: engine P1 (no text rewriting after parsing), P2 (parsing is pure syntax;
every spelling collapses to one node), P7 (slack rules retire by promotion to first-class grammar),
P9 (algebra type errors raise in the checker); system P10 (one canonical Mode-B scalar allowlist),
P13 (no new key kinds); core P4 (typed errors).

## Goals / Non-Goals

**Goals:** every healed spelling produces the same parsed node / key / merged query as its canonical
form, so SQL, result keys and values match by construction.

**Non-Goals:**
- A measure named like a column it aggregates (the issue's heal 2): dropped by decision —
  `MeasureNameCollidesWithColumnError` and `CanonicalAliasShadowsColumnError` stay, including the
  outer-stage case.
- `dow`, `dayofweek`, `weekday`: conflicting numberings across databases (Postgres/DuckDB/Snowflake
  `dow` 0–6 Sunday-first, Spark `DOW` 1–7 Sunday-first, MySQL/BigQuery `DAYOFWEEK` 1–7 Sunday-first,
  MySQL `WEEKDAY` 0–6 Monday-first) — they stay errors with an explanatory message.
- Method-call forms with column-valued further arguments or `partition_by=`.
- SQL operator spellings (DEV-2071).

## Decisions

**D1 — First-class grammar, no normalization warning.** No heal goes through
`slayer/engine/normalization.py`. Precedent: the legacy colon spelling, `countD`, `CASE WHEN`,
`LIKE`, the DEV-1575 order shorthand. Alternative (a `NormalizationWarning` per heal) rejected: P7
retires slack rules into grammar, and the brief asks for silent healing.

**D2 — One date-part alias table in core.** A mapping alias → `DatePart` lives next to `DatePart`
(`slayer/core/enums.py`) and is the single source for the alias function (`<alias>(x)` → the
`date_part` scalar node with that part literal), the `date_part` part argument (`_date_args`), and
`extract`. The alias names and `extract` join the reserved date-function names (custom aggregations
and custom granularities). Alternative (resolve aliases at binding with custom-aggregation precedence,
like DEV-1576 aggregation aliases) rejected: an alias would then mean different things per model, and
the date-function names are already reserved.

**D3 — `extract(… from …)` and `count(distinct …)` are pre-`ast` token rewrites inside
`parse_expr`.** Neither is Python syntax, so each gets a token-aware scanner in the style of
`_rewrite_case_when` / `_rewrite_sql_like`: string literals blanked, identifier adjacency guarded with
the existing Unicode-aware helpers, balanced-parenthesis matching to find the call's extent, and
exact-span replacement (`extract(p from x)` → `date_part('p', x)`; `count(distinct x, …)` →
`count_distinct(x, …)`). The `from` / first `distinct` is found only at depth 1 of the call. Every other
`distinct` keyword directly inside a call raises the targeted DISTINCT error. Runs for every Mode-B
position (both `parse_expr` and `parse_filter_expr`).

**D4 — Unknown-function error at binding.** The parser keeps deferring unknown aggregatable calls
(`syntax.py` unknown-name fallback → `AggCall`), since custom aggregations are model scope. At binding,
after the builtin / custom-aggregation / granularity lookups fail, the error is the typed
`UnknownFunctionError` "Unknown function '<name>'" naming the categories and a close match; for
`dow` / `dayofweek` / `weekday` it carries the two-convention explanation. Alternative (keep "Unknown
aggregation") rejected: the brief asks for it, and `isodow(x)` showed the old text misleads.

**D5 — Method-call aggregation in `_convert_call`.** A call whose callee is an attribute chain
`<path>.<name>` converts to the node its functional twin `<name>(<path>.<source>, …)` produces:
the source's bare-name leaves (or `*`) are rewritten onto `<path>` structurally on the parsed tree
(not text), further arguments must be literals, then the ordinary call dispatch runs on `<name>`
(builtins, aliases, `first`/`last` shape dispatch, unknown → aggregation candidate, granularity). A
scalar or transform-only `<name>`, a dotted source leaf, a non-literal further argument, or
`partition_by=` raises an error showing the functional spelling. Alternative (prefix every argument)
rejected: `partition_by=` and keyword parameters keep a query-root reading, so prefixing them is a
second reading.

**D6 — Order column object form in `core/query.py`.** `_coerce_order_column` and
`_capture_raw_formula` treat a dict whose only key is `name`, when that name is not a valid
`ColumnRef` name but is a formula candidate, exactly as the string. The schema hook (a callable
`json_schema_extra`, like `_advertise_string_time_dimensions`) adds the string alternative to
`OrderItem.column` and drops `raw_formula` from the generated JSON schema only; the field is still
accepted on input and persisted.

**D7 — Granularity-call partition keys.** `_bind_partition_keys` admits a `TimeTruncKey` element
(it has no time-dimension context). The existing partition-key check in `bind_inputs.py`, which holds
the projected time-dimension keys, accepts it only when it is one of them, and otherwise raises a
`PartitionKeyError` naming the bucket with the `time_dimensions` remedy. Join-path canonicalization,
custom-granularity equality, attributability and rank operand-grain checks apply unchanged, because
the bound key equals the time dimension's key.

**D8 — Refinement restatement in `refine_query`.** Before `_union`, a refinement measure whose formula
is a bare identifier equal to a saved measure entry's `name` (and whose own name is that name or
absent) is matched to that saved entry: fields in its `model_fields_set` among `label`, `type`,
`description`, `meta` are compared with the saved entry (explicit `null` compared as a value); equal →
dropped, different → `RefinementConflictError`. String shorthand sets no fields. This has one reading:
every other reading of the bare name fails today — a saved entry cannot share a name with a source or
stage column (`MeasureNameCollidesWithColumnError` stays), and selecting a model measure or column of
that name next to the saved entry duplicates its result key.

**D9 — Transform-named measures.** `ModelMeasure._reject_transform_shadowing` is deleted; the
transform-name reservation stays for custom `Aggregation` names (a call-position collision). The Cube
converter skip, the dbt `unconverted_metrics` routing and the OSI clean-fail for transform-named
measures are removed.

## Risks / Trade-offs

- [Reserving the alias names breaks a stored model that already uses one as a custom aggregation or
  custom granularity name] → names like `isodow` / `weekofyear` are unlikely aggregation names; the
  validation error names the reserved word.
- [Token rewriters mis-scan nested or quoted text] → D3's scanner reuses the existing literal-blanking
  and adjacency helpers; tests cover nested calls, arithmetic operands, quoted `from` / `distinct`,
  newlines, Unicode-adjacent identifiers and unbalanced input.
- [Unknown-function text changes break consumers matching "Unknown aggregation"] → in-repo assertions
  are updated; the error class stays typed.
- [A transform-named saved measure confuses a reader of `first(rev)` vs `first`] → resolution is by
  position (call vs bare name), already spec'd for `first` / `last`.
