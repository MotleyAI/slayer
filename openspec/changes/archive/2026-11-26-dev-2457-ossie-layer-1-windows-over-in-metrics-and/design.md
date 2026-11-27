## Context

See proposal.md for motivation. State on `main` that shapes the approach:

- `slayer/core/syntax.py` already parses the whole window syntax (`WindowCall`, `OrderItem`, `WindowFrame`, `WINDOW_FUNCTION_ARITIES`), but only under `parse_expr(windows=True)`, which the strict filter reader alone uses. Every other parse raises `IllegalWindowInFilterError` in `_reject_over`.
- Regex gates refuse `OVER` before the parser: `check_metric_text` (`core/models.py`, used by `DatasetMetric` and `QueryMetric`), `_validate_query_filter_string` (`core/query.py`), `parse_sql_predicate` (`sql/sql_predicate.py`), and `check_portable_window` (`sql/definitions.py`, both save doors).
- DEV-2432 computes a windowed field (a datasource-dialect entry holding a window) in the relation door: `_emit_relation` / `_materialised_windows` in `slayer/sql/generator.py` emit one subquery layer per dependency depth over the dataset's filtered rows, with to-one joins of the window's definition inside the layer and the session policy beneath it. `DatasetField.windowed` carries the classification.
- Transforms are `TransformKey`s evaluated in `TransformLayer`s over a `PlannedDomain` (DEV-2218); `_render_window_transform_sql` renders them with the database's native NULL placement plus explicit null flags.
- `slayer/storage/semantic_models.py` marks any entity holding `exp.Window` as "cannot evaluate yet" (`WINDOW_WARNING`), and `model_metric_rules.check_model_metric` refuses authoring one.
- The Layer 1 gate holds 21 window rows as strict expected failures naming DEV-2457 (`tests/_dev2446_required_manifest.py`), each run as a query dimension with the filter `orders.id = 3`.

Principles that apply: system P4, P8, P10, P11, P12; semantics Axioms 1, 2.6, 3.3, 9, 11, 13, 14, 15, Laws 3, 4, 7, Principles 1, 2, 4; engine P1, P2, P4, P6, P9; sql P1, P2, P5, P9, P10, P13, P14; core P3, P4, P5; importers P1.

## Goals / Non-Goals

**Goals:**

- One Mode B grammar that reads `OVER` everywhere, with one typing of a window (row-level or measure-level) made in the checker.
- Row-level windows reuse the DEV-2432 relation-door layering; measure-level windows reuse the transform-layer and domain machinery.
- Ossie's NULL placement produced by one dialect hook, on all nine Tier-1 databases.

**Non-Goals:**

- `IGNORE NULLS`, named windows (`WINDOW` clause), `GROUPS` and `EXCLUDE` frames: not in Ossie's syntax, and the parser does not read them.
- SLayer-only and custom aggregations as the function under `OVER` (decision 2).
- Self-join emulation of `COUNT(DISTINCT)`, `MEDIAN` and `PERCENTILE_CONT` under `OVER` (decision 9). `DISTINCT` aggregates beyond `COUNT`, `WITHIN GROUP` forms and `PERCENTILE_DISC` belong to DEV-2458.
- Exporting SLayer transforms (`cumsum`, `rank`, …) as Ossie windows.
- The Cube and dbt importers: windowed measures stay dropped with a report.
- A window in a quoted string argument or an aggregation definition's default (`aggregations/formula-templates`): its typed refusal stays.
- No model (`architecture/model.c4`) or import-law change is planned. If implementation needs a new edge, that is a plan change to bring to the owner.

## Decisions

1. **An inline row-level window is an anonymous windowed column, computed before query filters.** Weighed: the SQL reading (over the query's filtered rows), which gives one text two meanings in a field and in a query and makes a filter on the window circular; refusing inline windows, a position-dependent grammar against Axiom 13. Taken for position parity. (user)
2. **Only Ossie's functions under `OVER`; operands unrestricted.** Weighed: every aggregation, SLayer-only and custom included, which most databases cannot window and which would add a third `first` / `last` form. (user)
3. **A raw window in a field's `filter` is legal.** Weighed: keeping it refused with the "define a field" remedy. An extension beyond the issue's stated scope, taken so the row-level rule has no position exception. (user)
4. **A raw window in a dataset's `filters` raises the filter-cycle error.** It reads the dataset the filter defines (Axiom 3.3). (default)
5. **An inline window runs over the dataset its references are anchored at.** Weighed: the deepest dataset its operands live on (Axiom 2.2), which breaks parity with the field form for a window whose keys all sit on a joined dataset. (user)
6. **Axiom 1 wording**, added after the windowed-column sentence: "A window with no aggregate-valued argument or key written anywhere else is the windowed column of one dataset — in a definition its declaring dataset, in a query the one its references are anchored at (Principle 1; anchors apart meet as in 2.6) — and is illegal with none." (user, approved wording)
7. **Axiom 11 wording**, added after 11.6: "- **11.7 Windows.** A window function with an aggregate-valued argument or key is a preserving transform that means what SQL means over the cells of its domain (11.6): each key is aggregate-valued or an operand-grain member, and a step is a cell, never a calendar step (11.3)." (user, approved wording)
8. **Measure-level window semantics**: a transform typed by Axiom 11; keys are aggregate-valued or grain members; it runs over its own operand's cells; SQL's meaning with no implied partition and cell steps; measure filters apply afterwards; ties follow the database. Follow Axioms 9, 11, 14 and Laws 3, 7. (user, confirmed with 7)
9. **Holistic aggregates under `OVER` run natively or are refused.** `COUNT(DISTINCT)`, `MEDIAN`, `PERCENTILE_CONT` return their value where the database windows them and a typed error naming function and database elsewhere. Weighed: a self-join emulation, quadratic in partition size with nothing in the expression to warn of it. An approved limit on a REQUIRED construct; the emulation, if wanted, fits DEV-2458. (user)
10. **Every other REQUIRED construct runs on all nine Tier-1 databases**, natively or by an exact rewrite; a refusal is left only where Ossie mandates the error or for a frame outside Ossie's three forms the database lacks. Follows semantics P2.1 and P2.2. Known rewrites: SQL Server `NTH_VALUE` (no native form) over the three Ossie frame forms; MySQL and SQL Server NULL placement by the `CASE WHEN x IS NULL THEN 1 ELSE 0 END` key Ossie prescribes; ClickHouse `LAG` / `LEAD` framing and default-value differences; `FILTER` under `OVER` as a `CASE` argument where the database lacks it. Tier-2 databases use the base emission. (default)
11. **Ossie's NULL default applies to all Mode B**, an `ANSI_SQL` document entry read as Mode B included (ANSI leaves placement to the implementation, so Ossie's is a valid reading). Where the database has a `NULLS` clause the placement is always emitted, never left to the native default, because DuckDB and Snowflake sessions can change it; on MySQL and SQL Server, whose NULL-smallest order is fixed, the `CASE` key is added only where Ossie's placement differs from it. SLayer's own transforms and query-level `order` are unchanged. (user)
12. **Strict mode keeps `E_WINDOW_IN_WHERE` and `E_DEFERRED_WINDOWED_METRIC`.** Ossie mandates both and corpus cases `e08` and `e12` pin them. The issue's "strict accepts exactly the window constructs that run" is read as: the same functions under `OVER`, in the positions Ossie allows. (default)
13. **Classification by structure, in every position.** A window with no aggregate-valued operand is row-level even in a metric, where the aggregate-free-measure rules apply; a model-scoped metric that is one stays in the existing "reads a field outside an aggregate" category. (default)
14. **A frame on a ranking function or `LAG` / `LEAD` is dropped at emission.** It has no effect on the value, and SQL Server and Snowflake reject it. (default)
15. **One new key kind for a window call** (function, arguments, partition keys, order items, frame), phase derived from its operands: ROW for a row-level window, POST for a measure-level one. Weighed: reusing `TransformKey`, whose `op` / `time_key` / `partition_keys` contract a window does not have (`python.core` P4). (default)
16. **The window key stores each order item's resolved NULL placement, never "omitted".** A Mode B parse resolves an omitted clause to Ossie's default, so two windows that differ only in how the placement was written intern to one slot, and a window can never merge with one meaning something else. Datasource-dialect text stays opaque SQL and never becomes this key. (codex)
17. **The importer and exporter leave `first` / `last` and the window forms as they are**, pinned by tests. Checked: the exporter carries `first` / `last` in the `SLAYER` extension and never emits `LAST_VALUE`, so the issue's "wrong value now" worry does not hold. (default)
18. **Windowed fields and metrics export as Ossie expressions**; one with a SLayer-only operand, or whose exported text would nest a window directly in a window (Ossie-invalid), travels in the extension. (default)
19. **`IllegalWindowInFilterError` stays a public class**; no exception is removed. The new refusals raise in the checker as `QueryTypeError`s (engine P9); parse-shape refusals raise where the parser raises today. (default)
20. **The gate rows keep running as query dimensions**, with expected values recomputed for windows that run before the `orders.id = 3` filter (follows 1). (default)
21. **A window directly inside a window stays legal natively and refused under strict.** The inner window is a windowed column the outer one reads, which DEV-2432 already runs for named fields; refusing the inline spelling would be a closure violation. Strict keeps `E_INVALID_QUERY`. rejected (codex): the finding asked to refuse it natively too.
22. **Strict keeps its current frame checks.** rejected (codex): the finding asked to admit only Ossie's three listed frame forms, but Ossie's own example and corpus line `compliance/expressions/valid.txt:103` use `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`, which is none of the three, so the list is not an allowlist.
23. **Zoned reads inside a row-level window** are a stated requirement with an executed scenario: the window's arguments and keys read the query zone's wall clock; filters, masks and the session policy beneath it see stored values (system P12). (codex)
24. **A window whose only references are to the time spine is refused** like a window with no reference: the spine has no countable rows (Axiom 15). (codex)
25. **The Tier-1 matrix tests peers under the NULL keys**: `RANK`, `DENSE_RANK`, `CUME_DIST` and a `RANGE … CURRENT ROW` frame over several order items and all-NULL peers. (codex)
26. **Codex findings already covered**: native query-filter windows and the four specs that called windows unevaluable were in the plan's delta list; each is named in its delta. (codex)
27. **Existing tests aligned with the plan**, approved with the spec: `tests/test_dev2432_windowed_columns.py` (`TestPortableWindowedField`), `tests/test_dev2243_import.py`, `tests/test_dev2243_corpus.py`, `tests/test_dev2243_documents.py`, `tests/test_dev2243_save_scope.py`, `tests/test_saves_revalidate.py`, `tests/test_dev2264_not_evaluable.py`, `tests/test_dev2439_window_syntax.py`, `tests/test_sql_expr.py`, `tests/_dev2446_required_manifest.py`, `tests/_ossie_gate_suite.py`, `tests/test_dev2446_required_constructs.py`, `tests/test_dev2220_compliance_page.py`. Any other test pinning a lifted refusal is brought to the owner by name before it changes. (user)
28. **The PR description opens with a prominent note** on decision 9: the three functions, the databases that refuse them, and DEV-2458 as the place the emulation could go. (user)
29. **OpenSpec scenario titles are kept where a MODIFIED delta changes their body** ("A windowed field loads and fails only when read", "Authoring a windowed metric is refused", "A windowed metric is reported, not refused", "The native binding still refuses a window in the filter"): OpenSpec keys scenarios by title and refuses a rename. (default)

## Risks / Trade-offs

- [A window in a query does not see the query's filters, unlike plain SQL] → stated in `docs/concepts/expressions.md` with an example, and in the agent help memory.
- [Inline windows multiply relation-door layers] → windowed columns are deduplicated by key per dataset, and only those the query reads are computed (DEV-2432 decisions 7 and 9 carry over).
- [The NULL key changes peers under `RANGE` or `RANK`] → the key is `0/1` on the item's own NULL-ness and sits immediately before the item, so equal items keep equal keys; decision 25's tests pin it.
- [SQL Server `NTH_VALUE` rewrite is exact only for the three Ossie frame forms] → any other frame on SQL Server is a typed refusal (decision 10).
- [ClickHouse, BigQuery, Snowflake and Trino limits on holistic window aggregates were not probed at plan time] → the implementation probes each and records pass or refused per database; the rule (decision 9) is fixed, the per-database outcome is data.
- [A REQUIRED construct refused on some Tier-1 databases] → listed as refused on the compliance page and announced in the PR description (decision 28).
