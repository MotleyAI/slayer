## Context

See proposal.md — Why. Governing principles: `system.arc42.md` §3.6, `sql.arc42.md` §3.1
(AST end to end), §3.2 (quirks only in `dialects/`), §3.9 (fail closed). Import law:
`core.models → sql.*` arrows are `#legacy`; `engine → sql` and `sql.dialects → sql`-root
imports are permitted — so no `.c4` / `index.yaml` change.

Probes (sqlglot 30.17): replacing a node with `a + b` under `*` emits `a + b * w` — sqlglot
does NOT add precedence parens; the tokenizer lexes `{value}` as `L_BRACE VAR R_BRACE` and
`'{value}'` as one `STRING`; `exp.ApproxDistinct` emits every current Tier-2 approx
template verbatim (ClickHouse `uniq`, Redshift `APPROXIMATE COUNT(DISTINCT x)`,
Trino/Presto `APPROX_DISTINCT`, Spark/Databricks/BigQuery/Snowflake/DuckDB
`APPROX_COUNT_DISTINCT`) but `APPROX_DISTINCT` on Oracle / T-SQL.

## Goals / Non-Goals

**Goals:** no SQL text crosses a generator↔dialect or generator-internal boundary in the
aggregation render path; one template implementation; the dialect-hook shape enforced by
a type law.

**Non-Goals:** the rest of statement assembly — `emit_outer_wrap`'s `inner_sql: str` and
the T-SQL re-parse — DEV-1965. Save-time placeholder-name checking (user decision: parseability
only; `{value}` not required — `COUNT(*)` is a legitimate formula).

## Decisions

1. **One parse pipeline.** Extract `Generator._parse`'s body (reserved-word prequote,
   `rewrite_parsed_ast`, log-alias rewrite, mixed-case quoting, `rewrite_target_ast`) into
   `parse_expression(sql, *, dialect)` in the sql root; `_parse` and `SqlTemplate` both
   delegate. Alternative (SqlTemplate parses raw sqlglot) rejected: save-time and
   render-time would parse the same formula differently.
2. **`SqlTemplate`** (`slayer/sql/sql_template.py`, Pydantic, not a dataclass): constructed
   from `(text, dialect)`. Placeholders found by token span (tokenizer offsets are
   character-based). Each gets a sentinel name proven absent from the formula's tokens.
   After parsing, every sentinel must be exactly one unqualified `exp.Column` in expression
   position, else a typed template error (Codex #2). The parsed root is private and
   pristine; `render(bindings)` starts from `root.copy()` and inserts `binding.copy()` per
   occurrence (Codex #3). Unbound placeholder → typed error; unused bindings ignored.
3. **Paren rule.** Wrap a binding in `exp.Paren` iff the binding is operator-shaped
   (`exp.Binary`, non-`Paren` `exp.Unary`, `exp.Between`, `exp.In`) AND its parent is an
   operator (`exp.Binary` / non-`Paren` `exp.Unary`). Function args, CASE, CAST are
   self-delimiting — never wrapped. Codex #1 ("sqlglot parenthesises on emission")
   rejected by the probe above. Goldens re-bless only what this canonical emission
   produces (Codex #13).
4. **Value/param resolution is AST.** `_resolve_value_sql → _resolve_value_ast`;
   `_resolve_agg_param` returns AST; the picked-value `MAX(...)` consumes the AST directly.
5. **Percentile `p`.** Accept a numeric `exp.Literal`, optionally under `Paren` / `Neg` /
   unary plus; validate with finite `Decimal` in [0, 1]; emit the literal's own spelling;
   existing error messages kept (Codex #7).
6. **Dialect hooks are typed AST.** Operands `exp.Expression`, `p` an `exp.Expression`
   (validated literal), `agg_name` → `StatAgg1Name` / `StatAgg2Name` Literals,
   `build_time_offset_expr(granularity)` → an existing enum or a Literal of the unit
   values (whichever matches callers). No `parse` parameter. SQLite week/quarter via
   `exp.Anonymous` / `exp.Case`. Covar decomposition builds guard prototypes and inserts
   `.copy()` at every use (Codex #4).
7. **Approx-distinct without templates** (Codex #6, cleaner form): delete
   `approx_count_distinct_template`; base emits exact `exp.Count(exp.Distinct(x))`; a
   boolean `approx_count_distinct_native` emits `exp.ApproxDistinct`;
   `approx_count_distinct_anonymous_name` stays for Oracle / T-SQL.
8. **Save-time check in the engine**, not a Pydantic validator: a core validator would add
   a `core → sql` arrow and break loading stored models. Runs once in
   `QueryEngine.save_model`, after query-backed population and before any storage
   mutation / trial-execute; datasource dialect, generic sqlglot dialect if unresolvable
   (never skipped); error names model + aggregation (Codex #9).
9. **Type law** `tests/test_law_ast_dialect_hooks.py`: every `SqlDialect` subclass
   (inherited overrides included), every method whose resolved return annotation is an
   `exp.Expression` subtype: all params annotated; none `str` (incl. in an
   `Optional`/union) or `Callable`. Closed `Literal`s / enums are allowed by design.
   Hints resolved with `DatasourceConfig` supplied for `TYPE_CHECKING`-only refs.
   Name-based scoping (Codex #8) rejected by the user. No exemptions: the one other
   violator, `_outer_order_column(inner_sql: str)`, is fixed here (decision 11). Approved
   arc42 edit (lands with the
   test):

   ```diff
    1. **AST end to end**: statements are built and composed as sqlglot AST; text
       round-trips of already-emitted SQL are forbidden (dotted aliases corrupt on
   -   re-parse). [review]
   +   re-parse). A dialect hook returning AST takes only typed, non-`str`,
   +   non-callable operands. [review] [enforced: test:tests/test_law_ast_dialect_hooks.py]
   ```

10. **MCP `create_model` takes `aggregations`**: a list of aggregation dicts, validated
    as `Aggregation` like `edit_model`'s upserts, passed into the `SlayerModel` so the
    save-time check (decision 8) covers this door too. Rejected with `query` like the other
    table params.
11. **`_outer_order_column` without text**: `emit_outer_wrap` gains `projected:
    Sequence[str]` — every alias the inner statement projects (public + hidden hoists),
    supplied by the generator from its alias map; `_outer_order_column(col, public,
    projected)` matches candidates against it instead of `quote_identifier(c) in
    inner_sql`. `emit_outer_wrap`'s own `inner_sql: str` stays for DEV-1965.

## Risks / Trade-offs

- [Golden churn from canonical AST emission] → re-bless only paren/whitespace diffs;
  assert execution equivalence on SQLite/DuckDB for re-blessed formula goldens.
- [`{ value }` / keyword placeholders newly accepted] → harmless widening; tested.
- [Generic-dialect fallback may reject dialect-only syntax when datasource unresolvable]
  → such a model cannot be queried anyway; error names the aggregation.
- [ApproxDistinct emission drift across sqlglot upgrades] → per-dialect goldens pin it.
