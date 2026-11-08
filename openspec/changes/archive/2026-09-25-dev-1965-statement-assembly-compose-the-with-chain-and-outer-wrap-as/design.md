## Context

See proposal.md (Why). Current text round-trip sites that compose statements (line
numbers as of `185bae55`):

| Site | Where |
|---|---|
| `as_ast` fallback `parse_one` (producer hoist) | `generator.py` `generate_from_planned` ~1461 |
| POST-filter f-string `SELECT * FROM (…) AS _filtered WHERE …` | `_finalize_planned_transform_chain` ~1997 |
| Outer wrap with a text inner statement; T-SQL re-parse + silent fallback | `dialects/base.py` ~638, `dialects/tsql.py` ~304 |
| Stage / root CTE splits re-parse stage text | `_split_statement_ctes` ~4069, `_split_root_ctes` ~4116 |
| Producer body rendered to text and then re-parsed by the rename wrapper | producer attach ~4384 → `stage_wrapper.py` ~157 |
| Multi-stage stage wrap over text | `_stage_rename_wrapper` ~6659 |
| WHERE/HAVING: filter ASTs rendered, `" AND "`-joined, `_parse_predicate`d | ~6161–6185 |
| Query-backed model: wrapper parses final, fitted and mangled text, then decodes it | `query_engine.py` ~2815–2831 |

Text-returning branches of `_generate_from_planned_impl`: windowed (~3154), cross-model
combined (~3981), transform chain (via `emit_outer_wrap`), and plain when `as_ast` is
unset (~1636). The plain path's outer trim wrap (`_build_outer_trim_wrap_select` ~6293)
is already AST: its regroup `WITH` sits on the outer statement, and it paginates through
`dialect.apply_pagination`.

Principles: `sql.arc42.md` §3.1 (edited), §3.2, §3.6, §3.9, §3.11; `system.arc42.md` §3.1
and §3.6.

## Goals / Non-Goals

**Goals:** the internal render contract is `exp.Select` everywhere; one outer-wrap
builder; a single finishing step; a behavioural law that makes a statement render inside
composition fail the suite.

**Non-Goals:** converting `rewrite_emitted_sql` / dot-mangling / the ClickHouse `SETTINGS`
attach to AST (DEV-1961); value-layer text (`AggRenderSpec.sql`, `_resolve_sql`,
`column_expansion`, the `unmangle_dotted_table_refs(base_select)` repair in
`_build_base_select_for_planned`) (DEV-1972). `unmangle_dotted_table_refs` itself stays
for that one caller.

## Decisions

**D1. Builders return AST; finishing happens once.** Every `_generate_from_planned_impl`
branch returns `exp.Select`. A single finishing function performs render (pretty,
dialect) → `rewrite_emitted_sql` → `maybe_validate_scopes` → `assert_no_overlimit_identifiers`,
and every public text entry point goes through it: `generate_from_planned` and
`generate_planned_stages`, whose single-stage branch is included. Name it
`_finish_statement(ast, *, dialect, aliases, exempt) -> str`. The `as_ast` parameter is
deleted: internal callers call the AST builder. `_parse_cte_body` (no callers) is deleted.
*Alternative:* keep `as_ast` and make every branch honour it. Rejected: that keeps two
return types on one method.

**D2. One outer-wrap builder.** The chain path builds `SELECT <public aliases> FROM
(<chain final select, WITH detached>) AS _outer`, attaches the detached `With` to the
outer statement, adds the chain's ORDER BY terms, and paginates via
`dialect.apply_pagination` (ints). This is the same builder shape as the plain path's
`_build_outer_trim_wrap_select`; the two share one implementation, parameterised only by
how order terms are resolved (the plain path uses `_apply_planned_order_limit`'s host
env, the chain path uses `_planned_order_terms`). `emit_outer_wrap` (base + T-SQL) and
`_offset_ordering_fallback` are deleted; `_outer_order_column` is deleted if it has no
remaining caller, otherwise it moves beside the builder. `OUTER_WRAP_ALIAS` stays.
*Alternative (a), issue-literal:* keep `emit_outer_wrap` as an AST dialect hook. Rejected
by the owner, because it keeps two builders, the T-SQL override and a `WITH` nested in a
derived table on non-T-SQL dialects.
Hoisting is semantics-preserving: CTE names are allocated uniquely by the one allocator,
and the outer `SELECT` introduces no names of its own besides `_outer`.

**D3. POST filter = WHERE on the chain's final select.**
`_render_post_phase_filter_conditions` returns `List[Expression]`; the chain's
`inner_select` (`SELECT <carried aliases> FROM <tail>`) gets `.where(exp.and_(...))`,
with And/Or conjuncts parenthesised. This is equivalent to today's `_filtered` wrap, which
is `SELECT *` over exactly those carried aliases, so any alias a condition references
(composite-dimension aliases included) is a tail column. `FILTERED_ALIAS` is deleted.

**D4. Multi-stage and producer splits on AST.** Each stage renders through the AST
builder. `_split_statement_ctes` becomes `_split_ast_ctes` applied to the stage AST;
`_split_root_ctes` takes an `exp.Select`. The producer attach passes `producer_body` (AST)
to the wrapper. The repair-only `unmangle_dotted_table_refs` calls in
`generate_from_planned`, `_split_ast_ctes` and `build_flat_rename_wrapper` are deleted:
nothing is re-parsed, so there is nothing to repair.

**D5. WHERE/HAVING as AST.** `target_parts` hold `Expression`s (the Mode-A predicate from
`_enter_mode_a_predicate` is kept as AST). `where_clause = exp.and_(*parts)` when there
is more than one part, else the single part; `_SQL_AND_JOINER` is deleted if unused.

**D6. Query-backed model.** A shared `_build_planned_stages_ast(planned_queries, *,
bundle, dialect) -> exp.Select` is used by both `generate_planned_stages` (then
`_finish_statement`) and the engine. The engine path is `_build_planned_stages_ast` →
`build_flat_rename_wrapper(inner=<AST>, source_relation, expected_columns, dialect)` →
`_finish_statement(aliases=projection_result_keys)`. The inner canonical keys are included
because after wrapping they are derived-table columns, which the internal-CTE scan may not
cover. The engine fits the wrapper's flat output aliases on the AST with
`alias_rewrite_map(expected)` before finishing; a flat name equals the user-authored
`Column.name`, which the finishing pass exempts. `build_flat_rename_wrapper` loses `stage_sql: str`,
`projection_aliases` and the `decode_result_keys` step: it always sees canonical names.
`Column.sql` keeps `alias_rewrite_map(expected)`, the same map applied to those output aliases.

**D7. Law: no statement render inside composition.** An autouse test-harness fixture
(`tests/conftest.py`) wraps the internal AST builders (the generator's statement builders
and the engine's query-backed wrap) to mark their dynamic extent with a context variable.
It also wraps sqlglot's generation entry point (`Dialect.generate`, through which every
`Expression.sql()` passes) to fail when an `exp.Query` node is rendered while the flag is
set. Renders outside that extent (the final render in `_finish_statement`, the scope
validator, the engine's read-only analyses and the DEV-1961 post-render passes) are not
flagged. `tests/test_law_no_statement_reparse.py` proves the fixture fails on (i)
render→parse and (ii) render→text edit→parse inside a builder, and passes on an
AST-only composition.
*Alternatives:* (1) exact-text parse matching, which misses edited text (Codex finding);
(2) a static ban on `parse_one`, which is brittle because the generator has about 10
legitimate Mode-A parses.
If a value-layer render of a `Query` (a Mode-A scalar-subquery `Column.sql`) trips the
law, stop and ask. Do not allowlist it silently, because that case belongs to DEV-1972.
Until DEV-1972, `column_expansion.expand_derived_refs_sync` and
`SQLGenerator._expand_derived_column_sql` are law exemptions: they render user fragments that
may hold subqueries.

**D8. MySQL sample statistics.** The MySQL hook's `exp.Anonymous("VAR_SAMP")` survives
once D1 and D4 remove the re-parse. Goldens `dev1915` `corr::mysql` and
`covar_samp::mysql` are re-blessed. Focused tests cover each seam: producer hoist,
non-root stage split and root split.

## Risks / Trade-offs

- [Golden churn across dialects on transform-chain statements] → re-bless only after a
  reviewed diff whose changes are limited to hoisting the `WITH`, removing `_filtered`,
  whitespace, and D5 parenthesisation. SQLite/DuckDB execution results stay unchanged.
- [ORDER BY terms on the chain path that carry qualifiers] → the unified builder
  re-resolves any qualified column against the outer scope (`_outer_order_column`
  semantics). Covered by the BigQuery dotted ORDER BY and hidden-alias tests.
- [The query-backed persisted SQL changes bytes] → it is re-derived on expansion and not
  keyed by bytes elsewhere. Tests pin execution and key match.
- [Fixture coupling to builder method names] → the law test's self-check fails loudly if
  a wrapped name disappears (the law's self-test asserts each target exists).
