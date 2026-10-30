## Context

See proposal.md (Why). Every symptom was reproduced on the idiom fixture (`tests/_mcp_idiom_fixtures.py`, `make_idiom_engine`, model `orders`, DuckDB). Each has a structural cause.

- **Boolean parameters.**
  - `ResolvedAggKwarg` (`slayer/sql/generator.py`) carries no type, so the boolean-to-integer rule (`aggregate_input`, `slayer/sql/render/aggregates.py`) reaches only the aggregated value, through `_aggregated_value`.
  - A parameter is built in five places: `_resolve_agg_kwargs_for_key`, `_agg_param_exprs` (row and literal arms), the windowed `_w_p<i>` pick and the association `_p<i>` pick. None of them can lower a boolean.
- **FILTER naming.**
  - `_canonical_alias_for_formula` (`slayer/engine/bind_inputs.py`) names an unnamed aggregate formula from its bound key. A masked FILTER is already lowered into that key's source (`iif(c, value, NULL)`), so the name comes from the lowered form.
  - A formula with variables is named from its template text instead.
  - `canonical_aggregate_alias` (`slayer/sql/naming.py`) ignores `AggregateKey.filter`. So when the filter stays on the key (a ranked pick, or a star count over a joined condition), the filtered aggregate takes its unfiltered twin's names. In the ranked producer path this makes two distinct keys share a producer column, which breaks engine §3.4 ("structurally-equal keys intern to one slot").
  - Probed: `first(amount) FILTER (WHERE status = 'open') - first(amount)` returns 0; named `fo` and `f`, both return 20.
- **Branch typing.**
  - The CASE check is a side effect of slot typing, in `measure_key_type`'s top-level `iif` arm (`slayer/engine/key_metadata.py`).
  - It types branches with a partial `_branch_type` that knows only literals, local columns and aggregates.
  - It raises a plain `ValueError` outside the checker, which breaks engine §3.9.
  - The canonical `value_type` (`slayer/core/keys.py`) already types `concat` as TEXT, but the check never consults it.

Principles that apply:
- **system §3:** §3.6 (AST), §3.10 (one meaning on every dialect), §3.13 (fail closed on kind dispatch), and the new §3.17 "One typing" [target: DEV-2180];
- **sql §3:** §3.3 (one naming authority) and §3.9 (fail closed);
- **engine §3:** §3.1 (typed pipeline), §3.4 (interning), §3.9 (the checker raises every user-facing type error as a `QueryTypeError`);
- **semantics Law 3** (compositionality).

## Goals / Non-Goals

**Goals:**
- **One lowering door** for every aggregation input.
- **One public naming rule** for filtered formulas, plus internal names that keep distinct keys distinct.
- **One conditional-branch check**, in the checker, reading the canonical typing.
- **Inferred types** derived from the expression after coercion.

**Non-Goals:**
- Retiring the remaining parallel type derivations (`measure_key_type`'s aggregate arm, `stage_measure_type`, `date_scalar_type`, `dimension_key_metadata`, `_text_valued`, `_count_type`, `time_dimension_type`). That is DEV-2180. This change only removes the branch typing (`_branch_type`, `_literal_data_type`) and routes the conditional arm through `value_type`.
- Lowering boolean placeholders of custom aggregations (decision 2).
- Rejecting conditionals whose branch types cannot be determined (decision 8).

## Decisions

1. **Typed parameters, one lowering door.** `ResolvedAggKwarg` gains a required field `input_type: Optional[DataType]`, the parameter's `aggregation_source_type`. Making it required means a new construction site cannot forget it.
   - All five sites fill it. The windowed and association picks take the picked key's type, because MAX of a boolean is restored to BOOLEAN.
   - `_resolve_agg_param` returns `aggregate_input(value=…, aggregation=spec.aggregation, input_type=kwarg.input_type)`, the same function the value goes through.
   - Every parameter reaching that door belongs to its aggregation (`weight`, `other`, `p`, or a built-in-named override's placeholders). So the aggregation-level `BOOLEAN_LOWERED_AGGREGATIONS` set is the right test, and no per-parameter metadata is needed.
   - Rejected: patching each aggregation, because the issue asks for one fix covering every parameter. Rejected: per-parameter numeric metadata, because it adds nothing over the aggregation-level rule (codex finding 2, partially folded: the spec now lists each parameter's behaviour). (default, structural; codex)
2. **Custom aggregations receive raw booleans for `{value}` and every placeholder**, per the 2026-10-05 ruling. A model-level aggregation that reuses a built-in's name lowers, like the built-in.
   - Rejected: lowering every custom placeholder, because it breaks a template that uses a boolean placeholder as a condition, such as `CASE WHEN {flag} THEN …`, on Postgres.
   - Rejected: lowering only placeholders the template uses "numerically", because it is a heuristic over author SQL.
   - Codex finding 1 (follow the issue's "custom-aggregation placeholders") was rejected on the same grounds: the user chose this explicitly. (user; rejected (codex))
3. **Filtered `first` / `last` and joined-condition `count(*)` name collisions are in scope.** They have the same root cause as the FILTER naming bug and produce a silent wrong result. Rejected: an xfail, because no existing issue covers it. (user)
4. **Public naming follows the live spec rule.** An unnamed formula whose parsed tree contains a FILTER is named from its canonical text (`canonical_measure_text`, so spellings agree). A formula with variables uses its template text, so a variable names itself by its placeholder. (default; the live spec decides)
5. **Text-path alias derivation lives in `slayer/sql/naming.py`.** The text branch of `_canonical_alias_for_formula` moves there per sql §3.3; the engine calls it. (default, structural)
6. **Internal distinctness comes from key identity; the name is only a hint.**
   - `canonical_aggregate_alias` includes `key.filter` in every profile as a readable `_filter_<expression name>` suffix.
   - Distinctness of producers and slots is guaranteed by structural key identity (engine §3.4), never by the suffix. The ranked producer path is keyed by the key, and two distinct keys whose hints coincide are disambiguated by the allocator.
   - Covered by a scenario whose two conditions (`status = 'open'`, `status <> 'open'`) reduce to the same name tokens. (codex finding 3)
7. **The conditional-branch check lives in the checker.**
   - `check_conditional_branches(roots, column_type)` in `slayer/engine/elaborate_env.py` visits every node of every bound key, in every position: dimensions, time dimensions, measures (including inside aggregates, transforms and FILTER conditions), filters and order.
   - It runs on model and stage scopes alike, after the plan-time coercions (time points, lists, text-numbers, zoned comparisons).
   - For `iif`, `coalesce`, `ifnull`, `greatest` and `least` it joins the types of the returned arguments (`_returned_args`) through `join_conditional_branch_types`, reading the canonical `value_type`. `nullif` is excluded because its second argument is only compared.
   - A mismatch raises `ScalarOperandTypeError`, naming the call and both types and suggesting a CAST.
   - `value_type` / `_scalar_value_type` return the joined type for these scalars. They never raise: on a mismatch they return `None`, because the checker has already rejected it.
   - `measure_key_type`'s conditional arm delegates to `value_type`. `_branch_type` and `_literal_data_type` are deleted, and `join_conditional_branch_types` stays as the one join rule.
   - Rejected: CASE only, because that would mean two rules for one type-resolution mechanism. (user for the scalar set; default, engine §3.9 for the placement)
8. **Undeterminable types are absorbed.** Only provable mismatches fail. An untyped column, a `DataType.UNKNOWN` column (mapped to "untyped" before the join, never compared as a type) or an unrecognised expression passes to the database.
   - Rejected: failing closed on unknown types, which would refuse valid queries over untyped Mode-A columns.
   - Codex finding 5 was folded only for the UNKNOWN-column defect. (default; codex)
9. **Inferred types after coercion.** A measure or dimension whose type is not declared is typed from its post-coercion key: either inference moves after `_map_bound_keys`, or the types are recomputed there. Example: `greatest(status, amount)` types as DOUBLE, and stage schemas report the same type as the response. (codex finding 4)
10. **The new requirement lives in `queries/mixed-type-coercion`**, the capability that owns what an expression means when types meet. (default)
11. **Function matrix tests.** Each of `iif`, searched CASE, `coalesce`, `ifnull`, `greatest` and `least` is covered for:
    - NULL absorption;
    - INT/DOUBLE widening;
    - DATE/TIMESTAMP widening;
    - a boolean beside a number;
    - a TEXT/number rejection.

    Plus the issue's `||` branch, a joined-column branch, an arithmetic branch, a nested aggregate and a stage, on DuckDB and SQLite. (codex finding 6)
12. **Principle "One typing".** `architecture/system.arc42.md` §3 gains item 17, targeted at the new follow-up DEV-2180, approved verbatim (user, approved wording):

    > 17. **One typing**: a value's type is derived from its key in one place; every check, coercion and metadata lift reads that derivation. [target: DEV-2180]

    (user)

## Risks / Trade-offs

- [SQLite queries that silently mixed types in a conditional now fail at plan time] → This is intended per system §3.10. The error names both types and suggests a CAST, and the proposal marks it **BREAKING** for SQLite.
- [Renamed public keys for unnamed constant-condition FILTER formulas (`iif_status_paid_1_null_count` becomes `count_filter_where_status_paid`)] → FILTER has not shipped yet; it lives on the unmerged DEV-2101 → DEV-2137 stack, so no released client depends on the old key.
- [Running the branch check after coercion could miss a mismatch that coercion hides] → Coercion only rewrites TEXT beside a number into a number, which the spec defines as compatible. Every other mix survives coercion unchanged.
- [Recomputing inferred types after coercion changes metadata for existing queries] → The change only replaces `None` or a pre-coercion type with the post-coercion one, and tests pin the response metadata and stage schemas.
