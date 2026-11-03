## Context

See proposal.md › Why. Today `binding._bind_filtered_agg` lowers every FILTER as input masking: the source and each row-valued kwarg become `iif(c, v, NULL)`; a ranked pick and a joined-condition star count instead carry `c` on `AggregateKey.filter` and the renderer masks with `CASE WHEN c THEN v END` (`generator.py` ranked kernel and star spec). A custom formula (`generator._build_formula_agg`) substitutes only `{value}` and named placeholders, so the rest of its template reads every row.

**Structural cause.** Input masking equals row restriction only when the aggregation provably skips NULL inputs — the built-in semantics (semantics Axiom 4). The binder applies that semantics-dependent optimisation to every aggregation without consulting the semantics. A formula cannot be proven NULL-skipping: it may read rows directly (`COUNT(*)`, bare columns) or keep NULLs (`ARRAY_AGG({value})`, `SUM(COALESCE({value}, 0))`).

**Principles obeyed.**

* Semantics:
  * Axiom 4 (empty value).
  * Axiom 6 (an aggregate is a virtual model whose rows come from its source by the same row filters as any query).
  * Axiom 9 (no closure refusal; a formula's interior is never inspected).
  * Law 3 (sibling independence).
  * Law 6 (the built-in mask is a pure optimisation).
* python.sql:
  * P7 (empty grain = CROSS JOIN).
  * P9 (fail closed).
  * P10 (one composition primitive: the producer).
  * P11.
  * P12 (attach is cardinality-neutral).
* engine:
  * P2 (one aggregation node; FILTER stays a key field).
  * P4 (interning).
  * P6 (placement from plan typing).
* core P2 (the render strategy is chosen downstream, never baked into identity).
* system P13 (one key kind per meaning: reuse `AggregateKey.filter`) and §3.8.

No arc42 edit. The spec attaches to the existing cross-cutting `aggregations` capability.

## Goals / Non-Goals

**Goals:**

* A FILTERed non-built-in aggregation over rows is computed over only the passing rows.
* This holds for local, cross-model (target-rooted) and stage sources; in every consumer position (measure, saved `ModelMeasure.formula`, inside a larger formula, measure-typed filter, order); in association mode; with `partition_by=` and `window=`.
* The fix is made impossible to regress for any future non-built-in aggregation: the lowering is chosen from `AggregateKey.builtin`, one predicate, one routing.

**Non-Goals:**

* FILTER on a second-order aggregation (attached source) stays refused for every aggregation (DEV-2191).
* The empty-grain empty value of custom aggregations stays the formula's value over no rows (DEV-2094 case 2).
* Built-in FILTER lowering is unchanged, including the ranked / star `_carrying_filter` window refusal.
* No arc42 or model change.

## Decisions

1. **A FILTERed custom aggregation restricts rows, not inputs** — a producer whose WHERE carries `c`, attached by the null-safe LEFT JOIN on its complete grain. Rejected alternatives:
   * (B) Rewrite the formula AST (`CASE WHEN c THEN … END` per call, `COUNT(*)` → `COUNT(CASE WHEN c THEN 1 END)`): unsound for NULL-keeping aggregates, needs per-function-shape cases, and leaves bare columns outside aggregate calls unrestricted.
   * (C) Refuse FILTER on a formula that reads anything but placeholders: contradicts the spec, and is an Axiom 9 closure refusal.

   (user, DEV-2185 interview Q1 = A)
2. **Built-in aggregations keep the input mask** (Law 6: provably the same result). (user)
3. **A grouped cell with rows but none passing reads the custom aggregation's empty value, NULL** (Axiom 4; no producer row). (user)
4. **Empty grain: the formula's value over no rows** (`rows(amount) FILTER (WHERE false)` → 0).
   * The empty-grain producer always returns one row and attaches by CROSS JOIN. This is identical to today's ungrouped custom aggregation under an all-excluding query filter, and consistent with Axiom 6's virtual model.
   * Rejected: NULL at every grain. It needs a presence mechanism — a constant `GROUP BY` plus a LEFT JOIN ON TRUE attach against python.sql P7, or a row-count guard — and changes today's ungrouped results.
   * The open corner is recorded on DEV-2094 (case 2).

   (user, "A for now")
5. **Condition typing unchanged.** Row-level only; an aggregate / transform / measure inside `c` is rejected with the same typed error; `c` composes with `partition_by=` / `window=`; result-key naming is unchanged. (user)
6. **Equal-condition, equal-grain FILTERed custom aggregates may share one producer** via normal interning (default behaviour, not required). (user)
7. **Every non-built-in aggregation takes the producer route, placeholder-only formulas included.** The choice is decided by `AggregateKey.builtin`, never by inspecting the formula text. Masking is sound only for NULL-skipping semantics, which a formula cannot be proven to have (`ARRAY_AGG({value})`), and inspecting the interior would break Axiom 9. Built-in-named overrides with a formula are non-built-in (`builtin=False`) and so take the producer route; a formula-less declaration keeps the built-in semantics and the mask. (default)
8. **Reuse `AggregateKey.filter`; the lowering is chosen downstream from `builtin`** — not a new key kind or a second field. One predicate in `core/keys.py`, `restricts_own_rows(key) := key.filter is not None and not key.builtin`, is the one place that decides. The field's comment becomes: the FILTER condition; a built-in lowers it as an input mask, any other aggregation as its producer's row restriction. (default; system P13, core P2)
9. **`c` restricts only the aggregate's own rows** — never the population, a windowed aggregate's bucket set or endpoints, or siblings. (default; Axiom 6, Law 3, python.sql P12)
10. **The producer grouping key includes the condition.** `_local_regroup_groups` (`stages.py`, today `(grain, ident)` with `ident = None` for plain local aggregates) becomes keyed by at least (grain, window identity, condition), so an unfiltered or differently-filtered aggregate never shares a `c`-restricted producer. (default, sharpened by codex)
11. **Postgres coverage in its own pytest-postgresql unit file**, following `tests/test_dev2177_postgres.py`. (default)
12. **Cross-model source** (`rows(orders.amount) FILTER (WHERE orders.status = 'paid')` rooted at `customers`, via the target-rooted producer, `c` rerooted onto the target) is in scope and tested. (user)
13. **Stages are in scope and tested:** a FILTERed custom aggregation inside a non-final stage, consumed by a later stage (today it reads 5 where 3 is right). A custom aggregation over a `StageSchema` scope cannot be written — a stage has no custom aggregations (`UnknownFunctionError`, probed) — so the planning-time "stage source" item is realised as this stage case. (user; corrected after probe)
14. **Fail-closed renderer guard.** Rendering an aggregate with `filter` set and `builtin=False` (formula expansion, the agg render spec) raises an internal error instead of silently dropping `c`. A unit test feeds such a key directly. (codex)
15. **The anchor check stays the existing structural one.** Binding computes `source_anchor_path` of the would-be masked candidate (`iif(c, source-or-1, NULL)` against the source) purely to validate, then carries `c` unmasked on the key. A parity test asserts that a condition the built-in path rejects is rejected identically (same `AggregationFilterError`) for a custom aggregation. (codex)
16. **`c` enters the producer as its own row filter, outside the population machinery.**
    * It is appended after the inherited filters, never counted in `n_date_range`.
    * Decision 15 guarantees `c` is determined by the producer's root, so it is never a crossing / semi-join / association-restricted conjunct.
    * A test asserts that the joined-condition and cross-model cases emit no warnings.
    * Rejected Codex's alternative of a new filter stratum: machinery for a case the anchor rule rules out.

    (codex, partial)
17. **Window buckets come from the population.** For a windowed FILTERed custom aggregate, `c` filters only the interval rows (`_src`). The local windowed producer's bucket set / endpoints are the population's cells, via an endpoints plan like `_window_endpoints` where the local path lacks one, so a bucket with no passing rows stays. Tests cover a bucket with no passing rows of its own but a passing row in its frame (value), and a completely empty frame (NULL). (codex)
18. **The test matrix is enumerated.**
    * Postgres asserts `rows` / `per_order` grouped and ungrouped, the NULL cell, and the saved measure.
    * Custom cases in a measure-typed filter and an order position.
    * The saved measure is declared on the model, which the seeded engine persists through storage.

    (codex)
19. **FILTER on a second-order aggregation is deferred to DEV-2191.** It is refused identically for built-ins today, and its semantics (what `c` reads over operand cells) is a separate design question. (user, DEV-2191 created with the user's yes)

## Risks / Trade-offs

* [The FILTERed custom aggregate now always compiles as a producer, even where it used to be inline] → The extra CTE + join is the composition primitive's normal cost. Built-ins, the common case, keep the inline mask.
* [The grouped-NULL / total-0 asymmetry at the empty grain] → Documented in the spec, recorded on DEV-2094.
* [The windowed local producer may not yet draw buckets from the population] → Decision 17 plus its two tests; if the local path needs a new endpoints plan, it mirrors `_window_endpoints`.
* [The stage-source producer (`producer_model is None`) may need more than wiring `c`] → If it needs a new mechanism beyond the producer's row filters, stop and raise it rather than xfail.
