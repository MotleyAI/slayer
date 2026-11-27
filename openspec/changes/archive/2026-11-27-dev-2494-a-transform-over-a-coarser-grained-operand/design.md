## Context

See proposal.md — Why. Traced on `main` at `3bc68c4c3`, SQLite and DuckDB:

- `_route_producer` (`slayer/engine/compile/stages.py`) passes discovery `TransformRouting(answers=<every non-dimension declared metric>)`, and `routes()` (`slayer/engine/compile/discovery.py`) never routes an answer. So every transform answer of every synthesized producer is evaluated over that producer's own rows.
- That holds for a transform's own domain producer (`_synthesize_local_regroup`, `domain_of` set) and the shifted producer, whose rows are their answer's cells. It fails for the re-aggregation carrier (`_synthesize_reaggregation_producer` → `_build_carrier_attach`), whose rows are its operand supply at `carrier_grain`. That grain is wider than a transform constituent's result grain when an associated outer key (`row_outer`), a finer sibling source constituent (`union_grain`) or a parameter / ranking-key field joins it. In the issue's repro the carrier is at (city, region) and the rank runs over four rows instead of two cells.
- Metric position goes through `routes()`, which gives the transform its own domain producer. That is why the metric `r` is right and `m` is wrong.
- Further instances on `main`: a finer sibling (`max(rank(...) + sum(..., partition_by=[region, city]))` by region: North 73, truth 72), `sum(rank(...))` and `max(percent_rank(...))` by region and city, and a transform `weight=` parameter (no dimensions: 53.125, truth 320/6).
- Time-ordered transforms in the probed shapes are already right: their window partitions by every non-time carrier key.

Probe (temporary, reverted). Opt-in for (a) producers with `domain_of` and (b) a carrier whose sole cell-supplying source constituent is a transform with `carrier_grain == union_grain`; no answers for every other producer.
- All repros right on both backends.
- 183 transform / re-aggregation / golden test files green (7983 tests); no golden SQL change.
- Without (b), 93 tests fail ("Cross-dataset aggregate slot survived the regroup desugar") for carriers already at the transform's grain with cross-dataset inners.

Principles that apply:
- semantics Axioms 2.3 (opaque constituent: only its type is consulted), 2.4, 6, 8, 10, 11.4, 11.6; Laws 3 and 4;
- engine P4 (one interned producer), P6 (placement from typing), P9 (the checker owns user-facing type errors; the new check is an internal invariant, not a user error);
- system P4 (SQL built as AST).

The spec delta stays under the cross-cutting `queries` spec group (`architecture/index.yaml`).

## Goals / Non-Goals

**Goals:**
- One rule for where a transform evaluates, the same in metric position, aggregation source and aggregation parameter.
- The bug class (a transform evaluated over rows finer than its operand) fails loudly at plan time instead of returning a wrong value.
- No golden SQL change for shapes already right.

**Non-Goals:**
- A re-aggregation parameter combining several aggregates or transforms (`weight=rank(...) + 0`, `weight=sum(...) * sum(...)`), refused or crashing today for a separate cause: DEV-2505. The finer-sibling shape is therefore tested as a source only.
- Interning the metric-position and aggregation-position producers of one transform into one CTE (engine P4 would welcome it; not required here).

## Decisions

Decisions log — `(user)` the user's call, `(default)` an obvious default, `(codex)` a Codex plan-review finding folded in.

1. **Structural fix: in-place evaluation is opt-in, every other transform takes the one routing rule.** The blanket `TransformRouting.answers` exemption retires. A synthesized producer evaluates a transform over its own rows only when it declares that transform an in-place answer. Every other transform, in any plan, goes through discovery's `routes()`, exactly as in metric position. Alternative: patch the carrier to drop associated outer keys — rejected, it leaves the finer-sibling and parameter widenings and the two code paths standing. (user: "go for the structural solution, not a band-aid")
2. **Who declares in-place answers, by their declared answers (not `domain_of`).** The producer context carries an explicit set of in-place transform answers, keyed by the producer's own declared answer keys:
   - the transform's own domain producer: its answer;
   - the shifted producer: its declared answer (the shifted operand), whose rows are that operand's cells — DEV-2429's moved-frame rule relies on this;
   - the re-aggregation carrier: per decision 3.

   `domain_of` keeps its rendering role (the domain base) and no longer implies in-place evaluation. (default; codex finding 4)
3. **The carrier is a transform's domain iff its rows are that transform's cells.** That is: the transform is the carrier's only cell-supplying source constituent, and the carrier grain equals its result grain. The predicate reads only the transform's identity and grain (Axiom 2.3) and the carrier's own supply, never the transform's interior, so it holds unchanged when DEV-2375 turns attached values into their own key kind. Otherwise the transform gets its own domain producer and joins the carrier as a coarser constituent broadcast onto its rows (Axioms 2.3, 10). Alternative: always route out of the carrier — rejected; it is the same value through an extra producer, churns goldens, and broke carriers with cross-dataset inners in the probe. (default; codex finding 2)
4. **Routed transforms use the existing regroup attach path.** No new hand-carried attach plumbing (`carried_attaches`, `placeholder_originals`), so DEV-2375's one attach rule converts it with everything else. (default)
5. **Plan-time invariant.** A transform evaluated over a plan's own rows must have an operand grain equal to that plan's grain up to functional determination: both are compared after the same pruning of determined members. Otherwise the plan fails at plan time with an internal error, never a wrong value. Exact set equality was rejected: a producer's physical grain drops determined members. (default; codex finding 3)
6. **The outer aggregation's `associated` warning stays.** The outer grain's unattributable keys associate per Axiom 8 and the DEV-2416 spec. (default)
7. **Spec delta.** MODIFIED "Transforms evaluate over their operand's domain": the normative sentence lists all six positions explicitly, and the invariance sentence names the enclosing aggregation's dimensions, siblings and parameters. Scenarios cover source, parameter, filter and order, plus regression guards. (default; codex finding 7)
8. **Coverage of both positions.** Every core shape runs as an aggregation source and as a `weighted_avg(..., weight=T)` parameter, on SQLite and DuckDB:
   - the three issue repros;
   - `percent_rank`;
   - not the finer sibling, whose parameter form is DEV-2505's.

   (codex finding 1)
9. **Filter and order scenarios** keep the approved Law 4 tag honest: `max(rank(...))` as a filter and an order key reads its metric-position value. (codex finding 5)
10. **A carrier-in-place guard where the transform's cells reach beyond the host:** Appendix B `sum(rank(sum(returns.amount), direction='desc'))` by `customers.region` keeps the NORTH cell only `returns` reaches. (codex finding 6)
11. **`time_shift` guards inside an aggregation** (`max(time_shift(cumsum(...), -1))`, `max(time_shift(rank(...), -1))`) pin the shifted producer's in-place answer across the DEV-2429 merge. (codex finding 4)
12. **Time-ordered transforms** already right in the probed shapes are covered as regression guards, not failing tests. (default)
13. **Composite re-aggregation parameters out of scope**, filed as DEV-2505 (child of DEV-2415). (user)
14. **Tests** in `tests/test_transform_domain_in_aggregation.py`, named descriptively like `tests/test_transform_operand_grain.py`, whose `city_engine` seed it reuses. Appendix B cases use `tests/_dev2212_fixtures.py`. (default)
15. **Docs:** one sentence in `docs/concepts/expressions.md` § "Transforms read their operand's cells" that this holds inside an aggregation too. (default)
16. **arc42 tags.** (user, approved wording) In `architecture/semantics.arc42.md`, after `    [enforced: test:tests/test_transform_operand_grain.py]` in Axiom 11's list, add the line `    [enforced: test:tests/test_transform_domain_in_aggregation.py]`. Law 4's line `to the value. [review] [enforced: test:tests/test_dev2218_transform_domain.py]` becomes `to the value. [review] [enforced: test:tests/test_dev2218_transform_domain.py] [enforced: test:tests/test_transform_domain_in_aggregation.py]`.

## Risks / Trade-offs

- [Producers other than the three named ones lose in-place evaluation of a transform answer] → the probe ran that exact rule over 183 files with no failure or golden change; the plan-time invariant catches any producer that should have declared one.
- [The invariant misfires on a valid producer] → grains compared after determination pruning; a unit test where a determined member is pruned.
- [Textual conflicts with DEV-2375 (producer context, `_route_producer`) and DEV-2429 (`routes()`)] → semantics are orthogonal; the guards of decisions 10–11 pin the behaviour across either merge order.
