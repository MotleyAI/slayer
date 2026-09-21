## Why

A row-level filter conjunct that mixes a root-local reference with a cross-path reference under
`OR`/`NOT`, or whose cross-path references span several join branches, cannot be expressed as the
single INNER-correlated `EXISTS` DEV-1840 and DEV-1909 emit, so today it is dropped from every
producer with a warning (an error under `to_many_handling: "error"`) and fails closed with a typed
error whenever it would multiply the population — `tier = 'bronze' or orders.status = 'ok'` with
`spend:sum` errors instead of returning 460. DEV-1909 deferred the proper handling of this whole
class here.

## What Changes

- **Pushdown becomes total over the conjunct's boolean shape.** Every ROW conjunct whose references
  resolve to join paths from the root restricts the dataset by association; a genuinely
  unreachable reference (no resolvable join path) is refused at resolution with a typed error in
  every mode (decision 12).
- **One semantic, stated once.** A root row survives iff the conjunct holds on at least one row of
  the root row's join product over the branches it references, built as the inline path would join
  them (each hop with its declared join type, LEFT by default, so a hop with no related row contributes
  NULL columns); root-local references take the root row's values; conjuncts sharing a branch are
  judged on one product row. Negation keeps the existential reading (`NOT B` = some related row fails
  `B`); a null-test on a related column (`orders.id is null`) reads as absence. This is the semantic the
  host base's inline path and the association arm already have, so the `EXISTS` emission is a pure
  lowering of it (Law 6).
- **Per-branch binding on the host base.** A branch the consumer's grain already materialises binds
  the conjunct's references on that branch to the grouped row; only the remaining branches are
  quantified — filtering and grouping on one branch bind to one row even inside a multi-branch conjunct.
- **Emission.** A group's whole predicate stays inside one correlated `EXISTS` conjunct of the outer
  WHERE; when the predicate provably rejects a hop's NULL extension the hop renders as today's INNER
  correlation (byte-identical for every existing shape), otherwise the hop is LEFT-joined from a
  one-row spine with the correlation in its ON. Groups merge on shared branches; a multi-branch
  conjunct is judged on the product of its branches; two spellings of one edge (its name, its target
  model) share one correlation node.
- **Retired.** The OR/NOT and single-branch pushability restrictions, the population's `excluded`
  disposition, and the `check_population_filter_in_pushdown_scope` checker (its ledger row with it).
  Applies in every `to_many_handling` mode; error mode keeps refusing only lossy outcomes.
- **Reporting.** One `semi_join_pushed` entry per pushed conjunct, exactly as today.
- A pure cross-path null-test push (`orders.id is null`) changes meaning from "some related row has a
  NULL id" to "no related row"; the `excluded/mixed_or` golden re-blesses (the producer gains the
  spine-shaped `EXISTS`); every other golden is byte-identical.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/semantics`: MODIFIED *Filters restrict by association or fail loudly* — the out-of-scope
  residue is replaced by the join-product rule (mixed `OR`/`NOT`, several branches, cross-branch atoms
  all restrict by association; negation existential; null-test as absence; per-branch binding to a
  materialised branch); MODIFIED *Grain guarantee* — raw-row mode over a mixed disjunction returns
  each population row once.
- `queries/cross-model-aggregates`: MODIFIED *Producer filter routing* — pushability total over the
  boolean shape, the product/null-extension definition, same-row binding per shared branch (not per
  first reverse hop) with edge spellings unified, exclusion narrowed to genuinely unreachable
  references; the *Mixed disjunction stays dropped and warned* scenario flips to pushed.

## Impact

- `slayer/engine/compile/stages.py` (push plan without the OR/NOT and single-branch blocks; canonical
  hop identity; union-find grouping in first-appearance order; null-rejection analysis setting per-hop
  null extension; `PopulationFilters` two-way with per-branch materialisation), `slayer/engine/elaborate_env.py`
  (checker deleted), `slayer/ir/planned.py` (`SemiJoinHop.null_extended`), `slayer/sql/generator.py`
  (the one EXISTS builder gains the spine shape, CROSS-joined first-level hops, per-hop join kinds and
  outer-correlated materialised branches).
- Tests: new `tests/test_dev1935_boolean_lowering.py`, `tests/test_dev1935_golden_sql.py` + baseline;
  the residue pins in `test_dev1840_disposition.py`, `test_dev1840_strict_metadata.py`,
  `test_dev1841_association_filters.py`, `test_dev1841_error_mode.py`, `test_dev1836_broadcast_strict.py`,
  `test_dev1909_population_pushdown.py`, `test_dev1747_reroot_filter_routing.py` flip to pushed; the
  dropped-filter dedup tests in `test_dev1745_warning_contract.py`, `test_dev1836_warning_collector.py`,
  `test_dev1838_interning.py` re-point to the `semi_join_pushed` entry; the ledger row is removed;
  `dev1840` golden re-blessed.
- Docs: one sentence in `docs/concepts/queries.md` and one in `docs/database-support.md` (the spine
  shape needs MySQL 8.0.14+; BigQuery may refuse to decorrelate it — a loud engine error).
- `architecture/semantics.arc42.md`: Axiom 3, Axiom 14 and Laws 5 and 6 gain an enforced tag
  (normative edit, exact diff shown for approval before applying).
