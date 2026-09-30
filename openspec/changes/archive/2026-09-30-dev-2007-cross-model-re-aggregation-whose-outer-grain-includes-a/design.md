## Context

See proposal.md — Why. Verified trace (origin/main `cc795f28`):

- `_synthesize_reaggregation_producer` (`slayer/engine/compile/stages.py`) judges the outer
  grain against the operand union grain (`_reaggregation_determined` → `grain_determines`),
  correctly per Axioms 2.4 / 7.
- It then compiles the outer producer through `compile_synthesized` over a prebound from
  `_regroup_producer_prebound`. The outer aggregate's source is a regroup placeholder
  (`ColumnKey(path=(), leaf="__regroup__N__…")`) that reads as a host column, so in that
  compile `discovery._Walker.local_broadcasts` re-judges the grain from the host, emits
  `target_rooted`, and the outer aggregate leaves the outer plan →
  `outer_plan.aggregate_slots[0]` raises `IndexError`.
- Bind-time `_validate_partition_keys` (`slayer/engine/bind_inputs.py`) →
  `assert_partition_key_attributable` (`slayer/engine/join_safety.py`) judges explicit outer
  keys from the host too (a re-aggregation's source anchor is `()`).
- Probe: carrying the settled outer grain as the outer aggregate's `partition_keys` makes
  every shape compute (values match host-rooted spellings where those work) with the full
  unit suite green and no golden change.

Arc42 principles: semantics Axioms 2.4 (home = operand dataset), 2.8, 2.9, 4 (empty value),
5 (explicit grain = type), 7 (attributable iff home determines), 8 (mode axis; explicit keys),
9 (operators consult types only), 10; engine P9 (user-facing errors raise in the checker),
P10; system P8 (cardinality invariant), P13 (the placeholder encoding's violation is DEV-2017).

## Goals / Non-Goals

**Goals:** one judge of a re-aggregation's outer grain; the decision carried in the key's
type; the `IndexError` class removed for this shape in every position.

**Non-Goals:**
- A dedicated placeholder key kind (the root cause of host-misreads) — DEV-2017.
- Re-aggregation rooted at the to-many model over a `first`/`last` or windowed operand
  (RuntimeError / rendered-schema ValueError) — DEV-2006.
- A filter-only cross-model re-aggregation leaking its placeholder into a sibling producer —
  DEV-1997 (acceptance case added there).

## Decisions

**D1 — One judge, in the synthesis.** `_synthesize_reaggregation_producer` judges every outer
grain member against the union grain (`g in union_grain or _reaggregation_determined(...)`,
plus the existing expression-determined arm). Not determined:
- default (ungrained, `root.partition_keys is None`) member → per mode, unchanged;
- explicit member → `"associate"`: an associate dim (unchanged mechanics); otherwise a new
  checker raise.
Alternatives: keep the bind host check for undetermined explicit keys (two judges, and the
explicit spelling would refuse in associate what the ungrained spelling computes) — rejected.

**D2 — Carry the decision in the type.** After mode resolution build ONE canonical
`outer_agg = substitute_value_keys(root, constituent_placeholders).model_copy(update={"partition_keys": Grain.of(original_by_pk)})`
(the substituted settled outer grain; `Grain.EMPTY` when keyless) and use that same object
for `aggs`, `public_alias_by_agg`, `explicit_types` and the answer-slot lookup. An explicitly
grained aggregate is never re-judged from the host by the outer producer's compile (Axiom 9).
Alternatives: (A) teach `local_broadcasts` to skip placeholder-sourced aggregates — a sixth
`__regroup__` prefix sniff, leaves other source-leaf predicates exposed; (C) a placeholder
key kind — the real root fix but ~20 dispatch sites plus a recursion hazard (DEV-2017).

**D3 — Bind skips only the re-aggregation's own outer keys.** In `_validate_partition_keys`,
`assert_partition_key_attributable` is skipped when `is_reaggregation_key(key)`; everything
else (constituent / parameter aggregates' keys, mixed row+attached sources,
`check_partition_key_resolves`, the combined-consumer "keys are query dimensions" rule) is
unchanged. `rewrite_rank_partition_keys` visits every nested key separately, so a
constituent's keys are still checked.

**D4 — Checker raise with resolved facts (engine P9).** A new `elaborate_env` function takes
compiler-resolved facts only (alias, the undetermined explicit key displays, operand grain
display, mode) and raises `PartitionKeyError` naming the key, the operand grain and the
remedy (add it to the inner `partition_by=`, or choose `to_many_handling='associate'`).
`grain_determines` and mode planning stay in `stages.py`. Registered in the raise ledger
(`tests/_dev1871_raise_ledger.py`) for raise-parity.

**D5 — Keyed answer lookup.** `outer_plan.aggregate_slots[0]` becomes
`_regroup_answer_slot_id(value_slots=<outer plan value slots>, key=outer_agg, fallback=None)`
— the existing helper, which asserts with a message naming the missing key (internal
invariant style, as elsewhere).

**D6 — Docs.** One sentence appended to the re-aggregation section of
`docs/concepts/formulas.md`: "An outer dimension or explicit outer `partition_by=` key that
the operand's grain determines is attributed even when the query root reaches it only across
a to-many join (e.g. a joined model's time bucket); an explicit outer key the grain does not
determine is an error outside `associate` mode."

**D7 — arc42 (approved).** `semantics.arc42.md` Axiom 8: "crosses a fanning hop from its
host" → "from its home"; Axiom 7 gains
`[enforced: test:tests/test_reagg_outer_grain_attribution.py]`.

## Test oracles

Fixture `tests/_reagg_outer_grain_fixtures.py` (SQLite + DuckDB): `customers(id, name)` =
(1 Ann), (2 Bob), (3 Cy — no snapshots), (4 Dee); `account_snapshots(id, account_id, customer_id,
snapshot_date DATE, balance)` many-to-one → `customers`:
(1,10,1,2024-01-05,160) (2,10,1,2024-01-20,150) (3,11,1,2024-01-10,30) (4,10,1,2024-02-03,120)
(5,11,1,2024-02-15,40) (6,11,1,2024-02-25,35) (7,20,2,2024-01-07,500) (8,20,2,2024-02-07,700) (9,30,4,2024-01-12,80) (10,30,4,2024-02-12,60)
(11,31,4,2024-02-18,25) — Dee's account 31 is February-only, so association differs from broadcast.
`P = partition_by=[account_snapshots.account_id, id, account_snapshots.snapshot_date]`,
`Q = partition_by=[account_snapshots.account_id, id]`.

| Shape (rooted at `customers`) | Oracle |
| -- | -- |
| `sum(sum(balance, P))` by name, month | Ann Jan 340, Feb 195; Bob 500, 700; Dee 80, 85; Cy (NULL, NULL) |
| `sum(max(balance, P))` | Ann 190 / 160; Bob 500 / 700; Dee 80 / 85 |
| `sum(last(balance, P))` | Ann 180 / 155; Bob 500 / 700; Dee 80 / 85 |
| `sum(sum(balance, window='60d', Q))` | Ann 340 / 535; Bob 500 / 1200; Dee 80 / 165 |
| `sum(max(balance, Q))` by name, account_id | (Ann,10) 160, (Ann,11) 40, (Bob,20) 700, (Dee,30) 80, (Dee,31) 25, (Cy,NULL) NULL |
| `sum(sum(balance, P))` by month only | Jan 920, Feb 980, NULL-month row (Cy) NULL |
| `sum(max(balance, Q))` by name, month, associate | Ann 200 / 200; Bob 700 / 700; Dee 80 / 105 |
| same, default mode | broadcast Ann 200 / 200, Bob 700 / 700, Dee 105 / 105 + warning; outer grain lacks month |
| same, error mode | ReaggregationError |
| `sum(max(balance, Q), partition_by=[account_snapshots.account_id])` by name, account_id | 160 / 40 / 700 / 80 / 25 |
| `sum(max(balance, Q), partition_by=[name, account_snapshots.snapshot_date])` by name, month | default/error: PartitionKeyError; associate: 200 / 200 / 700 / 700 / 80 / 105 |
| `count(max(balance, Q))` by account_id | NULL→0, 10→1, 11→1, 20→1, 30→1, 31→1 |
| `sum(max(balance, Q))` by name, band (band = max(balance, Q) > 100) | (Ann,hi) 160, (Ann,lo) 40, (Bob,hi) 700, (Dee,lo) 105 |
| `sum(max(balance, Q))` keyless | one row, 1005 |

Case 3 on `tests/_dev1847_fixtures.py` sales: `avg(sum(amount, partition_by=city), partition_by=[region])`
by region → default/error PartitionKeyError; associate North 65, South 85
(`ASSOCIATE_AVG_CITY_BY_REGION`). `sum`/`max` rows also equal the same query rooted at
`account_snapshots` (`last`/windowed host-rooted forms are DEV-2006). Oracles confirmed by a
probe patch on SQLite and DuckDB. Depth-3 / computed-dimension / transform-input /
aggregate-parameter oracles are hand-derived in pr-tests from the same fixture.

## Risks / Trade-offs

- [Stamped `partition_keys` changes routing elsewhere (`combined_kind`, nesting rule,
  `_strip_redundant_partitions`, keyless `Grain.EMPTY`)] → probe ran the full unit suite
  green; explicit keyless / expression-grain / broadcast-dropped-dim tests added.
- [Behaviour change for explicit outer keys (case 3 broadcast → error, case 2 associate →
  value)] → spec-declared (BREAKING in proposal); any existing test pinning the old
  behaviour is a STOP-and-ask in implementation.
- [DEV-2006 in flight edits `_producer_nesting_rule` and the carrier] → touch points are
  disjoint (outer aggregate construction and bind), but merge origin/main before
  implementing and re-run the suite.
