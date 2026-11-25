## Context

See proposal.md for motivation. Today one parsed call node binds two ways
(`slayer/engine/binding.py` `_bind_agg_call_in`): a row-level or mixed operand gives a
ranked `AggregateKey` rendered with `ROW_NUMBER` (`slayer/sql/render/ranked.py`); an
attached-only operand gives a collapsing `TransformKey` rendered with `FIRST_VALUE`
(`slayer/sql/generator.py` `_render_window_transform_sql`), lowered to
`max(t, partition_by=…)` as a constituent (`slayer/core/keys.py`
`lower_collapsing_constituents`) and joined back through a collapsed regroup
(`slayer/engine/compile/stages.py` `_collapsed_axis`, `RegroupAttachPlan.collapsed`).
The two forms have different argument rules, refusals, typing and row supply.

Structural cause: one operation (a ranked pick over a dataset's rows) modelled as two
classes. Axiom 6 makes an aggregate a dataset and Axiom 2.4 homes a second-order
aggregation on its operand dataset, so one class suffices.

Constraint (DEV-2451 stored-format condition, in the issue body): no stored
`first(…)` / `last(…)` metric changes meaning; NULL behaviour stays as today.

Applicable principles: semantics Axioms 2.3–2.5, 2.7, 2.8, 4, 5, 6, 8, 9, 10, 11, 12.3–12.4
and Laws 3, 4, 5, 7, 8; system P4 (sqlglot AST), P8 (one reader of Mode B), P10 (one
typing), P11 (fail at save time); engine P1, P6, P10 (dependencies are closures); sql P1,
P9 (fail closed), P10 (one composition primitive), P12 (attach is cardinality-neutral).

## Goals / Non-Goals

**Goals:**
- One ranked aggregation for both forms; the collapsing-transform class is deleted and
  every transform preserves its operand's grain.
- Values of every shape the transform reading accepted are unchanged.
- One ranking-key predicate for both forms (closes the ranking-key half of DEV-2426).
- `first` / `last` supply rows like any aggregation (the `first` / `last` half of DEV-2419).

**Non-Goals:**
- `window=` and `FILTER` on a re-aggregation's outer aggregation, `first` / `last` over an
  aggregate included (DEV-2422).
- The windowed half of DEV-2419.
- The `partition_by=` cases of DEV-2426.
- Ossie `LAST_VALUE` NULL-key placement (DEV-2457, DEV-2461).
- Removing a ranking key's bucket from the grain (only an exact match is consumed).

## Decisions

### Semantics

1. **One ranked aggregation.** `first` / `last(value, [key], partition_by=, window=, FILTER)`
   is always an aggregation. Over a row-level or mixed source it ranks its home's rows (a
   mixed source is a row dataset: its attached aggregates are broadcast onto them, Axiom
   2.3). Over a source with no row-level leaf (`last(sum(x))`, a saved aggregate metric) it
   is a second-order aggregation ranking the operand dataset's cells (Axiom 2.4). A
   dataset-defined custom `first` / `last` is unchanged.
2. **Operand dataset** of the second-order form: exactly the transform domain of Axiom
   11.6 — its constituents' own cells at the recursive union of their grains (Axiom 10,
   as 11.1), coarser constituents broadcast to that union; extended over the spine only as
   11.6 says; never cells a sibling term supplies.
3. **Ranking key legality.** The key must be determined by the home (a field of the
   operand dataset), else the typed input-safety error. Row operand: a column, anything
   reached over to-one hops, or an attached aggregate broadcast onto the rows. Aggregate
   operand: a grain member, or an aggregate whose grain the operand determines
   (`last(sum(rev), sum(units))`). A literal stays refused.
4. **Default ranking key.** Over rows unchanged (explicit → first DATE / TIMESTAMP row-level
   dimension → the first time dimension's raw field → `default_time_dimension`). Over an
   aggregate: the query's active time bucket; when it is not in the operand grain the
   query fails with `TimeAxisError`.
5. **Grain.** Base = `partition_by=` when given, else the default: the query's dimensions
   over rows, the operand's grain over an aggregate. An unwindowed ranked aggregation then
   drops its ranking key from that base when the key is a member — exact structural match
   only, so a raw `ordered_at` key never drops `month(ordered_at)`. A windowed one keeps
   the time bucket (Axiom 2.3).
6. **A consumed key is a requested grain** (Axiom 8): the coarser result is broadcast like
   an explicit `partition_by=` — no broadcast warning, valid in every `to_many_handling`.
7. **NULLs** as today, both forms: a NULL ranking key is never picked; all-NULL keys give
   NULL; the picked value is returned as is, NULL included. Over an aggregate a NULL time
   bucket is a NULL key.
8. **Empty value**: a cell with no operand rows / cells is NULL. Ties on a non-unique key
   are nondeterministic (semantics Notes §5).
9. **`window=` / `FILTER`** over an aggregate operand fail with the re-aggregation errors
   (`ReaggregationError`, `AggregationFilterError`).

### Implementation

- `slayer/core/enums.py`: drop `first` / `last` from `TIME_TRANSFORMS` and the transform
  allow-lists; delete `AXIS_COLLAPSING_TRANSFORMS` after auditing every reference — boolean
  and value-type preservation (`slayer/core/keys.py` ~1636–1647, ~1916) moves onto the
  ranked `AggregateKey` (system P10).
- Binding: an attached-only operand binds as a ranked second-order `AggregateKey`; the
  transform route for `first` / `last` is deleted. `_is_row_level_ranking_key` is replaced
  by the one "determined by the home" predicate, judged by closure at the existing sites
  (`_ranking_key_crossing` / `_checked_ranking_time_key`), extended to the operand
  dataset — no second resolution path (DEV-1945 ruling).
- Grain: the consumption rule is applied where an aggregate's grain is computed; the
  collapsing branch of `_operand_grain`, `lower_collapsing_constituents`,
  `_collapsed_axis` and `RegroupAttachPlan.collapsed` are deleted; the collapse remedy of
  `_axis_suggestion` is reworded for a nested ranked aggregation that consumed the time
  bucket (decision 26).
- Emission: a second-order ranked aggregation renders through the re-aggregation producer
  with the existing ranked kernel (`ROW_NUMBER` over the operand cells, sql P10); the
  `FIRST_VALUE` branch of `_render_window_transform_sql` is deleted. A row-form attached
  key reuses the mixed-source attach so the value sits on the rows before `ROW_NUMBER`.
- Row supply: `_BROADCAST_ONLY` in `slayer/engine/population/row_supply.py` is deleted.
- `architecture/semantics.arc42.md`: the approved edits (decisions 23, 24), applied verbatim.

### Decisions log

1. One ranked aggregation; the collapsing-transform class is deleted — over keeping two
   classes; the issue's proposal and the structural fix (Axioms 2.4, 6). (default)
2. Default grain: over an aggregate the operand's grain minus the key; over rows the
   query's dimensions minus the key if present — over "query dimensions minus key for
   both" (changes stored grained-inner and dimension-position metrics) and "require
   `partition_by=`" (invalidates stored `last(sum(x))` text). (user)
3. The key is dropped only on an exact structural match — keeps per-period closing values
   (`last(balance)` by month); over "also drop buckets of the key". (user)
4. An explicit `partition_by=` also drops the ranking key, both forms. (user)
5. Key legality: the key is a field of the operand dataset (determined by the home), else
   a typed error. (user)
6. A mixed operand is a row dataset; a product of unrelated datasets is no operand
   (Axioms 2.2, 2.8). (user)
7. Ruling: a field-form key that is a query dimension — explicit, or defaulted (the first
   DATE / TIMESTAMP row-level dimension) — today an arbitrary tie-pick within each cell,
   now gives one value per coarser cell. (user)
8. NULLs unchanged: NULL ranking keys never picked, the picked value returned as is.
   (user, issue body "Stored-format constraints")
9. Default key over an aggregate: the active time bucket, else `TimeAxisError`. (default)
10. Ossie core has no `FIRST` / `LAST` aggregation (only `FIRST_VALUE` / `LAST_VALUE`
    windows), so the superset principle does not constrain it; the `LAST_VALUE` NULL-key
    divergence stays with DEV-2457 / DEV-2461. (default)
11. Ties on a non-unique key are nondeterministic (Notes §5). (default)
12. A consumed key is a requested grain (Axiom 8): no broadcast warning, every mode.
    (default)
13. A windowed ranked aggregation keeps the time bucket (Axiom 2.3). (default)
14. The ranking-key half of DEV-2426 is in scope — one key predicate for both forms, over
    a form-specific exception; its `partition_by=` cases stay in DEV-2426. (user)
15. `window=` / `FILTER` over an aggregate stay with DEV-2422, refused with the
    re-aggregation errors — over closing DEV-2422 here. (user)
16. The `first` / `last` half of DEV-2419 is in scope (`_BROADCAST_ONLY` deleted).
    Ruling: inferred-query row sets of associated `first` / `last` change (missing rows
    appear, spurious rows go); values in surviving cells unchanged. Windowed half stays
    in DEV-2419. (user)
17. No new capability: modified requirements in existing specs only. (default)
18. The operand grain is the recursive union of the constituents' grains with coarser
    constituents broadcast, then the key is consumed; parity tests for mixed-grain
    arithmetic, nested transforms and unrelated constituents. (codex)
19. The operand dataset is exactly the Axiom 11.6 domain — never sibling-supplied cells;
    tests for a spine-backed `last(count(*))` with an empty last bucket and a non-spine
    operand beside a sibling contributing a later bucket. (codex)
20. Audit `AXIS_COLLAPSING_TRANSFORMS` references before deleting it; boolean / value
    typing moves to the ranked `AggregateKey`; boolean `first` / `last` tests in filters,
    comparisons and the second-order form. (codex)
21. Stored-metric compatibility tests through on-disk `YAMLStorage` (row-ranked,
    aggregate-ranked via a saved inner, nested, filter / order key, spine), plus a
    dataset-defined custom `first` / `last` shadowing the built-in. (codex)
22. Dry-run SQL-shape / golden coverage on every Tier-1 dialect for the second-order form
    (NULL-key ordering, a picked NULL value, consumed-key attach). (codex)
23. Approved `architecture/semantics.arc42.md` edit — (user, approved wording):

    Axiom 2.3, replace
    `— its grain: the explicit \`partition_by=\`, else the query's dimensions; for a transform, its result grain (Axiom 11), where a windowed inner's`
    with
    `— its grain (Axiom 5; for a transform, its result grain, Axiom 11), where a windowed inner's`

    Axiom 2.4, replace
    `The ordering key of a ranked aggregation must be determined by the home and never widens it; the aggregation's own \`partition_by=\` is not an input.`
    with
    `A **ranked** aggregation (\`first\`, \`last\`) picks the value of the home row with the least or greatest **ranking key**, which must be determined by the home and never widens it; the aggregation's own \`partition_by=\` is not an input.`
    and add `[enforced: test:tests/test_dev2436_ranked_aggregation.py]` to Axiom 2's tags.

    Axiom 4, replace
    `A built-in aggregation skips NULL inputs, so a cell whose inputs are all NULL takes the same empty value —`
    with
    `A built-in aggregation skips NULL inputs (a ranked one, NULL ranking keys, returning the picked value as is), so a cell whose inputs are all NULL takes the same empty value —`

    Axiom 5, replace its text with
    `**Grain and cells**: an aggregate is typed by its grain — its partition_by dimension set, defaulting to the query's dimensions, or for a ranked aggregation over an aggregate to that operand's grain; an unwindowed ranked aggregation's grain drops its ranking key. One combination of grain values is a cell.`
    keeping `[enforced: test:tests/test_dev1871_grain_retype.py]` and adding
    `[enforced: test:tests/test_dev2436_ranked_aggregation.py]`.

    Axiom 11, intro becomes
    `**Transforms are typed**: a transform consumes an aggregate-valued dataset and produces one at its operand's grain, consulting only its operand's type (Axiom 9). That grain is resolved by:`;
    11.2 becomes
    `**11.2 Timeless.** The rank family's own \`partition_by=\` partitions the operand's cells and must name operand-grain members.`;
    11.3 opens
    `**11.3 Time-ordered** (\`cumsum\`, \`lag\`, \`lead\`, \`time_shift\`, \`change\`, \`change_pct\`, \`consecutive_periods\`). The axis is the query's active time bucket and must be in`
    (rest of its first three sentences unchanged), and the **11.3a Preserving** /
    **11.3b Collapsing** sentences are deleted.

24. Approved Axiom 9 edit — (user, approved wording): in the typed-residue sentence,
    `an aggregate in dimension position must declare its grain explicitly — the ungrained default`
    becomes
    `an aggregate in dimension position must declare its grain explicitly, unless Axiom 5 types it by an aggregate operand — the ungrained default`,
    and `a transform in dimension position must likewise wrap aggregates, each declaring \`partition_by=\``
    becomes
    `a transform, or such a ranked aggregation, in dimension position must likewise wrap aggregates, each declaring \`partition_by=\``.
    So `last(sum(x, partition_by=[region, month]))` stays legal as a dimension and
    `last(sum(x))` there fails with the transform-residue error.
25. A ranking key may be any attached value — an aggregate or a transform — whose grain
    the home determines (Axioms 2.3, 9: refusing a transform would be a closure
    violation). (default)
26. Time-axis messages: the `date_trunc` remedy for extraction dimensions applies to a
    defaulted key over an aggregate as to any time-ordered term; the nested-collapse
    suggestion is reworded to say the inner ranked aggregation consumed the time bucket.
    Message wording is not pinned by tests. (default)
27. A dataset-defined custom `first` / `last` over an aggregate stays a custom
    re-aggregation, as today. (default)
28. The stale `queries/partitioned-aggregates` scenario "Associated ranked picks stay a
    typed refusal" (no code raises that error since ranked picks joined the association
    kernel) is aligned: associated ranked picks associate like any aggregation, `error`
    mode refuses. (default)

## Risks / Trade-offs

- [Value drift on stored metrics through the new emission path] → parity tests with
  independent oracles for every transform-accepted shape, on SQLite and DuckDB, plus
  on-disk stored-metric tests (decisions 18–21).
- [Dialect differences in NULL ordering / row selection of the ranked kernel over operand
  cells] → Tier-1 dry-run SQL-shape goldens (decision 22); the ranked kernel already
  orders NULL keys explicitly on every dialect.
- [Visible row-set change for associated `first` / `last` in inferred queries] → recorded
  ruling (decision 16), pinned by DEV-2419's repros.
- [Existing tests pinning the transform reading] → aligned only with per-file approval in
  pr-tests (tasks.md lists them).
