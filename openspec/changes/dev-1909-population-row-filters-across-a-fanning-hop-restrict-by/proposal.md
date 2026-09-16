## Why

A row-level filter conjunct that reaches the query population only across a fanning hop is
applied to the host base query through a LEFT JOIN, so every aggregate evaluated over those rows
counts a host row once per matching joined row (`customers` filtered on `orders.status = 'ok'`:
`spend:sum` 520 instead of 420). DEV-1900 landed an interim typed error for the inline case only;
host-rooted producers built by the local regroup path (partitioned, windowed, first/last) inherit
the same filters verbatim and fan silently today (`sum(spend, partition_by=tier)` 290 instead of
190; `sum(spend, window='1y')` 520 instead of 420), the association producer misbinds a filter
sharing the dimension's branch (250/250 instead of 100/150), and raw-row mode returns
join-multiplied rows. The population's row filters are disposed in three places and only one of
them is safe.

## What Changes

- **One population disposition.** The host's ROW-phase filter conjuncts are disposed once, at
  the host root, into inline / semi-join / excluded, and every consumer reads that one result:
  the host base query and every producer rooted at the population (partitioned, windowed,
  first/last, crossing-local, broadcast-local, association, host-grain wrap). A conjunct reaching
  the population only across a fanning hop restricts it by association — a correlated `EXISTS`
  on the host base query — so the population is "host rows related to at least one row passing
  the predicate, each once", in every query shape and every `to_many_handling` mode.
- **Same-row binding is per consumer.** A consumer applies a fanning conjunct inline iff its own
  grain already materialises every fanning path of the conjunct (a projected dimension on the
  same branch); otherwise by semi-join. This keeps filter-and-dimension-on-one-branch queries
  bound to one joined row and fixes the association producer.
- **The DEV-1900 interim guard is retired.** Its fanning arm becomes the semi-join; its
  unanalyzable arm becomes a typed checker error raised by the disposition itself, for host and
  producers alike; a conjunct outside pushdown scope (OR/NOT mixing local and cross-path
  references, or several branches) stays applied through the join, is dropped from producers
  with the dropped-filter warning, and fails closed with a typed error only when a plain
  aggregate is evaluated inline over the population.
- **Reporting.** The population push surfaces as the existing `kind: "semi_join_pushed"`
  response entry with `measure: null` (the field becomes optional); never an error, no
  Python-level warning.
- **Raw-row mode** returns one row per population row passing the restriction.
- Goldens for shapes that fanned through the host base re-bless; the producer-only empty-base
  spine carries the semi-join so a population nobody passes yields zero rows.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/semantics`: MODIFIED *Filters restrict by association or fail loudly* — the population
  itself restricts by association (semi-join on the host base), same-row binding when the
  consumer's grain materialises the branch, the interim guard replaced by the out-of-scope and
  unanalyzable residues; MODIFIED *Grain guarantee* — raw-row mode is never multiplied by a
  population filter's join.
- `queries/cross-model-aggregates`: MODIFIED *Producer filter routing* — every producer rooted at
  the population inherits the population's disposition (including nested ones), same-row binding
  for conjuncts its grain materialises, and the informational entry for the population push names
  no aggregate.

## Impact

- `slayer/engine/compile/stages.py` (population disposition object and its consumers; guard and
  `_regroup_inherited_filters` deleted; association producer's duplicate disposition removed),
  `slayer/engine/elaborate_env.py` (two checker rules replace one), `slayer/ir/prebound.py` and
  `slayer/ir/planned.py` (semi-join groups on the prebound, `root_relation`, empty-base gating),
  `slayer/sql/generator.py` (empty-base placeholder applies the semi-joins),
  `slayer/core/warnings.py` + `slayer/engine/query_engine.py` (optional `measure`, top-level
  entries).
- Tests: new `tests/test_dev1909_population_pushdown.py`, `tests/test_dev1909_golden_sql.py` +
  baseline; `tests/test_dev1900_population_guard.py` deleted; six xfails and one golden
  carve-out re-pointed; `dev1747` / `dev1900` baselines re-blessed; ledger rows swapped;
  repository-wide audit of customers-rooted fanning filters.
- Docs: one sentence in `docs/concepts/queries.md` (Filters and Auto-Joins) and the `warnings`
  table cell; `architecture/semantics.arc42.md` axiom 14 gains an enforced tag (approved
  normative edit, exact diff shown before applying).
