## Why

`first` and `last` are two different things today: over a field a ranked aggregation,
over an aggregate a collapsing transform with its own argument rules, refusals, typing and
row supply. An aggregate is a dataset whose rows are its cells (semantics Axiom 6), so the
transform is the same ranked pick applied to another dataset; modelling it twice keeps two
implementations that must agree by hand and blocks capabilities aggregations already have.

## What Changes

- `first` / `last` are always aggregations. Over an aggregate they are second-order
  aggregations ranking the operand's cells; the collapsing-transform class is deleted and
  every transform preserves its operand's grain.
- A ranked aggregation's grain drops its ranking key: over an aggregate the default grain
  is the operand's grain minus the key (today's values), over rows the query's dimensions
  minus the key when it is one of them. **BREAKING** (ruling): a field-form ranking key
  that is itself a query dimension now yields one value per coarser cell instead of an
  arbitrary tie-pick per cell.
- One ranking-key rule for both forms: the key must be determined by the home. New: an
  attached aggregate or transform as a row-form key; a grain member or an aggregate as
  the key over an aggregate (`last(sum(revenue), sum(units))`, an argmax).
- `first` / `last` over an aggregate accept `partition_by=`, work in every aggregation
  position and associate like any aggregation; `window=` / `FILTER` on them fail with the
  re-aggregation errors.
- `first` / `last` supply rows in an inferred query like `sum`. **BREAKING** (ruling): an
  associated `first` / `last` gains the rows its home supplies and loses rows only the
  dimension anchor supplied.
- NULL behaviour unchanged; values of every shape the transform reading accepted unchanged.
- `architecture/semantics.arc42.md` Axioms 2.3, 2.4, 4, 5, 9 and 11 updated (approved
  wording in design.md).

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/semantics`: first / last over an aggregate as second-order aggregations; the
  ranking-key grain rule; second-order and row-grain sources type a ranked constituent at
  its own grain.
- `aggregations/functional-form`: dispatch (always an aggregation), ranking-key legality,
  default ranking key over an aggregate, NULL keys over an aggregate.
- `aggregations/expression-aggregation`: a ranked constituent's typing in expression
  sources.
- `queries/transforms`: first / last leave the transforms; every transform preserves its
  operand's grain.
- `queries/partitioned-aggregates`: partitioned / bare ranked measures, re-aggregation over
  first / last, mixed sources, attached parameters, associated ranked picks.
- `queries/population`: first / last supply rows like any aggregation.
- `queries/computed-dimensions`: first / last over an aggregate in dimension expressions.
- `queries/type-errors`: the time-axis error for a ranked aggregation over an aggregate.
- `queries/cross-model-aggregates`: staging scenarios read `last` as an aggregation.
- `queries/measure-naming`: first / last listed as aggregations.
- `queries/multi-stage`: the stage column type of first / last.

## Impact

- Code: `slayer/core/enums.py`, `slayer/core/keys.py`, `slayer/engine/binding.py`,
  `slayer/engine/bind_inputs.py`, `slayer/engine/elaborate_env.py`,
  `slayer/engine/ranked_planner.py`, `slayer/engine/compile/stages.py`,
  `slayer/engine/population/row_supply.py`, `slayer/ir/planned.py`,
  `slayer/sql/generator.py`, `slayer/sql/render/ranked.py`.
- Docs and agent help: `docs/concepts/datasets.md`, `expressions.md`, `references.md`,
  `docs/examples/04_time/time.md`, `slayer/memories/help_content/03_aggregations.md`,
  `04_transforms.md`, `05_time.md`.
- Harness: `architecture/semantics.arc42.md`.
- Stored models: no text migration; stored `first` / `last` metrics keep their values
  except under the two rulings above.
