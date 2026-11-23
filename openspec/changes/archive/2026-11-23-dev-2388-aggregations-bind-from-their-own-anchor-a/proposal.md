## Why

An aggregation is bound, homed, filtered, associated and carried in its population's coordinates: its home is found
through the population's route to the metric's anchor. A named population with two routes to that anchor is refused
although Ossie answers it (Layer 3 §5.4: each branch is evaluated from its home), a metric written through another
anchor walks back into its host, a column's first spelling decides its anchor, and a re-aggregation counts the host's
cells — so its value changes with the population and with sibling measures (Law 7). DEV-2212 left the `anchored_rows`
workaround for the first case; this change removes the cause.

## What Changes

- An aggregation binds from its own **source anchor** (semantics Axiom 2.6): the dataset its source's references are
  anchored at, or, when they are anchored apart, the smallest dataset determining every leaf's anchor over provably
  to-one hops; tied candidates are refused as ambiguous. Its source, parameters, definition defaults, `FILTER (WHERE …)`
  and ranking keys never route through the population.
- Every producer is rooted at its home, reads its grain and the filters restricting it from the home by each
  reference's own spelling, associates per home row inside the home's own join tree, and meets the result only on grain
  values.
- A named population decides only the rows (Axiom 2.9). **BREAKING**: a restricted population (model filters, a
  restricting stage) no longer narrows a home that does not read it on its own paths.
- A second-order aggregation runs over the row supply of its constituents at their union grain (Axiom 2.4), never over
  the population's rows. **BREAKING**: re-aggregation values can change where the population held union-grain cells no
  constituent reaches, or missed cells a constituent's home reaches.
- Each spelling of a reference is routed independently by every reader; a reference's order in the query never decides
  its anchor.
- The `anchored_rows` workaround and the inferred-stage hosting at a supplying home are removed; no value or refusal of
  an inferred stage depends on which dataset hosts it.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/semantics`: an aggregation binds from its own anchor (ADDED); a second-order aggregation's operand rows are
  its constituents' row supply; a home's candidates and definition defaults resolve from the source's anchor, never the
  query root; inside a second-order source the operand dataset, not the population, supplies grain-union cells
  (MODIFIED).
- `queries/references`: an aggregation's source anchor is a reading dataset; each spelling is routed on its own
  (MODIFIED).
- `queries/population`: an anchored query keeps its rows; its values are its homes' values (MODIFIED).
- `queries/multi-stage`: a restricted stage restricts a home only along the home's own paths (MODIFIED).
- `models/model-filters`: a model filter restricts only reads of its model; as the population it decides only the rows
  (MODIFIED).
- `queries/cross-model-aggregates`: inside a re-aggregation the empty value fills operand cells only (MODIFIED).
- `queries/attribution-modes`: a partition key fans or not from its own anchor, never from the population (RENAMED,
  MODIFIED).
- `queries/transforms`: a shifted leaf's fanning partition key is judged from the key's anchor (MODIFIED).

## Impact

- `slayer/engine/binding.py`, `slayer/engine/bind_inputs.py`: aggregate interiors bound in their anchor's scope;
  reference identity carries the anchored spelling.
- `slayer/engine/population/home.py`, `row_supply.py`, `query_engine.py`: home from the anchor; `anchored_rows`,
  `_anchored_rows`, `_StageInference.anchored` removed; `plan_rows` host choice.
- `slayer/engine/compile/stages.py`: producer synthesis, association arm, `_host_membership` removed, re-aggregation
  carrier over the constituents' row supply.
- `slayer/core/keys.py`, `slayer/ir/prebound.py`: key identity of anchored references and aggregates.
- `architecture/semantics.arc42.md` (approved edits applied, Axiom 8 included), docs (`concepts/queries.md`, `concepts/references.md`,
  `upgrading/2.0.md`), golden SQL whose join-back or carrier shape changes.
