## Why

A relationship named exactly `time_spine` takes the only spelling of its dataset's virtual links to the built-in time spine. The planner then finds no route from that dataset to the spine, so a spine query treats it as unrelated and repeats its total in every bucket (`orders` with such a relationship: 220 every month instead of 150 and 70). Ossie allows any relationship name, so the name cannot be reserved; the spine's links must stop depending on relationship names (semantics Axiom 15: a dataset reaches the spine by its nearest axis).

## What Changes

- A dataset's axis link and a spine population's product link to the spine hold whatever names the dataset's relationships carry, in standalone datasets, Ossie semantic-model documents and query-backed datasets or stages alike, in both directions (dataset → spine and spine-rooted).
- A virtual spine hop keeps its ordinary spelling `time_spine`; only when a declared name shadows it does it take a reserved internal spelling, as the population's product hop to an unrelated dataset already does. Queries no relationship name shadows produce byte-identical SQL, results, warnings and errors.
- The reserved spelling never reaches user-visible text; anything meant to be executed or round-tripped (suggested paths, qualified stored text) either resolves back to the same chain or is omitted.
- Route diagnostics stop reporting the shadowed virtual link as an unaddressable pair.
- Unchanged: relationship names stay free (no refusal, rename or warning); an explicit hop `orders.time_spine…` stays ambiguous under the general edge-name rule; a query extension relationship named `time_spine` stays refused; the stored-upgrade scenario "Stored names shadow the spine" is unchanged.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/time-spine`: the requirement "A dataset's axis joins the spine to-one" gains the rule that the axis join holds whatever the dataset's relationships are named, with scenarios.

## Impact

- `slayer/core/relationship_walker.py`, `slayer/core/relationship_search.py`: the virtual spine hops' spelling and addressability.
- `slayer/engine/cardinality/relationship_safety.py`, `slayer/engine/population/dimension_routing.py`: spine routes and product-path recognition read the hop structurally.
- Rendering of paths in SQL aliases, inspect, warnings and errors: unchanged for unaffected queries.
- No stored-format, API or dependency change.
