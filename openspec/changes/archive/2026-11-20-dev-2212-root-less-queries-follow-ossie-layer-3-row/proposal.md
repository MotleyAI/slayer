## Why

A root-less query today takes its rows from one inferred dataset, chosen from the dimensions and the field filters. Ossie Layer 3 takes rows from every metric's home dataset and never lets a filter choose them, so SLayer loses orphan home rows, lets a filter re-quantify the rows, shows dimension values no measure reaches, and refuses measures-only queries Ossie answers (DEV-2212, slice 5 of 2.0, umbrella DEV-2372).

## What Changes

- **BREAKING** A query without `population` lists the **row supply**: the union of the cells of every metric's full-grain aggregates (an aggregate grouped by the query's dimensions whose home determines or associates each). Dimension values no metric reaches disappear (D1/D2 of the issue); an orphan home row lands in the NULL tuple (D3).
- The **dimension anchor** — the smallest dataset determining every dimension, read from dimensions only — supplies the rows of a query with no metrics and extends the cells of coarser aggregates (`partition_by=`, unrelated, broadcast fallback) to full tuples. Groups of dimensions with no join path between them take the product of their anchors. A query fails closed on an undefined anchor only when it needs one.
- **BREAKING** Field filters restrict every home's rows (and the anchor's) and never choose the population or the anchor.
- A query with metrics and no dimensions returns exactly one row (the unit) instead of a population-inference error.
- An aggregate reads its grain dimensions and the filters restricting it from its own home (`queries/references`), in every mode.
- Responses report `population` as the anchor (`null` for the unit) and a new `row_sources` list.
- A saved query pins each inferred stage's anchor as its `population` together with the new public field `population_inferred: true`, which keeps the rows inferred; stored SlayerQuery version 17 → 18.
- Raw-row mode lists the rows of the named population, else of the anchor; every dimension must be provably determined.
- Spine queries keep `time_spine × P`, P now the anchor of the other dimensions; the row supply for spine queries and transform domains follow in DEV-2218.
- A query naming `population` keeps its rows and values.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/population`: the dimension anchor and the row supply replace the dimension-and-filter default; inference fails closed only when the anchor is needed; datasource scoping reads metric anchors; reporting adds `row_sources`; pinning stores the anchor plus `population_inferred`; recommendation follows the anchor.
- `queries/semantics`: grain guarantee, grain-union broadcasting, filter restriction and compositionality restated for inferred queries; the monotonicity, home-row preservation, filter non-expansion and unit laws.
- `queries/unrelated-datasets`: an unrelated aggregate supplies the anchor's cells; product-hop filters restrict every home; the dimensionless `unrelated_dataset` warning.
- `queries/references`: an aggregate's home is the reading dataset for its grain dimensions and filters (layered on DEV-2277's requirement).
- `queries/saved-query-refinement`: a refinement may not carry `population_inferred`.
- `queries/time-spine`: P is the dimension anchor of the other dimensions; filters restrict it, never choose it.

## Impact

- Engine: `slayer/engine/population/` (anchor inference, row supply), `slayer/engine/compile/stages.py` (row-supply planning, home-rooted grain routing), `slayer/engine/query_engine.py` (inference flow, pinning, reporting), `slayer/ir/planned.py`, `slayer/sql/generator.py` (key union), `slayer/core/query.py` (`population_inferred`), `slayer/storage/migrations.py` (SlayerQuery 18).
- Surfaces: `SlayerResponse`, REST `QueryResponse`, Python client, MCP output (`row_sources`), MCP Field descriptions and help memories.
- Docs: `docs/concepts/queries.md`, `docs/reference/{mcp,rest-api,python-client}.md`, `docs/upgrading/2.0.md`.
- Architecture: `semantics.arc42.md` (Axioms 2.9, 3.1, 12, 14, 15; Laws 3, 7–10; §4 P1), `python.engine.arc42.md` P8.
- Merges after DEV-2277, whose unarchived `queries/population`, `queries/references`, `queries/unrelated-datasets` and `queries/saved-query-refinement` deltas these deltas layer on.
