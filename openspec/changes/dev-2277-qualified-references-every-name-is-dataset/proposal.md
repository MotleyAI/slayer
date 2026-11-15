## Why

SLayer 2.0 is a strict superset of Ossie: a valid Ossie Layer 3 query must be a valid SLayer query with Ossie's meaning (umbrella DEV-2212). Ossie has three reference forms (`dataset.field`, `dataset.metric`, a bare model-scoped `metric`) and picks the join path itself (Ossie Layer 3 §3.1, §4). SLayer resolves every name relative to the query root. A bare name is a root column, a dotted path's first segment is first an edge of the root, and result keys follow the root-relative path, so `sum(orders.amount)` keys `customers.orders.amount_sum` under population `customers` and `orders.amount_sum` under `orders`. An Ossie query therefore can mean something else in SLayer.

## What Changes

- **BREAKING** A query reference is anchored at its first segment, which names a dataset of the reading dataset's datasource or a query-list stage. The rest of the path is an explicit route from it. An edge name is never a first segment; role-playing is spelled `orders.buyer.name`.
- **BREAKING** The anchor is reached by its only to-one path, else its only route, else the reference is refused as ambiguous. A direct edge has no precedence, so the literal-edge-first rule for directed FK cycles goes. One resolver serves binding, population inference, `recommend_population`, schema-drift analysis and the stored migration.
- **BREAKING** A bare name is a metric: a query alias, or later a model-scoped metric (DEV-2243). Anything else is an "unknown metric" error that suggests the qualified names. `count(*)` in a query with no population is refused, naming `count(<dataset>.*)`.
- **BREAKING** Result keys never contain the population:
  - an unaliased reference keys as its written anchor, canonical route and leaf (`customers.region`, `orders.amount_sum`, `orders._count`);
  - an aliased entry keys as its bare alias;
  - an unaliased expression keys as its references' common anchor plus its sanitized text, or as bare text when the references share no anchor.
  
  Stage column names stay the same.
- Legacy root-relative forms in a query that names a population convert with one deprecation warning per form, removed in 3.0: bare fields and metrics, `count(*)`, and paths whose first segment is an edge or a list column.
- A typed notice reports the one silent meaning change: a dataset-first reference that the 1.x reading sent through a direct edge.
- Stored queries migrate to the qualified form in the model-aware load pipeline, keeping their meaning and their stage column names. `SlayerQuery`, `SlayerModel` and `Memory` get new versions.
- `recommend_population(["count(orders.*)"])` works: the item is an attachment on `orders`. This absorbs DEV-2045.
- MCP descriptions, help memories and docs use the qualified syntax only. The migration guide gets a section.
- `architecture/semantics.arc42.md` §4 Principle 1 is rewritten and Axioms 2.4, 2.6 and 3.1 are reworded (approved).

## Capabilities

### New Capabilities
- `queries/references`: how a query reference is anchored, routed and keyed, what a bare name means, the legacy conversion and the meaning-change notice.

### Modified Capabilities
- `queries/dotted-dimension-routing`: routing reaches a first-segment dataset; the root-qualifier rule is removed; keys and suggestions follow the anchor.
- `queries/saved-measures`: a dataset metric is written `dataset.metric`; bare names are legacy; naming follows the anchor.
- `queries/measure-naming`: keys follow the anchor; an alias keys as itself.
- `queries/population`: inference uses the shared resolver without literal-first precedence; recommendation accepts `count(D.*)`.
- `queries/unrelated-datasets`: an unrelated dataset is named as a reference's anchor.
- `models/join-traversal`: edge names start no reference; the literal-edge-first cycle rule is replaced; keys follow the anchor.
- `mcp/tool-surface`: the documented idioms are written qualified.
- `models/stored-upgrade`: stored queries migrate to the qualified form.
- `compatibility/removal-tripwire`: the legacy reference forms and the notice are covered workarounds.

## Impact

- **Code:**
  - `slayer/core/syntax.py` (the parse-tree conversion);
  - `slayer/engine/binding.py`, `bind_inputs.py`, `slayer/core/join_walker.py`, `slayer/engine/population/` (shared anchor resolver), `slayer/engine/compile/stages.py`, `slayer/sql/naming.py` (keys);
  - `slayer/engine/query_engine.py` (recommend, notice, conversion entry);
  - `slayer/storage/` load pipeline and migrations; `slayer/memories/`;
  - `slayer/live_schema/schema_drift.py`;
  - MCP descriptions and `slayer/memories/help_content/`.
- **Results:** keys of joined fields, cross-dataset aggregates and aliased entries change for every caller. Stage column names do not.
- **Stored data:** new version steps for `SlayerQuery`, `SlayerModel` and `Memory`.
- **Tests:** the whole suite is respelled to the qualified syntax.
- **Downstream:** Storyline and scripts keep working on legacy spellings until 3.0, but read changed result keys.
- **Depends on:** DEV-1607 and DEV-2021 merging first. Followed by DEV-2279 (output names), DEV-2212 (row rule, path origin from each home) and DEV-2243 (model-scoped metrics).
