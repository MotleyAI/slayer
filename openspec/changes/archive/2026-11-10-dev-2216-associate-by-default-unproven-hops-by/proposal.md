## Why

Slice 4 of SLayer 2.0 (umbrella DEV-2212). A dimension an aggregate's home does not
provably determine — across a fanning hop or a merely unproven one — broadcasts by
default today: the grand total repeated per cell with a warning (Ossie Appendix B.1 with a
keyless `customers` and an undeclared `orders → customers` join gives 650 per region).
Ossie Layer 2 evaluates a measure over the distinct source rows reaching each group
(association), which gives EAST 350 · WEST 300 · NORTH NULL; making association the
default aligns SLayer with it and makes the dice–slice law hold by default.

## What Changes

- **BREAKING**: omitting `to_many_handling` now associates — each cell aggregates the
  distinct home entities associated with it, with the `associated` warning. The field
  becomes optional (`null` = the default); `"broadcast"`, `"associate"` and `"error"` keep
  their meaning when named.
- Association-ineligible aggregates (`window=`, `first`/`last`, a row-valued parameter
  the entity grain does not determine) broadcast under the default with a warning naming
  the reason; explicit `"associate"` keeps raising the typed error.
- A home without a primary or unique key is associated through a per-query row identity;
  the "root without a unique key" association error is removed in every mode.
- An ungrained re-aggregation inner (`sum(sum(customers.spend))`) resolves per the mode
  like its bare form, instead of ignoring `"associate"`.
- The broadcast warning gains an optional unsupported-association reason; the ordinary
  broadcast hint names omitting the mode as the reconciliation.
- **BREAKING**: stored queries carrying `to_many_handling: "broadcast"` (written whenever
  the old default was saved) load with the mode omitted; set `"broadcast"` again to keep
  the old values.
- Input safety is unchanged: an aggregation input crossing a fanning or unproven hop, and a
  partition key fanning from its host, still fail in every mode. (The issue listed
  unproven-hop inputs as a broadcast fallback; broadcast has no value to repeat for them,
  so they stay errors — see design.md.)
- Wording follows the new default: the error-mode remedy, the save-time unproven-arity
  warning, the join-safety audit, the MCP field descriptions and help memory, and the docs.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/attribution-modes`: the association default and its broadcast fallback, row
  identity for keyless homes, stored-default migration, mode-aware ungrained
  re-aggregation inners.
- `queries/semantics`: attribution, no double counting, loud degradation, dice–slice and
  second-order aggregation stated against the new default.
- `queries/cross-model-aggregates`: fan-out-safe grain, broadcast metadata (fallback
  reason, merge identity), explicit grain, composition and intermediate hops under the
  default.
- `queries/partitioned-aggregates`: explicit and outer partition keys associate by
  default; default-mode scenarios pinned to explicit `broadcast`; ranked picks fall back.
- `aggregations/expression-aggregation`: default-mode scenarios pinned to explicit
  `broadcast`.
- `mcp/tool-surface`: documented idioms and field guidance state the association default.
- `models/join-cardinality`: unproven joins resolve per mode (associated by default); the
  validation report states the new consequence.
- `queries/list-children`: parent measures associate by default; a keyless parent across
  a non-list hop uses the row identity.
- `queries/query-backed-inline`, `queries/saved-measures`, `queries/time-spine`,
  `queries/transforms`, `queries/unrelated-datasets`: scenarios restated against the new
  default or pinned to explicit `broadcast`.

## Impact

- **Code**: `slayer/core/query.py` (field, refinement settings), `slayer/ir/prebound.py`,
  `slayer/engine/bind_inputs.py` (resolved mode policy), `slayer/engine/compile/stages.py`
  and `discovery.py` (dispatch, eligibility classification, re-aggregation), 
  `slayer/engine/elaborate_env.py` (checks), `slayer/ir/planned.py` and
  `slayer/sql/generator.py` (row-identity entity key), `slayer/sql/dialects/` (row-number
  spelling), `slayer/sql/naming.py`, `slayer/core/warnings.py` and `errors.py`,
  `slayer/engine/query_engine.py` (warning merge, error-mode remedy),
  `slayer/engine/column_dependency.py`, `slayer/engine/cardinality/join_safety.py` (audit
  text), `slayer/storage/migrations.py` (SlayerQuery v10), `slayer/usage_reporting/`.
- **APIs**: REST and MCP `to_many_handling` default; response warnings
  (`associated` by default; `broadcast` gains `association_unsupported`).
- **Architecture**: `architecture/semantics.arc42.md` Axiom 8 and Law 5 (approved wording).
- **Docs**: `docs/concepts/{queries,formulas}.md`, `docs/reference/{rest-api,mcp,usage-reporting}.md`,
  the `06_joins` help memory, `docs/examples/16_list_columns`, `examples/comparisons`.
- **Tests**: existing tests pinning the broadcast default are realigned (explicit
  `broadcast` for mechanics, associated values for the default) with per-file approval.
