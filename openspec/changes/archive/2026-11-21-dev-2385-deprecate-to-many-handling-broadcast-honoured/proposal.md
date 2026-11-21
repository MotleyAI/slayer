## Why

Since every aggregation associates, `to_many_handling: "broadcast"` is only an explicit opt-in, and it adds
nothing the query language cannot already say: its value is each aggregate at the part of the grain its home
attributes, which an explicit `partition_by=` naming that part returns without a warning (Axiom 10). It is the
only mode that gives up dice–slice (Law 5), so it is deprecated in 2.x — honoured, never changing a result —
and refused in 3.0.

## What Changes

- A query stage whose effective mode is `"broadcast"` reports one `deprecated_input` warning (removal 3.0,
  replacement: an explicit `partition_by=` on each aggregate) on every run, from fresh input and stored
  documents alike; fresh input also emits one `FutureWarning` at construction. Saves report it too and persist
  `"broadcast"` as given.
- The per-run `broadcast` warning gains a structured `partition_by` list — the dimensions the aggregate
  attributes — and its hint names the aggregate call and the `partition_by=[…]` returning the same values.
- The query and refinement JSON schemas, and the invalid-value validation error, list only `associate` and
  `error`; validation still accepts `"broadcast"`.
- Agent-facing surfaces (field descriptions, messages, the relationships help memory, `docs/concepts`) stop
  offering `broadcast`; `docs/reference` lists it as deprecated; the 2.0 migration guide points to
  `partition_by=`.
- The `broadcast` value is a marked site of the 3.0 removal tripwire.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/attribution-modes`: `"broadcast"` becomes a deprecated mode with its reporting rules and the
  concrete replacement on the `broadcast` warning.
- `compatibility/removal-tripwire`: the covered set gains the `broadcast` value; a deprecated input form with no
  meaning-preserving conversion is kept as given and reported on every run that uses it.
- `mcp/tool-surface`: the guidance and the documented idiom spell a repeated total with `partition_by=[]`
  instead of the `broadcast` mode.

## Impact

`slayer/core/query.py`, `slayer/core/deprecation.py`, `slayer/core/warnings.py`, `slayer/core/errors.py`,
`slayer/engine/query_engine.py`, `slayer/engine/compile/stages.py`, `slayer/ir/planned.py`,
`slayer/engine/elaborate_env.py`, `slayer/engine/cardinality/relationship_safety.py`, the save paths (REST, MCP,
memories), `slayer/memories/help_content/06_relationships.md`, `docs/`, `examples/comparisons/matrix.yaml`,
`architecture/semantics.arc42.md`, `tests/test_law_removal_tripwire.py`. No result, SQL or storage-format change.
