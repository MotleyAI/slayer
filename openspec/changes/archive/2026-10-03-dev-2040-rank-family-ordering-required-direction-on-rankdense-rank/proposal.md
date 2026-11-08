## Why

The rank family always orders by the inner value descending, with no way to choose. An
agent asked for "the cheapest ACI" wrote `rank(total_fees) <= 1` and silently got the
most expensive one. The only workaround, `rank(-x)`, works only for numbers, so
"earliest" or "alphabetically first" cannot be ranked from the bottom at all.

## What Changes

- **BREAKING** `rank` and `dense_rank` take a required keyword `direction='asc' | 'desc'`
  (synonyms `ascending` / `descending`, any case — the words of `OrderItem.direction`).
  Omitting it is a typed `TransformArgumentError` that shows both spellings; there is
  no default.
- **BREAKING** `ntile` and `percent_rank` reject `direction` and order ascending:
  quartile 1 is the lowest, and a higher value gets a higher percent rank. This flips
  today's descending order and is not migrated.
- **BREAKING** For all four, a NULL inner value gives a NULL result, and NULL rows take
  no rank position, no bucket and no share of `percent_rank`'s denominator. The result
  is identical on every dialect.
- **BREAKING** An unnamed rank's derived result key spells its direction as a bare
  value (`rank_a_sum_desc` / `rank_a_sum_asc`); existing unnamed rank keys change.
- Stored artifacts migrate lazily on load: a bare `rank(` / `dense_rank(` in a
  persisted model, query or memory older than this change gains `direction='desc'`,
  keeping its meaning. Fresh payloads are never filled in. The migration registry gains
  a general stored-only step kind for this.
- One core rule for `direction`, shared by the query binder and the importer formula
  validator.
- Docs, examples and agent-facing text (the `query` tool's description, error
  suggestions) spell the direction.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/transforms`: adds the ordering-direction, NULL-input and stored-rank
  migration requirements; existing rank scenarios spell `direction` and their
  NULL-inner values become NULL.
- `queries/measure-naming`: the derived key of an unnamed rank spells its direction as
  a bare value.
- `queries/partitioned-aggregates`: rank scenarios spell `direction`; values for the
  all-NULL Void cell become NULL.
- `queries/computed-dimensions`: rank scenarios spell `direction`; the Void cell's rank
  becomes NULL.
- `queries/semantics`: rank scenarios spell `direction`.
- `aggregations/functional-form`: rank scenarios spell `direction`.

## Impact

- `slayer/core` (shared direction rule, `TransformArgumentError`, `OrderItem` synonym
  table), `slayer/core/formula.py` (importer validator), `slayer/engine/binding.py`
  (binder), `slayer/sql/generator.py` (window emission), the result-key naming of
  rank-family transforms.
- `slayer/storage/migrations.py` and new migration steps (`SlayerModel` v13,
  `SlayerQuery` v5, `Memory` v3), plus version stamping on the model and memory load
  paths in `slayer/storage/base.py`, `yaml_storage.py` and `sqlite_storage.py`.
- `slayer/mcp/server.py`, `slayer/sql/window_detect.py`, `slayer/core/errors.py`
  agent-facing text; `docs/`, `docs/examples/` notebooks, `examples/`.
- About 98 test files that use rank-family formulas; golden SQL baselines.
- Save-time validation of formulas is out of scope (DEV-2043): a bare `rank` saved
  after this change fails when queried.
