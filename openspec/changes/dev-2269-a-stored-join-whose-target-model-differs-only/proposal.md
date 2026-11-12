## Why

Unquoted model names match case-insensitively (`queries/identifier-case`), yet a stored join's `target_model` and the built-in `time_spine` are still compared exactly. A join to `customers` saves while the model is `Customers`, then fails every query through it; `source_model: TIME_SPINE` and `TIME_SPINE.timestamp` never reach the spine. A stored join to the spine saves and then fails every spine query.

## What Changes

- A save is refused when a join target reaches a stored model only by case (naming the declared spelling), when it reaches several by case, and when the saved model's name differs only in case from a peer's join target.
- Stored models below version 23 have each join target respelled to the one stored model it reaches by case, on load.
- A query's model-name positions reach the built-in `time_spine` in any case unless quoted; a stored model whose name differs from `time_spine` only in case makes the other spellings ambiguous.
- **BREAKING** A new model whose name equals `time_spine` in any case is refused; one already stored keeps working by its exact spelling and warns wherever it is loaded, saved or read.
- **BREAKING** A join targeting the built-in `time_spine` in any spelling is refused at save; on load (below version 23) such a join is dropped with a warning.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/identifier-case`: stored join targets reach models only by their exact declared spelling; case-only mismatches are refused at save and repaired on load.
- `queries/time-spine`: the built-in spine is reached in any case, its name is reserved in any case, and joins into it are refused.

## Impact

- `slayer/storage/base.py` (save validation, load-time upgrade), `slayer/storage/migrations.py` (`SlayerModel` version 23), `slayer/storage/access_filtered.py` (check order in the view).
- `slayer/core/syntax.py` (path-head respelling), `slayer/core/errors.py` (`JoinTargetCaseError`), `slayer/core/time_spine.py`.
- `slayer/engine/query_engine.py` (respelling at the entry), `slayer/engine/bundle_builder.py` (spine ambiguity, ambiguous-target backstop, warnings).
- Docs: `docs/concepts/` model joins and time spine pages.
