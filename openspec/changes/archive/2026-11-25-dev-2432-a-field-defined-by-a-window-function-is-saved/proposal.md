## Why

A dataset field whose dialect entry is a window function (`ROW_NUMBER() OVER (PARTITION BY region ORDER BY ordered_at)`) saves with validation on, then emits SQL the database rejects as a dimension, an aggregation source, inside `FILTER`, as a partition, ranking or order key, while a query filter on it is refused and raw rows compute the window over the query-filtered rows or a join's row product. The owner ruled (DEV-2432, target 1) that such a field is legal: the engine computes it over its dataset's rows before anything reads it, so every position returns the hand-oracle value.

## What Changes

- A **windowed field** — a field whose runnable dialect entry contains a window function — is computed once as a column of its dataset's filtered relation (`σ_filters(M)`), before any join, query filter or aggregation, and is read as that column in every position: dimension, time dimension, aggregation source, `FILTER`/`CASE`, partition key, ranking key, order key, query filter, relationship key and other fields' definitions.
- Query filters and joins no longer change a windowed field's value: raw-row queries that filter today return the window computed over the dataset's rows (a value change for those queries, required by Axiom 1 and Law 9).
- A derived field over a windowed field reads the materialised column; a window over a windowed field is computed one layer up; a window reading to-one joined fields is computed with those joins.
- The query-filter refusal of a windowed field (`IllegalWindowInFilterError` from the binder) is removed; raw `OVER` written into filter or metric text stays refused.
- A model filter reading a joined model's windowed field is accepted; one reading its own model's windowed field (directly or through derived fields) is refused as a filter cycle (`DatasetFilterCycleError`), at both save doors and at query time.
- A portable (Mode B) windowed field is refused by the storage save door as the engine door already refuses it; Mode B windows remain DEV-2457's.
- `architecture/semantics.arc42.md` Axiom 1 gains the windowed-column clause (approved wording).

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/field-expressions`: adds that a windowed field is a column of its dataset in every position, and that a portable windowed field is refused at both save doors.
- `models/model-filters`: a windowed field reads its own model's dataset in the filter-cycle rule; a filter reading a joined model's windowed field is no longer refused.

## Impact

- `slayer/engine/definitions.py` (windowed classification at resolution), `slayer/engine/binding.py` (windowed reference binds as a materialised column; filter refusal removed), `slayer/ir/planned.py` (typed computed join keys), `slayer/sql/generator.py` (`_emit_relation` wrapper; join-key rendering), `slayer/sql/field_expansion.py` (expansion stops at windowed fields), `slayer/engine/field_dependency.py` and `slayer/engine/elaborate_env.py` (filter-cycle walk), `slayer/storage/base.py` (portable windowed refusal).
- `architecture/semantics.arc42.md` Axiom 1.
- Docs: the field-definition concept page.
- Release: 2.x (bug fix plus additive); no API removal.
