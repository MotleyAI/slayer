## Why

A BI table preview projects every advertised column, saved measures included, so the facade groups — and grouping by a list column is refused, failing Metabase's previews of every model with a list column. The refusal is structural: the algebra defines no equality for a whole list, so every position that needs one (group key, `=` / `IN`, distinct count, grain member) refuses lists separately.

## What Changes

- A whole list gets one value for equality: its **element set** — its distinct non-NULL elements, sorted ascending (`collect`'s output shape); a NULL list stays NULL, `[NULL]` ≡ `[]`.
- A grouping query MAY have a list-valued dimension; its cells are formed by element-set equality and show the element set. Raw-rows queries keep projecting each record's stored list. A list MAY be a grain member wherever a group key may (`partition_by=` included).
- `=`, `<>`, `IS [NOT] DISTINCT FROM` and `IN` between two lists compare element sets; `L IN (M1, M2)` ≡ `L = M1 OR L = M2`, and an `IN` list MAY hold list operands (array literals, list variables) when its left side is a list. List-vs-scalar comparison, ordering comparisons, `BETWEEN` and `ORDER BY` on a list stay refused.
- **BREAKING** `length(L)` counts distinct non-NULL elements (`['go', 'sql', 'go']` → 2); the count with repeats stays `count(L.value)`.
- `count_distinct(L)` becomes legal: the number of distinct element sets. `count_distinct` joins `count` as a list column's default and allowed aggregation, at save time, on re-ingest and in facade catalogs.
- A structured warning names the per-element alternative `L.value` when a query groups by a whole list or counts distinct lists.
- **BREAKING** A list with opaque (`UNKNOWN`) elements supports only NULL tests and `count`; `length` over it is now refused.
- ClickHouse, which has no NULL list, reads an absent list as `[]`, alongside BigQuery's existing exception.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `models/list-columns`: lists group and compare as element sets; `count_distinct` over a list; opaque lists narrow to NULL tests and `count`; re-ingest keeps `count_distinct`; agent rendering names set grouping and comparison; ClickHouse NULL-list exception.
- `queries/list-functions`: `length` counts distinct non-NULL elements.
- `queries/expression-grammar`: an `IN` list may hold list operands.
- `facade/projection`: grouping on a list returns one row per element set; ordering by it stays refused.
- `facade/list-values`: catalogs advertise a list's `count_distinct` metric; a BI preview projecting a list column with a metric returns grouped rows.

## Impact

- `slayer/core`: a new `ListSetKey` key kind, `WholeListWarning`, ARRAY default aggregations, the save-time `allowed_aggregations` rule; `core/syntax.py` for list operands in `IN`.
- `slayer/engine`: one list-canonicalisation pass and the list checks in `elaborate_env.py` / `bind_inputs.py`; re-ingest re-typing in `ingestion.py`; stage schemas keep list types.
- `slayer/sql`: new dialect hooks `list_set` / `list_key` on the six native-list dialects (base refuses); every GROUP BY, comparison, distinct-count, window-partition and grain join-back site.
- `slayer/facade`: translator grouping checks; catalog advertises `<list>_count_distinct`.
- Agent surface (`inspect`, `search`, `models_summary`, help topic `01_models`) and docs (`docs/examples/16_list_columns`, `docs/concepts/queries.md`, `docs/concepts/references.md`, `docs/interfaces/pg-facade.md`, `docs/interfaces/flight-sql.md`).
- `architecture/semantics.arc42.md` Axiom 3.2 gains the element-set clause.
- Fixes the seven failing `metabase-e2e` tests on the parent PR.
