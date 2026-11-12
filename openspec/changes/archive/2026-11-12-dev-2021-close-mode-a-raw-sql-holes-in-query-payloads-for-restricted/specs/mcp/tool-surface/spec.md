## MODIFIED Requirements

### Requirement: Query-language guidance lives on the owning schema field, once
Each piece of query-language guidance SHALL appear in exactly one place across the `query` tool description and every description in its input schema (field- and definition-level), namely the field where an agent writes that value:
- `measures`: aggregation call form; `agg(x) FILTER (WHERE c)` aggregating only the rows where `c` holds; arithmetic combining aggregates over different models, with a cross-model ratio example rooted at customers; `partition_by=` with keys spelled like the query's dimensions (dotted paths), a share-of-group example and `partition_by=[]` for the grand total; `window=` with compact durations (`'90d'`, `'3m'`); nested aggregation; one line per transform (`cumsum`, `change` / `change_pct`, `time_shift`, `lag` / `lead`, `rank` / `dense_rank` with required direction, `ntile`, `consecutive_periods`); a joined model's aggregate is computed over that model's own rows and split per group by association across dimensions it cannot be attributed to, repeating its total instead only when `to_many_handling` is `broadcast`; aggregates over several child models in one query (orders and returns from customers) are each computed over their own rows; a child row without a parent counts in the NULL row of a parent dimension when that row exists and in an ungrouped total, and is never added again; an aggregate over a model no join path reaches from the population is computed over that model's own rows and repeated on every row, per bucket when a `time_spine` time dimension attributes it through that model's own axis, with an `unrelated_dataset` warning.
- `dimensions`: group by naturally readable labels: a name, never an id alone unless ids are asked for, and the id and the name when a name may repeat.
- `filters`: routing to row / aggregate / post-aggregation filtering; the SQL operators, including `IS [NOT] TRUE` / `IS [NOT] FALSE`, `IS [NOT] DISTINCT FROM` and `CAST`; the list functions `contains`, `contains_any` and `contains_all`; transforms in filters (top N per group via a rank); the anti-join (a joined model's key `is null`); the time-point forms.
- `variables` (a query object's): a placeholder binds a literal value only — its text inside quotes (`'{s}'`), a number, numeric string or boolean in a bare slot, a list as the whole right-hand side of `IN` or as a list-function argument (an empty list only as a list-function argument) — and a placeholder in a name position is an error.
- `order`: sorting by any aggregate or transform, displayed or not, with `limit` for top N; returning only the columns the question asks for, never adding the sort value to the measures.
- `time_dimensions`: buckets without rows are absent; a time dimension on `time_spine.timestamp` with a lower `date_range` bound returns every bucket, empty ones included, and lines several models up on one time axis.
- a time dimension's `date_range`: its forms; that `change`, `change_pct`, `time_shift` and `window=` read rows before the range (a forward `time_shift`, after it) so the range is not widened; that `cumsum` starts at the range start.
- `source_model`: rooting at the model whose every row must appear when the question asks for every such row, with a named conditional aggregate; otherwise rooting where the facts are, so rows without facts drop out, or omitting it to infer the smallest model determining every dimension and row-level condition column; a per-query column written as an expression in the position that uses it.
- `name` (stages): what needs no stage (top N per group, averaging or bucketing per-entity totals, shares and totals); that a filter on a transform's value applies after it, which is computed over every row the other filters keep; that a stage is needed only to drop rows by a dimension or time condition that a cross-row value must still see, or to join or reuse a whole result, because a row filter or `date_range` applies before a running total, `lag` / `lead`, rank, `ntile`, `consecutive_periods` or `partition_by` total while `change`, `change_pct`, `time_shift` and `window=` read past `date_range` or a filter on their own time dimension only; and that an outer stage references inner dotted columns by their flattened `__` names.

The one-query rule SHALL appear only in the server `instructions` and the `query` tool description. The gap-filling, several-child-models, parentless-child-row, cross-model-ratio, unrelated-model, projection, conditional-rooting and stage guidance SHALL also appear in no help topic and not in the server `instructions`; the instructions' fan-out reason SHALL appear only in the instructions. No description, the instructions or a help topic SHALL tell agents to use a stage to filter after a rank or window, to re-query a whole result, or to answer with one query only "whenever" it can.

Fields that reuse another field's syntax (a refinement's clauses, a measure's `formula`) SHALL point to the owning field instead of repeating its guidance. Guidance too long for its field SHALL be cited by the exact `inspect` call of the help topic that holds it.

#### Scenario: Each guidance marker has one owner
- **WHEN** the `query` tool description and every description in its input schema are collected
- **THEN** each guidance marker (for example the grand-total `partition_by=[]` example, the "rows before the range" rule, the flattened `__` stage-column rule, the stage rule, the readable-label rule) occurs in its owning field's description and nowhere else

#### Scenario: New grammar guidance has one owner
- **WHEN** the same texts are collected
- **THEN** the `FILTER (WHERE` marker occurs only in `measures`, the `IS DISTINCT FROM` and `contains_any` markers only in `filters`, the whole-`IN` list-binding marker only in a query object's `variables`, and the per-query-expression marker only in `source_model`

#### Scenario: No inline-extension-column advice remains
- **WHEN** the same texts are collected
- **THEN** none mentions an extension's `columns` or an inline model, and none advises anything but an expression for adding a per-query column

#### Scenario: Moved guidance has no second copy
- **WHEN** the `query` tool description, every description in its input schema, the server instructions and every help topic body are collected
- **THEN** each of the gap-filling (`time_spine.timestamp`), several-child-models, parentless-child-row, cross-model-ratio, unrelated-model, projection, conditional-rooting and stage-rule markers occurs only in its owning field's description, the fan-out-reason marker only in the instructions, and the one-query marker only in the instructions and the `query` description

#### Scenario: No retired stage or one-query wording remains
- **WHEN** the same texts are collected
- **THEN** none tells agents to use stages to filter after a window or rank or to re-query a whole result, none says to answer with one query "whenever" it can, and no help topic keeps a "One query first" section or a "Typical uses" list of stages

#### Scenario: Examples group by readable columns
- **WHEN** the same texts are collected
- **THEN** no example groups, partitions or flattens by a column whose name ends in `_id`

#### Scenario: A refinement clause points to the query field
- **WHEN** a client reads the `refine` argument's `measures` description
- **THEN** it refers to the query object's `measures` instead of repeating its guidance

#### Scenario: date_range is described
- **WHEN** a client reads a time dimension's `date_range` description
- **THEN** it states the accepted forms, that `change` / `change_pct` / `time_shift` / `window=` read rows before the range (a forward `time_shift`, after it), and that `cumsum` starts at the range start
