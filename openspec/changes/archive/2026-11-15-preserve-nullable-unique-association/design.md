## Context

The association producer groups by the result grain and an identity for each row in the measure's home dataset. It then aggregates the deduplicated rows. A standalone `unique` column can permit multiple NULLs and the model does not carry nullability. The producer's identity predicate filters out those rows before deduplication.

## Decision

Use the declared primary-key columns for association identity. When there is no primary key, use the existing per-query row number over the filtered home relation, before join fan-out. A row number identifies each physical row, including rows with NULL column values. The existing list-only path associates at element grain. Join-cardinality inference still uses `unique` metadata, where it can prove an equality join reaches at most one row.

Removing the presence predicate would group all NULL-key rows together. Using the existing numbered relation avoids that collapse and keeps the engine's current dialect-specific row-number emission.

Parameter checks use the numbered home in the association scope. A row-valued parameter such as `weighted_avg(amount, weight=weight)` remains determined by its home row, so it can contribute to the associated group.

## Trade-off

The window operation adds work for associated models that previously used a standalone unique column. A future optimization could choose a standalone key when the model can prove its values are non-NULL.
